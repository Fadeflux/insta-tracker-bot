// Deleted post detector
// ────────────────────
// When a tracked post starts returning "post not found" / 404 after having
// previously returned valid stats, we infer Instagram (or another platform)
// has deleted the post. This is often a TOS violation signal — the VA needs
// to know so they don't keep posting that style of content.
//
// We notify in the VA's ticket only (per the user's request) — managers see
// it in the VA's ticket too thanks to the role mention.
//
// Triggered from the scrape worker via notifyIfDeleted(post, scrapeError).

var config = require('../../config');
var logger = require('../utils/logger');

var discordClient = null;
function setDiscordClient(client) { discordClient = client; }

// Patterns that suggest the post was removed by the platform (not just a
// transient scrape error). We're conservative — we only notify if we get a
// strong signal AND the post had previously accumulated views.
var DELETED_PATTERNS = [
  /not\s*found/i,
  /404/,
  /post.*deleted/i,
  /post.*unavailable/i,
  /this\s*post\s*has\s*been\s*removed/i,
  /this\s*content\s*isn'?t\s*available/i,
  /la\s*page\s*n'?est\s*plus\s*disponible/i,
];

function looksLikeDeletion(errorMessage) {
  if (!errorMessage) return false;
  return DELETED_PATTERNS.some(function(re) { return re.test(errorMessage); });
}

// Find the VA's ticket channel
async function findVaTicketChannel(guild, vaUsername) {
  if (!guild || !vaUsername) return null;
  var target = String(vaUsername).toLowerCase().trim();
  if (target.startsWith('@')) target = target.slice(1);
  try {
    var found = guild.channels.cache.find(function(ch) {
      return ch && ch.type === 0 && ch.name && ch.name.toLowerCase() === target;
    });
    if (found) return found;
    var all = await guild.channels.fetch();
    var foundFresh = all.find(function(ch) {
      return ch && ch.type === 0 && ch.name && ch.name.toLowerCase() === target;
    });
    return foundFresh || null;
  } catch (e) { return null; }
}

// === Main entry: called from scrapeQueue when a scrape returns an error ===
// We require BOTH:
//   1. The error matches a deletion pattern
//   2. The post had previous valid snapshots with views > 0
// to avoid false positives (e.g. transient network errors during a fresh post).
async function notifyIfDeleted(db, post, scrapeError) {
  if (!discordClient || !post || !scrapeError) return;
  if (!looksLikeDeletion(scrapeError)) return;

  // Did this post ever have valid stats? If not, it was probably never
  // properly published — we don't want to falsely alert about brand-new
  // posts that fail their first scrape due to network issues.
  var prior;
  try {
    prior = await db.pool.query(
      "SELECT MAX(views) AS peak_views, COUNT(*) AS valid_count " +
      "FROM snapshots WHERE post_id = $1 AND views IS NOT NULL AND views > 0",
      [post.id]
    );
  } catch (e) {
    logger.warn('[DeletedPost] history check failed: ' + e.message);
    return;
  }
  var peakViews = Number(prior.rows[0] && prior.rows[0].peak_views || 0);
  var validCount = Number(prior.rows[0] && prior.rows[0].valid_count || 0);
  if (validCount < 1 || peakViews < 50) {
    // Not enough prior data to confidently say "this was deleted"
    return;
  }

  // Idempotency: don't notify twice for the same post. We piggyback on the
  // post_viral_milestones_sent table by using a sentinel threshold of -1
  // ("deletion notice sent") — saves us from creating yet another tracker
  // table for a single-flag case.
  try {
    var existing = await db.pool.query(
      "SELECT 1 FROM post_viral_milestones_sent WHERE post_id = $1 AND threshold = -1",
      [post.id]
    );
    if (existing.rows.length > 0) return; // already notified
  } catch (e) { /* table missing — best-effort skip */ }

  // Resolve guild + channel
  var platforms = config.getActivePlatforms();
  var pc = platforms.find(function(p) { return p.name === post.platform; });
  if (!pc || !pc.guildId) return;
  var guild;
  try { guild = await discordClient.guilds.fetch(pc.guildId); }
  catch (e) { return; }

  var channel = await findVaTicketChannel(guild, post.va_name);
  if (!channel && post.va_discord_id) {
    try {
      var member = await guild.members.fetch(post.va_discord_id);
      if (member) channel = await findVaTicketChannel(guild, member.user.username);
    } catch (e) {}
  }
  if (!channel) {
    logger.info('[DeletedPost] no ticket for VA ' + post.va_name + ' (post ' + post.id + ')');
    // Still record so we don't keep retrying
    try {
      await db.pool.query(
        "INSERT INTO post_viral_milestones_sent (post_id, threshold) VALUES ($1, -1) ON CONFLICT DO NOTHING",
        [post.id]
      );
    } catch (e) {}
    return;
  }

  function fm(n) { return Number(n || 0).toLocaleString('fr-FR'); }
  var vaMention = post.va_discord_id ? ('<@' + post.va_discord_id + '>') : ('@' + (post.va_name || 'VA'));
  var managerMention = pc.managerRoleId ? ('<@&' + pc.managerRoleId + '>') : '@manager';
  var teamLeaderMention = pc.teamLeaderRoleId ? (' <@&' + pc.teamLeaderRoleId + '>') : '';
  var leadership = managerMention + teamLeaderMention;

  var content =
    '🚨 **POST SUPPRIME PAR ' + post.platform.toUpperCase() + '** 🚨\n\n' +
    vaMention + ' ton post sur **@' + (post.account_username || '?') + '** a probablement ete supprime par la plateforme.\n\n' +
    '📊 Le post avait fait ' + fm(peakViews) + ' vues avant suppression.\n' +
    '🔗 ' + post.url + '\n\n' +
    '⚠️ Ca peut signifier :\n' +
    '   • Violation des regles de la plateforme (TOS)\n' +
    '   • Contenu signale par des utilisateurs\n' +
    '   • Probleme algorithmique\n\n' +
    '⚠️ ' + leadership + ' — A verifier. Eviter de reproduire ce style de contenu sur ce compte si TOS.';

  try {
    await channel.send({ content: content, allowedMentions: { parse: ['users', 'roles'] } });
    logger.info('[DeletedPost] notified for post ' + post.id + ' in #' + channel.name);
  } catch (e) {
    logger.warn('[DeletedPost] send failed: ' + e.message);
    return; // don't mark as sent — try again next time
  }

  // Record so we don't re-notify
  try {
    await db.pool.query(
      "INSERT INTO post_viral_milestones_sent (post_id, threshold) VALUES ($1, -1) ON CONFLICT DO NOTHING",
      [post.id]
    );
  } catch (e) {}
}

module.exports = {
  setDiscordClient: setDiscordClient,
  notifyIfDeleted: notifyIfDeleted,
  looksLikeDeletion: looksLikeDeletion,
};
