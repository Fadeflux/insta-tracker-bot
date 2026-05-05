// Personal record notifier
// ──────────────────────
// When a VA's post hits more views than ANY of their previous posts (across
// all their accounts and all platforms), they get a celebratory message in
// their ticket. Pure motivation feature — keeps the VAs engaged.
//
// Triggered after each successful snapshot. To avoid spam:
//   - We only fire when the post FIRST exceeds the previous all-time-best
//     (we mark the post as "record-notified" once)
//   - We require the new record to be at least 1.5× the old record AND
//     at least 1000 views (so we don't fire on tiny improvements at the
//     bottom of the curve)

var config = require('../../config');
var logger = require('../utils/logger');

var discordClient = null;
function setDiscordClient(client) { discordClient = client; }

// Minimum view count for a record-worthy post — below this the VA is too
// new and "record" notifications would be pointless
var MIN_RECORD_VIEWS = 1000;
// New record must beat the previous one by at least this multiplier
var RECORD_BOOST_FACTOR = 1.5;

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

// === Main: check if this scrape made the post the VA's new record ===
async function maybeNotifyPersonalRecord(db, post, stats) {
  if (!discordClient || !post || !stats) return;
  if (!post.va_discord_id) return;

  var views = Number(stats.views || 0);
  if (views < MIN_RECORD_VIEWS) return;

  // Idempotency: did we already mark this post as a record notification?
  // We piggyback on post_viral_milestones_sent with a sentinel threshold of -2.
  try {
    var existing = await db.pool.query(
      "SELECT 1 FROM post_viral_milestones_sent WHERE post_id = $1 AND threshold = -2",
      [post.id]
    );
    if (existing.rows.length > 0) return; // already notified for this post
  } catch (e) { /* table missing, skip silently */ }

  // Find the VA's previous best across ALL their other posts (excluding this one)
  var prevBest;
  try {
    var r = await db.pool.query(
      "SELECT COALESCE(MAX(latest.views), 0) AS best_views, COUNT(*) AS post_count " +
      "FROM posts p " +
      "LEFT JOIN LATERAL (" +
      "  SELECT views FROM snapshots s " +
      "  WHERE s.post_id = p.id AND COALESCE(s.error, '') <> 'coaching_sent' " +
      "  ORDER BY s.scraped_at DESC LIMIT 1" +
      ") latest ON true " +
      "WHERE p.va_discord_id = $1 AND p.deleted_at IS NULL AND p.id <> $2",
      [post.va_discord_id, post.id]
    );
    prevBest = {
      views: Number(r.rows[0] && r.rows[0].best_views || 0),
      postCount: Number(r.rows[0] && r.rows[0].post_count || 0),
    };
  } catch (e) {
    logger.warn('[PersonalRecord] history query failed: ' + e.message);
    return;
  }

  // Need at least 5 prior posts to consider this a "career record" — otherwise
  // a brand new VA's 2nd post automatically becomes their record, which isn't
  // really meaningful.
  if (prevBest.postCount < 5) return;

  // Must beat previous record AND by a meaningful margin
  if (views <= prevBest.views) return;
  if (prevBest.views > 0 && views < prevBest.views * RECORD_BOOST_FACTOR) return;

  // Resolve channel
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
    logger.info('[PersonalRecord] no ticket for VA ' + post.va_name);
    await markRecordSent(db, post.id);
    return;
  }

  function fm(n) { return Number(n || 0).toLocaleString('fr-FR'); }
  var vaMention = '<@' + post.va_discord_id + '>';
  var content =
    '🎉 **NOUVEAU RECORD PERSONNEL !** 🎉\n\n' +
    vaMention + ' ce post a fait **' + fm(views) + ' vues** — c\'est ton record absolu !\n\n' +
    (prevBest.views > 0
      ? '   📊 Ancien record : ' + fm(prevBest.views) + ' vues (+' + fm(views - prevBest.views) + ' vues, ×' + (views / prevBest.views).toFixed(1) + ')\n'
      : '') +
    '   🔗 ' + post.url + '\n' +
    '   📱 Compte : @' + (post.account_username || '?') + ' (' + post.platform.toUpperCase() + ')\n\n' +
    '🚀 **Felicitations, continue sur cette lancee !** Reproduis ce style de contenu pour confirmer.';

  try {
    await channel.send({ content: content, allowedMentions: { parse: ['users'] } });
    logger.info('[PersonalRecord] sent for VA ' + post.va_name + ' (post ' + post.id + ', ' + fm(views) + ' vues, prev=' + fm(prevBest.views) + ') in #' + channel.name);
    await markRecordSent(db, post.id);
  } catch (e) {
    logger.warn('[PersonalRecord] send failed: ' + e.message);
  }
}

async function markRecordSent(db, postId) {
  try {
    await db.pool.query(
      "INSERT INTO post_viral_milestones_sent (post_id, threshold) VALUES ($1, -2) ON CONFLICT DO NOTHING",
      [postId]
    );
  } catch (e) {
    logger.warn('[PersonalRecord] mark sent failed: ' + e.message);
  }
}

module.exports = {
  setDiscordClient: setDiscordClient,
  maybeNotifyPersonalRecord: maybeNotifyPersonalRecord,
};
