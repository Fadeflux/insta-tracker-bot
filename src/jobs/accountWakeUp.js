// Account wake-up detector
// ───────────────────────
// Notifies the VA when a previously-flopping account suddenly produces a
// post that performs well. This is a positive signal: the algorithm has
// started distributing the content again, and the VA should reproduce
// what worked.
//
// Trigger: a post crosses 5000 views AND the account's previous 10 posts
// were all under 500 views on average (= the account had been flopping).
//
// Like the deleted-post detector, we notify only in the VA's ticket.

var config = require('../../config');
var logger = require('../utils/logger');

var WAKEUP_VIEWS_THRESHOLD = 5000;     // post must hit this to count as wake-up
var FLOP_AVERAGE_THRESHOLD = 500;      // previous-posts average must be under
var MIN_PRIOR_POSTS = 10;              // need at least this many prior posts

var discordClient = null;
function setDiscordClient(client) { discordClient = client; }

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

// === Main entry: called from scrapeQueue after each successful snapshot ===
async function maybeNotifyWakeUp(db, post, stats) {
  if (!discordClient) return;
  if (!post || !post.account_id || !stats) return;
  var views = Number(stats.views || 0);
  if (views < WAKEUP_VIEWS_THRESHOLD) return;

  // Idempotency: only notify once per account per "wake-up phase".
  // We use account_alerts_sent with kind='wake_up' (severity = peak views
  // at time of notification, so we re-notify if the account wakes up
  // significantly more later).
  var prevSeverity = 0;
  try {
    var prev = await db.pool.query(
      "SELECT last_severity FROM account_alerts_sent WHERE account_id = $1 AND kind = 'wake_up'",
      [post.account_id]
    );
    if (prev.rows.length > 0) prevSeverity = Number(prev.rows[0].last_severity);
  } catch (e) { /* table missing — best-effort skip */ }
  // Only re-notify if the new wake-up is at least 2× the previous one
  if (prevSeverity > 0 && views < prevSeverity * 2) return;

  // Check the account's prior history. We want to confirm the account was
  // really flopping before this post, not just that it had one bad week.
  var historyResult;
  try {
    historyResult = await db.pool.query(
      "SELECT COUNT(*) AS count, AVG(latest.views) AS avg_views " +
      "FROM (SELECT p.id FROM posts p " +
      "  WHERE p.account_id = $1 AND p.deleted_at IS NULL AND p.id <> $2 " +
      "  ORDER BY p.created_at DESC LIMIT 10) recent " +
      "LEFT JOIN LATERAL (" +
      "  SELECT views FROM snapshots s " +
      "  WHERE s.post_id = recent.id AND COALESCE(s.error, '') <> 'coaching_sent' " +
      "  ORDER BY s.scraped_at DESC LIMIT 1" +
      ") latest ON true",
      [post.account_id, post.id]
    );
  } catch (e) {
    logger.warn('[WakeUp] history query failed: ' + e.message);
    return;
  }
  var priorCount = Number(historyResult.rows[0] && historyResult.rows[0].count || 0);
  var priorAvg = Number(historyResult.rows[0] && historyResult.rows[0].avg_views || 0);
  if (priorCount < MIN_PRIOR_POSTS) return; // not enough history
  if (priorAvg >= FLOP_AVERAGE_THRESHOLD) return; // account wasn't really flopping

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
    logger.info('[WakeUp] no ticket for VA ' + post.va_name);
    // Record anyway so we don't keep retrying
    await recordWakeUpSent(db, post.account_id, views);
    return;
  }

  function fm(n) { return Number(n || 0).toLocaleString('fr-FR'); }
  var vaMention = post.va_discord_id ? ('<@' + post.va_discord_id + '>') : ('@' + (post.va_name || 'VA'));

  var content =
    '🎉 **TON COMPTE SE REVEILLE !** 🎉\n\n' +
    vaMention + ' ton compte **@' + (post.account_username || '?') + '** vient de cartonner avec un post a ' + fm(views) + ' vues, alors qu\'il etait en flop (' + fm(Math.round(priorAvg)) + ' vues en moyenne sur les ' + priorCount + ' posts precedents).\n\n' +
    '🔗 ' + post.url + '\n\n' +
    '💡 L\'algorithme a peut-etre debloque le compte. Essaie de reproduire ce style de contenu pour confirmer la tendance !';

  try {
    await channel.send({ content: content, allowedMentions: { parse: ['users'] } });
    logger.info('[WakeUp] sent for @' + post.account_username + ' (post ' + post.id + ', ' + fm(views) + ' vues) in #' + channel.name);
    await recordWakeUpSent(db, post.account_id, views);
  } catch (e) {
    logger.warn('[WakeUp] send failed: ' + e.message);
  }
}

async function recordWakeUpSent(db, accountId, severity) {
  try {
    await db.pool.query(
      "INSERT INTO account_alerts_sent (account_id, kind, last_severity, sent_at) " +
      "VALUES ($1, 'wake_up', $2, NOW()) " +
      "ON CONFLICT (account_id, kind) DO UPDATE SET last_severity = EXCLUDED.last_severity, sent_at = NOW()",
      [accountId, severity]
    );
  } catch (e) {
    logger.warn('[WakeUp] record failed: ' + e.message);
  }
}

module.exports = {
  setDiscordClient: setDiscordClient,
  maybeNotifyWakeUp: maybeNotifyWakeUp,
};
