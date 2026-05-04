// Ticket viral notifier
// ─────────────────────
// When a tracked post crosses certain view thresholds (8k, 20k, 50k, 100k),
// post a callout in the VA's personal ticket channel and ping the manager
// role so they can ask the VA to repost the same video on the same account
// (with metadata changes) — viral content should be reused on its winning
// account.
//
// How we find the VA's ticket channel: the "Lola" bot creates one channel per
// VA when they receive the VA role on Discord, and names that channel after
// the VA's Discord username. So we just look up the channel by name in the
// guild associated with the post's platform.

var config = require('../../config');
var logger = require('../utils/logger');

// Threshold tiers: each one fires once per post in ascending order.
// Adjust the array if you want to add/remove tiers — just keep it sorted.
var THRESHOLDS = [8000, 20000, 50000, 100000];

// One Discord client is shared across the whole bot. Wired in at startup
// from the main entrypoint via setDiscordClient(client).
var discordClient = null;
function setDiscordClient(client) { discordClient = client; }

// Build the message text for a given threshold. We adapt the tone as the
// post climbs — 8k is "nice, repost it", 100k is "wow this is exceptional".
function buildMessage(opts) {
  var threshold = opts.threshold;
  var post = opts.post;
  var stats = opts.stats;
  var vaMention = opts.vaMention;       // "<@123>" or fallback to "@username"
  var managerMention = opts.managerMention; // "<@&456>" or "@manager"
  var emoji, header, urgency;
  if (threshold >= 100000) {
    emoji = '🚀🚀🚀'; header = 'POST EXCEPTIONNEL — 100K+ VUES';
    urgency = 'C\'est une pepite enorme. A REPOSTER IMMEDIATEMENT sur le meme compte avec changement de metadonnes.';
  } else if (threshold >= 50000) {
    emoji = '🔥🔥'; header = 'POST EN FEU — 50K+ VUES';
    urgency = 'Vraiment tres bonne perf. A reposter au plus vite sur le meme compte avec changement de metadonnes.';
  } else if (threshold >= 20000) {
    emoji = '🔥'; header = 'POST VIRAL — 20K+ VUES';
    urgency = 'Bien joue ! A reposter rapidement sur le meme compte avec changement de metadonnes.';
  } else {
    emoji = '✨'; header = 'POST EN BOOST — 8K+ VUES';
    urgency = 'Continue comme ca. A reposter sur le meme compte avec changement de metadonnes.';
  }
  // Format stats lines, using non-breaking thousand separators
  function fm(n) { return Number(n || 0).toLocaleString('fr-FR'); }
  var platformLabel = (post.platform || '').toUpperCase();
  var msg =
    emoji + ' **' + header + '** ' + emoji + '\n\n' +
    vaMention + ' ton post ' + platformLabel + ' vient de depasser ' + fm(threshold) + ' vues !\n\n' +
    '📊 **Stats actuelles**\n' +
    '   • ' + fm(stats.views) + ' vues\n' +
    '   • ' + fm(stats.likes) + ' likes\n' +
    '   • ' + fm(stats.comments) + ' commentaires\n\n' +
    '🔗 ' + (post.url || '(lien indisponible)') + '\n\n' +
    '⚠️ ' + managerMention + ' — ' + urgency;
  return msg;
}

// Find the VA's ticket channel inside the relevant platform guild. Lola
// names the channel after the VA's Discord username (lowercased usually),
// so we do a case-insensitive match across all text channels of the guild.
//
// Returns null if not found — the caller will silently skip the notification
// (we don't want to spam logs every time a VA hasn't received their ticket
// yet, e.g. just-added VAs or platform mismatches).
async function findVaTicketChannel(guild, vaUsername) {
  if (!guild || !vaUsername) return null;
  var target = String(vaUsername).toLowerCase().trim();
  // Strip leading @ if present (some va_name fields include it)
  if (target.startsWith('@')) target = target.slice(1);
  try {
    // Use the cache first (cheap), then fall back to a fetch if nothing matched
    var found = guild.channels.cache.find(function(ch) {
      return ch && ch.type === 0 /* GUILD_TEXT */ &&
             ch.name && ch.name.toLowerCase() === target;
    });
    if (found) return found;
    // Fetch all channels (paged) and try again — useful right after bot boot
    var all = await guild.channels.fetch();
    var foundFresh = all.find(function(ch) {
      return ch && ch.type === 0 && ch.name && ch.name.toLowerCase() === target;
    });
    return foundFresh || null;
  } catch (e) {
    logger.warn('[ViralTicket] findVaTicketChannel failed: ' + e.message);
    return null;
  }
}

// Main entry: called from the scrape worker after each successful snapshot.
// We check whether the post's current view count crossed any unsent threshold,
// and if so post a single message per crossed threshold.
//
// Args:
//   post   — the row from `posts` table (must contain id, url, platform,
//            va_discord_id, va_name, account_username)
//   stats  — { views, likes, comments } from the latest snapshot
//   db     — the queries module (so we can use its pool without re-requiring)
async function maybeNotifyMilestone(post, stats, db) {
  if (!discordClient) return; // bot not booted yet
  if (!post || !stats) return;
  var views = Number(stats.views || 0);
  if (views < THRESHOLDS[0]) return; // not even at the first tier — fast exit

  // Decide which thresholds this post has just crossed but not yet announced.
  // We pull the already-sent thresholds from DB to make this idempotent
  // across scrape runs.
  var sentRows;
  try {
    var r = await db.pool.query(
      'SELECT threshold FROM post_viral_milestones_sent WHERE post_id = $1',
      [post.id]
    );
    sentRows = r.rows;
  } catch (e) {
    logger.warn('[ViralTicket] failed to load sent milestones: ' + e.message);
    return;
  }
  var sent = {};
  sentRows.forEach(function(row) { sent[Number(row.threshold)] = true; });

  // Pick the HIGHEST unsent threshold the post has crossed. If a post jumps
  // from 5k to 25k between two scrapes we want to fire the 20k tier (and
  // record both 8k and 20k as sent). We send one Discord message per scrape
  // — the highest unlocked tier — to avoid spamming the channel.
  var crossed = [];
  for (var i = 0; i < THRESHOLDS.length; i++) {
    var t = THRESHOLDS[i];
    if (views >= t && !sent[t]) crossed.push(t);
  }
  if (crossed.length === 0) return;
  var topThreshold = crossed[crossed.length - 1];

  // Resolve the platform's guild and manager role from config.
  var platforms = config.getActivePlatforms();
  var pc = platforms.find(function(p) { return p.name === post.platform; });
  if (!pc || !pc.guildId) {
    logger.warn('[ViralTicket] no guild configured for platform ' + post.platform);
    return;
  }
  var guild;
  try {
    guild = await discordClient.guilds.fetch(pc.guildId);
  } catch (e) {
    logger.warn('[ViralTicket] cannot fetch guild ' + pc.guildId + ': ' + e.message);
    return;
  }

  // Find the ticket channel by VA username. We try va_name first because
  // Lola creates the channel from the Discord username — but va_name in
  // our DB sometimes carries display name instead, so we also try a cleaned
  // form of va_discord_id resolution if the first attempt fails.
  var channel = await findVaTicketChannel(guild, post.va_name);
  if (!channel && post.va_discord_id) {
    // Fallback: fetch the member, use their username
    try {
      var member = await guild.members.fetch(post.va_discord_id);
      if (member) {
        channel = await findVaTicketChannel(guild, member.user.username);
      }
    } catch (e) { /* fallback failed, leave channel null */ }
  }
  if (!channel) {
    logger.info('[ViralTicket] no ticket channel found for VA ' + (post.va_name || post.va_discord_id) + ' (post ' + post.id + ', threshold ' + topThreshold + ')');
    // We still record the crossed threshold(s) as sent — otherwise we'd retry
    // every minute forever for a VA who simply doesn't have a ticket. The
    // message is just lost, not a critical failure.
    await markThresholdsSent(db, post.id, crossed);
    return;
  }

  // Build mentions. VA gets a real mention if we have their discord_id;
  // manager always gets the role mention (so all managers see it).
  var vaMention = post.va_discord_id ? ('<@' + post.va_discord_id + '>') : ('@' + (post.va_name || 'VA'));
  var managerMention = pc.managerRoleId ? ('<@&' + pc.managerRoleId + '>') : '@manager';

  var content = buildMessage({
    threshold: topThreshold,
    post: post,
    stats: stats,
    vaMention: vaMention,
    managerMention: managerMention,
  });

  try {
    await channel.send({
      content: content,
      // Allow user + role pings so the manager and VA actually get notified
      allowedMentions: { parse: ['users', 'roles'] },
    });
    logger.info('[ViralTicket] sent threshold ' + topThreshold + ' notif for post ' + post.id + ' in #' + channel.name);
  } catch (e) {
    logger.warn('[ViralTicket] cannot send to #' + channel.name + ': ' + e.message);
    // Don't mark as sent — try again on the next scrape (channel may be
    // temporarily inaccessible due to permissions glitch)
    return;
  }

  // Mark all crossed thresholds as sent (not just the top one). Otherwise
  // a post that jumped from 7k to 25k would get the 20k message now and
  // the 8k message later — backwards order.
  await markThresholdsSent(db, post.id, crossed);
}

async function markThresholdsSent(db, postId, thresholds) {
  if (!thresholds || thresholds.length === 0) return;
  try {
    // Insert all crossed thresholds, ignoring duplicates (idempotent)
    var values = thresholds.map(function(_, idx) {
      return '($1, $' + (idx + 2) + ')';
    }).join(', ');
    var params = [postId].concat(thresholds);
    await db.pool.query(
      'INSERT INTO post_viral_milestones_sent (post_id, threshold) VALUES ' +
      values + ' ON CONFLICT DO NOTHING',
      params
    );
  } catch (e) {
    logger.warn('[ViralTicket] failed to mark thresholds sent: ' + e.message);
  }
}

module.exports = {
  setDiscordClient: setDiscordClient,
  maybeNotifyMilestone: maybeNotifyMilestone,
  THRESHOLDS: THRESHOLDS,
};
