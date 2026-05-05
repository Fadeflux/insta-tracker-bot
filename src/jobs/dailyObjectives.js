// Daily objectives notifier
// ────────────────────────
// Every morning at 7h Bénin time, posts a personalised message in each VA's
// ticket channel summarising:
//   - The post objective for each of their accounts (Instagram + Geelark only)
//     — depending on the day J of the account (J1=1, J2=2, J3+=3 for normal,
//       'rest' for shadowbanned accounts, 'dead' for collapsed ones)
//   - Reels that went viral N×2 days ago (2/4/6/.../14 days), suggesting
//     they reproduce them on the same account
//   - Encouragement when there's nothing to repost.
//
// Twitter and Threads are skipped — we don't have a per-day post objective
// rule for those platforms (the user hasn't formalised one).

var config = require('../../config');
var logger = require('../utils/logger');
var accountDayState = require('./accountDayState');

var discordClient = null;
function setDiscordClient(client) { discordClient = client; }

// Platforms that have day-based posting rules
var PLATFORMS_WITH_RULES = ['instagram', 'geelark'];

// Which "viral lookback offsets" we remind the VA about. A reel is suggested
// for repost on offsets 2, 4, 6, 8, 10, 12, 14 days after it became viral.
var VIRAL_REMINDER_OFFSETS = [2, 4, 6, 8, 10, 12, 14];

// Find the VA's ticket channel by Discord username (Lola creates them)
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

// === Find reels that became viral exactly N days ago (per offset) ===
// "Viral" = views ≥ 8000 (matches our viral milestone threshold).
// We look at the post's snapshots and find when it first crossed the threshold.
// For each VA, return rows like:
//   { va_discord_id, va_name, platform, account_username, post_url,
//     went_viral_on, days_ago, peak_views }
async function getViralReelsToRepost(db) {
  // We use the post_viral_milestones_sent table as a proxy for "went viral":
  // the first time a post crossed 8000 views, the row was created with
  // sent_at = that moment. So sent_at::date in Bénin TZ ≈ when it went viral.
  // Then we filter to posts whose viral_date is exactly N days ago.
  var sql =
    "SELECT p.id AS post_id, p.url, p.platform, p.account_username, " +
    "       p.va_discord_id, p.va_name, " +
    "       (mile.sent_at AT TIME ZONE 'Africa/Porto-Novo')::date AS went_viral_on, " +
    "       (NOW() AT TIME ZONE 'Africa/Porto-Novo')::date - " +
    "         (mile.sent_at AT TIME ZONE 'Africa/Porto-Novo')::date AS days_ago, " +
    "       COALESCE(latest.views, 0) AS peak_views " +
    "FROM post_viral_milestones_sent mile " +
    "JOIN posts p ON p.id = mile.post_id " +
    "LEFT JOIN LATERAL (" +
    "  SELECT views FROM snapshots s " +
    "  WHERE s.post_id = p.id AND COALESCE(s.error, '') <> 'coaching_sent' " +
    "  ORDER BY s.scraped_at DESC LIMIT 1" +
    ") latest ON true " +
    "WHERE mile.threshold = 8000 " + // we only count the first viral milestone
    "  AND p.deleted_at IS NULL " +
    "  AND p.va_discord_id IS NOT NULL " +
    "  AND ((NOW() AT TIME ZONE 'Africa/Porto-Novo')::date - " +
    "       (mile.sent_at AT TIME ZONE 'Africa/Porto-Novo')::date) = ANY($1::int[])";
  try {
    var r = await db.pool.query(sql, [VIRAL_REMINDER_OFFSETS]);
    return r.rows;
  } catch (e) {
    logger.warn('[DailyObj] getViralReelsToRepost failed: ' + e.message);
    return [];
  }
}

// === Group accounts by VA and produce one message per VA ===
function groupByVa(accountStates, viralRows) {
  var byVa = {}; // va_discord_id → { va_name, accounts: [...], virals: [...] }
  accountStates.forEach(function(s) {
    if (!s.va_discord_id) return;
    if (!byVa[s.va_discord_id]) {
      byVa[s.va_discord_id] = { va_name: s.va_name, va_discord_id: s.va_discord_id, accounts: [], virals: [] };
    }
    byVa[s.va_discord_id].accounts.push(s);
  });
  viralRows.forEach(function(v) {
    if (!v.va_discord_id) return;
    if (!byVa[v.va_discord_id]) {
      byVa[v.va_discord_id] = { va_name: v.va_name, va_discord_id: v.va_discord_id, accounts: [], virals: [] };
    }
    byVa[v.va_discord_id].virals.push(v);
  });
  return byVa;
}

// === Build the message text for one VA ===
function buildMessage(va) {
  function fm(n) { return Number(n || 0).toLocaleString('fr-FR'); }
  var lines = [];
  lines.push('☀️ **Bonjour ! Voici tes objectifs aujourd\'hui**');
  lines.push('');

  // Group accounts by platform
  var byPlatform = {};
  va.accounts.forEach(function(a) {
    if (!byPlatform[a.platform]) byPlatform[a.platform] = [];
    byPlatform[a.platform].push(a);
  });

  // Order: Instagram first, then Geelark
  var platformOrder = ['instagram', 'geelark'];
  var platformLabels = {
    instagram: '📸 INSTAGRAM',
    geelark: '📱 GEELARK',
  };

  var hasAnyObjective = false;
  platformOrder.forEach(function(plat) {
    var accs = byPlatform[plat];
    if (!accs || accs.length === 0) return;
    lines.push('**' + platformLabels[plat] + '**');
    var totalObjective = 0;
    accs.forEach(function(a) {
      var line;
      if (a.state === 'shadowban_rest') {
        line = '   • @' + a.username + ' — 🛌 ' + a.day_label + ' → **ne poste pas**';
      } else if (a.state === 'shadowban_rampup') {
        line = '   • @' + a.username + ' — 🚀 ' + a.day_label + ' → **' + a.objective + ' post' + (a.objective > 1 ? 's' : '') + '** (reprise progressive)';
        totalObjective += a.objective;
      } else if (a.state === 'dead') {
        line = '   • @' + a.username + ' — ❌ Compte mort, **arrete de poster dessus** (' + a.reason + ')';
      } else if (a.state === 'never_posted') {
        line = '   • @' + a.username + ' — Aucun post tracke pour le moment. J1 sera le jour de ton premier lien.';
      } else {
        // normal
        line = '   • @' + a.username + ' (' + a.day_label + ') → **' + a.objective + ' post' + (a.objective > 1 ? 's' : '') + ' a faire**';
        totalObjective += a.objective;
      }
      lines.push(line);
    });
    if (totalObjective > 0) {
      lines.push('   ─────────────────────────');
      lines.push('   **Total : ' + totalObjective + ' post' + (totalObjective > 1 ? 's' : '') + ' a faire aujourd\'hui**');
      hasAnyObjective = true;
    }
    lines.push('');
  });

  // Viral repost reminders
  if (va.virals && va.virals.length > 0) {
    lines.push('🔥 **Reels viraux a refaire aujourd\'hui (sur le meme compte)**');
    va.virals.forEach(function(v) {
      var ago = v.days_ago === 1 ? '1 jour' : v.days_ago + ' jours';
      lines.push('   • @' + v.account_username + ' — ' + v.url + ' (devenu viral il y a ' + ago + ', ' + fm(v.peak_views) + ' vues)');
    });
    lines.push('');
    lines.push('💡 Pour chaque reel : mets-la dans un drive, envoie le lien au manager pour changer les metadonnees, et reposte d\'ici 1-2 jours sur le meme compte.');
  } else {
    lines.push('💪 **Continue comme ca pour faire percer tes comptes !**');
  }

  return lines.join('\n');
}

// === Main entry: send a daily objective to each VA ===
async function sendDailyObjectives(db) {
  if (!discordClient) {
    logger.warn('[DailyObj] no discord client, skipping');
    return;
  }
  // Compute states for all eligible accounts
  var states = await accountDayState.computeDailyState(db, { platforms: PLATFORMS_WITH_RULES });
  // Get viral reels to repost
  var virals = await getViralReelsToRepost(db);
  // Group by VA
  var byVa = groupByVa(states, virals);

  // We need the platform → guild mapping. A VA can have accounts on multiple
  // platforms but their ticket lives in ONE guild (the guild where they got
  // the VA role). We try Instagram guild first (most common), then fallback
  // to other guilds in order.
  var platforms = config.getActivePlatforms();
  var guildOrder = []; // ordered list of {platform, guildId} to try
  ['instagram', 'geelark', 'twitter', 'threads'].forEach(function(plat) {
    var pc = platforms.find(function(p) { return p.name === plat; });
    if (pc && pc.guildId) guildOrder.push({ platform: plat, guildId: pc.guildId });
  });

  var sent = 0, skipped = 0, failed = 0;
  var vaIds = Object.keys(byVa);
  for (var i = 0; i < vaIds.length; i++) {
    var va = byVa[vaIds[i]];
    if (va.accounts.length === 0 && va.virals.length === 0) { skipped++; continue; }

    var content = buildMessage(va);
    // Find the channel by trying each guild in order.
    var channel = null;
    for (var g = 0; g < guildOrder.length; g++) {
      try {
        var guild = await discordClient.guilds.fetch(guildOrder[g].guildId);
        // First, try by va_name (which is what Lola uses)
        channel = await findVaTicketChannel(guild, va.va_name);
        if (channel) break;
        // Fallback: fetch the member's username and try with that
        if (va.va_discord_id) {
          try {
            var member = await guild.members.fetch(va.va_discord_id);
            if (member) {
              channel = await findVaTicketChannel(guild, member.user.username);
              if (channel) break;
            }
          } catch (e) { /* member not in this guild */ }
        }
      } catch (e) { /* try next guild */ }
    }
    if (!channel) {
      logger.info('[DailyObj] no ticket for VA ' + va.va_name + ' (' + va.va_discord_id + ')');
      skipped++;
      continue;
    }

    var vaMention = '<@' + va.va_discord_id + '>';
    var fullContent = vaMention + '\n\n' + content;

    try {
      await channel.send({ content: fullContent, allowedMentions: { parse: ['users'] } });
      logger.info('[DailyObj] sent to #' + channel.name + ' (' + va.accounts.length + ' accounts, ' + va.virals.length + ' virals)');
      sent++;
    } catch (e) {
      logger.warn('[DailyObj] send failed for #' + channel.name + ': ' + e.message);
      failed++;
    }

    // Small delay between sends to avoid Discord rate limits (50 msg/s globally
    // but per-channel cooldown applies). 200ms between messages = max 5/s.
    await new Promise(function(r) { setTimeout(r, 200); });
  }

  logger.info('[DailyObj] daily objectives sent: ' + sent + ' / skipped: ' + skipped + ' / failed: ' + failed);
  return { sent: sent, skipped: skipped, failed: failed, totalVas: vaIds.length };
}

module.exports = {
  setDiscordClient: setDiscordClient,
  sendDailyObjectives: sendDailyObjectives,
  // Exposed for testing
  buildMessage: buildMessage,
};
