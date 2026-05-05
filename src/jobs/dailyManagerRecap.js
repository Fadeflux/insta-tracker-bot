// Daily manager recap
// ──────────────────
// Posts a daily summary in each platform's #recap-quotidien channel at 23h59
// Bénin time (so managers can see the full day's results before going to bed).
//
// The recap covers the day that's ending (00h00..23h59 Bénin time on that
// same calendar day). Each platform has its own recap, sent to its own
// configured channel via CHANNEL_RECAP_QUOTIDIEN_{PLATFORM}. If a platform
// doesn't have the env var set, we silently skip it.
//
// Content format (per the user's spec):
//   📊 RECAP DU <date> — <platform>
//   📈 <date> — posts / views / virals / engagement
//   🏆 Top 3 VAs by views
//   🔥 Top 3 viral reels
//   ⚠️ Things to watch (dead accounts, shadowban, viral reposts)
//   📅 Tomorrow's expected workload (sum of objectives across VAs)

var config = require('../../config');
var logger = require('../utils/logger');

// Platforms we support for the daily recap. Twitter/Threads will work too
// if the user configures their channel — we don't filter by platform here.
var SUPPORTED_PLATFORMS = ['instagram', 'twitter', 'geelark', 'threads'];

// "Viral" threshold matches the rest of the codebase
var VIRAL_VIEWS = 5000;

var discordClient = null;
function setDiscordClient(client) { discordClient = client; }

// === Compute today's date in Bénin TZ as YYYY-MM-DD ===
async function getTodayBenin(db) {
  var r = await db.pool.query("SELECT TO_CHAR(NOW() AT TIME ZONE 'Africa/Porto-Novo', 'YYYY-MM-DD') AS d");
  return r.rows[0].d;
}

// === Compute yesterday's date in Bénin TZ ===
async function getYesterdayBenin(db) {
  var r = await db.pool.query(
    "SELECT TO_CHAR(NOW() AT TIME ZONE 'Africa/Porto-Novo' - INTERVAL '1 day', 'YYYY-MM-DD') AS d"
  );
  return r.rows[0].d;
}

// === Aggregate stats for the recap ===
// Returns:
//   {
//     date, platform,
//     posts_count, total_views, viral_count, avg_engagement,
//     prev_total_views,        // for delta computation
//     top_vas: [{va_name, total_views, posts}],
//     top_virals: [{account_username, va_name, views, url}],
//   }
async function computeRecapStats(db, date, prevDate, platform) {
  // Posts published on the date (Bénin TZ), with their latest snapshot stats
  var sql =
    "SELECT p.id, p.va_name, p.va_discord_id, p.account_username, p.url, " +
    "       COALESCE(latest.views, 0) AS views, " +
    "       COALESCE(latest.likes, 0) AS likes, " +
    "       COALESCE(latest.comments, 0) AS comments, " +
    "       COALESCE(latest.shares, 0) AS shares " +
    "FROM posts p " +
    "LEFT JOIN LATERAL (" +
    "  SELECT views, likes, comments, shares FROM snapshots s " +
    "  WHERE s.post_id = p.id AND COALESCE(s.error, '') <> 'coaching_sent' " +
    "  ORDER BY s.scraped_at DESC LIMIT 1" +
    ") latest ON true " +
    "WHERE p.deleted_at IS NULL " +
    "  AND p.platform = $1 " +
    "  AND (p.created_at AT TIME ZONE 'Africa/Porto-Novo')::date = $2";
  var todayRows = (await db.pool.query(sql, [platform, date])).rows;
  var prevRows = (await db.pool.query(sql, [platform, prevDate])).rows;

  // Aggregate today
  var totalViews = 0, viralCount = 0;
  var likesSum = 0, commentsSum = 0;
  var byVa = {};
  todayRows.forEach(function(p) {
    var views = Number(p.views || 0);
    totalViews += views;
    likesSum += Number(p.likes || 0);
    commentsSum += Number(p.comments || 0);
    if (views >= VIRAL_VIEWS) viralCount++;
    if (p.va_discord_id) {
      if (!byVa[p.va_discord_id]) {
        byVa[p.va_discord_id] = { va_name: p.va_name, total_views: 0, posts: 0 };
      }
      byVa[p.va_discord_id].total_views += views;
      byVa[p.va_discord_id].posts += 1;
    }
  });
  // Aggregate previous day (just total views for the delta)
  var prevTotalViews = prevRows.reduce(function(acc, p) { return acc + Number(p.views || 0); }, 0);

  // Top 3 VAs
  var topVas = Object.values(byVa).sort(function(a, b) {
    return b.total_views - a.total_views;
  }).slice(0, 3);

  // Top 3 viral reels (highest views)
  var topVirals = todayRows.filter(function(p) {
    return Number(p.views || 0) >= VIRAL_VIEWS;
  }).sort(function(a, b) {
    return Number(b.views || 0) - Number(a.views || 0);
  }).slice(0, 3);

  // Engagement = (likes + comments) / views, averaged across posts with views
  var engagementSum = 0, engagementCount = 0;
  todayRows.forEach(function(p) {
    var v = Number(p.views || 0);
    if (v > 0) {
      engagementSum += (Number(p.likes || 0) + Number(p.comments || 0)) / v;
      engagementCount++;
    }
  });
  var avgEngagement = engagementCount > 0 ? engagementSum / engagementCount : 0;

  return {
    date: date,
    platform: platform,
    posts_count: todayRows.length,
    total_views: totalViews,
    viral_count: viralCount,
    avg_engagement: avgEngagement,
    prev_total_views: prevTotalViews,
    top_vas: topVas,
    top_virals: topVirals,
  };
}

// === Aggregate "things to watch" for tomorrow's heads-up ===
// We surface the action items the manager needs to act on tomorrow morning:
//   - Dead accounts (state from accountDayState, on platforms with rules)
//   - Currently shadowbanned accounts (in rest)
//   - Viral reels that need to be reposted in the next day
async function computeAlerts(db, platform) {
  var alerts = { deadAccounts: 0, shadowbanAccounts: 0, viralRepostsTomorrow: 0 };

  // Only IG/Geelark have day-state rules — for other platforms, skip alerts
  // that come from accountDayState
  var hasRules = ['instagram', 'geelark'].indexOf(platform) !== -1;
  if (hasRules) {
    try {
      var accountDayState = require('./accountDayState');
      var states = await accountDayState.computeDailyState(db, { platforms: [platform] });
      states.forEach(function(s) {
        if (s.state === 'dead') alerts.deadAccounts++;
        else if (s.state === 'shadowban_rest' || s.state === 'shadowban_rampup') alerts.shadowbanAccounts++;
      });
    } catch (e) {
      logger.warn('[ManagerRecap] alerts state failed: ' + e.message);
    }
  }

  // Viral reposts tomorrow = posts that became viral on a date such that
  // (tomorrow - viral_date) is even and ≥2.
  try {
    var sql =
      "SELECT COUNT(*) AS cnt FROM post_viral_milestones_sent mile " +
      "JOIN posts p ON p.id = mile.post_id " +
      "LEFT JOIN accounts a ON a.id = p.account_id " +
      "WHERE mile.threshold = 8000 " +
      "  AND p.deleted_at IS NULL " +
      "  AND p.platform = $1 " +
      "  AND (a.id IS NULL OR a.status = 'active') " +
      "  AND (((NOW() AT TIME ZONE 'Africa/Porto-Novo')::date + INTERVAL '1 day')::date - " +
      "       (mile.sent_at AT TIME ZONE 'Africa/Porto-Novo')::date) >= 2 " +
      "  AND (((NOW() AT TIME ZONE 'Africa/Porto-Novo')::date + INTERVAL '1 day')::date - " +
      "       (mile.sent_at AT TIME ZONE 'Africa/Porto-Novo')::date) % 2 = 0";
    var r = (await db.pool.query(sql, [platform])).rows[0];
    alerts.viralRepostsTomorrow = Number(r && r.cnt || 0);
  } catch (e) {
    logger.warn('[ManagerRecap] alerts virals failed: ' + e.message);
  }

  return alerts;
}

// === Compute tomorrow's expected workload ===
// Sum of post objectives across all VAs for tomorrow. Only meaningful on
// IG/Geelark (other platforms have no per-day rule).
async function computeTomorrowWorkload(db, platform) {
  var hasRules = ['instagram', 'geelark'].indexOf(platform) !== -1;
  if (!hasRules) return null;
  try {
    var accountDayState = require('./accountDayState');
    var states = await accountDayState.computeDailyState(db, { platforms: [platform] });
    // Note: this is the state for "today", but tomorrow's objective for a J1
    // account is J2, etc. We could compute precisely, but for a heads-up at
    // 23h59 it's good enough to show the day-after-today figures (most
    // accounts will be J3+ anyway, where the objective doesn't change).
    var totalObjective = 0;
    var activeVAs = new Set();
    states.forEach(function(s) {
      // Tomorrow's objective: shadowban_rest stays 0 unless rest is over;
      // for simplicity we assume current state (close enough at 23h59).
      if (s.objective != null && s.objective > 0) {
        totalObjective += s.objective;
        activeVAs.add(s.va_discord_id);
      }
    });
    return { totalPosts: totalObjective, activeVAs: activeVAs.size };
  } catch (e) {
    logger.warn('[ManagerRecap] computeTomorrowWorkload failed: ' + e.message);
    return null;
  }
}

// === Build the recap message ===
function buildMessage(stats, alerts, workload) {
  function fm(n) { return Number(n || 0).toLocaleString('fr-FR'); }
  var medals = ['🥇', '🥈', '🥉'];
  // Format date as "5 mai 2026" in French
  function frDate(yyyymmdd) {
    var months = ['janvier','fevrier','mars','avril','mai','juin','juillet','aout','septembre','octobre','novembre','decembre'];
    var parts = yyyymmdd.split('-');
    return parseInt(parts[2], 10) + ' ' + months[parseInt(parts[1], 10) - 1] + ' ' + parts[0];
  }

  var platformLabels = {
    instagram: 'INSTAGRAM 📸',
    twitter: 'TWITTER 🐦',
    geelark: 'GEELARK 📱',
    threads: 'THREADS 🧵',
  };

  var lines = [];
  lines.push('📊 **RECAP DU ' + frDate(stats.date) + ' — ' + (platformLabels[stats.platform] || stats.platform.toUpperCase()) + '**');
  lines.push('');

  // Stats block
  lines.push('**📈 Aujourd\'hui**');
  lines.push('   • ' + fm(stats.posts_count) + ' posts publies');

  // Compute delta vs yesterday
  var deltaStr = '';
  if (stats.prev_total_views > 0) {
    var pct = Math.round(((stats.total_views - stats.prev_total_views) / stats.prev_total_views) * 100);
    var arrow = pct > 0 ? '📈' : (pct < 0 ? '📉' : '➡️');
    deltaStr = ' (' + arrow + ' ' + (pct >= 0 ? '+' : '') + pct + '% vs hier)';
  }
  lines.push('   • ' + fm(stats.total_views) + ' vues totales' + deltaStr);
  lines.push('   • ' + fm(stats.viral_count) + ' posts viraux (>' + fm(VIRAL_VIEWS) + ' vues)');
  lines.push('   • Engagement moyen : ' + (stats.avg_engagement * 100).toFixed(1) + '%');
  lines.push('');

  // Top VAs
  if (stats.top_vas && stats.top_vas.length > 0) {
    lines.push('**🏆 Top ' + stats.top_vas.length + ' VAs aujourd\'hui**');
    stats.top_vas.forEach(function(va, idx) {
      lines.push('   ' + medals[idx] + ' ' + (va.va_name || '?') + ' — ' + fm(va.total_views) + ' vues (' + va.posts + ' post' + (va.posts > 1 ? 's' : '') + ')');
    });
    lines.push('');
  }

  // Top viral reels
  if (stats.top_virals && stats.top_virals.length > 0) {
    lines.push('**🔥 Top ' + stats.top_virals.length + ' reels viraux aujourd\'hui**');
    stats.top_virals.forEach(function(p, idx) {
      var vaPart = p.va_name ? ' (' + p.va_name + ')' : '';
      lines.push('   ' + (idx + 1) + '. @' + (p.account_username || '?') + ' — ' + fm(p.views) + ' vues' + vaPart);
    });
    lines.push('');
  }

  // Alerts
  var hasAlerts = alerts && (alerts.deadAccounts > 0 || alerts.shadowbanAccounts > 0 || alerts.viralRepostsTomorrow > 0);
  if (hasAlerts) {
    lines.push('**⚠️ A surveiller**');
    if (alerts.deadAccounts > 0) {
      lines.push('   • ' + alerts.deadAccounts + ' compte' + (alerts.deadAccounts > 1 ? 's' : '') + ' mort' + (alerts.deadAccounts > 1 ? 's' : '') + ' detecte' + (alerts.deadAccounts > 1 ? 's' : ''));
    }
    if (alerts.shadowbanAccounts > 0) {
      lines.push('   • ' + alerts.shadowbanAccounts + ' compte' + (alerts.shadowbanAccounts > 1 ? 's' : '') + ' shadowban (en repos / reprise)');
    }
    if (alerts.viralRepostsTomorrow > 0) {
      var pl = alerts.viralRepostsTomorrow > 1;
      lines.push('   • ' + alerts.viralRepostsTomorrow + ' reel' + (pl ? 's' : '') + ' viral' + (pl ? 'x' : '') + ' a refaire demain');
    }
    lines.push('');
  }

  // Tomorrow's workload (only for platforms with day-rules)
  if (workload && workload.activeVAs > 0) {
    lines.push('**📅 Demain**');
    lines.push('   • ' + workload.activeVAs + ' VA' + (workload.activeVAs > 1 ? 's' : '') + ' avec objectifs actifs');
    lines.push('   • Total : ' + workload.totalPosts + ' posts attendus dans la journee');
  }

  return lines.join('\n');
}

// === Main entry: send recap for ALL configured platforms ===
async function sendDailyRecap(db) {
  if (!discordClient) {
    logger.warn('[ManagerRecap] no discord client, skipping');
    return;
  }
  var date = await getTodayBenin(db);
  var prevDate = await getYesterdayBenin(db);
  var platforms = config.getActivePlatforms();
  var sent = 0, skipped = 0, failed = 0;

  for (var i = 0; i < platforms.length; i++) {
    var pc = platforms[i];
    if (SUPPORTED_PLATFORMS.indexOf(pc.name) === -1) continue;
    var channelId = pc.channels && pc.channels.recapQuotidien;
    if (!channelId) {
      // No channel configured for this platform → skip silently. The user
      // adds CHANNEL_RECAP_QUOTIDIEN_{PLATFORM} when they want to enable it.
      continue;
    }

    try {
      // Compute everything in parallel for speed
      var statsPromise = computeRecapStats(db, date, prevDate, pc.name);
      var alertsPromise = computeAlerts(db, pc.name);
      var workloadPromise = computeTomorrowWorkload(db, pc.name);
      var stats = await statsPromise;
      var alerts = await alertsPromise;
      var workload = await workloadPromise;

      var message = buildMessage(stats, alerts, workload);

      // Fetch the channel and send
      var channel;
      try { channel = await discordClient.channels.fetch(channelId); }
      catch (e) {
        logger.warn('[ManagerRecap] cannot fetch channel ' + channelId + ' for ' + pc.name + ': ' + e.message);
        failed++;
        continue;
      }
      if (!channel) { failed++; continue; }

      try {
        await channel.send({ content: message, allowedMentions: { parse: [] } });
        logger.info('[ManagerRecap] sent for ' + pc.name + ' in #' + channel.name + ' (' + stats.posts_count + ' posts, ' + stats.viral_count + ' virals)');
        sent++;
      } catch (e) {
        logger.warn('[ManagerRecap] send failed for ' + pc.name + ': ' + e.message);
        failed++;
      }
    } catch (e) {
      logger.warn('[ManagerRecap] processing failed for ' + pc.name + ': ' + e.message);
      failed++;
    }
  }

  logger.info('[ManagerRecap] sent: ' + sent + ' / failed: ' + failed);
  return { sent: sent, failed: failed };
}

module.exports = {
  setDiscordClient: setDiscordClient,
  sendDailyRecap: sendDailyRecap,
  // Exposed for testing
  buildMessage: buildMessage,
};
