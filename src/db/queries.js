const { pool } = require('./init');
const logger = require('../utils/logger');

// Performance thresholds (configurable via env)
var VIRAL_VIEWS = parseInt(process.env.VIRAL_VIEWS || '5000');
var BON_VIEWS = parseInt(process.env.BON_VIEWS || '1000');
var MOYEN_VIEWS = parseInt(process.env.MOYEN_VIEWS || '300');

// =============================================
// ===== PERMISSIONS =====
// =============================================

async function setUserPermission(discordId, platform, role, grantedBy) {
  var sql = "INSERT INTO user_permissions (discord_id, platform, role, granted_by) VALUES ($1, $2, $3, $4) ON CONFLICT (discord_id, platform) DO UPDATE SET role = $3, granted_by = $4 RETURNING *";
  var result = await pool.query(sql, [discordId, platform, role, grantedBy || null]);
  return result.rows[0];
}

async function removeUserPermission(discordId, platform) {
  var sql = "DELETE FROM user_permissions WHERE discord_id = $1 AND platform = $2 RETURNING *";
  var result = await pool.query(sql, [discordId, platform]);
  return result.rows[0] || null;
}

async function getUserPermissions(discordId) {
  var result = await pool.query('SELECT * FROM user_permissions WHERE discord_id = $1', [discordId]);
  return result.rows;
}

async function getUserPlatforms(discordId) {
  var perms = await getUserPermissions(discordId);
  // If user has 'all' platform, return both
  var hasAll = perms.some(function(p) { return p.platform === 'all'; });
  if (hasAll) return ['instagram', 'twitter', 'geelark'];
  return perms.map(function(p) { return p.platform; });
}

async function getUserRole(discordId, platform) {
  // Check for 'all' platform first (admin)
  var allResult = await pool.query("SELECT role FROM user_permissions WHERE discord_id = $1 AND platform = 'all'", [discordId]);
  if (allResult.rows.length > 0) return allResult.rows[0].role;
  // Then check specific platform
  var result = await pool.query('SELECT role FROM user_permissions WHERE discord_id = $1 AND platform = $2', [discordId, platform]);
  return result.rows.length > 0 ? result.rows[0].role : null;
}

async function canAccessPlatform(discordId, platform) {
  var perms = await getUserPermissions(discordId);
  return perms.some(function(p) { return p.platform === 'all' || p.platform === platform; });
}

async function getAllPermissions() {
  var result = await pool.query('SELECT * FROM user_permissions ORDER BY platform, role, discord_id');
  return result.rows;
}

// =============================================
// ===== POSTS (with platform) =====
// =============================================

async function insertPost({ igPostId, url, vaDiscordId, vaName, caption, platform, guildId, accountId, accountUsername }) {
  platform = platform || 'instagram';
  var postType = 'post';
  if (platform === 'instagram') {
    postType = url.includes('/reel/') ? 'reel' : 'post';
  } else if (platform === 'twitter') {
    postType = 'tweet';
  }
  var sql = "INSERT INTO posts (ig_post_id, url, va_discord_id, va_name, post_type, caption, platform, guild_id, account_id, account_username, tracking_end) VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, (DATE_TRUNC('day', NOW() AT TIME ZONE 'Europe/Paris') + INTERVAL '23 hours 59 minutes') AT TIME ZONE 'Europe/Paris') ON CONFLICT (ig_post_id) DO NOTHING RETURNING *";
  var result = await pool.query(sql, [igPostId, url, vaDiscordId, vaName, postType, caption || null, platform, guildId || null, accountId || null, accountUsername || null]);
  return result.rows[0] || null;
}

// Update account on an existing post (used when the scraper resolves the
// IG username asynchronously after the post was already inserted).
async function updatePostAccount(postId, accountId, accountUsername) {
  var sql = 'UPDATE posts SET account_id = COALESCE(account_id, $1), account_username = COALESCE(account_username, $2) WHERE id = $3';
  await pool.query(sql, [accountId || null, accountUsername || null, postId]);
}

async function getPost(id) {
  var result = await pool.query('SELECT * FROM posts WHERE id = $1', [id]);
  return result.rows[0];
}

async function getPostByIgId(igPostId) {
  var result = await pool.query('SELECT * FROM posts WHERE ig_post_id = $1', [igPostId]);
  return result.rows[0];
}

async function getActivePosts(platform) {
  if (platform) {
    var result = await pool.query("SELECT * FROM posts WHERE status = 'active' AND platform = $1 ORDER BY created_at ASC", [platform]);
    return result.rows;
  }
  var result2 = await pool.query("SELECT * FROM posts WHERE status = 'active' ORDER BY created_at ASC");
  return result2.rows;
}

async function endTracking(postId) {
  await pool.query("UPDATE posts SET status = 'ended' WHERE id = $1", [postId]);
}

async function setPostError(postId, error) {
  await pool.query("UPDATE posts SET status = 'error' WHERE id = $1", [postId]);
  logger.warn('Post ' + postId + ' marked as error: ' + error);
}

async function setManagerMsgId(postId, msgId) {
  await pool.query('UPDATE posts SET manager_msg_id = $1 WHERE id = $2', [msgId, postId]);
}

async function updatePostPerformance(postId, views) {
  var perf = 'flop';
  if (views >= VIRAL_VIEWS) perf = 'viral';
  else if (views >= BON_VIEWS) perf = 'bon';
  else if (views >= MOYEN_VIEWS) perf = 'moyen';
  await pool.query('UPDATE posts SET performance = $1 WHERE id = $2', [perf, postId]);
  return perf;
}

// =============================================
// ===== SNAPSHOTS =====
// =============================================

async function insertSnapshot(postId, stats) {
  var sql = 'INSERT INTO snapshots (post_id, views, likes, comments, shares, retweets, quote_tweets, bookmarks, error) VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9) RETURNING *';
  var result = await pool.query(sql, [
    postId,
    stats.views || 0,
    stats.likes || 0,
    stats.comments || 0,
    stats.shares || 0,
    stats.retweets || 0,
    stats.quote_tweets || 0,
    stats.bookmarks || 0,
    stats.error || null,
  ]);

  if (stats.views > 0) {
    await updatePostPerformance(postId, stats.views);
  }

  // If scraper extracted the real publication time AND we don't have it yet,
  // store it AND compute the delay (in minutes) between created_at (link sent) and posted_at (real publish).
  if (stats.postedAt) {
    try {
      await pool.query(
        "UPDATE posts SET posted_at = COALESCE(posted_at, $1::timestamptz), " +
        "link_delay_minutes = COALESCE(link_delay_minutes, GREATEST(0, EXTRACT(EPOCH FROM (created_at - $1::timestamptz)) / 60)::int) " +
        "WHERE id = $2",
        [stats.postedAt, postId]
      );
    } catch (e) {
      console.log('[posted_at] failed to update post ' + postId + ': ' + e.message);
    }
  }

  return result.rows[0];
}

// Mark a post as "late alert sent" so the cron doesn't DM the VA twice.
async function markLateAlertSent(postId) {
  await pool.query('UPDATE posts SET late_alert_sent = TRUE WHERE id = $1', [postId]);
}

// Get posts where:
//  - posted_at is known (we managed to scrape the real time)
//  - link_delay_minutes >= threshold (link was sent way after the post)
//  - late_alert_sent is FALSE (we haven't notified yet)
//  - platform is filtered (only IG today)
async function getLateLinkPosts(thresholdMinutes, platform) {
  var sql =
    "SELECT p.id, p.url, p.va_discord_id, p.va_name, p.account_username, p.posted_at, p.created_at, p.link_delay_minutes, p.platform " +
    "FROM posts p " +
    "WHERE p.posted_at IS NOT NULL " +
    "  AND p.link_delay_minutes >= $1 " +
    "  AND COALESCE(p.late_alert_sent, FALSE) = FALSE " +
    "  AND p.platform = $2 " +
    "  AND p.created_at >= NOW() - INTERVAL '24 hours'";
  var result = await pool.query(sql, [thresholdMinutes, platform]);
  return result.rows;
}

// Idempotency check for slot reminders: returns true if reminder was already sent today.
async function wasReminderSent(reminderKey) {
  var result = await pool.query('SELECT 1 FROM slot_reminders_log WHERE reminder_key = $1', [reminderKey]);
  return result.rowCount > 0;
}

async function markReminderSent(reminderKey) {
  await pool.query(
    'INSERT INTO slot_reminders_log (reminder_key) VALUES ($1) ON CONFLICT (reminder_key) DO NOTHING',
    [reminderKey]
  );
}

// Count how many posts a VA submitted in a given time window.
// Used by the "late slot" cron to identify which VAs missed their slot.
async function countPostsBetween(vaDiscordId, platform, fromIso, toIso) {
  var result = await pool.query(
    'SELECT COUNT(*)::int AS n FROM posts WHERE deleted_at IS NULL AND va_discord_id = $1 AND platform = $2 AND created_at >= $3 AND created_at < $4',
    [vaDiscordId, platform, fromIso, toIso]
  );
  return result.rows[0].n;
}

async function getLatestSnapshot(postId) {
  var result = await pool.query('SELECT * FROM snapshots WHERE post_id = $1 ORDER BY scraped_at DESC LIMIT 1', [postId]);
  return result.rows[0];
}

async function getSnapshotHistory(postId) {
  var result = await pool.query('SELECT * FROM snapshots WHERE post_id = $1 ORDER BY scraped_at ASC', [postId]);
  return result.rows;
}

async function getSnapshotAtHour(postId, hours) {
  var sql = "SELECT s.* FROM snapshots s JOIN posts p ON p.id = s.post_id WHERE s.post_id = $1 AND s.scraped_at <= p.created_at + ($2 || ' hours')::interval ORDER BY s.scraped_at DESC LIMIT 1";
  var result = await pool.query(sql, [postId, hours]);
  return result.rows[0] || null;
}

async function getPostMilestones(postId) {
  var m1 = await getSnapshotAtHour(postId, 1);
  var m3 = await getSnapshotAtHour(postId, 3);
  var m6 = await getSnapshotAtHour(postId, 6);
  return {
    h1: m1 ? m1.views : null,
    h3: m3 ? m3.views : null,
    h6: m6 ? m6.views : null,
  };
}

// =============================================
// ===== DAILY SUMMARIES (with platform) =====
// =============================================

async function computeDailySummary(date, platform) {
  if (platform) {
    var sql = "INSERT INTO daily_summaries (va_discord_id, va_name, date, platform, post_count, total_views, total_likes, total_comments, total_shares) SELECT p.va_discord_id, p.va_name, $1::date, $3, COUNT(DISTINCT p.id), COALESCE(SUM(latest.views), 0), COALESCE(SUM(latest.likes), 0), COALESCE(SUM(latest.comments), 0), COALESCE(SUM(latest.shares), 0) FROM posts p LEFT JOIN LATERAL (SELECT views, likes, comments, shares FROM snapshots s WHERE s.post_id = p.id ORDER BY s.scraped_at DESC LIMIT 1) latest ON true WHERE p.deleted_at IS NULL AND p.created_at::date = $1::date AND p.platform = $2 GROUP BY p.va_discord_id, p.va_name ON CONFLICT (va_discord_id, date, platform) DO UPDATE SET post_count = EXCLUDED.post_count, total_views = EXCLUDED.total_views, total_likes = EXCLUDED.total_likes, total_comments = EXCLUDED.total_comments, total_shares = EXCLUDED.total_shares RETURNING *";
    var result = await pool.query(sql, [date, platform, platform]);
    return result.rows;
  }
  // Compute for all platforms separately
  var igRows = await computeDailySummary(date, 'instagram');
  var twRows = await computeDailySummary(date, 'twitter');
  return igRows.concat(twRows);
}

async function getDailySummaries(date, platform) {
  if (platform) {
    var result = await pool.query('SELECT * FROM daily_summaries WHERE date = $1 AND platform = $2 ORDER BY total_views DESC', [date, platform]);
    return result.rows;
  }
  var result2 = await pool.query('SELECT * FROM daily_summaries WHERE date = $1 ORDER BY total_views DESC', [date]);
  return result2.rows;
}

// Get summaries aggregated over a date range (or all time if fromDate is null).
// Returns the same shape as getDailySummaries (per-VA totals) but summed over the range.
// Used by the dashboard's "Depuis toujours" / period filters.
async function getRangeSummaries(fromDate, toDate, platform) {
  // Build WHERE clause
  var conditions = ['p.deleted_at IS NULL', 'p.va_discord_id IS NOT NULL'];
  var params = [];
  if (fromDate) {
    params.push(fromDate);
    conditions.push("p.created_at::date >= $" + params.length);
  }
  if (toDate) {
    params.push(toDate);
    conditions.push("p.created_at::date <= $" + params.length);
  }
  if (platform) {
    params.push(platform);
    conditions.push("p.platform = $" + params.length);
  }
  var whereClause = "WHERE " + conditions.join(' AND ');

  var sql =
    "SELECT p.va_discord_id, " +
    "       MAX(p.va_name) AS va_name, " +
    "       COUNT(DISTINCT p.id)::int AS post_count, " +
    "       COALESCE(SUM(latest.views), 0)::bigint AS total_views, " +
    "       COALESCE(SUM(latest.likes), 0)::bigint AS total_likes, " +
    "       COALESCE(SUM(latest.comments), 0)::bigint AS total_comments, " +
    "       COALESCE(SUM(latest.shares), 0)::bigint AS total_shares " +
    "FROM posts p " +
    "LEFT JOIN LATERAL ( " +
    "  SELECT views, likes, comments, shares FROM snapshots s " +
    "  WHERE s.post_id = p.id ORDER BY s.scraped_at DESC LIMIT 1 " +
    ") latest ON true " +
    whereClause + " " +
    "GROUP BY p.va_discord_id " +
    "ORDER BY total_views DESC";
  var result = await pool.query(sql, params);
  return result.rows;
}

async function getVaDailyStats(vaDiscordId, date, platform) {
  if (platform) {
    var result = await pool.query('SELECT * FROM daily_summaries WHERE va_discord_id = $1 AND date = $2 AND platform = $3', [vaDiscordId, date, platform]);
    return result.rows[0];
  }
  var result2 = await pool.query('SELECT * FROM daily_summaries WHERE va_discord_id = $1 AND date = $2', [vaDiscordId, date]);
  return result2.rows[0];
}

async function getVaPostsToday(vaDiscordId, date, platform) {
  if (platform) {
    var result = await pool.query('SELECT * FROM posts WHERE va_discord_id = $1 AND created_at::date = $2 AND platform = $3 ORDER BY created_at ASC', [vaDiscordId, date, platform]);
    return result.rows;
  }
  var result2 = await pool.query('SELECT * FROM posts WHERE va_discord_id = $1 AND created_at::date = $2 ORDER BY created_at ASC', [vaDiscordId, date]);
  return result2.rows;
}

async function getLeaderboard(date, platform) {
  if (platform) {
    var result = await pool.query('SELECT * FROM daily_summaries WHERE date = $1 AND platform = $2 ORDER BY total_views DESC', [date, platform]);
    return result.rows;
  }
  var result2 = await pool.query('SELECT * FROM daily_summaries WHERE date = $1 ORDER BY total_views DESC', [date]);
  return result2.rows;
}

async function endExpiredPosts() {
  var result = await pool.query("UPDATE posts SET status = 'ended' WHERE status = 'active' AND tracking_end < NOW()");
  if (result.rowCount > 0) logger.info('Ended tracking for ' + result.rowCount + ' expired posts');
  return result.rowCount;
}

async function getPostsForExport(date, platform) {
  var sql, params;
  if (platform) {
    sql = "SELECT p.va_name, p.url, p.ig_post_id, p.platform, p.created_at, p.status, p.performance, s.views, s.likes, s.comments, s.shares, s.retweets, s.quote_tweets, s.bookmarks, s.scraped_at FROM posts p JOIN snapshots s ON s.post_id = p.id WHERE p.deleted_at IS NULL AND p.created_at::date = $1 AND p.platform = $2 ORDER BY p.va_name, p.created_at, s.scraped_at";
    params = [date, platform];
  } else {
    sql = "SELECT p.va_name, p.url, p.ig_post_id, p.platform, p.created_at, p.status, p.performance, s.views, s.likes, s.comments, s.shares, s.retweets, s.quote_tweets, s.bookmarks, s.scraped_at FROM posts p JOIN snapshots s ON s.post_id = p.id WHERE p.deleted_at IS NULL AND p.created_at::date = $1 ORDER BY p.platform, p.va_name, p.created_at, s.scraped_at";
    params = [date];
  }
  var result = await pool.query(sql, params);
  return result.rows;
}

async function getTopPostsWithPerformance(date, platform) {
  var whereClause = platform
    ? "WHERE p.deleted_at IS NULL AND p.created_at::date = $1 AND p.platform = $2"
    : "WHERE p.created_at::date = $1";
  var params = platform ? [date, platform] : [date];
  var sql = "SELECT p.id, p.ig_post_id, p.url, p.va_name, p.va_discord_id, p.created_at, p.performance, p.post_type, p.platform, s.views, s.likes, s.comments, s.shares, s.retweets, s.quote_tweets, s.bookmarks FROM posts p LEFT JOIN LATERAL (SELECT views, likes, comments, shares, retweets, quote_tweets, bookmarks FROM snapshots sn WHERE sn.post_id = p.id ORDER BY sn.scraped_at DESC LIMIT 1) s ON true " + whereClause + " ORDER BY COALESCE(s.views, 0) DESC LIMIT 30";
  var result = await pool.query(sql, params);
  return result.rows;
}

async function getVaPerformanceHistory(vaDiscordId, days, platform) {
  if (platform) {
    var sql = "SELECT date, platform, post_count, total_views, total_likes, total_comments, total_shares FROM daily_summaries WHERE va_discord_id = $1 AND date >= CURRENT_DATE - $2::integer AND platform = $3 ORDER BY date ASC";
    var result = await pool.query(sql, [vaDiscordId, days, platform]);
    return result.rows;
  }
  var sql2 = "SELECT date, platform, post_count, total_views, total_likes, total_comments, total_shares FROM daily_summaries WHERE va_discord_id = $1 AND date >= CURRENT_DATE - $2::integer ORDER BY date ASC";
  var result2 = await pool.query(sql2, [vaDiscordId, days]);
  return result2.rows;
}

async function checkViralPosts(platform) {
  var whereClause = platform
    ? "WHERE p.status = 'active' AND p.platform = $1"
    : "WHERE p.status = 'active'";
  var params = platform ? [platform] : [];
  var sql = "SELECT p.id, p.ig_post_id, p.url, p.va_name, p.va_discord_id, p.platform, s.views, s.likes, s.comments, s.shares FROM posts p LEFT JOIN LATERAL (SELECT views, likes, comments, shares FROM snapshots sn WHERE sn.post_id = p.id ORDER BY sn.scraped_at DESC LIMIT 1) s ON true " + whereClause + " AND COALESCE(s.views, 0) >= " + VIRAL_VIEWS;
  var result = await pool.query(sql, params);
  return result.rows;
}

// Return posts that have crossed a given threshold AND have not yet been
// notified for that specific threshold. Used by the viral notification cron.
// Only posts from the last 48h are considered to avoid spamming old posts
// (an old post could re-trigger if we change the threshold later).
async function getNewPostsReachingThreshold(threshold, platform) {
  var whereClause = platform
    ? "WHERE p.platform = $1 AND p.created_at >= NOW() - INTERVAL '48 hours'"
    : "WHERE p.created_at >= NOW() - INTERVAL '48 hours'";
  var params = platform ? [platform] : [];
  var sql =
    "SELECT p.id, p.ig_post_id, p.url, p.va_name, p.va_discord_id, p.platform, p.account_username, p.caption, " +
    "       s.views, s.likes, s.comments, s.shares " +
    "FROM posts p " +
    "LEFT JOIN LATERAL (" +
    "  SELECT views, likes, comments, shares FROM snapshots sn " +
    "  WHERE sn.post_id = p.id AND COALESCE(sn.error, '') <> 'coaching_sent' " +
    "  ORDER BY sn.scraped_at DESC LIMIT 1" +
    ") s ON true " +
    whereClause + " " +
    "AND COALESCE(s.views, 0) >= " + parseInt(threshold, 10) + " " +
    "AND NOT EXISTS (SELECT 1 FROM viral_notifications vn WHERE vn.post_id = p.id AND vn.threshold = " + parseInt(threshold, 10) + ") " +
    "ORDER BY s.views DESC";
  var result = await pool.query(sql, params);
  return result.rows;
}

// Record that we've notified about a post crossing a threshold.
// Uses UNIQUE constraint to prevent double-notifications even on race conditions.
async function recordViralNotification(postId, vaDiscordId, threshold, viewsAtNotif) {
  var sql = "INSERT INTO viral_notifications (post_id, va_discord_id, threshold, views_at_notif) " +
    "VALUES ($1, $2, $3, $4) ON CONFLICT (post_id, threshold) DO NOTHING RETURNING *";
  var result = await pool.query(sql, [postId, vaDiscordId, threshold, viewsAtNotif]);
  return result.rows[0] || null;
}

// Record the result of a DM attempt to a VA (success or failure).
// Updates the va_dm_status row for that VA — no error if row doesn't exist.
async function recordDmAttempt(discordId, vaName, success, failReason) {
  if (!discordId) return;
  try {
    if (success) {
      await pool.query(
        "INSERT INTO va_dm_status (discord_id, va_name, last_ok_at, total_ok, updated_at) " +
        "VALUES ($1, $2, NOW(), 1, NOW()) " +
        "ON CONFLICT (discord_id) DO UPDATE SET " +
        "  va_name = COALESCE(EXCLUDED.va_name, va_dm_status.va_name), " +
        "  last_ok_at = NOW(), total_ok = va_dm_status.total_ok + 1, updated_at = NOW()",
        [discordId, vaName || null]
      );
    } else {
      await pool.query(
        "INSERT INTO va_dm_status (discord_id, va_name, last_fail_at, last_fail_reason, total_fail, updated_at) " +
        "VALUES ($1, $2, NOW(), $3, 1, NOW()) " +
        "ON CONFLICT (discord_id) DO UPDATE SET " +
        "  va_name = COALESCE(EXCLUDED.va_name, va_dm_status.va_name), " +
        "  last_fail_at = NOW(), last_fail_reason = $3, " +
        "  total_fail = va_dm_status.total_fail + 1, updated_at = NOW()",
        [discordId, vaName || null, failReason ? failReason.substring(0, 500) : 'unknown']
      );
    }
  } catch (e) {
    // Don't throw — DM tracking failures shouldn't break the calling code
    logger.warn('Failed to record DM attempt: ' + e.message);
  }
}

// Return all DM status rows for admin dashboard display.
async function getAllDmStatus() {
  var result = await pool.query(
    "SELECT discord_id, va_name, last_ok_at, last_fail_at, last_fail_reason, total_ok, total_fail, updated_at " +
    "FROM va_dm_status ORDER BY updated_at DESC"
  );
  return result.rows;
}

// Return only VAs whose most recent DM attempt failed (i.e. currently blocked).
// Used by the daily digest cron to alert admins in #alerts.
async function getBlockedDmVAs() {
  var result = await pool.query(
    "SELECT discord_id, va_name, last_ok_at, last_fail_at, last_fail_reason, total_ok, total_fail " +
    "FROM va_dm_status " +
    "WHERE last_fail_at IS NOT NULL " +
    "  AND (last_ok_at IS NULL OR last_fail_at > last_ok_at) " +
    "ORDER BY last_fail_at DESC"
  );
  return result.rows;
}

// Shadowban-specific detection. Unlike a plain "views dropped" alert, this
// cross-references two signals to distinguish:
//   - SHADOWBAN: reach collapsed BUT engagement rate stayed stable
//     (the few people who still see it still engage normally) → platform
//     issue, not a content issue.
//   - BAD CONTENT: both reach AND engagement rate dropped together → the
//     recent content is less good, not a shadowban.
//   - MIXED: some drop in both but less severe → could be either, watch.
//
// Returns one row per account with:
//   - views_ratio: recent_avg_views / baseline_avg_views (0-1, less = worse)
//   - engagement_ratio: recent_engagement_rate / baseline_engagement_rate
//   - shadowban_score: 0-100 (higher = more likely shadowban)
//   - diagnosis: 'shadowban' | 'content' | 'mixed' | 'ok'
async function getShadowbanCandidates(platform) {
  var platformFilter = platform ? "AND a.platform = '" + platform + "'" : "";

  var sql =
    "WITH recent AS (" +
    "  SELECT p.account_id, " +
    "         AVG(COALESCE(s.views, 0))::numeric AS avg_views, " +
    "         AVG(" +
    "           CASE WHEN COALESCE(s.views, 0) > 0 " +
    "                THEN (COALESCE(s.likes, 0) + COALESCE(s.comments, 0))::numeric / s.views " +
    "                ELSE 0 END" +
    "         )::numeric AS avg_engagement, " +
    "         COUNT(*)::int AS n_posts " +
    "  FROM posts p " +
    "  LEFT JOIN LATERAL (" +
    "    SELECT views, likes, comments FROM snapshots sn " +
    "    WHERE sn.post_id = p.id AND COALESCE(sn.error, '') <> 'coaching_sent' " +
    "    ORDER BY sn.scraped_at DESC LIMIT 1" +
    "  ) s ON true " +
    "  WHERE p.account_id IS NOT NULL " +
    "    AND p.created_at >= NOW() - INTERVAL '3 days' " +
    "  GROUP BY p.account_id " +
    "  HAVING COUNT(*) >= 1 AND AVG(COALESCE(s.views, 0)) > 0" +
    "), " +
    "baseline AS (" +
    "  SELECT p.account_id, " +
    "         AVG(COALESCE(s.views, 0))::numeric AS avg_views, " +
    "         AVG(" +
    "           CASE WHEN COALESCE(s.views, 0) > 0 " +
    "                THEN (COALESCE(s.likes, 0) + COALESCE(s.comments, 0))::numeric / s.views " +
    "                ELSE 0 END" +
    "         )::numeric AS avg_engagement, " +
    "         COUNT(*)::int AS n_posts " +
    "  FROM posts p " +
    "  LEFT JOIN LATERAL (" +
    "    SELECT views, likes, comments FROM snapshots sn " +
    "    WHERE sn.post_id = p.id AND COALESCE(sn.error, '') <> 'coaching_sent' " +
    "    ORDER BY sn.scraped_at DESC LIMIT 1" +
    "  ) s ON true " +
    "  WHERE p.account_id IS NOT NULL " +
    "    AND p.created_at >= NOW() - INTERVAL '10 days' " +
    "    AND p.created_at < NOW() - INTERVAL '3 days' " +
    "  GROUP BY p.account_id " +
    "  HAVING COUNT(*) >= 3 AND AVG(COALESCE(s.views, 0)) > 0" +
    ") " +
    "SELECT a.id, a.username, a.platform, a.va_discord_id, a.va_name, " +
    "       recent.avg_views AS recent_avg_views, " +
    "       baseline.avg_views AS baseline_avg_views, " +
    "       recent.avg_engagement AS recent_engagement, " +
    "       baseline.avg_engagement AS baseline_engagement, " +
    "       recent.n_posts AS recent_posts, " +
    "       baseline.n_posts AS baseline_posts, " +
    "       (recent.avg_views / baseline.avg_views)::numeric AS views_ratio, " +
    "       CASE WHEN baseline.avg_engagement > 0 " +
    "            THEN (recent.avg_engagement / baseline.avg_engagement)::numeric " +
    "            ELSE 1 END AS engagement_ratio " +
    "FROM accounts a " +
    "JOIN recent ON recent.account_id = a.id " +
    "JOIN baseline ON baseline.account_id = a.id " +
    "WHERE a.status = 'active' " + platformFilter + " " +
    "  AND (recent.avg_views / baseline.avg_views) < 0.7 " + // at least 30% reach drop to be a candidate
    "ORDER BY (recent.avg_views / baseline.avg_views) ASC";

  var result = await pool.query(sql);
  return result.rows;
}

// Compute a shadowban likelihood score (0-100) and diagnosis for a candidate row.
// This is a pure function — easy to reason about and easy to tweak thresholds.
//
// Reasoning:
//   - Big views drop + stable engagement   → STRONG shadowban signal
//   - Big views drop + big engagement drop → BAD CONTENT signal
//   - Small drops on both                   → MIXED (watch)
function computeShadowbanScore(row) {
  var vr = Number(row.views_ratio) || 0;         // e.g. 0.30 = views at 30% of normal
  var er = Number(row.engagement_ratio) || 1;     // e.g. 1.05 = engagement slightly up
  var viewsDropPct = 1 - vr;                       // 0.70 = 70% drop
  var engagementDropPct = Math.max(0, 1 - er);     // 0 if engagement UP or stable

  // Shadowban signature: big views drop, small/no engagement drop
  // Score formula: weighted by how "pure" the reach-only drop is
  var gap = viewsDropPct - engagementDropPct;
  // gap > 0 means reach dropped more than engagement → shadowban-like
  // gap <= 0 means engagement dropped as much or more → content issue

  var score = 0;
  var diagnosis = 'ok';

  if (viewsDropPct < 0.3) {
    diagnosis = 'ok';
    score = 0;
  } else if (gap >= 0.3 && er >= 0.85) {
    // Classic shadowban pattern: reach collapsed, engagement rate stable
    diagnosis = 'shadowban';
    score = Math.min(100, Math.round(viewsDropPct * 100 + gap * 30));
  } else if (gap < 0.1 && engagementDropPct >= 0.3) {
    // Both dropped together → content problem
    diagnosis = 'content';
    score = Math.round(viewsDropPct * 40); // lower score (not really shadowban)
  } else {
    // Somewhere in between
    diagnosis = 'mixed';
    score = Math.round(viewsDropPct * 60 + gap * 20);
  }

  return {
    shadowban_score: Math.max(0, Math.min(100, score)),
    diagnosis: diagnosis,
    views_drop_pct: Math.round(viewsDropPct * 100),
    engagement_drop_pct: Math.round(engagementDropPct * 100),
  };
}


// Groups by va_discord_id, optionally filtered by platform.
// For each VA: posts today, posts last 7 days, last post timestamp.
// NOTE: Uses va_discord_id from posts, which is populated when the VA posts.
// VAs who have NEVER posted won't appear — but the dashboard endpoint
// cross-references with the full Discord member list so newcomers still show.
async function getVaActivityStatus(platform) {
  var where = platform ? "WHERE p.deleted_at IS NULL AND p.platform = $1 AND p.va_discord_id IS NOT NULL" : "WHERE p.deleted_at IS NULL AND p.va_discord_id IS NOT NULL";
  var params = platform ? [platform] : [];
  var sql =
    "SELECT p.va_discord_id, " +
    "       MAX(p.va_name) AS va_name, " +
    "       COUNT(*) FILTER (WHERE p.created_at::date = CURRENT_DATE) AS posts_today, " +
    "       COUNT(*) FILTER (WHERE p.created_at >= NOW() - INTERVAL '7 days') AS posts_7d, " +
    "       MAX(p.created_at) AS last_post_at " +
    "FROM posts p " +
    where + " " +
    "GROUP BY p.va_discord_id";
  var result = await pool.query(sql, params);
  return result.rows;
}

async function getNuggets(date, platform) {
  var whereClause = platform
    ? "WHERE p.deleted_at IS NULL AND p.created_at::date = $1 AND p.platform = $2 AND COALESCE(s.views, 0) > 0"
    : "WHERE p.created_at::date = $1 AND COALESCE(s.views, 0) > 0";
  var params = platform ? [date, platform] : [date];
  var sql = "SELECT p.id, p.ig_post_id, p.url, p.va_name, p.va_discord_id, p.created_at, p.caption, p.performance, p.platform, s.views, s.likes, s.comments, s.shares FROM posts p LEFT JOIN LATERAL (SELECT views, likes, comments, shares FROM snapshots sn WHERE sn.post_id = p.id ORDER BY sn.scraped_at DESC LIMIT 1) s ON true " + whereClause + " ORDER BY COALESCE(s.views, 0) DESC LIMIT 15";
  var result = await pool.query(sql, params);
  return result.rows;
}

// getRecommendations accepts either:
//   - a single date string "YYYY-MM-DD" (legacy: single day analysis)
//   - a period object { from: "YYYY-MM-DD" | null, to: "YYYY-MM-DD" }
//     where from = null means "since beginning"
async function getRecommendations(dateOrPeriod, platform) {
  // Determine the date filter clauses
  var fromDate, toDate;
  var isPeriod = dateOrPeriod && typeof dateOrPeriod === 'object' && 'to' in dateOrPeriod;
  if (isPeriod) {
    fromDate = dateOrPeriod.from || null; // null = since forever
    toDate = dateOrPeriod.to;
  } else {
    // Legacy single-day mode
    fromDate = dateOrPeriod;
    toDate = dateOrPeriod;
  }

  // Build SQL date filter — supports null (no lower bound = since forever)
  function buildDateFilter(prefix, paramOffset) {
    // prefix: "p." or "" depending on table alias
    if (fromDate && toDate) {
      return {
        clause: prefix + 'created_at::date >= $' + paramOffset + ' AND ' + prefix + 'created_at::date <= $' + (paramOffset + 1),
        params: [fromDate, toDate],
      };
    } else if (toDate) {
      return {
        clause: prefix + 'created_at::date <= $' + paramOffset,
        params: [toDate],
      };
    }
    return { clause: '1=1', params: [] };
  }

  // === REPOSTS: posts that performed well enough to be reposted ===
  var repostSql, repostParams;
  var df = buildDateFilter('p.', 1);
  if (platform) {
    repostSql = "SELECT p.id, p.ig_post_id, p.url, p.va_name, p.caption, p.platform, p.created_at, s.views, s.likes, s.comments, s.shares FROM posts p LEFT JOIN LATERAL (SELECT views, likes, comments, shares FROM snapshots sn WHERE sn.post_id = p.id ORDER BY sn.scraped_at DESC LIMIT 1) s ON true WHERE p.deleted_at IS NULL AND " + df.clause + " AND p.platform = $" + (df.params.length + 1) + " AND COALESCE(s.views, 0) >= $" + (df.params.length + 2) + " ORDER BY COALESCE(s.views, 0) DESC LIMIT 30";
    repostParams = df.params.concat([platform, BON_VIEWS]);
  } else {
    repostSql = "SELECT p.id, p.ig_post_id, p.url, p.va_name, p.caption, p.platform, p.created_at, s.views, s.likes, s.comments, s.shares FROM posts p LEFT JOIN LATERAL (SELECT views, likes, comments, shares FROM snapshots sn WHERE sn.post_id = p.id ORDER BY sn.scraped_at DESC LIMIT 1) s ON true WHERE p.deleted_at IS NULL AND " + df.clause + " AND COALESCE(s.views, 0) >= $" + (df.params.length + 1) + " ORDER BY COALESCE(s.views, 0) DESC LIMIT 30";
    repostParams = df.params.concat([BON_VIEWS]);
  }
  var repost = await pool.query(repostSql, repostParams);

  // === DISTRIBUTION: count posts per perf tier (viral / bon / moyen / flop) ===
  var allPostsParams, allPostsSql;
  var df2 = buildDateFilter('p.', 1);
  if (platform) {
    allPostsSql = "SELECT p.id, p.ig_post_id, p.url, p.va_name, p.va_discord_id, p.caption, p.platform, p.created_at, s.views, s.likes, s.comments, s.shares FROM posts p LEFT JOIN LATERAL (SELECT views, likes, comments, shares FROM snapshots sn WHERE sn.post_id = p.id ORDER BY sn.scraped_at DESC LIMIT 1) s ON true WHERE p.deleted_at IS NULL AND " + df2.clause + " AND p.platform = $" + (df2.params.length + 1) + " ORDER BY COALESCE(s.views, 0) DESC";
    allPostsParams = df2.params.concat([platform]);
  } else {
    allPostsSql = "SELECT p.id, p.ig_post_id, p.url, p.va_name, p.va_discord_id, p.caption, p.platform, p.created_at, s.views, s.likes, s.comments, s.shares FROM posts p LEFT JOIN LATERAL (SELECT views, likes, comments, shares FROM snapshots sn WHERE sn.post_id = p.id ORDER BY sn.scraped_at DESC LIMIT 1) s ON true WHERE p.deleted_at IS NULL AND " + df2.clause + " ORDER BY COALESCE(s.views, 0) DESC";
    allPostsParams = df2.params;
  }
  // Cap at 1000 to avoid OOM on long periods. Very rare to exceed in practice.
  allPostsSql += " LIMIT 1000";
  var allPosts = await pool.query(allPostsSql, allPostsParams);
  var totalPosts = allPosts.rows.length;

  // Categorize each post by performance tier and keep them in lists
  var perfCount = { viral: 0, bon: 0, moyen: 0, flop: 0 };
  var postsByTier = { viral: [], bon: [], moyen: [], flop: [] };
  allPosts.rows.forEach(function(p) {
    var v = Number(p.views) || 0;
    var perf = v >= VIRAL_VIEWS ? 'viral' : v >= BON_VIEWS ? 'bon' : v >= MOYEN_VIEWS ? 'moyen' : 'flop';
    perfCount[perf]++;
    postsByTier[perf].push(p);
  });
  var pctPerf = totalPosts > 0 ? Math.round((perfCount.viral + perfCount.bon) / totalPosts * 100) : 0;
  var pctFlop = totalPosts > 0 ? Math.round(perfCount.flop / totalPosts * 100) : 0;
  var toRepost = perfCount.viral + perfCount.bon;

  // === NUGGETS: for short periods (<=2 days), take from getNuggets; for long
  // periods, just take the top viral/bon posts.
  var nuggets;
  if (isPeriod) {
    nuggets = postsByTier.viral.concat(postsByTier.bon).slice(0, 50);
  } else {
    nuggets = await getNuggets(toDate, platform);
  }

  // === Underperformers / topPerformers (only for single-day mode) ===
  var underperformers = [];
  var topPerformers = [];
  if (!isPeriod) {
    var summaries = await getDailySummaries(toDate, platform);
    if (summaries.length > 0) {
      var totalViewsSum = summaries.reduce(function(a, b) { return a + Number(b.total_views); }, 0);
      var avgVa = totalViewsSum / summaries.length;
      underperformers = summaries.filter(function(s) { return Number(s.total_views) < avgVa * 0.5; });
      topPerformers = summaries.filter(function(s) { return Number(s.total_views) >= avgVa * 1.5; });
    }
  }

  return {
    postsToRepost: repost.rows,
    nuggets: nuggets,
    postsByTier: postsByTier,  // NEW: full lists per tier for clickable distribution
    underperformers: underperformers,
    topPerformers: topPerformers,
    kpis: { totalPosts: totalPosts, perfCount: perfCount, pctPerf: pctPerf, pctFlop: pctFlop, toRepost: toRepost },
  };
}

async function getHourlyPerformance(days, platform) {
  // days can be a number or 'all' (since the beginning of time)
  var isAll = (days === 'all' || days === 'ALL');
  var dateClause, params;

  if (isAll) {
    // No date lower bound — all posts ever
    if (platform) {
      dateClause = "p.platform = $1";
      params = [platform];
    } else {
      dateClause = "1=1";
      params = [];
    }
  } else {
    // Last N days
    if (platform) {
      dateClause = "p.created_at >= NOW() - ($1 || ' days')::interval AND p.platform = $2";
      params = [days, platform];
    } else {
      dateClause = "p.created_at >= NOW() - ($1 || ' days')::interval";
      params = [days];
    }
  }

  var whereClause = "WHERE p.deleted_at IS NULL AND " + dateClause + " AND COALESCE(latest.views, 0) > 0";

  var sql = "SELECT EXTRACT(HOUR FROM p.created_at AT TIME ZONE 'Africa/Porto-Novo')::int AS hour, COUNT(DISTINCT p.id) AS post_count, COALESCE(AVG(latest.views), 0)::int AS avg_views, COALESCE(AVG(latest.likes), 0)::int AS avg_likes, COALESCE(AVG(latest.comments), 0)::int AS avg_comments, CASE WHEN COALESCE(AVG(latest.views), 0) > 0 THEN ROUND((COALESCE(AVG(latest.likes), 0) + COALESCE(AVG(latest.comments), 0)) / GREATEST(AVG(latest.views), 1) * 100, 2) ELSE 0 END AS avg_engagement FROM posts p LEFT JOIN LATERAL (SELECT views, likes, comments FROM snapshots sn WHERE sn.post_id = p.id ORDER BY sn.scraped_at DESC LIMIT 1) latest ON true " + whereClause + " GROUP BY hour ORDER BY hour";
  var result = await pool.query(sql, params);
  return result.rows;
}

async function getPostsForCoaching(platform) {
  var platformFilter = platform ? " AND p.platform = '" + platform + "'" : "";
  var sql = "SELECT p.id, p.ig_post_id, p.url, p.va_name, p.va_discord_id, p.created_at, p.caption, p.platform, s.views, s.likes, s.comments, s.shares FROM posts p LEFT JOIN LATERAL (SELECT views, likes, comments, shares FROM snapshots sn WHERE sn.post_id = p.id ORDER BY sn.scraped_at DESC LIMIT 1) s ON true WHERE p.deleted_at IS NULL AND p.status = 'active' AND p.created_at <= NOW() - INTERVAL '55 minutes' AND p.created_at >= NOW() - INTERVAL '75 minutes' AND NOT EXISTS (SELECT 1 FROM snapshots sn2 WHERE sn2.post_id = p.id AND sn2.scraped_at >= p.created_at + INTERVAL '50 minutes' AND sn2.error = 'coaching_sent')" + platformFilter + " ORDER BY p.created_at ASC";
  var result = await pool.query(sql);
  return result.rows;
}

async function markCoachingSent(postId) {
  await pool.query("INSERT INTO snapshots (post_id, views, likes, comments, shares, error) VALUES ($1, 0, 0, 0, 0, 'coaching_sent')", [postId]);
}

// =============================================
// ===== STREAKS (with platform) =====
// =============================================

async function updateStreak(vaDiscordId, vaName, date, platform) {
  platform = platform || 'instagram';
  var posts = await getVaPostsToday(vaDiscordId, date, platform);
  var metObjective = posts.length >= 6;

  var existing = await pool.query('SELECT * FROM va_streaks WHERE va_discord_id = $1 AND platform = $2', [vaDiscordId, platform]);

  if (existing.rows.length === 0) {
    await pool.query('INSERT INTO va_streaks (va_discord_id, va_name, platform, current_streak, best_streak, last_streak_date) VALUES ($1, $2, $3, $4, $4, $5)', [vaDiscordId, vaName, platform, metObjective ? 1 : 0, date]);
    return { current: metObjective ? 1 : 0, best: metObjective ? 1 : 0, isNew: true };
  }

  var streak = existing.rows[0];
  var lastDate = streak.last_streak_date ? new Date(streak.last_streak_date) : null;
  var currentDate = new Date(date);
  var dayDiff = lastDate ? Math.round((currentDate - lastDate) / (1000 * 60 * 60 * 24)) : 999;

  var newStreak, newBest;
  if (metObjective) {
    if (dayDiff === 1) {
      newStreak = Number(streak.current_streak) + 1;
    } else {
      newStreak = 1;
    }
    newBest = Math.max(newStreak, Number(streak.best_streak));
    await pool.query('UPDATE va_streaks SET current_streak = $1, best_streak = $2, last_streak_date = $3, va_name = $4, updated_at = NOW() WHERE va_discord_id = $5 AND platform = $6', [newStreak, newBest, date, vaName, vaDiscordId, platform]);
  } else {
    newStreak = 0;
    newBest = Number(streak.best_streak);
    await pool.query('UPDATE va_streaks SET current_streak = 0, va_name = $1, updated_at = NOW() WHERE va_discord_id = $2 AND platform = $3', [vaName, vaDiscordId, platform]);
  }

  return { current: newStreak, best: newBest, previous: Number(streak.current_streak), broken: !metObjective && Number(streak.current_streak) > 0 };
}

async function getAllStreaks(platform) {
  if (platform) {
    var result = await pool.query('SELECT * FROM va_streaks WHERE platform = $1 ORDER BY current_streak DESC, best_streak DESC', [platform]);
    return result.rows;
  }
  var result2 = await pool.query('SELECT * FROM va_streaks ORDER BY current_streak DESC, best_streak DESC');
  return result2.rows;
}

// =============================================
// ===== INACTIVITY & PERFORMANCE DROPS =====
// =============================================

async function getInactiveVAs(hours, platform) {
  var platformFilter = platform ? " AND p.platform = '" + platform + "'" : "";
  var sql = "SELECT DISTINCT p.va_discord_id, p.va_name, MAX(p.created_at) AS last_post_at FROM posts p WHERE 1=1" + platformFilter + " GROUP BY p.va_discord_id, p.va_name HAVING MAX(p.created_at) < NOW() - ($1 || ' hours')::interval ORDER BY MAX(p.created_at) ASC";
  var result = await pool.query(sql, [hours]);
  return result.rows;
}

async function getPerformanceDrops(platform) {
  var today = new Date().toISOString().split('T')[0];
  var platformFilter = platform ? " AND ds_today.platform = '" + platform + "'" : "";
  var platformFilter2 = platform ? " AND platform = '" + platform + "'" : "";
  var sql = "SELECT ds_today.va_discord_id, ds_today.va_name, ds_today.platform, ds_today.total_views AS today_views, avg_7d.avg_views, CASE WHEN avg_7d.avg_views > 0 THEN ROUND((ds_today.total_views::numeric / avg_7d.avg_views) * 100) ELSE 0 END AS pct_of_avg FROM daily_summaries ds_today JOIN (SELECT va_discord_id, ROUND(AVG(total_views)) AS avg_views FROM daily_summaries WHERE date >= CURRENT_DATE - 7 AND date < CURRENT_DATE" + platformFilter2 + " GROUP BY va_discord_id HAVING AVG(total_views) > 0) avg_7d ON avg_7d.va_discord_id = ds_today.va_discord_id WHERE ds_today.date = $1" + platformFilter + " AND ds_today.total_views < avg_7d.avg_views * 0.5 ORDER BY (ds_today.total_views::numeric / GREATEST(avg_7d.avg_views, 1)) ASC";
  var result = await pool.query(sql, [today]);
  return result.rows;
}

// =============================================
// ===== SAVED BEST POSTS =====
// =============================================

async function getSavedBestPosts(limit, platform) {
  var whereClause = platform
    ? "WHERE p.performance IN ('viral', 'bon') AND p.platform = $2"
    : "WHERE p.performance IN ('viral', 'bon')";
  var params = platform ? [limit, platform] : [limit];
  var sql = "SELECT p.id, p.ig_post_id, p.url, p.va_name, p.va_discord_id, p.created_at, p.caption, p.performance, p.platform, s.views, s.likes, s.comments, s.shares FROM posts p LEFT JOIN LATERAL (SELECT views, likes, comments, shares FROM snapshots sn WHERE sn.post_id = p.id ORDER BY sn.scraped_at DESC LIMIT 1) s ON true " + whereClause + " ORDER BY COALESCE(s.views, 0) DESC LIMIT $1";
  var result = await pool.query(sql, params);
  return result.rows;
}

// =============================================
// ===== GAMIFICATION: POINTS, WINNERS, DUELS =====
// =============================================

// Award daily points based on the leaderboard. Call after computeDailySummary.
// Returns the list of rows awarded.
async function awardDailyPoints(date, platform) {
  if (!date || !platform) throw new Error('date and platform required');
  var leaderboard = await getLeaderboard(date, platform);
  // Only award points to VAs who hit the minimum (6 posts) — avoids giving
  // points to someone who posted once and happened to rank high.
  var eligible = leaderboard.filter(function(r) { return Number(r.post_count) >= 6; });
  if (eligible.length === 0) return [];

  var pointsMap = { 0: 10, 1: 6, 2: 3 };
  var rows = [];
  for (var i = 0; i < eligible.length && i < 3; i++) {
    var r = eligible[i];
    var sql = 'INSERT INTO va_points (va_discord_id, va_name, platform, date, rank, points, total_views) ' +
      'VALUES ($1, $2, $3, $4, $5, $6, $7) ' +
      'ON CONFLICT (va_discord_id, date, platform) DO UPDATE SET ' +
      '  rank = EXCLUDED.rank, points = EXCLUDED.points, total_views = EXCLUDED.total_views ' +
      'RETURNING *';
    var result = await pool.query(sql, [r.va_discord_id, r.va_name, platform, date, i + 1, pointsMap[i], Number(r.total_views) || 0]);
    rows.push(result.rows[0]);
  }
  logger.info('Awarded daily points for ' + platform + ' on ' + date + ': ' + rows.length + ' VAs');
  return rows;
}

// Weekly standings — sum of points across the week.
async function getWeeklyStandings(weekStart, weekEnd, platform) {
  var sql = 'SELECT va_discord_id, va_name, SUM(points)::int AS total_points, ' +
    'SUM(total_views)::bigint AS total_views, COUNT(*) AS podium_count ' +
    'FROM va_points ' +
    'WHERE date >= $1 AND date <= $2 AND platform = $3 ' +
    'GROUP BY va_discord_id, va_name ' +
    'ORDER BY total_points DESC, total_views DESC';
  var result = await pool.query(sql, [weekStart, weekEnd, platform]);
  return result.rows;
}

// Mark the weekly winner (top of standings). Returns the winner row or null.
async function recordWeeklyWinner(weekStart, weekEnd, platform) {
  var standings = await getWeeklyStandings(weekStart, weekEnd, platform);
  if (standings.length === 0) return null;
  var winner = standings[0];

  // Count posts published that week by the winner
  var postsSql = "SELECT COUNT(*)::int AS cnt FROM posts WHERE va_discord_id = $1 AND platform = $2 " +
    "AND created_at::date >= $3 AND created_at::date <= $4";
  var postsResult = await pool.query(postsSql, [winner.va_discord_id, platform, weekStart, weekEnd]);
  var totalPosts = postsResult.rows[0].cnt || 0;

  var sql = 'INSERT INTO weekly_winners (week_start, week_end, platform, va_discord_id, va_name, total_points, total_views, total_posts) ' +
    'VALUES ($1, $2, $3, $4, $5, $6, $7, $8) ' +
    'ON CONFLICT (week_start, platform) DO UPDATE SET ' +
    '  va_discord_id = EXCLUDED.va_discord_id, va_name = EXCLUDED.va_name, ' +
    '  total_points = EXCLUDED.total_points, total_views = EXCLUDED.total_views, ' +
    '  total_posts = EXCLUDED.total_posts, announced_at = NOW() ' +
    'RETURNING *';
  var result = await pool.query(sql, [weekStart, weekEnd, platform, winner.va_discord_id, winner.va_name, winner.total_points, winner.total_views, totalPosts]);
  return result.rows[0];
}

async function getRecentWinners(platform, limit) {
  limit = limit || 8;
  var sql = platform
    ? 'SELECT * FROM weekly_winners WHERE platform = $1 ORDER BY week_start DESC LIMIT $2'
    : 'SELECT * FROM weekly_winners ORDER BY week_start DESC LIMIT $1';
  var params = platform ? [platform, limit] : [limit];
  var result = await pool.query(sql, params);
  return result.rows;
}

// Create duels for a given week. Takes a list of VA {id, name} and shuffles them
// into pairs. Odd VA sits out this week. Returns the created duels.
async function createWeeklyDuels(weekStart, weekEnd, platform, vaList) {
  if (!vaList || vaList.length < 2) return [];

  // Fisher-Yates shuffle
  var pool2 = vaList.slice();
  for (var i = pool2.length - 1; i > 0; i--) {
    var j = Math.floor(Math.random() * (i + 1));
    var tmp = pool2[i]; pool2[i] = pool2[j]; pool2[j] = tmp;
  }

  var duels = [];
  for (var k = 0; k + 1 < pool2.length; k += 2) {
    var a = pool2[k], b = pool2[k + 1];
    var sql = 'INSERT INTO duels (week_start, week_end, platform, va1_discord_id, va1_name, va2_discord_id, va2_name) ' +
      'VALUES ($1, $2, $3, $4, $5, $6, $7) RETURNING *';
    var result = await pool.query(sql, [weekStart, weekEnd, platform, a.id, a.name, b.id, b.name]);
    duels.push(result.rows[0]);
  }
  logger.info('Created ' + duels.length + ' duels for ' + platform + ' (week ' + weekStart + ')');
  return duels;
}

// Resolve all active duels for a week: compute total views for each duelist,
// pick a winner, mark the row as resolved. Returns resolved duels.
async function resolveWeeklyDuels(weekStart, weekEnd, platform) {
  var fetchSql = "SELECT * FROM duels WHERE week_start = $1 AND platform = $2 AND status = 'active'";
  var activeDuels = await pool.query(fetchSql, [weekStart, platform]);

  var resolved = [];
  for (var i = 0; i < activeDuels.rows.length; i++) {
    var d = activeDuels.rows[i];
    // Sum views for each duelist for the week
    var viewsSql = "SELECT va_discord_id, COALESCE(SUM(s.views), 0)::bigint AS views " +
      "FROM posts p " +
      "LEFT JOIN LATERAL ( " +
      "  SELECT views FROM snapshots sn WHERE sn.post_id = p.id AND COALESCE(sn.error, '') <> 'coaching_sent' " +
      "  ORDER BY sn.scraped_at DESC LIMIT 1 " +
      ") s ON true " +
      "WHERE p.platform = $1 AND p.va_discord_id IN ($2, $3) " +
      "AND p.created_at::date >= $4 AND p.created_at::date <= $5 " +
      "GROUP BY va_discord_id";
    var viewsResult = await pool.query(viewsSql, [platform, d.va1_discord_id, d.va2_discord_id, weekStart, weekEnd]);
    var map = {};
    viewsResult.rows.forEach(function(r) { map[r.va_discord_id] = Number(r.views) || 0; });
    var v1 = map[d.va1_discord_id] || 0;
    var v2 = map[d.va2_discord_id] || 0;
    var winner = v1 > v2 ? d.va1_discord_id : v2 > v1 ? d.va2_discord_id : null;

    await pool.query("UPDATE duels SET va1_views = $1, va2_views = $2, winner_id = $3, status = 'resolved', resolved_at = NOW() WHERE id = $4",
      [v1, v2, winner, d.id]);
    d.va1_views = v1; d.va2_views = v2; d.winner_id = winner; d.status = 'resolved';
    resolved.push(d);
  }
  logger.info('Resolved ' + resolved.length + ' duels for ' + platform + ' (week ' + weekStart + ')');
  return resolved;
}

async function getActiveDuels(platform) {
  var sql = platform
    ? "SELECT * FROM duels WHERE status = 'active' AND platform = $1 ORDER BY week_start DESC"
    : "SELECT * FROM duels WHERE status = 'active' ORDER BY week_start DESC";
  var params = platform ? [platform] : [];
  var result = await pool.query(sql, params);
  return result.rows;
}

// Helper: compute Monday and Sunday (Europe/Paris) for a given date.
function getWeekBounds(refDate) {
  var d = refDate ? new Date(refDate) : new Date();
  // Day of week: 0=Sun, 1=Mon, ..., 6=Sat. Normalize so Monday = 0.
  var day = d.getDay();
  var mondayOffset = day === 0 ? -6 : 1 - day;
  var monday = new Date(d);
  monday.setDate(d.getDate() + mondayOffset);
  monday.setHours(0, 0, 0, 0);
  var sunday = new Date(monday);
  sunday.setDate(monday.getDate() + 6);
  return {
    start: monday.toISOString().split('T')[0],
    end: sunday.toISOString().split('T')[0],
  };
}



// Inactivity threshold in days — after this, an account with no new post is marked inactive.
var ACCOUNT_INACTIVITY_DAYS = parseInt(process.env.ACCOUNT_INACTIVITY_DAYS || '7', 10);

// Upsert an account. Touches last_seen_at and updates va mapping if changed.
async function upsertAccount(username, platform, vaDiscordId, vaName) {
  if (!username || !platform) return null;
  username = String(username).toLowerCase().trim();
  if (!username) return null;
  var sql = "INSERT INTO accounts (username, platform, va_discord_id, va_name, status, last_seen_at) " +
    "VALUES ($1, $2, $3, $4, 'active', NOW()) " +
    "ON CONFLICT (username, platform) DO UPDATE SET " +
    "  last_seen_at = NOW(), " +
    "  status = 'active', " +
    "  va_discord_id = COALESCE(EXCLUDED.va_discord_id, accounts.va_discord_id), " +
    "  va_name = COALESCE(EXCLUDED.va_name, accounts.va_name) " +
    "RETURNING *";
  var result = await pool.query(sql, [username, platform, vaDiscordId || null, vaName || null]);
  return result.rows[0];
}

async function getAccount(id) {
  var result = await pool.query('SELECT * FROM accounts WHERE id = $1', [id]);
  return result.rows[0] || null;
}

async function getAccountByUsername(username, platform) {
  if (!username || !platform) return null;
  var result = await pool.query('SELECT * FROM accounts WHERE username = $1 AND platform = $2', [String(username).toLowerCase(), platform]);
  return result.rows[0] || null;
}

// List accounts with aggregated stats (posts count, total views, last post date).
// Optionally filter by platform, VA, or status.
async function listAccountsWithStats(opts) {
  opts = opts || {};
  var conditions = [];
  var params = [];
  if (opts.platform) { params.push(opts.platform); conditions.push('a.platform = $' + params.length); }
  if (opts.vaDiscordId) { params.push(opts.vaDiscordId); conditions.push('a.va_discord_id = $' + params.length); }
  if (opts.status) { params.push(opts.status); conditions.push('a.status = $' + params.length); }
  var where = conditions.length > 0 ? 'WHERE ' + conditions.join(' AND ') : '';

  // Aggregate snapshots via LATERAL to avoid row-multiplication on joins.
  var sql =
    "SELECT a.id, a.username, a.platform, a.va_discord_id, a.va_name, a.status, " +
    "       a.first_seen_at, a.last_seen_at, a.created_at, " +
    "       COALESCE(stats.total_posts, 0) AS total_posts, " +
    "       COALESCE(stats.posts_7d, 0) AS posts_7d, " +
    "       COALESCE(stats.total_views, 0) AS total_views, " +
    "       COALESCE(stats.total_likes, 0) AS total_likes, " +
    "       COALESCE(stats.total_comments, 0) AS total_comments, " +
    "       COALESCE(stats.total_shares, 0) AS total_shares, " +
    "       stats.last_post_at " +
    "FROM accounts a " +
    "LEFT JOIN LATERAL ( " +
    "  SELECT COUNT(DISTINCT p.id) AS total_posts, " +
    "         COUNT(DISTINCT p.id) FILTER (WHERE p.created_at >= NOW() - INTERVAL '7 days') AS posts_7d, " +
    "         COALESCE(SUM(latest.views), 0) AS total_views, " +
    "         COALESCE(SUM(latest.likes), 0) AS total_likes, " +
    "         COALESCE(SUM(latest.comments), 0) AS total_comments, " +
    "         COALESCE(SUM(latest.shares), 0) AS total_shares, " +
    "         MAX(p.created_at) AS last_post_at " +
    "  FROM posts p " +
    "  LEFT JOIN LATERAL ( " +
    "    SELECT views, likes, comments, shares FROM snapshots s " +
    "    WHERE s.post_id = p.id AND COALESCE(s.error, '') <> 'coaching_sent' " +
    "    ORDER BY s.scraped_at DESC LIMIT 1 " +
    "  ) latest ON true " +
    "  WHERE p.account_id = a.id " +
    ") stats ON true " +
    where + " " +
    "ORDER BY COALESCE(stats.last_post_at, a.last_seen_at) DESC NULLS LAST";

  var result = await pool.query(sql, params);
  return result.rows;
}

// Detailed view: account + recent posts with latest stats.
async function getAccountDetails(accountId, daysLimit) {
  daysLimit = daysLimit || 30;
  var account = await getAccount(accountId);
  if (!account) return null;
  var postsSql =
    "SELECT p.id, p.ig_post_id, p.url, p.va_name, p.va_discord_id, p.created_at, " +
    "       p.posted_at, p.link_delay_minutes, " +
    "       p.caption, p.performance, p.platform, p.status, " +
    "       p.deleted_at, p.deleted_by, " +
    "       s.views, s.likes, s.comments, s.shares, s.retweets, s.quote_tweets, s.bookmarks " +
    "FROM posts p " +
    "LEFT JOIN LATERAL ( " +
    "  SELECT views, likes, comments, shares, retweets, quote_tweets, bookmarks FROM snapshots sn " +
    "  WHERE sn.post_id = p.id AND COALESCE(sn.error, '') <> 'coaching_sent' " +
    "  ORDER BY sn.scraped_at DESC LIMIT 1 " +
    ") s ON true " +
    "WHERE p.account_id = $1 AND p.created_at >= NOW() - ($2 || ' days')::interval " +
    "ORDER BY p.created_at DESC";
  var posts = await pool.query(postsSql, [accountId, daysLimit]);

  // Split posts into active and deleted (the dashboard shows them in two sections)
  var active = [];
  var deleted = [];
  for (var i = 0; i < posts.rows.length; i++) {
    if (posts.rows[i].deleted_at) deleted.push(posts.rows[i]);
    else active.push(posts.rows[i]);
  }
  return { account: account, posts: active, deletedPosts: deleted };
}

// Soft delete a post: keeps the row but marks it as deleted.
// Deleted posts are excluded from rankings, totals, alerts, etc.
async function softDeletePost(postId, deletedBy) {
  var sql = "UPDATE posts SET deleted_at = NOW(), deleted_by = $2, status = 'deleted' WHERE id = $1 AND deleted_at IS NULL RETURNING id, platform, account_id";
  var result = await pool.query(sql, [postId, deletedBy || 'unknown']);
  return result.rows[0] || null;
}

// Restore a previously soft-deleted post back to active.
async function restorePost(postId) {
  var sql = "UPDATE posts SET deleted_at = NULL, deleted_by = NULL, status = 'active' WHERE id = $1 AND deleted_at IS NOT NULL RETURNING id, platform";
  var result = await pool.query(sql, [postId]);
  return result.rows[0] || null;
}

// Get a post by ID for permission checks before delete/restore.
async function getPostBasics(postId) {
  var result = await pool.query('SELECT id, platform, account_id, va_discord_id, deleted_at FROM posts WHERE id = $1', [postId]);
  return result.rows[0] || null;
}

// Manually mark an account active/inactive (e.g. admin override from the dashboard).
async function setAccountStatus(accountId, status) {
  if (status !== 'active' && status !== 'inactive') throw new Error('Invalid status');
  var result = await pool.query('UPDATE accounts SET status = $1 WHERE id = $2 RETURNING *', [status, accountId]);
  return result.rows[0] || null;
}

// Auto-sweep: accounts with no post in N days are marked inactive.
// Run from a daily cron. Returns how many accounts flipped status.
async function markInactiveAccounts(days) {
  var n = days || ACCOUNT_INACTIVITY_DAYS;
  var sql =
    "UPDATE accounts a SET status = 'inactive' " +
    "WHERE a.status = 'active' " +
    "  AND NOT EXISTS ( " +
    "    SELECT 1 FROM posts p " +
    "    WHERE p.account_id = a.id AND p.created_at >= NOW() - ($1 || ' days')::interval " +
    "  ) " +
    "  AND a.last_seen_at < NOW() - ($1 || ' days')::interval " +
    "RETURNING id, username, platform";
  var result = await pool.query(sql, [n]);
  if (result.rowCount > 0) {
    logger.info('Marked ' + result.rowCount + ' accounts inactive (>= ' + n + 'd of silence)');
  }
  return result.rows;
}

// Distinct usernames used by a given VA (handy for autocomplete / cards).
async function getAccountsForVa(vaDiscordId, platform) {
  var sql = platform
    ? "SELECT * FROM accounts WHERE va_discord_id = $1 AND platform = $2 ORDER BY last_seen_at DESC"
    : "SELECT * FROM accounts WHERE va_discord_id = $1 ORDER BY last_seen_at DESC";
  var params = platform ? [vaDiscordId, platform] : [vaDiscordId];
  var result = await pool.query(sql, params);
  return result.rows;
}

// Detect accounts whose recent views (last 3 days) dropped below a fraction
// of their baseline (days -4 to -10). Excludes accounts with no recent posts
// — "no post" is an inactivity problem, not a performance drop.
//
// Returns only accounts with a meaningful baseline (≥ 3 posts in the baseline
// window) to avoid false positives on brand-new accounts.
//
// threshold = 0.5 means "dropped to less than 50% of usual".
async function getAccountPerformanceDrops(thresholdRatio, platform) {
  thresholdRatio = thresholdRatio || 0.5;
  var platformFilter = platform ? "AND a.platform = '" + platform + "'" : "";

  var sql =
    "WITH recent AS (" +
    "  SELECT p.account_id, " +
    "         AVG(COALESCE(s.views, 0))::numeric AS avg_views, " +
    "         COUNT(*)::int AS n_posts " +
    "  FROM posts p " +
    "  LEFT JOIN LATERAL (" +
    "    SELECT views FROM snapshots sn " +
    "    WHERE sn.post_id = p.id AND COALESCE(sn.error, '') <> 'coaching_sent' " +
    "    ORDER BY sn.scraped_at DESC LIMIT 1" +
    "  ) s ON true " +
    "  WHERE p.account_id IS NOT NULL " +
    "    AND p.created_at >= NOW() - INTERVAL '3 days' " +
    "  GROUP BY p.account_id " +
    "  HAVING COUNT(*) >= 1" + // must have at least 1 post in recent window
    "), " +
    "baseline AS (" +
    "  SELECT p.account_id, " +
    "         AVG(COALESCE(s.views, 0))::numeric AS avg_views, " +
    "         COUNT(*)::int AS n_posts " +
    "  FROM posts p " +
    "  LEFT JOIN LATERAL (" +
    "    SELECT views FROM snapshots sn " +
    "    WHERE sn.post_id = p.id AND COALESCE(sn.error, '') <> 'coaching_sent' " +
    "    ORDER BY sn.scraped_at DESC LIMIT 1" +
    "  ) s ON true " +
    "  WHERE p.account_id IS NOT NULL " +
    "    AND p.created_at >= NOW() - INTERVAL '10 days' " +
    "    AND p.created_at < NOW() - INTERVAL '3 days' " +
    "  GROUP BY p.account_id " +
    "  HAVING COUNT(*) >= 3 AND AVG(COALESCE(s.views, 0)) > 0" + // baseline must be real
    ") " +
    "SELECT a.id, a.username, a.platform, a.va_discord_id, a.va_name, " +
    "       recent.avg_views AS recent_avg, " +
    "       baseline.avg_views AS baseline_avg, " +
    "       recent.n_posts AS recent_posts, " +
    "       baseline.n_posts AS baseline_posts, " +
    "       CASE WHEN baseline.avg_views > 0 " +
    "            THEN ROUND((recent.avg_views / baseline.avg_views) * 100) " +
    "            ELSE 0 END AS pct_of_baseline " +
    "FROM accounts a " +
    "JOIN recent ON recent.account_id = a.id " +
    "JOIN baseline ON baseline.account_id = a.id " +
    "WHERE a.status = 'active' " + platformFilter + " " +
    "  AND (recent.avg_views / baseline.avg_views) < $1 " +
    "ORDER BY (recent.avg_views / baseline.avg_views) ASC";

  var result = await pool.query(sql, [thresholdRatio]);
  return result.rows;
}

// Compute a simple health score per account based on:
//   - posting frequency (posts_7d)
//   - engagement tendency (recent vs baseline views)
//   - recency (days since last post)
// Returns the same rows as listAccountsWithStats but with 3 extra fields:
//   health_status: 'green' | 'orange' | 'red'
//   health_score: 0-100
//   health_reason: short human-readable explanation
function computeAccountHealth(account) {
  var posts7d = Number(account.posts_7d) || 0;
  var daysSince = account.days_since_last_post;
  var status = 'green';
  var score = 100;
  var reasons = [];

  // 1) Inactivity penalty (days since last post)
  if (daysSince == null) {
    score = 0;
    status = 'red';
    reasons.push('aucun post');
  } else if (daysSince >= 7) {
    score -= 60;
    reasons.push('pas poste depuis ' + daysSince + 'j');
  } else if (daysSince >= 3) {
    score -= 25;
    reasons.push('pas poste depuis ' + daysSince + 'j');
  }

  // 2) Posting frequency (on 7 days, we expect ≥ 6 posts for healthy)
  if (posts7d < 3) {
    score -= 30;
    reasons.push('freq faible (' + posts7d + '/7j)');
  } else if (posts7d < 6) {
    score -= 10;
    reasons.push('freq moyenne (' + posts7d + '/7j)');
  }

  // Clamp and classify
  if (score < 0) score = 0;
  if (score >= 70) status = 'green';
  else if (score >= 40) status = 'orange';
  else status = 'red';

  return {
    health_status: status,
    health_score: score,
    health_reason: reasons.length > 0 ? reasons.join(', ') : 'tout va bien',
  };
}



async function getDashboardUser(username) {
  var result = await pool.query('SELECT * FROM dashboard_users WHERE username = $1', [username]);
  return result.rows[0] || null;
}

async function upsertDashboardUser(username, passwordHash, role, platform, discordId) {
  var sql = "INSERT INTO dashboard_users (username, password_hash, role, platform, discord_id, status, revoked_at, revoked_reason) " +
    "VALUES ($1, $2, $3, $4, $5, 'active', NULL, NULL) " +
    "ON CONFLICT (username) DO UPDATE SET " +
    "  password_hash = $2, role = $3, platform = $4, discord_id = $5, " +
    "  status = 'active', revoked_at = NULL, revoked_reason = NULL " +
    "RETURNING *";
  var result = await pool.query(sql, [username, passwordHash, role || 'va', platform || 'all', discordId || null]);
  return result.rows[0];
}

// List ALL DB-stored dashboard users (ENV users are NOT in this table).
// Used by the daily revocation sweep.
async function getAllDashboardUsers() {
  var result = await pool.query('SELECT * FROM dashboard_users ORDER BY username');
  return result.rows;
}

// Mark a dashboard user as revoked (login blocked, DB record preserved).
async function revokeDashboardUser(username, reason) {
  var sql = "UPDATE dashboard_users SET status = 'revoked', revoked_at = NOW(), revoked_reason = $2, last_check_at = NOW() WHERE username = $1 RETURNING *";
  var result = await pool.query(sql, [username, reason || 'auto-revocation']);
  return result.rows[0] || null;
}

// Reactivate a previously revoked user (admin action).
async function reactivateDashboardUser(username) {
  var sql = "UPDATE dashboard_users SET status = 'active', revoked_at = NULL, revoked_reason = NULL WHERE username = $1 RETURNING *";
  var result = await pool.query(sql, [username]);
  return result.rows[0] || null;
}

// Touch last_check_at for a user we verified is still valid.
async function touchDashboardUserCheck(username) {
  await pool.query('UPDATE dashboard_users SET last_check_at = NOW() WHERE username = $1', [username]);
}

module.exports = {
  pool: require('./init').pool,
  // Permissions
  setUserPermission, removeUserPermission, getUserPermissions,
  getUserPlatforms, getUserRole, canAccessPlatform, getAllPermissions,
  // Posts
  insertPost, updatePostAccount, getPost, getPostByIgId, getActivePosts,
  endTracking, setPostError, setManagerMsgId,
  // Snapshots
  insertSnapshot, getLatestSnapshot, getSnapshotHistory, getSnapshotAtHour, getPostMilestones,
  // Summaries
  computeDailySummary, getDailySummaries, getRangeSummaries, getVaDailyStats, getVaPostsToday,
  getLeaderboard, endExpiredPosts, getPostsForExport,
  // Analytics
  getTopPostsWithPerformance, getVaPerformanceHistory, checkViralPosts,
  updatePostPerformance, getSavedBestPosts, getNuggets, getRecommendations,
  getHourlyPerformance, getPostsForCoaching, markCoachingSent,
  getNewPostsReachingThreshold, recordViralNotification,
  recordDmAttempt, getAllDmStatus, getBlockedDmVAs, getVaActivityStatus,
  getShadowbanCandidates, computeShadowbanScore,
  markLateAlertSent, getLateLinkPosts, wasReminderSent, markReminderSent, countPostsBetween,
  softDeletePost, restorePost, getPostBasics,
  // Streaks
  updateStreak, getAllStreaks,
  // Alerts
  getInactiveVAs, getPerformanceDrops,
  // Accounts
  upsertAccount, getAccount, getAccountByUsername, listAccountsWithStats,
  getAccountDetails, setAccountStatus, markInactiveAccounts, getAccountsForVa,
  getAccountPerformanceDrops, computeAccountHealth,
  ACCOUNT_INACTIVITY_DAYS,
  // Gamification
  awardDailyPoints, getWeeklyStandings, recordWeeklyWinner, getRecentWinners,
  createWeeklyDuels, resolveWeeklyDuels, getActiveDuels, getWeekBounds,
  // Dashboard
  getDashboardUser, upsertDashboardUser, getAllDashboardUsers,
  revokeDashboardUser, reactivateDashboardUser, touchDashboardUserCheck,
  // Constants
  VIRAL_VIEWS, BON_VIEWS, MOYEN_VIEWS,
};
