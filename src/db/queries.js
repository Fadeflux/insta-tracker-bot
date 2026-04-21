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
  if (hasAll) return ['instagram', 'twitter'];
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

async function insertPost({ igPostId, url, vaDiscordId, vaName, caption, platform, guildId }) {
  platform = platform || 'instagram';
  var postType = 'post';
  if (platform === 'instagram') {
    postType = url.includes('/reel/') ? 'reel' : 'post';
  } else if (platform === 'twitter') {
    postType = 'tweet';
  }
  var sql = "INSERT INTO posts (ig_post_id, url, va_discord_id, va_name, post_type, caption, platform, guild_id, tracking_end) VALUES ($1, $2, $3, $4, $5, $6, $7, $8, (DATE_TRUNC('day', NOW() AT TIME ZONE 'Europe/Paris') + INTERVAL '23 hours 59 minutes') AT TIME ZONE 'Europe/Paris') ON CONFLICT (ig_post_id) DO NOTHING RETURNING *";
  var result = await pool.query(sql, [igPostId, url, vaDiscordId, vaName, postType, caption || null, platform, guildId || null]);
  return result.rows[0] || null;
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

  return result.rows[0];
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
    var sql = "INSERT INTO daily_summaries (va_discord_id, va_name, date, platform, post_count, total_views, total_likes, total_comments, total_shares) SELECT p.va_discord_id, p.va_name, $1::date, $3, COUNT(DISTINCT p.id), COALESCE(SUM(latest.views), 0), COALESCE(SUM(latest.likes), 0), COALESCE(SUM(latest.comments), 0), COALESCE(SUM(latest.shares), 0) FROM posts p LEFT JOIN LATERAL (SELECT views, likes, comments, shares FROM snapshots s WHERE s.post_id = p.id ORDER BY s.scraped_at DESC LIMIT 1) latest ON true WHERE p.created_at::date = $1::date AND p.platform = $2 GROUP BY p.va_discord_id, p.va_name ON CONFLICT (va_discord_id, date, platform) DO UPDATE SET post_count = EXCLUDED.post_count, total_views = EXCLUDED.total_views, total_likes = EXCLUDED.total_likes, total_comments = EXCLUDED.total_comments, total_shares = EXCLUDED.total_shares RETURNING *";
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
    sql = "SELECT p.va_name, p.url, p.ig_post_id, p.platform, p.created_at, p.status, p.performance, s.views, s.likes, s.comments, s.shares, s.retweets, s.quote_tweets, s.bookmarks, s.scraped_at FROM posts p JOIN snapshots s ON s.post_id = p.id WHERE p.created_at::date = $1 AND p.platform = $2 ORDER BY p.va_name, p.created_at, s.scraped_at";
    params = [date, platform];
  } else {
    sql = "SELECT p.va_name, p.url, p.ig_post_id, p.platform, p.created_at, p.status, p.performance, s.views, s.likes, s.comments, s.shares, s.retweets, s.quote_tweets, s.bookmarks, s.scraped_at FROM posts p JOIN snapshots s ON s.post_id = p.id WHERE p.created_at::date = $1 ORDER BY p.platform, p.va_name, p.created_at, s.scraped_at";
    params = [date];
  }
  var result = await pool.query(sql, params);
  return result.rows;
}

async function getTopPostsWithPerformance(date, platform) {
  var whereClause = platform
    ? "WHERE p.created_at::date = $1 AND p.platform = $2"
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

async function getNuggets(date, platform) {
  var whereClause = platform
    ? "WHERE p.created_at::date = $1 AND p.platform = $2 AND COALESCE(s.views, 0) > 0"
    : "WHERE p.created_at::date = $1 AND COALESCE(s.views, 0) > 0";
  var params = platform ? [date, platform] : [date];
  var sql = "SELECT p.id, p.ig_post_id, p.url, p.va_name, p.va_discord_id, p.created_at, p.caption, p.performance, p.platform, s.views, s.likes, s.comments, s.shares FROM posts p LEFT JOIN LATERAL (SELECT views, likes, comments, shares FROM snapshots sn WHERE sn.post_id = p.id ORDER BY sn.scraped_at DESC LIMIT 1) s ON true " + whereClause + " ORDER BY COALESCE(s.views, 0) DESC LIMIT 15";
  var result = await pool.query(sql, params);
  return result.rows;
}

async function getRecommendations(date, platform) {
  var repostSql, repostParams;
  if (platform) {
    repostSql = "SELECT p.id, p.ig_post_id, p.url, p.va_name, p.caption, p.platform, s.views, s.likes, s.comments, s.shares FROM posts p LEFT JOIN LATERAL (SELECT views, likes, comments, shares FROM snapshots sn WHERE sn.post_id = p.id ORDER BY sn.scraped_at DESC LIMIT 1) s ON true WHERE p.created_at::date = $1 AND p.platform = $3 AND COALESCE(s.views, 0) >= $2 ORDER BY COALESCE(s.views, 0) DESC LIMIT 10";
    repostParams = [date, BON_VIEWS, platform];
  } else {
    repostSql = "SELECT p.id, p.ig_post_id, p.url, p.va_name, p.caption, p.platform, s.views, s.likes, s.comments, s.shares FROM posts p LEFT JOIN LATERAL (SELECT views, likes, comments, shares FROM snapshots sn WHERE sn.post_id = p.id ORDER BY sn.scraped_at DESC LIMIT 1) s ON true WHERE p.created_at::date = $1 AND COALESCE(s.views, 0) >= $2 ORDER BY COALESCE(s.views, 0) DESC LIMIT 10";
    repostParams = [date, BON_VIEWS];
  }
  var repost = await pool.query(repostSql, repostParams);

  var summaries = await getDailySummaries(date, platform);
  var totalAvg = 0;
  if (summaries.length > 0) {
    var totalViews = summaries.reduce(function(a, b) { return a + Number(b.total_views); }, 0);
    totalAvg = totalViews / summaries.length;
  }
  var underperformers = summaries.filter(function(s) { return Number(s.total_views) < totalAvg * 0.5; });
  var topPerformers = summaries.filter(function(s) { return Number(s.total_views) >= totalAvg * 1.5; });

  var allPostsWhere = platform
    ? "WHERE p.created_at::date = $1 AND p.platform = $2"
    : "WHERE p.created_at::date = $1";
  var allPostsParams = platform ? [date, platform] : [date];
  var allPostsSql = "SELECT p.id, p.performance, s.views FROM posts p LEFT JOIN LATERAL (SELECT views FROM snapshots sn WHERE sn.post_id = p.id ORDER BY sn.scraped_at DESC LIMIT 1) s ON true " + allPostsWhere;
  var allPosts = await pool.query(allPostsSql, allPostsParams);
  var totalPosts = allPosts.rows.length;
  var perfCount = { viral: 0, bon: 0, moyen: 0, flop: 0 };
  allPosts.rows.forEach(function(p) {
    var v = Number(p.views) || 0;
    var perf = v >= VIRAL_VIEWS ? 'viral' : v >= BON_VIEWS ? 'bon' : v >= MOYEN_VIEWS ? 'moyen' : 'flop';
    perfCount[perf]++;
  });
  var pctPerf = totalPosts > 0 ? Math.round((perfCount.viral + perfCount.bon) / totalPosts * 100) : 0;
  var pctFlop = totalPosts > 0 ? Math.round(perfCount.flop / totalPosts * 100) : 0;
  var toRepost = perfCount.viral + perfCount.bon;

  var nuggets = await getNuggets(date, platform);

  return {
    postsToRepost: repost.rows,
    nuggets: nuggets,
    underperformers: underperformers,
    topPerformers: topPerformers,
    kpis: { totalPosts: totalPosts, perfCount: perfCount, pctPerf: pctPerf, pctFlop: pctFlop, toRepost: toRepost },
  };
}

async function getHourlyPerformance(days, platform) {
  var whereClause = platform
    ? "WHERE p.created_at >= NOW() - ($1 || ' days')::interval AND p.platform = $2 AND COALESCE(latest.views, 0) > 0"
    : "WHERE p.created_at >= NOW() - ($1 || ' days')::interval AND COALESCE(latest.views, 0) > 0";
  var params = platform ? [days, platform] : [days];
  var sql = "SELECT EXTRACT(HOUR FROM p.created_at AT TIME ZONE 'Europe/Paris')::int AS hour, COUNT(DISTINCT p.id) AS post_count, COALESCE(AVG(latest.views), 0)::int AS avg_views, COALESCE(AVG(latest.likes), 0)::int AS avg_likes, COALESCE(AVG(latest.comments), 0)::int AS avg_comments, CASE WHEN COALESCE(AVG(latest.views), 0) > 0 THEN ROUND((COALESCE(AVG(latest.likes), 0) + COALESCE(AVG(latest.comments), 0)) / GREATEST(AVG(latest.views), 1) * 100, 2) ELSE 0 END AS avg_engagement FROM posts p LEFT JOIN LATERAL (SELECT views, likes, comments FROM snapshots sn WHERE sn.post_id = p.id ORDER BY sn.scraped_at DESC LIMIT 1) latest ON true " + whereClause + " GROUP BY hour ORDER BY hour";
  var result = await pool.query(sql, params);
  return result.rows;
}

async function getPostsForCoaching(platform) {
  var platformFilter = platform ? " AND p.platform = '" + platform + "'" : "";
  var sql = "SELECT p.id, p.ig_post_id, p.url, p.va_name, p.va_discord_id, p.created_at, p.caption, p.platform, s.views, s.likes, s.comments, s.shares FROM posts p LEFT JOIN LATERAL (SELECT views, likes, comments, shares FROM snapshots sn WHERE sn.post_id = p.id ORDER BY sn.scraped_at DESC LIMIT 1) s ON true WHERE p.status = 'active' AND p.created_at <= NOW() - INTERVAL '55 minutes' AND p.created_at >= NOW() - INTERVAL '75 minutes' AND NOT EXISTS (SELECT 1 FROM snapshots sn2 WHERE sn2.post_id = p.id AND sn2.scraped_at >= p.created_at + INTERVAL '50 minutes' AND sn2.error = 'coaching_sent')" + platformFilter + " ORDER BY p.created_at ASC";
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
// ===== DASHBOARD USERS =====
// =============================================

async function getDashboardUser(username) {
  var result = await pool.query('SELECT * FROM dashboard_users WHERE username = $1', [username]);
  return result.rows[0] || null;
}

async function upsertDashboardUser(username, passwordHash, role, platform) {
  var sql = "INSERT INTO dashboard_users (username, password_hash, role, platform) VALUES ($1, $2, $3, $4) ON CONFLICT (username) DO UPDATE SET password_hash = $2, role = $3, platform = $4 RETURNING *";
  var result = await pool.query(sql, [username, passwordHash, role || 'va', platform || 'all']);
  return result.rows[0];
}

module.exports = {
  pool: require('./init').pool,
  // Permissions
  setUserPermission, removeUserPermission, getUserPermissions,
  getUserPlatforms, getUserRole, canAccessPlatform, getAllPermissions,
  // Posts
  insertPost, getPost, getPostByIgId, getActivePosts,
  endTracking, setPostError, setManagerMsgId,
  // Snapshots
  insertSnapshot, getLatestSnapshot, getSnapshotHistory, getSnapshotAtHour, getPostMilestones,
  // Summaries
  computeDailySummary, getDailySummaries, getVaDailyStats, getVaPostsToday,
  getLeaderboard, endExpiredPosts, getPostsForExport,
  // Analytics
  getTopPostsWithPerformance, getVaPerformanceHistory, checkViralPosts,
  updatePostPerformance, getSavedBestPosts, getNuggets, getRecommendations,
  getHourlyPerformance, getPostsForCoaching, markCoachingSent,
  // Streaks
  updateStreak, getAllStreaks,
  // Alerts
  getInactiveVAs, getPerformanceDrops,
  // Dashboard
  getDashboardUser, upsertDashboardUser,
  // Constants
  VIRAL_VIEWS, BON_VIEWS, MOYEN_VIEWS,
};
