const { pool } = require('./init');
const logger = require('../utils/logger');

// Performance thresholds (configurable via env)
var VIRAL_VIEWS = parseInt(process.env.VIRAL_VIEWS || '5000');
var BON_VIEWS = parseInt(process.env.BON_VIEWS || '1000');
var MOYEN_VIEWS = parseInt(process.env.MOYEN_VIEWS || '300');

async function insertPost({ igPostId, url, vaDiscordId, vaName, caption }) {
  var postType = url.includes('/reel/') ? 'reel' : 'post';
  var sql = "INSERT INTO posts (ig_post_id, url, va_discord_id, va_name, post_type, caption, tracking_end) VALUES ($1, $2, $3, $4, $5, $6, (DATE_TRUNC('day', NOW() AT TIME ZONE 'Europe/Paris') + INTERVAL '23 hours 59 minutes') AT TIME ZONE 'Europe/Paris') ON CONFLICT (ig_post_id) DO NOTHING RETURNING *";
  var result = await pool.query(sql, [igPostId, url, vaDiscordId, vaName, postType, caption || null]);
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

async function getActivePosts() {
  var result = await pool.query("SELECT * FROM posts WHERE status = 'active' ORDER BY created_at ASC");
  return result.rows;
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

async function insertSnapshot(postId, stats) {
  var sql = 'INSERT INTO snapshots (post_id, views, likes, comments, shares, error) VALUES ($1, $2, $3, $4, $5, $6) RETURNING *';
  var result = await pool.query(sql, [postId, stats.views || 0, stats.likes || 0, stats.comments || 0, stats.shares || 0, stats.error || null]);

  // Update performance classification
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

// Get snapshot at specific hour offset from post creation
async function getSnapshotAtHour(postId, hours) {
  var sql = "SELECT s.* FROM snapshots s JOIN posts p ON p.id = s.post_id WHERE s.post_id = $1 AND s.scraped_at <= p.created_at + ($2 || ' hours')::interval ORDER BY s.scraped_at DESC LIMIT 1";
  var result = await pool.query(sql, [postId, hours]);
  return result.rows[0] || null;
}

// Get milestones (1h, 3h, 6h views)
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

async function computeDailySummary(date) {
  var sql = "INSERT INTO daily_summaries (va_discord_id, va_name, date, post_count, total_views, total_likes, total_comments, total_shares) SELECT p.va_discord_id, p.va_name, $1::date, COUNT(DISTINCT p.id), COALESCE(SUM(latest.views), 0), COALESCE(SUM(latest.likes), 0), COALESCE(SUM(latest.comments), 0), COALESCE(SUM(latest.shares), 0) FROM posts p LEFT JOIN LATERAL (SELECT views, likes, comments, shares FROM snapshots s WHERE s.post_id = p.id ORDER BY s.scraped_at DESC LIMIT 1) latest ON true WHERE p.created_at::date = $1::date GROUP BY p.va_discord_id, p.va_name ON CONFLICT (va_discord_id, date) DO UPDATE SET post_count = EXCLUDED.post_count, total_views = EXCLUDED.total_views, total_likes = EXCLUDED.total_likes, total_comments = EXCLUDED.total_comments, total_shares = EXCLUDED.total_shares RETURNING *";
  var result = await pool.query(sql, [date]);
  return result.rows;
}

async function getDailySummaries(date) {
  var result = await pool.query('SELECT * FROM daily_summaries WHERE date = $1 ORDER BY total_views DESC', [date]);
  return result.rows;
}

async function getVaDailyStats(vaDiscordId, date) {
  var result = await pool.query('SELECT * FROM daily_summaries WHERE va_discord_id = $1 AND date = $2', [vaDiscordId, date]);
  return result.rows[0];
}

async function getVaPostsToday(vaDiscordId, date) {
  var result = await pool.query('SELECT * FROM posts WHERE va_discord_id = $1 AND created_at::date = $2 ORDER BY created_at ASC', [vaDiscordId, date]);
  return result.rows;
}

async function getLeaderboard(date) {
  var result = await pool.query('SELECT * FROM daily_summaries WHERE date = $1 ORDER BY total_views DESC', [date]);
  return result.rows;
}

async function endExpiredPosts() {
  var result = await pool.query("UPDATE posts SET status = 'ended' WHERE status = 'active' AND tracking_end < NOW()");
  if (result.rowCount > 0) logger.info('Ended tracking for ' + result.rowCount + ' expired posts');
  return result.rowCount;
}

async function getPostsForExport(date) {
  var sql = "SELECT p.va_name, p.url, p.ig_post_id, p.created_at, p.status, p.performance, s.views, s.likes, s.comments, s.shares, s.scraped_at FROM posts p JOIN snapshots s ON s.post_id = p.id WHERE p.created_at::date = $1 ORDER BY p.va_name, p.created_at, s.scraped_at";
  var result = await pool.query(sql, [date]);
  return result.rows;
}

// Get top posts with performance data
async function getTopPostsWithPerformance(date) {
  var sql = "SELECT p.id, p.ig_post_id, p.url, p.va_name, p.va_discord_id, p.created_at, p.performance, p.post_type, s.views, s.likes, s.comments, s.shares FROM posts p LEFT JOIN LATERAL (SELECT views, likes, comments, shares FROM snapshots sn WHERE sn.post_id = p.id ORDER BY sn.scraped_at DESC LIMIT 1) s ON true WHERE p.created_at::date = $1 ORDER BY COALESCE(s.views, 0) DESC LIMIT 30";
  var result = await pool.query(sql, [date]);
  return result.rows;
}

// Get VA performance history over multiple days
async function getVaPerformanceHistory(vaDiscordId, days) {
  var sql = "SELECT date, post_count, total_views, total_likes, total_comments, total_shares FROM daily_summaries WHERE va_discord_id = $1 AND date >= CURRENT_DATE - $2::integer ORDER BY date ASC";
  var result = await pool.query(sql, [vaDiscordId, days]);
  return result.rows;
}

// Check if post just went viral (for alerts)
async function checkViralPosts() {
  var sql = "SELECT p.*, s.views, s.likes, s.comments FROM posts p LEFT JOIN LATERAL (SELECT views, likes, comments FROM snapshots sn WHERE sn.post_id = p.id ORDER BY sn.scraped_at DESC LIMIT 1) s ON true WHERE p.status = 'active' AND p.performance != 'viral' AND COALESCE(s.views, 0) >= $1";
  var result = await pool.query(sql, [VIRAL_VIEWS]);
  for (var i = 0; i < result.rows.length; i++) {
    await pool.query("UPDATE posts SET performance = 'viral' WHERE id = $1", [result.rows[i].id]);
  }
  return result.rows;
}

// Get saved best posts (to repost) - posts with bon or viral performance
async function getSavedBestPosts(limit) {
  var sql = "SELECT p.id, p.ig_post_id, p.url, p.va_name, p.va_discord_id, p.created_at, p.performance, p.caption, s.views, s.likes, s.comments, s.shares FROM posts p LEFT JOIN LATERAL (SELECT views, likes, comments, shares FROM snapshots sn WHERE sn.post_id = p.id ORDER BY sn.scraped_at DESC LIMIT 1) s ON true WHERE p.performance IN ('viral', 'bon') ORDER BY COALESCE(s.views, 0) DESC LIMIT $1";
  var result = await pool.query(sql, [limit || 50]);
  return result.rows;
}

// Get nuggets: top posts by views (best performers to scale/repost)
async function getNuggets(date) {
  var sql = "SELECT p.id, p.ig_post_id, p.url, p.va_name, p.va_discord_id, p.created_at, p.caption, p.performance, s.views, s.likes, s.comments, s.shares FROM posts p LEFT JOIN LATERAL (SELECT views, likes, comments, shares FROM snapshots sn WHERE sn.post_id = p.id ORDER BY sn.scraped_at DESC LIMIT 1) s ON true WHERE p.created_at::date = $1 AND COALESCE(s.views, 0) > 0 ORDER BY COALESCE(s.views, 0) DESC LIMIT 15";
  var result = await pool.query(sql, [date]);
  return result.rows;
}

// Get recommendations data for a date
async function getRecommendations(date) {
  // Posts to repost (bon/viral from today)
  var repostSql = "SELECT p.id, p.ig_post_id, p.url, p.va_name, p.caption, s.views, s.likes, s.comments, s.shares FROM posts p LEFT JOIN LATERAL (SELECT views, likes, comments, shares FROM snapshots sn WHERE sn.post_id = p.id ORDER BY sn.scraped_at DESC LIMIT 1) s ON true WHERE p.created_at::date = $1 AND COALESCE(s.views, 0) >= $2 ORDER BY COALESCE(s.views, 0) DESC LIMIT 10";
  var repost = await pool.query(repostSql, [date, BON_VIEWS]);

  // Underperforming VAs (below average)
  var summaries = await getDailySummaries(date);
  var totalAvg = 0;
  if (summaries.length > 0) {
    var totalViews = summaries.reduce(function(a, b) { return a + Number(b.total_views); }, 0);
    totalAvg = totalViews / summaries.length;
  }
  var underperformers = summaries.filter(function(s) { return Number(s.total_views) < totalAvg * 0.5; });
  var topPerformers = summaries.filter(function(s) { return Number(s.total_views) >= totalAvg * 1.5; });

  // KPIs
  var allPostsSql = "SELECT p.id, p.performance, s.views FROM posts p LEFT JOIN LATERAL (SELECT views FROM snapshots sn WHERE sn.post_id = p.id ORDER BY sn.scraped_at DESC LIMIT 1) s ON true WHERE p.created_at::date = $1";
  var allPosts = await pool.query(allPostsSql, [date]);
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

  // Nuggets
  var nuggets = await getNuggets(date);

  return {
    postsToRepost: repost.rows,
    nuggets: nuggets,
    underperformers: underperformers,
    topPerformers: topPerformers,
    kpis: { totalPosts: totalPosts, perfCount: perfCount, pctPerf: pctPerf, pctFlop: pctFlop, toRepost: toRepost },
  };
}

// Get performance stats by hour of day (for heatmap) over last N days
async function getHourlyPerformance(days) {
  var sql = "SELECT EXTRACT(HOUR FROM p.created_at AT TIME ZONE 'Europe/Paris')::int AS hour, COUNT(DISTINCT p.id) AS post_count, COALESCE(AVG(latest.views), 0)::int AS avg_views, COALESCE(AVG(latest.likes), 0)::int AS avg_likes, COALESCE(AVG(latest.comments), 0)::int AS avg_comments, CASE WHEN COALESCE(AVG(latest.views), 0) > 0 THEN ROUND((COALESCE(AVG(latest.likes), 0) + COALESCE(AVG(latest.comments), 0)) / GREATEST(AVG(latest.views), 1) * 100, 2) ELSE 0 END AS avg_engagement FROM posts p LEFT JOIN LATERAL (SELECT views, likes, comments FROM snapshots sn WHERE sn.post_id = p.id ORDER BY sn.scraped_at DESC LIMIT 1) latest ON true WHERE p.created_at >= NOW() - ($1 || ' days')::interval AND COALESCE(latest.views, 0) > 0 GROUP BY hour ORDER BY hour";
  var result = await pool.query(sql, [days]);
  return result.rows;
}

// Check posts that are ~1h old for coaching feedback
async function getPostsForCoaching() {
  var sql = "SELECT p.id, p.ig_post_id, p.url, p.va_name, p.va_discord_id, p.created_at, p.caption, s.views, s.likes, s.comments, s.shares FROM posts p LEFT JOIN LATERAL (SELECT views, likes, comments, shares FROM snapshots sn WHERE sn.post_id = p.id ORDER BY sn.scraped_at DESC LIMIT 1) s ON true WHERE p.status = 'active' AND p.created_at <= NOW() - INTERVAL '55 minutes' AND p.created_at >= NOW() - INTERVAL '75 minutes' AND NOT EXISTS (SELECT 1 FROM snapshots sn2 WHERE sn2.post_id = p.id AND sn2.scraped_at >= p.created_at + INTERVAL '50 minutes' AND sn2.error = 'coaching_sent') ORDER BY p.created_at ASC";
  var result = await pool.query(sql);
  return result.rows;
}

// Mark post as coaching sent (using error field as flag)
async function markCoachingSent(postId) {
  await pool.query("INSERT INTO snapshots (post_id, views, likes, comments, shares, error) VALUES ($1, 0, 0, 0, 0, 'coaching_sent')", [postId]);
}

// ===== STREAKS =====

// Update streak for a VA after daily summary
async function updateStreak(vaDiscordId, vaName, date) {
  var posts = await getVaPostsToday(vaDiscordId, date);
  var metObjective = posts.length >= 6;

  var existing = await pool.query('SELECT * FROM va_streaks WHERE va_discord_id = $1', [vaDiscordId]);

  if (existing.rows.length === 0) {
    // First time
    await pool.query('INSERT INTO va_streaks (va_discord_id, va_name, current_streak, best_streak, last_streak_date) VALUES ($1, $2, $3, $3, $4)', [vaDiscordId, vaName, metObjective ? 1 : 0, date]);
    return { current: metObjective ? 1 : 0, best: metObjective ? 1 : 0, isNew: true };
  }

  var streak = existing.rows[0];
  var lastDate = streak.last_streak_date ? new Date(streak.last_streak_date) : null;
  var currentDate = new Date(date);
  var dayDiff = lastDate ? Math.round((currentDate - lastDate) / (1000 * 60 * 60 * 24)) : 999;

  var newStreak, newBest;
  if (metObjective) {
    if (dayDiff === 1) {
      // Consecutive day
      newStreak = Number(streak.current_streak) + 1;
    } else {
      // Streak broken or first day
      newStreak = 1;
    }
    newBest = Math.max(newStreak, Number(streak.best_streak));
    await pool.query('UPDATE va_streaks SET current_streak = $1, best_streak = $2, last_streak_date = $3, va_name = $4, updated_at = NOW() WHERE va_discord_id = $5', [newStreak, newBest, date, vaName, vaDiscordId]);
  } else {
    newStreak = 0;
    newBest = Number(streak.best_streak);
    await pool.query('UPDATE va_streaks SET current_streak = 0, va_name = $1, updated_at = NOW() WHERE va_discord_id = $2', [vaName, vaDiscordId]);
  }

  return { current: newStreak, best: newBest, previous: Number(streak.current_streak), broken: !metObjective && Number(streak.current_streak) > 0 };
}

async function getAllStreaks() {
  var result = await pool.query('SELECT * FROM va_streaks ORDER BY current_streak DESC, best_streak DESC');
  return result.rows;
}

// ===== INACTIVITY DETECTION =====

// Get VAs who haven't posted in the last X hours
async function getInactiveVAs(hours) {
  var sql = "SELECT DISTINCT p.va_discord_id, p.va_name, MAX(p.created_at) AS last_post_at FROM posts p GROUP BY p.va_discord_id, p.va_name HAVING MAX(p.created_at) < NOW() - ($1 || ' hours')::interval ORDER BY MAX(p.created_at) ASC";
  var result = await pool.query(sql, [hours]);
  return result.rows;
}

// ===== PERFORMANCE DROP DETECTION =====

// Get VAs whose today performance is significantly lower than their 7-day average
async function getPerformanceDrops() {
  var today = new Date().toISOString().split('T')[0];
  var sql = "SELECT ds_today.va_discord_id, ds_today.va_name, ds_today.total_views AS today_views, avg_7d.avg_views, CASE WHEN avg_7d.avg_views > 0 THEN ROUND((ds_today.total_views::numeric / avg_7d.avg_views) * 100) ELSE 0 END AS pct_of_avg FROM daily_summaries ds_today JOIN (SELECT va_discord_id, ROUND(AVG(total_views)) AS avg_views FROM daily_summaries WHERE date >= CURRENT_DATE - 7 AND date < CURRENT_DATE GROUP BY va_discord_id HAVING AVG(total_views) > 0) avg_7d ON avg_7d.va_discord_id = ds_today.va_discord_id WHERE ds_today.date = $1 AND ds_today.total_views < avg_7d.avg_views * 0.5 ORDER BY (ds_today.total_views::numeric / GREATEST(avg_7d.avg_views, 1)) ASC";
  var result = await pool.query(sql, [today]);
  return result.rows;
}

module.exports = {
  pool: require('./init').pool,
  insertPost, getPost, getPostByIgId, getActivePosts,
  endTracking, setPostError, setManagerMsgId,
  insertSnapshot, getLatestSnapshot, getSnapshotHistory, getSnapshotAtHour, getPostMilestones,
  computeDailySummary, getDailySummaries, getVaDailyStats, getVaPostsToday,
  getLeaderboard, endExpiredPosts, getPostsForExport,
  getTopPostsWithPerformance, getVaPerformanceHistory, checkViralPosts,
  updatePostPerformance, getSavedBestPosts, getNuggets, getRecommendations,
  getHourlyPerformance, getPostsForCoaching, markCoachingSent,
  updateStreak, getAllStreaks, getInactiveVAs, getPerformanceDrops,
  VIRAL_VIEWS, BON_VIEWS, MOYEN_VIEWS,
};
