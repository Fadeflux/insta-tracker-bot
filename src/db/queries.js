const { pool } = require('./init');
const logger = require('../utils/logger');

async function insertPost({ igPostId, url, vaDiscordId, vaName }) {
  var sql = "INSERT INTO posts (ig_post_id, url, va_discord_id, va_name, tracking_end) VALUES ($1, $2, $3, $4, (DATE_TRUNC('day', NOW() AT TIME ZONE 'Europe/Paris') + INTERVAL '23 hours 59 minutes') AT TIME ZONE 'Europe/Paris') ON CONFLICT (ig_post_id) DO NOTHING RETURNING *";
  var result = await pool.query(sql, [igPostId, url, vaDiscordId, vaName]);
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

async function insertSnapshot(postId, stats) {
  var sql = 'INSERT INTO snapshots (post_id, views, likes, comments, shares, error) VALUES ($1, $2, $3, $4, $5, $6) RETURNING *';
  var result = await pool.query(sql, [postId, stats.views || 0, stats.likes || 0, stats.comments || 0, stats.shares || 0, stats.error || null]);
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
  var sql = "SELECT p.va_name, p.url, p.ig_post_id, p.created_at, p.status, s.views, s.likes, s.comments, s.shares, s.scraped_at FROM posts p JOIN snapshots s ON s.post_id = p.id WHERE p.created_at::date = $1 ORDER BY p.va_name, p.created_at, s.scraped_at";
  var result = await pool.query(sql, [date]);
  return result.rows;
}

module.exports = { pool: require('./init').pool, insertPost, getPost, getPostByIgId, getActivePosts, endTracking, setPostError, setManagerMsgId, insertSnapshot, getLatestSnapshot, getSnapshotHistory, computeDailySummary, getDailySummaries, getVaDailyStats, getVaPostsToday, getLeaderboard, endExpiredPosts, getPostsForExport };

