const { Queue, Worker } = require('bullmq');
const IORedis = require('ioredis');
const config = require('../../config');
const logger = require('../utils/logger');
const { scrapePost } = require('../scrapers/instagram');
const { scrapeTweet } = require('../scrapers/twitter');
const { scrapePost: scrapeThreadsPost } = require('../scrapers/threads');
const db = require('../db/queries');

var connection = new IORedis(config.redis.url, { maxRetriesPerRequest: null });

var scrapeQueue = new Queue('scrape', { connection: connection });
var notifyQueue = new Queue('notify', { connection: connection });

// Route to the right scraper based on platform
function scrapeByPlatform(url, platform) {
  if (platform === 'twitter') return scrapeTweet(url);
  if (platform === 'threads') return scrapeThreadsPost(url);
  // instagram AND geelark both use the Instagram scraper (geelark posts ARE IG posts)
  return scrapePost(url);
}

var scrapeWorker = new Worker(
  'scrape',
  async function(job) {
    var postId = job.data.postId;
    var url = job.data.url;
    var platform = job.data.platform || 'instagram';
    logger.info('[' + platform.toUpperCase() + '] Scraping job for post ' + postId + ': ' + url);

    var post = await db.getPost(postId);
    if (!post || post.status !== 'active') {
      logger.info('Post ' + postId + ' no longer active, skipping');
      return;
    }

    if (new Date() > new Date(post.tracking_end)) {
      await db.endTracking(postId);
      logger.info('Post ' + postId + ' tracking ended (past deadline)');
      return;
    }

    var previousSnapshot = await db.getLatestSnapshot(postId);
    var stats = await scrapeByPlatform(url, platform);

    if (stats.error) {
      await db.insertSnapshot(postId, stats);
      logger.warn('[' + platform.toUpperCase() + '] Scrape error for post ' + postId + ': ' + stats.error);
      return;
    }

    var snapshot = await db.insertSnapshot(postId, stats);

    await notifyQueue.add('hourly-update', {
      postId: postId,
      currentStats: stats,
      previousStats: previousSnapshot
        ? { views: previousSnapshot.views, likes: previousSnapshot.likes, comments: previousSnapshot.comments, shares: previousSnapshot.shares }
        : null,
      platform: platform,
    });

    // === Schedule next scrape ===
    // For Threads, we use a sparser schedule (H+1, H+6, H+12, H+24) to spare the
    // sacrificial accounts from getting banned too quickly. For other platforms,
    // we keep the hourly cadence.
    var nextScrapeDelayMs = computeNextScrapeDelay(post.created_at, platform);
    if (nextScrapeDelayMs != null) {
      var nextScrape = new Date(Date.now() + nextScrapeDelayMs);
      if (nextScrape < new Date(post.tracking_end)) {
        await scrapeQueue.add('scrape-post', { postId: postId, url: url, platform: platform }, { delay: nextScrapeDelayMs, jobId: 'scrape-' + postId + '-' + Date.now() });
        logger.info('[' + platform.toUpperCase() + '] Next scrape for post ' + postId + ' in ' + Math.round(nextScrapeDelayMs / 60000) + ' min');
      }
    }

    return snapshot;
  },
  {
    connection: connection,
    concurrency: config.scraping.concurrency,
    limiter: { max: 5, duration: 60000 },
  }
);

scrapeWorker.on('failed', function(job, err) { logger.error('Scrape job failed: ' + (job ? job.id : ''), { error: err.message }); });
scrapeWorker.on('completed', function(job) { logger.info('Scrape job completed: ' + job.id); });

// Decide when the next scrape should happen for a given post.
// Returns the delay in milliseconds, or null if no more scrapes are needed.
//
// For Threads: 4 scrapes total at H+1, H+6, H+12, H+24 (spares the API accounts).
// For Instagram/Twitter/Geelark: 72h tracking window.
//   - Day 1 (0-24h): hourly (24 scrapes)
//   - Day 2 (24-48h): twice (at H+36 and H+48)
//   - Day 3 (48-72h): twice (at H+60 and H+72)
//   Total: 28 scrapes per post over 3 days.
function computeNextScrapeDelay(postCreatedAt, platform) {
  var ageMs = Date.now() - new Date(postCreatedAt).getTime();
  var ageMin = ageMs / 60000;

  if (platform === 'threads') {
    // Sparser schedule for Threads to spare the sacrificial accounts
    var threadsSchedule = [60, 360, 720, 1440]; // H+1, H+6, H+12, H+24
    for (var i = 0; i < threadsSchedule.length; i++) {
      if (ageMin < threadsSchedule[i]) {
        return Math.round((threadsSchedule[i] - ageMin) * 60000);
      }
    }
    return null;
  }

  // Build the schedule for IG/Twitter/Geelark:
  //   Day 1: every hour from H+1 to H+24 → [60, 120, 180, ..., 1440]
  //   Day 2: H+36 (2160) and H+48 (2880)
  //   Day 3: H+60 (3600) and H+72 (4320)
  var schedule = [];
  for (var h = 1; h <= 24; h++) schedule.push(h * 60);
  schedule.push(36 * 60); // 2160
  schedule.push(48 * 60); // 2880
  schedule.push(60 * 60); // 3600
  schedule.push(72 * 60); // 4320

  for (var k = 0; k < schedule.length; k++) {
    if (ageMin < schedule[k]) {
      return Math.round((schedule[k] - ageMin) * 60000);
    }
  }
  return null; // past H+72, no more scrapes
}

async function scheduleInitialScrape(postId, url, platform) {
  platform = platform || 'instagram';
  await scrapeQueue.add('scrape-post', { postId: postId, url: url, platform: platform }, { jobId: 'scrape-' + postId + '-initial', delay: 5000 });
  logger.info('[' + platform.toUpperCase() + '] Scheduled initial scrape for post ' + postId);
}

// Called at bot startup. Looks at posts that should still be tracked (within their
// 72h window) and schedules a scrape for each one. Necessary because:
//   - Bull jobs aren't durable across redeploys reliably (depends on Redis state)
//   - Posts whose tracking_end was bumped by the migration need to resume scraping
//   - We want to recover gracefully from any missed scheduling
async function resumeOrphanScrapes() {
  try {
    var pool = require('../db/queries').pool;
    if (!pool) {
      // Fallback: load directly via the db module if pool isn't exposed
      var db = require('../db/queries');
      pool = db.pool;
    }
    var db2 = require('../db/queries');
    var result = await db2.pool.query(
      "SELECT id, url, platform, created_at FROM posts " +
      "WHERE deleted_at IS NULL " +
      "  AND tracking_end > NOW() " +
      "  AND created_at >= NOW() - INTERVAL '72 hours' " +
      "ORDER BY created_at DESC"
    );

    if (result.rows.length === 0) {
      logger.info('[Scrape] No orphan posts to resume');
      return;
    }

    var resumed = 0;
    for (var i = 0; i < result.rows.length; i++) {
      var p = result.rows[i];
      var nextDelay = computeNextScrapeDelay(p.created_at, p.platform);
      if (nextDelay == null) continue;
      try {
        await scrapeQueue.add(
          'scrape-post',
          { postId: p.id, url: p.url, platform: p.platform },
          { delay: nextDelay, jobId: 'scrape-' + p.id + '-resume-' + Date.now() }
        );
        resumed++;
      } catch (e) {
        // Job might already exist — ignore
      }
    }
    logger.info('[Scrape] Resumed scraping for ' + resumed + ' orphan posts (window: 72h)');
  } catch (e) {
    logger.warn('[Scrape] resumeOrphanScrapes failed: ' + e.message);
  }
}

async function getQueueStats() {
  var waiting = await scrapeQueue.getWaitingCount();
  var active = await scrapeQueue.getActiveCount();
  var delayed = await scrapeQueue.getDelayedCount();
  return { waiting: waiting, active: active, delayed: delayed };
}

module.exports = { scrapeQueue: scrapeQueue, notifyQueue: notifyQueue, scrapeWorker: scrapeWorker, scheduleInitialScrape: scheduleInitialScrape, resumeOrphanScrapes: resumeOrphanScrapes, getQueueStats: getQueueStats };
