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
// For others (IG/Twitter/Geelark): hourly scraping for ~24h.
function computeNextScrapeDelay(postCreatedAt, platform) {
  if (platform === 'threads') {
    var ageMs = Date.now() - new Date(postCreatedAt).getTime();
    var ageMin = ageMs / 60000;
    // Schedule (in minutes from creation): 60 (H+1), 360 (H+6), 720 (H+12), 1440 (H+24)
    var schedule = [60, 360, 720, 1440];
    for (var i = 0; i < schedule.length; i++) {
      if (ageMin < schedule[i]) {
        return Math.round((schedule[i] - ageMin) * 60000);
      }
    }
    return null; // past H+24, no more scrapes
  }
  // Default: 1 hour for all other platforms
  return 60 * 60 * 1000;
}

async function scheduleInitialScrape(postId, url, platform) {
  platform = platform || 'instagram';
  await scrapeQueue.add('scrape-post', { postId: postId, url: url, platform: platform }, { jobId: 'scrape-' + postId + '-initial', delay: 5000 });
  logger.info('[' + platform.toUpperCase() + '] Scheduled initial scrape for post ' + postId);
}

async function getQueueStats() {
  var waiting = await scrapeQueue.getWaitingCount();
  var active = await scrapeQueue.getActiveCount();
  var delayed = await scrapeQueue.getDelayedCount();
  return { waiting: waiting, active: active, delayed: delayed };
}

module.exports = { scrapeQueue: scrapeQueue, notifyQueue: notifyQueue, scrapeWorker: scrapeWorker, scheduleInitialScrape: scheduleInitialScrape, getQueueStats: getQueueStats };
