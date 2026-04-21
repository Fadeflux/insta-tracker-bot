const { Queue, Worker } = require('bullmq');
const IORedis = require('ioredis');
const config = require('../../config');
const logger = require('../utils/logger');
const { scrapePost } = require('../scrapers/instagram');
const { scrapeTweet } = require('../scrapers/twitter');
const db = require('../db/queries');

var connection = new IORedis(config.redis.url, { maxRetriesPerRequest: null });

var scrapeQueue = new Queue('scrape', { connection: connection });
var notifyQueue = new Queue('notify', { connection: connection });

// Route to the right scraper based on platform
function scrapeByPlatform(url, platform) {
  if (platform === 'twitter') return scrapeTweet(url);
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

    var nextScrape = new Date(Date.now() + 60 * 60 * 1000);
    if (nextScrape < new Date(post.tracking_end)) {
      await scrapeQueue.add('scrape-post', { postId: postId, url: url, platform: platform }, { delay: 60 * 60 * 1000, jobId: 'scrape-' + postId + '-' + Date.now() });
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
