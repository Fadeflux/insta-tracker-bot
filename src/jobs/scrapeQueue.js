const { Queue, Worker } = require('bullmq');
const IORedis = require('ioredis');
const config = require('../../config');
const logger = require('../utils/logger');
const { scrapePost } = require('../scrapers/instagram');
const db = require('../db/queries');

const connection = new IORedis(config.redis.url, { maxRetriesPerRequest: null });

const scrapeQueue = new Queue('scrape', { connection });
const notifyQueue = new Queue('notify', { connection });

const scrapeWorker = new Worker(
  'scrape',
  async (job) => {
    const { postId, url } = job.data;
    logger.info('Scraping job for post ' + postId + ': ' + url);

    const post = await db.getPost(postId);
    if (!post || post.status !== 'active') {
      logger.info('Post ' + postId + ' no longer active, skipping');
      return;
    }

    if (new Date() > new Date(post.tracking_end)) {
      await db.endTracking(postId);
      logger.info('Post ' + postId + ' tracking ended (past deadline)');
      return;
    }

    const previousSnapshot = await db.getLatestSnapshot(postId);
    const stats = await scrapePost(url);

    if (stats.error) {
      await db.insertSnapshot(postId, stats);
      logger.warn('Scrape error for post ' + postId + ': ' + stats.error);
      return;
    }

    const snapshot = await db.insertSnapshot(postId, stats);

    await notifyQueue.add('hourly-update', {
      postId,
      currentStats: stats,
      previousStats: previousSnapshot
        ? { views: previousSnapshot.views, likes: previousSnapshot.likes, comments: previousSnapshot.comments, shares: previousSnapshot.shares }
        : null,
    });

    const nextScrape = new Date(Date.now() + 60 * 60 * 1000);
    if (nextScrape < new Date(post.tracking_end)) {
      await scrapeQueue.add('scrape-post', { postId, url }, { delay: 60 * 60 * 1000, jobId: 'scrape-' + postId + '-' + Date.now() });
    }

    return snapshot;
  },
  {
    connection,
    concurrency: config.scraping.concurrency,
    limiter: { max: 5, duration: 60000 },
  }
);

scrapeWorker.on('failed', (job, err) => { logger.error('Scrape job failed: ' + job?.id, { error: err.message }); });
scrapeWorker.on('completed', (job) => { logger.info('Scrape job completed: ' + job.id); });

async function scheduleInitialScrape(postId, url) {
  await scrapeQueue.add('scrape-post', { postId, url }, { jobId: 'scrape-' + postId + '-initial', delay: 5000 });
  logger.info('Scheduled initial scrape for post ' + postId);
}

async function getQueueStats() {
  const waiting = await scrapeQueue.getWaitingCount();
  const active = await scrapeQueue.getActiveCount();
  const delayed = await scrapeQueue.getDelayedCount();
  return { waiting, active, delayed };
}

module.exports = { scrapeQueue, notifyQueue, scrapeWorker, scheduleInitialScrape, getQueueStats };
