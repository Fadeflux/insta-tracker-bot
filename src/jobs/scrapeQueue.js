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

    // Report to crash-alerts subsystem. We treat "stats.error" as a hard failure.
    // Wrapped in try/catch with a no-op fallback because reporting must NEVER
    // crash the scrape worker — if Discord is down, scraping must keep going.
    var crashAlerts = null;
    try { crashAlerts = require('./crashAlerts'); } catch (e) {}

    if (stats.error) {
      // Hard failure → increment per-platform counter
      if (crashAlerts) {
        try { await crashAlerts.reportFailure(platform, stats.error); }
        catch (e) { logger.warn('[CrashAlert] reportFailure threw: ' + e.message); }
      }
      await db.insertSnapshot(postId, stats);
      logger.warn('[' + platform.toUpperCase() + '] Scrape error for post ' + postId + ': ' + stats.error);

      // === ACCOUNT DISABLED detection ===
      // If the error suggests the post (or the account) is gone — 404, "not found",
      // "page not available", "user not found" — fire a notif so the manager
      // knows the account got banned/disabled by the platform.
      try {
        var errLower = String(stats.error).toLowerCase();
        var isAccountDead =
          errLower.indexOf('404') !== -1 ||
          errLower.indexOf('not found') !== -1 ||
          errLower.indexOf('page not available') !== -1 ||
          errLower.indexOf('user not found') !== -1 ||
          errLower.indexOf('this account') !== -1 ||
          errLower.indexOf('cette page') !== -1 ||
          errLower.indexOf('compte introuvable') !== -1;
        if (isAccountDead && post.account_id) {
          var accUser = post.account_username || '?';
          await db.insertNotification(
            platform,
            'account_disabled',
            postId,
            post.va_name || '?',
            '⛔ Compte introuvable',
            '@' + accUser + ' (gere par ' + (post.va_name||'?') + ') retourne une erreur ' + (errLower.indexOf('404')!==-1?'404':'"not found"') + '. Le compte a peut-etre ete banni ou supprime.',
            null,
            { accountUsername: accUser, accountId: post.account_id, error: String(stats.error).substring(0,200) }
          );
          logger.info('[Notif] account_disabled for @' + accUser + ' (error: ' + errLower.substring(0,80) + ')');
        }
      } catch (adErr) {
        logger.warn('[Notif] account_disabled detection failed: ' + adErr.message);
      }

      return;
    }

    // Success path → reset counter + send recovery message if we were down
    if (crashAlerts) {
      try { await crashAlerts.reportSuccess(platform); }
      catch (e) { logger.warn('[CrashAlert] reportSuccess threw: ' + e.message); }
    }

    var snapshot = await db.insertSnapshot(postId, stats);

    // === IN-APP NOTIFICATIONS ===
    // Two events trigger a bell-icon notification on the dashboard:
    //   - 'viral_confirmed': post crosses the VIRAL_VIEWS threshold for the first
    //     time (we use the previous snapshot to detect the crossing edge).
    //   - 'fast_growth': post gained > 1000 views since the previous scrape AND
    //     the time elapsed is ≤ 75 minutes (so we approximate views/hour).
    // Wrapped in try/catch so a notif failure never breaks the scrape pipeline.
    try {
      var prevViews = previousSnapshot ? Number(previousSnapshot.views || 0) : 0;
      var curViews = Number(stats.views || 0);
      var viralThreshold = config.scoring && config.scoring.viralViews ? config.scoring.viralViews : 5000;
      var postUrl = url;
      var vaName = post.va_name || '?';
      var titleBase = (post.account_username ? '@' + post.account_username : vaName);

      // Edge: just crossed viral threshold
      if (curViews >= viralThreshold && prevViews < viralThreshold) {
        await db.insertNotification(
          platform,
          'viral_confirmed',
          postId,
          vaName,
          '🔥 Post viral confirmé !',
          titleBase + ' a atteint ' + curViews.toLocaleString('fr-FR') + ' vues sur ' + platform + '.',
          postUrl,
          { views: curViews, likes: stats.likes || 0, comments: stats.comments || 0 }
        );
        logger.info('[Notif] viral_confirmed for post ' + postId + ' (' + curViews + ' views)');
      }

      // Fast growth: significant jump in a short window
      if (previousSnapshot && previousSnapshot.scraped_at) {
        var elapsedMin = (Date.now() - new Date(previousSnapshot.scraped_at).getTime()) / 60000;
        var deltaViews = curViews - prevViews;
        // Heuristic: > 1000 vues gagnées AND délai ≤ 75 min (≈ 1 scrape horaire)
        // AND on est en jour 1 (pour ne pas spammer sur les vieux posts qui ont
        // un intervalle de 12h+).
        if (elapsedMin > 0 && elapsedMin <= 75 && deltaViews >= 1000) {
          var ageH = (Date.now() - new Date(post.created_at).getTime()) / 3600000;
          if (ageH < 24) {
            await db.insertNotification(
              platform,
              'fast_growth',
              postId,
              vaName,
              '🚀 Croissance rapide',
              titleBase + ' a gagné ' + deltaViews.toLocaleString('fr-FR') + ' vues en ' + Math.round(elapsedMin) + ' min (total: ' + curViews.toLocaleString('fr-FR') + ').',
              postUrl,
              { views: curViews, deltaViews: deltaViews, elapsedMin: Math.round(elapsedMin) }
            );
            logger.info('[Notif] fast_growth for post ' + postId + ' (+' + deltaViews + ' views)');
          }
        }
      }

      // === SHADOWBAN DETECTION ===
      // After each scrape, check if this account's last 3 posts (≥24h old) all
      // have <300 views. If so, fire a notification. The notif is dedup'd per
      // post_id so we don't spam — but a NEW notif fires when the VA posts again
      // and that new post also fails (because post_id is different).
      if (post.account_id) {
        try {
          var sb = await db.detectShadowbannedAccount(post.account_id);
          if (sb) {
            // We tie the notification to the latest post_id, so each new failed
            // post produces a new notif (the per-post dedup in insertNotification
            // prevents repeats for the same post).
            var sbTitle = '🚫 Compte probablement shadowbanné';
            var sbBody =
              '@' + sb.accountUsername +
              ' (gere par ' + sb.vaName + ') — ' + sb.failedCount + ' posts a moins de 300 vues. ' +
              'Il faut envisager de changer de compte.';
            await db.insertNotification(
              platform,
              'shadowban_suspected',
              sb.latestPostId,
              sb.vaName,
              sbTitle,
              sbBody,
              null, // no specific URL — the manager should look at the dashboard
              {
                accountUsername: sb.accountUsername,
                accountId: sb.accountId,
                vaDiscordId: sb.vaDiscordId,
                failedCount: sb.failedCount,
                postIds: sb.postIds,
              }
            );
            logger.info('[Notif] shadowban_suspected for account @' + sb.accountUsername + ' (VA: ' + sb.vaName + ', ' + sb.failedCount + ' failed posts)');
          }
        } catch (sbErr) {
          logger.warn('[Notif] shadowban detection failed: ' + sbErr.message);
        }
      }
    } catch (notifErr) {
      logger.warn('[Notif] insert failed: ' + notifErr.message);
    }

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
    // we keep the hourly cadence over the 7-day tracking window.
    //
    // The jobId is built from the post id AND the scheduled execution time
    // (rounded to the minute). This makes it idempotent: if two scheduling
    // attempts produce the same target slot (e.g. resumeOrphanScrapes running
    // while the worker also reschedules), BullMQ rejects the second one and
    // we don't end up scraping the same post multiple times in the same minute.
    var nextScrapeDelayMs = computeNextScrapeDelay(post.created_at, platform);
    if (nextScrapeDelayMs != null) {
      var nextScrape = new Date(Date.now() + nextScrapeDelayMs);
      if (nextScrape < new Date(post.tracking_end)) {
        // Round target time to the nearest minute so close-but-not-identical
        // attempts collide on the same jobId.
        var slot = Math.floor((Date.now() + nextScrapeDelayMs) / 60000);
        var stableJobId = 'scrape-' + postId + '-slot-' + slot;
        try {
          await scrapeQueue.add(
            'scrape-post',
            { postId: postId, url: url, platform: platform },
            { delay: nextScrapeDelayMs, jobId: stableJobId }
          );
          logger.info('[' + platform.toUpperCase() + '] Next scrape for post ' + postId + ' in ' + Math.round(nextScrapeDelayMs / 60000) + ' min');
        } catch (e) {
          // BullMQ throws if the jobId already exists. That's the desired
          // "skip duplicate" behavior — log it quietly and move on.
          if (String(e.message).indexOf('exists') === -1) {
            logger.warn('[' + platform.toUpperCase() + '] Failed to schedule next scrape: ' + e.message);
          }
        }
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
// For Instagram/Twitter/Geelark: 7-day tracking window with decreasing frequency.
//   - Day 1 (0-24h):   every hour                    → 24 scrapes
//   - Day 2 (24-48h):  every 3 hours                 →  8 scrapes
//   - Day 3 (48-72h):  every 3 hours                 →  8 scrapes
//   - Days 4-7:        once per day at H+96, H+120, H+144, H+168 → 4 scrapes
//   Total: 44 scrapes per post over 7 days.
//
// Why 7 days: viral posts continue to gain views well past 72h. Calibration data
// showed the bot was capturing only ~85% of real views, mostly because viral
// posts kept growing after we stopped tracking. Extending to 7 days with
// progressively sparser scrapes lets us catch those late views without
// hammering the proxy: scrape rate goes from 1/h on day 1 down to 1/day at
// the end.
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

  // Build the schedule for IG/Twitter/Geelark over 7 days, decreasing cadence.
  var schedule = [];
  // Day 1 (1h..24h)
  for (var h = 1; h <= 24; h++) schedule.push(h * 60);
  // Day 2-3 (every 3h)
  for (var h2 = 27; h2 <= 72; h2 += 3) schedule.push(h2 * 60);
  // Day 4-7 (once per day)
  schedule.push(96 * 60);   // H+96  (day 4)
  schedule.push(120 * 60);  // H+120 (day 5)
  schedule.push(144 * 60);  // H+144 (day 6)
  schedule.push(168 * 60);  // H+168 (day 7)

  for (var k = 0; k < schedule.length; k++) {
    if (ageMin < schedule[k]) {
      return Math.round((schedule[k] - ageMin) * 60000);
    }
  }
  return null; // past H+168 (7 days), no more scrapes
}

async function scheduleInitialScrape(postId, url, platform) {
  platform = platform || 'instagram';
  await scrapeQueue.add('scrape-post', { postId: postId, url: url, platform: platform }, { jobId: 'scrape-' + postId + '-initial', delay: 5000 });
  logger.info('[' + platform.toUpperCase() + '] Scheduled initial scrape for post ' + postId);
}

// Called at bot startup. Looks at posts that should still be tracked (within
// their 7-day tracking window) and schedules a scrape for each one. Necessary
// because:
//   - Bull jobs aren't durable across redeploys reliably (depends on Redis state)
//   - Posts whose tracking_end was bumped by the migration need to resume scraping
//   - We want to recover gracefully from any missed scheduling
async function resumeOrphanScrapes() {
  try {
    var db2 = require('../db/queries');
    var result = await db2.pool.query(
      "SELECT id, url, platform, created_at FROM posts " +
      "WHERE deleted_at IS NULL " +
      "  AND tracking_end > NOW() " +
      "  AND created_at >= NOW() - INTERVAL '7 days' " +
      "ORDER BY created_at DESC"
    );

    if (result.rows.length === 0) {
      logger.info('[Scrape] No orphan posts to resume');
      return;
    }

    // STEP 1: Clean up any stale "resume" jobs from previous deploys.
    // Without this, every Railway redeploy stacked another batch of jobs on top
    // of the previous batch — the same post ended up being scraped 3-6 times in
    // a row at every cycle, hammering the proxy and risking IG bans.
    try {
      var delayed = await scrapeQueue.getJobs(['delayed', 'waiting']);
      var removed = 0;
      for (var k = 0; k < delayed.length; k++) {
        var j = delayed[k];
        if (j.id && j.id.indexOf('-resume-') !== -1) {
          await j.remove();
          removed++;
        }
      }
      if (removed > 0) logger.info('[Scrape] Cleared ' + removed + ' stale resume jobs before re-scheduling');
    } catch (cleanErr) {
      logger.warn('[Scrape] Failed to clean stale jobs: ' + cleanErr.message);
    }

    // STEP 2: Schedule one job per post, with a STABLE jobId based on the
    // target execution slot (post id + minute). This collides with the slots
    // used by the running worker, so if a job is already queued for the same
    // post and same minute, BullMQ rejects this one. No more duplicates.
    var resumed = 0, skipped = 0;
    for (var i = 0; i < result.rows.length; i++) {
      var p = result.rows[i];
      var nextDelay = computeNextScrapeDelay(p.created_at, p.platform);
      if (nextDelay == null) continue;
      var slot2 = Math.floor((Date.now() + nextDelay) / 60000);
      var jobId2 = 'scrape-' + p.id + '-slot-' + slot2;
      try {
        await scrapeQueue.add(
          'scrape-post',
          { postId: p.id, url: p.url, platform: p.platform },
          { delay: nextDelay, jobId: jobId2 }
        );
        resumed++;
      } catch (e) {
        // Job for this slot already exists → skip silently
        skipped++;
      }
    }
    logger.info('[Scrape] Resumed scraping for ' + resumed + ' orphan posts (window: 7d)' + (skipped ? ', ' + skipped + ' skipped (already queued)' : ''));
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
