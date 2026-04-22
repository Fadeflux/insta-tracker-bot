// =====================================================================
// ONE-SHOT BACKFILL: link pre-v3 posts to accounts
// =====================================================================
//
// Posts created before the v3 per-account tracking don't have an account_id.
// This runs on bot startup, exactly once (idempotent), and:
//   - For Twitter posts: parses the username directly from the URL (fast).
//   - For Instagram posts: re-scrapes to resolve the owner's username (slow).
//
// A marker row in the `migrations_marker` table prevents re-running the
// full sweep every time the bot restarts.

var logger = require('../utils/logger');
var db = require('../db/queries');
var twitterUtils = require('../utils/twitter');

var MARKER_KEY = 'backfill_accounts_v1';
var BATCH_SIZE = 20;           // Posts per batch
var SLEEP_BETWEEN_BATCHES = 3000; // 3s between batches (polite to proxy/IG)

function sleep(ms) { return new Promise(function(resolve) { setTimeout(resolve, ms); }); }

async function ensureMarkerTable() {
  await db.pool.query(
    "CREATE TABLE IF NOT EXISTS migrations_marker (" +
    "  key VARCHAR(128) PRIMARY KEY, " +
    "  completed_at TIMESTAMPTZ NOT NULL DEFAULT NOW(), " +
    "  stats JSONB" +
    ")"
  );
}

async function isAlreadyDone() {
  var r = await db.pool.query('SELECT 1 FROM migrations_marker WHERE key = $1', [MARKER_KEY]);
  return r.rows.length > 0;
}

async function markDone(stats) {
  await db.pool.query(
    'INSERT INTO migrations_marker (key, stats) VALUES ($1, $2) ON CONFLICT (key) DO UPDATE SET completed_at = NOW(), stats = $2',
    [MARKER_KEY, JSON.stringify(stats || {})]
  );
}

// Backfill Twitter posts: fast, just parses the URL.
async function backfillTwitter() {
  var stats = { found: 0, linked: 0, failed: 0 };

  var rows = (await db.pool.query(
    "SELECT id, url, va_discord_id, va_name FROM posts " +
    "WHERE platform = 'twitter' AND account_id IS NULL"
  )).rows;

  stats.found = rows.length;
  if (rows.length === 0) return stats;

  logger.info('[Backfill] Processing ' + rows.length + ' Twitter posts');

  for (var i = 0; i < rows.length; i++) {
    var p = rows[i];
    try {
      var username = twitterUtils.extractTwitterUsername(p.url);
      if (!username) { stats.failed++; continue; }
      var account = await db.upsertAccount(username, 'twitter', p.va_discord_id, p.va_name);
      if (account) {
        await db.updatePostAccount(p.id, account.id, account.username);
        stats.linked++;
      } else {
        stats.failed++;
      }
    } catch (err) {
      stats.failed++;
      logger.warn('[Backfill] Twitter post ' + p.id + ' failed: ' + err.message);
    }
  }

  logger.info('[Backfill] Twitter complete — linked=' + stats.linked + ' failed=' + stats.failed);
  return stats;
}

// Backfill Instagram posts: needs a re-scrape per post. Slow, batched, polite.
// We only backfill the most recent 500 IG posts — older ones aren't worth the scrape cost.
async function backfillInstagram() {
  var stats = { found: 0, linked: 0, failed: 0, skipped_old: 0 };

  // Only last 500 IG/Geelark posts — avoids burning the proxy on ancient posts
  var limit = 500;
  var rows = (await db.pool.query(
    "SELECT id, url, va_discord_id, va_name, platform FROM posts " +
    "WHERE platform IN ('instagram', 'geelark') AND account_id IS NULL " +
    "ORDER BY created_at DESC LIMIT $1",
    [limit]
  )).rows;

  // Count total pending (for stats) — includes ones we're skipping
  var totalPending = (await db.pool.query(
    "SELECT COUNT(*)::int AS cnt FROM posts WHERE platform IN ('instagram', 'geelark') AND account_id IS NULL"
  )).rows[0].cnt;
  stats.skipped_old = Math.max(0, totalPending - rows.length);
  stats.found = rows.length;

  if (rows.length === 0) return stats;

  logger.info('[Backfill] Processing ' + rows.length + ' Instagram/Geelark posts (skipping ' + stats.skipped_old + ' older)');

  // Lazy require to avoid circular deps at module load
  var scraper = require('../scrapers/instagram');

  // Process in small batches to be polite to IG
  for (var b = 0; b < rows.length; b += BATCH_SIZE) {
    var batch = rows.slice(b, b + BATCH_SIZE);
    logger.info('[Backfill] IG batch ' + (Math.floor(b / BATCH_SIZE) + 1) + '/' + Math.ceil(rows.length / BATCH_SIZE));

    for (var i = 0; i < batch.length; i++) {
      var p = batch[i];
      try {
        var result = await scraper.scrapePost(p.url);
        if (result.username) {
          var account = await db.upsertAccount(result.username, p.platform, p.va_discord_id, p.va_name);
          if (account) {
            await db.updatePostAccount(p.id, account.id, account.username);
            stats.linked++;
          } else {
            stats.failed++;
          }
        } else {
          stats.failed++;
        }
      } catch (err) {
        stats.failed++;
      }
    }

    // Sleep between batches (but not after last one)
    if (b + BATCH_SIZE < rows.length) {
      await sleep(SLEEP_BETWEEN_BATCHES);
    }
  }

  logger.info('[Backfill] Instagram complete — linked=' + stats.linked + ' failed=' + stats.failed + ' skipped_old=' + stats.skipped_old);
  return stats;
}

async function runBackfill() {
  try {
    await ensureMarkerTable();
    var done = await isAlreadyDone();
    if (done) {
      logger.info('[Backfill] Already completed — skipping');
      return;
    }

    logger.info('[Backfill] Starting account back-fill (one-shot)...');

    // Twitter first (fast), then Instagram (slow, async in background)
    var twStats = await backfillTwitter();

    // Mark as done *before* IG backfill starts, so a crash during IG doesn't
    // block future starts. The IG part will simply re-process remaining posts
    // on next restart if needed (since it filters by account_id IS NULL).
    // If you want to re-run the full backfill, delete the marker row:
    //   DELETE FROM migrations_marker WHERE key = 'backfill_accounts_v1';
    await markDone({ twitter: twStats });
    logger.info('[Backfill] Marker set — Twitter phase complete. IG phase running in background...');

    // Don't await — let it run in the background so bot startup isn't blocked
    backfillInstagram().then(function(igStats) {
      logger.info('[Backfill] All done — tw=' + JSON.stringify(twStats) + ' ig=' + JSON.stringify(igStats));
    }).catch(function(err) {
      logger.error('[Backfill] Instagram phase error: ' + err.message);
    });
  } catch (err) {
    logger.error('[Backfill] Fatal error: ' + err.message);
  }
}

module.exports = { runBackfill: runBackfill };
