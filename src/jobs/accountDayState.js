// Account day state
// ─────────────────
// For each account, computes:
//   - the current "day J" the account is on (J1, J2, J3, ...)
//   - the state: 'normal' | 'shadowban_rest' | 'shadowban_rampup' | 'dead'
//   - the daily post objective (1 / 2 / 3, or 0 during shadowban rest)
//
// State machine for shadowban accounts:
//   D0       Bot detects shadowban → row inserted in account_shadowban_state
//   D1..D7   "Rest day" — VA shouldn't post (objective = 0)
//   D8       "Rampup J1" — 1 post
//   D9       "Rampup J2" — 2 posts
//   D10+     "Rampup J3+" — 3 posts (= back to normal pace)
//   D17+     We clear the shadowban row and the account is back to "normal"
//
// State machine for normal accounts:
//   J1       1 post (= day of first tracked post)
//   J2       2 posts
//   J3+      3 posts
//
// "Dead" accounts (5+ posts <100 views): objective = "stop posting", we tell
// the VA via a different message in the daily summary.
//
// Day calculation is based on the Bénin timezone (Africa/Porto-Novo) which
// is the operational timezone for the agency.

var logger = require('../utils/logger');

// Number of rest days after shadowban detection (D1..D7 inclusive)
var SHADOWBAN_REST_DAYS = 7;
// Number of rampup days after rest (J1..J3 → 3 days)
var SHADOWBAN_RAMPUP_DAYS = 3;
// After this many days post-shadowban, we clear the state entirely
var SHADOWBAN_CLEAR_AFTER_DAYS = SHADOWBAN_REST_DAYS + SHADOWBAN_RAMPUP_DAYS + 7; // = 17

// === Compute day state for ALL accounts in a single SQL pass ===
// Returns an array of objects with shape:
//   {
//     account_id, username, platform, va_discord_id, va_name,
//     state: 'normal'|'shadowban_rest'|'shadowban_rampup'|'dead'|'never_posted',
//     day_label: 'J5' | 'Repos J3' | 'Reprise J2' | 'Mort' | null,
//     objective: 0 | 1 | 2 | 3 | null,
//     reason: human-readable explanation
//   }
async function computeDailyState(db, options) {
  options = options || {};
  var platforms = options.platforms || ['instagram', 'geelark']; // only platforms with day-rules
  // Big query that fetches first-post date, override date, shadowban state,
  // and dead-account heuristic in one go. We avoid N+1 by joining everything.
  var sql =
    "SELECT a.id AS account_id, a.username, a.platform, a.va_discord_id, a.va_name, " +
    "       a.status, " +
    "       (a.created_at AT TIME ZONE 'Africa/Porto-Novo')::date AS account_created_date, " +
    "       fp.first_post_date, " +
    "       ovr.start_date AS override_start, " +
    "       sb.shadowban_at, " +
    "       (NOW() AT TIME ZONE 'Africa/Porto-Novo')::date AS today_benin, " +
    "       dead.failed_posts " +
    "FROM accounts a " +
    "LEFT JOIN account_day_overrides ovr ON ovr.account_id = a.id " +
    "LEFT JOIN account_shadowban_state sb ON sb.account_id = a.id " +
    "LEFT JOIN LATERAL (" +
    "  SELECT MIN((p.created_at AT TIME ZONE 'Africa/Porto-Novo')::date) AS first_post_date " +
    "  FROM posts p WHERE p.account_id = a.id AND p.deleted_at IS NULL" +
    ") fp ON true " +
    "LEFT JOIN LATERAL (" +
    "  SELECT COUNT(*) AS failed_posts FROM posts p " +
    "  LEFT JOIN LATERAL (" +
    "    SELECT views FROM snapshots s " +
    "    WHERE s.post_id = p.id AND COALESCE(s.error, '') <> 'coaching_sent' " +
    "    ORDER BY s.scraped_at DESC LIMIT 1" +
    "  ) latest ON true " +
    "  WHERE p.account_id = a.id AND p.deleted_at IS NULL " +
    "    AND p.created_at >= NOW() - INTERVAL '14 days' " +
    "    AND p.created_at <= NOW() - INTERVAL '24 hours' " +
    "    AND COALESCE(latest.views, 0) < 100" +
    ") dead ON true " +
    "WHERE a.status = 'active' AND a.va_discord_id IS NOT NULL " +
    "  AND a.platform = ANY($1::varchar[])";

  var rows;
  try {
    rows = (await db.pool.query(sql, [platforms])).rows;
  } catch (e) {
    logger.warn('[DayState] query failed: ' + e.message);
    return [];
  }

  // Compute state for each row in JS — easier to read than complex SQL CASE
  return rows.map(function(r) { return computeStateForRow(r); }).filter(Boolean);
}

// Compute state for a single account row. Pure function (no DB).
function computeStateForRow(r) {
  var today = r.today_benin; // YYYY-MM-DD as JS Date or string from PG
  // Convert PG dates (which arrive as JS Date in node-postgres) → 'YYYY-MM-DD' string
  function toDateStr(v) {
    if (!v) return null;
    if (typeof v === 'string') return v.slice(0, 10);
    if (v instanceof Date) {
      // Use UTC components since node-pg dates are at midnight UTC for DATE columns
      var y = v.getUTCFullYear();
      var m = String(v.getUTCMonth() + 1).padStart(2, '0');
      var d = String(v.getUTCDate()).padStart(2, '0');
      return y + '-' + m + '-' + d;
    }
    return null;
  }

  var todayStr = toDateStr(today);
  var firstPostStr = toDateStr(r.first_post_date);
  var overrideStr = toDateStr(r.override_start);
  var shadowbanStr = toDateStr(r.shadowban_at);
  var failedPosts = Number(r.failed_posts || 0);

  // Helper: difference in days between two YYYY-MM-DD strings, today - earlier.
  // Returns 0 if same day, 1 if today is the day after, etc.
  function dayDiff(later, earlier) {
    if (!later || !earlier) return null;
    var l = new Date(later + 'T00:00:00Z');
    var e = new Date(earlier + 'T00:00:00Z');
    return Math.round((l.getTime() - e.getTime()) / 86400000);
  }

  // === Step 1: shadowban path takes priority ===
  if (shadowbanStr) {
    var daysSinceBan = dayDiff(todayStr, shadowbanStr);
    if (daysSinceBan == null) daysSinceBan = 0;
    if (daysSinceBan <= SHADOWBAN_REST_DAYS) {
      // Day 1..7 of rest — actually the ban day is "D0" in our model so
      // "Repos J1" is the day right after detection.
      var restJ = Math.max(1, daysSinceBan); // D1 = J1 of rest
      return {
        account_id: r.account_id, username: r.username, platform: r.platform,
        va_discord_id: r.va_discord_id, va_name: r.va_name,
        state: 'shadowban_rest',
        day_label: 'Repos J' + restJ + ' apres shadowban',
        objective: 0,
        reason: 'Compte en repos shadowban (' + restJ + '/' + SHADOWBAN_REST_DAYS + ' jours).',
      };
    }
    var rampupJ = daysSinceBan - SHADOWBAN_REST_DAYS; // J1 = first post day after rest
    if (rampupJ <= SHADOWBAN_RAMPUP_DAYS) {
      var rampupObjective = Math.min(rampupJ, 3);
      return {
        account_id: r.account_id, username: r.username, platform: r.platform,
        va_discord_id: r.va_discord_id, va_name: r.va_name,
        state: 'shadowban_rampup',
        day_label: 'Reprise J' + rampupJ + ' apres shadowban',
        objective: rampupObjective,
        reason: 'Reprise progressive apres shadowban (' + rampupJ + '/' + SHADOWBAN_RAMPUP_DAYS + ' jours).',
      };
    }
    // After rampup, fall through to normal logic — the cron should clear
    // the shadowban_at row but we tolerate it being still there.
  }

  // === Step 2: dead-account path ===
  // We mark a normal-state account as "dead" only if at least 5 of its
  // recent posts (older than 24h) failed. This complements the alert-side
  // logic in ticketAccountAlerts.js.
  if (failedPosts >= 5) {
    return {
      account_id: r.account_id, username: r.username, platform: r.platform,
      va_discord_id: r.va_discord_id, va_name: r.va_name,
      state: 'dead',
      day_label: 'Mort',
      objective: 0,
      reason: failedPosts + ' posts a moins de 100 vues. Compte a abandonner.',
    };
  }

  // === Step 3: normal path ===
  // Day = days since first effective start date. We use the override if
  // provided, else the date of the first tracked post. If neither, the
  // account hasn't been used yet — no objective.
  var startDate = overrideStr || firstPostStr;
  if (!startDate) {
    return {
      account_id: r.account_id, username: r.username, platform: r.platform,
      va_discord_id: r.va_discord_id, va_name: r.va_name,
      state: 'never_posted',
      day_label: null,
      objective: null,
      reason: 'Compte ajoute mais aucun post tracke. J1 sera le jour du premier post.',
    };
  }
  // J1 = startDate, J2 = startDate + 1, ...
  var dayJ = dayDiff(todayStr, startDate) + 1;
  if (dayJ < 1) dayJ = 1; // future-dated override → treat as J1
  var objective;
  if (dayJ === 1) objective = 1;
  else if (dayJ === 2) objective = 2;
  else objective = 3;
  return {
    account_id: r.account_id, username: r.username, platform: r.platform,
    va_discord_id: r.va_discord_id, va_name: r.va_name,
    state: 'normal',
    day_label: 'J' + dayJ,
    objective: objective,
    reason: 'Compte normal au jour ' + dayJ + '. Objectif = ' + objective + ' post(s).',
  };
}

// === Mark an account as freshly shadowbanned ===
// Called from the shadowban detection hook in scrapeQueue. Idempotent:
// re-firing for an already-flagged account doesn't reset the shadowban_at
// (otherwise the rest period would restart at every detection).
async function markShadowban(db, accountId) {
  if (!accountId) return;
  try {
    await db.pool.query(
      "INSERT INTO account_shadowban_state (account_id, shadowban_at, detected_at) " +
      "VALUES ($1, (NOW() AT TIME ZONE 'Africa/Porto-Novo')::date, NOW()) " +
      "ON CONFLICT (account_id) DO NOTHING",
      [accountId]
    );
  } catch (e) {
    logger.warn('[DayState] markShadowban failed: ' + e.message);
  }
}

// === Clear shadowban state for accounts that have completed the full cycle ===
// Run from cron once a day. Removes rows older than (rest+rampup+buffer) days
// so the account returns to its natural day count.
async function clearOldShadowbanStates(db) {
  try {
    var r = await db.pool.query(
      "DELETE FROM account_shadowban_state " +
      "WHERE shadowban_at < (NOW() AT TIME ZONE 'Africa/Porto-Novo')::date - $1::int",
      [SHADOWBAN_CLEAR_AFTER_DAYS]
    );
    if (r.rowCount > 0) {
      logger.info('[DayState] cleared ' + r.rowCount + ' expired shadowban states');
    }
  } catch (e) {
    logger.warn('[DayState] clearOldShadowbanStates failed: ' + e.message);
  }
}

// === Set/update an override start date for an account (admin action) ===
async function setDayOverride(db, accountId, startDate, updatedBy) {
  if (!accountId) throw new Error('accountId required');
  await db.pool.query(
    "INSERT INTO account_day_overrides (account_id, start_date, updated_by, updated_at) " +
    "VALUES ($1, $2, $3, NOW()) " +
    "ON CONFLICT (account_id) DO UPDATE SET start_date = EXCLUDED.start_date, " +
    "  updated_by = EXCLUDED.updated_by, updated_at = NOW()",
    [accountId, startDate, updatedBy || 'admin']
  );
}

async function clearDayOverride(db, accountId) {
  await db.pool.query("DELETE FROM account_day_overrides WHERE account_id = $1", [accountId]);
}

module.exports = {
  computeDailyState: computeDailyState,
  markShadowban: markShadowban,
  clearOldShadowbanStates: clearOldShadowbanStates,
  setDayOverride: setDayOverride,
  clearDayOverride: clearDayOverride,
  // For testing
  computeStateForRow: computeStateForRow,
  SHADOWBAN_REST_DAYS: SHADOWBAN_REST_DAYS,
  SHADOWBAN_RAMPUP_DAYS: SHADOWBAN_RAMPUP_DAYS,
};
