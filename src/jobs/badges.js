// VA badges system
// ───────────────
// Awards Discord roles + emoji-augmented nicknames to VAs based on their
// performance achievements. Badges:
//
//   🏆 top1        — Top 1 of the monthly leaderboard
//   🔥 firstViral  — First post that crossed 5k views
//   🚀 viral10     — 10+ viral posts (>5k views) in the calendar month
//   💎 regularity  — Posted at least once on every day of the past 30 days
//   💫 record100k  — Has at least one post with ≥100k views
//
// Badges are STICKY but expire if the VA earns no new badges for 14 days.
// "Top 1" is special: it changes every month — the previous month's winner
// loses the badge automatically when a new month's winner is crowned.
//
// We update both:
//   1. Discord roles assigned to the VA member in each guild
//   2. The VA's nickname in each guild (appending emoji badges to their name)
//
// Run via cron once per hour — that's frequent enough for "first viral" to
// fire quickly after a real-time event, without being too chatty.

var config = require('../../config');
var logger = require('../utils/logger');

var discordClient = null;
function setDiscordClient(client) { discordClient = client; }

// Badge metadata: emoji + role-config-key + display name
var BADGES = [
  { kind: 'top1',       emoji: '🏆', roleKey: 'top1',       label: 'Top 1 du mois',          monthlyReset: true  },
  { kind: 'firstViral', emoji: '🔥', roleKey: 'firstViral', label: 'Premier viral',          monthlyReset: false },
  { kind: 'viral10',    emoji: '🚀', roleKey: 'viral10',    label: '10+ viraux',             monthlyReset: true  },
  { kind: 'regularity', emoji: '💎', roleKey: 'regularity', label: 'Regularite 30j',         monthlyReset: false },
  { kind: 'record100k', emoji: '💫', roleKey: 'record100k', label: 'Record 100k+ vues',      monthlyReset: false },
];

// Number of days of inactivity (no new badge earned) before all badges expire
var BADGE_EXPIRY_DAYS = 14;

// Discord nickname max length (32 chars). We truncate the original name if
// adding badges would exceed the limit.
var DISCORD_NICKNAME_MAX = 32;

// ─────────────────────────────────────────────────────────────────────────────
// EVALUATE which badges each VA currently qualifies for
// ─────────────────────────────────────────────────────────────────────────────
async function evaluateAllBadges(db) {
  var awards = {}; // va_discord_id → { vaName, badges: Set('top1', ...) }

  // Helper: register a badge for a VA
  function award(vaId, vaName, kind) {
    if (!awards[vaId]) awards[vaId] = { vaName: vaName, badges: new Set() };
    awards[vaId].badges.add(kind);
  }

  // === firstViral: VA has at least one post >5k views ===
  // === record100k: VA has at least one post >100k views ===
  try {
    var vrSql =
      "SELECT p.va_discord_id, MAX(p.va_name) AS va_name, " +
      "       BOOL_OR(latest.views >= 5000) AS has_viral, " +
      "       BOOL_OR(latest.views >= 100000) AS has_100k " +
      "FROM posts p " +
      "LEFT JOIN LATERAL (" +
      "  SELECT views FROM snapshots s " +
      "  WHERE s.post_id = p.id AND COALESCE(s.error, '') <> 'coaching_sent' " +
      "  ORDER BY s.scraped_at DESC LIMIT 1" +
      ") latest ON true " +
      "WHERE p.deleted_at IS NULL AND p.va_discord_id IS NOT NULL " +
      "GROUP BY p.va_discord_id";
    (await db.pool.query(vrSql)).rows.forEach(function(r) {
      if (r.has_viral) award(r.va_discord_id, r.va_name, 'firstViral');
      if (r.has_100k) award(r.va_discord_id, r.va_name, 'record100k');
    });
  } catch (e) { logger.warn('[Badges] firstViral/record100k query failed: ' + e.message); }

  // === viral10: VA has 10+ posts >5k views in the current calendar month ===
  try {
    var v10Sql =
      "SELECT p.va_discord_id, MAX(p.va_name) AS va_name, " +
      "       COUNT(*) FILTER (WHERE latest.views >= 5000) AS viral_count " +
      "FROM posts p " +
      "LEFT JOIN LATERAL (" +
      "  SELECT views FROM snapshots s " +
      "  WHERE s.post_id = p.id AND COALESCE(s.error, '') <> 'coaching_sent' " +
      "  ORDER BY s.scraped_at DESC LIMIT 1" +
      ") latest ON true " +
      "WHERE p.deleted_at IS NULL AND p.va_discord_id IS NOT NULL " +
      "  AND DATE_TRUNC('month', p.created_at AT TIME ZONE 'Africa/Porto-Novo') = " +
      "      DATE_TRUNC('month', NOW() AT TIME ZONE 'Africa/Porto-Novo') " +
      "GROUP BY p.va_discord_id " +
      "HAVING COUNT(*) FILTER (WHERE latest.views >= 5000) >= 10";
    (await db.pool.query(v10Sql)).rows.forEach(function(r) {
      award(r.va_discord_id, r.va_name, 'viral10');
    });
  } catch (e) { logger.warn('[Badges] viral10 query failed: ' + e.message); }

  // === regularity: VA has posted on every day of the past 30 days ===
  // We compute the count of distinct days they posted on, and require == 30.
  try {
    var regSql =
      "SELECT p.va_discord_id, MAX(p.va_name) AS va_name, " +
      "       COUNT(DISTINCT (p.created_at AT TIME ZONE 'Africa/Porto-Novo')::date) AS unique_days " +
      "FROM posts p " +
      "WHERE p.deleted_at IS NULL AND p.va_discord_id IS NOT NULL " +
      "  AND p.created_at >= NOW() AT TIME ZONE 'Africa/Porto-Novo' - INTERVAL '30 days' " +
      "GROUP BY p.va_discord_id " +
      "HAVING COUNT(DISTINCT (p.created_at AT TIME ZONE 'Africa/Porto-Novo')::date) >= 30";
    (await db.pool.query(regSql)).rows.forEach(function(r) {
      award(r.va_discord_id, r.va_name, 'regularity');
    });
  } catch (e) { logger.warn('[Badges] regularity query failed: ' + e.message); }

  // === top1: VA with the most views in the current calendar month ===
  // Single VA per month gets it.
  try {
    var topSql =
      "SELECT p.va_discord_id, MAX(p.va_name) AS va_name, " +
      "       SUM(latest.views) AS total_views " +
      "FROM posts p " +
      "LEFT JOIN LATERAL (" +
      "  SELECT views FROM snapshots s " +
      "  WHERE s.post_id = p.id AND COALESCE(s.error, '') <> 'coaching_sent' " +
      "  ORDER BY s.scraped_at DESC LIMIT 1" +
      ") latest ON true " +
      "WHERE p.deleted_at IS NULL AND p.va_discord_id IS NOT NULL " +
      "  AND DATE_TRUNC('month', p.created_at AT TIME ZONE 'Africa/Porto-Novo') = " +
      "      DATE_TRUNC('month', NOW() AT TIME ZONE 'Africa/Porto-Novo') " +
      "GROUP BY p.va_discord_id " +
      "ORDER BY SUM(latest.views) DESC LIMIT 1";
    var topRows = (await db.pool.query(topSql)).rows;
    if (topRows.length > 0 && Number(topRows[0].total_views) > 0) {
      award(topRows[0].va_discord_id, topRows[0].va_name, 'top1');
    }
  } catch (e) { logger.warn('[Badges] top1 query failed: ' + e.message); }

  return awards;
}

// ─────────────────────────────────────────────────────────────────────────────
// PERSIST awards to DB and detect newly-earned badges (for the activity tracker)
// Returns { vaId → { vaName, currentBadges: Set, newlyEarned: [kinds] } }
// ─────────────────────────────────────────────────────────────────────────────
async function persistBadges(db, awards) {
  var summary = {};
  // For each VA in the awards map:
  //   1. Diff vs DB to find newly-earned + still-active badges
  //   2. Insert/update va_badges rows
  //   3. Update va_badge_activity if any new badge was earned
  var vaIds = Object.keys(awards);
  for (var i = 0; i < vaIds.length; i++) {
    var vaId = vaIds[i];
    var info = awards[vaId];

    // Get currently-stored badges for this VA
    var stored;
    try {
      stored = (await db.pool.query(
        "SELECT kind FROM va_badges WHERE va_discord_id = $1",
        [vaId]
      )).rows.map(function(r) { return r.kind; });
    } catch (e) { stored = []; }
    var storedSet = new Set(stored);

    var currentSet = info.badges; // Set of kinds the VA qualifies for now
    var newlyEarned = [];
    var keptBadges = [];

    // Determine new earnings (in current but not stored, or stored monthlyReset
    // badges which we always re-evaluate fresh each month).
    currentSet.forEach(function(kind) {
      if (!storedSet.has(kind)) newlyEarned.push(kind);
      else keptBadges.push(kind);
    });

    // Determine badges to remove (stored but no longer qualifying for monthlyReset
    // ones; permanent ones are not removed by re-evaluation, only by expiry).
    var toRemove = [];
    storedSet.forEach(function(kind) {
      var meta = BADGES.find(function(b) { return b.kind === kind; });
      if (!meta) { toRemove.push(kind); return; }
      if (meta.monthlyReset && !currentSet.has(kind)) {
        toRemove.push(kind);
      }
    });

    // Apply DB changes
    for (var n = 0; n < newlyEarned.length; n++) {
      try {
        await db.pool.query(
          "INSERT INTO va_badges (va_discord_id, kind, earned_at) VALUES ($1, $2, NOW()) " +
          "ON CONFLICT (va_discord_id, kind) DO UPDATE SET earned_at = NOW()",
          [vaId, newlyEarned[n]]
        );
      } catch (e) { logger.warn('[Badges] insert failed: ' + e.message); }
    }
    for (var r = 0; r < toRemove.length; r++) {
      try {
        await db.pool.query(
          "DELETE FROM va_badges WHERE va_discord_id = $1 AND kind = $2",
          [vaId, toRemove[r]]
        );
      } catch (e) { logger.warn('[Badges] delete failed: ' + e.message); }
    }

    // Bump activity timestamp if anything new was earned
    if (newlyEarned.length > 0) {
      try {
        await db.pool.query(
          "INSERT INTO va_badge_activity (va_discord_id, last_earned_at) VALUES ($1, NOW()) " +
          "ON CONFLICT (va_discord_id) DO UPDATE SET last_earned_at = NOW()",
          [vaId]
        );
      } catch (e) {}
    }

    summary[vaId] = {
      vaName: info.vaName,
      currentBadges: new Set(keptBadges.concat(newlyEarned).filter(function(b) {
        return !toRemove.includes(b);
      })),
      newlyEarned: newlyEarned,
      removed: toRemove,
    };
  }

  return summary;
}

// ─────────────────────────────────────────────────────────────────────────────
// EXPIRE all badges for VAs who haven't earned any badge in BADGE_EXPIRY_DAYS
// ─────────────────────────────────────────────────────────────────────────────
async function expireIdleBadges(db) {
  try {
    // Find VAs whose last_earned_at is too old AND who currently have badges
    var idleSql =
      "SELECT DISTINCT vb.va_discord_id FROM va_badges vb " +
      "LEFT JOIN va_badge_activity vba ON vba.va_discord_id = vb.va_discord_id " +
      "WHERE COALESCE(vba.last_earned_at, vb.earned_at) < NOW() - ($1 || ' days')::interval";
    var idleVas = (await db.pool.query(idleSql, [BADGE_EXPIRY_DAYS])).rows;
    if (idleVas.length === 0) return [];
    var ids = idleVas.map(function(r) { return r.va_discord_id; });
    await db.pool.query("DELETE FROM va_badges WHERE va_discord_id = ANY($1::text[])", [ids]);
    logger.info('[Badges] expired all badges for ' + ids.length + ' idle VAs (>' + BADGE_EXPIRY_DAYS + ' days)');
    return ids;
  } catch (e) {
    logger.warn('[Badges] expireIdleBadges failed: ' + e.message);
    return [];
  }
}

// ─────────────────────────────────────────────────────────────────────────────
// SYNC Discord roles + nicknames for a VA in a given guild
// ─────────────────────────────────────────────────────────────────────────────
async function syncVaInGuild(guild, vaDiscordId, currentBadges, platformConfig) {
  if (!guild || !vaDiscordId || !platformConfig) return { changed: false };
  var member;
  try { member = await guild.members.fetch(vaDiscordId); }
  catch (e) { return { changed: false, error: 'member-not-in-guild' }; }
  if (!member) return { changed: false, error: 'member-null' };

  var changed = false;

  // === Sync roles ===
  // For each badge, determine the role ID from platform config. Add roles
  // that should be present, remove badge roles that shouldn't.
  var badgeRoleMap = platformConfig.badgeRoles || {};
  var allBadgeRoleIds = Object.values(badgeRoleMap).filter(Boolean);

  for (var b = 0; b < BADGES.length; b++) {
    var meta = BADGES[b];
    var roleId = badgeRoleMap[meta.roleKey];
    if (!roleId) continue; // role not configured for this platform → skip

    var hasBadge = currentBadges.has(meta.kind);
    var hasRole = member.roles.cache.has(roleId);

    if (hasBadge && !hasRole) {
      try { await member.roles.add(roleId); changed = true; }
      catch (e) { logger.warn('[Badges] add role ' + meta.kind + ' failed for ' + member.user.tag + ': ' + e.message); }
    } else if (!hasBadge && hasRole) {
      try { await member.roles.remove(roleId); changed = true; }
      catch (e) { logger.warn('[Badges] remove role ' + meta.kind + ' failed for ' + member.user.tag + ': ' + e.message); }
    }
  }

  // === Sync nickname ===
  // We append the badge emojis to the VA's "base name". The base name is the
  // current nickname stripped of any badge emojis (so we can update cleanly).
  var currentNick = member.nickname || member.user.username;
  var baseName = stripBadgeEmojis(currentNick);
  var emojis = BADGES
    .filter(function(b) { return currentBadges.has(b.kind); })
    .map(function(b) { return b.emoji; })
    .join('');
  var targetNick = emojis ? (baseName + ' ' + emojis) : baseName;
  // Truncate if too long (Discord nickname limit = 32 chars)
  if (targetNick.length > DISCORD_NICKNAME_MAX) {
    var availableForBase = DISCORD_NICKNAME_MAX - emojis.length - 1; // -1 for the space
    if (availableForBase < 1) availableForBase = 1;
    targetNick = baseName.slice(0, availableForBase) + (emojis ? ' ' + emojis : '');
  }
  if (targetNick !== currentNick) {
    try { await member.setNickname(targetNick); changed = true; }
    catch (e) { logger.warn('[Badges] setNickname failed for ' + member.user.tag + ' in ' + guild.name + ': ' + e.message); }
  }

  return { changed: changed };
}

// Strip all known badge emojis from a name. Used to find the "base" name.
// We only strip our specific badge emojis (not any emoji), to avoid eating
// emojis the VA put there themselves.
function stripBadgeEmojis(name) {
  if (!name) return '';
  var stripped = name;
  BADGES.forEach(function(b) {
    // Replace " EMOJI" or "EMOJI" anywhere in the string
    stripped = stripped.split(b.emoji).join('');
  });
  // Collapse multiple spaces and trim
  return stripped.replace(/\s+/g, ' ').trim();
}

// ─────────────────────────────────────────────────────────────────────────────
// MAIN: evaluate, persist, expire, then sync Discord state for all VAs
// ─────────────────────────────────────────────────────────────────────────────
async function syncAllBadges(db) {
  if (!discordClient) {
    logger.warn('[Badges] no discord client, skipping');
    return;
  }
  // Step 1: expire idle VAs first so they're cleared before re-evaluating
  await expireIdleBadges(db);

  // Step 2: evaluate fresh awards
  var awards = await evaluateAllBadges(db);

  // Step 3: persist to DB and get the diff
  var summary = await persistBadges(db, awards);

  // Step 4: also handle VAs who lost ALL badges (e.g. expired or removed)
  // — they need their nickname/roles cleaned up. So we also fetch all VAs
  // who currently have ZERO badges from DB.
  var allKnownVas;
  try {
    allKnownVas = (await db.pool.query(
      "SELECT DISTINCT va_discord_id FROM posts WHERE va_discord_id IS NOT NULL"
    )).rows.map(function(r) { return r.va_discord_id; });
  } catch (e) { allKnownVas = []; }

  // Build a master list: every VA we know about, with their current badges
  // (from the awards summary, or empty Set if they have none).
  var allVaState = {}; // vaId → Set of kinds
  allKnownVas.forEach(function(vaId) { allVaState[vaId] = new Set(); });
  Object.keys(summary).forEach(function(vaId) {
    allVaState[vaId] = summary[vaId].currentBadges;
  });

  // Step 5: for each platform/guild, sync each VA
  var platforms = config.getActivePlatforms();
  var totalSynced = 0, totalChanged = 0;
  for (var p = 0; p < platforms.length; p++) {
    var pc = platforms[p];
    if (!pc.guildId) continue;
    var guild;
    try { guild = await discordClient.guilds.fetch(pc.guildId); }
    catch (e) { logger.warn('[Badges] guild fetch failed for ' + pc.name + ': ' + e.message); continue; }

    var vaIds2 = Object.keys(allVaState);
    for (var v = 0; v < vaIds2.length; v++) {
      var vaId = vaIds2[v];
      try {
        var result = await syncVaInGuild(guild, vaId, allVaState[vaId], pc);
        totalSynced++;
        if (result.changed) totalChanged++;
      } catch (e) {
        logger.warn('[Badges] sync failed for VA ' + vaId + ' in ' + pc.name + ': ' + e.message);
      }
      // Avoid Discord rate-limiting (50 req/s app-wide; we're well under).
      await new Promise(function(r) { setTimeout(r, 100); });
    }
  }

  logger.info('[Badges] sync done. checked=' + totalSynced + ', changed=' + totalChanged);
  return { synced: totalSynced, changed: totalChanged };
}

module.exports = {
  setDiscordClient: setDiscordClient,
  syncAllBadges: syncAllBadges,
  evaluateAllBadges: evaluateAllBadges,
  expireIdleBadges: expireIdleBadges,
  BADGES: BADGES,
};
