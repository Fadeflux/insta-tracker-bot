// Inactivity alerts
// ────────────────
// Two flavours of alerts, both posted in the VA's ticket channel:
//
//   1. VA-LEVEL alert  — VA hasn't posted ANY content (across all platforms)
//      since N hours ago. Three escalating tiers: 24h / 48h / 72h. Replaces
//      the old `va_inactive` dashboard-only notification with a more visible
//      Discord ticket message.
//
//   2. ACCOUNT-LEVEL alert — VA has been posting (so they're active overall)
//      but some of their accounts have been ignored for ≥3 days. Useful to
//      surface accounts the VA may have implicitly given up on. Excludes
//      accounts in "rest" (shadowban) or "dead" state — those have their own
//      dedicated alerts.
//
// Idempotency: we re-notify only when the situation worsens (i.e. moving to
// a higher tier, or the same tier after the alert has aged out). We do NOT
// re-fire the same tier within 24h.

var config = require('../../config');
var logger = require('../utils/logger');

var discordClient = null;
function setDiscordClient(client) { discordClient = client; }

// Alert tiers (in hours since last post)
var VA_TIERS = [
  { hours: 24, severity: 1, key: 'tier1_24h' },
  { hours: 48, severity: 2, key: 'tier2_48h' },
  { hours: 72, severity: 3, key: 'tier3_72h' },
];

// Account-level threshold (in hours)
var ACCOUNT_INACTIVITY_HOURS = 72; // 3 days

// Skip alerting on VAs gone for >14 days — they probably left the agency
var ABANDONED_AFTER_HOURS = 14 * 24;

async function findVaTicketChannel(guild, vaUsername) {
  if (!guild || !vaUsername) return null;
  var target = String(vaUsername).toLowerCase().trim();
  if (target.startsWith('@')) target = target.slice(1);
  try {
    var found = guild.channels.cache.find(function(ch) {
      return ch && ch.type === 0 && ch.name && ch.name.toLowerCase() === target;
    });
    if (found) return found;
    var all = await guild.channels.fetch();
    var foundFresh = all.find(function(ch) {
      return ch && ch.type === 0 && ch.name && ch.name.toLowerCase() === target;
    });
    return foundFresh || null;
  } catch (e) { return null; }
}

// ─────────────────────────────────────────────────────────────────────────────
// VA-LEVEL: find VAs who haven't posted ANYWHERE since at least 24h
// ─────────────────────────────────────────────────────────────────────────────
async function findInactiveVAs(db) {
  var sql =
    "SELECT p.va_discord_id, MAX(p.va_name) AS va_name, " +
    "       MAX(p.created_at) AS last_post_at, " +
    "       EXTRACT(EPOCH FROM (NOW() - MAX(p.created_at))) / 3600 AS hours_since " +
    "FROM posts p " +
    "WHERE p.va_discord_id IS NOT NULL AND p.deleted_at IS NULL " +
    "GROUP BY p.va_discord_id " +
    "HAVING MAX(p.created_at) < NOW() - INTERVAL '24 hours' " +
    "   AND MAX(p.created_at) > NOW() - ($1 || ' hours')::interval";
  var r = await db.pool.query(sql, [ABANDONED_AFTER_HOURS]);
  return r.rows.map(function(row) {
    return {
      va_discord_id: row.va_discord_id,
      va_name: row.va_name,
      last_post_at: row.last_post_at,
      hours_since: Math.round(Number(row.hours_since)),
    };
  });
}

// ─────────────────────────────────────────────────────────────────────────────
// ACCOUNT-LEVEL: find accounts that haven't been posted on for ≥72h, EXCLUDING:
//   - accounts in shadowban rest (we expect them not to post)
//   - accounts marked dead
//   - accounts where the VA themselves is fully inactive (covered by VA-level)
//   - inactive/disabled accounts
// ─────────────────────────────────────────────────────────────────────────────
async function findInactiveAccounts(db) {
  var sql =
    "WITH va_activity AS (" +
    "  SELECT p.va_discord_id, MAX(p.created_at) AS last_post_at " +
    "  FROM posts p " +
    "  WHERE p.va_discord_id IS NOT NULL AND p.deleted_at IS NULL " +
    "  GROUP BY p.va_discord_id" +
    ") " +
    "SELECT a.id AS account_id, a.username, a.platform, a.va_discord_id, a.va_name, " +
    "       acc_last.last_post_at AS account_last_post_at, " +
    "       EXTRACT(EPOCH FROM (NOW() - acc_last.last_post_at)) / 3600 AS hours_since " +
    "FROM accounts a " +
    "LEFT JOIN account_shadowban_state sb ON sb.account_id = a.id " +
    "JOIN va_activity v ON v.va_discord_id = a.va_discord_id " +
    "LEFT JOIN LATERAL (" +
    "  SELECT MAX(p.created_at) AS last_post_at " +
    "  FROM posts p " +
    "  WHERE p.account_id = a.id AND p.deleted_at IS NULL" +
    ") acc_last ON true " +
    "WHERE a.status = 'active' " +
    "  AND a.va_discord_id IS NOT NULL " +
    "  AND sb.account_id IS NULL " + // not in shadowban rest
    "  AND v.last_post_at >= NOW() - INTERVAL '24 hours' " + // VA is active overall
    "  AND (" +
    "    acc_last.last_post_at IS NULL " + // never posted on this account
    "    OR acc_last.last_post_at < NOW() - INTERVAL '72 hours'" +
    "  ) " +
    "  AND a.created_at < NOW() - INTERVAL '72 hours' " + // skip brand-new accounts (let them get J1)
    "  AND (acc_last.last_post_at IS NULL OR acc_last.last_post_at > NOW() - INTERVAL '14 days')";
  var r = await db.pool.query(sql);
  // Group by VA so each VA gets ONE message listing all their inactive accounts
  var byVa = {};
  r.rows.forEach(function(row) {
    if (!byVa[row.va_discord_id]) {
      byVa[row.va_discord_id] = { va_discord_id: row.va_discord_id, va_name: row.va_name, accounts: [] };
    }
    byVa[row.va_discord_id].accounts.push({
      account_id: row.account_id,
      username: row.username,
      platform: row.platform,
      last_post_at: row.account_last_post_at,
      hours_since: row.account_last_post_at ? Math.round(Number(row.hours_since)) : null,
    });
  });
  return Object.values(byVa);
}

// ─────────────────────────────────────────────────────────────────────────────
// Idempotency: track which severity tier was last sent for each VA
// We piggyback on the existing post_viral_milestones_sent table is wrong here;
// inactivity is per-VA not per-post. We use a dedicated approach: a small table.
// To avoid yet another DDL, we'll use account_alerts_sent with kind='inactivity'
// keyed on a synthetic account_id = -1 (per VA via va_discord_id encoded into
// last_severity is messy)... actually, cleaner: we use account_alerts_sent for
// account-level alerts only, and rely on an in-memory cooldown for VA-level
// — combined with a daily cron, that gives us "max 1 VA-level alert per day".
// ─────────────────────────────────────────────────────────────────────────────

// Persistent VA-level dedup using va_alerts_sent table.
// Rule: send if (a) we never sent before, OR (b) severity is HIGHER than
// last, OR (c) >24h have passed since the last send at the same severity.
async function shouldSendVaAlert(db, vaId, severity) {
  try {
    var r = await db.pool.query(
      "SELECT last_severity, sent_at FROM va_alerts_sent WHERE va_discord_id = $1 AND kind = 'inactivity_va'",
      [vaId]
    );
    if (r.rows.length === 0) return true;
    var prev = r.rows[0];
    var prevSeverity = Number(prev.last_severity);
    if (severity > prevSeverity) return true;
    var hoursSincePrev = (Date.now() - new Date(prev.sent_at).getTime()) / 3600000;
    if (hoursSincePrev > 24) return true;
    return false;
  } catch (e) {
    logger.warn('[Inactivity] shouldSendVaAlert query failed: ' + e.message);
    return false;
  }
}

async function recordVaAlertSent(db, vaId, severity) {
  try {
    await db.pool.query(
      "INSERT INTO va_alerts_sent (va_discord_id, kind, last_severity, sent_at) " +
      "VALUES ($1, 'inactivity_va', $2, NOW()) " +
      "ON CONFLICT (va_discord_id, kind) DO UPDATE SET last_severity = EXCLUDED.last_severity, sent_at = NOW()",
      [vaId, severity]
    );
  } catch (e) {
    logger.warn('[Inactivity] recordVaAlertSent failed: ' + e.message);
  }
}

// ─────────────────────────────────────────────────────────────────────────────
// Build the message for VA-level alert based on severity tier
// ─────────────────────────────────────────────────────────────────────────────
function buildVaInactiveMessage(va, severity, mentions) {
  var hoursSince = va.hours_since;
  var dayCount = Math.floor(hoursSince / 24);
  var dayStr = dayCount + ' jour' + (dayCount > 1 ? 's' : '');

  if (severity === 1) {
    // 24h tier — gentle reminder, no manager ping
    return '😴 **Petit rappel**\n\n' +
      mentions.va + ' on a remarque que tu n\'as pas poste depuis ' + dayStr + '. Tout va bien ?\n\n' +
      'Si tu as un souci, fais signe a ton manager. Sinon, on compte sur toi pour reposter aujourd\'hui !';
  } else if (severity === 2) {
    // 48h tier — gets more serious, manager mention
    return '⚠️ **Attention — 2 jours sans poster**\n\n' +
      mentions.va + ' tu n\'as pas poste depuis ' + dayStr + ' sur aucun compte.\n\n' +
      mentions.leadership + ' — Le VA semble inactif depuis 2 jours, peut-etre besoin d\'un coup de main ou d\'une discussion ?';
  } else {
    // 72h+ tier — full alert
    return '🚨 **ALERTE — VA inactif depuis ' + dayStr + '** 🚨\n\n' +
      mentions.va + ' tu n\'as poste sur aucun de tes comptes depuis ' + dayStr + '. C\'est beaucoup.\n\n' +
      mentions.leadership + ' — Situation critique. Il faut contacter le VA en direct pour comprendre ce qu\'il se passe (vacances ? probleme technique ? probleme personnel ?).';
  }
}

// Build the message for account-level alert (VA has multiple inactive accounts)
function buildAccountInactiveMessage(vaGroup, mentions) {
  function fm(d) {
    if (!d) return 'jamais';
    var diff = Math.round((Date.now() - new Date(d).getTime()) / 3600000);
    var days = Math.floor(diff / 24);
    return days + ' jour' + (days > 1 ? 's' : '');
  }
  var lines = [];
  lines.push('📭 **Comptes negliges depuis 3 jours+**');
  lines.push('');
  lines.push(mentions.va + ' tu as poste recemment, mais ces comptes n\'ont rien recu depuis longtemps :');
  lines.push('');
  vaGroup.accounts.forEach(function(a) {
    var emoji = a.platform === 'instagram' ? '📸' : (a.platform === 'geelark' ? '📱' : (a.platform === 'twitter' ? '🐦' : '🧵'));
    var since = a.last_post_at ? 'pas poste depuis ' + fm(a.last_post_at) : '⚠️ jamais poste';
    lines.push('   • ' + emoji + ' @' + a.username + ' — ' + since);
  });
  lines.push('');
  lines.push('💡 Si certains comptes sont morts ou shadowban, tu peux les desactiver. Sinon, n\'oublie pas de poster aussi sur eux !');
  lines.push('');
  lines.push(mentions.leadership + ' — A surveiller : ces comptes sont peut-etre abandonnes.');
  return lines.join('\n');
}

// ─────────────────────────────────────────────────────────────────────────────
// Resolve VA's ticket channel + build mentions
// ─────────────────────────────────────────────────────────────────────────────
async function resolveTicket(vaDiscordId, vaName) {
  var platforms = config.getActivePlatforms();
  var guildOrder = [];
  ['instagram', 'geelark', 'twitter', 'threads'].forEach(function(plat) {
    var pc = platforms.find(function(p) { return p.name === plat; });
    if (pc && pc.guildId) guildOrder.push({ pc: pc, guildId: pc.guildId });
  });
  for (var i = 0; i < guildOrder.length; i++) {
    var g = guildOrder[i];
    try {
      var guild = await discordClient.guilds.fetch(g.guildId);
      var channel = await findVaTicketChannel(guild, vaName);
      if (!channel && vaDiscordId) {
        try {
          var member = await guild.members.fetch(vaDiscordId);
          if (member) channel = await findVaTicketChannel(guild, member.user.username);
        } catch (e) {}
      }
      if (channel) {
        // Build mentions using THIS platform's manager/team-leader role
        var vaMention = vaDiscordId ? ('<@' + vaDiscordId + '>') : ('@' + (vaName || 'VA'));
        var managerMention = g.pc.managerRoleId ? ('<@&' + g.pc.managerRoleId + '>') : '@manager';
        var teamLeaderMention = g.pc.teamLeaderRoleId ? (' <@&' + g.pc.teamLeaderRoleId + '>') : '';
        return {
          channel: channel,
          mentions: { va: vaMention, leadership: managerMention + teamLeaderMention },
        };
      }
    } catch (e) {}
  }
  return null;
}

// ─────────────────────────────────────────────────────────────────────────────
// Main: run all inactivity checks (called from cron)
// ─────────────────────────────────────────────────────────────────────────────
async function runInactivityChecks(db) {
  if (!discordClient) {
    logger.warn('[Inactivity] no discord client, skipping');
    return;
  }

  var vaSent = 0, vaSkipped = 0;
  var accountSent = 0, accountSkipped = 0;

  // ===== VA-LEVEL =====
  var inactiveVAs = await findInactiveVAs(db);
  for (var i = 0; i < inactiveVAs.length; i++) {
    var va = inactiveVAs[i];
    // Determine which tier the VA falls into (highest qualifying)
    var tier = null;
    for (var t = VA_TIERS.length - 1; t >= 0; t--) {
      if (va.hours_since >= VA_TIERS[t].hours) { tier = VA_TIERS[t]; break; }
    }
    if (!tier) continue; // shouldn't happen since findInactiveVAs filters at 24h

    if (!(await shouldSendVaAlert(db, va.va_discord_id, tier.severity))) {
      vaSkipped++;
      continue;
    }
    var ticket = await resolveTicket(va.va_discord_id, va.va_name);
    if (!ticket) {
      logger.info('[Inactivity] no ticket for VA ' + va.va_name + ' (' + va.va_discord_id + ')');
      await recordVaAlertSent(db, va.va_discord_id, tier.severity); // record anyway to avoid retry storm
      vaSkipped++;
      continue;
    }
    var content = buildVaInactiveMessage(va, tier.severity, ticket.mentions);
    try {
      await ticket.channel.send({ content: content, allowedMentions: { parse: ['users', 'roles'] } });
      logger.info('[Inactivity] VA-level tier=' + tier.severity + ' sent for ' + va.va_name + ' (' + va.hours_since + 'h) in #' + ticket.channel.name);
      await recordVaAlertSent(db, va.va_discord_id, tier.severity);
      vaSent++;
    } catch (e) {
      logger.warn('[Inactivity] send failed for ' + va.va_name + ': ' + e.message);
      vaSkipped++;
    }
    await new Promise(function(r) { setTimeout(r, 200); });
  }

  // ===== ACCOUNT-LEVEL =====
  // We send these for VAs who are otherwise active. Idempotency uses the
  // dedicated va_alerts_sent table so we don't re-send daily for the same
  // set of inactive accounts.
  var inactiveByVa = await findInactiveAccounts(db);
  for (var j = 0; j < inactiveByVa.length; j++) {
    var vaGroup = inactiveByVa[j];
    if (!vaGroup.accounts.length) continue;
    // We re-notify when the COMPOSITION of inactive accounts changes
    // (a new account joins the inactive list = bigger problem). We use a
    // monotonic hash that increases when count or specific account IDs change.
    var maxId = vaGroup.accounts.reduce(function(acc, a) { return Math.max(acc, a.account_id); }, 0);
    var hashSeverity = (vaGroup.accounts.length * 100000) + maxId;
    var prev;
    try {
      var rPrev = await db.pool.query(
        "SELECT last_severity, sent_at FROM va_alerts_sent WHERE va_discord_id = $1 AND kind = 'inactivity_accounts'",
        [vaGroup.va_discord_id]
      );
      prev = rPrev.rows[0];
    } catch (e) { prev = null; }
    if (prev) {
      var prevSeverity = Number(prev.last_severity);
      var prevSentMs = new Date(prev.sent_at).getTime();
      var hoursSincePrev = (Date.now() - prevSentMs) / 3600000;
      // Skip if same composition AND <24h since last
      if (prevSeverity === hashSeverity && hoursSincePrev < 24) {
        accountSkipped++;
        continue;
      }
    }
    var ticket2 = await resolveTicket(vaGroup.va_discord_id, vaGroup.va_name);
    if (!ticket2) {
      accountSkipped++;
      continue;
    }
    var content2 = buildAccountInactiveMessage(vaGroup, ticket2.mentions);
    try {
      await ticket2.channel.send({ content: content2, allowedMentions: { parse: ['users', 'roles'] } });
      logger.info('[Inactivity] account-level sent for ' + vaGroup.va_name + ' (' + vaGroup.accounts.length + ' accounts) in #' + ticket2.channel.name);
      try {
        await db.pool.query(
          "INSERT INTO va_alerts_sent (va_discord_id, kind, last_severity, sent_at) " +
          "VALUES ($1, 'inactivity_accounts', $2, NOW()) " +
          "ON CONFLICT (va_discord_id, kind) DO UPDATE SET last_severity = EXCLUDED.last_severity, sent_at = NOW()",
          [vaGroup.va_discord_id, hashSeverity]
        );
      } catch (e) { /* best effort */ }
      accountSent++;
    } catch (e) {
      logger.warn('[Inactivity] account-level send failed for ' + vaGroup.va_name + ': ' + e.message);
      accountSkipped++;
    }
    await new Promise(function(r) { setTimeout(r, 200); });
  }

  logger.info('[Inactivity] done. VA-level: ' + vaSent + ' sent / ' + vaSkipped + ' skipped. Account-level: ' + accountSent + ' sent / ' + accountSkipped + ' skipped.');
  return { vaSent: vaSent, vaSkipped: vaSkipped, accountSent: accountSent, accountSkipped: accountSkipped };
}

module.exports = {
  setDiscordClient: setDiscordClient,
  runInactivityChecks: runInactivityChecks,
  // Exposed for testing
  buildVaInactiveMessage: buildVaInactiveMessage,
  buildAccountInactiveMessage: buildAccountInactiveMessage,
};
