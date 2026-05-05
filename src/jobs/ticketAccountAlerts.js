// Ticket account alerts
// ─────────────────────
// Three alerts are fired into the VA's ticket channel when something is wrong
// with one of their accounts:
//
//   1. dead_account        — 5+ recent posts each <100 views → account isn't
//                            performing, suggest dropping it.
//   2. shadowban           — 3 latest posts (≥24h old) all <300 views → likely
//                            shadowbanned by the platform.
//   3. concentrated_views  — VA has 3+ accounts but ONE makes ≥80% of 7-day
//                            views → others may be worth replacing.
//
// Each alert is keyed on (account_id, kind) and we re-notify ONLY if severity
// gets worse — e.g. dead_account fires at 5 failed posts, but if it grows
// to 10 failed posts we re-notify (it's getting worse, manager should
// definitely act). For concentrated_views, the severity is the % of views
// on the dominant account.
//
// All alerts go in the VA's ticket channel (named after their Discord
// username — see ticketViralNotifier.js for the lookup logic).

var config = require('../../config');
var logger = require('../utils/logger');

// Shared Discord client wired at boot
var discordClient = null;
function setDiscordClient(client) { discordClient = client; }

// Severity bumps below the previous level shouldn't trigger re-notification.
// We only re-notify when the new severity exceeds previous + a small step
// to avoid spamming on every minor jump (e.g. 5 → 6 failed posts).
var SEVERITY_STEP = {
  dead_account: 3,        // +3 failed posts to re-notify (5 → 8 → 11 ...)
  shadowban: 2,           // +2 failed posts (3 → 5 → 7 ...)
  concentrated_views: 5,  // +5 percentage points (80% → 85% → 90% ...)
};

// === Reuse the channel resolution from ticketViralNotifier ===
// Same logic: find the channel that matches the VA's Discord username.
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
  } catch (e) {
    logger.warn('[AccountAlert] findVaTicketChannel failed: ' + e.message);
    return null;
  }
}

// Build mentions for the platform — VA + manager + team leader (when configured)
function buildMentions(vaDiscordId, vaName, platformConfig) {
  var vaMention = vaDiscordId ? ('<@' + vaDiscordId + '>') : ('@' + (vaName || 'VA'));
  var managerMention = platformConfig.managerRoleId ? ('<@&' + platformConfig.managerRoleId + '>') : '@manager';
  var teamLeaderMention = platformConfig.teamLeaderRoleId ? (' <@&' + platformConfig.teamLeaderRoleId + '>') : '';
  return {
    va: vaMention,
    leadership: managerMention + teamLeaderMention,
  };
}

// === Resolve the platform config + guild + ticket channel for a VA ===
// Returns { channel, mentions } or null if anything fails.
async function resolveVaTicket(platform, vaDiscordId, vaName) {
  if (!discordClient) return null;
  var platforms = config.getActivePlatforms();
  var pc = platforms.find(function(p) { return p.name === platform; });
  if (!pc || !pc.guildId) return null;
  var guild;
  try { guild = await discordClient.guilds.fetch(pc.guildId); }
  catch (e) {
    logger.warn('[AccountAlert] cannot fetch guild ' + pc.guildId + ': ' + e.message);
    return null;
  }
  var channel = await findVaTicketChannel(guild, vaName);
  if (!channel && vaDiscordId) {
    try {
      var member = await guild.members.fetch(vaDiscordId);
      if (member) channel = await findVaTicketChannel(guild, member.user.username);
    } catch (e) { /* fallback failed */ }
  }
  if (!channel) return null;
  return { channel: channel, mentions: buildMentions(vaDiscordId, vaName, pc) };
}

// === Idempotent send: only fire if severity is new or worse ===
// Returns true if we sent (or recorded) an alert, false if skipped.
async function shouldNotifyOrUpdate(db, accountId, kind, newSeverity) {
  try {
    var r = await db.pool.query(
      'SELECT last_severity FROM account_alerts_sent WHERE account_id = $1 AND kind = $2',
      [accountId, kind]
    );
    if (r.rows.length === 0) {
      // Never notified before → notify
      return { send: true, previousSeverity: 0 };
    }
    var prev = Number(r.rows[0].last_severity);
    var step = SEVERITY_STEP[kind] || 1;
    if (newSeverity >= prev + step) {
      // Worsened beyond the threshold → re-notify
      return { send: true, previousSeverity: prev };
    }
    // Same or marginal change → skip
    return { send: false, previousSeverity: prev };
  } catch (e) {
    logger.warn('[AccountAlert] severity check failed: ' + e.message);
    return { send: false, previousSeverity: 0 };
  }
}

async function recordAlertSent(db, accountId, kind, severity) {
  try {
    await db.pool.query(
      'INSERT INTO account_alerts_sent (account_id, kind, last_severity, sent_at) ' +
      'VALUES ($1, $2, $3, NOW()) ' +
      'ON CONFLICT (account_id, kind) DO UPDATE SET last_severity = EXCLUDED.last_severity, sent_at = NOW()',
      [accountId, kind, severity]
    );
  } catch (e) {
    logger.warn('[AccountAlert] failed to record sent state: ' + e.message);
  }
}

// ─────────────────────────────────────────────────────────────────────────────
// ALERT 1: DEAD ACCOUNT
// Triggered when 5+ recent posts of the account each have <100 views.
// Severity = number of failed posts (more posts → worse).
// ─────────────────────────────────────────────────────────────────────────────
async function notifyDeadAccount(db, account, failedPostCount) {
  if (failedPostCount < 5) return; // not enough signal yet
  var decision = await shouldNotifyOrUpdate(db, account.id, 'dead_account', failedPostCount);
  if (!decision.send) return;

  var ticket = await resolveVaTicket(account.platform, account.va_discord_id, account.va_name);
  if (!ticket) {
    logger.info('[AccountAlert] dead_account: no ticket for VA ' + account.va_name + ' (account @' + account.username + ')');
    await recordAlertSent(db, account.id, 'dead_account', failedPostCount);
    return;
  }

  var isEscalation = decision.previousSeverity > 0;
  var emoji = isEscalation ? '🚨' : '💀';
  var header = isEscalation ? 'COMPTE TOUJOURS MORT — ' + failedPostCount + ' POSTS RATES' : 'COMPTE PROBABLEMENT MORT';
  var content =
    emoji + ' **' + header + '** ' + emoji + '\n\n' +
    ticket.mentions.va + ' ton compte **@' + account.username + '** (' + account.platform.toUpperCase() + ') ne decolle pas.\n\n' +
    '📉 ' + failedPostCount + ' posts recents sont a moins de 100 vues chacun.\n\n' +
    (isEscalation
      ? 'La situation s\'aggrave depuis la derniere alerte (' + decision.previousSeverity + ' posts rates → ' + failedPostCount + ' maintenant).\n'
      : '') +
    '⚠️ ' + ticket.mentions.leadership + ' — Ce compte ne fonctionne plus. Il faut envisager de l\'abandonner et de creer un nouveau compte.';

  try {
    await ticket.channel.send({ content: content, allowedMentions: { parse: ['users', 'roles'] } });
    logger.info('[AccountAlert] dead_account sent for @' + account.username + ' (severity=' + failedPostCount + ') in #' + ticket.channel.name);
    await recordAlertSent(db, account.id, 'dead_account', failedPostCount);
  } catch (e) {
    logger.warn('[AccountAlert] dead_account send failed: ' + e.message);
  }
}

// ─────────────────────────────────────────────────────────────────────────────
// ALERT 2: SHADOWBAN
// Triggered when shadowban detection fires from the scrape worker.
// Severity = number of failed posts at the time of detection.
// ─────────────────────────────────────────────────────────────────────────────
async function notifyShadowban(db, account, failedPostCount) {
  if (failedPostCount < 3) return;
  var decision = await shouldNotifyOrUpdate(db, account.id, 'shadowban', failedPostCount);
  if (!decision.send) return;

  var ticket = await resolveVaTicket(account.platform, account.va_discord_id, account.va_name);
  if (!ticket) {
    logger.info('[AccountAlert] shadowban: no ticket for VA ' + account.va_name + ' (account @' + account.username + ')');
    await recordAlertSent(db, account.id, 'shadowban', failedPostCount);
    return;
  }

  var isEscalation = decision.previousSeverity > 0;
  var emoji = isEscalation ? '🚨' : '🚫';
  var header = isEscalation ? 'SHADOWBAN CONFIRME — SITUATION QUI EMPIRE' : 'COMPTE PROBABLEMENT SHADOWBAN';
  var content =
    emoji + ' **' + header + '** ' + emoji + '\n\n' +
    ticket.mentions.va + ' ton compte **@' + account.username + '** (' + account.platform.toUpperCase() + ') a ete probablement shadowban par la plateforme.\n\n' +
    '📉 ' + failedPostCount + ' posts recents (≥24h) ont chacun moins de 300 vues — comportement typique du shadowban.\n\n' +
    (isEscalation
      ? 'La situation continue de se degrader (' + decision.previousSeverity + ' → ' + failedPostCount + ' posts touches).\n'
      : '') +
    '⚠️ ' + ticket.mentions.leadership + ' — Le compte est probablement bloque par l\'algorithme. Il faut envisager de l\'abandonner et d\'en creer un nouveau.';

  try {
    await ticket.channel.send({ content: content, allowedMentions: { parse: ['users', 'roles'] } });
    logger.info('[AccountAlert] shadowban sent for @' + account.username + ' (severity=' + failedPostCount + ') in #' + ticket.channel.name);
    await recordAlertSent(db, account.id, 'shadowban', failedPostCount);
  } catch (e) {
    logger.warn('[AccountAlert] shadowban send failed: ' + e.message);
  }
}

// ─────────────────────────────────────────────────────────────────────────────
// ALERT 3: CONCENTRATED VIEWS
// Periodic check (cron): for each VA with 3+ accounts on a platform, if ONE
// account makes ≥80% of total 7-day views, post an info notice in the
// dominant account's ticket suggesting the others might be worth replacing.
// Severity = the % of views on the dominant account (rounded).
// ─────────────────────────────────────────────────────────────────────────────
async function checkConcentratedViews(db) {
  if (!discordClient) return;
  try {
    // Get per-VA per-account 7-day view sums. We only consider VAs with at
    // least 3 active accounts on the same platform.
    var sql =
      "WITH per_account AS (" +
      "  SELECT a.id AS account_id, a.username, a.platform, a.va_discord_id, a.va_name, " +
      "         COALESCE(SUM(latest.views) FILTER (WHERE p.created_at >= NOW() - INTERVAL '7 days'), 0) AS views_7d " +
      "  FROM accounts a " +
      "  LEFT JOIN posts p ON p.account_id = a.id AND p.deleted_at IS NULL " +
      "  LEFT JOIN LATERAL (" +
      "    SELECT views FROM snapshots s " +
      "    WHERE s.post_id = p.id AND COALESCE(s.error, '') <> 'coaching_sent' " +
      "    ORDER BY s.scraped_at DESC LIMIT 1" +
      "  ) latest ON true " +
      "  WHERE a.va_discord_id IS NOT NULL AND a.status = 'active' " +
      "  GROUP BY a.id, a.username, a.platform, a.va_discord_id, a.va_name" +
      "), per_va AS (" +
      "  SELECT va_discord_id, platform, COUNT(*) AS account_count, SUM(views_7d) AS total_views " +
      "  FROM per_account " +
      "  GROUP BY va_discord_id, platform " +
      "  HAVING COUNT(*) >= 3 AND SUM(views_7d) > 0" +
      ") " +
      "SELECT pa.account_id, pa.username, pa.platform, pa.va_discord_id, pa.va_name, " +
      "       pa.views_7d, pv.total_views, pv.account_count " +
      "FROM per_account pa " +
      "JOIN per_va pv ON pv.va_discord_id = pa.va_discord_id AND pv.platform = pa.platform " +
      "WHERE pv.total_views > 0 AND (pa.views_7d::float / pv.total_views::float) >= 0.80";
    var rows = (await db.pool.query(sql)).rows;
    for (var i = 0; i < rows.length; i++) {
      var row = rows[i];
      var pct = Math.round((Number(row.views_7d) / Number(row.total_views)) * 100);
      // We attach the alert to the DOMINANT account (the one making 80%+).
      // Severity = the % concentration.
      await notifyConcentratedViews(db, {
        id: row.account_id,
        username: row.username,
        platform: row.platform,
        va_discord_id: row.va_discord_id,
        va_name: row.va_name,
      }, {
        accountCount: Number(row.account_count),
        dominantPct: pct,
        dominantViews: Number(row.views_7d),
        totalViews: Number(row.total_views),
      });
    }
  } catch (e) {
    logger.warn('[AccountAlert] concentrated_views scan failed: ' + e.message);
  }
}

async function notifyConcentratedViews(db, account, info) {
  var decision = await shouldNotifyOrUpdate(db, account.id, 'concentrated_views', info.dominantPct);
  if (!decision.send) return;

  var ticket = await resolveVaTicket(account.platform, account.va_discord_id, account.va_name);
  if (!ticket) {
    logger.info('[AccountAlert] concentrated_views: no ticket for VA ' + account.va_name);
    await recordAlertSent(db, account.id, 'concentrated_views', info.dominantPct);
    return;
  }

  function fm(n) { return Number(n || 0).toLocaleString('fr-FR'); }
  var content =
    '📊 **CONCENTRATION DES VUES — UN COMPTE PORTE TOUT**\n\n' +
    ticket.mentions.va + ' sur tes ' + info.accountCount + ' comptes ' + account.platform.toUpperCase() + ', **@' + account.username + '** fait ' + info.dominantPct + '% des vues (' + fm(info.dominantViews) + ' / ' + fm(info.totalViews) + ' sur 7 jours).\n\n' +
    'Les autres comptes ne decollent pas. Ce n\'est pas obligatoirement un probleme — peut-etre que ce compte cartonne juste — mais ca peut aussi vouloir dire que les autres comptes meritent d\'etre remplaces.\n\n' +
    '💡 ' + ticket.mentions.leadership + ' — A discuter : faut-il remplacer les comptes qui n\'apportent rien ?';

  try {
    await ticket.channel.send({ content: content, allowedMentions: { parse: ['users', 'roles'] } });
    logger.info('[AccountAlert] concentrated_views sent for @' + account.username + ' (' + info.dominantPct + '%) in #' + ticket.channel.name);
    await recordAlertSent(db, account.id, 'concentrated_views', info.dominantPct);
  } catch (e) {
    logger.warn('[AccountAlert] concentrated_views send failed: ' + e.message);
  }
}

// ─────────────────────────────────────────────────────────────────────────────
// PERIODIC DEAD-ACCOUNT SCAN
// Scans all active accounts and counts their recent (last 14 days) posts
// that have <100 views. Triggers notifyDeadAccount for those above threshold.
// Run from cron (e.g. once a day).
// ─────────────────────────────────────────────────────────────────────────────
async function checkDeadAccounts(db) {
  if (!discordClient) return;
  try {
    var sql =
      "SELECT a.id, a.username, a.platform, a.va_discord_id, a.va_name, " +
      "  COUNT(p.id) FILTER (" +
      "    WHERE p.deleted_at IS NULL " +
      "      AND p.created_at >= NOW() - INTERVAL '14 days' " +
      "      AND COALESCE(latest.views, 0) < 100 " +
      "      AND p.created_at <= NOW() - INTERVAL '24 hours' " + // only count posts that had time to perform
      "  ) AS failed_count " +
      "FROM accounts a " +
      "LEFT JOIN posts p ON p.account_id = a.id " +
      "LEFT JOIN LATERAL (" +
      "  SELECT views FROM snapshots s " +
      "  WHERE s.post_id = p.id AND COALESCE(s.error, '') <> 'coaching_sent' " +
      "  ORDER BY s.scraped_at DESC LIMIT 1" +
      ") latest ON true " +
      "WHERE a.status = 'active' AND a.va_discord_id IS NOT NULL " +
      "GROUP BY a.id " +
      "HAVING COUNT(p.id) FILTER (" +
      "  WHERE p.deleted_at IS NULL " +
      "    AND p.created_at >= NOW() - INTERVAL '14 days' " +
      "    AND COALESCE(latest.views, 0) < 100 " +
      "    AND p.created_at <= NOW() - INTERVAL '24 hours'" +
      ") >= 5";
    var rows = (await db.pool.query(sql)).rows;
    for (var i = 0; i < rows.length; i++) {
      var r = rows[i];
      await notifyDeadAccount(db, {
        id: r.id,
        username: r.username,
        platform: r.platform,
        va_discord_id: r.va_discord_id,
        va_name: r.va_name,
      }, Number(r.failed_count));
    }
  } catch (e) {
    logger.warn('[AccountAlert] dead-account scan failed: ' + e.message);
  }
}

module.exports = {
  setDiscordClient: setDiscordClient,
  notifyDeadAccount: notifyDeadAccount,
  notifyShadowban: notifyShadowban,
  checkConcentratedViews: checkConcentratedViews,
  checkDeadAccounts: checkDeadAccounts,
};
