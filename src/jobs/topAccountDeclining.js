// Top account declining detector
// ─────────────────────────────
// For each VA, identifies their "favorite account" — the one with the most
// views over the last 30 days — and checks if that account's performance
// has dropped significantly this week vs its 30-day baseline. If so, posts
// a warning in the VA's ticket so they can adjust their content.
//
// This is different from shadowban detection: a shadowbanned account has
// posts at <300 views. Here we catch the more subtle "audience fatigue"
// where a 10k-view account suddenly drops to 4k.
//
// Runs as a cron once a day (10h Bénin) since week-over-week trends don't
// move that fast.

var config = require('../../config');
var logger = require('../utils/logger');

var discordClient = null;
function setDiscordClient(client) { discordClient = client; }

// We only flag a decline when the drop is significant. A 10% drop is normal
// noise; we want to surface 30%+ drops on accounts that matter (>1000 avg).
var SIGNIFICANT_DROP_PCT = 30;
var MIN_AVG_VIEWS = 1000; // ignore accounts that don't perform anyway
var MIN_RECENT_POSTS = 3; // need at least 3 posts this week to trust the average

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

// === Find each VA's "favorite account" + this-week vs 30-day comparison ===
// Returns rows where the favorite account dropped >SIGNIFICANT_DROP_PCT %
async function findDecliningTopAccounts(db) {
  // For each VA, pick the account that had the most total views in the
  // last 30 days. Then compute its 30-day avg vs this week avg.
  // We use a window function to rank accounts per VA and keep only #1.
  var sql =
    "WITH per_account_30d AS (" +
    "  SELECT a.id AS account_id, a.username, a.platform, a.va_discord_id, a.va_name, " +
    "         COALESCE(SUM(latest.views) FILTER (WHERE p.created_at >= NOW() - INTERVAL '30 days'), 0) AS total_30d, " +
    "         COALESCE(AVG(latest.views) FILTER (WHERE p.created_at >= NOW() - INTERVAL '30 days' AND p.created_at < NOW() - INTERVAL '7 days'), 0) AS avg_baseline, " +
    "         COALESCE(AVG(latest.views) FILTER (WHERE p.created_at >= NOW() - INTERVAL '7 days'), 0) AS avg_week, " +
    "         COUNT(*) FILTER (WHERE p.created_at >= NOW() - INTERVAL '7 days') AS posts_week, " +
    "         COUNT(*) FILTER (WHERE p.created_at >= NOW() - INTERVAL '30 days' AND p.created_at < NOW() - INTERVAL '7 days') AS posts_baseline " +
    "  FROM accounts a " +
    "  LEFT JOIN posts p ON p.account_id = a.id AND p.deleted_at IS NULL " +
    "  LEFT JOIN LATERAL (" +
    "    SELECT views FROM snapshots s " +
    "    WHERE s.post_id = p.id AND COALESCE(s.error, '') <> 'coaching_sent' " +
    "    ORDER BY s.scraped_at DESC LIMIT 1" +
    "  ) latest ON true " +
    "  WHERE a.status = 'active' AND a.va_discord_id IS NOT NULL " +
    "  GROUP BY a.id, a.username, a.platform, a.va_discord_id, a.va_name" +
    "), ranked AS (" +
    "  SELECT *, ROW_NUMBER() OVER (PARTITION BY va_discord_id ORDER BY total_30d DESC) AS rk " +
    "  FROM per_account_30d WHERE total_30d > 0" +
    ") " +
    "SELECT * FROM ranked WHERE rk = 1 " +
    "  AND avg_baseline > $1 " + // baseline meaningful
    "  AND posts_week >= $2 " +    // enough posts this week
    "  AND posts_baseline >= 3 " + // enough baseline posts
    "  AND avg_week < avg_baseline * (1 - $3::float / 100)";
  var rows;
  try {
    rows = (await db.pool.query(sql, [MIN_AVG_VIEWS, MIN_RECENT_POSTS, SIGNIFICANT_DROP_PCT])).rows;
  } catch (e) {
    logger.warn('[TopDecline] query failed: ' + e.message);
    return [];
  }
  return rows.map(function(r) {
    return {
      account_id: r.account_id,
      username: r.username,
      platform: r.platform,
      va_discord_id: r.va_discord_id,
      va_name: r.va_name,
      avg_baseline: Number(r.avg_baseline),
      avg_week: Number(r.avg_week),
      drop_pct: Math.round((1 - Number(r.avg_week) / Number(r.avg_baseline)) * 100),
      posts_week: Number(r.posts_week),
    };
  });
}

// === Main: scan and notify ===
async function checkDecliningTopAccounts(db) {
  if (!discordClient) {
    logger.warn('[TopDecline] no discord client, skipping');
    return { sent: 0, skipped: 0 };
  }
  var declining = await findDecliningTopAccounts(db);
  var sent = 0, skipped = 0;

  for (var i = 0; i < declining.length; i++) {
    var d = declining[i];

    // Idempotency: don't re-notify within 7 days for the same account
    try {
      var prev = await db.pool.query(
        "SELECT sent_at FROM account_alerts_sent WHERE account_id = $1 AND kind = 'top_declining'",
        [d.account_id]
      );
      if (prev.rows.length > 0) {
        var hoursSince = (Date.now() - new Date(prev.rows[0].sent_at).getTime()) / 3600000;
        if (hoursSince < 24 * 7) { skipped++; continue; }
      }
    } catch (e) { /* table missing, ok */ }

    // Resolve channel + mentions
    var platforms = config.getActivePlatforms();
    var pc = platforms.find(function(p) { return p.name === d.platform; });
    if (!pc || !pc.guildId) { skipped++; continue; }
    var guild;
    try { guild = await discordClient.guilds.fetch(pc.guildId); }
    catch (e) { skipped++; continue; }
    var channel = await findVaTicketChannel(guild, d.va_name);
    if (!channel && d.va_discord_id) {
      try {
        var member = await guild.members.fetch(d.va_discord_id);
        if (member) channel = await findVaTicketChannel(guild, member.user.username);
      } catch (e) {}
    }
    if (!channel) {
      logger.info('[TopDecline] no ticket for ' + d.va_name);
      // Record so we don't retry every day
      try {
        await db.pool.query(
          "INSERT INTO account_alerts_sent (account_id, kind, last_severity, sent_at) " +
          "VALUES ($1, 'top_declining', $2, NOW()) ON CONFLICT (account_id, kind) DO UPDATE " +
          "SET last_severity = EXCLUDED.last_severity, sent_at = NOW()",
          [d.account_id, d.drop_pct]
        );
      } catch (e) {}
      skipped++;
      continue;
    }

    function fm(n) { return Number(n || 0).toLocaleString('fr-FR'); }
    var vaMention = d.va_discord_id ? ('<@' + d.va_discord_id + '>') : ('@' + d.va_name);
    var managerMention = pc.managerRoleId ? ('<@&' + pc.managerRoleId + '>') : '@manager';
    var teamLeaderMention = pc.teamLeaderRoleId ? (' <@&' + pc.teamLeaderRoleId + '>') : '';
    var leadership = managerMention + teamLeaderMention;

    var content =
      '📉 **TON MEILLEUR COMPTE SOUS-PERFORME** 📉\n\n' +
      vaMention + ' ton compte **@' + d.username + '** (' + d.platform.toUpperCase() + ') etait ton compte phare, mais il chute cette semaine.\n\n' +
      '📊 Baseline 30 jours : **' + fm(Math.round(d.avg_baseline)) + ' vues/post**\n' +
      '📊 Cette semaine : **' + fm(Math.round(d.avg_week)) + ' vues/post**\n' +
      '📉 Chute de **' + d.drop_pct + '%** sur ' + d.posts_week + ' posts\n\n' +
      '💡 C\'est souvent un signe de **fatigue de l\'audience**. Quelques pistes :\n' +
      '   • Change le style/format de tes reels\n' +
      '   • Teste de nouvelles musiques tendance\n' +
      '   • Modifie ton hook (3 premieres secondes)\n' +
      '   • Reproduis un de tes anciens viraux avec des metadonnees changees\n\n' +
      '⚠️ ' + leadership + ' — Compte phare en perte de vitesse, a discuter avec le VA.';

    try {
      await channel.send({ content: content, allowedMentions: { parse: ['users', 'roles'] } });
      logger.info('[TopDecline] sent for @' + d.username + ' (-' + d.drop_pct + '%) in #' + channel.name);
      try {
        await db.pool.query(
          "INSERT INTO account_alerts_sent (account_id, kind, last_severity, sent_at) " +
          "VALUES ($1, 'top_declining', $2, NOW()) ON CONFLICT (account_id, kind) DO UPDATE " +
          "SET last_severity = EXCLUDED.last_severity, sent_at = NOW()",
          [d.account_id, d.drop_pct]
        );
      } catch (e) {}
      sent++;
    } catch (e) {
      logger.warn('[TopDecline] send failed: ' + e.message);
      skipped++;
    }
    await new Promise(function(r) { setTimeout(r, 200); });
  }

  logger.info('[TopDecline] done. sent=' + sent + ', skipped=' + skipped);
  return { sent: sent, skipped: skipped };
}

module.exports = {
  setDiscordClient: setDiscordClient,
  checkDecliningTopAccounts: checkDecliningTopAccounts,
};
