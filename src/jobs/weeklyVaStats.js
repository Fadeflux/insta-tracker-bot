// Weekly VA stats
// ──────────────
// Every Sunday at 20h Bénin time, sends each VA a personal recap of their
// week in their ticket channel. Helps motivate the VA by showing concrete
// numbers and progression vs the previous week.
//
// We compute per-VA across ALL platforms they posted on (so a VA who posts
// on both Instagram and Geelark gets a single combined recap).
//
// Format:
//   📊 Ta semaine en chiffres (du X au Y)
//   • Posts publies
//   • Vues totales (with delta vs previous week)
//   • Meilleur post de la semaine (with link)
//   • Place dans le classement de l'agence
//   • Comptes qui cartonnent / qui flop
//   • Reels viraux de la semaine

var config = require('../../config');
var logger = require('../utils/logger');

var discordClient = null;
function setDiscordClient(client) { discordClient = client; }

// "Viral" threshold — same as everywhere else
var VIRAL_VIEWS = 5000;

// Find the VA's ticket channel (same logic as other ticket modules)
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

// === Compute per-VA week stats across all platforms ===
// Returns an array: [{ va_discord_id, va_name, posts_count, total_views,
//                      viral_count, best_post: {url, views, account},
//                      prev_views, rank, total_vas }]
async function computeWeeklyVaStats(db) {
  // Window: last 7 full days in Bénin TZ. We use "the week ending now"
  // (so on Sunday 20h Bénin, the week covers the last 7 days = Mon-Sun).
  // Previous week for the delta = the 7 days before that.
  var sql =
    "WITH this_week AS (" +
    "  SELECT p.va_discord_id, p.va_name, p.id AS post_id, p.url, p.account_username, " +
    "         p.platform, p.created_at, " +
    "         COALESCE(latest.views, 0) AS views " +
    "  FROM posts p " +
    "  LEFT JOIN LATERAL (" +
    "    SELECT views FROM snapshots s " +
    "    WHERE s.post_id = p.id AND COALESCE(s.error, '') <> 'coaching_sent' " +
    "    ORDER BY s.scraped_at DESC LIMIT 1" +
    "  ) latest ON true " +
    "  WHERE p.deleted_at IS NULL AND p.va_discord_id IS NOT NULL " +
    "    AND p.created_at >= NOW() - INTERVAL '7 days'" +
    ") " +
    "SELECT va_discord_id, MAX(va_name) AS va_name, " +
    "       COUNT(*) AS posts_count, " +
    "       SUM(views) AS total_views, " +
    "       COUNT(*) FILTER (WHERE views >= $1) AS viral_count " +
    "FROM this_week " +
    "GROUP BY va_discord_id " +
    "ORDER BY total_views DESC";
  var perVa = (await db.pool.query(sql, [VIRAL_VIEWS])).rows;

  if (perVa.length === 0) return [];

  // Compute rank per VA for the readout
  var totalVAs = perVa.length;
  perVa.forEach(function(r, idx) { r.rank = idx + 1; r.total_vas = totalVAs; });

  // Get previous-week views for each VA, for the delta
  var prevSql =
    "SELECT p.va_discord_id, SUM(COALESCE(latest.views, 0)) AS prev_views " +
    "FROM posts p " +
    "LEFT JOIN LATERAL (" +
    "  SELECT views FROM snapshots s " +
    "  WHERE s.post_id = p.id AND COALESCE(s.error, '') <> 'coaching_sent' " +
    "  ORDER BY s.scraped_at DESC LIMIT 1" +
    ") latest ON true " +
    "WHERE p.deleted_at IS NULL AND p.va_discord_id IS NOT NULL " +
    "  AND p.created_at >= NOW() - INTERVAL '14 days' " +
    "  AND p.created_at < NOW() - INTERVAL '7 days' " +
    "GROUP BY p.va_discord_id";
  var prevByVa = {};
  (await db.pool.query(prevSql)).rows.forEach(function(r) {
    prevByVa[r.va_discord_id] = Number(r.prev_views || 0);
  });
  perVa.forEach(function(r) { r.prev_views = prevByVa[r.va_discord_id] || 0; });

  // Get best post for each VA this week
  var bestSql =
    "SELECT DISTINCT ON (p.va_discord_id) " +
    "  p.va_discord_id, p.url, p.account_username, p.platform, " +
    "  COALESCE(latest.views, 0) AS views " +
    "FROM posts p " +
    "LEFT JOIN LATERAL (" +
    "  SELECT views FROM snapshots s " +
    "  WHERE s.post_id = p.id AND COALESCE(s.error, '') <> 'coaching_sent' " +
    "  ORDER BY s.scraped_at DESC LIMIT 1" +
    ") latest ON true " +
    "WHERE p.deleted_at IS NULL AND p.va_discord_id IS NOT NULL " +
    "  AND p.created_at >= NOW() - INTERVAL '7 days' " +
    "ORDER BY p.va_discord_id, COALESCE(latest.views, 0) DESC NULLS LAST";
  var bestByVa = {};
  (await db.pool.query(bestSql)).rows.forEach(function(r) {
    bestByVa[r.va_discord_id] = r;
  });
  perVa.forEach(function(r) { r.best_post = bestByVa[r.va_discord_id] || null; });

  return perVa;
}

// === Build a personalised message for one VA ===
function buildMessage(va) {
  function fm(n) { return Number(n || 0).toLocaleString('fr-FR'); }
  // Compute date range labels in Bénin TZ
  var now = new Date();
  var weekEnd = now;
  var weekStart = new Date(now.getTime() - 7 * 86400000);
  function frDate(d) {
    var months = ['janv.','fevr.','mars','avril','mai','juin','juill.','aout','sept.','oct.','nov.','dec.'];
    return d.getDate() + ' ' + months[d.getMonth()];
  }
  var rangeLabel = frDate(weekStart) + ' au ' + frDate(weekEnd);

  var lines = [];
  lines.push('📊 **Ta semaine en chiffres** (' + rangeLabel + ')');
  lines.push('');

  lines.push('   • **' + va.posts_count + ' post' + (va.posts_count > 1 ? 's' : '') + ' publie' + (va.posts_count > 1 ? 's' : '') + '**');

  // Total views with delta
  var deltaStr = '';
  if (Number(va.prev_views) > 0) {
    var pct = Math.round(((Number(va.total_views) - Number(va.prev_views)) / Number(va.prev_views)) * 100);
    var arrow = pct > 0 ? '📈' : (pct < 0 ? '📉' : '➡️');
    deltaStr = ' ' + arrow + ' ' + (pct >= 0 ? '+' : '') + pct + '% vs semaine passee';
  } else if (Number(va.total_views) > 0) {
    deltaStr = ' (premiere semaine de stats !)';
  }
  lines.push('   • **' + fm(va.total_views) + ' vues totales**' + deltaStr);

  // Viral count
  if (Number(va.viral_count) > 0) {
    var pl = Number(va.viral_count) > 1;
    var word = pl ? 'posts viraux' : 'post viral';
    lines.push('   • **' + va.viral_count + ' ' + word + '** (>' + fm(VIRAL_VIEWS) + ' vues)');
  } else {
    lines.push('   • **0 post viral** cette semaine');
  }

  // Best post
  if (va.best_post && Number(va.best_post.views) > 0) {
    lines.push('   • **Meilleur post** : ' + fm(va.best_post.views) + ' vues sur @' + va.best_post.account_username);
    lines.push('     ' + va.best_post.url);
  }

  // Rank
  var rankSuffix = (va.rank === 1) ? 'ere' : 'eme';
  lines.push('   • **Place dans l\'agence** : ' + va.rank + rankSuffix + ' sur ' + va.total_vas + ' VAs');

  lines.push('');

  // Encouragement based on rank
  if (va.rank <= 3) {
    lines.push('🏆 **Tu es dans le top 3 cette semaine, super boulot !** Continue comme ca.');
  } else if (va.rank <= Math.ceil(va.total_vas / 2)) {
    lines.push('💪 **Tu es dans la moitie haute du classement.** Continue tes efforts pour grimper !');
  } else {
    lines.push('🚀 **Cette semaine peut etre meilleure.** Concentre-toi sur les posts qui marchent et reproduis ce qui a deja viral !');
  }

  return lines.join('\n');
}

// === Main entry: send weekly stats to all VAs ===
async function sendWeeklyStats(db) {
  if (!discordClient) {
    logger.warn('[WeeklyStats] no discord client, skipping');
    return;
  }
  var stats = await computeWeeklyVaStats(db);
  if (stats.length === 0) {
    logger.info('[WeeklyStats] no VAs with posts this week, skipping');
    return { sent: 0, skipped: 0, failed: 0, totalVas: 0 };
  }

  var platforms = config.getActivePlatforms();
  var guildOrder = [];
  ['instagram', 'geelark', 'twitter', 'threads'].forEach(function(plat) {
    var pc = platforms.find(function(p) { return p.name === plat; });
    if (pc && pc.guildId) guildOrder.push({ platform: plat, guildId: pc.guildId });
  });

  var sent = 0, skipped = 0, failed = 0;
  for (var i = 0; i < stats.length; i++) {
    var va = stats[i];
    var content = buildMessage(va);

    // Find ticket channel by trying each guild
    var channel = null;
    for (var g = 0; g < guildOrder.length; g++) {
      try {
        var guild = await discordClient.guilds.fetch(guildOrder[g].guildId);
        channel = await findVaTicketChannel(guild, va.va_name);
        if (channel) break;
        if (va.va_discord_id) {
          try {
            var member = await guild.members.fetch(va.va_discord_id);
            if (member) {
              channel = await findVaTicketChannel(guild, member.user.username);
              if (channel) break;
            }
          } catch (e) {}
        }
      } catch (e) {}
    }
    if (!channel) {
      logger.info('[WeeklyStats] no ticket for VA ' + va.va_name);
      skipped++;
      continue;
    }

    var fullContent = '<@' + va.va_discord_id + '>\n\n' + content;
    try {
      await channel.send({ content: fullContent, allowedMentions: { parse: ['users'] } });
      logger.info('[WeeklyStats] sent to #' + channel.name + ' (' + va.posts_count + ' posts, rank ' + va.rank + '/' + va.total_vas + ')');
      sent++;
    } catch (e) {
      logger.warn('[WeeklyStats] send failed: ' + e.message);
      failed++;
    }

    // Avoid Discord rate limits
    await new Promise(function(r) { setTimeout(r, 200); });
  }

  logger.info('[WeeklyStats] done. sent=' + sent + ', skipped=' + skipped + ', failed=' + failed);
  return { sent: sent, skipped: skipped, failed: failed, totalVas: stats.length };
}

module.exports = {
  setDiscordClient: setDiscordClient,
  sendWeeklyStats: sendWeeklyStats,
  buildMessage: buildMessage,
  computeWeeklyVaStats: computeWeeklyVaStats,
};
