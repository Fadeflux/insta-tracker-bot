const { SlashCommandBuilder } = require('discord.js');
const config = require('../../config');
const db = require('../db/queries');
const embeds = require('../utils/embeds');
const { getQueueStats } = require('../jobs/scrapeQueue');
const { sendDailySummaryForPlatform } = require('../jobs/cron');
const logger = require('../utils/logger');

var commands = [
  new SlashCommandBuilder().setName('stats').setDescription('Voir tes stats du jour')
    .addUserOption(function(opt) { return opt.setName('va').setDescription('VA specifique (managers only)').setRequired(false); }),
  new SlashCommandBuilder().setName('leaderboard').setDescription('Classement du jour')
    .addStringOption(function(opt) { return opt.setName('date').setDescription('Date (YYYY-MM-DD)').setRequired(false); }),
  new SlashCommandBuilder().setName('export').setDescription('Exporter les donnees en CSV (managers only)')
    .addStringOption(function(opt) { return opt.setName('date').setDescription('Date (YYYY-MM-DD)').setRequired(false); }),
  new SlashCommandBuilder().setName('status').setDescription('Statut du bot et des queues (managers only)'),
  new SlashCommandBuilder().setName('force-summary').setDescription('Forcer le resume de fin de journee (managers only)'),
  new SlashCommandBuilder().setName('permission').setDescription('Gerer les permissions (admin only)')
    .addSubcommand(function(sub) {
      return sub.setName('add').setDescription('Ajouter une permission')
        .addUserOption(function(opt) { return opt.setName('user').setDescription('Utilisateur').setRequired(true); })
        .addStringOption(function(opt) { return opt.setName('platform').setDescription('Plateforme').setRequired(true).addChoices({ name: 'Instagram', value: 'instagram' }, { name: 'Twitter', value: 'twitter' }, { name: 'Toutes', value: 'all' }); })
        .addStringOption(function(opt) { return opt.setName('role').setDescription('Role').setRequired(true).addChoices({ name: 'Admin', value: 'admin' }, { name: 'Manager', value: 'manager' }, { name: 'VA', value: 'va' }); });
    })
    .addSubcommand(function(sub) {
      return sub.setName('remove').setDescription('Retirer une permission')
        .addUserOption(function(opt) { return opt.setName('user').setDescription('Utilisateur').setRequired(true); })
        .addStringOption(function(opt) { return opt.setName('platform').setDescription('Plateforme').setRequired(true).addChoices({ name: 'Instagram', value: 'instagram' }, { name: 'Twitter', value: 'twitter' }, { name: 'Toutes', value: 'all' }); });
    })
    .addSubcommand(function(sub) {
      return sub.setName('list').setDescription('Voir toutes les permissions');
    }),
  new SlashCommandBuilder().setName('streaks').setDescription('Voir les streaks actuels'),
  new SlashCommandBuilder().setName('points').setDescription('Voir le classement de points de la semaine'),
  new SlashCommandBuilder().setName('duel').setDescription('Voir ton duel de la semaine en cours'),
];

// Helper: get platform from interaction guild
function getPlatform(interaction) {
  var platformInfo = config.getPlatformByGuild(interaction.guildId);
  return platformInfo ? platformInfo.name : 'instagram';
}

function getManagerRoleId(interaction) {
  var platformInfo = config.getPlatformByGuild(interaction.guildId);
  return platformInfo ? platformInfo.config.managerRoleId : config.discord.managerRoleId;
}

function isManager(interaction) {
  var roleId = getManagerRoleId(interaction);
  return roleId && interaction.member.roles.cache.has(roleId);
}

function isAdminUser(interaction) {
  return config.isAdmin(interaction.user.id);
}

async function handleCommand(interaction) {
  var commandName = interaction.commandName;
  try {
    if (commandName === 'stats') return await handleStats(interaction);
    if (commandName === 'leaderboard') return await handleLeaderboard(interaction);
    if (commandName === 'export') return await handleExport(interaction);
    if (commandName === 'status') return await handleStatus(interaction);
    if (commandName === 'force-summary') return await handleForceSummary(interaction);
    if (commandName === 'permission') return await handlePermission(interaction);
    if (commandName === 'streaks') return await handleStreaks(interaction);
    if (commandName === 'points') return await handlePoints(interaction);
    if (commandName === 'duel') return await handleDuel(interaction);
    await interaction.reply({ content: 'Commande inconnue.', ephemeral: true });
  } catch (err) {
    logger.error('Command error: ' + commandName, { error: err.message });
    var reply = { content: 'Une erreur est survenue.', ephemeral: true };
    if (interaction.replied || interaction.deferred) await interaction.followUp(reply);
    else await interaction.reply(reply);
  }
}

async function handleStats(interaction) {
  await interaction.deferReply({ ephemeral: true });
  var platform = getPlatform(interaction);
  var targetUser = interaction.options.getUser('va') || interaction.user;
  var today = new Date().toISOString().split('T')[0];
  await db.computeDailySummary(today, platform);
  var stats = await db.getVaDailyStats(targetUser.id, today, platform);
  var posts = await db.getVaPostsToday(targetUser.id, today, platform);
  if (!stats && posts.length === 0) return interaction.editReply({ content: 'Aucun post trouve pour ' + targetUser.username + ' aujourd hui.' });
  var vaName = (posts[0] && posts[0].va_name) || targetUser.username;
  var embed = embeds.vaStatsEmbed(vaName, stats || { total_views: 0, total_likes: 0, total_comments: 0, total_shares: 0 }, posts, platform);
  await interaction.editReply({ embeds: [embed] });
}

async function handleLeaderboard(interaction) {
  await interaction.deferReply();
  var platform = getPlatform(interaction);
  var date = interaction.options.getString('date') || new Date().toISOString().split('T')[0];
  await db.computeDailySummary(date, platform);
  var rankings = await db.getLeaderboard(date, platform);
  if (rankings.length === 0) return interaction.editReply({ content: 'Aucune donnee pour le ' + date + '.' });
  var embed = embeds.leaderboardEmbed(rankings, date, platform);
  await interaction.editReply({ embeds: [embed] });
}

async function handleExport(interaction) {
  if (!isManager(interaction) && !isAdminUser(interaction)) return interaction.reply({ content: 'Reserve aux managers.', ephemeral: true });
  await interaction.deferReply({ ephemeral: true });
  var platform = getPlatform(interaction);
  var date = interaction.options.getString('date') || new Date().toISOString().split('T')[0];
  var rows = await db.getPostsForExport(date, platform);
  if (rows.length === 0) return interaction.editReply({ content: 'Aucune donnee a exporter pour le ' + date + '.' });
  var Parser = require('json2csv').Parser;
  var fields = ['va_name', 'url', 'ig_post_id', 'platform', 'created_at', 'status', 'views', 'likes', 'comments', 'shares'];
  if (platform === 'twitter') fields = fields.concat(['retweets', 'quote_tweets', 'bookmarks']);
  var parser = new Parser({ fields: fields });
  var csv = parser.parse(rows);
  var buffer = Buffer.from(csv, 'utf-8');
  await interaction.editReply({ content: 'Export ' + platform + ' du ' + date + ' (' + rows.length + ' entrees)', files: [{ attachment: buffer, name: 'export-' + platform + '-' + date + '.csv' }] });
}

async function handleStatus(interaction) {
  if (!isManager(interaction) && !isAdminUser(interaction)) return interaction.reply({ content: 'Reserve aux managers.', ephemeral: true });
  var platform = getPlatform(interaction);
  var queueStats = await getQueueStats();
  var activePosts = await db.getActivePosts(platform);
  var allActive = await db.getActivePosts();
  await interaction.reply({
    content: '**Statut du bot — ' + platform.toUpperCase() + '**\n' +
      'Posts actifs (' + platform + '): ' + activePosts.length + '\n' +
      'Posts actifs (total): ' + allActive.length + '\n' +
      'Queue — Attente: ' + queueStats.waiting + ' | Actif: ' + queueStats.active + ' | Differe: ' + queueStats.delayed + '\n' +
      'Uptime: ' + Math.floor(process.uptime() / 60) + ' min',
    ephemeral: true,
  });
}

async function handleForceSummary(interaction) {
  if (!isManager(interaction) && !isAdminUser(interaction)) return interaction.reply({ content: 'Reserve aux managers.', ephemeral: true });
  await interaction.deferReply({ ephemeral: true });
  var platform = getPlatform(interaction);
  await sendDailySummaryForPlatform(platform);
  await interaction.editReply({ content: 'Resume de fin de journee envoye pour ' + platform + '.' });
}

async function handlePermission(interaction) {
  if (!isAdminUser(interaction)) return interaction.reply({ content: 'Reserve aux admins.', ephemeral: true });

  var sub = interaction.options.getSubcommand();

  if (sub === 'add') {
    var user = interaction.options.getUser('user');
    var platform = interaction.options.getString('platform');
    var role = interaction.options.getString('role');
    await db.setUserPermission(user.id, platform, role, interaction.user.id);
    await interaction.reply({ content: '✅ Permission ajoutee: **' + user.username + '** → ' + role + ' sur ' + platform, ephemeral: true });
  } else if (sub === 'remove') {
    var user2 = interaction.options.getUser('user');
    var platform2 = interaction.options.getString('platform');
    var removed = await db.removeUserPermission(user2.id, platform2);
    if (removed) {
      await interaction.reply({ content: '✅ Permission retiree pour **' + user2.username + '** sur ' + platform2, ephemeral: true });
    } else {
      await interaction.reply({ content: 'Aucune permission trouvee pour cet utilisateur sur ' + platform2, ephemeral: true });
    }
  } else if (sub === 'list') {
    var perms = await db.getAllPermissions();
    if (perms.length === 0) return interaction.reply({ content: 'Aucune permission configuree.', ephemeral: true });
    var lines = perms.map(function(p) {
      return '`' + p.discord_id + '` → **' + p.role + '** sur ' + p.platform;
    });
    await interaction.reply({ content: '**Permissions:**\n' + lines.join('\n'), ephemeral: true });
  }
}

async function handleStreaks(interaction) {
  await interaction.deferReply();
  var platform = getPlatform(interaction);
  var streaks = await db.getAllStreaks(platform);
  if (streaks.length === 0) return interaction.editReply({ content: 'Aucun streak enregistre pour ' + platform + '.' });

  var lines = streaks.map(function(s, i) {
    var fire = s.current_streak >= 5 ? '🔥' : s.current_streak >= 3 ? '⚡' : '';
    return (i + 1) + '. **' + s.va_name + '** — ' + fire + ' ' + s.current_streak + ' jours (record: ' + s.best_streak + ')';
  });

  await interaction.editReply({
    content: '**🏅 Streaks ' + platform.toUpperCase() + '**\n\n' + lines.join('\n')
  });
}

async function handlePoints(interaction) {
  await interaction.deferReply();
  var platform = getPlatform(interaction);
  var bounds = db.getWeekBounds();
  var standings = await db.getWeeklyStandings(bounds.start, bounds.end, platform);

  if (standings.length === 0) {
    return interaction.editReply({ content: 'Aucun point attribue cette semaine pour ' + platform + '. Les points sont distribues chaque soir au top 3 des VA ayant fait 6+ posts.' });
  }

  var medals = ['🥇', '🥈', '🥉'];
  var lines = standings.slice(0, 10).map(function(s, i) {
    var prefix = i < 3 ? medals[i] : '  ' + (i + 1) + '.';
    return prefix + ' **' + s.va_name + '** — **' + s.total_points + ' pts** (' + s.podium_count + ' podiums cette semaine)';
  });

  var recentWinners = await db.getRecentWinners(platform, 3);
  var winnersLine = recentWinners.length > 0
    ? '\n\n**🏆 Derniers champions hebdomadaires :**\n' + recentWinners.map(function(w) {
        return '• **' + w.va_name + '** — semaine du ' + w.week_start + ' (' + w.total_points + ' pts)';
      }).join('\n')
    : '';

  await interaction.editReply({
    content: '**🎯 Classement points — semaine du ' + bounds.start + ' au ' + bounds.end + '**\n\n' +
      lines.join('\n') + '\n\n' +
      '_Baremage quotidien : 10 / 6 / 3 pts pour le top 3 des VA a 6+ posts. Le #1 de dimanche est couronne champion de la semaine._' +
      winnersLine
  });
}

async function handleDuel(interaction) {
  await interaction.deferReply({ ephemeral: true });
  var platform = getPlatform(interaction);
  var userId = interaction.user.id;
  var activeDuels = await db.getActiveDuels(platform);
  var myDuel = activeDuels.find(function(d) { return d.va1_discord_id === userId || d.va2_discord_id === userId; });

  if (!myDuel) {
    return interaction.editReply({ content: 'Tu n\'as pas de duel actif cette semaine sur ' + platform + '. Les duels sont crees automatiquement chaque dimanche soir.' });
  }

  var isV1 = myDuel.va1_discord_id === userId;
  var myName = isV1 ? myDuel.va1_name : myDuel.va2_name;
  var opponentName = isV1 ? myDuel.va2_name : myDuel.va1_name;
  var opponentId = isV1 ? myDuel.va2_discord_id : myDuel.va1_discord_id;

  // Compute current views for both
  var bounds = db.getWeekBounds();
  var sql = "SELECT va_discord_id, COALESCE(SUM(s.views), 0)::bigint AS views " +
    "FROM posts p " +
    "LEFT JOIN LATERAL ( " +
    "  SELECT views FROM snapshots sn WHERE sn.post_id = p.id AND COALESCE(sn.error, '') <> 'coaching_sent' " +
    "  ORDER BY sn.scraped_at DESC LIMIT 1 " +
    ") s ON true " +
    "WHERE p.platform = $1 AND p.va_discord_id IN ($2, $3) " +
    "AND p.created_at::date >= $4 AND p.created_at::date <= $5 " +
    "GROUP BY va_discord_id";
  var res = await db.pool.query(sql, [platform, myDuel.va1_discord_id, myDuel.va2_discord_id, bounds.start, bounds.end]);
  var map = {};
  res.rows.forEach(function(r) { map[r.va_discord_id] = Number(r.views) || 0; });
  var myViews = map[userId] || 0;
  var oppViews = map[opponentId] || 0;
  var diff = myViews - oppViews;
  var status = diff > 0 ? '🟢 **Tu es en tete !**' : diff < 0 ? '🔴 **Tu es en retard...**' : '🟡 **Egalite parfaite.**';

  await interaction.editReply({
    content: '**⚔️ Ton duel de la semaine — ' + platform.toUpperCase() + '**\n' +
      'Du ' + myDuel.week_start + ' au ' + myDuel.week_end + '\n\n' +
      '👤 **' + myName + '** : ' + myViews.toLocaleString('fr-FR') + ' vues\n' +
      '🆚 **' + opponentName + '** : ' + oppViews.toLocaleString('fr-FR') + ' vues\n\n' +
      status + '\n' +
      'Ecart : **' + Math.abs(diff).toLocaleString('fr-FR') + ' vues**\n\n' +
      '_Le perdant devra poster un message de felicitations au gagnant dimanche soir._'
  });
}

module.exports = { commands: commands, handleCommand: handleCommand };
