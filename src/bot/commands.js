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

module.exports = { commands: commands, handleCommand: handleCommand };
