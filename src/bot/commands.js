const { SlashCommandBuilder } = require('discord.js');
const db = require('../db/queries');
const embeds = require('../utils/embeds');
const { getQueueStats } = require('../jobs/scrapeQueue');
const { sendDailySummary } = require('../jobs/cron');
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
];

async function handleCommand(interaction) {
  var commandName = interaction.commandName;
  try {
    if (commandName === 'stats') return await handleStats(interaction);
    if (commandName === 'leaderboard') return await handleLeaderboard(interaction);
    if (commandName === 'export') return await handleExport(interaction);
    if (commandName === 'status') return await handleStatus(interaction);
    if (commandName === 'force-summary') return await handleForceSummary(interaction);
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
  var targetUser = interaction.options.getUser('va') || interaction.user;
  var today = new Date().toISOString().split('T')[0];
  await db.computeDailySummary(today);
  var stats = await db.getVaDailyStats(targetUser.id, today);
  var posts = await db.getVaPostsToday(targetUser.id, today);
  if (!stats && posts.length === 0) return interaction.editReply({ content: 'Aucun post trouve pour ' + targetUser.username + ' aujourd hui.' });
  var vaName = (posts[0] && posts[0].va_name) || targetUser.username;
  var embed = embeds.vaStatsEmbed(vaName, stats || { total_views: 0, total_likes: 0, total_comments: 0, total_shares: 0 }, posts);
  await interaction.editReply({ embeds: [embed] });
}

async function handleLeaderboard(interaction) {
  await interaction.deferReply();
  var date = interaction.options.getString('date') || new Date().toISOString().split('T')[0];
  await db.computeDailySummary(date);
  var rankings = await db.getLeaderboard(date);
  if (rankings.length === 0) return interaction.editReply({ content: 'Aucune donnee pour le ' + date + '.' });
  var embed = embeds.leaderboardEmbed(rankings, date);
  await interaction.editReply({ embeds: [embed] });
}

async function handleExport(interaction) {
  if (!interaction.member.roles.cache.has(process.env.MANAGER_ROLE_ID)) return interaction.reply({ content: 'Reserve aux managers.', ephemeral: true });
  await interaction.deferReply({ ephemeral: true });
  var date = interaction.options.getString('date') || new Date().toISOString().split('T')[0];
  var rows = await db.getPostsForExport(date);
  if (rows.length === 0) return interaction.editReply({ content: 'Aucune donnee a exporter pour le ' + date + '.' });
  var Parser = require('json2csv').Parser;
  var parser = new Parser({ fields: ['va_name', 'url', 'ig_post_id', 'created_at', 'status', 'views', 'likes', 'comments', 'shares', 'scraped_at'] });
  var csv = parser.parse(rows);
  var buffer = Buffer.from(csv, 'utf-8');
  await interaction.editReply({ content: 'Export du ' + date + ' (' + rows.length + ' entrees)', files: [{ attachment: buffer, name: 'export-' + date + '.csv' }] });
}

async function handleStatus(interaction) {
  if (!interaction.member.roles.cache.has(process.env.MANAGER_ROLE_ID)) return interaction.reply({ content: 'Reserve aux managers.', ephemeral: true });
  var queueStats = await getQueueStats();
  var activePosts = await db.getActivePosts();
  await interaction.reply({
    content: '**Statut du bot**\nPosts actifs: ' + activePosts.length + '\nQueue - Attente: ' + queueStats.waiting + ' | Actif: ' + queueStats.active + ' | Differe: ' + queueStats.delayed + '\nUptime: ' + Math.floor(process.uptime() / 60) + ' min',
    ephemeral: true,
  });
}

async function handleForceSummary(interaction) {
  if (!interaction.member.roles.cache.has(process.env.MANAGER_ROLE_ID)) return interaction.reply({ content: 'Reserve aux managers.', ephemeral: true });
  await interaction.deferReply({ ephemeral: true });
  await sendDailySummary();
  await interaction.editReply({ content: 'Resume de fin de journee envoye.' });
}

module.exports = { commands: commands, handleCommand: handleCommand };
