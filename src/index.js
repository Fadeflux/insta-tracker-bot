const { Client, GatewayIntentBits, Partials } = require('discord.js');
const config = require('../config');
const logger = require('./utils/logger');
const { initDb } = require('./db/init');
const { handleMessage } = require('./bot/messageHandler');
const { handleCommand } = require('./bot/commands');
const { initCronJobs } = require('./jobs/cron');
const { setDiscordClient, createNotifyWorker } = require('./jobs/notifyWorker');
const { initBrowser, closeBrowser } = require('./scrapers/instagram');

var client = new Client({
  intents: [
    GatewayIntentBits.Guilds,
    GatewayIntentBits.GuildMessages,
    GatewayIntentBits.MessageContent,
    GatewayIntentBits.GuildMembers,
  ],
  partials: [Partials.Message, Partials.Channel],
});

client.once('ready', async function() {
  logger.info('Bot connected as ' + client.user.tag);
  logger.info('Serving guild: ' + config.discord.guildId);
  setDiscordClient(client);
  createNotifyWorker();
  initCronJobs(client);
  await initBrowser();
  logger.info('All systems operational');
});

client.on('messageCreate', async function(message) {
  try { await handleMessage(message); } catch (err) { logger.error('Message handler error', { error: err.message, stack: err.stack }); }
});

client.on('interactionCreate', async function(interaction) {
  if (!interaction.isChatInputCommand()) return;
  try { await handleCommand(interaction); } catch (err) { logger.error('Command handler error', { error: err.message, stack: err.stack }); }
});

client.on('error', function(err) { logger.error('Discord client error', { error: err.message }); });

async function start() {
  try {
    await initDb();
    logger.info('Database ready');
    await client.login(config.discord.token);
  } catch (err) {
    logger.error('Startup failed', { error: err.message });
    process.exit(1);
  }
}

async function shutdown(signal) {
  logger.info(signal + ' received, shutting down...');
  try { await closeBrowser(); client.destroy(); process.exit(0); } catch(e) { process.exit(1); }
}

process.on('SIGINT', function() { shutdown('SIGINT'); });
process.on('SIGTERM', function() { shutdown('SIGTERM'); });
process.on('unhandledRejection', function(err) { logger.error('Unhandled rejection', { error: err ? err.message : '', stack: err ? err.stack : '' }); });

start();
