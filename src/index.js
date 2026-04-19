var config = require('../config');
console.log('Config loaded OK');
console.log('Token exists:', !!config.discord.token);
console.log('DB URL exists:', !!config.db.url);
console.log('Redis URL exists:', !!config.redis.url);

var { Client, GatewayIntentBits, Partials } = require('discord.js');
console.log('Discord.js loaded OK');

var { initDb } = require('./db/init');
var { handleMessage } = require('./bot/messageHandler');
var { handleCommand } = require('./bot/commands');
var { initCronJobs } = require('./jobs/cron');
var { setDiscordClient, createNotifyWorker } = require('./jobs/notifyWorker');
var { createWebServer } = require('./web/server');
console.log('All modules loaded OK');

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
  console.log('Bot connected as ' + client.user.tag);
  setDiscordClient(client);
  createNotifyWorker();
  initCronJobs(client);
  console.log('All systems operational');
});

client.on('messageCreate', async function(message) {
  try { await handleMessage(message); } catch (err) { console.error('Message error', err); }
});

client.on('interactionCreate', async function(interaction) {
  if (!interaction.isChatInputCommand()) return;
  try { await handleCommand(interaction); } catch (err) { console.error('Command error', err); }
});

client.on('error', function(err) { console.error('Discord error', err); });

async function start() {
  try {
    console.log('Initializing database...');
    await initDb();
    console.log('Database ready');

    // Start web dashboard
    createWebServer();

    console.log('Logging in to Discord...');
    await client.login(config.discord.token);
  } catch (err) {
    console.error('Startup failed:', err);
    process.exit(1);
  }
}

process.on('unhandledRejection', function(err) { console.error('Unhandled rejection:', err); });

start();
