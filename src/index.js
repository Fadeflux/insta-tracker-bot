var config = require('../config');
console.log('Config loaded OK');
console.log('Token exists:', !!config.discord.token);
console.log('DB URL exists:', !!config.db.url);
console.log('Redis URL exists:', !!config.redis.url);
console.log('Active platforms:', config.getActivePlatforms().join(', '));

var { Client, GatewayIntentBits, Partials, REST, Routes } = require('discord.js');
console.log('Discord.js loaded OK');

var { initDb } = require('./db/init');
var { handleMessage } = require('./bot/messageHandler');
var { handleCommand, commands: slashCommands } = require('./bot/commands');
var { initCronJobs } = require('./jobs/cron');
var { setDiscordClient, createNotifyWorker } = require('./jobs/notifyWorker');
var { createWebServer } = require('./web/server');
var { runBackfill } = require('./jobs/backfillAccounts');
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

// Registers all slash commands with Discord for every configured guild.
// Runs on every startup — Discord handles diffing, so there's no cost if
// the commands are already up to date. This ensures that any new command
// added to commands.js is immediately available in Discord after a deploy.
async function registerSlashCommands() {
  try {
    var clientId = process.env.DISCORD_CLIENT_ID;
    if (!clientId) {
      console.warn('[Commands] DISCORD_CLIENT_ID not set — skipping slash command registration');
      return;
    }

    var body = slashCommands.map(function(cmd) { return cmd.toJSON(); });
    var guildIds = config.getAllGuildIds();

    if (guildIds.length === 0) {
      console.warn('[Commands] No guild IDs configured — skipping registration');
      return;
    }

    var rest = new REST({ version: '10' }).setToken(config.discord.token);

    console.log('[Commands] Registering ' + body.length + ' slash commands on ' + guildIds.length + ' guild(s)...');
    for (var i = 0; i < guildIds.length; i++) {
      var guildId = guildIds[i];
      try {
        await rest.put(Routes.applicationGuildCommands(clientId, guildId), { body: body });
        console.log('[Commands] ✓ Registered on guild ' + guildId);
      } catch (err) {
        console.error('[Commands] ✗ Failed to register on guild ' + guildId + ': ' + err.message);
      }
    }
    console.log('[Commands] All slash commands registered successfully');
  } catch (err) {
    console.error('[Commands] Global registration error: ' + err.message);
    // Don't crash the bot — it can still work with the old commands
  }
}

client.once('ready', async function() {
  console.log('Bot connected as ' + client.user.tag);
  console.log('Guilds: ' + client.guilds.cache.map(function(g) { return g.name + ' (' + g.id + ')'; }).join(', '));
  setDiscordClient(client);

  // Hook the crash-alerts module to the Discord client so it can send #notif-crash messages
  try {
    var crashAlerts = require('./jobs/crashAlerts');
    crashAlerts.setDiscordClient(client);
    console.log('[CrashAlert] Module wired to Discord client (threshold=' + crashAlerts.THRESHOLD + ' consecutive failures)');
  } catch (e) {
    console.error('[CrashAlert] Setup failed:', e.message);
  }

  createNotifyWorker();
  initCronJobs(client);
  // Register slash commands (idempotent — safe to run on every boot)
  registerSlashCommands().catch(function(err) { console.error('[Commands] registerSlashCommands threw:', err.message); });
  // Resume scraping for posts whose tracking window is still open. This catches
  // up any posts whose scrape jobs were lost during the redeploy or whose
  // tracking_end was just extended by the migration.
  try {
    var scrapeQueueModule = require('./jobs/scrapeQueue');
    if (scrapeQueueModule.resumeOrphanScrapes) {
      scrapeQueueModule.resumeOrphanScrapes().catch(function(err) {
        console.error('[Scrape] resumeOrphanScrapes failed:', err.message);
      });
    }
  } catch (e) { console.error('[Scrape] resume require failed:', e.message); }
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

    // Run one-shot account backfill (fast Twitter phase sync, slow IG phase async)
    runBackfill().catch(function(err) { console.error('Backfill error:', err.message); });

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
