require('dotenv').config();

// Per-platform guild configuration
var platforms = {
  instagram: {
    guildId: process.env.GUILD_ID_INSTAGRAM || process.env.GUILD_ID,
    channels: {
      links: process.env.CHANNEL_LINKS_INSTAGRAM || process.env.CHANNEL_LINKS,
      managers: process.env.CHANNEL_MANAGERS_INSTAGRAM || process.env.CHANNEL_MANAGERS,
      results: process.env.CHANNEL_RESULTS_INSTAGRAM || process.env.CHANNEL_RESULTS,
      alerts: process.env.CHANNEL_ALERTS_INSTAGRAM || process.env.CHANNEL_ALERTS,
      coaching: process.env.CHANNEL_COACHING_INSTAGRAM || process.env.CHANNEL_COACHING,
      results6h: process.env.CHANNEL_RESULTS_6H_INSTAGRAM || process.env.CHANNEL_RESULTS_6H,
      duels: process.env.CHANNEL_DUELS_INSTAGRAM,
      viral: process.env.CHANNEL_VIRAL_INSTAGRAM,
    },
    managerRoleId: process.env.MANAGER_ROLE_ID_INSTAGRAM || process.env.MANAGER_ROLE_ID,
    vaRoleId: process.env.VA_ROLE_ID_INSTAGRAM || process.env.VA_ROLE_ID,
  },
  twitter: {
    guildId: process.env.GUILD_ID_TWITTER,
    channels: {
      links: process.env.CHANNEL_LINKS_TWITTER,
      managers: process.env.CHANNEL_MANAGERS_TWITTER,
      results: process.env.CHANNEL_RESULTS_TWITTER,
      alerts: process.env.CHANNEL_ALERTS_TWITTER,
      coaching: process.env.CHANNEL_COACHING_TWITTER,
      results6h: process.env.CHANNEL_RESULTS_6H_TWITTER,
      duels: process.env.CHANNEL_DUELS_TWITTER,
      viral: process.env.CHANNEL_VIRAL_TWITTER,
    },
    managerRoleId: process.env.MANAGER_ROLE_ID_TWITTER,
    vaRoleId: process.env.VA_ROLE_ID_TWITTER,
  },
  geelark: {
    guildId: process.env.GUILD_ID_GEELARK,
    channels: {
      links: process.env.CHANNEL_LINKS_GEELARK,
      managers: process.env.CHANNEL_MANAGERS_GEELARK,
      results: process.env.CHANNEL_RESULTS_GEELARK,
      alerts: process.env.CHANNEL_ALERTS_GEELARK,
      coaching: process.env.CHANNEL_COACHING_GEELARK,
      results6h: process.env.CHANNEL_RESULTS_6H_GEELARK,
      duels: process.env.CHANNEL_DUELS_GEELARK,
      viral: process.env.CHANNEL_VIRAL_GEELARK,
    },
    managerRoleId: process.env.MANAGER_ROLE_ID_GEELARK,
    vaRoleId: process.env.VA_ROLE_ID_GEELARK,
  },
  threads: {
    guildId: process.env.GUILD_ID_THREADS,
    channels: {
      links: process.env.CHANNEL_LINKS_THREADS,
      managers: process.env.CHANNEL_MANAGERS_THREADS,
      results: process.env.CHANNEL_RESULTS_THREADS,
      alerts: process.env.CHANNEL_ALERTS_THREADS,
      coaching: process.env.CHANNEL_COACHING_THREADS,
      results6h: process.env.CHANNEL_RESULTS_6H_THREADS,
      duels: process.env.CHANNEL_DUELS_THREADS,
      viral: process.env.CHANNEL_VIRAL_THREADS,
    },
    managerRoleId: process.env.MANAGER_ROLE_ID_THREADS,
    vaRoleId: process.env.VA_ROLE_ID_THREADS,
  },
};

// Build guild-to-platform lookup map
var guildToPlatform = {};
if (platforms.instagram.guildId) guildToPlatform[platforms.instagram.guildId] = 'instagram';
if (platforms.twitter.guildId) guildToPlatform[platforms.twitter.guildId] = 'twitter';
if (platforms.geelark.guildId) guildToPlatform[platforms.geelark.guildId] = 'geelark';
if (platforms.threads.guildId) guildToPlatform[platforms.threads.guildId] = 'threads';

// Build channel-to-platform lookup map (for message routing)
var channelToPlatform = {};
Object.keys(platforms).forEach(function(p) {
  var plat = platforms[p];
  Object.keys(plat.channels).forEach(function(ch) {
    if (plat.channels[ch]) channelToPlatform[plat.channels[ch]] = p;
  });
});

// Admin Discord IDs (see all platforms)
var adminDiscordIds = (process.env.ADMIN_DISCORD_IDS || '').split(',').map(function(s) { return s.trim(); }).filter(Boolean);

// Helper: get platform config from a guild ID
function getPlatformByGuild(guildId) {
  var p = guildToPlatform[guildId];
  if (!p) return null;
  return { name: p, config: platforms[p] };
}

// Helper: get platform config from a channel ID
function getPlatformByChannel(channelId) {
  var p = channelToPlatform[channelId];
  if (!p) return null;
  return { name: p, config: platforms[p] };
}

// Helper: get all active platforms (ones with a guildId set)
function getActivePlatforms() {
  return Object.keys(platforms).filter(function(p) {
    return !!platforms[p].guildId;
  });
}

// Helper: get all guild IDs for deploy-commands
function getAllGuildIds() {
  return Object.keys(platforms).map(function(p) {
    return platforms[p].guildId;
  }).filter(Boolean);
}

// Check if a Discord user ID is admin
function isAdmin(discordId) {
  return adminDiscordIds.indexOf(discordId) !== -1;
}

module.exports = {
  discord: {
    token: process.env.DISCORD_TOKEN,
    clientId: process.env.DISCORD_CLIENT_ID,
    // Legacy single-guild fields (backward compat — use platforms instead)
    guildId: process.env.GUILD_ID_INSTAGRAM || process.env.GUILD_ID,
    channels: platforms.instagram.channels,
    managerRoleId: platforms.instagram.managerRoleId,
    vaRoleId: platforms.instagram.vaRoleId,
  },
  platforms: platforms,
  guildToPlatform: guildToPlatform,
  channelToPlatform: channelToPlatform,
  adminDiscordIds: adminDiscordIds,
  getPlatformByGuild: getPlatformByGuild,
  getPlatformByChannel: getPlatformByChannel,
  getActivePlatforms: getActivePlatforms,
  getAllGuildIds: getAllGuildIds,
  isAdmin: isAdmin,
  db: {
    url: process.env.DATABASE_URL,
  },
  redis: {
    url: process.env.REDIS_URL || 'redis://localhost:6379',
  },
  scraping: {
    concurrency: parseInt(process.env.SCRAPE_CONCURRENCY || '3', 10),
    retryMax: parseInt(process.env.SCRAPE_RETRY_MAX || '3', 10),
  },
  twitter: {
    bearerToken: process.env.TWITTER_BEARER_TOKEN,
  },
  timezone: process.env.TZ || 'Europe/Paris',
};
