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
    },
    managerRoleId: process.env.MANAGER_ROLE_ID_TWITTER,
    vaRoleId: process.env.VA_ROLE_ID_TWITTER,
  },
};

// Build guild-to-platform lookup map
var guildToPlatform = {};
if (platforms.instagram.guildId) guildToPlatform[platforms.instagram.guildId] = 'instagram';
if (platforms.twitter.guildId) guildToPlatform[platforms.twitter.guildId] = 'twitter';

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
