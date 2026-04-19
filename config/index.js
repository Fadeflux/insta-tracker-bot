require('dotenv').config();

module.exports = {
  discord: {
    token: process.env.DISCORD_TOKEN,
    clientId: process.env.DISCORD_CLIENT_ID,
    guildId: process.env.GUILD_ID,
    channels: {
      links: process.env.CHANNEL_LINKS,
      managers: process.env.CHANNEL_MANAGERS,
      results: process.env.CHANNEL_RESULTS,
      alerts: process.env.CHANNEL_ALERTS,
    },
    managerRoleId: process.env.MANAGER_ROLE_ID,
    vaRoleId: process.env.VA_ROLE_ID,
  },
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
  timezone: process.env.TZ || 'Europe/Paris',
};
