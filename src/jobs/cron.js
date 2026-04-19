const cron = require('node-cron');
const config = require('../../config');
const logger = require('../utils/logger');
const db = require('../db/queries');
const embeds = require('../utils/embeds');

let discordClient = null;

function initCronJobs(client) {
  discordClient = client;

  cron.schedule('*/5 * * * *', async () => {
    try { await db.endExpiredPosts(); } catch (err) { logger.error('Expiration cron failed', { error: err.message }); }
  });

  cron.schedule('59 23 * * *', async () => {
    try { await sendDailySummary(); } catch (err) { logger.error('Daily summary cron failed', { error: err.message }); }
  }, { timezone: config.timezone });

  logger.info('Cron jobs initialized');
}

async function sendDailySummary() {
  const today = new Date().toISOString().split('T')[0];
  const summaries = await db.computeDailySummary(today);

  if (summaries.length === 0) { logger.info('No posts today, skipping'); return; }
  summaries.sort((a, b) => Number(b.total_views) - Number(a.total_views));

  try {
    const resultsChannel = await discordClient.channels.fetch(config.discord.channels.results);
    if (!resultsChannel) { logger.error('Results channel not found'); return; }

    await resultsChannel.send({ content: '# Resultats du ' + today + '\n---' });

    for (let i = 0; i < summaries.length; i++) {
      const s = summaries[i];
      const embed = embeds.dailySummaryEmbed(s.va_name, s, i + 1);
      await resultsChannel.send({ embeds: [embed] });
    }

    const leaderboardEmbed = embeds.leaderboardEmbed(summaries, today);
    await resultsChannel.send({ embeds: [leaderboardEmbed] });

    const managersChannel = await discordClient.channels.fetch(config.discord.channels.managers);
    if (managersChannel) {
      await managersChannel.send({ content: '# Rapport fin de journee - ' + today, embeds: [leaderboardEmbed] });
    }

    logger.info('Daily summary sent for ' + today + ': ' + summaries.length + ' VAs');
  } catch (err) {
    logger.error('Failed to send daily summary', { error: err.message });
  }
}

module.exports = { initCronJobs, sendDailySummary };
