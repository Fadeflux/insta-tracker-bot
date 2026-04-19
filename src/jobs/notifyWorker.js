const { Worker } = require('bullmq');
const IORedis = require('ioredis');
const config = require('../../config');
const logger = require('../utils/logger');
const db = require('../db/queries');
const embeds = require('../utils/embeds');

let discordClient = null;

function setDiscordClient(client) { discordClient = client; }

function createNotifyWorker() {
  const connection = new IORedis(config.redis.url, { maxRetriesPerRequest: null });

  const worker = new Worker(
    'notify',
    async (job) => {
      if (!discordClient) { logger.warn('Discord client not ready'); return; }

      const { postId, currentStats, previousStats } = job.data;
      const post = await db.getPost(postId);
      if (!post) return;

      try {
        const managersChannel = await discordClient.channels.fetch(config.discord.channels.managers);
        if (!managersChannel) { logger.error('Managers channel not found'); return; }

        if (job.name === 'new-post') {
          const embed = embeds.newPostEmbed(post, currentStats);
          const msg = await managersChannel.send({ embeds: [embed] });
          await db.setManagerMsgId(post.id, msg.id);
          logger.info('Sent new post notification for post ' + postId);
        } else if (job.name === 'hourly-update') {
          const embed = embeds.hourlyUpdateEmbed(post, currentStats, previousStats);
          await managersChannel.send({ embeds: [embed] });
          logger.info('Sent hourly update for post ' + postId);
        }
      } catch (err) {
        logger.error('Notification failed for post ' + postId, { error: err.message });
      }
    },
    { connection, concurrency: 1 }
  );

  worker.on('failed', (job, err) => { logger.error('Notify job failed: ' + job?.id, { error: err.message }); });
  return worker;
}

module.exports = { setDiscordClient, createNotifyWorker };
