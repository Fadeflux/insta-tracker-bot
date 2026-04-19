const { Worker } = require('bullmq');
const IORedis = require('ioredis');
const config = require('../../config');
const logger = require('../utils/logger');
const db = require('../db/queries');
const embeds = require('../utils/embeds');

var discordClient = null;

function setDiscordClient(client) { discordClient = client; }

function createNotifyWorker() {
  var connection = new IORedis(config.redis.url, { maxRetriesPerRequest: null });

  var worker = new Worker(
    'notify',
    async function(job) {
      if (!discordClient) { logger.warn('Discord client not ready'); return; }

      var postId = job.data.postId;
      var currentStats = job.data.currentStats;
      var previousStats = job.data.previousStats;
      var post = await db.getPost(postId);
      if (!post) return;

      try {
        var managersChannel = await discordClient.channels.fetch(config.discord.channels.managers);
        if (!managersChannel) { logger.error('Managers channel not found'); return; }

        if (job.name === 'new-post') {
          var embed = embeds.newPostEmbed(post, currentStats);
          var msg = await managersChannel.send({ embeds: [embed] });
          await db.setManagerMsgId(post.id, msg.id);
          logger.info('Sent new post notification for post ' + postId);
        } else if (job.name === 'hourly-update') {
          var embed2 = embeds.hourlyUpdateEmbed(post, currentStats, previousStats);
          await managersChannel.send({ embeds: [embed2] });
          logger.info('Sent hourly update for post ' + postId);
        }
      } catch (err) {
        logger.error('Notification failed for post ' + postId, { error: err.message });
      }
    },
    { connection: connection, concurrency: 1 }
  );

  worker.on('failed', function(job, err) { logger.error('Notify job failed: ' + (job ? job.id : ''), { error: err.message }); });
  return worker;
}

module.exports = { setDiscordClient: setDiscordClient, createNotifyWorker: createNotifyWorker };
