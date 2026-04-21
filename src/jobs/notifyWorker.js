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
      var platform = job.data.platform || 'instagram';
      var post = await db.getPost(postId);
      if (!post) return;

      try {
        if (job.name === 'hourly-update') {
          var VIRAL = parseInt(process.env.VIRAL_VIEWS || '5000');
          var prevViews = previousStats ? previousStats.views : 0;
          if (currentStats.views >= VIRAL && prevViews < VIRAL) {
            var viralEmbed = embeds.viralAlertEmbed(post, currentStats, platform);

            // Send to the correct platform's alerts channel
            var platConfig = config.platforms[platform];
            if (platConfig && platConfig.channels.alerts) {
              try {
                var alertsChannel = await discordClient.channels.fetch(platConfig.channels.alerts);
                if (alertsChannel) {
                  await alertsChannel.send({ embeds: [viralEmbed] });
                }
              } catch(e) {
                logger.warn('Could not send viral alert to ' + platform + ': ' + e.message);
              }
            }

            logger.info('[' + platform.toUpperCase() + '] VIRAL ALERT sent for post ' + postId + ' (' + currentStats.views + ' views)');
          }
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
