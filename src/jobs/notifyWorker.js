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
          // NOTE: Viral detection is now handled by the scheduled cron
          // in src/jobs/cron.js (notifyViralPosts, runs every 10 min).
          // That system:
          //   - De-duplicates via the viral_notifications table
          //   - Posts to the dedicated #viral channel (with #results fallback)
          //   - DMs the VA personally
          // We intentionally keep this hourly-update worker wired up in case
          // we want to add other real-time notifications later (e.g. coaching
          // triggers, bad performance alerts at the post level). For now it
          // is a no-op.
          //
          // Previous implementation (disabled to avoid duplicate messages in
          // #alertes-posts every hour):
          //
          //   var VIRAL = parseInt(process.env.VIRAL_VIEWS || '5000');
          //   var prevViews = previousStats ? previousStats.views : 0;
          //   if (currentStats.views >= VIRAL && prevViews < VIRAL) {
          //     var viralEmbed = embeds.viralAlertEmbed(post, currentStats, platform);
          //     var platConfig = config.platforms[platform];
          //     if (platConfig && platConfig.channels.alerts) {
          //       var alertsChannel = await discordClient.channels.fetch(platConfig.channels.alerts);
          //       if (alertsChannel) await alertsChannel.send({ embeds: [viralEmbed] });
          //     }
          //   }
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
