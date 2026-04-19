var cron = require('node-cron');
var config = require('../../config');
var db = require('../db/queries');
var embeds = require('../utils/embeds');

var discordClient = null;

function initCronJobs(client) {
  discordClient = client;

  // Every 5 min: end expired posts
  cron.schedule('*/5 * * * *', async function() {
    try { await db.endExpiredPosts(); } catch (err) { console.error('Expiration cron failed', err.message); }
  });

  // Daily summary at 23:59 Europe/Paris
  cron.schedule('59 23 * * *', async function() {
    try { await sendDailySummary(); } catch (err) { console.error('Daily summary cron failed', err.message); }
  }, { timezone: config.timezone });

  // ?? ALERTS (GMT+1 Benin times) ??

  // 9h GMT+1 (= 8h UTC) - Reminder to post
  cron.schedule('0 8 * * *', async function() {
    try { await sendPostReminder(); } catch (err) { console.error('Reminder cron failed', err.message); }
  }, { timezone: 'UTC' });

  // 10h GMT+1 (= 9h UTC) - Alert if no post yet
  cron.schedule('0 9 * * *', async function() {
    try { await sendPostAlert(1); } catch (err) { console.error('Alert 1 cron failed', err.message); }
  }, { timezone: 'UTC' });

  // 17h GMT+1 (= 16h UTC) - Alert if less than 2 posts
  cron.schedule('0 16 * * *', async function() {
    try { await sendPostAlert(2); } catch (err) { console.error('Alert 2 cron failed', err.message); }
  }, { timezone: 'UTC' });

  // 23h GMT+1 (= 22h UTC) - Alert if less than 3 posts
  cron.schedule('0 22 * * *', async function() {
    try { await sendPostAlert(3); } catch (err) { console.error('Alert 3 cron failed', err.message); }
  }, { timezone: 'UTC' });

  console.log('Cron jobs initialized (with alerts)');
}

// ?? Morning reminder at 9h Benin / 11h Madagascar ??
async function sendPostReminder() {
  if (!discordClient) return;
  try {
    var alertsChannel = await discordClient.channels.fetch(config.discord.channels.alerts);
    if (!alertsChannel) { console.error('Alerts channel not found'); return; }

    var vaRoleId = config.discord.vaRoleId;

    await alertsChannel.send({
      content: '**Rappel du matin !**\n\n' +
        '<@&' + vaRoleId + '> C\'est l\'heure de poster votre **1er post** du jour !\n\n' +
        ':flag_bj: Heure du Benin : **9h00**\n' +
        ':flag_mg: Heure de Madagascar : **11h00**\n\n' +
        'Envoyez votre lien dans <#' + config.discord.channels.links + '> des que c\'est publie.'
    });

    console.log('Morning reminder sent');
  } catch (err) {
    console.error('Failed to send reminder', err.message);
  }
}

// ?? Alert for VA who haven't posted enough ??
async function sendPostAlert(requiredPosts) {
  if (!discordClient) return;
  try {
    var alertsChannel = await discordClient.channels.fetch(config.discord.channels.alerts);
    if (!alertsChannel) { console.error('Alerts channel not found'); return; }

    // Get guild and VA role members
    var guild = await discordClient.guilds.fetch(config.discord.guildId);
    await guild.members.fetch();
    var vaMembers = guild.members.cache.filter(function(m) {
      return m.roles.cache.has(config.discord.vaRoleId) && !m.user.bot;
    });

    if (vaMembers.size === 0) { console.log('No VA members found'); return; }

    // Get today's date
    var today = new Date().toISOString().split('T')[0];

    // Check each VA
    var lateVAs = [];
    for (var member of vaMembers.values()) {
      var posts = await db.getVaPostsToday(member.id, today);
      if (posts.length < requiredPosts) {
        lateVAs.push({
          id: member.id,
          name: member.displayName,
          postCount: posts.length,
        });
      }
    }

    if (lateVAs.length === 0) {
      await alertsChannel.send({
        content: '**Tout le monde est a jour !** Tous les VA ont au moins **' + requiredPosts + ' post(s)** aujourd\'hui.'
      });
      console.log('Alert ' + requiredPosts + ': all VAs on track');
      return;
    }

    // Build alert message
    var timeLabels = {
      1: { benin: '10h00', mada: '12h00' },
      2: { benin: '17h00', mada: '19h00' },
      3: { benin: '23h00', mada: '01h00' },
    };
    var times = timeLabels[requiredPosts];

    var lines = lateVAs.map(function(va) {
      return '- <@' + va.id + '> : **' + va.postCount + '/' + requiredPosts + '** posts';
    });

    await alertsChannel.send({
      content: '**Alerte Posts !**\n\n' +
        ':flag_bj: **' + times.benin + '** (Benin) | :flag_mg: **' + times.mada + '** (Madagascar)\n\n' +
        'Les VA suivants n\'ont pas encore atteint **' + requiredPosts + ' post(s)** aujourd\'hui :\n\n' +
        lines.join('\n') + '\n\n' +
        'Envoyez vos liens dans <#' + config.discord.channels.links + '> !'
    });

    console.log('Alert ' + requiredPosts + ': ' + lateVAs.length + ' VAs late');
  } catch (err) {
    console.error('Failed to send alert', err.message);
  }
}

async function sendDailySummary() {
  var today = new Date().toISOString().split('T')[0];
  var summaries = await db.computeDailySummary(today);

  if (summaries.length === 0) { console.log('No posts today, skipping'); return; }
  summaries.sort(function(a, b) { return Number(b.total_views) - Number(a.total_views); });

  try {
    var resultsChannel = await discordClient.channels.fetch(config.discord.channels.results);
    if (!resultsChannel) { console.error('Results channel not found'); return; }

    await resultsChannel.send({ content: '# Resultats du ' + today + '\n---' });

    for (var i = 0; i < summaries.length; i++) {
      var s = summaries[i];
      var embed = embeds.dailySummaryEmbed(s.va_name, s, i + 1);
      await resultsChannel.send({ embeds: [embed] });
    }

    var leaderboardEmbed = embeds.leaderboardEmbed(summaries, today);
    await resultsChannel.send({ embeds: [leaderboardEmbed] });

    var managersChannel = await discordClient.channels.fetch(config.discord.channels.managers);
    if (managersChannel) {
      await managersChannel.send({ content: '# Rapport fin de journee - ' + today, embeds: [leaderboardEmbed] });
    }

    console.log('Daily summary sent for ' + today + ': ' + summaries.length + ' VAs');
  } catch (err) {
    console.error('Failed to send daily summary', err.message);
  }
}

module.exports = { initCronJobs: initCronJobs, sendDailySummary: sendDailySummary };
