var cron = require('node-cron');
var config = require('../../config');
var db = require('../db/queries');
var embeds = require('../utils/embeds');

var discordClient = null;

function initCronJobs(client) {
  discordClient = client;

  cron.schedule('*/5 * * * *', async function() {
    try { await db.endExpiredPosts(); } catch (err) { console.error('Expiration cron failed', err.message); }
  });

  // Daily summary at 23:59 Europe/Paris
  cron.schedule('59 23 * * *', async function() {
    try { await sendDailySummary(); } catch (err) { console.error('Daily summary cron failed', err.message); }
  }, { timezone: config.timezone });

  // 9h GMT+1 (= 8h UTC) - Reminder
  cron.schedule('0 8 * * *', async function() {
    try { await sendPostReminder(); } catch (err) { console.error('Reminder cron failed', err.message); }
  }, { timezone: 'UTC' });

  // 10h GMT+1 (= 9h UTC) - Alert 1 post
  cron.schedule('0 9 * * *', async function() {
    try { await sendPostAlert(1); } catch (err) { console.error('Alert 1 cron failed', err.message); }
  }, { timezone: 'UTC' });

  // 17h GMT+1 (= 16h UTC) - Alert 2 posts
  cron.schedule('0 16 * * *', async function() {
    try { await sendPostAlert(2); } catch (err) { console.error('Alert 2 cron failed', err.message); }
  }, { timezone: 'UTC' });

  // 23h GMT+1 (= 22h UTC) - Alert 3 posts
  cron.schedule('0 22 * * *', async function() {
    try { await sendPostAlert(3); } catch (err) { console.error('Alert 3 cron failed', err.message); }
  }, { timezone: 'UTC' });

  console.log('Cron jobs initialized (with alerts + classement)');
}

async function sendPostReminder() {
  if (!discordClient) return;
  try {
    var alertsChannel = await discordClient.channels.fetch(config.discord.channels.alerts);
    if (!alertsChannel) return;

    await alertsChannel.send({
      content: '**?? Rappel du matin !**\n\n' +
        '<@&' + config.discord.vaRoleId + '> C\'est l\'heure de poster votre **1er post** du jour !\n\n' +
        '???? Heure du Benin : **9h00**\n' +
        '???? Heure de Madagascar : **11h00**\n\n' +
        'Envoyez votre lien dans <#' + config.discord.channels.links + '> des que c\'est publie.'
    });
    console.log('Morning reminder sent');
  } catch (err) {
    console.error('Failed to send reminder', err.message);
  }
}

async function sendPostAlert(requiredPosts) {
  if (!discordClient) return;
  try {
    var alertsChannel = await discordClient.channels.fetch(config.discord.channels.alerts);
    if (!alertsChannel) return;

    var guild = await discordClient.guilds.fetch(config.discord.guildId);
    await guild.members.fetch();
    var vaMembers = guild.members.cache.filter(function(m) {
      return m.roles.cache.has(config.discord.vaRoleId) && !m.user.bot;
    });

    if (vaMembers.size === 0) return;

    var today = new Date().toISOString().split('T')[0];
    var lateVAs = [];

    for (var member of vaMembers.values()) {
      var posts = await db.getVaPostsToday(member.id, today);
      if (posts.length < requiredPosts) {
        lateVAs.push({ id: member.id, name: member.displayName, postCount: posts.length });
      }
    }

    if (lateVAs.length === 0) {
      await alertsChannel.send({ content: '? **Tout le monde est a jour !** Tous les VA ont au moins **' + requiredPosts + ' post(s)**.' });
      return;
    }

    var timeLabels = {
      1: { benin: '10h00', mada: '12h00' },
      2: { benin: '17h00', mada: '19h00' },
      3: { benin: '23h00', mada: '01h00' },
    };
    var times = timeLabels[requiredPosts];

    var lines = lateVAs.map(function(va) {
      return '?? <@' + va.id + '> : **' + va.postCount + '/' + requiredPosts + '** posts';
    });

    await alertsChannel.send({
      content: '**?? Alerte Posts !**\n\n' +
        '???? **' + times.benin + '** (Benin) | ???? **' + times.mada + '** (Madagascar)\n\n' +
        'Les VA suivants n\'ont pas encore atteint **' + requiredPosts + ' post(s)** :\n\n' +
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

  // Sort by views descending
  summaries.sort(function(a, b) { return Number(b.total_views) - Number(a.total_views); });

  try {
    var resultsChannel = await discordClient.channels.fetch(config.discord.channels.results);
    if (!resultsChannel) return;

    // Header
    await resultsChannel.send({
      content: '# ?? Resultats du ' + today + '\n??????????????????????????'
    });

    // Individual VA results
    for (var i = 0; i < summaries.length; i++) {
      var s = summaries[i];
      var embed = embeds.dailySummaryEmbed(s.va_name, s, i + 1);
      await resultsChannel.send({ embeds: [embed] });
    }

    // Leaderboard
    var leaderboardEmbed = embeds.leaderboardEmbed(summaries, today);
    await resultsChannel.send({ embeds: [leaderboardEmbed] });

    // Check who didn't reach 6 posts
    var guild = await discordClient.guilds.fetch(config.discord.guildId);
    await guild.members.fetch();
    var vaMembers = guild.members.cache.filter(function(m) {
      return m.roles.cache.has(config.discord.vaRoleId) && !m.user.bot;
    });

    var lateVAs = [];
    for (var member of vaMembers.values()) {
      var posts = await db.getVaPostsToday(member.id, today);
      if (posts.length < 6) {
        lateVAs.push({ name: member.displayName, postCount: posts.length });
      }
    }

    // Also check VAs who posted but aren't in summaries
    var summaryIds = summaries.map(function(s) { return s.va_discord_id; });
    for (var member2 of vaMembers.values()) {
      if (summaryIds.indexOf(member2.id) === -1) {
        var alreadyListed = lateVAs.some(function(v) { return v.name === member2.displayName; });
        if (!alreadyListed) {
          lateVAs.push({ name: member2.displayName, postCount: 0 });
        }
      }
    }

    if (lateVAs.length > 0) {
      var missingEmbed = embeds.missingPostsEmbed(lateVAs, today);
      await resultsChannel.send({ embeds: [missingEmbed] });
    } else {
      var allMetEmbed = embeds.allPostsMetEmbed(today);
      await resultsChannel.send({ embeds: [allMetEmbed] });
    }

    // Also send to managers
    var managersChannel = await discordClient.channels.fetch(config.discord.channels.managers);
    if (managersChannel) {
      await managersChannel.send({ content: '# ?? Rapport fin de journee ? ' + today, embeds: [leaderboardEmbed] });
      if (lateVAs.length > 0) {
        var missingEmbed2 = embeds.missingPostsEmbed(lateVAs, today);
        await managersChannel.send({ embeds: [missingEmbed2] });
      }
    }

    console.log('Daily summary sent: ' + summaries.length + ' VAs, ' + lateVAs.length + ' late');
  } catch (err) {
    console.error('Failed to send daily summary', err.message);
  }
}

module.exports = { initCronJobs: initCronJobs, sendDailySummary: sendDailySummary };
