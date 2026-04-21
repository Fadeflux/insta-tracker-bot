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

  // Coaching: check posts ~1h old every 10 minutes
  cron.schedule('*/10 * * * *', async function() {
    try { await sendCoachingFeedback(); } catch (err) { console.error('Coaching cron failed', err.message); }
  });

  // Results every 6h: 6h, 12h, 18h, 00h (Europe/Paris)
  cron.schedule('0 6,12,18,0 * * *', async function() {
    try { await send6hResults(); } catch (err) { console.error('6h results cron failed', err.message); }
  }, { timezone: config.timezone });

  console.log('Cron jobs initialized (with alerts + classement + coaching)');
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

    // Only send the leaderboard — clean and simple
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

    var summaryIds = summaries.map(function(s) { return s.va_discord_id; });
    for (var member2 of vaMembers.values()) {
      if (summaryIds.indexOf(member2.id) === -1) {
        var alreadyListed = lateVAs.some(function(v) { return v.name === member2.displayName; });
        if (!alreadyListed) {
          lateVAs.push({ name: member2.displayName, postCount: 0 });
        }
      }
    }

    // Send missing posts to ALERTS channel instead of results
    if (lateVAs.length > 0) {
      try {
        var alertsChannel = await discordClient.channels.fetch(config.discord.channels.alerts);
        if (alertsChannel) {
          var missingEmbed = embeds.missingPostsEmbed(lateVAs, today);
          await alertsChannel.send({ embeds: [missingEmbed] });
        }
      } catch(e) {
        console.error('Failed to send missing posts to alerts', e.message);
      }
    } else {
      await resultsChannel.send({ embeds: [embeds.allPostsMetEmbed(today)] });
    }

    console.log('Daily summary sent: ' + summaries.length + ' VAs, ' + lateVAs.length + ' late');
  } catch (err) {
    console.error('Failed to send daily summary', err.message);
  }
}

async function send6hResults() {
  if (!discordClient) return;
  if (!config.discord.channels.results6h) return;

  try {
    var results6hChannel = await discordClient.channels.fetch(config.discord.channels.results6h);
    if (!results6hChannel) return;

    var today = new Date().toISOString().split('T')[0];
    await db.computeDailySummary(today);
    var summaries = await db.getDailySummaries(today);

    if (summaries.length === 0) return;

    summaries.sort(function(a, b) { return Number(b.total_views) - Number(a.total_views); });

    var now = new Date();
    var hours = now.toLocaleTimeString('fr-FR', { hour: '2-digit', minute: '2-digit', timeZone: 'Europe/Paris' });

    // Build a clean leaderboard
    var medals = { 0: '🥇', 1: '🥈', 2: '🥉' };
    var lines = summaries.map(function(r, i) {
      var medal = medals[i] || '  ' + (i + 1) + '.';
      var tv = Number(r.total_views);
      var tl = Number(r.total_likes);
      var pc = Number(r.post_count);
      return medal + ' **' + r.va_name + '** → 👁️ ' + fmt(tv) + ' | ❤️ ' + fmt(tl) + ' | 📝 ' + pc + ' posts';
    });

    var totalViews = summaries.reduce(function(a, b) { return a + Number(b.total_views); }, 0);
    var totalLikes = summaries.reduce(function(a, b) { return a + Number(b.total_likes); }, 0);
    var totalPosts = summaries.reduce(function(a, b) { return a + Number(b.post_count); }, 0);

    var embed = new (require('discord.js').EmbedBuilder)()
      .setColor(0x7c3aed)
      .setTitle('📊 Resultats a ' + hours + ' — ' + today)
      .setDescription(
        '**Totaux:** 👁️ ' + fmt(totalViews) + ' vues | ❤️ ' + fmt(totalLikes) + ' likes | 📝 ' + totalPosts + ' posts\n\n' +
        lines.join('\n')
      )
      .setFooter({ text: 'Mise a jour automatique toutes les 6h' })
      .setTimestamp();

    await results6hChannel.send({ embeds: [embed] });
    console.log('6h results sent at ' + hours);
  } catch (err) {
    console.error('Failed to send 6h results', err.message);
  }
}

function fmt(n) {
  if (n == null) return '0';
  return Number(n).toLocaleString('fr-FR');
}

async function sendCoachingFeedback() {
  if (!discordClient) return;
  if (!config.discord.channels.coaching) return;

  try {
    var coachingChannel = await discordClient.channels.fetch(config.discord.channels.coaching);
    if (!coachingChannel) return;

    var posts = await db.getPostsForCoaching();
    if (posts.length === 0) return;

    for (var i = 0; i < posts.length; i++) {
      var p = posts[i];
      var views = Number(p.views) || 0;
      var likes = Number(p.likes) || 0;
      var comments = Number(p.comments) || 0;
      var engagement = views > 0 ? (likes + comments) / views : 0;
      var engPct = (engagement * 100).toFixed(1);

      var category, emoji, color, feedback;

      if (views >= 500 || engagement >= 0.015) {
        // Tres bon
        category = 'TRES BON';
        emoji = '🔥';
        feedback = 'Excellent travail ! Ce post performe tres bien. Continue comme ca, c\'est le bon type de contenu !';
      } else if (engagement >= 0.005 && engagement < 0.015) {
        // Moyen
        category = 'MOYEN';
        emoji = '👍';
        feedback = 'Pas mal ! Le contenu engage mais les vues sont encore faibles. Essaie de poster aux meilleures heures et d\'utiliser des hooks plus accrocheurs dans les 3 premieres secondes.';
      } else {
        // Faible
        category = 'FAIBLE';
        emoji = '❌';
        feedback = 'Ce post a du mal a performer. Regarde les posts des meilleurs VA pour t\'inspirer. Demande conseil dans <#' + (config.discord.channels.managers || '') + '> pour ameliorer ton contenu.';
      }

      var msg = emoji + ' **Feedback 1h — ' + category + '**\n\n' +
        '**VA:** <@' + p.va_discord_id + '>\n' +
        '**Post:** ' + p.url + '\n\n' +
        '**Stats a 1h:**\n' +
        '👁️ Vues: **' + views + '**\n' +
        '❤️ Likes: **' + likes + '**\n' +
        '💬 Commentaires: **' + comments + '**\n' +
        '📊 Engagement: **' + engPct + '%**\n\n' +
        '💡 **Conseil:** ' + feedback;

      await coachingChannel.send({ content: msg });
      await db.markCoachingSent(p.id);
      console.log('Coaching sent for post ' + p.ig_post_id + ' (' + category + ')');
    }
  } catch (err) {
    console.error('Coaching feedback failed', err.message);
  }
}

module.exports = { initCronJobs: initCronJobs, sendDailySummary: sendDailySummary };
