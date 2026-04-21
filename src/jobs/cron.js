var cron = require('node-cron');
var config = require('../../config');
var db = require('../db/queries');
var embeds = require('../utils/embeds');

var discordClient = null;

function initCronJobs(client) {
  discordClient = client;

  // Expire posts every 5 min (all platforms)
  cron.schedule('*/5 * * * *', async function() {
    try { await db.endExpiredPosts(); } catch (err) { console.error('Expiration cron failed', err.message); }
  });

  // Daily summary at 23:59 Europe/Paris — for each platform
  cron.schedule('59 23 * * *', async function() {
    try { await runForEachPlatform(sendDailySummaryForPlatform); } catch (err) { console.error('Daily summary cron failed', err.message); }
  }, { timezone: config.timezone });

  // 9h GMT+1 (= 8h UTC) - Reminder
  cron.schedule('0 8 * * *', async function() {
    try { await runForEachPlatform(sendPostReminder); } catch (err) { console.error('Reminder cron failed', err.message); }
  }, { timezone: 'UTC' });

  // 10h GMT+1 - Alert 1 post
  cron.schedule('0 9 * * *', async function() {
    try { await runForEachPlatform(function(p) { return sendPostAlert(p, 1); }); } catch (err) { console.error('Alert 1 cron failed', err.message); }
  }, { timezone: 'UTC' });

  // 17h GMT+1 - Alert 2 posts
  cron.schedule('0 16 * * *', async function() {
    try { await runForEachPlatform(function(p) { return sendPostAlert(p, 2); }); } catch (err) { console.error('Alert 2 cron failed', err.message); }
  }, { timezone: 'UTC' });

  // 23h GMT+1 - Alert 3 posts
  cron.schedule('0 22 * * *', async function() {
    try { await runForEachPlatform(function(p) { return sendPostAlert(p, 3); }); } catch (err) { console.error('Alert 3 cron failed', err.message); }
  }, { timezone: 'UTC' });

  // Coaching every 10 min
  cron.schedule('*/10 * * * *', async function() {
    try { await runForEachPlatform(sendCoachingFeedback); } catch (err) { console.error('Coaching cron failed', err.message); }
  });

  // Results every 6h
  cron.schedule('0 6,12,18,0 * * *', async function() {
    try { await runForEachPlatform(send6hResults); } catch (err) { console.error('6h results cron failed', err.message); }
  }, { timezone: config.timezone });

  // Inactivity check every 2 hours
  cron.schedule('0 10,12,14,16,18,20,22 * * *', async function() {
    try { await runForEachPlatform(sendInactivityAlert); } catch (err) { console.error('Inactivity alert failed', err.message); }
  }, { timezone: config.timezone });

  // Performance drop check at 18h
  cron.schedule('0 18 * * *', async function() {
    try { await runForEachPlatform(sendPerformanceDropAlert); } catch (err) { console.error('Perf drop alert failed', err.message); }
  }, { timezone: config.timezone });

  // Daily sweep of inactive accounts — 01:05 Europe/Paris, low traffic window
  cron.schedule('5 1 * * *', async function() {
    try {
      var flipped = await db.markInactiveAccounts();
      if (flipped && flipped.length > 0) {
        console.log('[Accounts] ' + flipped.length + ' account(s) flagged inactive');
      }
    } catch (err) { console.error('Account inactivity sweep failed', err.message); }
  }, { timezone: config.timezone });

  console.log('Cron jobs initialized (multi-platform)');
}

// Run a function for each active platform
async function runForEachPlatform(fn) {
  var platforms = config.getActivePlatforms();
  for (var i = 0; i < platforms.length; i++) {
    try {
      await fn(platforms[i]);
    } catch (err) {
      console.error('Cron error for platform ' + platforms[i] + ':', err.message);
    }
  }
}

// Helper: get channel for a platform
async function getChannel(platform, channelName) {
  if (!discordClient) return null;
  var platConfig = config.platforms[platform];
  if (!platConfig || !platConfig.channels[channelName]) return null;
  try {
    return await discordClient.channels.fetch(platConfig.channels[channelName]);
  } catch(e) {
    return null;
  }
}

// Helper: get VA members for a platform's guild
async function getVaMembers(platform) {
  if (!discordClient) return [];
  var platConfig = config.platforms[platform];
  if (!platConfig || !platConfig.guildId) return [];
  try {
    var guild = await discordClient.guilds.fetch(platConfig.guildId);
    await guild.members.fetch();
    var vaRoleId = platConfig.vaRoleId;
    if (!vaRoleId) return [];
    var members = guild.members.cache.filter(function(m) {
      return m.roles.cache.has(vaRoleId) && !m.user.bot;
    });
    return Array.from(members.values());
  } catch(e) {
    console.error('Failed to fetch VA members for ' + platform + ':', e.message);
    return [];
  }
}

async function sendPostReminder(platform) {
  var alertsChannel = await getChannel(platform, 'alerts');
  if (!alertsChannel) return;
  var platConfig = config.platforms[platform];
  var platformLabel = embeds.getPlatformLabel(platform);
  var emoji = embeds.getPlatformEmoji(platform);

  await alertsChannel.send({
    content: '**' + emoji + ' Rappel du matin ! — ' + platformLabel + '**\n\n' +
      '<@&' + platConfig.vaRoleId + '> C\'est l\'heure de poster votre **1er post** du jour !\n\n' +
      '🇧🇯 Heure du Benin : **9h00**\n' +
      '🇲🇬 Heure de Madagascar : **11h00**\n\n' +
      'Envoyez votre lien dans <#' + platConfig.channels.links + '> des que c\'est publie.'
  });
  console.log('[' + platform.toUpperCase() + '] Morning reminder sent');
}

async function sendPostAlert(platform, requiredPosts) {
  var alertsChannel = await getChannel(platform, 'alerts');
  if (!alertsChannel) return;
  var platConfig = config.platforms[platform];
  var vaMembers = await getVaMembers(platform);
  if (vaMembers.length === 0) return;

  var today = new Date().toISOString().split('T')[0];
  var lateVAs = [];

  for (var j = 0; j < vaMembers.length; j++) {
    var member = vaMembers[j];
    var posts = await db.getVaPostsToday(member.id, today, platform);
    if (posts.length < requiredPosts) {
      lateVAs.push({ id: member.id, name: member.displayName, postCount: posts.length });
    }
  }

  if (lateVAs.length === 0) {
    await alertsChannel.send({ content: '✅ **' + embeds.getPlatformEmoji(platform) + ' Tout le monde est a jour sur ' + embeds.getPlatformLabel(platform) + ' !** Tous les VA ont au moins **' + requiredPosts + ' post(s)**.' });
    return;
  }

  var timeLabels = {
    1: { benin: '10h00', mada: '12h00' },
    2: { benin: '17h00', mada: '19h00' },
    3: { benin: '23h00', mada: '01h00' },
  };
  var times = timeLabels[requiredPosts];

  var lines = lateVAs.map(function(va) {
    return '⚠️ <@' + va.id + '> : **' + va.postCount + '/' + requiredPosts + '** posts';
  });

  await alertsChannel.send({
    content: '**' + embeds.getPlatformEmoji(platform) + ' Alerte Posts — ' + embeds.getPlatformLabel(platform) + ' !**\n\n' +
      '🇧🇯 **' + times.benin + '** (Benin) | 🇲🇬 **' + times.mada + '** (Madagascar)\n\n' +
      'Les VA suivants n\'ont pas encore atteint **' + requiredPosts + ' post(s)** :\n\n' +
      lines.join('\n') + '\n\n' +
      'Envoyez vos liens dans <#' + platConfig.channels.links + '> !'
  });

  console.log('[' + platform.toUpperCase() + '] Alert ' + requiredPosts + ': ' + lateVAs.length + ' VAs late');
}

async function sendDailySummaryForPlatform(platform) {
  var today = new Date().toISOString().split('T')[0];
  var summaries = await db.computeDailySummary(today, platform);

  if (summaries.length === 0) { console.log('[' + platform.toUpperCase() + '] No posts today, skipping'); return; }

  summaries.sort(function(a, b) { return Number(b.total_views) - Number(a.total_views); });

  var resultsChannel = await getChannel(platform, 'results');
  if (!resultsChannel) return;

  var leaderboardEmbed = embeds.leaderboardEmbed(summaries, today, platform);
  await resultsChannel.send({ embeds: [leaderboardEmbed] });

  // Check who didn't reach 6 posts
  var vaMembers = await getVaMembers(platform);
  var lateVAs = [];
  for (var j = 0; j < vaMembers.length; j++) {
    var member = vaMembers[j];
    var posts = await db.getVaPostsToday(member.id, today, platform);
    if (posts.length < 6) {
      lateVAs.push({ name: member.displayName, postCount: posts.length });
    }
  }

  var summaryIds = summaries.map(function(s) { return s.va_discord_id; });
  for (var k = 0; k < vaMembers.length; k++) {
    var member2 = vaMembers[k];
    if (summaryIds.indexOf(member2.id) === -1) {
      var alreadyListed = lateVAs.some(function(v) { return v.name === member2.displayName; });
      if (!alreadyListed) {
        lateVAs.push({ name: member2.displayName, postCount: 0 });
      }
    }
  }

  if (lateVAs.length > 0) {
    var alertsChannel = await getChannel(platform, 'alerts');
    if (alertsChannel) {
      var missingEmbed = embeds.missingPostsEmbed(lateVAs, today, platform);
      await alertsChannel.send({ embeds: [missingEmbed] });
    }
  } else {
    await resultsChannel.send({ embeds: [embeds.allPostsMetEmbed(today, platform)] });
  }

  // Update streaks
  var streakMessages = [];
  for (var l = 0; l < vaMembers.length; l++) {
    var member3 = vaMembers[l];
    var streakResult = await db.updateStreak(member3.id, member3.displayName, today, platform);
    if (streakResult.current >= 5 && streakResult.current % 5 === 0) {
      streakMessages.push('🔥 **' + member3.displayName + '** — **' + streakResult.current + ' jours** consecutifs avec 6+ posts !');
    } else if (streakResult.current === 3) {
      streakMessages.push('⚡ **' + member3.displayName + '** — 3 jours de suite avec 6+ posts !');
    }
    if (streakResult.broken && streakResult.previous >= 3) {
      streakMessages.push('💔 **' + member3.displayName + '** a perdu son streak de ' + streakResult.previous + ' jours');
    }
  }

  if (streakMessages.length > 0) {
    await resultsChannel.send({
      content: '**🏅 Streaks ' + embeds.getPlatformLabel(platform) + '**\n\n' + streakMessages.join('\n')
    });
  }

  console.log('[' + platform.toUpperCase() + '] Daily summary sent: ' + summaries.length + ' VAs, ' + lateVAs.length + ' late');
}

async function send6hResults(platform) {
  var results6hChannel = await getChannel(platform, 'results6h');
  if (!results6hChannel) return;

  var today = new Date().toISOString().split('T')[0];
  await db.computeDailySummary(today, platform);
  var summaries = await db.getDailySummaries(today, platform);

  if (summaries.length === 0) return;

  summaries.sort(function(a, b) { return Number(b.total_views) - Number(a.total_views); });

  var now = new Date();
  var hours = now.toLocaleTimeString('fr-FR', { hour: '2-digit', minute: '2-digit', timeZone: 'Europe/Paris' });

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

  var { EmbedBuilder } = require('discord.js');
  var embed = new EmbedBuilder()
    .setColor(embeds.getPlatformColor(platform))
    .setTitle(embeds.getPlatformEmoji(platform) + ' 📊 Resultats a ' + hours + ' — ' + today)
    .setDescription(
      '**Totaux:** 👁️ ' + fmt(totalViews) + ' vues | ❤️ ' + fmt(totalLikes) + ' likes | 📝 ' + totalPosts + ' posts\n\n' +
      lines.join('\n')
    )
    .setFooter({ text: embeds.getPlatformLabel(platform) + ' | Mise a jour automatique toutes les 6h' })
    .setTimestamp();

  await results6hChannel.send({ embeds: [embed] });
  console.log('[' + platform.toUpperCase() + '] 6h results sent at ' + hours);
}

function fmt(n) {
  if (n == null) return '0';
  return Number(n).toLocaleString('fr-FR');
}

async function sendCoachingFeedback(platform) {
  var coachingChannel = await getChannel(platform, 'coaching');
  if (!coachingChannel) return;
  var platConfig = config.platforms[platform];

  var posts = await db.getPostsForCoaching(platform);
  if (posts.length === 0) return;

  for (var i = 0; i < posts.length; i++) {
    var p = posts[i];
    var views = Number(p.views) || 0;
    var likes = Number(p.likes) || 0;
    var comments = Number(p.comments) || 0;
    var engagement = views > 0 ? (likes + comments) / views : 0;
    var engPct = (engagement * 100).toFixed(1);

    var category, emoji, feedback;
    var typeLabel = platform === 'twitter' ? 'tweet' : 'post';

    if (views >= 500 || engagement >= 0.015) {
      category = 'TRES BON';
      emoji = '🔥';
      feedback = 'Excellent travail ! Ce ' + typeLabel + ' performe tres bien. Continue comme ca !';
    } else if (engagement >= 0.005 && engagement < 0.015) {
      category = 'MOYEN';
      emoji = '👍';
      feedback = 'Pas mal ! Le contenu engage mais les vues sont encore faibles. Essaie de poster aux meilleures heures.';
    } else {
      category = 'FAIBLE';
      emoji = '❌';
      feedback = 'Ce ' + typeLabel + ' a du mal a performer. Regarde les ' + typeLabel + 's des meilleurs VA pour t\'inspirer.';
    }

    var msg = emoji + ' **Feedback 1h — ' + category + ' ' + embeds.getPlatformEmoji(platform) + '**\n\n' +
      '**VA:** <@' + p.va_discord_id + '>\n' +
      '**' + (platform === 'twitter' ? 'Tweet' : 'Post') + ':** ' + p.url + '\n\n' +
      '**Stats a 1h:**\n' +
      '👁️ Vues: **' + views + '**\n' +
      '❤️ Likes: **' + likes + '**\n' +
      '💬 ' + (platform === 'twitter' ? 'Replies' : 'Commentaires') + ': **' + comments + '**\n' +
      '📊 Engagement: **' + engPct + '%**\n\n' +
      '💡 **Conseil:** ' + feedback;

    await coachingChannel.send({ content: msg });
    await db.markCoachingSent(p.id);
    console.log('[' + platform.toUpperCase() + '] Coaching sent for ' + p.ig_post_id + ' (' + category + ')');
  }
}

async function sendInactivityAlert(platform) {
  var alertsChannel = await getChannel(platform, 'alerts');
  if (!alertsChannel) return;

  var inactiveVAs = await db.getInactiveVAs(4, platform);
  if (inactiveVAs.length === 0) return;

  var vaMembers = await getVaMembers(platform);
  var vaIds = new Set();
  vaMembers.forEach(function(m) { vaIds.add(m.id); });

  var relevantInactive = inactiveVAs.filter(function(v) { return vaIds.has(v.va_discord_id); });
  if (relevantInactive.length === 0) return;

  var lines = relevantInactive.map(function(v) {
    var lastPost = new Date(v.last_post_at);
    var hoursAgo = Math.round((Date.now() - lastPost.getTime()) / (1000 * 60 * 60));
    return '⏰ <@' + v.va_discord_id + '> — dernier post il y a **' + hoursAgo + 'h**';
  });

  await alertsChannel.send({
    content: '**⚠️ VA inactifs depuis 4h+ — ' + embeds.getPlatformLabel(platform) + '**\n\n' + lines.join('\n') + '\n\nPensez a poster regulierement !'
  });

  console.log('[' + platform.toUpperCase() + '] Inactivity alert sent: ' + relevantInactive.length + ' VAs');
}

async function sendPerformanceDropAlert(platform) {
  var alertsChannel = await getChannel(platform, 'alerts');
  if (!alertsChannel) return;

  var drops = await db.getPerformanceDrops(platform);
  if (drops.length === 0) return;

  var lines = drops.map(function(d) {
    return '📉 **' + d.va_name + '** — ' + fmt(Number(d.today_views)) + ' vues aujourd\'hui vs ' + fmt(Number(d.avg_views)) + ' en moyenne (' + d.pct_of_avg + '%)';
  });

  await alertsChannel.send({
    content: '**📉 Chute de performance — ' + embeds.getPlatformLabel(platform) + '**\n\nCes VA sont en dessous de 50% de leur moyenne :\n\n' + lines.join('\n')
  });

  console.log('[' + platform.toUpperCase() + '] Performance drop alert: ' + drops.length + ' VAs');
}

module.exports = { initCronJobs: initCronJobs, sendDailySummaryForPlatform: sendDailySummaryForPlatform };
