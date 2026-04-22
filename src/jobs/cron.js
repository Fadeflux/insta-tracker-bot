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

  // ==================== GAMIFICATION ====================

  // Award daily points — 23:58 Europe/Paris (just before the daily summary).
  cron.schedule('58 23 * * *', async function() {
    try { await runForEachPlatform(awardDailyPointsForPlatform); } catch (err) { console.error('Daily points cron failed', err.message); }
  }, { timezone: config.timezone });

  // Every Sunday at 21:00 Europe/Paris: announce weekly winner + resolve duels + create new duels for next week
  cron.schedule('0 21 * * 0', async function() {
    try { await runForEachPlatform(runWeeklyCeremony); } catch (err) { console.error('Weekly ceremony failed', err.message); }
  }, { timezone: config.timezone });

  // Dashboard user revocation sweep — every 6 hours (02h, 08h, 14h, 20h Europe/Paris)
  cron.schedule('0 2,8,14,20 * * *', async function() {
    try { await sweepDashboardUsers(); } catch (err) { console.error('Dashboard revocation sweep failed', err.message); }
  }, { timezone: config.timezone });

  // Viral post notifications — every 10 min
  cron.schedule('*/10 * * * *', async function() {
    try { await runForEachPlatform(notifyViralPosts); } catch (err) { console.error('Viral notification cron failed', err.message); }
  });

  console.log('Cron jobs initialized (multi-platform, gamification, auto-revocation, viral notifications)');
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
  var platConfig = config.platforms[platform];
  var vaMembers = await getVaMembers(platform);
  if (vaMembers.length === 0) return;

  var today = new Date().toISOString().split('T')[0];
  var lateVAs = [];
  var onTrackVAs = [];

  // Fetch current leaderboard once to reuse in personalized DMs
  await db.computeDailySummary(today, platform);
  var leaderboard = await db.getLeaderboard(today, platform);
  var topThree = leaderboard.slice(0, 3);

  for (var j = 0; j < vaMembers.length; j++) {
    var member = vaMembers[j];
    var posts = await db.getVaPostsToday(member.id, today, platform);
    if (posts.length < requiredPosts) {
      lateVAs.push({ id: member.id, name: member.displayName, postCount: posts.length });
    } else {
      onTrackVAs.push({ id: member.id, name: member.displayName, postCount: posts.length });
    }
  }

  var timeLabels = {
    1: { benin: '10h00', mada: '12h00' },
    2: { benin: '17h00', mada: '19h00' },
    3: { benin: '23h00', mada: '01h00' },
  };
  var times = timeLabels[requiredPosts];

  // All good → short public confirmation (no DM needed)
  if (lateVAs.length === 0) {
    if (alertsChannel) {
      await alertsChannel.send({ content: '✅ **' + embeds.getPlatformEmoji(platform) + ' Tout le monde est a jour sur ' + embeds.getPlatformLabel(platform) + ' !** Tous les VA ont au moins **' + requiredPosts + ' post(s)**.' });
    }
    return;
  }

  // Send personalized DM to each late VA
  var dmSuccessCount = 0;
  for (var k = 0; k < lateVAs.length; k++) {
    var va = lateVAs[k];
    var topThreeLine = topThree.length > 0
      ? '\n\n**📊 Top 3 actuel sur ' + embeds.getPlatformLabel(platform) + ' :**\n' +
        topThree.map(function(t, i) { return ['🥇','🥈','🥉'][i] + ' ' + t.va_name + ' — ' + t.post_count + ' posts'; }).join('\n')
      : '';

    var urgency = '';
    if (requiredPosts === 1) urgency = 'La journee commence, garde le rythme ! 💪';
    else if (requiredPosts === 2) urgency = 'Mi-journee — tu peux encore remonter dans le classement.';
    else urgency = 'Derniere ligne droite — plus que quelques heures pour eviter de perdre ton streak.';

    var msg = '👋 Salut ' + va.name + ' !\n\n' +
      '⏰ Il est **' + times.benin + '** (Benin) / **' + times.mada + '** (Madagascar).\n' +
      '📝 Tu es a **' + va.postCount + '/' + requiredPosts + ' posts** aujourd\'hui.\n\n' +
      urgency + topThreeLine + '\n\n' +
      '👉 Envoie tes liens dans le channel #links du serveur ' + embeds.getPlatformLabel(platform) + '.';

    var ok = await sendVaDM(va.id, msg);
    if (ok) dmSuccessCount++;
  }

  // Public summary (without @mentions) — useful for managers watching the channel
  if (alertsChannel) {
    await alertsChannel.send({
      content: '**' + embeds.getPlatformEmoji(platform) + ' Point de la journee — ' + embeds.getPlatformLabel(platform) + '** (' + times.benin + ' Benin / ' + times.mada + ' Mada)\n\n' +
        '✅ **' + onTrackVAs.length + '** VA a jour (' + requiredPosts + '+ posts)\n' +
        '⚠️ **' + lateVAs.length + '** VA en retard — DM envoye a ' + dmSuccessCount + '/' + lateVAs.length + ' (les autres ont bloque les DMs du bot)\n\n' +
        '_Les VA en retard :_ ' + lateVAs.map(function(v) { return v.name + ' (' + v.postCount + '/' + requiredPosts + ')'; }).join(', ')
    });
  }

  console.log('[' + platform.toUpperCase() + '] Alert ' + requiredPosts + ': ' + lateVAs.length + ' VAs late, ' + dmSuccessCount + ' DMs delivered');
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

async function awardDailyPointsForPlatform(platform) {
  var today = new Date().toISOString().split('T')[0];
  try {
    await db.computeDailySummary(today, platform);
    var awarded = await db.awardDailyPoints(today, platform);
    if (awarded.length === 0) {
      console.log('[' + platform.toUpperCase() + '] No points awarded today (no one hit 6 posts)');
      return;
    }
    var resultsChannel = await getChannel(platform, 'results');
    if (!resultsChannel) return;

    var medals = ['🥇', '🥈', '🥉'];
    var lines = awarded.map(function(r, i) {
      return medals[i] + ' **' + r.va_name + '** — **+' + r.points + ' pts** (' + fmt(Number(r.total_views)) + ' vues)';
    });

    var bounds = db.getWeekBounds(today);
    var standings = await db.getWeeklyStandings(bounds.start, bounds.end, platform);
    var standingsLines = standings.slice(0, 5).map(function(s, i) {
      return (i + 1) + '. **' + s.va_name + '** — ' + s.total_points + ' pts';
    });

    await resultsChannel.send({
      content: '**' + embeds.getPlatformEmoji(platform) + ' Points du jour — ' + today + '**\n\n' +
        lines.join('\n') + '\n\n' +
        '**📊 Classement de la semaine :**\n' + (standingsLines.join('\n') || '_Pas encore de points_') + '\n\n' +
        '_Le #1 de la semaine est couronne champion dimanche soir._'
    });
    console.log('[' + platform.toUpperCase() + '] Daily points awarded: ' + awarded.length + ' VAs');
  } catch (err) {
    console.error('awardDailyPointsForPlatform failed for ' + platform + ':', err.message);
  }
}

async function runWeeklyCeremony(platform) {
  try {
    var today = new Date();
    var bounds = db.getWeekBounds(today);

    // Use dedicated duels channel if configured, else fall back to results
    var duelsChannel = await getChannel(platform, 'duels');
    var resultsChannel = await getChannel(platform, 'results');
    var targetChannel = duelsChannel || resultsChannel;

    // 1) Announce weekly winner
    var winner = await db.recordWeeklyWinner(bounds.start, bounds.end, platform);

    if (winner && targetChannel) {
      await targetChannel.send({
        content: '🏆🏆🏆 **CHAMPION DE LA SEMAINE — ' + embeds.getPlatformLabel(platform) + '** 🏆🏆🏆\n\n' +
          '**<@' + winner.va_discord_id + '>** remporte la semaine du ' + bounds.start + ' au ' + bounds.end + ' !\n\n' +
          '🎯 Total points : **' + winner.total_points + '**\n' +
          '👁️ Vues cumulees : **' + fmt(Number(winner.total_views)) + '**\n' +
          '📝 Posts publies : **' + winner.total_posts + '**\n\n' +
          'Bravo ' + winner.va_name + ' ! 🎉\n\n' +
          'A tous les autres : continuez comme ca et vous serez le champion de la semaine prochaine. 💪'
      });
      console.log('[' + platform.toUpperCase() + '] Weekly winner announced: ' + winner.va_name);
    }

    // 2) Resolve current duels and announce results
    var resolvedDuels = await db.resolveWeeklyDuels(bounds.start, bounds.end, platform);
    if (resolvedDuels.length > 0 && targetChannel) {
      var duelLines = resolvedDuels.map(function(d) {
        if (!d.winner_id) {
          return '🤝 <@' + d.va1_discord_id + '> vs <@' + d.va2_discord_id + '> — EGALITE (' + fmt(Number(d.va1_views)) + ' vs ' + fmt(Number(d.va2_views)) + ')';
        }
        var isV1 = d.winner_id === d.va1_discord_id;
        var winId = isV1 ? d.va1_discord_id : d.va2_discord_id;
        var loserId = isV1 ? d.va2_discord_id : d.va1_discord_id;
        var winViews = isV1 ? d.va1_views : d.va2_views;
        var loseViews = isV1 ? d.va2_views : d.va1_views;
        return '⚔️ <@' + winId + '> **bat** <@' + loserId + '> — ' + fmt(Number(winViews)) + ' vs ' + fmt(Number(loseViews)) + ' vues';
      });
      await targetChannel.send({
        content: '**⚔️ Resultats des duels de la semaine — ' + embeds.getPlatformLabel(platform) + '**\n\n' +
          duelLines.join('\n') + '\n\n' +
          '_Perdants : message de felicitations obligatoire au gagnant. Les nouveaux duels de la semaine prochaine arrivent !_'
      });
    }

    // 3) Create next week's duels (starts tomorrow = Monday)
    var nextMonday = new Date(today);
    nextMonday.setDate(today.getDate() + 1); // Sunday + 1 = Monday
    var nextBounds = db.getWeekBounds(nextMonday);
    var vaMembers = await getVaMembers(platform);
    var vaList = vaMembers.map(function(m) { return { id: m.id, name: m.displayName }; });
    var newDuels = await db.createWeeklyDuels(nextBounds.start, nextBounds.end, platform, vaList);

    if (newDuels.length > 0 && targetChannel) {
      var newDuelLines = newDuels.map(function(d, i) {
        return '⚔️ **Duel #' + (i + 1) + '** : <@' + d.va1_discord_id + '> VS <@' + d.va2_discord_id + '>';
      });
      var oddVA = vaList.length % 2 === 1 ? '\n\n_' + vaList[vaList.length - 1].name + ' se repose cette semaine (nombre impair de VA)._' : '';
      await targetChannel.send({
        content: '**⚔️ DUELS DE LA SEMAINE — ' + embeds.getPlatformLabel(platform) + '**\n' +
          'Du lundi ' + nextBounds.start + ' au dimanche ' + nextBounds.end + '\n\n' +
          newDuelLines.join('\n') + '\n\n' +
          '_Celui qui fait le plus de vues cumulees gagne. Le perdant doit poster un message de felicitations au gagnant dimanche soir._' +
          oddVA
      });
    }
  } catch (err) {
    console.error('runWeeklyCeremony failed for ' + platform + ':', err.message);
  }
}

// Send a private DM to a VA. Silently no-ops if DM fails (user blocked bot etc.)
async function sendVaDM(discordId, content) {
  if (!discordClient) return false;
  try {
    var user = await discordClient.users.fetch(discordId);
    await user.send({ content: content });
    return true;
  } catch (e) {
    console.log('[DM] Could not DM ' + discordId + ': ' + e.message);
    return false;
  }
}

// =====================================================================
// ===== VIRAL POST NOTIFICATIONS =====
// =====================================================================
//
// Every 10 min, detect any post that has crossed the VIRAL_VIEWS threshold
// for the first time. Each viral detection triggers:
//   1. A DM to the VA congratulating them
//   2. A public celebration message in #results (group effect — showing
//      others who did well motivates everyone)
//
// Idempotency: the `viral_notifications` table records every (post_id,
// threshold) pair we've notified about, so no post is ever congratulated
// twice (even if views fluctuate around the threshold).

var VIRAL_THRESHOLD = parseInt(process.env.VIRAL_VIEWS || '5000', 10);

async function notifyViralPosts(platform) {
  try {
    var newViral = await db.getNewPostsReachingThreshold(VIRAL_THRESHOLD, platform);
    if (newViral.length === 0) return;

    // Use dedicated #viral channel if configured, else fall back to #results
    var viralChannel = await getChannel(platform, 'viral');
    var resultsChannel = await getChannel(platform, 'results');
    var targetChannel = viralChannel || resultsChannel;

    for (var i = 0; i < newViral.length; i++) {
      var post = newViral[i];
      var views = Number(post.views) || 0;

      // Record first — prevents double-notification on races (unique constraint)
      var recorded = await db.recordViralNotification(post.id, post.va_discord_id, VIRAL_THRESHOLD, views);
      if (!recorded) continue; // Another worker beat us to it

      // 1) DM to the VA — personal, motivating
      var dmMsg =
        '🔥🔥🔥 **FELICITATIONS ! Ton post est VIRAL** 🔥🔥🔥\n\n' +
        'Salut ! Je viens de detecter que ton post vient de passer les **' + fmt(VIRAL_THRESHOLD) + ' vues** sur ' + embeds.getPlatformLabel(platform) + '.\n\n' +
        '📊 **Stats actuelles :**\n' +
        '👁️ **' + fmt(views) + ' vues** (et ca continue de monter !)\n' +
        '❤️ ' + fmt(Number(post.likes) || 0) + ' likes\n' +
        '💬 ' + fmt(Number(post.comments) || 0) + ' commentaires\n\n' +
        '🔗 Ton post : ' + post.url + '\n\n' +
        '💡 **Conseil :** reposte le meme type de contenu dans les 24h pour surfer sur la tendance. Les algos adorent la consistance.\n\n' +
        'Bravo, continue comme ca ! 💪';
      await sendVaDM(post.va_discord_id, dmMsg);

      // 2) Public celebration in #viral (or #results as fallback) — the group effect is the real engine
      if (targetChannel) {
        var celebMsg =
          '🔥 **POST VIRAL !** ' + embeds.getPlatformEmoji(platform) + '\n\n' +
          '<@' + post.va_discord_id + '> vient de franchir les **' + fmt(VIRAL_THRESHOLD) + ' vues** !\n' +
          (post.account_username ? '📱 Compte : **@' + post.account_username + '**\n' : '') +
          '👁️ **' + fmt(views) + '** vues · ❤️ ' + fmt(Number(post.likes) || 0) + ' likes · 💬 ' + fmt(Number(post.comments) || 0) + ' com.\n' +
          '🔗 ' + post.url;
        try {
          await targetChannel.send({ content: celebMsg });
        } catch (e) {
          console.log('[Viral] Could not post to viral/results channel for ' + platform + ': ' + e.message);
        }
      }

      console.log('[' + platform.toUpperCase() + '] Viral notification sent for post ' + post.id + ' (' + fmt(views) + ' views) to ' + post.va_name);
    }
  } catch (err) {
    console.error('notifyViralPosts failed for ' + platform + ':', err.message);
  }
}

// =====================================================================
// ===== DASHBOARD USER AUTO-REVOCATION =====
// =====================================================================
//
// Runs once per day. For each DB-stored dashboard user with a linked
// discord_id, verifies:
//   1. That discord_id is still a member of the guild(s) corresponding
//      to the user's platform(s).
//   2. That the member still has the right Discord role matching their
//      dashboard role (VA role for 'va', manager role for 'manager',
//      and admin Discord IDs list for 'admin').
//
// If either check fails, the account status is set to 'revoked'.
// Revoked users can no longer log in, but their DB record is preserved
// (including history) so an admin can reactivate if needed.
//
// ENV-defined users (DASHBOARD_USERS env var) are NEVER touched by this
// sweep — they are your emergency backdoor.

async function sweepDashboardUsers() {
  if (!discordClient) {
    console.log('[Revoke] Discord client not ready, skipping sweep');
    return;
  }

  var users = await db.getAllDashboardUsers();
  if (users.length === 0) {
    console.log('[Revoke] No DB dashboard users to check');
    return;
  }

  var revokedCount = 0;
  var okCount = 0;
  var errors = 0;

  for (var i = 0; i < users.length; i++) {
    var u = users[i];
    // Skip users already revoked
    if (u.status === 'revoked') continue;
    // Skip users with no discord_id linked — nothing to check against
    if (!u.discord_id) {
      console.log('[Revoke] Skipping ' + u.username + ' (no discord_id linked)');
      continue;
    }

    try {
      var check = await checkUserStillValid(u.discord_id, u.role, u.platform);
      if (check.valid) {
        await db.touchDashboardUserCheck(u.username);
        okCount++;
      } else {
        await db.revokeDashboardUser(u.username, check.reason);
        revokedCount++;
        console.log('[Revoke] ' + u.username + ' revoked — ' + check.reason);

        // Try to notify the user & the admin channel
        notifyRevocation(u, check.reason).catch(function(){});
      }
    } catch (err) {
      errors++;
      console.error('[Revoke] Error checking ' + u.username + ':', err.message);
    }
  }

  console.log('[Revoke] Sweep complete — active:' + okCount + ' revoked:' + revokedCount + ' errors:' + errors);
}

// Determines whether a Discord ID still has valid access for a given
// dashboard role+platform combination.
// Returns { valid: boolean, reason?: string }.
async function checkUserStillValid(discordId, role, platform) {
  // Determine which platforms we need to check
  var platformsToCheck = [];
  if (platform === 'all') {
    platformsToCheck = config.getActivePlatforms();
  } else if (platform.indexOf(',') !== -1) {
    platformsToCheck = platform.split(',').filter(function(p) { return !!config.platforms[p]; });
  } else if (config.platforms[platform]) {
    platformsToCheck = [platform];
  }

  if (platformsToCheck.length === 0) {
    return { valid: false, reason: 'No active platform configured for this user' };
  }

  // Admins: valid if their Discord ID is in ADMIN_DISCORD_IDS AND they're still in at least one guild.
  // Other roles: valid if they're in the guild of their platform AND still have the matching role.
  var foundInAnyGuild = false;
  var hasValidRoleSomewhere = false;
  var checkedPlatforms = [];

  for (var i = 0; i < platformsToCheck.length; i++) {
    var p = platformsToCheck[i];
    var plat = config.platforms[p];
    if (!plat || !plat.guildId) continue;

    var guild;
    try {
      guild = await discordClient.guilds.fetch(plat.guildId);
      // Ensure member cache is warm
      await guild.members.fetch({ user: discordId }).catch(function() {});
    } catch (e) {
      continue;
    }

    var member = guild.members.cache.get(discordId);
    if (!member) continue; // Not in this guild

    foundInAnyGuild = true;
    checkedPlatforms.push(p);

    // Check role based on dashboard role
    if (role === 'admin') {
      // Admin is valid if their Discord ID is in ADMIN_DISCORD_IDS (global check, not guild-specific)
      if (config.isAdmin(discordId)) {
        hasValidRoleSomewhere = true;
      }
    } else if (role === 'manager') {
      if (plat.managerRoleId && member.roles.cache.has(plat.managerRoleId)) {
        hasValidRoleSomewhere = true;
      }
    } else if (role === 'va') {
      if (plat.vaRoleId && member.roles.cache.has(plat.vaRoleId)) {
        hasValidRoleSomewhere = true;
      }
      // Also accept manager role as a valid role for a VA-level dashboard account (managers can see VA pages)
      if (plat.managerRoleId && member.roles.cache.has(plat.managerRoleId)) {
        hasValidRoleSomewhere = true;
      }
    }
  }

  if (!foundInAnyGuild) {
    return { valid: false, reason: 'User left/was banned from all relevant Discord server(s)' };
  }
  if (!hasValidRoleSomewhere) {
    return { valid: false, reason: 'User no longer has the required Discord role (' + role + ') on any platform' };
  }
  return { valid: true };
}

// Try to DM the user and post a note in admin channels when revocation happens.
async function notifyRevocation(user, reason) {
  // DM the user if possible — gives them a chance to contact admin if it's a mistake
  if (user.discord_id) {
    try {
      var u = await discordClient.users.fetch(user.discord_id);
      await u.send({
        content: '🔒 **Acces dashboard Shinra revoque**\n\n' +
          'Ton compte dashboard (**' + user.username + '**) vient d\'etre desactive automatiquement.\n\n' +
          'Raison : ' + reason + '\n\n' +
          'Si tu penses que c\'est une erreur, contacte un admin pour reactiver ton acces.'
      });
    } catch (e) {
      // DM failed (user blocked bot or left server) — silent no-op
    }
  }

  // Log in the first available alerts channel we have access to
  try {
    var platforms = config.getActivePlatforms();
    for (var i = 0; i < platforms.length; i++) {
      var ch = await getChannel(platforms[i], 'alerts');
      if (ch) {
        await ch.send({
          content: '🔒 **Revocation automatique** — compte dashboard `' + user.username + '` desactive.\n' +
            'Raison : ' + reason + '\n' +
            'Role : ' + user.role + ' · Plateforme : ' + user.platform
        });
        break; // Only post in one channel to avoid noise
      }
    }
  } catch (e) {}
}

module.exports = { initCronJobs: initCronJobs, sendDailySummaryForPlatform: sendDailySummaryForPlatform, sendVaDM: sendVaDM, sweepDashboardUsers: sweepDashboardUsers };
