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

  // Daily prune of old in-app notifications (older than 14 days). Runs at 04:00
  // Bénin time which is a quiet hour for the bot.
  cron.schedule('0 4 * * *', async function() {
    try {
      var pruned = await db.pruneOldNotifications();
      if (pruned > 0) console.log('[Notif] Pruned ' + pruned + ' old notifications (>14d)');
    } catch (err) { console.error('Notif prune failed:', err.message); }
  }, { timezone: 'Africa/Porto-Novo' });

  // Daily VA-inactive detection at 10:00 Bénin time. We pick a morning hour so
  // the manager can react during work hours. We check each platform separately
  // (a VA might be active on Twitter but not Instagram).
  // Threshold: 72h with no posts. We ignore VAs gone for >14 days (they probably
  // left the agency and we don't want to re-notify forever).
  cron.schedule('0 10 * * *', async function() {
    try {
      var platforms = config.getActivePlatforms();
      for (var i = 0; i < platforms.length; i++) {
        var plat = platforms[i];
        var inactive = await db.findInactiveVAs(plat, 72);
        for (var j = 0; j < inactive.length; j++) {
          var v = inactive[j];
          var lastPost = new Date(v.last_post_at);
          var hoursSince = Math.round((Date.now() - lastPost.getTime()) / 3600000);
          var dayCount = Math.floor(hoursSince / 24);
          var dayStr = dayCount + ' jour' + (dayCount > 1 ? 's' : '');
          try {
            await db.insertNotification(
              plat,
              'va_inactive',
              null,
              v.va_name,
              '😴 VA inactif',
              v.va_name + ' n\'a pas poste depuis ' + dayStr + ' sur ' + plat + '. A relancer ?',
              null,
              { vaDiscordId: v.va_discord_id, hoursSinceLastPost: hoursSince, lastPostAt: v.last_post_at }
            );
          } catch (e) { /* skip dup or bad row, continue with next VA */ }
        }
        if (inactive.length > 0) {
          console.log('[Notif] va_inactive: ' + inactive.length + ' VA(s) inactifs sur ' + plat);
        }
      }
    } catch (err) { console.error('VA inactive cron failed:', err.message); }
  }, { timezone: 'Africa/Porto-Novo' });

  // Daily summary at 23:59 Africa/Porto-Novo (Benin) — for each platform.
  // Same TZ as personal recap so both are aligned with the team's working day.
  cron.schedule('59 23 * * *', async function() {
    try { await runForEachPlatform(sendDailySummaryForPlatform); } catch (err) { console.error('Daily summary cron failed', err.message); }
  }, { timezone: 'Africa/Porto-Novo' });

  // Personal daily recap DM at 23:59 Africa/Porto-Novo (Benin) — for each platform.
  // Sent to EVERY active VA (even those with 0 posts, so they know they missed the day).
  cron.schedule('59 23 * * *', async function() {
    try { await runForEachPlatform(sendPersonalDailyRecap); } catch (err) { console.error('Personal recap cron failed', err.message); }
  }, { timezone: 'Africa/Porto-Novo' });

  // === SLOT REMINDERS (Instagram only, Benin time) ===
  // 09h00 — "It's time to post (morning slot)"
  cron.schedule('0 9 * * *', async function() {
    try { await sendSlotReminder('morning'); } catch (err) { console.error('Slot reminder morning failed:', err.message); }
  }, { timezone: 'Africa/Porto-Novo' });
  // 10h00 — "You're late on the morning slot" (only to VAs who didn't post 9-10)
  cron.schedule('0 10 * * *', async function() {
    try { await sendLateSlotAlert('morning'); } catch (err) { console.error('Late alert morning failed:', err.message); }
  }, { timezone: 'Africa/Porto-Novo' });
  // 17h00 — "It's time to post (afternoon slot)"
  cron.schedule('0 17 * * *', async function() {
    try { await sendSlotReminder('afternoon'); } catch (err) { console.error('Slot reminder afternoon failed:', err.message); }
  }, { timezone: 'Africa/Porto-Novo' });
  // 18h00 — "You're late on the afternoon slot"
  cron.schedule('0 18 * * *', async function() {
    try { await sendLateSlotAlert('afternoon'); } catch (err) { console.error('Late alert afternoon failed:', err.message); }
  }, { timezone: 'Africa/Porto-Novo' });
  // 23h00 — "It's time to post (evening slot)"
  cron.schedule('0 23 * * *', async function() {
    try { await sendSlotReminder('evening'); } catch (err) { console.error('Slot reminder evening failed:', err.message); }
  }, { timezone: 'Africa/Porto-Novo' });
  // 00h00 — "You're late on the evening slot"
  cron.schedule('0 0 * * *', async function() {
    try { await sendLateSlotAlert('evening'); } catch (err) { console.error('Late alert evening failed:', err.message); }
  }, { timezone: 'Africa/Porto-Novo' });

  // === DELAY > 2H ALERT (every 10 min) ===
  // Detects posts where the link was sent >2h after the actual publication.
  // Sends a DM to the VA + an alert in #alerts of the platform.
  cron.schedule('*/10 * * * *', async function() {
    try { await checkLatePostLinks('instagram'); } catch (err) { console.error('Late link check failed:', err.message); }
  }, { timezone: 'UTC' });

  // 9h Benin - Public reminder (ping VAs in #alerts)
  cron.schedule('0 9 * * *', async function() {
    try { await runForEachPlatform(sendPostReminder); } catch (err) { console.error('Reminder cron failed', err.message); }
  }, { timezone: 'Africa/Porto-Novo' });

  // 10h Benin - Alert: should have at least 1 post
  cron.schedule('0 10 * * *', async function() {
    try { await runForEachPlatform(function(p) { return sendPostAlert(p, 1); }); } catch (err) { console.error('Alert 1 cron failed', err.message); }
  }, { timezone: 'Africa/Porto-Novo' });

  // 17h Benin - Alert: should have at least 2 posts
  cron.schedule('0 17 * * *', async function() {
    try { await runForEachPlatform(function(p) { return sendPostAlert(p, 2); }); } catch (err) { console.error('Alert 2 cron failed', err.message); }
  }, { timezone: 'Africa/Porto-Novo' });

  // 23h Benin - Alert: should have all 3 posts
  cron.schedule('0 23 * * *', async function() {
    try { await runForEachPlatform(function(p) { return sendPostAlert(p, 3); }); } catch (err) { console.error('Alert 3 cron failed', err.message); }
  }, { timezone: 'Africa/Porto-Novo' });

  // Coaching every 10 min
  cron.schedule('*/10 * * * *', async function() {
    try { await runForEachPlatform(sendCoachingFeedback); } catch (err) { console.error('Coaching cron failed', err.message); }
  });

  // Results every 6h
  cron.schedule('0 6,12,18,0 * * *', async function() {
    try { await runForEachPlatform(send6hResults); } catch (err) { console.error('6h results cron failed', err.message); }
  }, { timezone: 'Africa/Porto-Novo' });

  // Inactivity check every 2 hours
  cron.schedule('0 10,12,14,16,18,20,22 * * *', async function() {
    try { await runForEachPlatform(sendInactivityAlert); } catch (err) { console.error('Inactivity alert failed', err.message); }
  }, { timezone: 'Africa/Porto-Novo' });

  // Performance drop check at 18h
  cron.schedule('0 18 * * *', async function() {
    try { await runForEachPlatform(sendPerformanceDropAlert); } catch (err) { console.error('Perf drop alert failed', err.message); }
  }, { timezone: 'Africa/Porto-Novo' });

  // Per-account performance drop check at 19h (1h after per-VA) — catches shadowbans
  cron.schedule('0 19 * * *', async function() {
    try { await runForEachPlatform(sendAccountDropAlert); } catch (err) { console.error('Account drop alert failed', err.message); }
  }, { timezone: 'Africa/Porto-Novo' });

  // Daily DM-blocked digest at 09h00 Europe/Paris
  // Alerts admins about VAs whose DMs are currently blocked (so they can nag them to activate).
  cron.schedule('0 9 * * *', async function() {
    try { await runForEachPlatform(sendDmBlockedDigest); } catch (err) { console.error('DM blocked digest failed', err.message); }
  }, { timezone: 'Africa/Porto-Novo' });

  // Daily sweep of inactive accounts — 01:05 Europe/Paris, low traffic window
  cron.schedule('5 1 * * *', async function() {
    try {
      var flipped = await db.markInactiveAccounts();
      if (flipped && flipped.length > 0) {
        console.log('[Accounts] ' + flipped.length + ' account(s) flagged inactive');
      }
    } catch (err) { console.error('Account inactivity sweep failed', err.message); }
  }, { timezone: 'Africa/Porto-Novo' });

  // ==================== GAMIFICATION ====================

  // Award daily points — 23:58 Europe/Paris (just before the daily summary).
  cron.schedule('58 23 * * *', async function() {
    try { await runForEachPlatform(awardDailyPointsForPlatform); } catch (err) { console.error('Daily points cron failed', err.message); }
  }, { timezone: 'Africa/Porto-Novo' });

  // Every Sunday at 21:00 Europe/Paris: announce weekly winner + resolve duels + create new duels for next week
  cron.schedule('0 21 * * 0', async function() {
    try { await runForEachPlatform(runWeeklyCeremony); } catch (err) { console.error('Weekly ceremony failed', err.message); }
  }, { timezone: 'Africa/Porto-Novo' });

  // Dashboard user revocation sweep — DISABLED automatic schedule.
  // We previously revoked dashboard access automatically every 6h when a user
  // had lost their Discord role, but this caused false-positive revocations
  // for users who were still active (e.g. a manager who genuinely used the
  // dashboard yesterday but happened to lose a role today).
  // The admin can still trigger a sweep manually from the dashboard's admin
  // panel ("Sweep Discord" button) when they want to clean up explicitly.
  // cron.schedule('0 2,8,14,20 * * *', async function() {
  //   try { await sweepDashboardUsers(); } catch (err) { console.error('Dashboard revocation sweep failed', err.message); }
  // }, { timezone: 'Africa/Porto-Novo' });

  // Daily account-level health checks — runs at 11h Bénin time, after the
  // VA inactivity scan (10h). Sends ticket alerts for:
  //   - Dead accounts: 5+ posts (older than 24h) with <100 views each
  //   - Concentrated views: VAs with 3+ accounts where one makes ≥80% of views
  // Each alert is idempotent and only re-fires when severity worsens — see
  // src/jobs/ticketAccountAlerts.js for the per-alert dedup logic.
  cron.schedule('0 11 * * *', async function() {
    try {
      var ticketAccountAlerts = require('./ticketAccountAlerts');
      await ticketAccountAlerts.checkDeadAccounts(db);
      await ticketAccountAlerts.checkConcentratedViews(db);
    } catch (err) {
      console.error('Daily account alerts cron failed', err.message);
    }
  }, { timezone: 'Africa/Porto-Novo' });

  // Daily morning objectives — 7h Bénin time. Posts a personalised message
  // in each VA's ticket channel summarising:
  //   - Per-account post objectives based on day J (J1=1, J2=2, J3+=3)
  //   - Shadowban rest/rampup status if applicable
  //   - Viral reels to repost (every 2 days for up to 14 days post-viral)
  // Also clears expired shadowban states so accounts naturally return
  // to standard rules after their full 17-day rest+rampup cycle.
  cron.schedule('0 7 * * *', async function() {
    try {
      var dailyObjectives = require('./dailyObjectives');
      var accountDayState = require('./accountDayState');
      // First clear stale shadowban states so the morning summary reflects
      // the right post-rampup state.
      await accountDayState.clearOldShadowbanStates(db);
      await dailyObjectives.sendDailyObjectives(db);
    } catch (err) {
      console.error('Daily objectives cron failed', err.message);
    }
  }, { timezone: 'Africa/Porto-Novo' });

  // Daily manager recap — 23h59 Bénin time. Posts a summary of the day in
  // the platform's #recap-quotidien channel (CHANNEL_RECAP_QUOTIDIEN_{PLAT}).
  // Includes posts/views/virals, top 3 VAs, top 3 viral reels, alerts,
  // and tomorrow's expected workload. Skips platforms without a configured
  // channel.
  cron.schedule('59 23 * * *', async function() {
    try {
      var dailyManagerRecap = require('./dailyManagerRecap');
      await dailyManagerRecap.sendDailyRecap(db);
    } catch (err) {
      console.error('Daily manager recap cron failed', err.message);
    }
  }, { timezone: 'Africa/Porto-Novo' });

  // Weekly VA stats — Sunday 20h Bénin time. Sends a personal weekly recap
  // in each VA's ticket channel summarising posts published, total views
  // (with delta vs previous week), best post, viral count, and rank in the
  // agency. Pure motivation feature. Cron runs every Sunday (day 0).
  cron.schedule('0 20 * * 0', async function() {
    try {
      var weeklyVaStats = require('./weeklyVaStats');
      await weeklyVaStats.sendWeeklyStats(db);
    } catch (err) {
      console.error('Weekly VA stats cron failed', err.message);
    }
  }, { timezone: 'Africa/Porto-Novo' });

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

  var embed = embeds.morningPingEmbed(platConfig.vaRoleId, platConfig.channels.links, platform);
  await alertsChannel.send({
    content: '<@&' + platConfig.vaRoleId + '>',
    embeds: [embed],
    allowedMentions: { parse: ['roles'] }
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

  // Public summary embed (without @mentions) — useful for managers watching the channel
  if (alertsChannel) {
    var slotName = 'Point ' + times.benin + ' Benin · ' + times.mada + ' Mada';
    var checkEmbed = embeds.progressCheckEmbed(onTrackVAs, lateVAs, requiredPosts, slotName, platform);
    await alertsChannel.send({ embeds: [checkEmbed] });
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
  var hours = now.toLocaleTimeString('fr-FR', { hour: '2-digit', minute: '2-digit', timeZone: 'Africa/Porto-Novo' });

  var embed = embeds.results6hEmbed(summaries, hours, today, platform);
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
    } else if (engagement >= 0.005 && engagement < 0.015) {
      category = 'MOYEN';
    } else {
      category = 'FAIBLE';
    }

    var feedbackEmbed = embeds.coachingFeedbackEmbed(p, { views: views, likes: likes, comments: comments }, category, platform);
    await coachingChannel.send({ embeds: [feedbackEmbed] });
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

// Send a personal daily recap DM to every active VA on a platform.
// Fires at 23:59 local time (Benin) so VAs see their own numbers for the
// day that just ended — NOT a public leaderboard, just their own stats,
// plus a gentle nudge when they're under target.
async function sendPersonalDailyRecap(platform) {
  try {
    var members = await getVaMembers(platform);
    if (members.length === 0) return;

    // Use Paris "today" because the daily_summaries table is keyed on that.
    // VAs in Benin running this at 23:59 local time will get the recap
    // for the Paris-day that is still ongoing or just ended — close enough,
    // and it matches the data they see elsewhere in the bot.
    // Note: at 23:59 Benin (= 22:59 or 23:59 Paris depending on DST),
    // Paris is still on the same ISO date, so toISOString-split works fine.
    var today = new Date().toISOString().split('T')[0];
    var leaderboard = await db.getLeaderboard(today, platform);
    var platLabel = embeds.getPlatformLabel(platform);
    var platEmoji = embeds.getPlatformEmoji(platform);
    var TARGET = 6; // posts/day target

    // Build quick rank lookup from the leaderboard (only VAs who posted are ranked)
    var rankByVa = {};
    leaderboard.forEach(function(row, idx) { rankByVa[row.va_discord_id] = idx + 1; });

    var sent = 0, failed = 0, skipped = 0;

    for (var i = 0; i < members.length; i++) {
      var m = members[i];
      var vaDiscordId = m.user.id;

      try {
        var stats = await db.getVaDailyStats(vaDiscordId, today, platform);
        var posts = await db.getVaPostsToday(vaDiscordId, today, platform);

        var nbPosts = posts.length;
        var totalViews = stats ? Number(stats.total_views) || 0 : 0;
        var totalLikes = stats ? Number(stats.total_likes) || 0 : 0;
        var totalComments = stats ? Number(stats.total_comments) || 0 : 0;
        var rank = rankByVa[vaDiscordId];
        var nbRanked = leaderboard.length;

        // Find best post (most views)
        var bestPost = null;
        if (posts.length > 0) {
          bestPost = posts.reduce(function(acc, p) {
            var v = Number(p.views) || 0;
            if (!acc || v > (Number(acc.views) || 0)) return p;
            return acc;
          }, null);
        }

        // Build message — tone adapts to whether they hit target or not
        var msg = '📊 **Ton resume du jour - ' + platLabel + '** ' + platEmoji + '\n\n';

        if (nbPosts === 0) {
          msg += '⚠️ **Tu n\'as poste aucun contenu aujourd\'hui.**\n\n' +
            'L\'objectif est de **' + TARGET + ' posts/jour**. Meme 1 post vaut mieux que 0 !\n' +
            'Demain est un nouveau jour. On compte sur toi 💪\n\n' +
            '_Besoin d\'aide ou d\'idees de contenu ? Contacte un manager._';
        } else if (nbPosts < TARGET) {
          msg += '📝 **Posts du jour : ' + nbPosts + '/' + TARGET + '**\n' +
            '👁️ **' + fmt(totalViews) + '** vues cumulees\n' +
            '❤️ ' + fmt(totalLikes) + ' likes · 💬 ' + fmt(totalComments) + ' commentaires\n';
          if (rank) msg += '🏅 Classement du jour : **' + rank + 'e** sur ' + nbRanked + '\n';
          msg += '\n⚠️ Tu as fait moins que l\'objectif de ' + TARGET + ' posts. ';
          msg += 'Si quelque chose t\'a bloque aujourd\'hui (idees, soucis technique, fatigue...), n\'hesite pas a en parler a un manager.\n';
          if (bestPost) {
            msg += '\n🔝 **Ton meilleur post du jour :**\n' +
              '👁️ ' + fmt(Number(bestPost.views) || 0) + ' vues — ' + bestPost.url + '\n';
          }
          msg += '\nOn se retrouve demain 💪';
        } else {
          // Hit or exceeded target
          msg += '🎉 **Objectif atteint ! ' + nbPosts + '/' + TARGET + ' posts** 🎉\n\n' +
            '👁️ **' + fmt(totalViews) + '** vues cumulees\n' +
            '❤️ ' + fmt(totalLikes) + ' likes · 💬 ' + fmt(totalComments) + ' commentaires\n';
          if (rank) msg += '🏅 Classement du jour : **' + rank + 'e** sur ' + nbRanked + '\n';
          if (bestPost) {
            msg += '\n🔝 **Ton meilleur post du jour :**\n' +
              '👁️ ' + fmt(Number(bestPost.views) || 0) + ' vues — ' + bestPost.url + '\n';
          }
          msg += '\nBravo, continue sur ta lancee ! 🔥';
        }

        var ok = await sendVaDM(vaDiscordId, msg);
        if (ok) sent++; else failed++;
      } catch (err) {
        failed++;
        console.log('[PersonalRecap] Failed for ' + vaDiscordId + ': ' + err.message);
      }
    }

    console.log('[' + platform.toUpperCase() + '] Personal recap DMs — sent:' + sent + ' failed:' + failed + ' skipped:' + skipped);
  } catch (err) {
    console.error('sendPersonalDailyRecap failed for ' + platform + ':', err.message);
  }
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

  // Also DM each VA concerned — personal touch, invites them to talk to a manager.
  for (var i = 0; i < drops.length; i++) {
    var d = drops[i];
    if (!d.va_discord_id) continue;
    var dmMsg =
      '📉 **Chute de performance detectee sur ' + embeds.getPlatformLabel(platform) + '**\n\n' +
      'Salut ! Tes stats du jour sont bien en dessous de ta moyenne habituelle :\n\n' +
      '👁️ **' + fmt(Number(d.today_views)) + '** vues aujourd\'hui\n' +
      '📊 vs **' + fmt(Number(d.avg_views)) + '** en moyenne (**' + d.pct_of_avg + '%** de ta moyenne)\n\n' +
      'C\'est pas forcement grave — ca peut arriver (un post moins bon, un jour creux...). ' +
      'Mais si ca dure plusieurs jours, ou si tu sens qu\'il y a un probleme :\n\n' +
      '💬 **Contacte un manager** pour en parler. On est la pour t\'aider a debloquer la situation (nouvelle angle, changement de niche, coaching, etc).\n\n' +
      'Ne reste pas seul face a une baisse de perf 💪';
    await sendVaDM(d.va_discord_id, dmMsg);
  }

  console.log('[' + platform.toUpperCase() + '] Performance drop alert: ' + drops.length + ' VAs (channel + DM)');
}

// Per-account performance drop — detects accounts shadowbanned or losing
// reach while the VA is still posting normally. Alert goes to #alerts so
// the manager can review/rotate the account before more content is burnt.
async function sendAccountDropAlert(platform) {
  var alertsChannel = await getChannel(platform, 'alerts');
  if (!alertsChannel) return;

  var drops = await db.getAccountPerformanceDrops(0.5, platform);
  if (drops.length === 0) return;

  // Load shadowban candidates (same accounts but with engagement rate analysis)
  // We match by username to enrich each drop row with a shadowban diagnosis.
  var shadowbanRows = await db.getShadowbanCandidates(platform);
  var sbByUsername = {};
  shadowbanRows.forEach(function(r) { sbByUsername[r.username] = r; });

  // Format each drop with its diagnosis (shadowban / content / mixed)
  var labelByDiagnosis = {
    shadowban: '🚨 SHADOWBAN probable',
    content: '📉 Contenu en baisse',
    mixed: '⚠️ Mixte',
    ok: '',
  };

  var lines = drops.map(function(d) {
    var sb = sbByUsername[d.username];
    var diagText = '';
    if (sb) {
      var score = db.computeShadowbanScore(sb);
      var lbl = labelByDiagnosis[score.diagnosis] || '';
      if (lbl) {
        diagText = ' — **' + lbl + '** (score ' + score.shadowban_score + '/100)';
      }
    }
    return '⚠️ **@' + d.username + '** (VA: ' + (d.va_name || '—') + ') — ' +
      fmt(Number(d.recent_avg)) + ' vues/post (3j) vs ' +
      fmt(Number(d.baseline_avg)) + ' en moyenne (**' + d.pct_of_baseline + '%**)' + diagText;
  });

  // Limit to top 10 worst to avoid spamming the channel
  var maxLines = Math.min(lines.length, 10);
  var tail = lines.length > 10 ? '\n\n_...et ' + (lines.length - 10) + ' autre(s) compte(s) en chute._' : '';

  await alertsChannel.send({
    content: '**📉 Comptes en chute — ' + embeds.getPlatformLabel(platform) + '**\n\n' +
      'Ces comptes performent a <50% de leur moyenne des 7j precedents :\n\n' +
      lines.slice(0, maxLines).join('\n') + tail + '\n\n' +
      '💡 **Comment lire** :\n' +
      '🚨 **SHADOWBAN probable** = reach en chute mais engagement rate stable → probleme algo, pas contenu\n' +
      '📉 **Contenu en baisse** = reach ET engagement rate chutent ensemble → contenu moins bon\n' +
      '⚠️ **Mixte** = les deux baissent partiellement → a surveiller'
  });

  // Group drops by VA so each VA gets a single DM covering all their affected accounts.
  var dropsByVa = {};
  drops.forEach(function(d) {
    if (!d.va_discord_id) return;
    if (!dropsByVa[d.va_discord_id]) dropsByVa[d.va_discord_id] = [];
    dropsByVa[d.va_discord_id].push(d);
  });

  var vaIds = Object.keys(dropsByVa);
  for (var v = 0; v < vaIds.length; v++) {
    var vaId = vaIds[v];
    var accDrops = dropsByVa[vaId];
    var hasShadowban = false;
    var accLines = accDrops.map(function(d) {
      var sb = sbByUsername[d.username];
      var diagText = '';
      if (sb) {
        var score = db.computeShadowbanScore(sb);
        if (score.diagnosis === 'shadowban') {
          hasShadowban = true;
          diagText = ' **[SHADOWBAN probable]**';
        } else if (score.diagnosis === 'content') {
          diagText = ' **[Contenu en baisse]**';
        } else if (score.diagnosis === 'mixed') {
          diagText = ' **[Signal mixte]**';
        }
      }
      return '• **@' + d.username + '** — ' +
        fmt(Number(d.recent_avg)) + ' vues/post (3j) vs ' +
        fmt(Number(d.baseline_avg)) + ' avant (**' + d.pct_of_baseline + '%**)' + diagText;
    }).join('\n');

    var suffix = accDrops.length > 1 ? ' comptes sont' : ' compte est';
    var conseil = hasShadowban
      ? '**Potentiel shadowban detecte** : reach en chute mais ton engagement rate reste stable, ce qui indique un probleme cote algo Instagram (pas ton contenu).\n\n' +
        '💬 **Contacte un manager** pour decider :\n' +
        '• Pause de 48h minimum sur le(s) compte(s) concerne(s) ?\n' +
        '• Verifier avec un autre compte si les hashtags passent ?\n' +
        '• Strategie de relance (moins de hashtags, pas de lien bio...)\n'
      : 'Le reach ET l\'engagement rate baissent ensemble, ce qui suggere un probleme de contenu plutot qu\'un shadowban.\n\n' +
        '💬 **Contacte un manager** pour decider :\n' +
        '• Changement d\'angle / de niche ?\n' +
        '• Analyse des posts qui marchaient avant ?\n' +
        '• Rotation vers un autre compte ?\n';

    var dmMsg =
      '⚠️ **Alerte : ' + accDrops.length + ' de tes compte' + (accDrops.length > 1 ? 's' : '') + ' en chute sur ' + embeds.getPlatformLabel(platform) + '**\n\n' +
      (accDrops.length > 1 ? 'Ces comptes performent' : 'Ce compte performe') + ' a <50% de leur moyenne des 7 derniers jours :\n\n' +
      accLines + '\n\n' +
      conseil + '\n' +
      'Plus on agit vite, moins on brule de contenu pour rien 💪';

    await sendVaDM(vaId, dmMsg);
  }

  console.log('[' + platform.toUpperCase() + '] Account drop alert: ' + drops.length + ' accounts across ' + vaIds.length + ' VA(s) (channel + DM)');
}

// Send a digest of VAs whose DMs are currently blocked, so admins can
// nag them to re-enable DMs. Runs once per day at 09h Paris.
// Only alerts if there is at least 1 blocked VA on the platform.
async function sendDmBlockedDigest(platform) {
  try {
    var alertsChannel = await getChannel(platform, 'alerts');
    if (!alertsChannel) return;

    var blocked = await db.getBlockedDmVAs();
    if (!blocked || blocked.length === 0) return;

    // We only want to mention VAs who are CURRENTLY on this platform (have the VA role).
    // That way, an admin of platform X doesn't see a blocked VA from platform Y.
    var platConfig = config.platforms[platform];
    if (!platConfig || !platConfig.guildId || !platConfig.vaRoleId) return;

    var platVaIds = {};
    try {
      var guild = await discordClient.guilds.fetch(platConfig.guildId);
      await guild.members.fetch();
      guild.members.cache.forEach(function(m) {
        if (m.roles.cache.has(platConfig.vaRoleId) && !m.user.bot) {
          platVaIds[m.user.id] = m.displayName || m.user.username;
        }
      });
    } catch (e) {
      console.log('[DM Digest] Could not fetch guild members for ' + platform + ': ' + e.message);
      return;
    }

    var platBlocked = blocked.filter(function(b) { return !!platVaIds[b.discord_id]; });
    if (platBlocked.length === 0) return;

    // Build a compact list
    var lines = platBlocked.map(function(b) {
      var name = platVaIds[b.discord_id] || b.va_name || b.discord_id;
      var since = b.last_fail_at ? new Date(b.last_fail_at).toLocaleDateString('fr-FR', { day: '2-digit', month: 'short' }) : '?';
      return '• <@' + b.discord_id + '> (' + name + ') — bloque depuis le ' + since;
    });

    var msg =
      '📬 **Digest quotidien — DM bloques sur ' + embeds.getPlatformLabel(platform) + '**\n\n' +
      'Ces VA ne recoivent pas les DM du bot (resumes personnels, felicitations virales, alertes de chute) :\n\n' +
      lines.join('\n') + '\n\n' +
      '💡 **Action** : demande-leur d\'activer les DM serveur :\n' +
      '_Maintiens appuye sur l\'icone du serveur → Parametres de confidentialite → Messages prives directs_\n\n' +
      '_Consulte le panneau admin du dashboard pour plus de details._';

    await alertsChannel.send({ content: msg });
    console.log('[' + platform.toUpperCase() + '] DM blocked digest: ' + platBlocked.length + ' VA(s)');
  } catch (err) {
    console.error('sendDmBlockedDigest failed for ' + platform + ':', err.message);
  }
}

// ============================================================================
// SLOT REMINDERS (Instagram only, Benin schedule: 9h, 17h, 23h)
// ============================================================================

var SLOT_INFO = {
  morning:   { label: 'matin',         hour: 9,  emoji: '🌅' },
  afternoon: { label: 'apres-midi',    hour: 17, emoji: '☀️' },
  evening:   { label: 'soir',          hour: 23, emoji: '🌙' },
};

// Get today's date as YYYY-MM-DD in Benin TZ (used for idempotency keys).
function getBeninToday() {
  var p = new Intl.DateTimeFormat('en-CA', { timeZone: 'Africa/Porto-Novo', year: 'numeric', month: '2-digit', day: '2-digit' }).format(new Date());
  return p; // already YYYY-MM-DD in en-CA locale
}

// Build the start/end timestamps (UTC ISO) for a given slot today in Benin.
// e.g. 'morning' -> from today 09:00 Benin, to today 10:00 Benin.
// Returns { fromIso, toIso } in UTC.
function getSlotWindow(slot) {
  var info = SLOT_INFO[slot];
  if (!info) return null;
  // Benin is UTC+1 year-round (no DST). 09h Benin = 08h UTC, etc.
  var today = getBeninToday(); // YYYY-MM-DD
  var pad = function(n) { return n < 10 ? '0' + n : '' + n; };
  var startUtcHour = info.hour - 1;
  var endUtcHour = info.hour;
  var fromIso, toIso;
  if (slot === 'evening') {
    // 23h Benin = 22h UTC; window is 23h-00h Benin = 22h-23h UTC
    fromIso = today + 'T' + pad(startUtcHour) + ':00:00Z';
    toIso   = today + 'T' + pad(endUtcHour)   + ':00:00Z';
  } else {
    fromIso = today + 'T' + pad(startUtcHour) + ':00:00Z';
    toIso   = today + 'T' + pad(endUtcHour)   + ':00:00Z';
  }
  return { fromIso: fromIso, toIso: toIso };
}

// Send a DM to all active Instagram VAs reminding them it's time to post.
// Idempotent: each (slot, day) pair is only sent once even if the cron fires twice.
async function sendSlotReminder(slot) {
  var info = SLOT_INFO[slot];
  if (!info) return;
  var today = getBeninToday();
  var key = 'reminder_' + slot + '_' + today;
  try {
    if (await db.wasReminderSent(key)) {
      console.log('[SlotReminder ' + slot + '] already sent today, skip');
      return;
    }
  } catch (e) { /* table may not exist yet on first run; continue */ }

  var pc = config.platforms.instagram;
  if (!pc || !pc.guildId || !pc.vaRoleId) {
    console.log('[SlotReminder ' + slot + '] Instagram platform not configured, skip');
    return;
  }

  var client = null;
  try { var nw = require('./notifyWorker'); if (nw.getDiscordClient) client = nw.getDiscordClient(); } catch (e) {}
  if (!client) { client = discordClient; }
  if (!client) { console.log('[SlotReminder ' + slot + '] No Discord client, skip'); return; }

  var guild;
  try {
    guild = await client.guilds.fetch(pc.guildId);
    if (guild.members.cache.size < 2) await guild.members.fetch();
  } catch (e) {
    console.log('[SlotReminder ' + slot + '] Failed to fetch guild: ' + e.message);
    return;
  }

  var members = guild.members.cache.filter(function(m) {
    return m.roles.cache.has(pc.vaRoleId) && !m.user.bot;
  });

  var msg =
    info.emoji + ' **C\'est l\'heure de poster — slot du ' + info.label + ' (' + info.hour + 'h)** \n\n' +
    'Hey ! C\'est ton creneau de post du ' + info.label + '. Pense a poster sur Instagram et a envoyer ton lien dans le canal #links des que possible. \n\n' +
    'Tu as 1h pour poster, sinon je te re-pingue 😉';

  var sent = 0, failed = 0;
  for (var [id, m] of members) {
    try {
      await sendVaDM(id, msg);
      sent++;
    } catch (e) {
      failed++;
    }
  }

  console.log('[SlotReminder ' + slot + '] sent=' + sent + ' failed=' + failed);
  try { await db.markReminderSent(key); } catch (e) {}
}

// Send a DM to VAs who did NOT post in the slot window.
// Also runs idempotently per (slot, day).
async function sendLateSlotAlert(slot) {
  var info = SLOT_INFO[slot];
  if (!info) return;
  var today = getBeninToday();
  var key = 'late_' + slot + '_' + today;
  try {
    if (await db.wasReminderSent(key)) {
      console.log('[LateAlert ' + slot + '] already sent today, skip');
      return;
    }
  } catch (e) {}

  var pc = config.platforms.instagram;
  if (!pc || !pc.guildId || !pc.vaRoleId) return;

  var client = null;
  try { var nw = require('./notifyWorker'); if (nw.getDiscordClient) client = nw.getDiscordClient(); } catch (e) {}
  if (!client) client = discordClient;
  if (!client) { console.log('[LateAlert ' + slot + '] No Discord client'); return; }

  var guild;
  try {
    guild = await client.guilds.fetch(pc.guildId);
    if (guild.members.cache.size < 2) await guild.members.fetch();
  } catch (e) { return; }

  var members = guild.members.cache.filter(function(m) {
    return m.roles.cache.has(pc.vaRoleId) && !m.user.bot;
  });

  var window = getSlotWindow(slot);
  if (!window) return;

  var lateCount = 0;
  for (var [id, m] of members) {
    try {
      var n = await db.countPostsBetween(id, 'instagram', window.fromIso, window.toIso);
      if (n === 0) {
        var msg =
          '⚠️ **Tu es en retard sur ton post du ' + info.label + ' (' + info.hour + 'h)** \n\n' +
          'Tu n\'as pas encore envoye de lien depuis le rappel de ' + info.hour + 'h. \n' +
          'Mets ton lien dans le canal #links des que possible pour qu\'on tracke les performances ! \n\n' +
          'Si tu as deja poste mais oublie d\'envoyer le lien, fais-le maintenant 🙏';
        try { await sendVaDM(id, msg); lateCount++; } catch (e) {}
      }
    } catch (e) { /* skip this VA */ }
  }

  console.log('[LateAlert ' + slot + '] late VAs notified: ' + lateCount);
  try { await db.markReminderSent(key); } catch (e) {}
}

// Detect posts where link was sent >2h after the real publication time.
// For each: DM to the VA + alert in #alerts of the platform.
// Marks late_alert_sent=TRUE to avoid double-sending.
async function checkLatePostLinks(platform) {
  var DELAY_THRESHOLD_MIN = 120; // 2 hours

  var late = await db.getLateLinkPosts(DELAY_THRESHOLD_MIN, platform);
  if (!late || late.length === 0) return;

  var alertsChannel = await getChannel(platform, 'alerts');

  for (var i = 0; i < late.length; i++) {
    var p = late[i];
    var delayH = Math.floor(p.link_delay_minutes / 60);
    var delayM = p.link_delay_minutes % 60;
    var delayStr = delayH + 'h' + (delayM < 10 ? '0' : '') + delayM;
    var postedTime = new Date(p.posted_at).toLocaleString('fr-FR', { timeZone: 'Africa/Porto-Novo', day: '2-digit', month: 'short', hour: '2-digit', minute: '2-digit' });
    var sentTime = new Date(p.created_at).toLocaleString('fr-FR', { timeZone: 'Africa/Porto-Novo', day: '2-digit', month: 'short', hour: '2-digit', minute: '2-digit' });

    // 1) DM to VA
    try {
      var dmMsg =
        '⏰ **Delai d\'envoi de lien important detecte** \n\n' +
        'Tu as poste sur **@' + (p.account_username || '?') + '** a **' + postedTime + '**, ' +
        'mais tu as envoye le lien au bot a **' + sentTime + '** (delai : **' + delayStr + '**). \n\n' +
        'Pense a envoyer ton lien des que tu publies — sinon les performances ne sont pas trackees correctement, ' +
        'ce qui peut affecter ton score et le suivi qualite de ton compte.';
      await sendVaDM(p.va_discord_id, dmMsg);
    } catch (e) { /* ignore */ }

    // 2) Alert in #alerts
    if (alertsChannel) {
      try {
        var chMsg =
          '⏰ **Delai d\'envoi de lien — ' + embeds.getPlatformLabel(platform) + '**\n' +
          '<@' + p.va_discord_id + '> (' + (p.va_name || '?') + ') a poste sur **@' + (p.account_username || '?') + '** ' +
          'a ' + postedTime + ' mais a envoye le lien a ' + sentTime + ' (delai : **' + delayStr + '**).';
        await alertsChannel.send({ content: chMsg });
      } catch (e) { /* ignore */ }
    }

    // 3) Mark as alerted
    try { await db.markLateAlertSent(p.id); } catch (e) {}
  }

  console.log('[LateLinkCheck] notified for ' + late.length + ' post(s) on ' + platform);
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

    var bounds = db.getWeekBounds(today);
    var standings = await db.getWeeklyStandings(bounds.start, bounds.end, platform);

    var pointsEmbed = embeds.dailyPointsEmbed(awarded, standings, today, platform);
    await resultsChannel.send({ embeds: [pointsEmbed] });
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
  var vaName = null;
  try {
    var user = await discordClient.users.fetch(discordId);
    vaName = user.username || null;
    await user.send({ content: content });
    // Record success (fire-and-forget)
    db.recordDmAttempt(discordId, vaName, true, null).catch(function(){});
    return true;
  } catch (e) {
    console.log('[DM] Could not DM ' + discordId + ': ' + e.message);
    db.recordDmAttempt(discordId, vaName, false, e.message).catch(function(){});
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
        try {
          var celebEmbed = embeds.viralCelebrationEmbed(post, views, Number(post.likes) || 0, Number(post.comments) || 0, VIRAL_THRESHOLD, platform);
          await targetChannel.send({ embeds: [celebEmbed] });
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

// Notification on revocation — DISABLED.
// The user requested that all dashboard user lifecycle decisions be made
// manually from the admin panel; the bot should not contact users by DM nor
// post in alert channels when access is revoked. Kept as a no-op so callers
// (sweepDashboardUsers, manual revoke endpoints) don't need to be rewired.
async function notifyRevocation(user, reason) {
  // Intentional no-op. To re-enable, restore the previous DM + channel logic.
}

module.exports = { initCronJobs: initCronJobs, sendDailySummaryForPlatform: sendDailySummaryForPlatform, sendVaDM: sendVaDM, sweepDashboardUsers: sweepDashboardUsers, runWeeklyCeremony: runWeeklyCeremony, getDiscordClient: function() { return discordClient; } };
