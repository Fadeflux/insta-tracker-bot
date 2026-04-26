var { EmbedBuilder } = require('discord.js');

var COLORS = {
  instagram: 0xe1306c,
  twitter: 0x1da1f2,
  threads: 0x101010,
  success: 0x2ecc71,
  warning: 0xf39c12,
  error: 0xe74c3c,
  info: 0x3498db,
  neutral: 0x95a5a6,
  gold: 0xffd700,
  silver: 0xc0c0c0,
  bronze: 0xcd7f32,
  viral: 0xff00ff,
};

function getPlatformColor(platform) {
  if (platform === 'twitter') return COLORS.twitter;
  if (platform === 'geelark') return 0x00c853;
  if (platform === 'threads') return COLORS.threads;
  return COLORS.instagram;
}

function getPlatformEmoji(platform) {
  if (platform === 'twitter') return '🐦';
  if (platform === 'geelark') return '📱';
  if (platform === 'threads') return '🧵';
  return '📸';
}

function getPlatformLabel(platform) {
  if (platform === 'twitter') return 'Twitter/X';
  if (platform === 'geelark') return 'Geelark Insta';
  if (platform === 'threads') return 'Threads';
  return 'Instagram';
}

function calcScore(stats) {
  return (stats.likes || 0) + (stats.comments || 0) * 3 + (stats.shares || 0) * 5;
}

function calcEngagement(stats) {
  if (!stats.views || stats.views === 0) return 0;
  return ((stats.likes || 0) + (stats.comments || 0)) / stats.views;
}

function getPerformanceLabel(views) {
  var VIRAL = parseInt(process.env.VIRAL_VIEWS || '5000');
  var BON = parseInt(process.env.BON_VIEWS || '1000');
  var MOYEN = parseInt(process.env.MOYEN_VIEWS || '300');
  if (views >= VIRAL) return { label: 'VIRAL', emoji: '🔥', color: COLORS.viral };
  if (views >= BON) return { label: 'BON', emoji: '✅', color: COLORS.success };
  if (views >= MOYEN) return { label: 'MOYEN', emoji: '➡️', color: COLORS.warning };
  return { label: 'FLOP', emoji: '⬇️', color: COLORS.error };
}

function getVaBadge(totalViews, postCount) {
  var avg = postCount > 0 ? totalViews / postCount : 0;
  if (avg >= 2000) return '⭐ Top';
  if (avg >= 500) return '👍 Bon';
  return '📉 Faible';
}

function formatStats(s, platform) {
  if (!s) return 'Donnees indisponibles';
  var lines = '👁️ Vues: **' + fmt(s.views) + '**\n❤️ Likes: **' + fmt(s.likes) + '**\n💬 ' + (platform === 'twitter' ? 'Replies' : 'Commentaires') + ': **' + fmt(s.comments) + '**';
  if (platform === 'twitter') {
    lines += '\n🔁 Retweets: **' + fmt(s.retweets || 0) + '**';
    lines += '\n💬 Quotes: **' + fmt(s.quote_tweets || 0) + '**';
    lines += '\n🔖 Bookmarks: **' + fmt(s.bookmarks || 0) + '**';
  } else {
    lines += '\n🔄 Republications: **' + fmt(s.shares) + '**';
  }
  return lines;
}

function computeDiff(current, previous) {
  if (!previous) return { views: 0, likes: 0, comments: 0, shares: 0 };
  return {
    views: (current?.views || 0) - (previous?.views || 0),
    likes: (current?.likes || 0) - (previous?.likes || 0),
    comments: (current?.comments || 0) - (previous?.comments || 0),
    shares: (current?.shares || 0) - (previous?.shares || 0),
  };
}

function formatDiff(d) {
  var sign = function(n) { return n >= 0 ? '+' + fmt(n) : '' + fmt(n); };
  return '👁️ ' + sign(d.views) + '\n❤️ ' + sign(d.likes) + '\n💬 ' + sign(d.comments) + '\n🔄 ' + sign(d.shares);
}

function newPostEmbed(post, stats, platform) {
  platform = platform || post.platform || 'instagram';
  var score = calcScore(stats);
  var engagement = calcEngagement(stats);
  var perf = getPerformanceLabel(stats.views || 0);

  return new EmbedBuilder()
    .setColor(perf.color)
    .setTitle(getPlatformEmoji(platform) + ' New post detected')
    .addFields(
      { name: 'VA', value: post.va_name, inline: true },
      { name: 'Post ID', value: post.ig_post_id || 'N/A', inline: true },
      { name: 'Heure', value: '<t:' + Math.floor(new Date(post.created_at).getTime() / 1000) + ':R>', inline: true },
      { name: 'Lien', value: post.url },
      { name: 'Stats initiales', value: formatStats(stats, platform) },
      { name: 'Score', value: '🏆 ' + fmt(score), inline: true },
      { name: 'Engagement', value: '📊 ' + (engagement * 100).toFixed(1) + '%', inline: true },
    )
    .setTimestamp();
}

function hourlyUpdateEmbed(post, currentStats, previousStats, platform) {
  platform = platform || post.platform || 'instagram';
  var diff = computeDiff(currentStats, previousStats);
  var score = calcScore(currentStats);
  var engagement = calcEngagement(currentStats);
  var perf = getPerformanceLabel(currentStats.views || 0);

  return new EmbedBuilder()
    .setColor(perf.color)
    .setTitle(perf.emoji + ' ' + getPlatformEmoji(platform) + ' Mise a jour horaire - ' + perf.label)
    .addFields(
      { name: 'VA', value: post.va_name, inline: true },
      { name: 'Post ID', value: post.ig_post_id || 'N/A', inline: true },
      { name: 'Lien', value: post.url },
      { name: 'Stats actuelles', value: formatStats(currentStats, platform), inline: true },
      { name: 'Evolution (+/-)', value: formatDiff(diff), inline: true },
      { name: 'Performance', value: perf.emoji + ' ' + perf.label + ' | 🏆 Score: ' + fmt(score) + ' | 📊 Engagement: ' + (engagement * 100).toFixed(1) + '%' },
    )
    .setTimestamp();
}

function viralAlertEmbed(post, stats, platform) {
  platform = platform || post.platform || 'instagram';
  var score = calcScore(stats);
  var typeLabel = platform === 'twitter' ? 'TWEET' : 'POST';
  return new EmbedBuilder()
    .setColor(COLORS.viral)
    .setTitle('🔥🔥🔥 ' + typeLabel + ' VIRAL DETECTE ! 🔥🔥🔥')
    .setDescription('Le ' + typeLabel.toLowerCase() + ' de **' + post.va_name + '** vient de passer le seuil viral !')
    .addFields(
      { name: 'VA', value: post.va_name, inline: true },
      { name: 'Post ID', value: post.ig_post_id || 'N/A', inline: true },
      { name: 'Lien', value: post.url },
      { name: 'Stats', value: formatStats(stats, platform) },
      { name: 'Score', value: '🏆 ' + fmt(score), inline: true },
      { name: 'Engagement', value: '📊 ' + (calcEngagement(stats) * 100).toFixed(1) + '%', inline: true },
    )
    .setTimestamp();
}

function vaStatsEmbed(vaName, stats, posts, platform) {
  platform = platform || 'instagram';
  return new EmbedBuilder()
    .setColor(getPlatformColor(platform))
    .setTitle(getPlatformEmoji(platform) + ' Stats du jour - ' + vaName)
    .addFields(
      { name: 'Posts', value: '' + posts.length + '/6', inline: true },
      { name: '👁️ Vues', value: fmt(stats.total_views), inline: true },
      { name: '❤️ Likes', value: fmt(stats.total_likes), inline: true },
      { name: '💬 ' + (platform === 'twitter' ? 'Replies' : 'Commentaires'), value: fmt(stats.total_comments), inline: true },
      { name: '🔄 ' + (platform === 'twitter' ? 'Retweets+Quotes' : 'Republications'), value: fmt(stats.total_shares), inline: true }
    )
    .setFooter({ text: getPlatformLabel(platform) })
    .setTimestamp();
}

function leaderboardEmbed(rankings, date, platform) {
  platform = platform || 'instagram';
  var medals = { 0: '🥇', 1: '🥈', 2: '🥉' };
  var lines = rankings.map(function(r, i) {
    var medal = medals[i] || '` ' + (i + 1 < 10 ? ' ' : '') + (i + 1) + '`';
    var ok = Number(r.post_count) >= 6 ? '✅' : '`' + r.post_count + '/6`';
    var engRate = Number(r.total_views) > 0 ? ((Number(r.total_likes) + Number(r.total_comments)) / Number(r.total_views) * 100).toFixed(1) : '0.0';
    return medal + ' **' + r.va_name + '** · ' + fmt(r.total_views) + ' vues · ' + fmt(r.total_likes) + ' ❤️ · ' + engRate + '% · ' + ok;
  });

  var totalViews = rankings.reduce(function(a, b) { return a + Number(b.total_views || 0); }, 0);
  var totalLikes = rankings.reduce(function(a, b) { return a + Number(b.total_likes || 0); }, 0);
  var totalPosts = rankings.reduce(function(a, b) { return a + Number(b.post_count || 0); }, 0);

  return new EmbedBuilder()
    .setColor(getPlatformColor(platform))
    .setAuthor({ name: '🏆 Classement quotidien · ' + getPlatformLabel(platform) })
    .setTitle(date)
    .setDescription(
      '**' + fmt(totalViews) + '** vues · **' + fmt(totalLikes) + '** likes · **' + totalPosts + '** posts\n\n' +
      (lines.join('\n') || '_Aucune donnee_')
    )
    .setFooter({ text: 'Score = likes + comments×3 + shares×5' })
    .setTimestamp();
}

function missingPostsEmbed(lateVAs, date, platform) {
  platform = platform || 'instagram';
  // Group by post count for visual scan
  var groups = {};
  lateVAs.forEach(function(v) {
    var k = v.postCount;
    if (!groups[k]) groups[k] = [];
    groups[k].push(v.name);
  });

  var sortedKeys = Object.keys(groups).map(Number).sort(function(a, b) { return a - b; });
  var lines = sortedKeys.map(function(k) {
    var icon = k === 0 ? '🔴' : k <= 2 ? '🟠' : '🟡';
    return icon + ' **' + k + '/6** · ' + groups[k].join(', ');
  });

  return new EmbedBuilder()
    .setColor(COLORS.error)
    .setAuthor({ name: '⚠️ VA n\'ayant pas atteint l\'objectif · ' + getPlatformLabel(platform) })
    .setTitle(date)
    .setDescription(
      '**' + lateVAs.length + '** VA' + (lateVAs.length > 1 ? 's' : '') + ' n\'ont pas atteint les 6 posts.\n\n' +
      lines.join('\n')
    )
    .setTimestamp();
}

function allPostsMetEmbed(date, platform) {
  platform = platform || 'instagram';
  return new EmbedBuilder()
    .setColor(COLORS.success)
    .setAuthor({ name: '🎉 Objectif atteint ! · ' + getPlatformLabel(platform) })
    .setTitle(date)
    .setDescription('Tous les VA ont poste au moins **6 posts** aujourd\'hui.\nBravo a toute l\'equipe ! 🙌')
    .setTimestamp();
}

// New embed: 6h periodic results.
function results6hEmbed(rankings, hours, date, platform) {
  platform = platform || 'instagram';
  var medals = { 0: '🥇', 1: '🥈', 2: '🥉' };
  var top15 = rankings.slice(0, 15);
  var lines = top15.map(function(r, i) {
    var medal = medals[i] || '` ' + (i + 1 < 10 ? ' ' : '') + (i + 1) + '`';
    return medal + ' **' + r.va_name + '** · ' + fmt(r.total_views) + ' vues · ' + fmt(r.total_likes) + ' ❤️ · ' + r.post_count + ' posts';
  });

  var totalViews = rankings.reduce(function(a, b) { return a + Number(b.total_views || 0); }, 0);
  var totalLikes = rankings.reduce(function(a, b) { return a + Number(b.total_likes || 0); }, 0);
  var totalPosts = rankings.reduce(function(a, b) { return a + Number(b.post_count || 0); }, 0);

  return new EmbedBuilder()
    .setColor(getPlatformColor(platform))
    .setAuthor({ name: '📊 Resultats ' + hours + ' · ' + getPlatformLabel(platform) })
    .setTitle(date)
    .setDescription(
      '**' + fmt(totalViews) + '** vues · **' + fmt(totalLikes) + '** likes · **' + totalPosts + '** posts\n\n' +
      lines.join('\n') +
      (rankings.length > 15 ? '\n\n_+' + (rankings.length - 15) + ' autres VAs_' : '')
    )
    .setFooter({ text: 'Mise a jour automatique toutes les 6h' })
    .setTimestamp();
}

// New embed: daily points + weekly standings.
function dailyPointsEmbed(awarded, weeklyStandings, date, platform) {
  platform = platform || 'instagram';
  var medals = { 0: '🥇', 1: '🥈', 2: '🥉' };
  var awardedLines = awarded.map(function(r, i) {
    return medals[i] + ' **' + r.va_name + '** · **+' + r.points + ' pts** _(' + fmt(Number(r.total_views)) + ' vues)_';
  });
  var weeklyLines = weeklyStandings.slice(0, 5).map(function(s, i) {
    var medal = medals[i] || '` ' + (i + 1) + '`';
    return medal + ' **' + s.va_name + '** · ' + s.total_points + ' pts';
  });

  return new EmbedBuilder()
    .setColor(COLORS.gold)
    .setAuthor({ name: '🏆 Points du jour · ' + getPlatformLabel(platform) })
    .setTitle(date)
    .addFields(
      { name: '🎯 Top 3 du jour', value: awardedLines.join('\n') || '_Aucun point distribue_', inline: false },
      { name: '📊 Classement de la semaine', value: weeklyLines.join('\n') || '_Pas encore de points_', inline: false }
    )
    .setFooter({ text: 'Le #1 de la semaine est couronne champion dimanche soir' })
    .setTimestamp();
}

// New embed: hourly progress check (10h, 17h, 23h Benin).
function progressCheckEmbed(onTrackVAs, lateVAs, requiredPosts, slotName, platform) {
  platform = platform || 'instagram';
  var color = lateVAs.length === 0 ? COLORS.success : lateVAs.length > onTrackVAs.length ? COLORS.error : COLORS.warning;

  // Build a compact "VAs en retard" line
  var lateLine = '';
  if (lateVAs.length > 0) {
    var grouped = {};
    lateVAs.forEach(function(v) {
      var k = v.postCount;
      if (!grouped[k]) grouped[k] = [];
      grouped[k].push(v.name);
    });
    var keys = Object.keys(grouped).map(Number).sort(function(a, b) { return a - b; });
    lateLine = keys.map(function(k) {
      return '` ' + k + '/' + requiredPosts + ' ` ' + grouped[k].join(', ');
    }).join('\n');
  }

  var embed = new EmbedBuilder()
    .setColor(color)
    .setAuthor({ name: '⏰ ' + slotName + ' · ' + getPlatformLabel(platform) })
    .setDescription(
      '**' + onTrackVAs.length + '** VA a jour (' + requiredPosts + '+ posts)\n' +
      '**' + lateVAs.length + '** VA en retard'
    );

  if (lateVAs.length > 0) {
    embed.addFields({ name: 'VAs en retard', value: lateLine, inline: false });
  }
  embed.setTimestamp();
  return embed;
}

// New embed: viral post celebration in public channel.
function viralCelebrationEmbed(post, views, likes, comments, threshold, platform) {
  platform = platform || post.platform || 'instagram';
  var lines = [];
  if (post.account_username) lines.push('📱 **@' + post.account_username + '**');
  lines.push('👁️ **' + fmt(views) + '** vues · ❤️ ' + fmt(likes) + ' · 💬 ' + fmt(comments));

  return new EmbedBuilder()
    .setColor(COLORS.viral)
    .setAuthor({ name: '🔥 POST VIRAL · ' + getPlatformLabel(platform) })
    .setDescription(
      '<@' + post.va_discord_id + '> vient de franchir les **' + fmt(threshold) + ' vues** !\n\n' +
      lines.join('\n')
    )
    .setURL(post.url)
    .setTitle('Voir le post ↗')
    .setTimestamp();
}

// New embed: 1h coaching feedback (sent in #coaching).
function coachingFeedbackEmbed(post, stats, category, platform) {
  platform = platform || post.platform || 'instagram';
  var color, emoji, conseil, typeLabel = (platform === 'twitter' ? 'tweet' : 'post');

  if (category === 'TRES BON') {
    color = COLORS.success; emoji = '🔥';
    conseil = 'Excellent travail ! Ce ' + typeLabel + ' performe tres bien. Continue comme ca !';
  } else if (category === 'MOYEN') {
    color = COLORS.warning; emoji = '👍';
    conseil = 'Pas mal ! Le contenu engage mais les vues sont encore faibles. Essaie de poster aux meilleures heures.';
  } else {
    color = COLORS.error; emoji = '⚠️';
    conseil = 'Ce ' + typeLabel + ' a du mal a performer. Regarde les ' + typeLabel + 's des meilleurs VA pour t\'inspirer.';
  }

  var engPct = stats.views > 0 ? ((Number(stats.likes || 0) + Number(stats.comments || 0)) / stats.views * 100).toFixed(1) : '0.0';
  var statsLine = '👁️ **' + fmt(stats.views || 0) + '** vues · ❤️ ' + fmt(stats.likes || 0) + ' · 💬 ' + fmt(stats.comments || 0) + ' · 📊 ' + engPct + '%';

  return new EmbedBuilder()
    .setColor(color)
    .setAuthor({ name: emoji + ' Feedback 1h · ' + category + ' · ' + getPlatformLabel(platform) })
    .setDescription(
      '<@' + post.va_discord_id + '>\n' +
      statsLine + '\n\n' +
      '💡 ' + conseil
    )
    .setURL(post.url)
    .setTitle('Voir le ' + typeLabel + ' ↗')
    .setTimestamp();
}

// Public morning ping (replaces the long verbose text).
function morningPingEmbed(vaRoleId, linksChannelId, platform) {
  platform = platform || 'instagram';
  return new EmbedBuilder()
    .setColor(getPlatformColor(platform))
    .setAuthor({ name: '🌅 Rappel du matin · ' + getPlatformLabel(platform) })
    .setDescription(
      '<@&' + vaRoleId + '> C\'est l\'heure de poster votre **1er post** du jour !\n\n' +
      '🇧🇯 Benin **9h** · 🇲🇬 Madagascar **11h**\n' +
      'Envoyez votre lien dans <#' + linksChannelId + '> des publication.'
    )
    .setTimestamp();
}

function fmt(n) {
  if (n == null) return '-';
  return Number(n).toLocaleString('fr-FR');
}

module.exports = {
  COLORS, getPlatformColor, getPlatformEmoji, getPlatformLabel,
  calcScore, calcEngagement, getPerformanceLabel, getVaBadge,
  newPostEmbed, hourlyUpdateEmbed, viralAlertEmbed,
  vaStatsEmbed, leaderboardEmbed,
  missingPostsEmbed, allPostsMetEmbed,
  results6hEmbed, dailyPointsEmbed, progressCheckEmbed,
  viralCelebrationEmbed, coachingFeedbackEmbed, morningPingEmbed,
  formatStats, fmt,
};
