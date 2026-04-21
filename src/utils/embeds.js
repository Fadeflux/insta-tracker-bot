var { EmbedBuilder } = require('discord.js');

var COLORS = {
  instagram: 0xe1306c,
  twitter: 0x1da1f2,
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
  return COLORS.instagram;
}

function getPlatformEmoji(platform) {
  if (platform === 'twitter') return '🐦';
  if (platform === 'geelark') return '📱';
  return '📸';
}

function getPlatformLabel(platform) {
  if (platform === 'twitter') return 'Twitter/X';
  if (platform === 'geelark') return 'Geelark Insta';
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
    var medal = medals[i] || '  ' + (i + 1) + '.';
    var postStatus = Number(r.post_count) >= 6 ? '✅' : '⚠️ (' + r.post_count + '/6)';
    var badge = getVaBadge(Number(r.total_views), Number(r.post_count));
    var engRate = Number(r.total_views) > 0 ? ((Number(r.total_likes) + Number(r.total_comments)) / Number(r.total_views) * 100).toFixed(1) : '0';
    return medal + ' **' + r.va_name + '** [' + badge + '] → 👁️ ' + fmt(r.total_views) + ' | ❤️ ' + fmt(r.total_likes) + ' | 📊 ' + engRate + '% | ' + postStatus;
  });
  return new EmbedBuilder()
    .setColor(COLORS.gold)
    .setTitle(getPlatformEmoji(platform) + ' 🏆 Classement du ' + date)
    .setDescription(lines.join('\n') || 'Aucune donnee')
    .setFooter({ text: getPlatformLabel(platform) + ' | Score = likes + comments×3 + shares×5' })
    .setTimestamp();
}

function missingPostsEmbed(lateVAs, date, platform) {
  platform = platform || 'instagram';
  var lines = lateVAs.map(function(va) {
    return '⚠️ **' + va.name + '** → ' + va.postCount + '/6 posts';
  });
  return new EmbedBuilder()
    .setColor(COLORS.error)
    .setTitle('❌ VA n\'ayant pas atteint 6 posts — ' + date)
    .setDescription(lines.join('\n') || 'Tout le monde a atteint l\'objectif !')
    .setFooter({ text: getPlatformLabel(platform) })
    .setTimestamp();
}

function allPostsMetEmbed(date, platform) {
  platform = platform || 'instagram';
  return new EmbedBuilder()
    .setColor(COLORS.success)
    .setTitle('🎉 Objectif atteint ! — ' + date)
    .setDescription('Tous les VA ont poste au moins **6 posts** aujourd\'hui. Bravo a toute l\'equipe !')
    .setFooter({ text: getPlatformLabel(platform) })
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
  formatStats, fmt,
};
