var { EmbedBuilder } = require('discord.js');

var COLORS = {
  primary: 0xe1306c,
  success: 0x2ecc71,
  warning: 0xf39c12,
  error: 0xe74c3c,
  info: 0x3498db,
  neutral: 0x95a5a6,
  gold: 0xffd700,
  silver: 0xc0c0c0,
  bronze: 0xcd7f32,
};

function newPostEmbed(post, stats) {
  return new EmbedBuilder()
    .setColor(COLORS.primary)
    .setTitle('New post detected')
    .addFields(
      { name: 'VA', value: post.va_name, inline: true },
      { name: 'Post ID', value: post.ig_post_id || 'N/A', inline: true },
      { name: 'Heure', value: '<t:' + Math.floor(new Date(post.created_at).getTime() / 1000) + ':R>', inline: true },
      { name: 'Lien', value: post.url },
      { name: 'Stats initiales', value: formatStats(stats) }
    )
    .setTimestamp();
}

function hourlyUpdateEmbed(post, currentStats, previousStats) {
  var diff = computeDiff(currentStats, previousStats);
  return new EmbedBuilder()
    .setColor(COLORS.info)
    .setTitle('Mise a jour horaire')
    .addFields(
      { name: 'VA', value: post.va_name, inline: true },
      { name: 'Post ID', value: post.ig_post_id || 'N/A', inline: true },
      { name: 'Lien', value: post.url },
      { name: 'Stats actuelles', value: formatStats(currentStats), inline: true },
      { name: 'Evolution (+/-)', value: formatDiff(diff), inline: true }
    )
    .setTimestamp();
}

function dailySummaryEmbed(vaName, summary, rank) {
  var medals = { 1: '??', 2: '??', 3: '??' };
  var medal = medals[rank] || '#' + rank;
  var rankColors = { 1: COLORS.gold, 2: COLORS.silver, 3: COLORS.bronze };
  var color = rankColors[rank] || COLORS.neutral;

  var postStatus = Number(summary.post_count) >= 6 ? '?' : '??';

  return new EmbedBuilder()
    .setColor(color)
    .setTitle(medal + ' ' + vaName)
    .addFields(
      { name: 'Posts', value: postStatus + ' ' + summary.post_count + '/6', inline: true },
      { name: '??? Vues totales', value: fmt(summary.total_views), inline: true },
      { name: '?? Likes totaux', value: fmt(summary.total_likes), inline: true },
      { name: '?? Commentaires', value: fmt(summary.total_comments), inline: true },
      { name: '?? Republications', value: fmt(summary.total_shares), inline: true }
    )
    .setTimestamp();
}

function vaStatsEmbed(vaName, stats, posts) {
  return new EmbedBuilder()
    .setColor(COLORS.primary)
    .setTitle('Stats du jour - ' + vaName)
    .addFields(
      { name: 'Posts', value: '' + posts.length + '/6', inline: true },
      { name: '??? Vues', value: fmt(stats.total_views), inline: true },
      { name: '?? Likes', value: fmt(stats.total_likes), inline: true },
      { name: '?? Commentaires', value: fmt(stats.total_comments), inline: true },
      { name: '?? Republications', value: fmt(stats.total_shares), inline: true }
    )
    .setTimestamp();
}

function leaderboardEmbed(rankings, date) {
  var medals = { 0: '??', 1: '??', 2: '??' };
  var lines = rankings.map(function(r, i) {
    var medal = medals[i] || '  ' + (i + 1) + '.';
    var postStatus = Number(r.post_count) >= 6 ? '?' : '?? (' + r.post_count + '/6)';
    return medal + ' **' + r.va_name + '** ? ??? ' + fmt(r.total_views) + ' | ?? ' + fmt(r.total_likes) + ' | ' + postStatus;
  });
  return new EmbedBuilder()
    .setColor(COLORS.gold)
    .setTitle('?? Classement du ' + date)
    .setDescription(lines.join('\n') || 'Aucune donnee')
    .setFooter({ text: 'Classe par nombre de vues | ? = 6+ posts | ?? = objectif non atteint' })
    .setTimestamp();
}

function missingPostsEmbed(lateVAs, date) {
  var lines = lateVAs.map(function(va) {
    return '?? **' + va.name + '** ? ' + va.postCount + '/6 posts';
  });
  return new EmbedBuilder()
    .setColor(COLORS.error)
    .setTitle('? VA n\'ayant pas atteint 6 posts ? ' + date)
    .setDescription(lines.join('\n') || 'Tout le monde a atteint l\'objectif !')
    .setTimestamp();
}

function allPostsMetEmbed(date) {
  return new EmbedBuilder()
    .setColor(COLORS.success)
    .setTitle('?? Objectif atteint ! ? ' + date)
    .setDescription('Tous les VA ont poste au moins **6 posts** aujourd\'hui. Bravo a toute l\'equipe !')
    .setTimestamp();
}

function formatStats(s) {
  if (!s) return 'Donnees indisponibles';
  return '??? Vues: **' + fmt(s.views) + '**\n?? Likes: **' + fmt(s.likes) + '**\n?? Commentaires: **' + fmt(s.comments) + '**\n?? Republications: **' + fmt(s.shares) + '**';
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
  return '??? ' + sign(d.views) + '\n?? ' + sign(d.likes) + '\n?? ' + sign(d.comments) + '\n?? ' + sign(d.shares);
}

function fmt(n) {
  if (n == null) return '-';
  return Number(n).toLocaleString('fr-FR');
}

module.exports = { COLORS: COLORS, newPostEmbed: newPostEmbed, hourlyUpdateEmbed: hourlyUpdateEmbed, dailySummaryEmbed: dailySummaryEmbed, vaStatsEmbed: vaStatsEmbed, leaderboardEmbed: leaderboardEmbed, missingPostsEmbed: missingPostsEmbed, allPostsMetEmbed: allPostsMetEmbed };
