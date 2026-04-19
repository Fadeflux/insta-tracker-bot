function extractPostId(url) {
  const patterns = [
    /instagram\.com\/(?:p|reel|tv)\/([A-Za-z0-9_-]+)/,
    /instagr\.am\/(?:p|reel|tv)\/([A-Za-z0-9_-]+)/,
  ];
  for (const pattern of patterns) {
    const match = url.match(pattern);
    if (match) return match[1];
  }
  return null;
}

function isInstagramPostUrl(text) {
  return /https?:\/\/(www\.)?(instagram\.com|instagr\.am)\/(p|reel|tv)\/[A-Za-z0-9_-]+/i.test(text);
}

function extractInstagramUrls(text) {
  const regex = /https?:\/\/(www\.)?(instagram\.com|instagr\.am)\/(p|reel|tv)\/[A-Za-z0-9_-]+\/?(\?[^\s]*)*/gi;
  return (text.match(regex) || []).map((url) => url.replace(/\/$/, ''));
}

function normalizeUrl(url) {
  const id = extractPostId(url);
  if (!id) return null;
  const typeMatch
@'
const { EmbedBuilder } = require('discord.js');

const COLORS = {
  primary: 0xe1306c,
  success: 0x2ecc71,
  warning: 0xf39c12,
  error: 0xe74c3c,
  info: 0x3498db,
  neutral: 0x95a5a6,
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
  const diff = computeDiff(currentStats, previousStats);
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
  const medal = rank <= 3 ? ['1er', '2e', '3e'][rank - 1] : '#' + rank;
  return new EmbedBuilder()
    .setColor(rank <= 3 ? COLORS.success : COLORS.neutral)
    .setTitle(medal + ' ' + vaName)
    .addFields(
      { name: 'Posts', value: '' + summary.post_count + '/6', inline: true },
      { name: 'Vues totales', value: fmt(summary.total_views), inline: true },
      { name: 'Likes totaux', value: fmt(summary.total_likes), inline: true },
      { name: 'Commentaires', value: fmt(summary.total_comments), inline: true },
      { name: 'Republications', value: fmt(summary.total_shares), inline: true }
    )
    .setTimestamp();
}

function vaStatsEmbed(vaName, stats, posts) {
  return new EmbedBuilder()
    .setColor(COLORS.primary)
    .setTitle('Stats du jour - ' + vaName)
    .addFields(
      { name: 'Posts', value: '' + posts.length + '/6', inline: true },
      { name: 'Vues', value: fmt(stats.total_views), inline: true },
      { name: 'Likes', value: fmt(stats.total_likes), inline: true },
      { name: 'Commentaires', value: fmt(stats.total_comments), inline: true },
      { name: 'Republications', value: fmt(stats.total_shares), inline: true }
    )
    .setTimestamp();
}

function leaderboardEmbed(rankings, date) {
  const lines = rankings.map((r, i) => {
    const medal = i < 3 ? ['1er', '2e', '3e'][i] : (i + 1) + '.';
    return medal + ' **' + r.va_name + '** - ' + r.post_count + ' posts | Vues ' + fmt(r.total_views) + ' | Likes ' + fmt(r.total_likes);
  });
  return new EmbedBuilder()
    .setColor(COLORS.success)
    .setTitle('Classement du ' + date)
    .setDescription(lines.join('\n') || 'Aucune donnee')
    .setTimestamp();
}

function formatStats(s) {
  if (!s) return 'Donnees indisponibles';
  return 'Vues: **' + fmt(s.views) + '**\nLikes: **' + fmt(s.likes) + '**\nCommentaires: **' + fmt(s.comments) + '**\nRepublications: **' + fmt(s.shares) + '**';
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
  const sign = (n) => (n >= 0 ? '+' + fmt(n) : '' + fmt(n));
  return 'Vues ' + sign(d.views) + '\nLikes ' + sign(d.likes) + '\nCommentaires ' + sign(d.comments) + '\nRepublications ' + sign(d.shares);
}

function fmt(n) {
  if (n == null) return '-';
  return Number(n).toLocaleString('fr-FR');
}

module.exports = { COLORS, newPostEmbed, hourlyUpdateEmbed, dailySummaryEmbed, vaStatsEmbed, leaderboardEmbed };
