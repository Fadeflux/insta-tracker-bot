const config = require('../../config');
const logger = require('../utils/logger');
const { isInstagramPostUrl, extractInstagramUrls, extractPostId, normalizeUrl } = require('../utils/instagram');
const db = require('../db/queries');
const { scheduleInitialScrape } = require('../jobs/scrapeQueue');
const { notifyQueue } = require('../jobs/scrapeQueue');
const { scrapePost } = require('../scrapers/instagram');
const embeds = require('../utils/embeds');

async function handleMessage(message) {
  if (message.author.bot) return;
  if (message.channel.id !== config.discord.channels.links) return;

  const urls = extractInstagramUrls(message.content);

  if (urls.length === 0) {
    try {
      await message.delete();
      const reply = await message.channel.send({ content: '<@' + message.author.id + '> Ce canal est reserve aux liens Instagram. Ton message a ete supprime.' });
      setTimeout(() => reply.delete().catch(() => {}), 10000);
    } catch {}
    return;
  }

  for (const rawUrl of urls) {
    const url = normalizeUrl(rawUrl);
    const igPostId = extractPostId(rawUrl);

    if (!url || !igPostId) { logger.warn('Invalid URL skipped: ' + rawUrl); continue; }

    const existing = await db.getPostByIgId(igPostId);
    if (existing) {
      try {
        const reply = await message.channel.send({ content: '<@' + message.author.id + '> Ce post a deja ete enregistre (par ' + existing.va_name + ').' });
        setTimeout(() => reply.delete().catch(() => {}), 10000);
      } catch {}
      continue;
    }

    const post = await db.insertPost({
      igPostId, url,
      vaDiscordId: message.author.id,
      vaName: message.member?.displayName || message.author.username,
    });

    if (!post) { logger.warn('Post insert returned null: ' + igPostId); continue; }
    logger.info('New post registered: ' + igPostId + ' by ' + post.va_name);

    try {
      const stats = await scrapePost(url);
      await db.insertSnapshot(post.id, stats);
      await notifyQueue.add('new-post', { postId: post.id, currentStats: stats, previousStats: null });
    } catch (err) {
      logger.error('Initial scrape failed for ' + igPostId, { error: err.message });
      await db.insertSnapshot(post.id, { views: 0, likes: 0, comments: 0, shares: 0, error: err.message });
    }

    await scheduleInitialScrape(post.id, url);

    try {
      const reply = await message.channel.send({ content: 'Post de <@' + message.author.id + '> enregistre ! Tracking actif jusqu a 23h59.' });
      setTimeout(() => reply.delete().catch(() => {}), 10000);
    } catch {}
  }

  try { await message.delete(); } catch (err) { logger.warn('Could not delete message: ' + err.message); }
}

module.exports = { handleMessage };
