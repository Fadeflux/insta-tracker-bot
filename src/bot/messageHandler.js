const config = require('../../config');
const logger = require('../utils/logger');
const { isInstagramPostUrl, extractInstagramUrls, extractPostId, normalizeUrl } = require('../utils/instagram');
const db = require('../db/queries');
const { scheduleInitialScrape, notifyQueue } = require('../jobs/scrapeQueue');
const { scrapePost } = require('../scrapers/instagram');
const embeds = require('../utils/embeds');

async function handleMessage(message) {
  if (message.author.bot) return;
  if (message.channel.id !== config.discord.channels.links) return;

  var urls = extractInstagramUrls(message.content);

  if (urls.length === 0) {
    try {
      await message.delete();
      var reply = await message.channel.send({ content: '<@' + message.author.id + '> Ce canal est reserve aux liens Instagram. Ton message a ete supprime.' });
      setTimeout(function() { reply.delete().catch(function() {}); }, 10000);
    } catch (e) {}
    return;
  }

  for (var i = 0; i < urls.length; i++) {
    var rawUrl = urls[i];
    var url = normalizeUrl(rawUrl);
    var igPostId = extractPostId(rawUrl);

    if (!url || !igPostId) { logger.warn('Invalid URL skipped: ' + rawUrl); continue; }

    var existing = await db.getPostByIgId(igPostId);
    if (existing) {
      try {
        var reply2 = await message.channel.send({ content: '<@' + message.author.id + '> Ce post a deja ete enregistre (par ' + existing.va_name + ').' });
        setTimeout(function() { reply2.delete().catch(function() {}); }, 10000);
      } catch (e) {}
      continue;
    }

    var post = await db.insertPost({
      igPostId: igPostId,
      url: url,
      vaDiscordId: message.author.id,
      vaName: (message.member && message.member.displayName) || message.author.username,
    });

    if (!post) { logger.warn('Post insert returned null: ' + igPostId); continue; }
    logger.info('New post registered: ' + igPostId + ' by ' + post.va_name);

    try {
      var stats = await scrapePost(url);
      await db.insertSnapshot(post.id, stats);
      await notifyQueue.add('new-post', { postId: post.id, currentStats: stats, previousStats: null });
    } catch (err) {
      logger.error('Initial scrape failed for ' + igPostId, { error: err.message });
      await db.insertSnapshot(post.id, { views: 0, likes: 0, comments: 0, shares: 0, error: err.message });
    }

    await scheduleInitialScrape(post.id, url);

    try {
      var reply3 = await message.channel.send({ content: 'Post de <@' + message.author.id + '> enregistre ! Tracking actif jusqu a 23h59.' });
      setTimeout(function() { reply3.delete().catch(function() {}); }, 10000);
    } catch (e) {}
  }

  try { await message.delete(); } catch (err) { logger.warn('Could not delete message: ' + err.message); }
}

module.exports = { handleMessage: handleMessage };
