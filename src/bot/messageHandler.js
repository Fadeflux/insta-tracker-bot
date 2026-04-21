const config = require('../../config');
const logger = require('../utils/logger');
const { isInstagramPostUrl, extractInstagramUrls, extractPostId, normalizeUrl } = require('../utils/instagram');
const { isTwitterUrl, extractTwitterUrls, extractTweetId, normalizeTwitterUrl } = require('../utils/twitter');
const db = require('../db/queries');
const { scheduleInitialScrape, notifyQueue } = require('../jobs/scrapeQueue');
const { scrapePost } = require('../scrapers/instagram');
const { scrapeTweet } = require('../scrapers/twitter');

async function handleMessage(message) {
  if (message.author.bot) return;

  // Detect platform from guild
  var platformInfo = config.getPlatformByGuild(message.guild && message.guild.id);
  if (!platformInfo) return; // Message from unknown guild

  var platform = platformInfo.name;
  var platConfig = platformInfo.config;

  // Only process messages in the #links channel of this platform
  if (message.channel.id !== platConfig.channels.links) return;

  // Route to the right handler
  if (platform === 'instagram') {
    await handleInstagramMessage(message, platConfig);
  } else if (platform === 'twitter') {
    await handleTwitterMessage(message, platConfig);
  }
}

async function handleInstagramMessage(message, platConfig) {
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

    if (!url || !igPostId) { logger.warn('Invalid IG URL skipped: ' + rawUrl); continue; }

    var existing = await db.getPostByIgId(igPostId);
    if (existing) {
      try {
        var reply2 = await message.channel.send({ content: '<@' + message.author.id + '> Ce post a deja ete enregistre (par ' + existing.va_name + ').' });
        setTimeout(function() { reply2.delete().catch(function() {}); }, 10000);
      } catch (e) {}
      continue;
    }

    var caption = extractCaption(message.content, rawUrl);

    var post = await db.insertPost({
      igPostId: igPostId,
      url: url,
      vaDiscordId: message.author.id,
      vaName: (message.member && message.member.displayName) || message.author.username,
      caption: caption,
      platform: 'instagram',
      guildId: message.guild.id,
    });

    if (!post) { logger.warn('Post insert returned null: ' + igPostId); continue; }
    logger.info('[IG] New post registered: ' + igPostId + ' by ' + post.va_name);

    try {
      var reply3 = await message.channel.send({ content: 'Post de <@' + message.author.id + '> enregistre ! Tracking actif jusqu a 23h59.' });
      setTimeout(function() { reply3.delete().catch(function() {}); }, 10000);
    } catch (e) {}

    // Scrape in background
    scrapePost(url).then(function(stats) {
      return db.insertSnapshot(post.id, stats).then(function() {
        return notifyQueue.add('new-post', { postId: post.id, currentStats: stats, previousStats: null, platform: 'instagram' });
      });
    }).catch(function(err) {
      logger.error('[IG] Initial scrape failed for ' + igPostId, { error: err.message });
      db.insertSnapshot(post.id, { views: 0, likes: 0, comments: 0, shares: 0, error: err.message }).catch(function() {});
    });

    await scheduleInitialScrape(post.id, url, 'instagram');
  }

  try { await message.delete(); } catch (err) { logger.warn('Could not delete message: ' + err.message); }
}

async function handleTwitterMessage(message, platConfig) {
  var urls = extractTwitterUrls(message.content);

  if (urls.length === 0) {
    try {
      await message.delete();
      var reply = await message.channel.send({ content: '<@' + message.author.id + '> Ce canal est reserve aux liens Twitter/X. Ton message a ete supprime.' });
      setTimeout(function() { reply.delete().catch(function() {}); }, 10000);
    } catch (e) {}
    return;
  }

  for (var i = 0; i < urls.length; i++) {
    var rawUrl = urls[i];
    var url = normalizeTwitterUrl(rawUrl);
    var tweetId = extractTweetId(rawUrl);

    if (!url || !tweetId) { logger.warn('Invalid Twitter URL skipped: ' + rawUrl); continue; }

    // Use tweet ID as ig_post_id (shared column)
    var postKey = 'tw_' + tweetId;
    var existing = await db.getPostByIgId(postKey);
    if (existing) {
      try {
        var reply2 = await message.channel.send({ content: '<@' + message.author.id + '> Ce tweet a deja ete enregistre (par ' + existing.va_name + ').' });
        setTimeout(function() { reply2.delete().catch(function() {}); }, 10000);
      } catch (e) {}
      continue;
    }

    var caption = extractCaption(message.content, rawUrl);

    var post = await db.insertPost({
      igPostId: postKey,
      url: url,
      vaDiscordId: message.author.id,
      vaName: (message.member && message.member.displayName) || message.author.username,
      caption: caption,
      platform: 'twitter',
      guildId: message.guild.id,
    });

    if (!post) { logger.warn('Post insert returned null: ' + postKey); continue; }
    logger.info('[TW] New tweet registered: ' + tweetId + ' by ' + post.va_name);

    try {
      var reply3 = await message.channel.send({ content: 'Tweet de <@' + message.author.id + '> enregistre ! Tracking actif jusqu a 23h59.' });
      setTimeout(function() { reply3.delete().catch(function() {}); }, 10000);
    } catch (e) {}

    // Scrape in background
    scrapeTweet(url).then(function(stats) {
      return db.insertSnapshot(post.id, stats).then(function() {
        return notifyQueue.add('new-post', { postId: post.id, currentStats: stats, previousStats: null, platform: 'twitter' });
      });
    }).catch(function(err) {
      logger.error('[TW] Initial scrape failed for ' + tweetId, { error: err.message });
      db.insertSnapshot(post.id, { views: 0, likes: 0, comments: 0, shares: 0, retweets: 0, quote_tweets: 0, bookmarks: 0, error: err.message }).catch(function() {});
    });

    await scheduleInitialScrape(post.id, url, 'twitter');
  }

  try { await message.delete(); } catch (err) { logger.warn('Could not delete message: ' + err.message); }
}

function extractCaption(content, rawUrl) {
  var urlIdx = content.indexOf(rawUrl);
  if (urlIdx === -1) return null;
  var afterUrl = content.substring(urlIdx + rawUrl.length).trim();
  afterUrl = afterUrl.replace(/https?:\/\/\S+/g, '').trim();
  if (afterUrl.length > 0) return afterUrl.substring(0, 500);
  return null;
}

module.exports = { handleMessage: handleMessage };
