const config = require('../../config');
const logger = require('../utils/logger');
const { isInstagramPostUrl, extractInstagramUrls, extractPostId, normalizeUrl } = require('../utils/instagram');
const { isTwitterUrl, extractTwitterUrls, extractTweetId, normalizeTwitterUrl, extractTwitterUsername } = require('../utils/twitter');
const { extractThreadsUrls, extractThreadsPostId, normalizeThreadsUrl, extractThreadsUsername } = require('../utils/threads');
const db = require('../db/queries');
const { scheduleInitialScrape, notifyQueue } = require('../jobs/scrapeQueue');
const { scrapePost } = require('../scrapers/instagram');
const { scrapeTweet } = require('../scrapers/twitter');
const { scrapePost: scrapeThreadsPost } = require('../scrapers/threads');

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
    await handleInstagramMessage(message, platConfig, 'instagram');
  } else if (platform === 'twitter') {
    await handleTwitterMessage(message, platConfig);
  } else if (platform === 'geelark') {
    await handleInstagramMessage(message, platConfig, 'geelark');
  } else if (platform === 'threads') {
    await handleThreadsMessage(message, platConfig);
  }
}

async function handleInstagramMessage(message, platConfig, platform) {
  platform = platform || 'instagram';
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
      platform: platform,
      guildId: message.guild.id,
      // account_id will be filled asynchronously once the scraper resolves the username
    });

    if (!post) { logger.warn('Post insert returned null: ' + igPostId); continue; }
    logger.info('[' + platform.toUpperCase() + '] New post registered: ' + igPostId + ' by ' + post.va_name);

    try {
      var reply3 = await message.channel.send({ content: 'Post de <@' + message.author.id + '> enregistre ! Tracking actif jusqu a 23h59.' });
      setTimeout(function() { reply3.delete().catch(function() {}); }, 10000);
    } catch (e) {}

    // Scrape in background — also hydrates the account from the resolved username.
    (function(postRef, vaId, vaName) {
      scrapePost(url).then(function(stats) {
        return db.insertSnapshot(postRef.id, stats).then(function() {
          return linkAccountIfAny(postRef, stats, platform, vaId, vaName);
        }).then(function() {
          return notifyQueue.add('new-post', { postId: postRef.id, currentStats: stats, previousStats: null, platform: platform });
        });
      }).catch(function(err) {
        logger.error('[' + platform.toUpperCase() + '] Initial scrape failed for ' + igPostId, { error: err.message });
        db.insertSnapshot(postRef.id, { views: 0, likes: 0, comments: 0, shares: 0, error: err.message }).catch(function() {});
      });
    })(post, message.author.id, (message.member && message.member.displayName) || message.author.username);

    await scheduleInitialScrape(post.id, url, platform);
  }

  try { await message.delete(); } catch (err) { logger.warn('Could not delete message: ' + err.message); }
}

// Upsert the account matching a scraper result (if any username was resolved)
// and attach it to the post. Silent no-op if the scraper didn't find a handle.
async function linkAccountIfAny(post, stats, platform, vaDiscordId, vaName) {
  if (!stats || !stats.username) {
    logger.warn('[' + platform.toUpperCase() + '] Could not resolve account username for post ' + post.id);
    return;
  }
  try {
    var account = await db.upsertAccount(stats.username, platform, vaDiscordId, vaName);
    if (account) {
      await db.updatePostAccount(post.id, account.id, account.username);
      logger.info('[' + platform.toUpperCase() + '] Post ' + post.id + ' linked to @' + account.username);
    }
  } catch (err) {
    logger.error('[' + platform.toUpperCase() + '] Failed to link account for post ' + post.id, { error: err.message });
  }
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
    var vaName = (message.member && message.member.displayName) || message.author.username;

    // Twitter: username is in the URL, so we can link the account up-front.
    var urlUsername = extractTwitterUsername(rawUrl);
    var accountRow = null;
    if (urlUsername) {
      try {
        accountRow = await db.upsertAccount(urlUsername, 'twitter', message.author.id, vaName);
      } catch (e) {
        logger.warn('[TW] upsertAccount failed: ' + e.message);
      }
    }

    var post = await db.insertPost({
      igPostId: postKey,
      url: url,
      vaDiscordId: message.author.id,
      vaName: vaName,
      caption: caption,
      platform: 'twitter',
      guildId: message.guild.id,
      accountId: accountRow ? accountRow.id : null,
      accountUsername: accountRow ? accountRow.username : urlUsername,
    });

    if (!post) { logger.warn('Post insert returned null: ' + postKey); continue; }
    logger.info('[TW] New tweet registered: ' + tweetId + ' by ' + post.va_name + (urlUsername ? ' on @' + urlUsername : ''));

    try {
      var reply3 = await message.channel.send({ content: 'Tweet de <@' + message.author.id + '> enregistre ! Tracking actif jusqu a 23h59.' });
      setTimeout(function() { reply3.delete().catch(function() {}); }, 10000);
    } catch (e) {}

    // Scrape in background. If the scraper reports a different username
    // (e.g. URL had i/web), it takes priority since it comes from FxTwitter.
    (function(postRef, vaId, vaNm, alreadyLinked) {
      scrapeTweet(url).then(function(stats) {
        return db.insertSnapshot(postRef.id, stats).then(function() {
          if (!alreadyLinked || (stats.username && alreadyLinked && stats.username !== alreadyLinked)) {
            return linkAccountIfAny(postRef, stats, 'twitter', vaId, vaNm);
          }
        }).then(function() {
          return notifyQueue.add('new-post', { postId: postRef.id, currentStats: stats, previousStats: null, platform: 'twitter' });
        });
      }).catch(function(err) {
        logger.error('[TW] Initial scrape failed for ' + tweetId, { error: err.message });
        db.insertSnapshot(postRef.id, { views: 0, likes: 0, comments: 0, shares: 0, retweets: 0, quote_tweets: 0, bookmarks: 0, error: err.message }).catch(function() {});
      });
    })(post, message.author.id, vaName, urlUsername);

    await scheduleInitialScrape(post.id, url, 'twitter');
  }

  try { await message.delete(); } catch (err) { logger.warn('Could not delete message: ' + err.message); }
}

async function handleThreadsMessage(message, platConfig) {
  var urls = extractThreadsUrls(message.content);

  if (urls.length === 0) {
    try {
      await message.delete();
      var reply = await message.channel.send({ content: '<@' + message.author.id + '> Ce canal est reserve aux liens Threads. Ton message a ete supprime.' });
      setTimeout(function() { reply.delete().catch(function() {}); }, 10000);
    } catch (e) {}
    return;
  }

  for (var i = 0; i < urls.length; i++) {
    var rawUrl = urls[i];
    var url = normalizeThreadsUrl(rawUrl);
    var postCode = extractThreadsPostId(rawUrl);

    if (!url || !postCode) { logger.warn('Invalid Threads URL skipped: ' + rawUrl); continue; }

    // Use post code as ig_post_id (shared column), prefixed to avoid collisions
    var postKey = 'th_' + postCode;
    var existing = await db.getPostByIgId(postKey);
    if (existing) {
      try {
        var reply2 = await message.channel.send({ content: '<@' + message.author.id + '> Ce post Threads a deja ete enregistre (par ' + existing.va_name + ').' });
        setTimeout(function() { reply2.delete().catch(function() {}); }, 10000);
      } catch (e) {}
      continue;
    }

    var caption = extractCaption(message.content, rawUrl);
    var vaName = (message.member && message.member.displayName) || message.author.username;

    // Threads: username is usually in the URL, link the account up-front.
    var urlUsername = extractThreadsUsername(rawUrl);
    var accountRow = null;
    if (urlUsername) {
      try {
        accountRow = await db.upsertAccount(urlUsername, 'threads', message.author.id, vaName);
      } catch (e) {
        logger.warn('[Threads] upsertAccount failed: ' + e.message);
      }
    }

    var post = await db.insertPost({
      igPostId: postKey,
      url: url,
      vaDiscordId: message.author.id,
      vaName: vaName,
      caption: caption,
      platform: 'threads',
      guildId: message.guild.id,
      accountId: accountRow ? accountRow.id : null,
      accountUsername: accountRow ? accountRow.username : urlUsername,
    });

    if (!post) { logger.warn('Post insert returned null: ' + postKey); continue; }
    logger.info('[Threads] New post registered: ' + postCode + ' by ' + post.va_name + (urlUsername ? ' on @' + urlUsername : ''));

    try {
      var reply3 = await message.channel.send({ content: 'Post Threads de <@' + message.author.id + '> enregistre ! Tracking actif jusqu\'a 23h59.' });
      setTimeout(function() { reply3.delete().catch(function() {}); }, 10000);
    } catch (e) {}

    // Scrape in background. Similar pattern as Twitter — allow scraper-extracted
    // username to take priority if different from URL.
    (function(postRef, vaId, vaNm, alreadyLinked) {
      scrapeThreadsPost(url).then(function(stats) {
        return db.insertSnapshot(postRef.id, stats).then(function() {
          if (!alreadyLinked || (stats.username && alreadyLinked && stats.username !== alreadyLinked)) {
            return linkAccountIfAny(postRef, stats, 'threads', vaId, vaNm);
          }
        }).then(function() {
          return notifyQueue.add('new-post', { postId: postRef.id, currentStats: stats, previousStats: null, platform: 'threads' });
        });
      }).catch(function(err) {
        logger.error('[Threads] Initial scrape failed for ' + postCode, { error: err.message });
        db.insertSnapshot(postRef.id, { views: 0, likes: 0, comments: 0, shares: 0, error: err.message }).catch(function() {});
      });
    })(post, message.author.id, vaName, urlUsername);

    await scheduleInitialScrape(post.id, url, 'threads');
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
