const { chromium } = require('playwright');
const logger = require('../utils/logger');
const config = require('../../config');

var browser = null;

async function initBrowser() {
  if (!browser) {
    browser = await chromium.launch({
      headless: true,
      args: ['--no-sandbox', '--disable-setuid-sandbox', '--disable-dev-shm-usage', '--disable-gpu'],
    });
    logger.info('Browser launched');
  }
  return browser;
}

async function closeBrowser() {
  if (browser) { await browser.close(); browser = null; logger.info('Browser closed'); }
}

async function scrapePost(url) {
  var b = await initBrowser();
  var context = await b.newContext({
    userAgent: 'Mozilla/5.0 (iPhone; CPU iPhone OS 17_0 like Mac OS X) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/17.0 Mobile/15E148 Safari/604.1',
    viewport: { width: 390, height: 844 },
    locale: 'en-US',
  });
  var page = await context.newPage();
  var result = { views: 0, likes: 0, comments: 0, shares: 0 };

  try {
    await page.route('**/*', function(route) {
      var type = route.request().resourceType();
      if (['image', 'media', 'font', 'stylesheet'].includes(type)) route.abort();
      else route.continue();
    });

    await page.goto(url, { waitUntil: 'domcontentloaded', timeout: 30000 });
    await page.waitForTimeout(3000);

    var metaContent = await page.evaluate(function() {
      var ogDesc = document.querySelector('meta[property="og:description"]');
      if (ogDesc) return ogDesc.getAttribute('content');
      return null;
    });

    if (metaContent) {
      var likesMatch = metaContent.match(/([\d,.KMkm]+)\s*likes?/i);
      var commentsMatch = metaContent.match(/([\d,.KMkm]+)\s*comments?/i);
      if (likesMatch) result.likes = parseMetricValue(likesMatch[1]);
      if (commentsMatch) result.comments = parseMetricValue(commentsMatch[1]);
    }

    var pageStats = await page.evaluate(function() {
      var stats = {};
      var allText = document.body.innerText;
      var viewsMatch = allText.match(/([\d,.]+[KMkm]?)\s*(?:views?|plays?|vues?)/i);
      if (viewsMatch) stats.views = viewsMatch[1];
      var likesMatch = allText.match(/([\d,.]+[KMkm]?)\s*likes?/i);
      if (likesMatch) stats.likes = likesMatch[1];
      var commentsMatch = allText.match(/([\d,.]+[KMkm]?)\s*comments?/i);
      if (commentsMatch) stats.comments = commentsMatch[1];
      return stats;
    });

    if (pageStats.views) result.views = parseMetricValue(pageStats.views);
    if (pageStats.likes && result.likes === 0) result.likes = parseMetricValue(pageStats.likes);
    if (pageStats.comments && result.comments === 0) result.comments = parseMetricValue(pageStats.comments);

    var jsonLd = await page.evaluate(function() {
      var scripts = document.querySelectorAll('script[type="application/ld+json"]');
      for (var i = 0; i < scripts.length; i++) {
        try {
          var data = JSON.parse(scripts[i].textContent);
          if (data.interactionStatistic) return data;
          if (data['@type'] === 'VideoObject' || data['@type'] === 'ImageObject') return data;
        } catch(e) {}
      }
      return null;
    });

    if (jsonLd && jsonLd.interactionStatistic) {
      for (var i = 0; i < jsonLd.interactionStatistic.length; i++) {
        var stat = jsonLd.interactionStatistic[i];
        var type = (stat.interactionType && stat.interactionType['@type']) || stat.interactionType || '';
        var count = parseInt(stat.userInteractionCount || '0', 10);
        if (type.includes('Like')) result.likes = count;
        if (type.includes('Comment')) result.comments = count;
        if (type.includes('Watch') || type.includes('View')) result.views = count;
        if (type.includes('Share')) result.shares = count;
      }
    }

    logger.info('Scraped ' + url, result);
    return result;
  } catch (err) {
    logger.error('Scrape failed for ' + url, { error: err.message });
    result.error = err.message;
    return result;
  } finally {
    await context.close();
  }
}

function parseMetricValue(str) {
  if (!str) return 0;
  str = str.replace(/,/g, '').trim();
  var multipliers = { k: 1000, m: 1000000 };
  var match = str.match(/^([\d.]+)\s*([KMkm])?$/);
  if (!match) return parseInt(str, 10) || 0;
  var num = parseFloat(match[1]);
  var mult = match[2] ? multipliers[match[2].toLowerCase()] || 1 : 1;
  return Math.round(num * mult);
}

module.exports = { scrapePost, initBrowser, closeBrowser };
