var https = require('https');

async function scrapePost(url) {
  var result = { views: 0, likes: 0, comments: 0, shares: 0 };
  var postId = extractId(url);
  if (!postId) { result.error = 'Invalid URL'; return result; }

  try {
    // Strategy 1: Try oembed endpoint
    var oembedUrl = 'https://api.instagram.com/oembed/?url=https://www.instagram.com/p/' + postId + '/';
    try {
      var oembedData = await fetchJson(oembedUrl);
      if (oembedData && oembedData.title) {
        console.log('Oembed title: ' + oembedData.title);
      }
    } catch(e) {
      console.log('Oembed failed: ' + e.message);
    }

    // Strategy 2: Try embed page
    var embedUrl = 'https://www.instagram.com/p/' + postId + '/embed/';
    var embedHtml = await fetchPage(embedUrl);
    console.log('Embed page length: ' + embedHtml.length);

    // Extract likes from embed
    var likesPatterns = [
      /"edge_liked_by"\s*:\s*\{\s*"count"\s*:\s*(\d+)/,
      /"edge_media_preview_like"\s*:\s*\{\s*"count"\s*:\s*(\d+)/,
      /(\d[\d,.]*)\s*likes?/i,
      /likes?\s*:\s*(\d[\d,.]*)/i,
      /"like_count"\s*:\s*(\d+)/,
    ];
    for (var i = 0; i < likesPatterns.length; i++) {
      var m = embedHtml.match(likesPatterns[i]);
      if (m && result.likes === 0) {
        result.likes = parseMetricValue(m[1]);
        console.log('Likes found with pattern ' + i + ': ' + result.likes);
        break;
      }
    }

    // Extract comments
    var commentsPatterns = [
      /"edge_media_to_comment"\s*:\s*\{\s*"count"\s*:\s*(\d+)/,
      /"edge_media_preview_comment"\s*:\s*\{\s*"count"\s*:\s*(\d+)/,
      /"comment_count"\s*:\s*(\d+)/,
      /(\d[\d,.]*)\s*comments?/i,
    ];
    for (var j = 0; j < commentsPatterns.length; j++) {
      var mc = embedHtml.match(commentsPatterns[j]);
      if (mc && result.comments === 0) {
        result.comments = parseMetricValue(mc[1]);
        console.log('Comments found with pattern ' + j + ': ' + result.comments);
        break;
      }
    }

    // Extract views
    var viewsPatterns = [
      /"video_view_count"\s*:\s*(\d+)/,
      /"play_count"\s*:\s*(\d+)/,
      /"view_count"\s*:\s*(\d+)/,
      /(\d[\d,.]*)\s*(?:views?|plays?|vues?)/i,
    ];
    for (var k = 0; k < viewsPatterns.length; k++) {
      var mv = embedHtml.match(viewsPatterns[k]);
      if (mv && result.views === 0) {
        result.views = parseMetricValue(mv[1]);
        console.log('Views found with pattern ' + k + ': ' + result.views);
        break;
      }
    }

    // Strategy 3: Try main page with different user agent
    if (result.likes === 0 && result.views === 0) {
      var mainHtml = await fetchPage(url);
      console.log('Main page length: ' + mainHtml.length);

      // og:description often has stats
      var ogMatch = mainHtml.match(/content="([^"]*?\d+[^"]*?likes?[^"]*)"/i);
      if (ogMatch) {
        console.log('OG content: ' + ogMatch[1]);
        var lm = ogMatch[1].match(/([\d,.]+[KMkm]?)\s*likes?/i);
        var cm = ogMatch[1].match(/([\d,.]+[KMkm]?)\s*comments?/i);
        if (lm) result.likes = parseMetricValue(lm[1]);
        if (cm) result.comments = parseMetricValue(cm[1]);
      }

      // Try JSON data in page
      var jsonLikes = mainHtml.match(/"like_count"\s*:\s*(\d+)/);
      if (jsonLikes && result.likes === 0) result.likes = parseInt(jsonLikes[1], 10);

      var jsonViews = mainHtml.match(/"play_count"\s*:\s*(\d+)/);
      if (jsonViews && result.views === 0) result.views = parseInt(jsonViews[1], 10);

      var jsonComments = mainHtml.match(/"comment_count"\s*:\s*(\d+)/);
      if (jsonComments && result.comments === 0) result.comments = parseInt(jsonComments[1], 10);
    }

    console.log('Final result for ' + postId + ': ' + JSON.stringify(result));
    return result;
  } catch (err) {
    console.error('Scrape failed for ' + url + ': ' + err.message);
    result.error = err.message;
    return result;
  }
}

function extractId(url) {
  var m = url.match(/instagram\.com\/(?:p|reel|tv)\/([A-Za-z0-9_-]+)/);
  return m ? m[1] : null;
}

function fetchPage(url) {
  return new Promise(function(resolve, reject) {
    var options = {
      headers: {
        'User-Agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36',
        'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,*/*;q=0.8',
        'Accept-Language': 'en-US,en;q=0.9',
        'Accept-Encoding': 'identity',
        'Cache-Control': 'no-cache',
        'Sec-Fetch-Dest': 'document',
        'Sec-Fetch-Mode': 'navigate',
        'Sec-Fetch-Site': 'none',
      }
    };

    function doRequest(reqUrl, redirects) {
      if (redirects > 5) return reject(new Error('Too many redirects'));
      https.get(reqUrl, options, function(res) {
        if (res.statusCode >= 300 && res.statusCode < 400 && res.headers.location) {
          var loc = res.headers.location;
          if (loc.startsWith('/')) loc = 'https://www.instagram.com' + loc;
          return doRequest(loc, redirects + 1);
        }
        if (res.statusCode === 429) return reject(new Error('Rate limited'));
        var data = '';
        res.on('data', function(chunk) { data += chunk; });
        res.on('end', function() { resolve(data); });
        res.on('error', reject);
      }).on('error', reject);
    }

    doRequest(url, 0);
  });
}

function fetchJson(url) {
  return fetchPage(url).then(function(text) {
    return JSON.parse(text);
  });
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

function initBrowser() { return Promise.resolve(); }
function closeBrowser() { return Promise.resolve(); }

module.exports = { scrapePost: scrapePost, initBrowser: initBrowser, closeBrowser: closeBrowser };
