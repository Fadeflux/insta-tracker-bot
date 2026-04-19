var https = require('https');
var http = require('http');

async function scrapePost(url) {
  var result = { views: 0, likes: 0, comments: 0, shares: 0 };

  try {
    var html = await fetchPage(url);
    console.log('Fetched page length: ' + html.length);

    // Strategy 1: og:description meta tag
    var ogMatch = html.match(/<meta\s+(?:property|name)="og:description"\s+content="([^"]+)"/i);
    if (!ogMatch) ogMatch = html.match(/content="([^"]+)"\s+(?:property|name)="og:description"/i);

    if (ogMatch) {
      var desc = ogMatch[1];
      console.log('OG description: ' + desc);
      var likesMatch = desc.match(/([\d,.]+[KMkm]?)\s*likes?/i);
      var commentsMatch = desc.match(/([\d,.]+[KMkm]?)\s*comments?/i);
      if (likesMatch) result.likes = parseMetricValue(likesMatch[1]);
      if (commentsMatch) result.comments = parseMetricValue(commentsMatch[1]);
    }

    // Strategy 2: video view count
    var viewsMatch = html.match(/"video_view_count"\s*:\s*(\d+)/);
    if (viewsMatch) result.views = parseInt(viewsMatch[1], 10);

    var playMatch = html.match(/"play_count"\s*:\s*(\d+)/);
    if (playMatch && result.views === 0) result.views = parseInt(playMatch[1], 10);

    // Strategy 3: like count from JSON
    var likeJson = html.match(/"edge_media_preview_like"\s*:\s*\{\s*"count"\s*:\s*(\d+)/);
    if (likeJson && result.likes === 0) result.likes = parseInt(likeJson[1], 10);

    var commentJson = html.match(/"edge_media_preview_comment"\s*:\s*\{\s*"count"\s*:\s*(\d+)/);
    if (commentJson && result.comments === 0) result.comments = parseInt(commentJson[1], 10);

    // Strategy 4: edge_media_to_comment
    var commentJson2 = html.match(/"edge_media_to_comment"\s*:\s*\{\s*"count"\s*:\s*(\d+)/);
    if (commentJson2 && result.comments === 0) result.comments = parseInt(commentJson2[1], 10);

    console.log('Scrape result:', JSON.stringify(result));
    return result;
  } catch (err) {
    console.error('Scrape failed for ' + url + ': ' + err.message);
    result.error = err.message;
    return result;
  }
}

function fetchPage(url) {
  return new Promise(function(resolve, reject) {
    var client = url.startsWith('https') ? https : http;
    var options = {
      headers: {
        'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36',
        'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8',
        'Accept-Language': 'en-US,en;q=0.5',
      }
    };

    function doRequest(reqUrl, redirects) {
      if (redirects > 5) return reject(new Error('Too many redirects'));
      client.get(reqUrl, options, function(res) {
        if (res.statusCode >= 300 && res.statusCode < 400 && res.headers.location) {
          return doRequest(res.headers.location, redirects + 1);
        }
        var data = '';
        res.on('data', function(chunk) { data += chunk; });
        res.on('end', function() { resolve(data); });
        res.on('error', reject);
      }).on('error', reject);
    }

    doRequest(url, 0);
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
