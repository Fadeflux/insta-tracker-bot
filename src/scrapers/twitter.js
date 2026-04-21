var https = require('https');
var http = require('http');
var net = require('net');

var PROXY_HOST = process.env.PROXY_HOST || '5.161.16.191';
var PROXY_PORT = parseInt(process.env.PROXY_PORT || '19751');
var PROXY_USER = process.env.PROXY_USER || '';
var PROXY_PASS = process.env.PROXY_PASS || '';

// Rotating User-Agents
var USER_AGENTS = [
  'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/125.0.0.0 Safari/537.36',
  'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/125.0.0.0 Safari/537.36',
  'Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/125.0.0.0 Safari/537.36',
  'Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:126.0) Gecko/20100101 Firefox/126.0',
  'Mozilla/5.0 (Macintosh; Intel Mac OS X 10.15; rv:126.0) Gecko/20100101 Firefox/126.0',
];

function getRandomUA() {
  return USER_AGENTS[Math.floor(Math.random() * USER_AGENTS.length)];
}

async function scrapeTweet(url) {
  var result = { views: 0, likes: 0, comments: 0, shares: 0, retweets: 0, quote_tweets: 0, bookmarks: 0 };
  var tweetId = extractId(url);
  if (!tweetId) { result.error = 'Invalid Twitter URL'; return result; }

  var ua = getRandomUA();

  try {
    // === STRATEGY 1: Syndication API (public, no auth needed) ===
    try {
      var syndicationUrl = 'https://cdn.syndication.twimg.com/tweet-result?id=' + tweetId + '&lang=en&token=x';
      var synData = '';
      try {
        synData = await fetchViaProxy(syndicationUrl, ua);
        console.log('[Twitter] Syndication via proxy length: ' + synData.length);
      } catch(e) {
        console.log('[Twitter] Syndication proxy failed, trying direct: ' + e.message);
        try {
          synData = await fetchDirect(syndicationUrl, ua);
          console.log('[Twitter] Syndication direct length: ' + synData.length);
        } catch(e2) {
          console.log('[Twitter] Syndication direct also failed: ' + e2.message);
        }
      }

      if (synData.length > 0 && synData.trimStart()[0] === '{') {
        try {
          var synJson = JSON.parse(synData.trim());
          extractFromSyndication(synJson, result);
          console.log('[Twitter] Syndication parsed OK for ' + tweetId);
        } catch(pe) {
          console.log('[Twitter] Syndication JSON parse failed: ' + pe.message);
        }
      }
    } catch(e) {
      console.log('[Twitter] Syndication fetch failed: ' + e.message);
    }

    // === STRATEGY 2: FxTwitter / FixupX API (third-party public API) ===
    if (result.likes === 0 && result.views === 0) {
      try {
        var fxUrl = 'https://api.fxtwitter.com/status/' + tweetId;
        var fxData = '';
        try {
          fxData = await fetchDirect(fxUrl, ua);
          console.log('[Twitter] FxTwitter length: ' + fxData.length);
        } catch(e) {
          console.log('[Twitter] FxTwitter direct failed: ' + e.message);
          try {
            fxData = await fetchViaProxy(fxUrl, ua);
            console.log('[Twitter] FxTwitter via proxy length: ' + fxData.length);
          } catch(e2) {
            console.log('[Twitter] FxTwitter proxy also failed: ' + e2.message);
          }
        }

        if (fxData.length > 0 && fxData.trimStart()[0] === '{') {
          try {
            var fxJson = JSON.parse(fxData.trim());
            extractFromFxTwitter(fxJson, result);
            console.log('[Twitter] FxTwitter parsed OK for ' + tweetId);
          } catch(pe) {
            console.log('[Twitter] FxTwitter JSON parse failed: ' + pe.message);
          }
        }
      } catch(e) {
        console.log('[Twitter] FxTwitter fetch failed: ' + e.message);
      }
    }

    // === STRATEGY 3: Nitter instances (public frontends) ===
    if (result.likes === 0 && result.views === 0) {
      var nitterInstances = [
        'https://nitter.privacydev.net',
        'https://nitter.poast.org',
        'https://nitter.woodland.cafe',
      ];

      // Extract username from URL
      var userMatch = url.match(/(?:twitter\.com|x\.com)\/(\w+)\/status\//);
      var username = userMatch ? userMatch[1] : null;

      if (username) {
        for (var ni = 0; ni < nitterInstances.length; ni++) {
          try {
            var nitterUrl = nitterInstances[ni] + '/' + username + '/status/' + tweetId;
            var nitterHtml = '';
            try {
              nitterHtml = await fetchDirect(nitterUrl, ua);
            } catch(e) {
              try { nitterHtml = await fetchViaProxy(nitterUrl, ua); } catch(e2) {}
            }
            if (nitterHtml.length > 500) {
              extractFromNitter(nitterHtml, result);
              if (result.likes > 0 || result.views > 0) {
                console.log('[Twitter] Nitter parsed OK from ' + nitterInstances[ni]);
                break;
              }
            }
          } catch(e) {
            console.log('[Twitter] Nitter ' + nitterInstances[ni] + ' failed: ' + e.message);
          }
        }
      }
    }

    // === STRATEGY 4: Embed page scraping (publish.twitter.com) ===
    if (result.likes === 0 && result.views === 0) {
      try {
        var embedUrl = 'https://publish.twitter.com/oembed?url=https://x.com/i/status/' + tweetId + '&omit_script=true';
        var embedData = '';
        try {
          embedData = await fetchDirect(embedUrl, ua);
        } catch(e) {
          try { embedData = await fetchViaProxy(embedUrl, ua); } catch(e2) {}
        }
        if (embedData.length > 0 && embedData.trimStart()[0] === '{') {
          try {
            var embedJson = JSON.parse(embedData.trim());
            extractFromOembed(embedJson, result);
            console.log('[Twitter] oEmbed parsed OK for ' + tweetId);
          } catch(pe) {
            console.log('[Twitter] oEmbed JSON parse failed');
          }
        }
      } catch(e) {
        console.log('[Twitter] oEmbed failed: ' + e.message);
      }
    }

    // === STRATEGY 5: Direct x.com page scrape (last resort) ===
    if (result.likes === 0 && result.views === 0) {
      try {
        var pageUrl = 'https://x.com/i/status/' + tweetId;
        var pageHtml = '';
        try {
          pageHtml = await fetchViaProxy(pageUrl, ua);
          console.log('[Twitter] X.com page via proxy length: ' + pageHtml.length);
        } catch(e) {
          try {
            pageHtml = await fetchDirect(pageUrl, ua);
            console.log('[Twitter] X.com page direct length: ' + pageHtml.length);
          } catch(e2) {}
        }
        if (pageHtml.length > 0) {
          extractFromPageHtml(pageHtml, result);
        }
      } catch(e) {
        console.log('[Twitter] X.com page scrape failed: ' + e.message);
      }
    }

    // shares = retweets + quote_tweets (for unified scoring with Instagram)
    result.shares = (result.retweets || 0) + (result.quote_tweets || 0);

    console.log('[Twitter] Final result for ' + tweetId + ': ' + JSON.stringify(result));
    return result;
  } catch (err) {
    console.error('[Twitter] Scrape failed for ' + url + ': ' + err.message);
    result.error = err.message;
    return result;
  }
}

// ===== EXTRACTION HELPERS =====

function extractFromSyndication(data, result) {
  // Syndication API returns: { favorite_count, retweet_count, reply_count, views_count, quote_count, bookmark_count }
  if (data.favorite_count != null && data.favorite_count > result.likes) {
    result.likes = Number(data.favorite_count);
  }
  if (data.retweet_count != null && data.retweet_count > result.retweets) {
    result.retweets = Number(data.retweet_count);
  }
  if (data.reply_count != null && data.reply_count > result.comments) {
    result.comments = Number(data.reply_count);
  }
  if (data.views_count != null) {
    var views = Number(data.views_count);
    if (views > result.views) result.views = views;
  }
  if (data.quote_count != null && data.quote_count > result.quote_tweets) {
    result.quote_tweets = Number(data.quote_count);
  }
  if (data.bookmark_count != null && data.bookmark_count > result.bookmarks) {
    result.bookmarks = Number(data.bookmark_count);
  }

  // Alternative field names
  if (data.favorites != null && Number(data.favorites) > result.likes) {
    result.likes = Number(data.favorites);
  }
  if (data.retweets != null && Number(data.retweets) > result.retweets) {
    result.retweets = Number(data.retweets);
  }
  if (data.replies != null && Number(data.replies) > result.comments) {
    result.comments = Number(data.replies);
  }

  // Sometimes nested in mediaDetails or __typename structures
  if (data.mediaDetails && Array.isArray(data.mediaDetails)) {
    data.mediaDetails.forEach(function(m) {
      if (m.viewCount != null && Number(m.viewCount) > result.views) {
        result.views = Number(m.viewCount);
      }
    });
  }
}

function extractFromFxTwitter(data, result) {
  // FxTwitter API: { code: 200, tweet: { likes, retweets, replies, views, quote_tweets, bookmarks } }
  var tweet = data.tweet || data;

  if (tweet.likes != null && Number(tweet.likes) > result.likes) result.likes = Number(tweet.likes);
  if (tweet.retweets != null && Number(tweet.retweets) > result.retweets) result.retweets = Number(tweet.retweets);
  if (tweet.replies != null && Number(tweet.replies) > result.comments) result.comments = Number(tweet.replies);
  if (tweet.views != null && Number(tweet.views) > result.views) result.views = Number(tweet.views);
  if (tweet.quote_tweets != null && Number(tweet.quote_tweets) > result.quote_tweets) result.quote_tweets = Number(tweet.quote_tweets);
  if (tweet.bookmarks != null && Number(tweet.bookmarks) > result.bookmarks) result.bookmarks = Number(tweet.bookmarks);

  // Alternative nested structure
  if (tweet.twitter_card && tweet.twitter_card.view_count) {
    var vc = Number(tweet.twitter_card.view_count);
    if (vc > result.views) result.views = vc;
  }
}

function extractFromNitter(html, result) {
  // Nitter shows stats in spans like: <span class="tweet-stat">...</span>
  // Likes
  var likesMatch = html.match(/icon-heart[^<]*<\/span>\s*<span[^>]*>([^<]+)/i) ||
                   html.match(/likes?["\s]*>[\s]*([0-9,.\s]+[KMkm]?)/i) ||
                   html.match(/favorites?["\s]*>[\s]*([0-9,.\s]+[KMkm]?)/i);
  if (likesMatch) {
    var l = parseNum(likesMatch[1]);
    if (l > result.likes) result.likes = l;
  }

  // Retweets
  var rtMatch = html.match(/icon-retweet[^<]*<\/span>\s*<span[^>]*>([^<]+)/i) ||
                html.match(/retweets?["\s]*>[\s]*([0-9,.\s]+[KMkm]?)/i);
  if (rtMatch) {
    var rt = parseNum(rtMatch[1]);
    if (rt > result.retweets) result.retweets = rt;
  }

  // Replies/comments
  var replyMatch = html.match(/icon-comment[^<]*<\/span>\s*<span[^>]*>([^<]+)/i) ||
                   html.match(/replies?["\s]*>[\s]*([0-9,.\s]+[KMkm]?)/i);
  if (replyMatch) {
    var r = parseNum(replyMatch[1]);
    if (r > result.comments) result.comments = r;
  }

  // Quotes
  var quoteMatch = html.match(/quotes?["\s]*>[\s]*([0-9,.\s]+[KMkm]?)/i);
  if (quoteMatch) {
    var q = parseNum(quoteMatch[1]);
    if (q > result.quote_tweets) result.quote_tweets = q;
  }
}

function extractFromOembed(data, result) {
  // oEmbed mostly gives HTML embed, but we can try to parse numbers from the HTML string
  if (data.html) {
    var html = data.html;

    // Sometimes engagement numbers are in the embed HTML
    var likesMatch = html.match(/(\d[\d,]*)\s*(?:likes?|♥)/i);
    if (likesMatch) {
      var l = parseNum(likesMatch[1]);
      if (l > result.likes) result.likes = l;
    }

    var rtMatch = html.match(/(\d[\d,]*)\s*(?:retweets?|RT)/i);
    if (rtMatch) {
      var rt = parseNum(rtMatch[1]);
      if (rt > result.retweets) result.retweets = rt;
    }
  }
}

function extractFromPageHtml(html, result) {
  // X.com page contains JSON data in script tags
  var unescaped = html.replace(/\\\\"/g, '"').replace(/\\"/g, '"').replace(/\\\\/g, '\\');

  // Look for engagement counts in JSON blobs
  var viewPatterns = [
    /view_count\\*"?\s*:\s*"?(\d+)/,
    /views_count\\*"?\s*:\s*"?(\d+)/,
    /viewCount\\*"?\s*:\s*"?(\d+)/,
    /"views"?\s*:\s*"?(\d+)/,
  ];
  for (var i = 0; i < viewPatterns.length; i++) {
    var vm = unescaped.match(viewPatterns[i]) || html.match(viewPatterns[i]);
    if (vm) { var v = parseInt(vm[1], 10); if (v > result.views) result.views = v; break; }
  }

  var likePatterns = [
    /favorite_count\\*"?\s*:\s*(\d+)/,
    /favourites_count\\*"?\s*:\s*(\d+)/,
    /like_count\\*"?\s*:\s*(\d+)/,
  ];
  for (var j = 0; j < likePatterns.length; j++) {
    var lm = unescaped.match(likePatterns[j]) || html.match(likePatterns[j]);
    if (lm) { var l = parseInt(lm[1], 10); if (l > result.likes) result.likes = l; break; }
  }

  var rtPatterns = [
    /retweet_count\\*"?\s*:\s*(\d+)/,
  ];
  for (var k = 0; k < rtPatterns.length; k++) {
    var rm = unescaped.match(rtPatterns[k]) || html.match(rtPatterns[k]);
    if (rm) { var r = parseInt(rm[1], 10); if (r > result.retweets) result.retweets = r; break; }
  }

  var replyPatterns = [
    /reply_count\\*"?\s*:\s*(\d+)/,
  ];
  for (var p = 0; p < replyPatterns.length; p++) {
    var rpm = unescaped.match(replyPatterns[p]) || html.match(replyPatterns[p]);
    if (rpm) { var rp = parseInt(rpm[1], 10); if (rp > result.comments) result.comments = rp; break; }
  }

  var quotePatterns = [
    /quote_count\\*"?\s*:\s*(\d+)/,
  ];
  for (var q = 0; q < quotePatterns.length; q++) {
    var qm = unescaped.match(quotePatterns[q]) || html.match(quotePatterns[q]);
    if (qm) { var qv = parseInt(qm[1], 10); if (qv > result.quote_tweets) result.quote_tweets = qv; break; }
  }

  var bookmarkPatterns = [
    /bookmark_count\\*"?\s*:\s*(\d+)/,
  ];
  for (var b = 0; b < bookmarkPatterns.length; b++) {
    var bm = unescaped.match(bookmarkPatterns[b]) || html.match(bookmarkPatterns[b]);
    if (bm) { var bv = parseInt(bm[1], 10); if (bv > result.bookmarks) result.bookmarks = bv; break; }
  }
}

// ===== NETWORK HELPERS (same pattern as instagram.js) =====

function extractId(url) {
  var m = url.match(/(?:twitter\.com|x\.com)\/\w+\/status\/(\d+)/);
  return m ? m[1] : null;
}

function fetchViaProxy(targetUrl, ua) {
  return new Promise(function(resolve, reject) {
    var timeout = setTimeout(function() { reject(new Error('Proxy timeout')); }, 30000);

    var parsed = new URL(targetUrl);
    var socket = new net.Socket();

    socket.connect(PROXY_PORT, PROXY_HOST, function() {
      var hasAuth = PROXY_USER && PROXY_PASS;
      if (hasAuth) {
        socket.write(Buffer.from([0x05, 0x02, 0x00, 0x02]));
      } else {
        socket.write(Buffer.from([0x05, 0x01, 0x00]));
      }
    });

    var step = 0;

    socket.on('data', function(chunk) {
      if (step === 0) {
        if (chunk[0] !== 0x05) { clearTimeout(timeout); socket.destroy(); return reject(new Error('Not SOCKS5')); }
        if (chunk[1] === 0x02) {
          var userBuf = Buffer.from(PROXY_USER);
          var passBuf = Buffer.from(PROXY_PASS);
          var authBuf = Buffer.alloc(3 + userBuf.length + passBuf.length);
          authBuf[0] = 0x01;
          authBuf[1] = userBuf.length;
          userBuf.copy(authBuf, 2);
          authBuf[2 + userBuf.length] = passBuf.length;
          passBuf.copy(authBuf, 3 + userBuf.length);
          socket.write(authBuf);
          step = 1;
        } else if (chunk[1] === 0x00) {
          sendConnect(socket, parsed.hostname, parseInt(parsed.port || '443'));
          step = 2;
        } else {
          clearTimeout(timeout); socket.destroy(); reject(new Error('Auth method rejected'));
        }
      } else if (step === 1) {
        if (chunk[1] !== 0x00) { clearTimeout(timeout); socket.destroy(); return reject(new Error('Auth failed')); }
        sendConnect(socket, parsed.hostname, parseInt(parsed.port || '443'));
        step = 2;
      } else if (step === 2) {
        if (chunk[1] !== 0x00) { clearTimeout(timeout); socket.destroy(); return reject(new Error('Connect failed: ' + chunk[1])); }

        var tls = require('tls');
        var tlsSocket = tls.connect({ socket: socket, servername: parsed.hostname }, function() {
          var req = 'GET ' + parsed.pathname + (parsed.search || '') + ' HTTP/1.1\r\n' +
            'Host: ' + parsed.hostname + '\r\n' +
            'User-Agent: ' + (ua || getRandomUA()) + '\r\n' +
            'Accept: text/html,application/xhtml+xml,application/xml;q=0.9,application/json,*/*;q=0.8\r\n' +
            'Accept-Language: en-US,en;q=0.9,fr;q=0.8\r\n' +
            'Accept-Encoding: identity\r\n' +
            'Sec-Ch-Ua: "Chromium";v="125", "Not(A:Brand";v="24"\r\n' +
            'Sec-Ch-Ua-Mobile: ?0\r\n' +
            'Sec-Ch-Ua-Platform: "Windows"\r\n' +
            'Sec-Fetch-Dest: document\r\n' +
            'Sec-Fetch-Mode: navigate\r\n' +
            'Sec-Fetch-Site: none\r\n' +
            'Sec-Fetch-User: ?1\r\n' +
            'Upgrade-Insecure-Requests: 1\r\n' +
            'Connection: close\r\n\r\n';
          tlsSocket.write(req);
        });

        var responseData = '';
        tlsSocket.on('data', function(d) { responseData += d.toString(); });
        tlsSocket.on('end', function() {
          clearTimeout(timeout);
          var bodyStart = responseData.indexOf('\r\n\r\n');
          if (bodyStart === -1) return resolve(responseData);
          resolve(responseData.slice(bodyStart + 4));
        });
        tlsSocket.on('error', function(e) { clearTimeout(timeout); reject(e); });
        step = 3;
      }
    });

    socket.on('error', function(e) { clearTimeout(timeout); reject(e); });
    socket.on('timeout', function() { clearTimeout(timeout); socket.destroy(); reject(new Error('Socket timeout')); });
    socket.setTimeout(25000);
  });
}

function sendConnect(socket, host, port) {
  var hostBuf = Buffer.from(host);
  var buf = Buffer.alloc(7 + hostBuf.length);
  buf[0] = 0x05;
  buf[1] = 0x01;
  buf[2] = 0x00;
  buf[3] = 0x03;
  buf[4] = hostBuf.length;
  hostBuf.copy(buf, 5);
  buf.writeUInt16BE(port, 5 + hostBuf.length);
  socket.write(buf);
}

function fetchDirect(url, ua) {
  return new Promise(function(resolve, reject) {
    var options = {
      headers: {
        'User-Agent': ua || getRandomUA(),
        'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,application/json,*/*;q=0.8',
        'Accept-Language': 'en-US,en;q=0.9,fr;q=0.8',
        'Accept-Encoding': 'identity',
        'Sec-Ch-Ua': '"Chromium";v="125", "Not(A:Brand";v="24"',
        'Sec-Ch-Ua-Mobile': '?0',
        'Sec-Ch-Ua-Platform': '"Windows"',
        'Sec-Fetch-Dest': 'document',
        'Sec-Fetch-Mode': 'navigate',
        'Sec-Fetch-Site': 'none',
        'Sec-Fetch-User': '?1',
        'Upgrade-Insecure-Requests': '1',
      }
    };
    function doReq(u, redir) {
      if (redir > 5) return reject(new Error('Too many redirects'));
      https.get(u, options, function(res) {
        if (res.statusCode >= 300 && res.statusCode < 400 && res.headers.location) {
          var loc = res.headers.location;
          if (loc.startsWith('/')) loc = 'https://x.com' + loc;
          return doReq(loc, redir + 1);
        }
        var d = '';
        res.on('data', function(c) { d += c; });
        res.on('end', function() { resolve(d); });
        res.on('error', reject);
      }).on('error', reject);
    }
    doReq(url, 0);
  });
}

function parseNum(str) {
  if (!str) return 0;
  str = str.toString().replace(/,/g, '').replace(/\s/g, '').trim();
  var multipliers = { k: 1000, m: 1000000, b: 1000000000 };
  var match = str.match(/^([\d.]+)\s*([KMBkmb])?$/);
  if (!match) return parseInt(str, 10) || 0;
  var num = parseFloat(match[1]);
  var mult = match[2] ? multipliers[match[2].toLowerCase()] || 1 : 1;
  return Math.round(num * mult);
}

module.exports = { scrapeTweet: scrapeTweet };
