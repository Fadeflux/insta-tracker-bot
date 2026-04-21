var https = require('https');
var net = require('net');

var PROXY_HOST = process.env.PROXY_HOST || '5.161.16.191';
var PROXY_PORT = parseInt(process.env.PROXY_PORT || '19751');
var PROXY_USER = process.env.PROXY_USER || '';
var PROXY_PASS = process.env.PROXY_PASS || '';

var USER_AGENTS = [
  'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/125.0.0.0 Safari/537.36',
  'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/125.0.0.0 Safari/537.36',
  'Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/125.0.0.0 Safari/537.36',
  'Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:126.0) Gecko/20100101 Firefox/126.0',
];

function getRandomUA() {
  return USER_AGENTS[Math.floor(Math.random() * USER_AGENTS.length)];
}

async function scrapeTweet(url) {
  var result = { views: 0, likes: 0, comments: 0, shares: 0, retweets: 0, quote_tweets: 0, bookmarks: 0 };
  var tweetId = extractId(url);
  if (!tweetId) { result.error = 'Invalid Twitter URL'; return result; }

  // Extract username from URL for APIs that need it
  var userMatch = url.match(/(?:twitter\.com|x\.com)\/(\w+)\/status\//);
  var username = userMatch ? userMatch[1] : 'i';

  try {
    // === STRATEGY 1: FxTwitter API (best — supports NSFW via elongator) ===
    try {
      var fxUrl = 'https://api.fxtwitter.com/' + username + '/status/' + tweetId;
      var fxData = await fetchDirect(fxUrl);
      console.log('[Twitter] FxTwitter length: ' + fxData.length);

      if (fxData.length > 0 && fxData.trimStart()[0] === '{') {
        var fxJson = JSON.parse(fxData.trim());
        if (fxJson.tweet) {
          var tw = fxJson.tweet;
          if (tw.replies != null) result.comments = Number(tw.replies);
          if (tw.retweets != null) result.retweets = Number(tw.retweets);
          if (tw.likes != null) result.likes = Number(tw.likes);
          if (tw.views != null) result.views = Number(tw.views);
          if (tw.quote_tweets != null) result.quote_tweets = Number(tw.quote_tweets);
          if (tw.bookmarks != null) result.bookmarks = Number(tw.bookmarks);
          console.log('[Twitter] FxTwitter OK: views=' + result.views + ' likes=' + result.likes + ' replies=' + result.comments + ' rt=' + result.retweets);
        }
      }
    } catch(e) {
      console.log('[Twitter] FxTwitter failed: ' + e.message);
    }

    // === STRATEGY 2: vxTwitter API (good fallback, different codebase) ===
    if (result.views === 0 && result.likes === 0) {
      try {
        var vxUrl = 'https://api.vxtwitter.com/' + username + '/status/' + tweetId;
        var vxData = await fetchDirect(vxUrl);
        console.log('[Twitter] vxTwitter length: ' + vxData.length);

        if (vxData.length > 0 && vxData.trimStart()[0] === '{') {
          var vxJson = JSON.parse(vxData.trim());
          if (vxJson.likes != null && Number(vxJson.likes) > result.likes) result.likes = Number(vxJson.likes);
          if (vxJson.retweets != null && Number(vxJson.retweets) > result.retweets) result.retweets = Number(vxJson.retweets);
          if (vxJson.replies != null && Number(vxJson.replies) > result.comments) result.comments = Number(vxJson.replies);
          if (vxJson.views != null && Number(vxJson.views) > result.views) result.views = Number(vxJson.views);
          if (vxJson.qrtCount != null && Number(vxJson.qrtCount) > result.quote_tweets) result.quote_tweets = Number(vxJson.qrtCount);
          if (vxJson.bookmarks != null && Number(vxJson.bookmarks) > result.bookmarks) result.bookmarks = Number(vxJson.bookmarks);
          console.log('[Twitter] vxTwitter OK: views=' + result.views + ' likes=' + result.likes + ' replies=' + result.comments);
        }
      } catch(e) {
        console.log('[Twitter] vxTwitter failed: ' + e.message);
      }
    }

    // === STRATEGY 3: Syndication API (official Twitter, but blocks NSFW) ===
    if (result.views === 0 && result.likes === 0) {
      try {
        var synUrl = 'https://cdn.syndication.twimg.com/tweet-result?id=' + tweetId + '&lang=en&token=x';
        var synData = '';
        try { synData = await fetchDirect(synUrl); } catch(e) {
          try { synData = await fetchViaProxy(synUrl); } catch(e2) {}
        }

        if (synData.length > 0 && synData.trimStart()[0] === '{') {
          var synJson = JSON.parse(synData.trim());
          if (synJson.favorite_count != null && Number(synJson.favorite_count) > result.likes) result.likes = Number(synJson.favorite_count);
          if (synJson.retweet_count != null && Number(synJson.retweet_count) > result.retweets) result.retweets = Number(synJson.retweet_count);
          if (synJson.reply_count != null && Number(synJson.reply_count) > result.comments) result.comments = Number(synJson.reply_count);
          if (synJson.views_count != null && Number(synJson.views_count) > result.views) result.views = Number(synJson.views_count);
          // Check nested mediaDetails for video views
          if (synJson.mediaDetails && Array.isArray(synJson.mediaDetails)) {
            synJson.mediaDetails.forEach(function(m) {
              if (m.viewCount != null && Number(m.viewCount) > result.views) result.views = Number(m.viewCount);
            });
          }
          console.log('[Twitter] Syndication OK: views=' + result.views + ' likes=' + result.likes);
        }
      } catch(e) {
        console.log('[Twitter] Syndication failed: ' + e.message);
      }
    }

    // === STRATEGY 4: FxTwitter via proxy (if direct was blocked) ===
    if (result.views === 0 && result.likes === 0) {
      try {
        var fxUrl2 = 'https://api.fxtwitter.com/' + username + '/status/' + tweetId;
        var fxData2 = await fetchViaProxy(fxUrl2);
        console.log('[Twitter] FxTwitter via proxy length: ' + fxData2.length);

        if (fxData2.length > 0 && fxData2.trimStart()[0] === '{') {
          var fxJson2 = JSON.parse(fxData2.trim());
          if (fxJson2.tweet) {
            var tw2 = fxJson2.tweet;
            if (tw2.replies != null && Number(tw2.replies) > result.comments) result.comments = Number(tw2.replies);
            if (tw2.retweets != null && Number(tw2.retweets) > result.retweets) result.retweets = Number(tw2.retweets);
            if (tw2.likes != null && Number(tw2.likes) > result.likes) result.likes = Number(tw2.likes);
            if (tw2.views != null && Number(tw2.views) > result.views) result.views = Number(tw2.views);
            if (tw2.quote_tweets != null && Number(tw2.quote_tweets) > result.quote_tweets) result.quote_tweets = Number(tw2.quote_tweets);
            if (tw2.bookmarks != null && Number(tw2.bookmarks) > result.bookmarks) result.bookmarks = Number(tw2.bookmarks);
            console.log('[Twitter] FxTwitter proxy OK');
          }
        }
      } catch(e) {
        console.log('[Twitter] FxTwitter proxy failed: ' + e.message);
      }
    }

    // === STRATEGY 5: Direct x.com page scrape (last resort) ===
    if (result.views === 0 && result.likes === 0) {
      try {
        var pageUrl = 'https://x.com/' + username + '/status/' + tweetId;
        var pageHtml = '';
        try { pageHtml = await fetchViaProxy(pageUrl); } catch(e) {
          try { pageHtml = await fetchDirect(pageUrl); } catch(e2) {}
        }
        if (pageHtml.length > 1000) {
          extractFromPageHtml(pageHtml, result);
          console.log('[Twitter] Page scrape: views=' + result.views + ' likes=' + result.likes);
        }
      } catch(e) {
        console.log('[Twitter] Page scrape failed: ' + e.message);
      }
    }

    // shares = retweets + quote_tweets (for unified scoring)
    result.shares = (result.retweets || 0) + (result.quote_tweets || 0);

    console.log('[Twitter] Final for ' + tweetId + ': ' + JSON.stringify(result));
    return result;
  } catch (err) {
    console.error('[Twitter] Scrape failed for ' + url + ': ' + err.message);
    result.error = err.message;
    return result;
  }
}

// ===== PAGE HTML EXTRACTION (last resort) =====

function extractFromPageHtml(html, result) {
  var unescaped = html.replace(/\\\\"/g, '"').replace(/\\"/g, '"').replace(/\\\\/g, '\\');

  var patterns = {
    views: [/view_count["\s:]*"?(\d+)/, /views_count["\s:]*"?(\d+)/, /viewCount["\s:]*"?(\d+)/],
    likes: [/favorite_count["\s:]*(\d+)/, /like_count["\s:]*(\d+)/, /favourites_count["\s:]*(\d+)/],
    retweets: [/retweet_count["\s:]*(\d+)/],
    comments: [/reply_count["\s:]*(\d+)/],
    quote_tweets: [/quote_count["\s:]*(\d+)/],
    bookmarks: [/bookmark_count["\s:]*(\d+)/],
  };

  Object.keys(patterns).forEach(function(field) {
    patterns[field].forEach(function(pat) {
      if (result[field] === 0) {
        var m = unescaped.match(pat) || html.match(pat);
        if (m) {
          var val = parseInt(m[1], 10);
          if (val > result[field]) result[field] = val;
        }
      }
    });
  });
}

// ===== NETWORK HELPERS =====

function extractId(url) {
  var m = url.match(/(?:twitter\.com|x\.com)\/\w+\/status\/(\d+)/);
  return m ? m[1] : null;
}

function fetchDirect(url) {
  return new Promise(function(resolve, reject) {
    var timeout = setTimeout(function() { reject(new Error('Timeout')); }, 20000);
    var options = {
      headers: {
        'User-Agent': getRandomUA(),
        'Accept': 'application/json, text/html, */*',
        'Accept-Language': 'en-US,en;q=0.9',
        'Accept-Encoding': 'identity',
      }
    };
    function doReq(u, redir) {
      if (redir > 5) { clearTimeout(timeout); return reject(new Error('Too many redirects')); }
      https.get(u, options, function(res) {
        if (res.statusCode >= 300 && res.statusCode < 400 && res.headers.location) {
          var loc = res.headers.location;
          if (loc.startsWith('/')) {
            var parsed = new URL(u);
            loc = parsed.protocol + '//' + parsed.host + loc;
          }
          return doReq(loc, redir + 1);
        }
        var d = '';
        res.on('data', function(c) { d += c; });
        res.on('end', function() { clearTimeout(timeout); resolve(d); });
        res.on('error', function(e) { clearTimeout(timeout); reject(e); });
      }).on('error', function(e) { clearTimeout(timeout); reject(e); });
    }
    doReq(url, 0);
  });
}

function fetchViaProxy(targetUrl) {
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
          authBuf[0] = 0x01; authBuf[1] = userBuf.length;
          userBuf.copy(authBuf, 2);
          authBuf[2 + userBuf.length] = passBuf.length;
          passBuf.copy(authBuf, 3 + userBuf.length);
          socket.write(authBuf);
          step = 1;
        } else if (chunk[1] === 0x00) {
          sendConnect(socket, parsed.hostname, parseInt(parsed.port || '443'));
          step = 2;
        } else { clearTimeout(timeout); socket.destroy(); reject(new Error('Auth rejected')); }
      } else if (step === 1) {
        if (chunk[1] !== 0x00) { clearTimeout(timeout); socket.destroy(); return reject(new Error('Auth failed')); }
        sendConnect(socket, parsed.hostname, parseInt(parsed.port || '443'));
        step = 2;
      } else if (step === 2) {
        if (chunk[1] !== 0x00) { clearTimeout(timeout); socket.destroy(); return reject(new Error('Connect failed')); }
        var tls = require('tls');
        var tlsSocket = tls.connect({ socket: socket, servername: parsed.hostname }, function() {
          var req = 'GET ' + parsed.pathname + (parsed.search || '') + ' HTTP/1.1\r\n' +
            'Host: ' + parsed.hostname + '\r\n' +
            'User-Agent: ' + getRandomUA() + '\r\n' +
            'Accept: application/json, text/html, */*\r\n' +
            'Accept-Language: en-US,en;q=0.9\r\n' +
            'Accept-Encoding: identity\r\n' +
            'Connection: close\r\n\r\n';
          tlsSocket.write(req);
        });
        var responseData = '';
        tlsSocket.on('data', function(d) { responseData += d.toString(); });
        tlsSocket.on('end', function() {
          clearTimeout(timeout);
          var bodyStart = responseData.indexOf('\r\n\r\n');
          resolve(bodyStart === -1 ? responseData : responseData.slice(bodyStart + 4));
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
  buf[0] = 0x05; buf[1] = 0x01; buf[2] = 0x00; buf[3] = 0x03;
  buf[4] = hostBuf.length;
  hostBuf.copy(buf, 5);
  buf.writeUInt16BE(port, 5 + hostBuf.length);
  socket.write(buf);
}

module.exports = { scrapeTweet: scrapeTweet };
