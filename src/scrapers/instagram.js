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

async function scrapePost(url) {
  var result = { views: 0, likes: 0, comments: 0, shares: 0 };
  var postId = extractId(url);
  if (!postId) { result.error = 'Invalid URL'; return result; }

  var isReel = url.includes('/reel/');
  var ua = getRandomUA();

  try {
    // === STRATEGY 1: Embed with /captioned/ (more data) ===
    var embedUrl = 'https://www.instagram.com/p/' + postId + '/embed/captioned/';
    var embedHtml = '';
    try {
      embedHtml = await fetchViaProxy(embedUrl, ua);
      console.log('Embed via proxy length: ' + embedHtml.length);
    } catch(e) {
      console.log('Embed proxy failed, trying direct: ' + e.message);
      try {
        embedHtml = await fetchDirect(embedUrl, ua);
        console.log('Embed direct length: ' + embedHtml.length);
      } catch(e2) {
        console.log('Embed direct also failed: ' + e2.message);
      }
    }

    if (embedHtml.length > 0) {
      extractFromHtml(embedHtml, result);
      extractFromScripts(embedHtml, result);
    }

    // === STRATEGY 2: GraphQL query endpoint ===
    if (result.likes === 0 && result.views === 0) {
      try {
        var gqlUrl = 'https://www.instagram.com/graphql/query/?query_hash=b3055c01b4b222b8a47dc12b090e4e64&variables=' +
          encodeURIComponent(JSON.stringify({ shortcode: postId, child_comment_count: 0, fetch_comment_count: 0, parent_comment_count: 0, has_threaded_comments: false }));
        var gqlData = await fetchViaProxy(gqlUrl, ua);
        console.log('GraphQL length: ' + gqlData.length);
        if (gqlData.length > 0 && gqlData.trimStart()[0] === '{') {
          try {
            var gqlJson = JSON.parse(gqlData.trim());
            var media = gqlJson.data && gqlJson.data.shortcode_media;
            if (media) {
              if (media.edge_media_preview_like && result.likes === 0)
                result.likes = media.edge_media_preview_like.count || 0;
              if (media.edge_media_to_comment && result.comments === 0)
                result.comments = media.edge_media_to_comment.count || 0;
              if (media.video_view_count != null && result.views === 0)
                result.views = media.video_view_count || 0;
              if (media.edge_media_preview_comment && result.comments === 0)
                result.comments = media.edge_media_preview_comment.count || 0;
              console.log('GraphQL parsed OK for ' + postId);
            }
          } catch(pe) {
            console.log('GraphQL JSON parse failed: ' + pe.message);
          }
        }
      } catch(e) {
        console.log('GraphQL fetch failed: ' + e.message);
      }
    }

    // === STRATEGY 3: ?__a=1&__d=dis JSON endpoint ===
    if (result.likes === 0 && result.views === 0) {
      try {
        var aUrl = 'https://www.instagram.com/' + (isReel ? 'reel' : 'p') + '/' + postId + '/?__a=1&__d=dis';
        var aData = await fetchViaProxy(aUrl, ua);
        console.log('__a=1 length: ' + aData.length);
        if (aData.length > 0 && aData.trimStart()[0] === '{') {
          try {
            var aJson = JSON.parse(aData.trim());
            var items = aJson.items || (aJson.graphql && aJson.graphql.shortcode_media ? [aJson.graphql.shortcode_media] : []);
            if (items.length > 0) {
              var item = items[0];
              if (item.like_count != null && result.likes === 0) result.likes = item.like_count;
              if (item.comment_count != null && result.comments === 0) result.comments = item.comment_count;
              if (item.play_count != null && result.views === 0) result.views = item.play_count;
              if (item.view_count != null && result.views === 0) result.views = item.view_count;
              console.log('__a=1 parsed OK for ' + postId);
            }
          } catch(pe2) {
            console.log('__a=1 JSON parse failed');
          }
        }
      } catch(e) {
        console.log('__a=1 fetch failed: ' + e.message);
      }
    }

    // === STRATEGY 4: Main page with full browser headers ===
    if (result.likes === 0 && result.views === 0) {
      var mainUrl = 'https://www.instagram.com/' + (isReel ? 'reel' : 'p') + '/' + postId + '/';
      var mainHtml = '';
      try {
        mainHtml = await fetchViaProxy(mainUrl, ua);
        console.log('Main via proxy length: ' + mainHtml.length);
      } catch(e2) {
        try {
          mainHtml = await fetchDirect(mainUrl, ua);
          console.log('Main direct length: ' + mainHtml.length);
        } catch(e3) {
          console.log('Main fetch failed: ' + e3.message);
        }
      }

      if (mainHtml.length > 0) {
        extractFromHtml(mainHtml, result);
        extractFromScripts(mainHtml, result);
        extractFromOgMeta(mainHtml, result);
      }
    }

    // === STRATEGY 5: Reel-specific page ===
    if (result.views === 0 && isReel) {
      try {
        var reelUrl = 'https://www.instagram.com/reel/' + postId + '/';
        var reelHtml = await fetchViaProxy(reelUrl, ua);
        console.log('Reel via proxy length: ' + reelHtml.length);
        extractFromHtml(reelHtml, result);
        extractFromScripts(reelHtml, result);
      } catch(e3) {
        console.log('Reel fetch failed: ' + e3.message);
      }
    }

    // === STRATEGY 6: Embed without proxy as last resort ===
    if (result.likes === 0 && result.views === 0) {
      try {
        var embedUrl2 = 'https://www.instagram.com/p/' + postId + '/embed/captioned/';
        var embedHtml2 = await fetchDirect(embedUrl2, ua);
        console.log('Embed direct fallback length: ' + embedHtml2.length);
        if (embedHtml2.length > 0) {
          extractFromHtml(embedHtml2, result);
          extractFromScripts(embedHtml2, result);
        }
      } catch(e) {
        console.log('Embed direct fallback failed: ' + e.message);
      }
    }

    console.log('Final result for ' + postId + ': ' + JSON.stringify(result));
    return result;
  } catch (err) {
    console.error('Scrape failed for ' + url + ': ' + err.message);
    result.error = err.message;
    return result;
  }
}

// ===== EXTRACTION HELPERS =====

function extractFromHtml(html, result) {
  // First, try to unescape the HTML to normalize the JSON
  var unescaped = html.replace(/\\\\"/g, '"').replace(/\\"/g, '"').replace(/\\\\/g, '\\');

  // likes
  if (result.likes === 0) {
    var likesPatterns = [
      /like_count\\*"?\s*:\s*(\d+)/,
      /edge_media_preview_like\\*"?\s*:\s*\{?\s*\\*"?count\\*"?\s*:\s*(\d+)/,
      /edge_liked_by\\*"?\s*:\s*\{?\s*\\*"?count\\*"?\s*:\s*(\d+)/,
    ];
    for (var i = 0; i < likesPatterns.length; i++) {
      var m = unescaped.match(likesPatterns[i]) || html.match(likesPatterns[i]);
      if (m) { result.likes = parseNum(m[1]); break; }
    }
  }

  // comments
  if (result.comments === 0) {
    var commentsPatterns = [
      /comment_count\\*"?\s*:\s*(\d+)/,
      /edge_media_to_comment\\*"?\s*:\s*\{?\s*\\*"?count\\*"?\s*:\s*(\d+)/,
      /edge_media_preview_comment\\*"?\s*:\s*\{?\s*\\*"?count\\*"?\s*:\s*(\d+)/,
      /edge_media_to_parent_comment\\*"?\s*:\s*\{?\s*\\*"?count\\*"?\s*:\s*(\d+)/,
    ];
    for (var j = 0; j < commentsPatterns.length; j++) {
      var mc = unescaped.match(commentsPatterns[j]) || html.match(commentsPatterns[j]);
      if (mc) { result.comments = parseNum(mc[1]); break; }
    }
  }

  // views
  if (result.views === 0) {
    var viewsPatterns = [
      /video_view_count\\*"?\s*:\s*(\d+)/,
      /play_count\\*"?\s*:\s*(\d+)/,
      /view_count\\*"?\s*:\s*(\d+)/,
      /video_play_count\\*"?\s*:\s*(\d+)/,
      /ig_play_count\\*"?\s*:\s*(\d+)/,
    ];
    for (var k = 0; k < viewsPatterns.length; k++) {
      var mv = unescaped.match(viewsPatterns[k]) || html.match(viewsPatterns[k]);
      if (mv) { result.views = parseNum(mv[1]); break; }
    }
  }

  // shares
  if (result.shares === 0) {
    var sharesPatterns = [
      /share_count\\*"?\s*:\s*(\d+)/,
      /reshare_count\\*"?\s*:\s*(\d+)/,
    ];
    for (var s = 0; s < sharesPatterns.length; s++) {
      var ms = unescaped.match(sharesPatterns[s]) || html.match(sharesPatterns[s]);
      if (ms) { result.shares = parseNum(ms[1]); break; }
    }
  }
}

function extractFromScripts(html, result) {
  if (result.likes > 0 && result.views > 0) return;

  var scriptPatterns = [
    /window\.__additionalDataLoaded\s*\(\s*['"][^'"]*['"]\s*,\s*(\{.+?\})\s*\)\s*;/,
    /window\._sharedData\s*=\s*(\{.+?\})\s*;/,
    /window\.__initialData\s*=\s*(\{.+?\})\s*;/,
    /<script[^>]*type="application\/ld\+json"[^>]*>(\{.+?\})<\/script>/,
  ];

  for (var i = 0; i < scriptPatterns.length; i++) {
    var m = html.match(scriptPatterns[i]);
    if (m) {
      try {
        var data = JSON.parse(m[1]);
        extractFromHtml(JSON.stringify(data), result);
        if (result.likes > 0 || result.views > 0) {
          console.log('Found data in script pattern ' + i);
          break;
        }
      } catch(e) {
        extractFromHtml(m[1], result);
      }
    }
  }

  // Search for JSON blobs containing stats
  if (result.likes === 0 && result.views === 0) {
    var blobPatterns = [
      /\{"like_count":\d+[^}]*\}/g,
      /\{"play_count":\d+[^}]*\}/g,
      /\{"video_view_count":\d+[^}]*\}/g,
    ];
    for (var b = 0; b < blobPatterns.length; b++) {
      var blobs = html.match(blobPatterns[b]);
      if (blobs) {
        for (var j = 0; j < blobs.length; j++) {
          extractFromHtml(blobs[j], result);
          if (result.likes > 0 || result.views > 0) break;
        }
        if (result.likes > 0 || result.views > 0) break;
      }
    }
  }
}

function extractFromOgMeta(html, result) {
  var ogMatch = html.match(/<meta[^>]*property="og:description"[^>]*content="([^"]*)"/) ||
                html.match(/<meta[^>]*content="([^"]*)"[^>]*property="og:description"/);
  if (ogMatch) {
    var desc = ogMatch[1];
    console.log('OG description: ' + desc.substring(0, 120));
    if (result.likes === 0) {
      var lm = desc.match(/([\d,.\s]+[KMkm]?)\s*likes?/i);
      if (lm) result.likes = parseNum(lm[1]);
    }
    if (result.comments === 0) {
      var cm = desc.match(/([\d,.\s]+[KMkm]?)\s*comments?/i);
      if (cm) result.comments = parseNum(cm[1]);
    }
    if (result.views === 0) {
      var vm = desc.match(/([\d,.\s]+[KMkm]?)\s*(?:views?|plays?|vues?)/i);
      if (vm) result.views = parseNum(vm[1]);
    }
  }
}

// ===== NETWORK HELPERS =====

function extractId(url) {
  var m = url.match(/instagram\.com\/(?:p|reel|tv)\/([A-Za-z0-9_-]+)/);
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
            'Accept: text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8\r\n' +
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
        'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,*/*;q=0.8',
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
          if (loc.startsWith('/')) loc = 'https://www.instagram.com' + loc;
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
