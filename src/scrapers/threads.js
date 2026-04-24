// Threads scraper.
//
// Strategy: Threads is Meta's Twitter clone and shares a lot of infra with IG.
// Unlike IG, Threads does NOT expose the ?__a=1 or /embed/captioned/ endpoints.
// What we CAN do without login:
//   1. Fetch the public post page HTML
//   2. Parse the embedded JSON (Server-Side Rendered) for counts
//   3. Fallback on OpenGraph / Twitter meta tags
//   4. Extract the username from URL or from the og:title/meta
//
// Known limits without login:
//   - Views count is usually NOT available (Threads hides it to anon visitors)
//   - Likes/reposts counts ARE usually in the HTML
//   - Replies count ARE usually in the HTML
// => We treat views as 0 when unavailable and document this to user.

var https = require('https');
var http = require('http');
var net = require('net');

var PROXY_HOST = process.env.PROXY_HOST || '5.161.16.191';
var PROXY_PORT = parseInt(process.env.PROXY_PORT || '19751');
var PROXY_USER = process.env.PROXY_USER || '';
var PROXY_PASS = process.env.PROXY_PASS || '';
var VIEW_MULTIPLIER = parseFloat(process.env.VIEW_MULTIPLIER || '1');

var USER_AGENTS = [
  'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/125.0.0.0 Safari/537.36',
  'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/125.0.0.0 Safari/537.36',
  'Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/125.0.0.0 Safari/537.36',
  'Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:126.0) Gecko/20100101 Firefox/126.0',
];

function getRandomUA() { return USER_AGENTS[Math.floor(Math.random() * USER_AGENTS.length)]; }

// Extract the post code from a Threads URL.
// Supports: threads.net/@user/post/CODE, threads.com/@user/post/CODE,
//           threads.net/t/CODE (short form)
function extractId(url) {
  if (!url) return null;
  var m = url.match(/threads\.(?:net|com)\/(?:@[^\/]+\/post|t)\/([A-Za-z0-9_-]+)/);
  return m ? m[1] : null;
}

// Extract the author username from a Threads URL if present.
function extractUsername(url) {
  if (!url) return null;
  var m = url.match(/threads\.(?:net|com)\/@([A-Za-z0-9_.]+)/);
  return m ? m[1] : null;
}

async function scrapePost(url) {
  var result = { views: 0, likes: 0, comments: 0, shares: 0, username: null };
  var postCode = extractId(url);
  if (!postCode) { result.error = 'Invalid URL'; return result; }

  // Username: first try URL-based, fallback to meta during parse
  result.username = extractUsername(url);

  var ua = getRandomUA();
  var normalizedUrl = url.split('?')[0].replace('threads.com', 'threads.net');

  // === STRATEGY 1: Public page fetch via proxy ===
  var html = '';
  try {
    html = await fetchViaProxy(normalizedUrl, ua);
    console.log('[Threads] Page via proxy length: ' + html.length);
  } catch (e) {
    console.log('[Threads] Proxy fetch failed, trying direct: ' + e.message);
    try {
      html = await fetchDirect(normalizedUrl, ua);
      console.log('[Threads] Page direct length: ' + html.length);
    } catch (e2) {
      console.log('[Threads] Direct fetch also failed: ' + e2.message);
      result.error = 'All fetch strategies failed: ' + e2.message;
      return result;
    }
  }

  if (!html || html.length < 1000) {
    result.error = 'Empty/short response (login wall?)';
    return result;
  }

  // === USERNAME FALLBACK: og:title pattern "Name (@username) on Threads" ===
  if (!result.username) {
    var ogT = html.match(/<meta\s+property="og:title"\s+content="([^"]*@([A-Za-z0-9_.]+)[^"]*)"/i);
    if (ogT) {
      result.username = ogT[2];
    } else {
      // Try meta "twitter:title"
      var twT = html.match(/<meta\s+(?:name|property)="twitter:title"\s+content="[^@]*@([A-Za-z0-9_.]+)[^"]*"/i);
      if (twT) result.username = twT[1];
    }
  }

  // === STRATEGY 2: Parse embedded JSON for counts ===
  // Threads SSR injects <script type="application/json">...</script> with data.
  // We look for keys: like_count, text_post_app_info.direct_reply_count,
  //                    view_count, repost_count, quote_count
  var likes = findFirstNumber(html, [/"like_count"\s*:\s*(\d+)/]);
  var comments = findFirstNumber(html, [
    /"direct_reply_count"\s*:\s*(\d+)/,
    /"reply_count"\s*:\s*(\d+)/,
  ]);
  var shares = findFirstNumber(html, [
    /"repost_count"\s*:\s*(\d+)/,
    /"quote_count"\s*:\s*(\d+)/,
  ]);
  var views = findFirstNumber(html, [/"view_count"\s*:\s*(\d+)/]);

  if (likes != null) result.likes = likes;
  if (comments != null) result.comments = comments;
  if (shares != null) result.shares = shares;
  if (views != null) result.views = Math.round(views * VIEW_MULTIPLIER);

  // === STRATEGY 3: Fallback on meta tags for counts (less reliable, text-formatted) ===
  if (result.likes === 0 && result.comments === 0) {
    // Some pages have a meta description like: "2.3K likes, 45 replies..."
    var desc = html.match(/<meta\s+(?:name|property)="(?:og:description|description)"\s+content="([^"]+)"/i);
    if (desc) {
      var txt = desc[1];
      var mLike = txt.match(/([\d.,]+[KMB]?)\s*likes?/i);
      if (mLike) result.likes = parseCount(mLike[1]);
      var mRep = txt.match(/([\d.,]+[KMB]?)\s*repl/i);
      if (mRep) result.comments = parseCount(mRep[1]);
    }
  }

  console.log('[Threads] Post ' + postCode + ' — username=' + (result.username || '?') + ' views=' + result.views + ' likes=' + result.likes + ' comments=' + result.comments + ' shares=' + result.shares);

  // If we couldn't parse anything meaningful, flag as partial
  if (result.likes === 0 && result.comments === 0 && result.views === 0) {
    result.error = 'Could not extract stats (login required?)';
  }

  return result;
}

function findFirstNumber(html, patterns) {
  for (var i = 0; i < patterns.length; i++) {
    var m = html.match(patterns[i]);
    if (m) return parseInt(m[1], 10) || 0;
  }
  return null;
}

function parseCount(str) {
  if (!str) return 0;
  var s = String(str).trim().replace(/,/g, '');
  var multipliers = { k: 1000, m: 1000000, b: 1000000000 };
  var match = s.match(/^([\d.]+)\s*([KMBkmb])?$/);
  if (!match) return parseInt(s, 10) || 0;
  var num = parseFloat(match[1]);
  var mult = match[2] ? multipliers[match[2].toLowerCase()] || 1 : 1;
  return Math.round(num * mult);
}

// ==============================
// FETCH HELPERS (proxy + direct)
// ==============================

function fetchDirect(url, ua) {
  return new Promise(function(resolve, reject) {
    var req = https.get(url, {
      headers: {
        'User-Agent': ua,
        'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8',
        'Accept-Language': 'en-US,en;q=0.9,fr-FR;q=0.8',
        'Accept-Encoding': 'identity',
      },
      timeout: 15000,
    }, function(res) {
      if (res.statusCode >= 300 && res.statusCode < 400 && res.headers.location) {
        return fetchDirect(res.headers.location, ua).then(resolve, reject);
      }
      var chunks = [];
      res.on('data', function(c) { chunks.push(c); });
      res.on('end', function() { resolve(Buffer.concat(chunks).toString('utf8')); });
    });
    req.on('timeout', function() { req.destroy(); reject(new Error('timeout')); });
    req.on('error', reject);
  });
}

function fetchViaProxy(url, ua) {
  return new Promise(function(resolve, reject) {
    if (!PROXY_HOST) return reject(new Error('no proxy configured'));

    var parsed = new URL(url);
    var targetHost = parsed.hostname;
    var targetPort = 443;

    var sock = net.connect(PROXY_PORT, PROXY_HOST, function() {
      // Send HTTP CONNECT (assuming HTTP proxy; SOCKS5 requires different handshake)
      var auth = PROXY_USER ? 'Proxy-Authorization: Basic ' + Buffer.from(PROXY_USER + ':' + PROXY_PASS).toString('base64') + '\r\n' : '';
      sock.write('CONNECT ' + targetHost + ':' + targetPort + ' HTTP/1.1\r\nHost: ' + targetHost + ':' + targetPort + '\r\n' + auth + '\r\n');
    });

    var connectBuffer = '';
    var connected = false;

    sock.on('data', function(chunk) {
      if (!connected) {
        connectBuffer += chunk.toString('utf8');
        if (connectBuffer.indexOf('\r\n\r\n') !== -1) {
          if (connectBuffer.indexOf('200') === -1) {
            sock.destroy();
            return reject(new Error('proxy CONNECT failed: ' + connectBuffer.split('\r\n')[0]));
          }
          connected = true;

          // Upgrade to TLS
          var tls = require('tls');
          var tlsSock = tls.connect({
            socket: sock,
            servername: targetHost,
            rejectUnauthorized: false,
          }, function() {
            var path = parsed.pathname + (parsed.search || '');
            tlsSock.write(
              'GET ' + path + ' HTTP/1.1\r\n' +
              'Host: ' + targetHost + '\r\n' +
              'User-Agent: ' + ua + '\r\n' +
              'Accept: text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8\r\n' +
              'Accept-Language: en-US,en;q=0.9\r\n' +
              'Accept-Encoding: identity\r\n' +
              'Connection: close\r\n\r\n'
            );
          });

          var response = Buffer.alloc(0);
          tlsSock.on('data', function(d) { response = Buffer.concat([response, d]); });
          tlsSock.on('end', function() {
            var raw = response.toString('utf8');
            var sep = raw.indexOf('\r\n\r\n');
            var body = sep !== -1 ? raw.substring(sep + 4) : raw;
            // Handle chunked transfer encoding
            if (/Transfer-Encoding:\s*chunked/i.test(raw.substring(0, sep))) {
              body = decodeChunked(body);
            }
            resolve(body);
          });
          tlsSock.on('error', reject);
        }
      }
    });

    sock.on('error', reject);
    sock.setTimeout(15000, function() { sock.destroy(); reject(new Error('proxy timeout')); });
  });
}

function decodeChunked(body) {
  var out = '';
  var idx = 0;
  while (idx < body.length) {
    var end = body.indexOf('\r\n', idx);
    if (end === -1) break;
    var sizeStr = body.substring(idx, end);
    var size = parseInt(sizeStr, 16);
    if (isNaN(size) || size === 0) break;
    idx = end + 2;
    out += body.substring(idx, idx + size);
    idx += size + 2;
  }
  return out;
}

function initBrowser() { return Promise.resolve(); }
function closeBrowser() { return Promise.resolve(); }

module.exports = { scrapePost: scrapePost, initBrowser: initBrowser, closeBrowser: closeBrowser };
