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
  var result = { views: 0, likes: 0, comments: 0, shares: 0, username: null, postedAt: null };
  var postCode = extractId(url);
  if (!postCode) { result.error = 'Invalid URL'; return result; }

  // Username: first try URL-based, fallback to meta during parse
  result.username = extractUsername(url);

  var ua = getRandomUA();
  var normalizedUrl = url.split('?')[0].replace('threads.com', 'threads.net');

  // === Cascade de strategies ===
  // Threads peut bloquer via threads.net selon la geo IP du proxy. On tente
  // 3 strategies dans l'ordre, en gardant le HTML le plus consistant.
  var urlNet = url.split('?')[0].replace('threads.com', 'threads.net');
  var urlCom = url.split('?')[0].replace('threads.net', 'threads.com');

  var attempts = [
    { label: 'proxy threads.com', url: urlCom, fn: fetchViaProxy },
    { label: 'proxy threads.net', url: urlNet, fn: fetchViaProxy },
    { label: 'direct threads.com', url: urlCom, fn: fetchDirect },
    { label: 'direct threads.net', url: urlNet, fn: fetchDirect },
  ];

  var html = '';
  for (var ai = 0; ai < attempts.length; ai++) {
    var att = attempts[ai];
    try {
      var got = await att.fn(att.url, ua);
      var gotLen = (got || '').length;
      console.log('[Threads] ' + att.label + ' length: ' + gotLen);
      if (gotLen > 1000) {
        html = got;
        console.log('[Threads] Using ' + att.label + ' (length=' + gotLen + ')');
        break;
      }
    } catch (e) {
      console.log('[Threads] ' + att.label + ' failed: ' + e.message);
    }
  }

  if (!html || html.length < 1000) {
    result.error = 'All strategies returned empty/short HTML';
    console.log('[Threads DEBUG] All 4 strategies failed. Likely the proxy blocks Meta Threads or Threads requires login.');
    return result;
  }

  // === DIAGNOSTIC LOGGING (temporaire) ===
  // Compte les occurrences de chaque champ stat dans le HTML pour comprendre
  // ce que Threads renvoie aux visiteurs anonymes via notre proxy.
  console.log('[Threads DIAG] === Diagnostic du HTML recu ===');
  console.log('[Threads DIAG] like_count occurrences: ' + ((html.match(/"like_count"\s*:\s*\d+/g) || []).length));
  console.log('[Threads DIAG] view_count occurrences: ' + ((html.match(/"view_count"\s*:\s*\d+/g) || []).length));
  console.log('[Threads DIAG] direct_reply_count occurrences: ' + ((html.match(/"direct_reply_count"\s*:\s*\d+/g) || []).length));
  console.log('[Threads DIAG] taken_at occurrences: ' + ((html.match(/"taken_at"\s*:\s*\d+/g) || []).length));
  console.log('[Threads DIAG] __NEXT_DATA__ present: ' + (/<script[^>]*id="__NEXT_DATA__"/.test(html) ? 'YES' : 'NO'));
  console.log('[Threads DIAG] application/json scripts: ' + ((html.match(/<script[^>]*type="application\/json"/g) || []).length));
  console.log('[Threads DIAG] login wall detected: ' + (/Sign up to see|Log in to|loginButton/i.test(html) ? 'YES' : 'NO'));
  // Print first found values if any
  var firstLike = html.match(/"like_count"\s*:\s*(\d+)/);
  var firstView = html.match(/"view_count"\s*:\s*(\d+)/);
  var firstReply = html.match(/"direct_reply_count"\s*:\s*(\d+)/);
  console.log('[Threads DIAG] First like_count value: ' + (firstLike ? firstLike[1] : '(not found)'));
  console.log('[Threads DIAG] First view_count value: ' + (firstView ? firstView[1] : '(not found)'));
  console.log('[Threads DIAG] First direct_reply value: ' + (firstReply ? firstReply[1] : '(not found)'));
  // OG description (often contains stats summary)
  var ogDesc = html.match(/<meta[^>]+property="og:description"[^>]+content="([^"]+)"/);
  if (ogDesc) console.log('[Threads DIAG] OG description: ' + ogDesc[1].substring(0, 200));
  console.log('[Threads DIAG] === Fin diagnostic ===');

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

  // taken_at: Unix timestamp from embedded JSON
  var tsM = html.match(/"taken_at"\s*:\s*(\d+)/);
  if (tsM) {
    var tsVal = parseInt(tsM[1], 10);
    if (tsVal > 1000000000) result.postedAt = new Date(tsVal * 1000).toISOString();
  }
  // Fallback: <time datetime="...">
  if (!result.postedAt) {
    var timeM = html.match(/<time[^>]*datetime\s*=\s*["']([^"']+)["']/i);
    if (timeM) {
      var d = new Date(timeM[1]);
      if (!isNaN(d.getTime())) result.postedAt = d.toISOString();
    }
  }

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

function fetchViaProxy(targetUrl, ua) {
  return new Promise(function(resolve, reject) {
    if (!PROXY_HOST) return reject(new Error('no proxy configured'));

    var timeout = setTimeout(function() { reject(new Error('Proxy timeout')); }, 30000);

    var parsed = new URL(targetUrl);
    var socket = new net.Socket();

    socket.connect(PROXY_PORT, PROXY_HOST, function() {
      var hasAuth = PROXY_USER && PROXY_PASS;
      if (hasAuth) {
        socket.write(Buffer.from([0x05, 0x02, 0x00, 0x02])); // version 5, 2 methods: no-auth + user/pass
      } else {
        socket.write(Buffer.from([0x05, 0x01, 0x00])); // version 5, 1 method: no-auth
      }
    });

    var step = 0;

    socket.on('data', function(chunk) {
      if (step === 0) {
        // SOCKS5 method selection response
        if (chunk[0] !== 0x05) { clearTimeout(timeout); socket.destroy(); return reject(new Error('Not SOCKS5')); }
        if (chunk[1] === 0x02) {
          // Server wants user/pass auth
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
          // No auth required, go to CONNECT
          sendSocksConnect(socket, parsed.hostname, parseInt(parsed.port || '443'));
          step = 2;
        } else {
          clearTimeout(timeout); socket.destroy(); reject(new Error('Auth method rejected'));
        }
      } else if (step === 1) {
        // Auth response
        if (chunk[1] !== 0x00) { clearTimeout(timeout); socket.destroy(); return reject(new Error('Auth failed')); }
        sendSocksConnect(socket, parsed.hostname, parseInt(parsed.port || '443'));
        step = 2;
      } else if (step === 2) {
        // CONNECT response
        if (chunk[1] !== 0x00) { clearTimeout(timeout); socket.destroy(); return reject(new Error('Connect failed: ' + chunk[1])); }

        // Upgrade to TLS through the SOCKS5 tunnel
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
          var headers = responseData.substring(0, bodyStart);
          var body = responseData.slice(bodyStart + 4);
          // Handle chunked transfer encoding
          if (/Transfer-Encoding:\s*chunked/i.test(headers)) {
            body = decodeChunked(body);
          }
          resolve(body);
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

function sendSocksConnect(socket, host, port) {
  var hostBuf = Buffer.from(host);
  var buf = Buffer.alloc(7 + hostBuf.length);
  buf[0] = 0x05; // version
  buf[1] = 0x01; // CONNECT command
  buf[2] = 0x00; // reserved
  buf[3] = 0x03; // address type: domain
  buf[4] = hostBuf.length;
  hostBuf.copy(buf, 5);
  buf.writeUInt16BE(port, 5 + hostBuf.length);
  socket.write(buf);
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
