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

  // === STRATEGY 0: Authenticated mobile API call (preferred when accounts are configured) ===
  // Uses Meta's internal mobile API endpoint (i.instagram.com/api/v1/...).
  // This requires a logged-in Threads account (cookies in env vars).
  // If no account is configured or if the API fails, we fall back to anonymous HTML scraping.
  var threadsAccounts = require('./threadsAccounts');
  var account = threadsAccounts.getNextAccount();
  if (account) {
    console.log('[Threads] Using account #' + account.index + ' for ' + postCode);
    try {
      var apiResult = await fetchViaMobileAPI(postCode, account, ua);
      if (apiResult && apiResult.success) {
        threadsAccounts.markAccountSuccess(account);
        // Merge into result and return early — no need for HTML fallback
        if (apiResult.username) result.username = apiResult.username;
        result.views = apiResult.views || 0;
        result.likes = apiResult.likes || 0;
        result.comments = apiResult.comments || 0;
        result.shares = apiResult.shares || 0;
        result.postedAt = apiResult.postedAt || null;
        console.log('[Threads] API OK for ' + postCode + ' — views=' + result.views + ' likes=' + result.likes + ' comments=' + result.comments + ' (via account #' + account.index + ')');
        return result;
      } else if (apiResult) {
        threadsAccounts.markAccountError(account, apiResult.errorType || 'unknown', apiResult.errorMessage || '?');
        console.log('[Threads] API failed for ' + postCode + ': ' + (apiResult.errorMessage || 'unknown'));
      }
    } catch (e) {
      threadsAccounts.markAccountError(account, 'unknown', e.message);
      console.log('[Threads] API exception for ' + postCode + ': ' + e.message);
    }
    // Fall through to HTML scraping below (which will likely also fail without auth, but worth trying)
  } else {
    console.log('[Threads] No accounts configured, using anonymous HTML scraping (likely to return 0)');
  }

  // === Cascade de strategies HTML (fallback anonyme) ===
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
    console.log('[Threads DEBUG] All HTML strategies failed and no API account was usable.');
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

  // === DEEP DIAGNOSTIC: dump JSON scripts to find the real field names ===
  console.log('[Threads DEEP] === Recherche des compteurs avec patterns alternatifs ===');
  // Chercher tous les patterns potentiels Meta utilise
  var altPatterns = [
    'likeCount', 'viewCount', 'replyCount', 'commentCount',
    'reposts', 'shareCount', 'feedback_count',
    'play_count', 'video_view_count', 'media_overlay_info',
    'stat_text', 'engagement', 'metrics',
  ];
  altPatterns.forEach(function(name) {
    var re = new RegExp('"' + name + '"\\s*:\\s*([^,\\}]+)', 'g');
    var matches = html.match(re);
    if (matches && matches.length > 0) {
      console.log('[Threads DEEP] Pattern "' + name + '": ' + matches.length + ' matches | first 3: ' + matches.slice(0, 3).join(' || '));
    }
  });

  // Dump les 2 premiers scripts JSON (tronqués) pour voir leur structure
  var jsonScripts = html.match(/<script[^>]*type="application\/json"[^>]*>([\s\S]*?)<\/script>/g);
  if (jsonScripts && jsonScripts.length > 0) {
    for (var si = 0; si < Math.min(2, jsonScripts.length); si++) {
      var content = jsonScripts[si].replace(/<script[^>]*>/, '').replace(/<\/script>/, '');
      // Cherche le passage qui contient des chiffres genre stats
      var stats = content.match(/[^,\{]*?(?:like|view|reply|share|repost|count|stat|engagement)[^,\{]{0,50}/gi);
      if (stats) {
        console.log('[Threads DEEP] Script ' + si + ' stats fragments (first 5): ' + stats.slice(0, 5).join(' | '));
      }
      // Premiers 800 char du script
      console.log('[Threads DEEP] Script ' + si + ' preview (800ch): ' + content.substring(0, 800));
    }
  }

  // Cherche les nombres genre "24.5K" dans le texte
  var humanCounts = html.match(/[\d.,]+\s*[KMB]?\s*(?:vues|views|likes|j.aime)/gi);
  if (humanCounts) {
    console.log('[Threads DEEP] Human-readable counts: ' + humanCounts.slice(0, 5).join(' | '));
  }
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

// === Mobile API fetch (authenticated) ===
// Uses Meta's internal i.instagram.com endpoints with a logged-in Threads session.
// We try a couple of endpoint variants because Meta has historically used different
// ones over time. If one returns 200 with valid JSON, we use it. Otherwise we try the next.
//
// Headers required:
//   Cookie: sessionid=...; csrftoken=...; ds_user_id=...
//   X-CSRFToken: <csrftoken>
//   X-IG-App-ID: 238260118697367   (Threads web app id, public)
//   X-FB-LSD: <random>             (anti-CSRF, but seems optional)
//   X-ASBD-ID: 198387
//   User-Agent: Barcelona <ver> Android (...) — mobile-style; or web Chrome
//
// Returns:
//   { success: true, views, likes, comments, shares, username, postedAt }
//   { success: false, errorType: 'banned'|'auth_failed'|'rate_limited'|'unknown', errorMessage }
async function fetchViaMobileAPI(postCode, account, ua) {
  // Cookie header
  var cookie = 'sessionid=' + account.sessionid +
    '; csrftoken=' + account.csrftoken +
    '; ds_user_id=' + account.userid;

  // Endpoint variants to try
  var endpoints = [
    {
      label: 'web GraphQL by shortcode',
      host: 'www.threads.net',
      path: '/api/graphql',
      method: 'POST',
      body: 'lsd=AVqbxe3J_YA&fb_api_caller_class=RelayModern&fb_api_req_friendly_name=BarcelonaPostPageContentQuery' +
        '&variables=' + encodeURIComponent(JSON.stringify({ postID: postCode, withShallowTree: false, includePromotedPosts: false })) +
        '&doc_id=25460088170359441',
      contentType: 'application/x-www-form-urlencoded',
    },
    {
      label: 'i.instagram.com text_feed',
      host: 'i.instagram.com',
      path: '/api/v1/text_feed/' + postCode + '/info/',
      method: 'GET',
      body: null,
      contentType: null,
    },
  ];

  for (var ei = 0; ei < endpoints.length; ei++) {
    var ep = endpoints[ei];
    try {
      console.log('[Threads API] Trying ' + ep.label + ' for ' + postCode);
      var resp = await sendApiRequest(ep, cookie, account.csrftoken, ua);
      console.log('[Threads API] ' + ep.label + ' status=' + resp.status + ' bodyLen=' + (resp.body || '').length);
      if (resp.status === 401 || resp.status === 403) {
        return { success: false, errorType: 'auth_failed', errorMessage: 'HTTP ' + resp.status + ' on ' + ep.label };
      }
      if (resp.status === 429) {
        return { success: false, errorType: 'rate_limited', errorMessage: 'HTTP 429 on ' + ep.label };
      }
      if (resp.status >= 200 && resp.status < 300 && resp.body && resp.body.length > 100) {
        var parsed = parseApiResponse(resp.body, postCode);
        if (parsed) {
          return { success: true, ...parsed };
        }
        // Body got but couldn't parse — dump details to identify what Meta returned
        var preview = resp.body.substring(0, 1500);
        console.log('[Threads API DUMP] Response from ' + ep.label + ' (status=' + resp.status + '):');
        console.log('[Threads API DUMP] Content-Type detection: HTML=' + /^<!DOCTYPE|<html/i.test(preview) + ' JSON=' + /^[\[\{]/.test(preview.trim()));
        console.log('[Threads API DUMP] First 1500 chars: ' + preview);
        // Look for tell-tale signs
        if (/login|Connexion|Iniciar sesi/i.test(preview)) console.log('[Threads API DUMP] -> LOGIN PAGE detected (cookies invalid?)');
        if (/captcha|Verify|Confirm/i.test(preview)) console.log('[Threads API DUMP] -> CAPTCHA / VERIFICATION detected');
        if (/restricted|suspended|disabled/i.test(preview)) console.log('[Threads API DUMP] -> ACCOUNT RESTRICTED detected');
        if (/checkpoint/i.test(preview)) console.log('[Threads API DUMP] -> CHECKPOINT detected (Meta wants 2FA verification)');
      }
    } catch (e) {
      console.log('[Threads API] ' + ep.label + ' threw: ' + e.message);
    }
  }
  return { success: false, errorType: 'unknown', errorMessage: 'All API endpoints failed' };
}

// Make a single HTTPS request via SOCKS5 proxy with auth headers.
function sendApiRequest(endpoint, cookieHeader, csrfToken, ua) {
  return new Promise(function(resolve, reject) {
    if (!PROXY_HOST) return reject(new Error('No proxy configured'));
    var timeout = setTimeout(function() { reject(new Error('API request timeout')); }, 30000);
    var socket = new net.Socket();

    socket.connect(PROXY_PORT, PROXY_HOST, function() {
      var hasAuth = PROXY_USER && PROXY_PASS;
      if (hasAuth) socket.write(Buffer.from([0x05, 0x02, 0x00, 0x02]));
      else socket.write(Buffer.from([0x05, 0x01, 0x00]));
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
          sendSocksConnect(socket, endpoint.host, 443);
          step = 2;
        } else {
          clearTimeout(timeout); socket.destroy(); reject(new Error('Auth method rejected'));
        }
      } else if (step === 1) {
        if (chunk[1] !== 0x00) { clearTimeout(timeout); socket.destroy(); return reject(new Error('Auth failed')); }
        sendSocksConnect(socket, endpoint.host, 443);
        step = 2;
      } else if (step === 2) {
        if (chunk[1] !== 0x00) { clearTimeout(timeout); socket.destroy(); return reject(new Error('Connect failed: ' + chunk[1])); }
        var tls = require('tls');
        var tlsSocket = tls.connect({ socket: socket, servername: endpoint.host }, function() {
          var headers = endpoint.method + ' ' + endpoint.path + ' HTTP/1.1\r\n' +
            'Host: ' + endpoint.host + '\r\n' +
            'User-Agent: ' + ua + '\r\n' +
            'Accept: */*\r\n' +
            'Accept-Language: en-US,en;q=0.9\r\n' +
            'Accept-Encoding: identity\r\n' +
            'Cookie: ' + cookieHeader + '\r\n' +
            'X-CSRFToken: ' + csrfToken + '\r\n' +
            'X-IG-App-ID: 238260118697367\r\n' +
            'X-ASBD-ID: 198387\r\n' +
            'X-FB-LSD: AVqbxe3J_YA\r\n' +
            'X-Requested-With: XMLHttpRequest\r\n' +
            'Origin: https://www.threads.net\r\n' +
            'Referer: https://www.threads.net/\r\n';
          if (endpoint.method === 'POST' && endpoint.body) {
            headers += 'Content-Type: ' + endpoint.contentType + '\r\n';
            headers += 'Content-Length: ' + Buffer.byteLength(endpoint.body) + '\r\n';
          }
          headers += 'Connection: close\r\n\r\n';
          if (endpoint.method === 'POST' && endpoint.body) headers += endpoint.body;
          tlsSocket.write(headers);
        });
        var responseData = '';
        tlsSocket.on('data', function(d) { responseData += d.toString(); });
        tlsSocket.on('end', function() {
          clearTimeout(timeout);
          var bodyStart = responseData.indexOf('\r\n\r\n');
          if (bodyStart === -1) return resolve({ status: 0, body: responseData });
          var head = responseData.substring(0, bodyStart);
          var body = responseData.slice(bodyStart + 4);
          var statusM = head.match(/^HTTP\/[\d.]+\s+(\d+)/);
          var status = statusM ? parseInt(statusM[1], 10) : 0;
          if (/Transfer-Encoding:\s*chunked/i.test(head)) body = decodeChunked(body);
          resolve({ status: status, body: body });
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

// Parse the JSON response from any Meta API endpoint and extract counts.
function parseApiResponse(body, postCode) {
  try {
    var data = JSON.parse(body);
    // Search recursively for the post that matches our postCode
    var post = findPostInResponse(data, postCode);
    if (!post) {
      console.log('[Threads API] Post not found in response. Top-level keys: ' + Object.keys(data).join(','));
      return null;
    }
    var likeCount = post.like_count != null ? post.like_count : 0;
    var viewCount = post.view_count != null ? post.view_count : (post.feedback_info && post.feedback_info.view_count) || 0;
    var replyCount = (post.text_post_app_info && post.text_post_app_info.direct_reply_count) || post.direct_reply_count || post.reply_count || 0;
    var repostCount = (post.text_post_app_info && post.text_post_app_info.repost_count) || post.repost_count || 0;
    var quoteCount = (post.text_post_app_info && post.text_post_app_info.quote_count) || post.quote_count || 0;
    var username = (post.user && post.user.username) || (post.owner && post.owner.username) || null;
    var postedAt = post.taken_at ? new Date(Number(post.taken_at) * 1000).toISOString() : null;

    return {
      views: Math.round(viewCount * VIEW_MULTIPLIER),
      likes: likeCount,
      comments: replyCount,
      shares: repostCount + quoteCount,
      username: username ? String(username).toLowerCase() : null,
      postedAt: postedAt,
    };
  } catch (e) {
    console.log('[Threads API] JSON parse error: ' + e.message);
    return null;
  }
}

// Recursive search for the post object matching the given code.
function findPostInResponse(obj, code, depth) {
  depth = depth || 0;
  if (depth > 12 || !obj || typeof obj !== 'object') return null;
  // Check current node
  if (obj.code === code || obj.pk === code || obj.id === code) {
    if (obj.like_count != null || obj.text_post_app_info || obj.user) return obj;
  }
  // Recurse arrays
  if (Array.isArray(obj)) {
    for (var i = 0; i < obj.length; i++) {
      var found = findPostInResponse(obj[i], code, depth + 1);
      if (found) return found;
    }
    return null;
  }
  // Recurse object values
  var keys = Object.keys(obj);
  for (var k = 0; k < keys.length; k++) {
    var found2 = findPostInResponse(obj[keys[k]], code, depth + 1);
    if (found2) return found2;
  }
  return null;
}

function initBrowser() { return Promise.resolve(); }
function closeBrowser() { return Promise.resolve(); }

// Boot: load accounts pool from env
require('./threadsAccounts').loadAccounts();

module.exports = { scrapePost: scrapePost, initBrowser: initBrowser, closeBrowser: closeBrowser };
