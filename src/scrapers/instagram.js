var https = require('https');
var http = require('http');
var net = require('net');

var PROXY_HOST = process.env.PROXY_HOST || '5.161.16.191';
var PROXY_PORT = parseInt(process.env.PROXY_PORT || '19751');
var PROXY_USER = process.env.PROXY_USER || '';
var PROXY_PASS = process.env.PROXY_PASS || '';

async function scrapePost(url) {
  var result = { views: 0, likes: 0, comments: 0, shares: 0 };
  var postId = extractId(url);
  if (!postId) { result.error = 'Invalid URL'; return result; }

  try {
    // Try embed page through proxy
    var embedUrl = 'https://www.instagram.com/p/' + postId + '/embed/';
    var embedHtml = '';
    try {
      embedHtml = await fetchViaProxy(embedUrl);
      console.log('Embed via proxy length: ' + embedHtml.length);
    } catch(e) {
      console.log('Embed proxy failed, trying direct: ' + e.message);
      embedHtml = await fetchDirect(embedUrl);
      console.log('Embed direct length: ' + embedHtml.length);
    }

    // Extract from embed
    var likesPatterns = [/"like_count"\s*:\s*(\d+)/, /"edge_media_preview_like"\s*:\s*\{\s*"count"\s*:\s*(\d+)/, /(\d[\d,.]*)\s*likes?/i];
    for (var i = 0; i < likesPatterns.length; i++) {
      var m = embedHtml.match(likesPatterns[i]);
      if (m && result.likes === 0) { result.likes = parseNum(m[1]); break; }
    }

    var commentsPatterns = [/"comment_count"\s*:\s*(\d+)/, /"edge_media_to_comment"\s*:\s*\{\s*"count"\s*:\s*(\d+)/, /"edge_media_preview_comment"\s*:\s*\{\s*"count"\s*:\s*(\d+)/];
    for (var j = 0; j < commentsPatterns.length; j++) {
      var mc = embedHtml.match(commentsPatterns[j]);
      if (mc && result.comments === 0) { result.comments = parseNum(mc[1]); break; }
    }

    var viewsPatterns = [/"video_view_count"\s*:\s*(\d+)/, /"play_count"\s*:\s*(\d+)/, /"view_count"\s*:\s*(\d+)/];
    for (var k = 0; k < viewsPatterns.length; k++) {
      var mv = embedHtml.match(viewsPatterns[k]);
      if (mv && result.views === 0) { result.views = parseNum(mv[1]); break; }
    }

    // Try main page through proxy for more data
    if (result.likes === 0 && result.views === 0) {
      var mainHtml = '';
      try {
        mainHtml = await fetchViaProxy(url);
        console.log('Main via proxy length: ' + mainHtml.length);
      } catch(e2) {
        mainHtml = await fetchDirect(url);
        console.log('Main direct length: ' + mainHtml.length);
      }

      var ogMatch = mainHtml.match(/content="([^"]*?\d+[^"]*?likes?[^"]*)"/i);
      if (ogMatch) {
        console.log('OG: ' + ogMatch[1]);
        var lm = ogMatch[1].match(/([\d,.]+[KMkm]?)\s*likes?/i);
        var cm = ogMatch[1].match(/([\d,.]+[KMkm]?)\s*comments?/i);
        if (lm && result.likes === 0) result.likes = parseNum(lm[1]);
        if (cm && result.comments === 0) result.comments = parseNum(cm[1]);
      }

      var jl = mainHtml.match(/"like_count"\s*:\s*(\d+)/);
      if (jl && result.likes === 0) result.likes = parseInt(jl[1], 10);
      var jv = mainHtml.match(/"play_count"\s*:\s*(\d+)/);
      if (jv && result.views === 0) result.views = parseInt(jv[1], 10);
      var jc = mainHtml.match(/"comment_count"\s*:\s*(\d+)/);
      if (jc && result.comments === 0) result.comments = parseInt(jc[1], 10);

      // Try graphql data
      var gql = mainHtml.match(/"edge_media_preview_like"\s*:\s*\{"count"\s*:\s*(\d+)/);
      if (gql && result.likes === 0) result.likes = parseInt(gql[1], 10);
      var gqlv = mainHtml.match(/"video_view_count"\s*:\s*(\d+)/);
      if (gqlv && result.views === 0) result.views = parseInt(gqlv[1], 10);
    }

    // Try reel page if it is a reel
    if (result.views === 0 && url.includes('/reel/')) {
      try {
        var reelUrl = 'https://www.instagram.com/reel/' + postId + '/';
        var reelHtml = await fetchViaProxy(reelUrl);
        console.log('Reel via proxy length: ' + reelHtml.length);
        var rv = reelHtml.match(/"play_count"\s*:\s*(\d+)/);
        if (rv) result.views = parseInt(rv[1], 10);
        var rl = reelHtml.match(/"like_count"\s*:\s*(\d+)/);
        if (rl && result.likes === 0) result.likes = parseInt(rl[1], 10);
        var rc = reelHtml.match(/"comment_count"\s*:\s*(\d+)/);
        if (rc && result.comments === 0) result.comments = parseInt(rc[1], 10);
      } catch(e3) {
        console.log('Reel fetch failed: ' + e3.message);
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

function extractId(url) {
  var m = url.match(/instagram\.com\/(?:p|reel|tv)\/([A-Za-z0-9_-]+)/);
  return m ? m[1] : null;
}

// SOCKS5 proxy fetch
function fetchViaProxy(targetUrl) {
  return new Promise(function(resolve, reject) {
    var timeout = setTimeout(function() { reject(new Error('Proxy timeout')); }, 30000);

    var parsed = new URL(targetUrl);
    var socket = new net.Socket();

    socket.connect(PROXY_PORT, PROXY_HOST, function() {
      // SOCKS5 greeting with auth
      var hasAuth = PROXY_USER && PROXY_PASS;
      if (hasAuth) {
        socket.write(Buffer.from([0x05, 0x02, 0x00, 0x02]));
      } else {
        socket.write(Buffer.from([0x05, 0x01, 0x00]));
      }
    });

    var step = 0;
    var data = Buffer.alloc(0);

    socket.on('data', function(chunk) {
      if (step === 0) {
        // Greeting response
        if (chunk[0] !== 0x05) { clearTimeout(timeout); socket.destroy(); return reject(new Error('Not SOCKS5')); }

        if (chunk[1] === 0x02) {
          // Need auth
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
          // No auth needed, send connect
          sendConnect(socket, parsed.hostname, parseInt(parsed.port || '443'));
          step = 2;
        } else {
          clearTimeout(timeout); socket.destroy(); reject(new Error('Auth method rejected'));
        }
      } else if (step === 1) {
        // Auth response
        if (chunk[1] !== 0x00) { clearTimeout(timeout); socket.destroy(); return reject(new Error('Auth failed')); }
        sendConnect(socket, parsed.hostname, parseInt(parsed.port || '443'));
        step = 2;
      } else if (step === 2) {
        // Connect response
        if (chunk[1] !== 0x00) { clearTimeout(timeout); socket.destroy(); return reject(new Error('Connect failed: ' + chunk[1])); }

        // TLS handshake
        var tls = require('tls');
        var tlsSocket = tls.connect({ socket: socket, servername: parsed.hostname }, function() {
          var req = 'GET ' + parsed.pathname + (parsed.search || '') + ' HTTP/1.1\r\n' +
            'Host: ' + parsed.hostname + '\r\n' +
            'User-Agent: Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36\r\n' +
            'Accept: text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8\r\n' +
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
  buf[0] = 0x05; // SOCKS5
  buf[1] = 0x01; // CONNECT
  buf[2] = 0x00; // Reserved
  buf[3] = 0x03; // Domain name
  buf[4] = hostBuf.length;
  hostBuf.copy(buf, 5);
  buf.writeUInt16BE(port, 5 + hostBuf.length);
  socket.write(buf);
}

function fetchDirect(url) {
  return new Promise(function(resolve, reject) {
    var options = {
      headers: {
        'User-Agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36',
        'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8',
        'Accept-Language': 'en-US,en;q=0.9',
        'Accept-Encoding': 'identity',
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
