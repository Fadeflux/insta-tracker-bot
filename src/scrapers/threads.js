// Threads scraper using Puppeteer (headless Chrome).
//
// Why Puppeteer:
//   - Threads exposes view counts only to logged-in users
//   - Cookie-only HTTP requests are blocked (Meta requires JS-generated headers)
//   - A real browser session with cookies works because Chrome runs the JS that
//     produces the right headers automatically
//
// How it works:
//   1. On boot, we launch one shared Chromium instance (kept alive)
//   2. We load cookies from env (THREADS_ACCOUNT_1_*) into the browser context
//   3. For each scrape, we open a new page, navigate to the post, intercept the
//      GraphQL/feed XHR responses (which contain stats), parse them, close the page
//   4. If multiple accounts are configured, we rotate between them
//
// RAM budget on Railway Hobby (512 MB): kept around 250-330 MB by:
//   - Using a single browser instance (not one per scrape)
//   - Disabling images/fonts/css to save memory
//   - Closing pages after each scrape (only the browser stays warm)

var threadsAccounts = require('./threadsAccounts');

var puppeteer;
try { puppeteer = require('puppeteer-core'); } catch (e) {
  console.warn('[Threads] puppeteer-core not available — Threads scraping will return 0s');
}

var browser = null;
var browserLaunching = null;

function buildPostUrl(postCode, username) {
  if (username) return 'https://www.threads.com/@' + username + '/post/' + postCode;
  return 'https://www.threads.com/t/' + postCode;
}

function extractId(url) {
  if (!url) return null;
  var m = url.match(/threads\.(?:net|com)\/(?:@[^\/]+\/post|t)\/([A-Za-z0-9_-]+)/);
  return m ? m[1] : null;
}

function extractUsername(url) {
  if (!url) return null;
  var m = url.match(/threads\.(?:net|com)\/@([A-Za-z0-9_.]+)/);
  return m ? m[1] : null;
}

async function getBrowser() {
  if (browser) return browser;
  if (browserLaunching) return browserLaunching;
  if (!puppeteer) throw new Error('puppeteer-core not installed');

  browserLaunching = (async function() {
    var executablePath = process.env.PUPPETEER_EXECUTABLE_PATH || '/usr/bin/chromium';
    console.log('[Threads/Puppeteer] Launching Chromium at: ' + executablePath);

    var launchArgs = [
      '--no-sandbox',
      '--disable-setuid-sandbox',
      '--disable-dev-shm-usage',
      '--disable-accelerated-2d-canvas',
      '--no-first-run',
      '--no-zygote',
      '--single-process',
      '--disable-gpu',
      '--disable-blink-features=AutomationControlled',
      '--disable-features=IsolateOrigins,site-per-process',
    ];

    var proxyHost = process.env.PROXY_HOST;
    var proxyPort = process.env.PROXY_PORT;
    var proxyUser = process.env.PROXY_USER;
    if (proxyHost && proxyPort && !proxyUser) {
      launchArgs.push('--proxy-server=socks5://' + proxyHost + ':' + proxyPort);
      console.log('[Threads/Puppeteer] Using SOCKS5 proxy ' + proxyHost + ':' + proxyPort);
    } else if (proxyHost && proxyUser) {
      console.log('[Threads/Puppeteer] Proxy with auth detected — skipping (Chrome CLI does not support SOCKS5+auth). Threads will see Railway IP.');
    }

    var b = await puppeteer.launch({
      executablePath: executablePath,
      headless: 'new',
      args: launchArgs,
      defaultViewport: { width: 1280, height: 800 },
    });

    b.on('disconnected', function() {
      console.log('[Threads/Puppeteer] Browser disconnected, will relaunch on next scrape');
      browser = null;
    });

    return b;
  })();

  try {
    browser = await browserLaunching;
    return browser;
  } finally {
    browserLaunching = null;
  }
}

async function applyCookies(page, account) {
  var cookies = [
    { name: 'sessionid', value: account.sessionid, domain: '.threads.com', path: '/', httpOnly: true, secure: true, sameSite: 'Lax' },
    { name: 'sessionid', value: account.sessionid, domain: '.threads.net', path: '/', httpOnly: true, secure: true, sameSite: 'Lax' },
    { name: 'csrftoken', value: account.csrftoken, domain: '.threads.com', path: '/', secure: true, sameSite: 'Lax' },
    { name: 'csrftoken', value: account.csrftoken, domain: '.threads.net', path: '/', secure: true, sameSite: 'Lax' },
    { name: 'ds_user_id', value: account.userid, domain: '.threads.com', path: '/', secure: true, sameSite: 'Lax' },
    { name: 'ds_user_id', value: account.userid, domain: '.threads.net', path: '/', secure: true, sameSite: 'Lax' },
  ];
  await page.setCookie.apply(page, cookies);
}

async function scrapePost(url) {
  var result = { views: 0, likes: 0, comments: 0, shares: 0, username: null, postedAt: null };
  var postCode = extractId(url);
  if (!postCode) { result.error = 'Invalid URL'; return result; }
  result.username = extractUsername(url);

  if (threadsAccounts.getActiveCount() === 0) {
    threadsAccounts.loadAccounts();
    if (threadsAccounts.getActiveCount() === 0) {
      console.log('[Threads] No accounts configured (THREADS_ACCOUNT_1_* env vars missing) — returning zeros');
      result.error = 'No Threads accounts configured';
      return result;
    }
  }

  var account = threadsAccounts.getNextAccount();
  if (!account) {
    result.error = 'All Threads accounts banned';
    console.log('[Threads] All accounts marked as banned/rate_limited');
    return result;
  }

  var page = null;
  try {
    var b = await getBrowser();
    page = await b.newPage();

    await page.setRequestInterception(true);
    page.on('request', function(req) {
      var type = req.resourceType();
      if (type === 'image' || type === 'media' || type === 'font' || type === 'stylesheet') {
        req.abort();
      } else {
        req.continue();
      }
    });

    var capturedStats = null;
    page.on('response', async function(response) {
      try {
        var url = response.url();
        if (!/\/(graphql|api\/v1|threads_post|barcelona)/.test(url)) return;
        var ct = response.headers()['content-type'] || '';
        if (!/json/i.test(ct)) return;

        var bodyText;
        try { bodyText = await response.text(); } catch (e) { return; }
        if (!bodyText || bodyText.length < 50) return;

        var stats = tryExtractStats(bodyText, postCode);
        if (stats) capturedStats = stats;
      } catch (e) { /* ignore */ }
    });

    await applyCookies(page, account);

    var targetUrl = buildPostUrl(postCode, result.username);
    console.log('[Threads/Puppeteer] Account #' + account.index + ' navigating to ' + targetUrl);

    await page.goto(targetUrl, { waitUntil: 'networkidle2', timeout: 30000 });
    await new Promise(function(r) { setTimeout(r, 1500); });

    if (capturedStats) {
      threadsAccounts.markAccountSuccess(account);
      result.views = capturedStats.views || 0;
      result.likes = capturedStats.likes || 0;
      result.comments = capturedStats.comments || 0;
      result.shares = capturedStats.shares || 0;
      if (capturedStats.username) result.username = capturedStats.username;
      if (capturedStats.postedAt) result.postedAt = capturedStats.postedAt;
      console.log('[Threads/Puppeteer] OK ' + postCode + ' — views=' + result.views + ' likes=' + result.likes + ' comments=' + result.comments + ' (account #' + account.index + ')');
      return result;
    }

    // DOM fallback
    var domStats = await page.evaluate(function() {
      function parseHumanCount(s) {
        if (!s) return 0;
        s = String(s).trim().replace(/\s+/g, '');
        var m = s.match(/^([\d.,]+)\s*([KMB]?)/i);
        if (!m) return 0;
        var num = parseFloat(m[1].replace(',', '.'));
        var suf = (m[2] || '').toUpperCase();
        if (suf === 'K') num *= 1000;
        if (suf === 'M') num *= 1000000;
        if (suf === 'B') num *= 1000000000;
        return Math.round(num);
      }
      var stats = { views: 0, likes: 0, comments: 0 };
      var all = document.querySelectorAll('span, div, header');
      for (var i = 0; i < all.length; i++) {
        var t = (all[i].innerText || '').trim();
        if (!t) continue;
        if (/\d.*vues?$/i.test(t) || /\d.*views?$/i.test(t)) {
          var v = parseHumanCount(t);
          if (v > stats.views) stats.views = v;
        }
      }
      var likeBtn = document.querySelector('[aria-label*="like" i], [aria-label*="J\'aime" i]');
      if (likeBtn) {
        var lblM = (likeBtn.getAttribute('aria-label') || '').match(/([\d.,]+\s*[KMB]?)/i);
        if (lblM) stats.likes = parseHumanCount(lblM[1]);
      }
      return stats;
    });

    if (domStats && (domStats.views > 0 || domStats.likes > 0)) {
      threadsAccounts.markAccountSuccess(account);
      result.views = domStats.views;
      result.likes = domStats.likes;
      result.comments = domStats.comments;
      console.log('[Threads/Puppeteer] DOM-fallback ' + postCode + ' — views=' + result.views + ' likes=' + result.likes + ' (account #' + account.index + ')');
      return result;
    }

    var loggedOut = await page.evaluate(function() {
      return /Log in|Connexion|Iniciar sesi/.test(document.body.innerText) && !/profile|@/.test(document.title);
    });
    if (loggedOut) {
      threadsAccounts.markAccountError(account, 'auth_failed', 'Page shows login wall');
      result.error = 'Account session invalid (login wall)';
      console.log('[Threads/Puppeteer] Account #' + account.index + ' invalid session — login wall on ' + postCode);
      return result;
    }

    threadsAccounts.markAccountError(account, 'unknown', 'No stats found in page');
    console.log('[Threads/Puppeteer] No stats parsed for ' + postCode + ' — page loaded but counters absent');
    return result;
  } catch (e) {
    threadsAccounts.markAccountError(account, 'unknown', e.message);
    console.log('[Threads/Puppeteer] Exception scraping ' + postCode + ': ' + e.message);
    result.error = 'Puppeteer exception: ' + e.message;
    return result;
  } finally {
    if (page) { try { await page.close(); } catch (e) {} }
  }
}

function tryExtractStats(jsonText, postCode) {
  try {
    var data = JSON.parse(jsonText);
    var post = findPostByCode(data, postCode);
    if (!post) return null;
    var likes = post.like_count != null ? post.like_count : 0;
    var views = post.view_count != null ? post.view_count : 0;
    var replies = (post.text_post_app_info && post.text_post_app_info.direct_reply_count) || post.direct_reply_count || 0;
    var reposts = (post.text_post_app_info && post.text_post_app_info.repost_count) || post.repost_count || 0;
    var quotes = (post.text_post_app_info && post.text_post_app_info.quote_count) || post.quote_count || 0;
    var username = (post.user && post.user.username) || null;
    var postedAt = post.taken_at ? new Date(Number(post.taken_at) * 1000).toISOString() : null;
    return {
      views: views,
      likes: likes,
      comments: replies,
      shares: reposts + quotes,
      username: username ? String(username).toLowerCase() : null,
      postedAt: postedAt,
    };
  } catch (e) { return null; }
}

function findPostByCode(obj, code, depth) {
  depth = depth || 0;
  if (depth > 12 || !obj || typeof obj !== 'object') return null;
  if (obj.code === code && (obj.like_count != null || obj.text_post_app_info || obj.user)) return obj;
  if (Array.isArray(obj)) {
    for (var i = 0; i < obj.length; i++) {
      var f = findPostByCode(obj[i], code, depth + 1);
      if (f) return f;
    }
    return null;
  }
  var keys = Object.keys(obj);
  for (var k = 0; k < keys.length; k++) {
    var f2 = findPostByCode(obj[keys[k]], code, depth + 1);
    if (f2) return f2;
  }
  return null;
}

async function initBrowser() {
  threadsAccounts.loadAccounts();
  if (threadsAccounts.getActiveCount() === 0) {
    console.log('[Threads/Puppeteer] No accounts configured, browser will not be pre-launched');
    return;
  }
  try {
    await getBrowser();
    console.log('[Threads/Puppeteer] Browser ready');
  } catch (e) {
    console.log('[Threads/Puppeteer] Initial browser launch failed: ' + e.message);
  }
}

async function closeBrowser() {
  if (browser) {
    try { await browser.close(); } catch (e) {}
    browser = null;
  }
}

module.exports = { scrapePost: scrapePost, initBrowser: initBrowser, closeBrowser: closeBrowser };
