var express = require('express');
var path = require('path');
var config = require('../../config');
var db = require('../db/queries');

// DASHBOARD_USERS format: username:password:role:platform[:discord_id]
// Example: admin:admin123:admin:all,aicha:pass1:va:instagram:123456789012345678
var DASHBOARD_USERS = {};

// Users defined in ENV — these can NEVER be overwritten by DB
var ENV_USERNAMES = new Set();

function loadUsers() {
  var raw = process.env.DASHBOARD_USERS || 'admin:admin123:admin:all';
  var pairs = raw.split(',');
  pairs.forEach(function(pair) {
    var parts = pair.trim().split(':');
    if (parts.length >= 2) {
      var username = parts[0];
      ENV_USERNAMES.add(username);
      DASHBOARD_USERS[username] = {
        password: parts[1],
        role: parts[2] || 'va',
        platform: parts[3] || 'all',
        discordId: parts[4] || null,
      };
      console.log('[Users] ENV user loaded: ' + username + ' role=' + (parts[2]||'va') + ' platform=' + (parts[3]||'all') + (parts[4] ? ' discord_id=' + parts[4] : ''));
    }
  });

  // Load DB users (skip any that exist in ENV, skip revoked users)
  db.pool.query('SELECT * FROM dashboard_users').then(function(result) {
    var revokedSkipped = 0;
    result.rows.forEach(function(row) {
      if (ENV_USERNAMES.has(row.username)) {
        console.log('[Users] Skipping DB user (ENV has priority): ' + row.username);
        db.pool.query('DELETE FROM dashboard_users WHERE username = $1', [row.username]).catch(function(){});
      } else if (row.status === 'revoked') {
        revokedSkipped++;
        // Do NOT load revoked users — they cannot log in.
      } else {
        DASHBOARD_USERS[row.username] = {
          password: row.password_hash,
          role: row.role,
          platform: row.platform,
          discordId: row.discord_id || null,
        };
        console.log('[Users] DB user loaded: ' + row.username + ' role=' + row.role + (row.discord_id ? ' discord_id=' + row.discord_id : ''));
      }
    });
    console.log('[Users] Total active: ' + Object.keys(DASHBOARD_USERS).length + (revokedSkipped > 0 ? ' (' + revokedSkipped + ' revoked users skipped)' : ''));
  }).catch(function(e) {
    console.log('[Users] DB error: ' + e.message + ' — using ENV only');
  });
}

function checkAuth(req, res, next) {
  var token = req.headers['x-auth-token'] || req.query.token;
  if (!token) return res.status(401).json({ error: 'No token' });
  try {
    var decoded = Buffer.from(token, 'base64').toString();
    var parts = decoded.split(':');
    if (parts.length >= 2 && DASHBOARD_USERS[parts[0]] && DASHBOARD_USERS[parts[0]].password === parts[1]) {
      req.user = parts[0];
      req.userRole = DASHBOARD_USERS[parts[0]].role;
      req.userPlatform = DASHBOARD_USERS[parts[0]].platform;
      req.userDiscordId = DASHBOARD_USERS[parts[0]].discordId || null;
      return next();
    }
  } catch(e) {}
  return res.status(401).json({ error: 'Invalid token' });
}

// Check if user can access requested platform
function checkPlatformAccess(req, res, next) {
  var requestedPlatform = req.query.platform || req.params.platform;
  if (!requestedPlatform || req.userPlatform === 'all') return next();
  // Handle comma-separated platforms
  var userPlats = req.userPlatform.split(',');
  if (userPlats.indexOf(requestedPlatform) !== -1) return next();
  return res.status(403).json({ error: 'Access denied for platform: ' + requestedPlatform });
}

function calcScore(s) {
  return (Number(s.likes) || 0) + (Number(s.comments) || 0) * 3 + (Number(s.shares) || 0) * 5;
}

function calcEngagement(s) {
  var v = Number(s.views) || 0;
  if (v === 0) return 0;
  return ((Number(s.likes) || 0) + (Number(s.comments) || 0)) / v;
}

function getPerf(views) {
  var VIRAL = parseInt(process.env.VIRAL_VIEWS || '5000');
  var BON = parseInt(process.env.BON_VIEWS || '1000');
  var MOYEN = parseInt(process.env.MOYEN_VIEWS || '300');
  if (views >= VIRAL) return 'viral';
  if (views >= BON) return 'bon';
  if (views >= MOYEN) return 'moyen';
  return 'flop';
}

function calcAdvancedScore(s) {
  var v = Number(s.views) || 0;
  if (v <= 1) return 0;
  var eng = calcEngagement(s);
  return Math.round((eng * 100) * Math.log10(v) * 100) / 100;
}

// Get effective platform for queries (user's platform or requested)
function getEffectivePlatform(req) {
  var requested = req.query.platform;
  if (req.userPlatform === 'all') return requested || null; // null = all platforms

  // If the user has a multi-platform value like "instagram,threads", they can
  // freely switch between those platforms (?platform=instagram). If the
  // requested platform is one they're allowed, honor it. Otherwise fall back
  // to their first allowed platform so we don't expose data they shouldn't see.
  if (req.userPlatform && req.userPlatform.indexOf(',') !== -1) {
    var allowed = req.userPlatform.split(',').map(function(p) { return String(p).toLowerCase().trim(); }).filter(Boolean);
    if (requested && allowed.indexOf(requested) !== -1) return requested;
    return allowed[0]; // default to the first allowed platform
  }

  return req.userPlatform; // single-platform user
}

// Get the list of platforms a user is allowed to see/query.
// Used for supervision endpoints (activity-status, dm-status) so a manager
// only sees their assigned platforms.
function getUserAllowedPlatforms(req) {
  var userPlat = req.userPlatform;
  if (!userPlat || userPlat === 'all') return config.getActivePlatforms();
  if (userPlat.indexOf(',') !== -1) {
    return userPlat.split(',').map(function(p) { return String(p).toLowerCase().trim(); }).filter(Boolean);
  }
  return [String(userPlat).toLowerCase().trim()];
}

function createWebServer() {
  loadUsers();
  var app = express();
  app.use(express.json());

  // Login — returns allowed platforms
  app.post('/api/login', async function(req, res) {
    var username = req.body.username;
    var password = req.body.password;
    console.log('[Login] Attempt: ' + username + ' | stored role: ' + (DASHBOARD_USERS[username] ? DASHBOARD_USERS[username].role : 'NOT FOUND') + ' | stored platform: ' + (DASHBOARD_USERS[username] ? DASHBOARD_USERS[username].platform : 'N/A'));

    if (!DASHBOARD_USERS[username] || DASHBOARD_USERS[username].password !== password) {
      return res.status(401).json({ error: 'Invalid credentials' });
    }

    // ENV users always bypass the DB check (they're your emergency access).
    if (!ENV_USERNAMES.has(username)) {
      try {
        var dbRow = await db.getDashboardUser(username);
        if (dbRow && dbRow.status === 'revoked') {
          // Remove from in-memory map so subsequent attempts short-circuit
          delete DASHBOARD_USERS[username];
          console.log('[Login] Blocked: ' + username + ' is revoked (' + (dbRow.revoked_reason || 'no reason') + ')');
          return res.status(403).json({ error: 'Compte desactive. Contacte un admin.' });
        }
      } catch (e) {
        // DB unreachable — fall through to allow login, prefer availability over strict revocation
        console.log('[Login] DB status check failed for ' + username + ': ' + e.message);
      }
    }

    var user = DASHBOARD_USERS[username];
    var token = Buffer.from(username + ':' + password).toString('base64');
    var allowedPlatforms = [];
    if (user.platform === 'all') {
      allowedPlatforms = config.getActivePlatforms();
    } else if (user.platform.indexOf(',') !== -1) {
      allowedPlatforms = user.platform.split(',');
    } else {
      allowedPlatforms = [user.platform];
    }
    // Normalize: lowercase + trim + dedupe to protect against mixed-case stored values
    allowedPlatforms = allowedPlatforms
      .map(function(p) { return String(p).toLowerCase().trim(); })
      .filter(function(p, i, a) { return p && a.indexOf(p) === i; });
    console.log('[Login] Success: ' + username + ' role=' + user.role + ' platform=' + user.platform);
    return res.json({
      token: token,
      username: username,
      role: user.role,
      platform: user.platform,
      discordId: user.discordId || null,
      allowedPlatforms: allowedPlatforms,
    });
  });

  // User info
  app.get('/api/me', checkAuth, function(req, res) {
    var user = DASHBOARD_USERS[req.user];
    var allowedPlatforms = [];
    if (user.platform === 'all') {
      allowedPlatforms = config.getActivePlatforms();
    } else if (user.platform.indexOf(',') !== -1) {
      allowedPlatforms = user.platform.split(',');
    } else {
      allowedPlatforms = [user.platform];
    }
    allowedPlatforms = allowedPlatforms
      .map(function(p) { return String(p).toLowerCase().trim(); })
      .filter(function(p, i, a) { return p && a.indexOf(p) === i; });
    res.json({
      username: req.user,
      role: user.role,
      platform: user.platform,
      discordId: user.discordId || null,
      allowedPlatforms: allowedPlatforms,
    });
  });

  app.get('/api/today', checkAuth, async function(req, res) {
    try {
      var platform = getEffectivePlatform(req);
      var today = new Date().toISOString().split('T')[0];
      await db.computeDailySummary(today, platform);
      var summaries = await db.getDailySummaries(today, platform);
      var activePosts = await db.getActivePosts(platform);

      summaries = summaries.map(function(s) {
        var tv = Number(s.total_views), tl = Number(s.total_likes), tc = Number(s.total_comments), ts = Number(s.total_shares), pc = Number(s.post_count);
        s.avg_views = pc > 0 ? Math.round(tv / pc) : 0;
        s.total_score = tl + tc * 3 + ts * 5;
        s.avg_score = pc > 0 ? Math.round(s.total_score / pc) : 0;
        s.engagement_rate = tv > 0 ? ((tl + tc) / tv * 100).toFixed(1) : '0.0';
        s.badge = s.avg_views >= 2000 ? 'top' : s.avg_views >= 500 ? 'bon' : 'faible';
        return s;
      });

      res.json({ date: today, platform: platform || 'all', summaries: summaries, activePosts: activePosts.length });
    } catch(err) { res.status(500).json({ error: err.message }); }
  });

  app.get('/api/stats/:date', checkAuth, async function(req, res) {
    try {
      var platform = getEffectivePlatform(req);
      var date = req.params.date;
      await db.computeDailySummary(date, platform);
      var summaries = await db.getDailySummaries(date, platform);
      res.json({ date: date, platform: platform || 'all', summaries: summaries });
    } catch(err) { res.status(500).json({ error: err.message }); }
  });

  app.get('/api/va/:discordId', checkAuth, async function(req, res) {
    try {
      var platform = getEffectivePlatform(req);
      var fromDate = req.query.from;
      var toDate = req.query.to;
      var allTime = req.query.all === '1';
      var date = req.query.date || new Date().toISOString().split('T')[0];

      // Decide which post fetch strategy to use:
      //   - all=1                → all posts ever
      //   - from && to           → all posts in [from, to]
      //   - else (legacy)        → posts on a single date (default = today)
      var posts;
      if (allTime) {
        posts = await db.getVaPostsAll(req.params.discordId, platform);
      } else if (fromDate && toDate) {
        posts = await db.getVaPostsRange(req.params.discordId, fromDate, toDate, platform);
      } else {
        posts = await db.getVaPostsToday(req.params.discordId, date, platform);
      }

      var snapshots = [];
      for (var i = 0; i < posts.length; i++) {
        var history = await db.getSnapshotHistory(posts[i].id);
        var milestones = await db.getPostMilestones(posts[i].id);
        var lastSnap = history.length > 0 ? history[history.length - 1] : { views: 0, likes: 0, comments: 0, shares: 0 };
        var score = calcScore(lastSnap);
        var engagement = calcEngagement(lastSnap);
        var perf = getPerf(Number(lastSnap.views));
        snapshots.push({
          post: posts[i],
          snapshots: history,
          milestones: milestones,
          score: score,
          engagement: engagement,
          performance: perf,
        });
      }

      // Stats: aggregate over the range / all time / single day
      var stats;
      if (allTime) {
        // Aggregate from all daily_summaries
        var sumSql = 'SELECT MAX(va_name) AS va_name, SUM(post_count)::int AS post_count, ' +
          'SUM(total_views)::bigint AS total_views, SUM(total_likes)::bigint AS total_likes, ' +
          'SUM(total_comments)::bigint AS total_comments, SUM(total_shares)::bigint AS total_shares ' +
          'FROM daily_summaries WHERE va_discord_id = $1' + (platform ? ' AND platform = $2' : '');
        var sumParams = platform ? [req.params.discordId, platform] : [req.params.discordId];
        var allStats = await db.pool.query(sumSql, sumParams);
        stats = allStats.rows[0];
      } else if (fromDate && toDate) {
        stats = await db.getVaRangeStats(req.params.discordId, fromDate, toDate, platform);
      } else {
        stats = await db.getVaDailyStats(req.params.discordId, date, platform);
      }

      res.json({
        va_id: req.params.discordId,
        date: date,
        from: fromDate || null,
        to: toDate || null,
        allTime: allTime,
        platform: platform || 'all',
        posts: snapshots,
        stats: stats,
      });
    } catch(err) { res.status(500).json({ error: err.message }); }
  });

  // Returns aggregated totals across all time (or a date range if from/to provided).
  // Used by the Dashboard's "Depuis toujours" view. Same shape as /api/stats/:date
  // for the summaries field but with totals across the whole period.
  app.get('/api/all-time-stats', checkAuth, async function(req, res) {
    try {
      var platform = getEffectivePlatform(req);
      var fromDate = req.query.from || null;
      var toDate = req.query.to || null;
      var summaries = await db.getRangeSummaries(fromDate, toDate, platform);
      res.json({ from: fromDate, to: toDate, platform: platform || 'all', summaries: summaries });
    } catch(err) { res.status(500).json({ error: err.message }); }
  });

  app.get('/api/history/:days', checkAuth, async function(req, res) {
    try {
      var platform = getEffectivePlatform(req);
      var days = parseInt(req.params.days) || 7;
      var results = [];
      for (var i = 0; i < days; i++) {
        var d = new Date();
        d.setDate(d.getDate() - i);
        var date = d.toISOString().split('T')[0];
        await db.computeDailySummary(date, platform);
        var summaries = await db.getDailySummaries(date, platform);
        results.push({ date: date, summaries: summaries });
      }
      res.json({ days: days, platform: platform || 'all', history: results });
    } catch(err) { res.status(500).json({ error: err.message }); }
  });

  app.get('/api/leaderboard', checkAuth, async function(req, res) {
    try {
      var platform = getEffectivePlatform(req);
      var date = req.query.date || new Date().toISOString().split('T')[0];
      await db.computeDailySummary(date, platform);
      var rankings = await db.getLeaderboard(date, platform);

      rankings = rankings.map(function(r) {
        var tv = Number(r.total_views), tl = Number(r.total_likes), tc = Number(r.total_comments), ts = Number(r.total_shares), pc = Number(r.post_count);
        r.avg_views = pc > 0 ? Math.round(tv / pc) : 0;
        r.total_score = tl + tc * 3 + ts * 5;
        r.engagement_rate = tv > 0 ? ((tl + tc) / tv * 100).toFixed(1) : '0.0';
        r.badge = r.avg_views >= 2000 ? 'top' : r.avg_views >= 500 ? 'bon' : 'faible';
        return r;
      });

      res.json({ date: date, platform: platform || 'all', rankings: rankings });
    } catch(err) { res.status(500).json({ error: err.message }); }
  });

  app.get('/api/compare', checkAuth, async function(req, res) {
    try {
      var platform = getEffectivePlatform(req);
      var va1 = req.query.va1;
      var va2 = req.query.va2;
      var days = parseInt(req.query.days) || 7;
      var result = { va1: [], va2: [] };
      for (var i = 0; i < days; i++) {
        var d = new Date();
        d.setDate(d.getDate() - i);
        var date = d.toISOString().split('T')[0];
        var s1 = await db.getVaDailyStats(va1, date, platform);
        var s2 = await db.getVaDailyStats(va2, date, platform);
        result.va1.push({ date: date, stats: s1 || null });
        result.va2.push({ date: date, stats: s2 || null });
      }
      res.json(result);
    } catch(err) { res.status(500).json({ error: err.message }); }
  });

  app.get('/api/thresholds', checkAuth, function(req, res) {
    res.json({
      viral: parseInt(process.env.VIRAL_VIEWS || '5000'),
      bon: parseInt(process.env.BON_VIEWS || '1000'),
      moyen: parseInt(process.env.MOYEN_VIEWS || '300'),
    });
  });

  app.get('/api/recommendations', checkAuth, async function(req, res) {
    try {
      var platform = getEffectivePlatform(req);

      // Build the period from query params.
      // Either ?date=YYYY-MM-DD (single day, legacy)
      // Or ?period=today|7d|15d|30d|all (relative range)
      // Or ?from=YYYY-MM-DD&to=YYYY-MM-DD (custom range)
      var dateOrPeriod;
      var todayStr = new Date().toISOString().split('T')[0];
      function daysAgo(n) {
        var d = new Date();
        d.setUTCDate(d.getUTCDate() - n);
        return d.toISOString().split('T')[0];
      }

      if (req.query.from || req.query.period === 'all') {
        // Period mode
        if (req.query.period === 'all') {
          dateOrPeriod = { from: null, to: todayStr };
        } else {
          dateOrPeriod = { from: req.query.from, to: req.query.to || todayStr };
        }
      } else if (req.query.period) {
        var to = todayStr;
        var from;
        if (req.query.period === 'today') from = todayStr;
        else if (req.query.period === '7d') from = daysAgo(6);  // 7 days including today
        else if (req.query.period === '15d') from = daysAgo(14);
        else if (req.query.period === '30d') from = daysAgo(29);
        else from = todayStr;
        dateOrPeriod = { from: from, to: to };
      } else {
        // Legacy: single date
        dateOrPeriod = req.query.date || todayStr;
      }

      // For single-day legacy mode, compute the daily summary cache (used by underperformers)
      if (typeof dateOrPeriod === 'string') {
        await db.computeDailySummary(dateOrPeriod, platform);
      }

      var recs = await db.getRecommendations(dateOrPeriod, platform);

      recs.postsToRepost = recs.postsToRepost.map(function(p) {
        p.score = calcScore(p);
        p.engagement = calcEngagement(p);
        p.advancedScore = calcAdvancedScore(p);
        return p;
      });
      recs.nuggets = recs.nuggets.map(function(p) {
        p.score = calcScore(p);
        p.engagement = calcEngagement(p);
        p.advancedScore = calcAdvancedScore(p);
        return p;
      });
      // Also compute scores for postsByTier
      if (recs.postsByTier) {
        ['viral', 'bon', 'moyen', 'flop'].forEach(function(tier) {
          if (recs.postsByTier[tier]) {
            recs.postsByTier[tier] = recs.postsByTier[tier].map(function(p) {
              p.score = calcScore(p);
              p.engagement = calcEngagement(p);
              return p;
            });
          }
        });
      }

      res.json(recs);
    } catch(err) { res.status(500).json({ error: err.message }); }
  });

  app.get('/api/saved-posts', checkAuth, async function(req, res) {
    try {
      var platform = getEffectivePlatform(req);
      var limit = parseInt(req.query.limit) || 50;
      var posts = await db.getSavedBestPosts(limit, platform);
      posts = posts.map(function(p) {
        p.score = calcScore(p);
        p.engagement = calcEngagement(p);
        p.advancedScore = calcAdvancedScore(p);
        p.perf = getPerf(Number(p.views) || 0);
        return p;
      });
      res.json({ platform: platform || 'all', posts: posts });
    } catch(err) { res.status(500).json({ error: err.message }); }
  });

  app.get('/api/streaks', checkAuth, async function(req, res) {
    try {
      var platform = getEffectivePlatform(req);
      var streaks = await db.getAllStreaks(platform);
      res.json({ platform: platform || 'all', streaks: streaks });
    } catch(err) { res.status(500).json({ error: err.message }); }
  });

  app.get('/api/heatmap', checkAuth, async function(req, res) {
    try {
      var platform = getEffectivePlatform(req);
      var daysParam = req.query.days;
      // Accept 'all' or a number; default 7
      var days;
      if (daysParam === 'all') days = 'all';
      else days = parseInt(daysParam) || 7;

      var hourly = await db.getHourlyPerformance(days, platform);

      var hourMap = {};
      for (var h = 0; h < 24; h++) hourMap[h] = { hour: h, post_count: 0, avg_views: 0, avg_likes: 0, avg_comments: 0, avg_engagement: 0, score: 0 };
      hourly.forEach(function(row) {
        var hr = Number(row.hour);
        hourMap[hr] = {
          hour: hr,
          post_count: Number(row.post_count),
          avg_views: Number(row.avg_views),
          avg_likes: Number(row.avg_likes),
          avg_comments: Number(row.avg_comments),
          avg_engagement: Number(row.avg_engagement),
          score: Number(row.avg_views) > 1 ? Math.round((Number(row.avg_engagement) / 100) * 100 * Math.log10(Number(row.avg_views)) * 100) / 100 : 0,
        };
      });

      var result = Object.values(hourMap).sort(function(a, b) { return a.hour - b.hour; });
      // Sum posts across all hours so the UI can display the total.
      var totalPosts = result.reduce(function(a, b) { return a + (b.post_count || 0); }, 0);
      res.json({ days: days, platform: platform || 'all', hours: result, totalPosts: totalPosts });
    } catch(err) { res.status(500).json({ error: err.message }); }
  });

  // ==================== ACCOUNTS (per-handle tracking) ====================

  // List accounts with aggregated stats. Filters: platform (via effective),
  // status=active|inactive|all (default active), vaDiscordId (optional).
  app.get('/api/accounts', checkAuth, async function(req, res) {
    try {
      var platform = getEffectivePlatform(req);
      var status = req.query.status || 'active';
      var opts = {};
      if (platform) opts.platform = platform;
      if (status && status !== 'all') opts.status = status;
      if (req.query.va) opts.vaDiscordId = req.query.va;
      var accounts = await db.listAccountsWithStats(opts);

      // Load shadowban candidates for this platform (or all) in one go, then
      // lookup by username during the enrichment loop.
      var sbByUsername = {};
      try {
        var sbRows = await db.getShadowbanCandidates(platform || null);
        sbRows.forEach(function(r) { sbByUsername[r.username + '|' + r.platform] = r; });
      } catch (e) { /* non-fatal */ }

      // Enrich with derived fields: days since last post + health + shadowban
      var now = Date.now();
      accounts = accounts.map(function(a) {
        var ref = a.last_post_at || a.last_seen_at;
        a.days_since_last_post = ref ? Math.floor((now - new Date(ref).getTime()) / (1000 * 60 * 60 * 24)) : null;
        var health = db.computeAccountHealth(a);
        a.health_status = health.health_status;
        a.health_score = health.health_score;
        a.health_reason = health.health_reason;
        // Shadowban fields (only populated for accounts with a drop candidate)
        var sb = sbByUsername[a.username + '|' + a.platform];
        if (sb) {
          var sbScore = db.computeShadowbanScore(sb);
          a.shadowban_score = sbScore.shadowban_score;
          a.shadowban_diagnosis = sbScore.diagnosis;
          a.views_drop_pct = sbScore.views_drop_pct;
          a.engagement_drop_pct = sbScore.engagement_drop_pct;
        } else {
          a.shadowban_score = 0;
          a.shadowban_diagnosis = 'ok';
          a.views_drop_pct = 0;
          a.engagement_drop_pct = 0;
        }
        return a;
      });
      res.json({ platform: platform || 'all', status: status, count: accounts.length, accounts: accounts });
    } catch(err) { res.status(500).json({ error: err.message }); }
  });

  // Account detail page: recent posts with latest stats.
  app.get('/api/accounts/:id', checkAuth, async function(req, res) {
    try {
      var days = parseInt(req.query.days) || 30;
      var details = await db.getAccountDetails(req.params.id, days);
      if (!details) return res.status(404).json({ error: 'Account not found' });
      // Respect platform ACL
      if (req.userPlatform !== 'all') {
        var userPlats = req.userPlatform.split(',');
        if (userPlats.indexOf(details.account.platform) === -1) {
          return res.status(403).json({ error: 'Access denied for this platform' });
        }
      }
      details.posts = details.posts.map(function(p) {
        p.score = calcScore(p);
        p.engagement = calcEngagement(p);
        p.perf = getPerf(Number(p.views) || 0);
        return p;
      });
      res.json(details);
    } catch(err) { res.status(500).json({ error: err.message }); }
  });

  // Manually flip an account's status (manager+ only).
  app.post('/api/accounts/:id/status', checkAuth, async function(req, res) {
    if (req.userRole !== 'admin' && req.userRole !== 'manager') {
      return res.status(403).json({ error: 'Manager or admin only' });
    }
    try {
      var status = req.body.status;
      if (status !== 'active' && status !== 'inactive') {
        return res.status(400).json({ error: 'status must be active or inactive' });
      }
      var updated = await db.setAccountStatus(req.params.id, status);
      if (!updated) return res.status(404).json({ error: 'Account not found' });
      res.json({ success: true, account: updated });
    } catch(err) { res.status(500).json({ error: err.message }); }
  });

  // Trigger an on-demand inactivity sweep (admin only).
  app.post('/api/accounts/sweep-inactive', checkAuth, async function(req, res) {
    if (req.userRole !== 'admin') return res.status(403).json({ error: 'Admin only' });
    try {
      var days = parseInt(req.body.days) || undefined;
      var flipped = await db.markInactiveAccounts(days);
      res.json({ success: true, flipped: flipped.length, accounts: flipped });
    } catch(err) { res.status(500).json({ error: err.message }); }
  });

  // ==================== VA PERSONAL OVERVIEW (/me page) ====================

  // Returns everything a VA needs to see their own status in one call.
  // Requires the user to have a discord_id linked. Platform is resolved from
  // the user's allowed platforms (if 'all', use the currently selected one).
  app.get('/api/me/overview', checkAuth, async function(req, res) {
    try {
      var discordId = req.userDiscordId;
      if (!discordId) {
        return res.status(400).json({ error: 'Aucun Discord ID associe a ce compte. Contacte un admin pour lier ton compte Discord.' });
      }
      var platform = getEffectivePlatform(req);
      // For 'all' users without ?platform=, pick the first allowed platform
      if (!platform) {
        var allowed = req.userPlatform === 'all' ? config.getActivePlatforms() : req.userPlatform.split(',');
        platform = allowed[0] || 'instagram';
      }

      var today = new Date().toISOString().split('T')[0];

      // Ensure today's daily summary is fresh, then fetch it for ranking
      await db.computeDailySummary(today, platform);
      var leaderboard = await db.getLeaderboard(today, platform);
      var myIndex = leaderboard.findIndex(function(r) { return r.va_discord_id === discordId; });
      var myStats = myIndex >= 0 ? leaderboard[myIndex] : null;

      // Today's posts with their latest snapshot
      var myPosts = await db.getVaPostsToday(discordId, today, platform);
      var postsWithStats = [];
      for (var i = 0; i < myPosts.length; i++) {
        var snap = await db.getLatestSnapshot(myPosts[i].id);
        postsWithStats.push({
          id: myPosts[i].id,
          url: myPosts[i].url,
          account: myPosts[i].account_username || null,
          post_type: myPosts[i].post_type,
          created_at: myPosts[i].created_at,
          caption: myPosts[i].caption,
          views: snap ? Number(snap.views) || 0 : 0,
          likes: snap ? Number(snap.likes) || 0 : 0,
          comments: snap ? Number(snap.comments) || 0 : 0,
          shares: snap ? Number(snap.shares) || 0 : 0,
          perf: getPerf(snap ? Number(snap.views) || 0 : 0),
        });
      }

      // Streak for this platform
      var streaks = await db.getAllStreaks(platform);
      var myStreak = streaks.find(function(s) { return s.va_discord_id === discordId; }) || null;

      // Weekly points standings (for my rank this week)
      var bounds = db.getWeekBounds();
      var weekly = await db.getWeeklyStandings(bounds.start, bounds.end, platform);
      var myWeeklyIndex = weekly.findIndex(function(r) { return r.va_discord_id === discordId; });
      var myWeekly = myWeeklyIndex >= 0 ? weekly[myWeeklyIndex] : null;

      // Active duel this week
      var activeDuels = await db.getActiveDuels(platform);
      var myDuel = activeDuels.find(function(d) { return d.va1_discord_id === discordId || d.va2_discord_id === discordId; }) || null;
      var duelData = null;
      if (myDuel) {
        // Compute live views for both duelists
        var viewsSql = "SELECT va_discord_id, COALESCE(SUM(s.views), 0)::bigint AS views " +
          "FROM posts p LEFT JOIN LATERAL ( " +
          "  SELECT views FROM snapshots sn WHERE sn.post_id = p.id AND COALESCE(sn.error, '') <> 'coaching_sent' " +
          "  ORDER BY sn.scraped_at DESC LIMIT 1 " +
          ") s ON true " +
          "WHERE p.platform = $1 AND p.va_discord_id IN ($2, $3) " +
          "AND p.created_at::date >= $4 AND p.created_at::date <= $5 " +
          "GROUP BY va_discord_id";
        var viewsResult = await db.pool.query(viewsSql, [platform, myDuel.va1_discord_id, myDuel.va2_discord_id, myDuel.week_start, myDuel.week_end]);
        var viewsMap = {};
        viewsResult.rows.forEach(function(r) { viewsMap[r.va_discord_id] = Number(r.views) || 0; });
        var isV1 = myDuel.va1_discord_id === discordId;
        duelData = {
          week_start: myDuel.week_start,
          week_end: myDuel.week_end,
          my_name: isV1 ? myDuel.va1_name : myDuel.va2_name,
          opponent_name: isV1 ? myDuel.va2_name : myDuel.va1_name,
          opponent_id: isV1 ? myDuel.va2_discord_id : myDuel.va1_discord_id,
          my_views: viewsMap[discordId] || 0,
          opponent_views: viewsMap[isV1 ? myDuel.va2_discord_id : myDuel.va1_discord_id] || 0,
        };
      }

      // Goal progress (6 posts/day)
      var goalPosts = 6;
      var goalProgress = Math.min(100, Math.round((postsWithStats.length / goalPosts) * 100));

      res.json({
        discordId: discordId,
        username: req.user,
        platform: platform,
        date: today,
        goal: { required: goalPosts, done: postsWithStats.length, progress: goalProgress },
        rank: {
          position: myIndex >= 0 ? myIndex + 1 : null,
          total: leaderboard.length,
          my_views: myStats ? Number(myStats.total_views) : 0,
          my_likes: myStats ? Number(myStats.total_likes) : 0,
          top3: leaderboard.slice(0, 3).map(function(r) {
            return { name: r.va_name, post_count: r.post_count, total_views: Number(r.total_views) };
          }),
        },
        streak: myStreak ? {
          current: Number(myStreak.current_streak),
          best: Number(myStreak.best_streak),
        } : { current: 0, best: 0 },
        weekly_points: myWeekly ? {
          position: myWeeklyIndex + 1,
          total_points: Number(myWeekly.total_points),
          podium_count: Number(myWeekly.podium_count),
        } : { position: null, total_points: 0, podium_count: 0 },
        duel: duelData,
        posts: postsWithStats,
      });
    } catch(err) { res.status(500).json({ error: err.message }); }
  });

  // ==================== ADMIN: USER MANAGEMENT ====================

  function checkAdmin(req, res, next) {
    if (req.userRole !== 'admin') return res.status(403).json({ error: 'Admin only' });
    return next();
  }

  // Managers and admins both have access — used for supervision features
  // (activity status, DM status) that managers need to supervise their team.
  function checkManagerOrAdmin(req, res, next) {
    if (req.userRole !== 'admin' && req.userRole !== 'manager') {
      return res.status(403).json({ error: 'Manager or admin only' });
    }
    return next();
  }

  // List all dashboard users
  app.get('/api/admin/users', checkAuth, checkAdmin, function(req, res) {
    var users = Object.keys(DASHBOARD_USERS).map(function(u) {
      return { username: u, role: DASHBOARD_USERS[u].role, platform: DASHBOARD_USERS[u].platform, discordId: DASHBOARD_USERS[u].discordId || null };
    });
    res.json({ users: users });
  });

  // Create or update a dashboard user
  app.post('/api/admin/users', checkAuth, checkAdmin, async function(req, res) {
    var username = (req.body.username || '').trim().toLowerCase();
    var password = req.body.password || '';
    var role = req.body.role || 'va';
    var platform = req.body.platform || 'all';
    var discordId = (req.body.discord_id || req.body.discordId || '').trim() || null;

    if (!username || username.length < 2) return res.status(400).json({ error: 'Username trop court (min 2 caracteres)' });
    if (!password || password.length < 4) return res.status(400).json({ error: 'Mot de passe trop court (min 4 caracteres)' });
    if (['admin', 'manager', 'va'].indexOf(role) === -1) return res.status(400).json({ error: 'Role invalide (admin, manager, va)' });

    // Validate platform: accept 'all', a single platform, OR a comma-separated combo.
    // Allowed individual platforms: instagram, twitter, geelark, threads.
    var validPlats = ['instagram', 'twitter', 'geelark', 'threads'];
    if (platform !== 'all') {
      var platParts = platform.split(',').map(function(p) { return p.trim(); }).filter(Boolean);
      if (platParts.length === 0) return res.status(400).json({ error: 'Plateforme manquante' });
      var invalid = platParts.filter(function(p) { return validPlats.indexOf(p) === -1; });
      if (invalid.length > 0) return res.status(400).json({ error: 'Plateforme(s) invalide(s): ' + invalid.join(', ') });
      // Re-normalize so the stored value is always sorted/clean
      platform = platParts.join(',');
    }

    if (discordId && !/^\d{17,20}$/.test(discordId)) {
      return res.status(400).json({ error: 'Discord ID doit faire 17-20 chiffres' });
    }

    var isNew = !DASHBOARD_USERS[username];
    DASHBOARD_USERS[username] = { password: password, role: role, platform: platform, discordId: discordId };

    // Persist to DB. We AWAIT this so we can return a real error to the frontend
    // if the DB rejects (which is what was happening before with the CHECK constraint).
    try {
      await db.upsertDashboardUser(username, password, role, platform, discordId);
    } catch (e) {
      console.log('[Users] DB save FAILED for ' + username + ': ' + e.message);
      // Roll back the in-memory change so the UI shows the truth
      if (isNew) delete DASHBOARD_USERS[username];
      return res.status(500).json({ error: 'Sauvegarde en base echouee: ' + e.message });
    }

    console.log('[Users] ' + (isNew ? 'CREATED' : 'UPDATED') + ' ' + username + ' role=' + role + ' platform=' + platform + (discordId ? ' discord=' + discordId : ''));
    res.json({ success: true, action: isNew ? 'cree' : 'modifie' });
  });
  // Update user platform/role (without changing password)
  app.put('/api/admin/users/:username', checkAuth, checkAdmin, async function(req, res) {
    var username = req.params.username;
    if (!DASHBOARD_USERS[username]) return res.status(404).json({ error: 'Utilisateur non trouve' });

    var role = req.body.role || DASHBOARD_USERS[username].role;
    var platform = req.body.platform || DASHBOARD_USERS[username].platform;
    var password = req.body.password || DASHBOARD_USERS[username].password;
    var discordId = req.body.discord_id !== undefined ? req.body.discord_id : (req.body.discordId !== undefined ? req.body.discordId : DASHBOARD_USERS[username].discordId);
    if (discordId === '') discordId = null;
    if (discordId && !/^\d{17,20}$/.test(String(discordId))) {
      return res.status(400).json({ error: 'Discord ID doit faire 17-20 chiffres' });
    }

    // Validate platform — same logic as POST
    var validPlats = ['instagram', 'twitter', 'geelark', 'threads'];
    if (platform !== 'all') {
      var platParts = String(platform).split(',').map(function(p) { return p.trim(); }).filter(Boolean);
      if (platParts.length === 0) return res.status(400).json({ error: 'Plateforme manquante' });
      var invalid = platParts.filter(function(p) { return validPlats.indexOf(p) === -1; });
      if (invalid.length > 0) return res.status(400).json({ error: 'Plateforme(s) invalide(s): ' + invalid.join(', ') });
      platform = platParts.join(',');
    }

    var oldUser = Object.assign({}, DASHBOARD_USERS[username]);
    DASHBOARD_USERS[username] = { password: password, role: role, platform: platform, discordId: discordId || null };

    try {
      await db.upsertDashboardUser(username, password, role, platform, discordId || null);
    } catch (e) {
      console.log('[Users] DB update FAILED for ' + username + ': ' + e.message);
      DASHBOARD_USERS[username] = oldUser; // rollback
      return res.status(500).json({ error: 'Sauvegarde en base echouee: ' + e.message });
    }

    console.log('[Users] UPDATED ' + username + ' role=' + role + ' platform=' + platform + (discordId ? ' discord=' + discordId : ''));
    res.json({ success: true, username: username, role: role, platform: platform, discordId: discordId || null });
  });

  // Delete a dashboard user
  app.delete('/api/admin/users/:username', checkAuth, checkAdmin, function(req, res) {
    var username = req.params.username;
    if (!DASHBOARD_USERS[username]) return res.status(404).json({ error: 'Utilisateur non trouve' });
    if (username === req.user) return res.status(400).json({ error: 'Tu ne peux pas te supprimer toi-meme' });

    delete DASHBOARD_USERS[username];

    // Also remove from DB
    db.pool.query('DELETE FROM dashboard_users WHERE username = $1', [username]).catch(function(e) {
      console.error('Failed to delete user from DB:', e.message);
    });

    console.log('[Admin] User deleted: ' + username);
    res.json({ success: true, deleted: username });
  });

  // List revoked dashboard users (not loaded in memory — read from DB)
  app.get('/api/admin/revoked-users', checkAuth, checkAdmin, async function(req, res) {
    try {
      var result = await db.pool.query("SELECT username, role, platform, discord_id, revoked_at, revoked_reason FROM dashboard_users WHERE status = 'revoked' ORDER BY revoked_at DESC");
      res.json({ users: result.rows });
    } catch(err) { res.status(500).json({ error: err.message }); }
  });

  // Reactivate a previously revoked user.
  app.post('/api/admin/users/:username/reactivate', checkAuth, checkAdmin, async function(req, res) {
    try {
      var username = req.params.username;
      var row = await db.reactivateDashboardUser(username);
      if (!row) return res.status(404).json({ error: 'Utilisateur non trouve ou pas revoque' });
      // Reload into in-memory map
      DASHBOARD_USERS[username] = {
        password: row.password_hash,
        role: row.role,
        platform: row.platform,
        discordId: row.discord_id || null,
      };
      console.log('[Admin] User reactivated: ' + username);
      res.json({ success: true, username: username });
    } catch(err) { res.status(500).json({ error: err.message }); }
  });

  // Trigger the dashboard user revocation sweep on demand (admin only).
  app.post('/api/admin/sweep-users', checkAuth, checkAdmin, async function(req, res) {
    try {
      // Lazy require to avoid circular imports at load-time
      var cron = require('../jobs/cron');
      if (typeof cron.sweepDashboardUsers !== 'function') {
        return res.status(500).json({ error: 'Sweep function not available' });
      }
      await cron.sweepDashboardUsers();
      res.json({ success: true, message: 'Sweep lance — consulte les logs et la liste des revoques.' });
    } catch(err) { res.status(500).json({ error: err.message }); }
  });

  // VA activity status: cross-reference current Discord VA members with their
  // posting activity (posts today, posts 7d, last post date) to flag those
  // who are inactive or under-posting. Three severity levels:
  //   - red:    no post for 2+ days, OR < 10 posts in 7 days, OR never posted
  //   - orange: 0 posts today past 14:00 Paris, OR under-posting today
  //   - green:  on pace
  app.get('/api/admin/activity-status', checkAuth, checkManagerOrAdmin, async function(req, res) {
    try {
      // Try to get the Discord client from any of the modules that hold it.
      // notifyWorker is always loaded and receives setDiscordClient() at startup.
      var client = null;
      try {
        var notifyW = require('../jobs/notifyWorker');
        if (notifyW.getDiscordClient) client = notifyW.getDiscordClient();
      } catch(e) {}
      if (!client) {
        try {
          var cron = require('../jobs/cron');
          if (cron.getDiscordClient) client = cron.getDiscordClient();
        } catch(e) {}
      }
      if (!client) return res.json({ count: 0, users: [], warning: 'Discord client not ready' });

      // Optional platform filter (query param). If set, restrict to that single platform.
      // Always restrict to the user's allowed platforms (for managers).
      var requestedPlatform = req.query.platform;
      var allowedForUser = getUserAllowedPlatforms(req);
      var activePlats = config.getActivePlatforms();
      var platforms = activePlats.filter(function(p) {
        return allowedForUser.indexOf(p) !== -1;
      });
      console.log('[activity-status] user=' + req.user + ' userPlat=' + req.userPlatform + ' requested=' + requestedPlatform + ' allowed=' + JSON.stringify(allowedForUser) + ' active=' + JSON.stringify(activePlats) + ' final=' + JSON.stringify(platforms));
      if (requestedPlatform && platforms.indexOf(requestedPlatform) !== -1) {
        platforms = [requestedPlatform];
      }

      // Gather activity rows per platform
      var activityByIdPlat = {}; // { [platform]: { [discordId]: row } }
      for (var i = 0; i < platforms.length; i++) {
        var p = platforms[i];
        activityByIdPlat[p] = {};
        try {
          var rows = await db.getVaActivityStatus(p);
          rows.forEach(function(r) { activityByIdPlat[p][r.va_discord_id] = r; });
        } catch (e) {
          // continue
        }
      }

      // Paris hour for orange-tier logic
      var parisHour;
      try {
        var parts = new Intl.DateTimeFormat('en-US', { timeZone: 'Europe/Paris', hour: 'numeric', hour12: false }).formatToParts(new Date());
        var hpart = parts.find(function(p){return p.type==='hour'});
        parisHour = hpart ? parseInt(hpart.value, 10) : new Date().getUTCHours();
      } catch (e) { parisHour = new Date().getUTCHours(); }

      var seen = {};
      var out = [];

      for (var j = 0; j < platforms.length; j++) {
        var plat = platforms[j];
        var pc = config.platforms[plat];
        if (!pc || !pc.guildId || !pc.vaRoleId) {
          console.log('[activity-status] SKIP ' + plat + ': pc=' + (!!pc) + ' guildId=' + (pc?pc.guildId:'-') + ' vaRoleId=' + (pc?pc.vaRoleId:'-'));
          continue;
        }
        var guild = null;
        try {
          guild = await client.guilds.fetch(pc.guildId);
        } catch (e) {
          console.log('[activity-status] guilds.fetch failed for ' + plat + ': ' + e.message);
          continue;
        }
        // Try to refresh member cache, but fall back to whatever cache is already there.
        // This avoids Discord rate-limits when we request multiple guilds in succession.
        try {
          if (guild.members.cache.size < 2) {
            await guild.members.fetch();
          }
        } catch (e) {
          console.log('[activity-status] members.fetch failed for ' + plat + ', using cache of size ' + guild.members.cache.size + ': ' + e.message);
        }
        try {
          var members = guild.members.cache.filter(function(m) {
            return m.roles.cache.has(pc.vaRoleId) && !m.user.bot;
          });
          console.log('[activity-status] ' + plat + ': cache=' + guild.members.cache.size + ' VA-role=' + members.size + ' (vaRoleId=' + pc.vaRoleId + ')');
          members.forEach(function(m) {
            var id = m.user.id;
            if (seen[id]) {
              seen[id].platforms.push(plat);
              var extra = activityByIdPlat[plat][id];
              if (extra) {
                seen[id].posts_today += Number(extra.posts_today) || 0;
                seen[id].posts_7d += Number(extra.posts_7d) || 0;
                if (extra.last_post_at && (!seen[id].last_post_at || new Date(extra.last_post_at) > new Date(seen[id].last_post_at))) {
                  seen[id].last_post_at = extra.last_post_at;
                }
              }
              return;
            }
            var act = activityByIdPlat[plat][id] || { posts_today: 0, posts_7d: 0, last_post_at: null };
            var row = {
              discord_id: id,
              va_name: m.displayName || m.user.username,
              platforms: [plat],
              posts_today: Number(act.posts_today) || 0,
              posts_7d: Number(act.posts_7d) || 0,
              last_post_at: act.last_post_at,
            };
            seen[id] = row;
            out.push(row);
          });
        } catch (e) {
          console.log('[activity-status] Member iteration failed for ' + plat + ': ' + e.message);
        }
      }

      var nowMs = Date.now();
      out.forEach(function(r) {
        var daysSinceLast = r.last_post_at ? Math.floor((nowMs - new Date(r.last_post_at).getTime()) / (1000*60*60*24)) : null;
        r.days_since_last = daysSinceLast;
        var status, label, reason;
        if (daysSinceLast === null) {
          status = 'red'; label = 'URGENT'; reason = "n'a jamais poste";
        } else if (daysSinceLast >= 2) {
          status = 'red'; label = 'URGENT'; reason = 'pas poste depuis ' + daysSinceLast + 'j';
        } else if (r.posts_7d < 10) {
          status = 'red'; label = 'URGENT'; reason = 'seulement ' + r.posts_7d + ' posts/7j (objectif: 42)';
        } else if (r.posts_today === 0 && parisHour >= 14) {
          status = 'orange'; label = 'En retard'; reason = '0 post aujourd\'hui apres 14h';
        } else if (r.posts_today === 0) {
          status = 'orange'; label = 'A surveiller'; reason = '0 post aujourd\'hui (encore tot)';
        } else if (r.posts_today < 6) {
          status = 'orange'; label = 'Sous objectif'; reason = r.posts_today + '/6 posts aujourd\'hui';
        } else {
          status = 'green'; label = 'OK'; reason = 'objectif atteint';
        }
        r.status = status;
        r.label = label;
        r.reason = reason;
      });

      var order = { red: 0, orange: 1, green: 2 };
      out.sort(function(a, b) {
        if (order[a.status] !== order[b.status]) return order[a.status] - order[b.status];
        if (a.posts_today !== b.posts_today) return a.posts_today - b.posts_today;
        return (a.va_name || '').localeCompare(b.va_name || '');
      });

      res.json({
        count: out.length,
        red: out.filter(function(r){return r.status==='red'}).length,
        orange: out.filter(function(r){return r.status==='orange'}).length,
        green: out.filter(function(r){return r.status==='green'}).length,
        users: out,
      });
    } catch(err) { res.status(500).json({ error: err.message }); }
  });
  app.get('/api/admin/dm-status', checkAuth, checkManagerOrAdmin, async function(req, res) {
    try {
      var dbRows = await db.getAllDmStatus();
      var byId = {};
      dbRows.forEach(function(r) { byId[r.discord_id] = r; });

      // Optional platform filter
      // Always restrict to the user's allowed platforms (for managers).
      var requestedPlatform = req.query.platform;
      var allowedForUser = getUserAllowedPlatforms(req);
      var platforms = config.getActivePlatforms().filter(function(p) {
        return allowedForUser.indexOf(p) !== -1;
      });
      if (requestedPlatform && platforms.indexOf(requestedPlatform) !== -1) {
        platforms = [requestedPlatform];
      }

      var out = [];
      var seen = {};

      // Try to get the Discord client from notifyWorker first (always loaded),
      // fall back to cron if needed.
      var client = null;
      try {
        var notifyW = require('../jobs/notifyWorker');
        if (notifyW.getDiscordClient) client = notifyW.getDiscordClient();
      } catch(e) {}
      if (!client) {
        try {
          var cron = require('../jobs/cron');
          if (cron.getDiscordClient) client = cron.getDiscordClient();
        } catch(e) {}
      }

      if (client) {
        for (var i = 0; i < platforms.length; i++) {
          var p = platforms[i];
          var pc = config.platforms[p];
          if (!pc || !pc.guildId || !pc.vaRoleId) continue;
          var guild = null;
          try {
            guild = await client.guilds.fetch(pc.guildId);
          } catch (e) {
            console.log('[dm-status] guilds.fetch failed for ' + p + ': ' + e.message);
            continue;
          }
          try {
            if (guild.members.cache.size < 2) {
              await guild.members.fetch();
            }
          } catch (e) {
            console.log('[dm-status] members.fetch failed for ' + p + ', using cache of size ' + guild.members.cache.size + ': ' + e.message);
          }
          try {
            var members = guild.members.cache.filter(function(m) {
              return m.roles.cache.has(pc.vaRoleId) && !m.user.bot;
            });
            members.forEach(function(m) {
              var id = m.user.id;
              if (seen[id]) {
                // Already added from another platform — just append this platform
                seen[id].platforms.push(p);
                return;
              }
              var rec = byId[id];
              var status, label;
              if (!rec || (!rec.last_ok_at && !rec.last_fail_at)) {
                status = 'unknown'; label = 'Jamais teste';
              } else if (rec.last_ok_at && (!rec.last_fail_at || new Date(rec.last_ok_at) >= new Date(rec.last_fail_at))) {
                status = 'ok'; label = 'OK';
              } else {
                status = 'blocked'; label = 'DM bloques';
              }
              var row = {
                discord_id: id,
                va_name: m.displayName || m.user.username,
                platforms: [p],
                status: status,
                label: label,
                last_ok_at: rec ? rec.last_ok_at : null,
                last_fail_at: rec ? rec.last_fail_at : null,
                last_fail_reason: rec ? rec.last_fail_reason : null,
                total_ok: rec ? rec.total_ok : 0,
                total_fail: rec ? rec.total_fail : 0,
              };
              seen[id] = row;
              out.push(row);
            });
          } catch (e) {
            console.log('[dm-status] Member iteration failed for ' + p + ': ' + e.message);
          }
        }
      }

      // Sort: blocked first (action needed), then unknown, then ok
      var order = { blocked: 0, unknown: 1, ok: 2 };
      out.sort(function(a, b) {
        if (order[a.status] !== order[b.status]) return order[a.status] - order[b.status];
        return (a.va_name || '').localeCompare(b.va_name || '');
      });

      res.json({
        count: out.length,
        ok: out.filter(function(r){return r.status==='ok'}).length,
        blocked: out.filter(function(r){return r.status==='blocked'}).length,
        unknown: out.filter(function(r){return r.status==='unknown'}).length,
        users: out,
      });
    } catch(err) { res.status(500).json({ error: err.message }); }
  });

  // Force the weekly ceremony (champion announcement + duel creation) for a platform.
  app.post('/api/admin/force-duels', checkAuth, checkAdmin, async function(req, res) {
    try {
      var cron = require('../jobs/cron');
      if (typeof cron.runWeeklyCeremony !== 'function') {
        return res.status(500).json({ error: 'Weekly ceremony function not available' });
      }
      var platform = req.body && req.body.platform ? req.body.platform : null;
      if (!platform) return res.status(400).json({ error: 'Plateforme manquante' });
      if (!config.platforms[platform] || !config.platforms[platform].guildId) {
        return res.status(400).json({ error: 'Plateforme invalide ou non configuree: ' + platform });
      }
      await cron.runWeeklyCeremony(platform);
      res.json({ success: true, message: 'Ceremonie forcee pour ' + platform + '. Va voir les canaux #results et #duels.' });
    } catch(err) { res.status(500).json({ error: err.message }); }
  });

  // === Threads accounts pool monitoring (admin) ===
  // Returns the status of each Threads scraper account (active / banned / rate_limited)
  // along with success/error counters. Useful to know when to rotate accounts.
  app.get('/api/admin/threads-accounts', checkAuth, checkAdmin, function(req, res) {
    try {
      var threadsAccounts = require('../scrapers/threadsAccounts');
      var status = threadsAccounts.getAccountsStatus();
      var activeCount = threadsAccounts.getActiveCount();
      res.json({
        accounts: status,
        activeCount: activeCount,
        totalCount: status.length,
      });
    } catch(err) { res.status(500).json({ error: err.message }); }
  });

  // === Crash alerts state monitoring (admin) ===
  // Returns the per-platform consecutive-failure counters and whether an alert
  // has been fired for each. Useful to debug "why didn't I get an alert?".
  app.get('/api/admin/crash-alerts', checkAuth, checkAdmin, function(req, res) {
    try {
      var crashAlerts = require('../jobs/crashAlerts');
      res.json({
        threshold: crashAlerts.THRESHOLD,
        platforms: crashAlerts.getStatus(),
      });
    } catch(err) { res.status(500).json({ error: err.message }); }
  });

  // === In-app notifications (bell icon) ===
  // List recent notifications, optionally filtered by platform.
  app.get('/api/notifications', checkAuth, async function(req, res) {
    try {
      var platform = getEffectivePlatform(req);
      var limit = Math.min(parseInt(req.query.limit) || 50, 200);
      var notifs = await db.getNotifications(platform, limit);
      res.json({ notifications: notifs, platform: platform || 'all' });
    } catch(err) { res.status(500).json({ error: err.message }); }
  });

  // Unread count badge for the bell icon. Tied to req.user so two users on the
  // same platform have independent unread counts.
  app.get('/api/notifications/unread', checkAuth, async function(req, res) {
    try {
      var platform = getEffectivePlatform(req);
      var count = await db.getUnreadCount(req.user, platform);
      res.json({ unread: count, platform: platform || 'all' });
    } catch(err) { res.status(500).json({ error: err.message }); }
  });

  // Mark all notifications for this user/platform as read up to NOW.
  app.post('/api/notifications/mark-read', checkAuth, async function(req, res) {
    try {
      var platform = getEffectivePlatform(req);
      await db.markNotificationsRead(req.user, platform);
      res.json({ success: true });
    } catch(err) { res.status(500).json({ error: err.message }); }
  });

  // === Soft delete a post (admin or manager-of-platform) ===
  // The post stays in the DB with deleted_at filled. It's hidden from rankings
  // and totals but can be restored. Permission check: managers can only delete
  // posts on their platform; admins can delete anywhere.
  app.post('/api/posts/:id/delete', checkAuth, checkManagerOrAdmin, async function(req, res) {
    try {
      var postId = parseInt(req.params.id, 10);
      if (!postId || isNaN(postId)) return res.status(400).json({ error: 'Invalid post ID' });

      var post = await db.getPostBasics(postId);
      if (!post) return res.status(404).json({ error: 'Post non trouve' });
      if (post.deleted_at) return res.status(400).json({ error: 'Post deja supprime' });

      // Permission check for managers: must match their assigned platform
      if (req.userRole === 'manager') {
        var allowed = getUserAllowedPlatforms(req);
        if (allowed && allowed.length > 0 && allowed.indexOf(post.platform) === -1) {
          return res.status(403).json({ error: 'Tu ne peux supprimer que les posts de ta plateforme' });
        }
      }

      var deletedBy = req.username || (req.userRole + '_unknown');
      var deleted = await db.softDeletePost(postId, deletedBy);
      if (!deleted) return res.status(500).json({ error: 'Suppression echouee' });

      res.json({ success: true, message: 'Post supprime' });
    } catch(err) { res.status(500).json({ error: err.message }); }
  });

  // === Restore a soft-deleted post ===
  app.post('/api/posts/:id/restore', checkAuth, checkManagerOrAdmin, async function(req, res) {
    try {
      var postId = parseInt(req.params.id, 10);
      if (!postId || isNaN(postId)) return res.status(400).json({ error: 'Invalid post ID' });

      var post = await db.getPostBasics(postId);
      if (!post) return res.status(404).json({ error: 'Post non trouve' });
      if (!post.deleted_at) return res.status(400).json({ error: 'Post n\'est pas supprime' });

      // Same permission check as delete
      if (req.userRole === 'manager') {
        var allowed = getUserAllowedPlatforms(req);
        if (allowed && allowed.length > 0 && allowed.indexOf(post.platform) === -1) {
          return res.status(403).json({ error: 'Tu ne peux restaurer que les posts de ta plateforme' });
        }
      }

      var restored = await db.restorePost(postId);
      if (!restored) return res.status(500).json({ error: 'Restauration echouee' });

      res.json({ success: true, message: 'Post restaure' });
    } catch(err) { res.status(500).json({ error: err.message }); }
  });

  // Force the daily summary for a platform (useful for testing or re-sending).
  app.post('/api/admin/force-summary', checkAuth, checkAdmin, async function(req, res) {
    try {
      var cron = require('../jobs/cron');
      if (typeof cron.sendDailySummaryForPlatform !== 'function') {
        return res.status(500).json({ error: 'Daily summary function not available' });
      }
      var platform = req.body && req.body.platform ? req.body.platform : null;
      if (!platform) return res.status(400).json({ error: 'Plateforme manquante' });
      if (!config.platforms[platform] || !config.platforms[platform].guildId) {
        return res.status(400).json({ error: 'Plateforme invalide ou non configuree: ' + platform });
      }
      await cron.sendDailySummaryForPlatform(platform);
      res.json({ success: true, message: 'Resume du jour envoye pour ' + platform + '.' });
    } catch(err) { res.status(500).json({ error: err.message }); }
  });

  // ==================== STATIC PAGES ====================

  app.get('/', function(req, res) {
    res.sendFile(path.join(__dirname, 'dashboard.html'));
  });

  // VA personal mobile page (PWA entry point)
  app.get('/me', function(req, res) {
    res.sendFile(path.join(__dirname, 'me.html'));
  });

  // Admins / managers can use this shortcut to reach the full dashboard
  app.get('/dashboard', function(req, res) {
    res.sendFile(path.join(__dirname, 'dashboard.html'));
  });

  // PWA manifest + service worker
  app.get('/manifest.json', function(req, res) {
    res.setHeader('Content-Type', 'application/manifest+json');
    res.sendFile(path.join(__dirname, 'manifest.json'));
  });
  app.get('/sw.js', function(req, res) {
    res.setHeader('Content-Type', 'application/javascript');
    res.setHeader('Cache-Control', 'no-cache');
    res.sendFile(path.join(__dirname, 'sw.js'));
  });

  // PWA icons (served from src/web/)
  app.get('/icon-192.png', function(req, res) {
    res.setHeader('Cache-Control', 'public, max-age=86400');
    res.sendFile(path.join(__dirname, 'icon-192.png'));
  });
  app.get('/icon-512.png', function(req, res) {
    res.setHeader('Cache-Control', 'public, max-age=86400');
    res.sendFile(path.join(__dirname, 'icon-512.png'));
  });

  var port = process.env.PORT || 3000;
  app.listen(port, function() {
    console.log('Dashboard running on port ' + port);
  });

  return app;
}

module.exports = { createWebServer: createWebServer };
