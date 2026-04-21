var express = require('express');
var path = require('path');
var config = require('../../config');
var db = require('../db/queries');

// DASHBOARD_USERS format: username:password:role:platform
// Example: admin:admin123:admin:all,manager1:pass1:manager:instagram
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
      };
      console.log('[Users] ENV user loaded: ' + username + ' role=' + (parts[2]||'va') + ' platform=' + (parts[3]||'all'));
    }
  });

  // Load DB users (skip any that exist in ENV)
  db.pool.query('SELECT * FROM dashboard_users').then(function(result) {
    result.rows.forEach(function(row) {
      if (ENV_USERNAMES.has(row.username)) {
        console.log('[Users] Skipping DB user (ENV has priority): ' + row.username);
        db.pool.query('DELETE FROM dashboard_users WHERE username = $1', [row.username]).catch(function(){});
      } else {
        DASHBOARD_USERS[row.username] = {
          password: row.password_hash,
          role: row.role,
          platform: row.platform,
        };
        console.log('[Users] DB user loaded: ' + row.username + ' role=' + row.role);
      }
    });
    console.log('[Users] Total: ' + Object.keys(DASHBOARD_USERS).length);
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
      return next();
    }
  } catch(e) {}
  return res.status(401).json({ error: 'Invalid token' });
}

// Check if user can access requested platform
function checkPlatformAccess(req, res, next) {
  var requestedPlatform = req.query.platform || req.params.platform;
  if (!requestedPlatform || req.userPlatform === 'all') return next();
  if (req.userPlatform !== requestedPlatform) {
    return res.status(403).json({ error: 'Access denied for platform: ' + requestedPlatform });
  }
  return next();
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
  return req.userPlatform; // forced to user's platform
}

function createWebServer() {
  loadUsers();
  var app = express();
  app.use(express.json());

  // Login — returns allowed platforms
  app.post('/api/login', function(req, res) {
    var username = req.body.username;
    var password = req.body.password;
    console.log('[Login] Attempt: ' + username + ' | stored role: ' + (DASHBOARD_USERS[username] ? DASHBOARD_USERS[username].role : 'NOT FOUND') + ' | stored platform: ' + (DASHBOARD_USERS[username] ? DASHBOARD_USERS[username].platform : 'N/A'));
    if (DASHBOARD_USERS[username] && DASHBOARD_USERS[username].password === password) {
      var user = DASHBOARD_USERS[username];
      var token = Buffer.from(username + ':' + password).toString('base64');
      var allowedPlatforms = [];
      if (user.platform === 'all') {
        allowedPlatforms = config.getActivePlatforms();
      } else {
        allowedPlatforms = [user.platform];
      }
      console.log('[Login] Success: ' + username + ' role=' + user.role + ' platform=' + user.platform);
      return res.json({
        token: token,
        username: username,
        role: user.role,
        platform: user.platform,
        allowedPlatforms: allowedPlatforms,
      });
    }
    return res.status(401).json({ error: 'Invalid credentials' });
  });

  // User info
  app.get('/api/me', checkAuth, function(req, res) {
    var user = DASHBOARD_USERS[req.user];
    var allowedPlatforms = [];
    if (user.platform === 'all') {
      allowedPlatforms = config.getActivePlatforms();
    } else {
      allowedPlatforms = [user.platform];
    }
    res.json({
      username: req.user,
      role: user.role,
      platform: user.platform,
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
      var date = req.query.date || new Date().toISOString().split('T')[0];
      var posts = await db.getVaPostsToday(req.params.discordId, date, platform);
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
      var stats = await db.getVaDailyStats(req.params.discordId, date, platform);
      res.json({ va_id: req.params.discordId, date: date, platform: platform || 'all', posts: snapshots, stats: stats });
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

  app.get('/api/top-posts', checkAuth, async function(req, res) {
    try {
      var platform = getEffectivePlatform(req);
      var date = req.query.date || new Date().toISOString().split('T')[0];
      var posts = await db.getTopPostsWithPerformance(date, platform);

      posts = posts.map(function(p) {
        p.score = calcScore(p);
        p.engagement = calcEngagement(p);
        p.perf = getPerf(Number(p.views) || 0);
        return p;
      });

      res.json({ date: date, platform: platform || 'all', posts: posts });
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
      var date = req.query.date || new Date().toISOString().split('T')[0];
      await db.computeDailySummary(date, platform);
      var recs = await db.getRecommendations(date, platform);

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
      var days = parseInt(req.query.days) || 7;
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
      res.json({ days: days, platform: platform || 'all', hours: result });
    } catch(err) { res.status(500).json({ error: err.message }); }
  });

  // ==================== ADMIN: USER MANAGEMENT ====================

  function checkAdmin(req, res, next) {
    if (req.userRole !== 'admin') return res.status(403).json({ error: 'Admin only' });
    return next();
  }

  // List all dashboard users
  app.get('/api/admin/users', checkAuth, checkAdmin, function(req, res) {
    var users = Object.keys(DASHBOARD_USERS).map(function(u) {
      return { username: u, role: DASHBOARD_USERS[u].role, platform: DASHBOARD_USERS[u].platform };
    });
    res.json({ users: users });
  });

  // Create or update a dashboard user
  app.post('/api/admin/users', checkAuth, checkAdmin, function(req, res) {
    var username = (req.body.username || '').trim().toLowerCase();
    var password = req.body.password || '';
    var role = req.body.role || 'va';
    var platform = req.body.platform || 'all';

    if (!username || username.length < 2) return res.status(400).json({ error: 'Username trop court (min 2 caracteres)' });
    if (!password || password.length < 4) return res.status(400).json({ error: 'Mot de passe trop court (min 4 caracteres)' });
    if (['admin', 'manager', 'va'].indexOf(role) === -1) return res.status(400).json({ error: 'Role invalide (admin, manager, va)' });
    if (['all', 'instagram', 'twitter', 'geelark'].indexOf(platform) === -1) return res.status(400).json({ error: 'Plateforme invalide' });

    var isNew = !DASHBOARD_USERS[username];
    DASHBOARD_USERS[username] = { password: password, role: role, platform: platform };

    // Also save to DB for persistence across restarts
    db.upsertDashboardUser(username, password, role, platform).catch(function(e) {
      console.error('Failed to save user to DB:', e.message);
    });

    console.log('[Admin] User ' + (isNew ? 'created' : 'updated') + ': ' + username + ' (' + role + '/' + platform + ')');
    res.json({ success: true, action: isNew ? 'created' : 'updated', username: username, role: role, platform: platform });
  });

  // Update user platform/role (without changing password)
  app.put('/api/admin/users/:username', checkAuth, checkAdmin, function(req, res) {
    var username = req.params.username;
    if (!DASHBOARD_USERS[username]) return res.status(404).json({ error: 'Utilisateur non trouve' });

    var role = req.body.role || DASHBOARD_USERS[username].role;
    var platform = req.body.platform || DASHBOARD_USERS[username].platform;
    var password = req.body.password || DASHBOARD_USERS[username].password;

    DASHBOARD_USERS[username] = { password: password, role: role, platform: platform };

    db.upsertDashboardUser(username, password, role, platform).catch(function(e) {
      console.error('Failed to update user in DB:', e.message);
    });

    console.log('[Admin] User updated: ' + username + ' (' + role + '/' + platform + ')');
    res.json({ success: true, username: username, role: role, platform: platform });
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

  app.get('/', function(req, res) {
    res.sendFile(path.join(__dirname, 'dashboard.html'));
  });

  var port = process.env.PORT || 3000;
  app.listen(port, function() {
    console.log('Dashboard running on port ' + port);
  });

  return app;
}

module.exports = { createWebServer: createWebServer };
