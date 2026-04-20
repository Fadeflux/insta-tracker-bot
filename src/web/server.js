var express = require('express');
var path = require('path');
var config = require('../../config');
var db = require('../db/queries');

var DASHBOARD_USERS = {};

function loadUsers() {
  var raw = process.env.DASHBOARD_USERS || 'admin:admin123';
  var pairs = raw.split(',');
  pairs.forEach(function(pair) {
    var parts = pair.trim().split(':');
    if (parts.length === 2) {
      DASHBOARD_USERS[parts[0]] = parts[1];
    }
  });
  console.log('Dashboard users loaded: ' + Object.keys(DASHBOARD_USERS).length);
}

function checkAuth(req, res, next) {
  var token = req.headers['x-auth-token'] || req.query.token;
  if (!token) return res.status(401).json({ error: 'No token' });
  try {
    var decoded = Buffer.from(token, 'base64').toString();
    var parts = decoded.split(':');
    if (parts.length === 2 && DASHBOARD_USERS[parts[0]] === parts[1]) {
      req.user = parts[0];
      return next();
    }
  } catch(e) {}
  return res.status(401).json({ error: 'Invalid token' });
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

function createWebServer() {
  loadUsers();
  var app = express();
  app.use(express.json());

  app.post('/api/login', function(req, res) {
    var username = req.body.username;
    var password = req.body.password;
    if (DASHBOARD_USERS[username] && DASHBOARD_USERS[username] === password) {
      var token = Buffer.from(username + ':' + password).toString('base64');
      return res.json({ token: token, username: username });
    }
    return res.status(401).json({ error: 'Invalid credentials' });
  });

  app.get('/api/today', checkAuth, async function(req, res) {
    try {
      var today = new Date().toISOString().split('T')[0];
      await db.computeDailySummary(today);
      var summaries = await db.getDailySummaries(today);
      var activePosts = await db.getActivePosts();

      // Add performance metrics to each summary
      summaries = summaries.map(function(s) {
        var tv = Number(s.total_views), tl = Number(s.total_likes), tc = Number(s.total_comments), ts = Number(s.total_shares), pc = Number(s.post_count);
        s.avg_views = pc > 0 ? Math.round(tv / pc) : 0;
        s.total_score = tl + tc * 3 + ts * 5;
        s.avg_score = pc > 0 ? Math.round(s.total_score / pc) : 0;
        s.engagement_rate = tv > 0 ? ((tl + tc) / tv * 100).toFixed(1) : '0.0';
        s.badge = s.avg_views >= 2000 ? 'top' : s.avg_views >= 500 ? 'bon' : 'faible';
        return s;
      });

      res.json({ date: today, summaries: summaries, activePosts: activePosts.length });
    } catch(err) { res.status(500).json({ error: err.message }); }
  });

  app.get('/api/stats/:date', checkAuth, async function(req, res) {
    try {
      var date = req.params.date;
      await db.computeDailySummary(date);
      var summaries = await db.getDailySummaries(date);
      res.json({ date: date, summaries: summaries });
    } catch(err) { res.status(500).json({ error: err.message }); }
  });

  app.get('/api/va/:discordId', checkAuth, async function(req, res) {
    try {
      var date = req.query.date || new Date().toISOString().split('T')[0];
      var posts = await db.getVaPostsToday(req.params.discordId, date);
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
      var stats = await db.getVaDailyStats(req.params.discordId, date);
      res.json({ va_id: req.params.discordId, date: date, posts: snapshots, stats: stats });
    } catch(err) { res.status(500).json({ error: err.message }); }
  });

  app.get('/api/history/:days', checkAuth, async function(req, res) {
    try {
      var days = parseInt(req.params.days) || 7;
      var results = [];
      for (var i = 0; i < days; i++) {
        var d = new Date();
        d.setDate(d.getDate() - i);
        var date = d.toISOString().split('T')[0];
        await db.computeDailySummary(date);
        var summaries = await db.getDailySummaries(date);
        results.push({ date: date, summaries: summaries });
      }
      res.json({ days: days, history: results });
    } catch(err) { res.status(500).json({ error: err.message }); }
  });

  app.get('/api/leaderboard', checkAuth, async function(req, res) {
    try {
      var date = req.query.date || new Date().toISOString().split('T')[0];
      await db.computeDailySummary(date);
      var rankings = await db.getLeaderboard(date);

      rankings = rankings.map(function(r) {
        var tv = Number(r.total_views), tl = Number(r.total_likes), tc = Number(r.total_comments), ts = Number(r.total_shares), pc = Number(r.post_count);
        r.avg_views = pc > 0 ? Math.round(tv / pc) : 0;
        r.total_score = tl + tc * 3 + ts * 5;
        r.engagement_rate = tv > 0 ? ((tl + tc) / tv * 100).toFixed(1) : '0.0';
        r.badge = r.avg_views >= 2000 ? 'top' : r.avg_views >= 500 ? 'bon' : 'faible';
        return r;
      });

      res.json({ date: date, rankings: rankings });
    } catch(err) { res.status(500).json({ error: err.message }); }
  });

  app.get('/api/compare', checkAuth, async function(req, res) {
    try {
      var va1 = req.query.va1;
      var va2 = req.query.va2;
      var days = parseInt(req.query.days) || 7;
      var result = { va1: [], va2: [] };
      for (var i = 0; i < days; i++) {
        var d = new Date();
        d.setDate(d.getDate() - i);
        var date = d.toISOString().split('T')[0];
        var s1 = await db.getVaDailyStats(va1, date);
        var s2 = await db.getVaDailyStats(va2, date);
        result.va1.push({ date: date, stats: s1 || null });
        result.va2.push({ date: date, stats: s2 || null });
      }
      res.json(result);
    } catch(err) { res.status(500).json({ error: err.message }); }
  });

  app.get('/api/top-posts', checkAuth, async function(req, res) {
    try {
      var date = req.query.date || new Date().toISOString().split('T')[0];
      var posts = await db.getTopPostsWithPerformance(date);

      posts = posts.map(function(p) {
        p.score = calcScore(p);
        p.engagement = calcEngagement(p);
        p.perf = getPerf(Number(p.views) || 0);
        return p;
      });

      res.json({ date: date, posts: posts });
    } catch(err) { res.status(500).json({ error: err.message }); }
  });

  // Performance thresholds info
  app.get('/api/thresholds', checkAuth, function(req, res) {
    res.json({
      viral: parseInt(process.env.VIRAL_VIEWS || '5000'),
      bon: parseInt(process.env.BON_VIEWS || '1000'),
      moyen: parseInt(process.env.MOYEN_VIEWS || '300'),
    });
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
