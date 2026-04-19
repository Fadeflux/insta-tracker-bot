var express = require('express');
var path = require('path');
var config = require('../../config');
var db = require('../db/queries');

var DASHBOARD_USERS = {};

// Load users from env: DASHBOARD_USERS=user1:pass1,user2:pass2
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

  // Token format: base64(username:password)
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

function createWebServer() {
  loadUsers();
  var app = express();
  app.use(express.json());

  // Login endpoint
  app.post('/api/login', function(req, res) {
    var username = req.body.username;
    var password = req.body.password;
    if (DASHBOARD_USERS[username] && DASHBOARD_USERS[username] === password) {
      var token = Buffer.from(username + ':' + password).toString('base64');
      return res.json({ token: token, username: username });
    }
    return res.status(401).json({ error: 'Invalid credentials' });
  });

  // API: Today stats
  app.get('/api/today', checkAuth, async function(req, res) {
    try {
      var today = new Date().toISOString().split('T')[0];
      await db.computeDailySummary(today);
      var summaries = await db.getDailySummaries(today);
      var activePosts = await db.getActivePosts();
      res.json({ date: today, summaries: summaries, activePosts: activePosts.length });
    } catch(err) {
      res.status(500).json({ error: err.message });
    }
  });

  // API: Specific date stats
  app.get('/api/stats/:date', checkAuth, async function(req, res) {
    try {
      var date = req.params.date;
      await db.computeDailySummary(date);
      var summaries = await db.getDailySummaries(date);
      res.json({ date: date, summaries: summaries });
    } catch(err) {
      res.status(500).json({ error: err.message });
    }
  });

  // API: VA detail
  app.get('/api/va/:discordId', checkAuth, async function(req, res) {
    try {
      var today = new Date().toISOString().split('T')[0];
      var posts = await db.getVaPostsToday(req.params.discordId, today);
      var snapshots = [];
      for (var i = 0; i < posts.length; i++) {
        var history = await db.getSnapshotHistory(posts[i].id);
        snapshots.push({ post: posts[i], snapshots: history });
      }
      res.json({ va_id: req.params.discordId, posts: snapshots });
    } catch(err) {
      res.status(500).json({ error: err.message });
    }
  });

  // API: History (last N days)
  app.get('/api/history/:days', checkAuth, async function(req, res) {
    try {
      var days = parseInt(req.params.days) || 7;
      var results = [];
      for (var i = 0; i < days; i++) {
        var d = new Date();
        d.setDate(d.getDate() - i);
        var date = d.toISOString().split('T')[0];
        var summaries = await db.getDailySummaries(date);
        results.push({ date: date, summaries: summaries });
      }
      res.json({ days: days, history: results });
    } catch(err) {
      res.status(500).json({ error: err.message });
    }
  });

  // API: Leaderboard
  app.get('/api/leaderboard', checkAuth, async function(req, res) {
    try {
      var date = req.query.date || new Date().toISOString().split('T')[0];
      await db.computeDailySummary(date);
      var rankings = await db.getLeaderboard(date);
      res.json({ date: date, rankings: rankings });
    } catch(err) {
      res.status(500).json({ error: err.message });
    }
  });

  // Serve dashboard HTML
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
