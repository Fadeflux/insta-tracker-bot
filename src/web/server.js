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
          discordId: row.discord_id || null,
        };
        console.log('[Users] DB user loaded: ' + row.username + ' role=' + row.role + (row.discord_id ? ' discord_id=' + row.discord_id : ''));
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
      } else if (user.platform.indexOf(',') !== -1) {
        allowedPlatforms = user.platform.split(',');
      } else {
        allowedPlatforms = [user.platform];
      }
      console.log('[Login] Success: ' + username + ' role=' + user.role + ' platform=' + user.platform);
      return res.json({
        token: token,
        username: username,
        role: user.role,
        platform: user.platform,
        discordId: user.discordId || null,
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
    } else if (user.platform.indexOf(',') !== -1) {
      allowedPlatforms = user.platform.split(',');
    } else {
      allowedPlatforms = [user.platform];
    }
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
      // Enrich with a small derived field: days since last post.
      var now = Date.now();
      accounts = accounts.map(function(a) {
        var ref = a.last_post_at || a.last_seen_at;
        a.days_since_last_post = ref ? Math.floor((now - new Date(ref).getTime()) / (1000 * 60 * 60 * 24)) : null;
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

  // List all dashboard users
  app.get('/api/admin/users', checkAuth, checkAdmin, function(req, res) {
    var users = Object.keys(DASHBOARD_USERS).map(function(u) {
      return { username: u, role: DASHBOARD_USERS[u].role, platform: DASHBOARD_USERS[u].platform, discordId: DASHBOARD_USERS[u].discordId || null };
    });
    res.json({ users: users });
  });

  // Create or update a dashboard user
  app.post('/api/admin/users', checkAuth, checkAdmin, function(req, res) {
    var username = (req.body.username || '').trim().toLowerCase();
    var password = req.body.password || '';
    var role = req.body.role || 'va';
    var platform = req.body.platform || 'all';
    var discordId = (req.body.discord_id || req.body.discordId || '').trim() || null;

    if (!username || username.length < 2) return res.status(400).json({ error: 'Username trop court (min 2 caracteres)' });
    if (!password || password.length < 4) return res.status(400).json({ error: 'Mot de passe trop court (min 4 caracteres)' });
    if (['admin', 'manager', 'va'].indexOf(role) === -1) return res.status(400).json({ error: 'Role invalide (admin, manager, va)' });
    if (['all', 'instagram', 'twitter', 'geelark'].indexOf(platform) === -1) {
      // Check if comma-separated combo like "instagram,geelark"
      var platParts = platform.split(',');
      var validPlats = ['instagram', 'twitter', 'geelark'];
      var allValid = platParts.every(function(p) { return validPlats.indexOf(p) !== -1; });
      if (!allValid) return res.status(400).json({ error: 'Plateforme invalide' });
    }
    if (discordId && !/^\d{17,20}$/.test(discordId)) {
      return res.status(400).json({ error: 'Discord ID doit faire 17-20 chiffres' });
    }

    var isNew = !DASHBOARD_USERS[username];
    DASHBOARD_USERS[username] = { password: password, role: role, platform: platform, discordId: discordId };

    // Also save to DB for persistence across restarts
    db.upsertDashboardUser(username, password, role, platform, discordId).catch(function(e) {
      console.error('Failed to save user to DB:', e.message);
    });

    console.log('[Admin] User ' + (isNew ? 'created' : 'updated') + ': ' + username + ' (' + role + '/' + platform + ')' + (discordId ? ' discord_id=' + discordId : ''));
    res.json({ success: true, action: isNew ? 'created' : 'updated', username: username, role: role, platform: platform, discordId: discordId });
  });

  // Update user platform/role (without changing password)
  app.put('/api/admin/users/:username', checkAuth, checkAdmin, function(req, res) {
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

    DASHBOARD_USERS[username] = { password: password, role: role, platform: platform, discordId: discordId || null };

    db.upsertDashboardUser(username, password, role, platform, discordId || null).catch(function(e) {
      console.error('Failed to update user in DB:', e.message);
    });

    console.log('[Admin] User updated: ' + username + ' (' + role + '/' + platform + ')' + (discordId ? ' discord_id=' + discordId : ''));
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
