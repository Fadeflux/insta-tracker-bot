const { Pool } = require('pg');
const config = require('../../config');
const logger = require('../utils/logger');

const pool = new Pool({ connectionString: config.db.url });

const SCHEMA = `
CREATE TABLE IF NOT EXISTS posts (
  id            SERIAL PRIMARY KEY,
  ig_post_id    VARCHAR(64) UNIQUE,
  url           TEXT NOT NULL,
  va_discord_id VARCHAR(32) NOT NULL,
  va_name       VARCHAR(128) NOT NULL,
  created_at    TIMESTAMPTZ NOT NULL DEFAULT NOW(),
  tracking_end  TIMESTAMPTZ,
  status        VARCHAR(16) NOT NULL DEFAULT 'active'
    CHECK (status IN ('active', 'ended', 'error')),
  manager_msg_id VARCHAR(32)
);

CREATE TABLE IF NOT EXISTS snapshots (
  id         SERIAL PRIMARY KEY,
  post_id    INTEGER NOT NULL REFERENCES posts(id) ON DELETE CASCADE,
  views      INTEGER DEFAULT 0,
  likes      INTEGER DEFAULT 0,
  comments   INTEGER DEFAULT 0,
  shares     INTEGER DEFAULT 0,
  scraped_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
  error      TEXT
);

CREATE TABLE IF NOT EXISTS daily_summaries (
  id           SERIAL PRIMARY KEY,
  va_discord_id VARCHAR(32) NOT NULL,
  va_name       VARCHAR(128) NOT NULL,
  date          DATE NOT NULL,
  post_count    INTEGER DEFAULT 0,
  total_views   BIGINT DEFAULT 0,
  total_likes   BIGINT DEFAULT 0,
  total_comments BIGINT DEFAULT 0,
  total_shares   BIGINT DEFAULT 0,
  UNIQUE (va_discord_id, date)
);

CREATE INDEX IF NOT EXISTS idx_posts_status ON posts(status);
CREATE INDEX IF NOT EXISTS idx_posts_va ON posts(va_discord_id);
CREATE INDEX IF NOT EXISTS idx_posts_created ON posts(created_at);
CREATE INDEX IF NOT EXISTS idx_snapshots_post ON snapshots(post_id);
CREATE INDEX IF NOT EXISTS idx_snapshots_time ON snapshots(scraped_at);
CREATE INDEX IF NOT EXISTS idx_daily_date ON daily_summaries(date);
`;

const MIGRATIONS = `
DO $$ BEGIN
  -- === EXISTING MIGRATIONS (from v1) ===
  IF NOT EXISTS (SELECT 1 FROM information_schema.columns WHERE table_name='posts' AND column_name='caption') THEN
    ALTER TABLE posts ADD COLUMN caption TEXT;
  END IF;
  IF NOT EXISTS (SELECT 1 FROM information_schema.columns WHERE table_name='posts' AND column_name='post_type') THEN
    ALTER TABLE posts ADD COLUMN post_type VARCHAR(16) DEFAULT 'reel';
  END IF;
  IF NOT EXISTS (SELECT 1 FROM information_schema.columns WHERE table_name='posts' AND column_name='performance') THEN
    ALTER TABLE posts ADD COLUMN performance VARCHAR(16) DEFAULT 'pending';
  END IF;
  IF NOT EXISTS (SELECT 1 FROM information_schema.columns WHERE table_name='posts' AND column_name='platform') THEN
    ALTER TABLE posts ADD COLUMN platform VARCHAR(16) DEFAULT 'instagram';
  END IF;

  -- === V2 MIGRATIONS: Multi-platform support ===

  -- Add platform to daily_summaries
  IF NOT EXISTS (SELECT 1 FROM information_schema.columns WHERE table_name='daily_summaries' AND column_name='platform') THEN
    ALTER TABLE daily_summaries ADD COLUMN platform VARCHAR(16) DEFAULT 'instagram';
  END IF;

  -- Add retweets column for Twitter (maps to shares for IG)
  IF NOT EXISTS (SELECT 1 FROM information_schema.columns WHERE table_name='snapshots' AND column_name='retweets') THEN
    ALTER TABLE snapshots ADD COLUMN retweets INTEGER DEFAULT 0;
  END IF;

  -- Add quote_tweets column for Twitter
  IF NOT EXISTS (SELECT 1 FROM information_schema.columns WHERE table_name='snapshots' AND column_name='quote_tweets') THEN
    ALTER TABLE snapshots ADD COLUMN quote_tweets INTEGER DEFAULT 0;
  END IF;

  -- Add bookmarks column (Twitter bookmarks)
  IF NOT EXISTS (SELECT 1 FROM information_schema.columns WHERE table_name='snapshots' AND column_name='bookmarks') THEN
    ALTER TABLE snapshots ADD COLUMN bookmarks INTEGER DEFAULT 0;
  END IF;

  -- Add guild_id to posts (to know which Discord server the post came from)
  IF NOT EXISTS (SELECT 1 FROM information_schema.columns WHERE table_name='posts' AND column_name='guild_id') THEN
    ALTER TABLE posts ADD COLUMN guild_id VARCHAR(32);
  END IF;

  -- Add posted_at: actual publication time as scraped from the platform.
  -- Different from created_at which is when the VA submitted the link to the bot.
  -- The delay (created_at - posted_at) tells us if the VA reported their post on time.
  IF NOT EXISTS (SELECT 1 FROM information_schema.columns WHERE table_name='posts' AND column_name='posted_at') THEN
    ALTER TABLE posts ADD COLUMN posted_at TIMESTAMPTZ;
  END IF;

  -- Add link_delay_minutes: cached delay in minutes between posted_at and created_at.
  -- NULL = posted_at unknown. Computed once on scrape.
  IF NOT EXISTS (SELECT 1 FROM information_schema.columns WHERE table_name='posts' AND column_name='link_delay_minutes') THEN
    ALTER TABLE posts ADD COLUMN link_delay_minutes INTEGER;
  END IF;

  -- Add late_alert_sent: avoid sending the >2h delay DM more than once per post.
  IF NOT EXISTS (SELECT 1 FROM information_schema.columns WHERE table_name='posts' AND column_name='late_alert_sent') THEN
    ALTER TABLE posts ADD COLUMN late_alert_sent BOOLEAN DEFAULT FALSE;
  END IF;

  -- Drop and recreate the accounts.platform CHECK constraint to allow 'threads'.
  -- Older deployments created the table without 'threads' in the allowed list.
  IF EXISTS (
    SELECT 1 FROM information_schema.table_constraints
    WHERE table_name = 'accounts' AND constraint_name = 'accounts_platform_check'
  ) THEN
    ALTER TABLE accounts DROP CONSTRAINT accounts_platform_check;
    ALTER TABLE accounts ADD CONSTRAINT accounts_platform_check
      CHECK (platform IN ('instagram', 'twitter', 'geelark', 'threads'));
  END IF;

  -- Soft delete columns: when deleted_at IS NOT NULL, the post is hidden from
  -- normal queries but still in the database (recoverable). deleted_by stores
  -- who pressed the delete button (for audit / "deleted by X on Y" display).
  IF NOT EXISTS (SELECT 1 FROM information_schema.columns WHERE table_name='posts' AND column_name='deleted_at') THEN
    ALTER TABLE posts ADD COLUMN deleted_at TIMESTAMPTZ;
  END IF;
  IF NOT EXISTS (SELECT 1 FROM information_schema.columns WHERE table_name='posts' AND column_name='deleted_by') THEN
    ALTER TABLE posts ADD COLUMN deleted_by VARCHAR(64);
  END IF;
  CREATE INDEX IF NOT EXISTS idx_posts_deleted_at ON posts(deleted_at);

  -- dashboard_users.platform: relax constraint to allow 'threads' and
  -- comma-separated combos like 'instagram,threads,geelark'. The original
  -- definition was VARCHAR(16) CHECK (platform IN (4 values)), which rejects
  -- 'threads' AND any multi-platform combo. We widen the column and drop
  -- the CHECK so the application layer is the source of truth for validation.
  IF EXISTS (SELECT 1 FROM information_schema.columns WHERE table_name='dashboard_users' AND column_name='platform') THEN
    -- Widen to 64 chars so combos like 'instagram,twitter,geelark,threads' fit
    BEGIN
      ALTER TABLE dashboard_users ALTER COLUMN platform TYPE VARCHAR(64);
    EXCEPTION WHEN OTHERS THEN NULL;
    END;
    -- Drop the CHECK constraint by name (matches what Postgres auto-generates)
    BEGIN
      ALTER TABLE dashboard_users DROP CONSTRAINT IF EXISTS dashboard_users_platform_check;
    EXCEPTION WHEN OTHERS THEN NULL;
    END;
  END IF;

END $$;

-- Idempotency table for slot reminders (prevents double-sending if cron fires twice)
CREATE TABLE IF NOT EXISTS slot_reminders_log (
  reminder_key   VARCHAR(64) PRIMARY KEY,  -- e.g. "morning_2026-04-25" or "morning_late_2026-04-25"
  sent_at        TIMESTAMPTZ DEFAULT NOW()
);
CREATE INDEX IF NOT EXISTS idx_slot_reminders_sent ON slot_reminders_log(sent_at);

-- === STREAKS TABLE (from v1) ===
CREATE TABLE IF NOT EXISTS va_streaks (
  va_discord_id VARCHAR(32) PRIMARY KEY,
  va_name       VARCHAR(128) NOT NULL,
  current_streak INTEGER DEFAULT 0,
  best_streak    INTEGER DEFAULT 0,
  last_streak_date DATE,
  updated_at     TIMESTAMPTZ DEFAULT NOW()
);

-- === V2: Add platform to va_streaks ===
DO $$ BEGIN
  IF NOT EXISTS (SELECT 1 FROM information_schema.columns WHERE table_name='va_streaks' AND column_name='platform') THEN
    ALTER TABLE va_streaks ADD COLUMN platform VARCHAR(16) DEFAULT 'instagram';
    -- Drop old PK and recreate as composite
    ALTER TABLE va_streaks DROP CONSTRAINT IF EXISTS va_streaks_pkey;
    ALTER TABLE va_streaks ADD PRIMARY KEY (va_discord_id, platform);
  END IF;
END $$;

-- === V2: USER PERMISSIONS TABLE ===
CREATE TABLE IF NOT EXISTS user_permissions (
  id            SERIAL PRIMARY KEY,
  discord_id    VARCHAR(32) NOT NULL,
  platform      VARCHAR(16) NOT NULL CHECK (platform IN ('instagram', 'twitter', 'geelark', 'all')),
  role          VARCHAR(16) NOT NULL DEFAULT 'va' CHECK (role IN ('admin', 'manager', 'va')),
  granted_by    VARCHAR(32),
  created_at    TIMESTAMPTZ NOT NULL DEFAULT NOW(),
  UNIQUE (discord_id, platform)
);

-- === V2: DASHBOARD SESSIONS TABLE (for platform-aware web auth) ===
CREATE TABLE IF NOT EXISTS dashboard_users (
  id            SERIAL PRIMARY KEY,
  username      VARCHAR(64) UNIQUE NOT NULL,
  password_hash VARCHAR(255) NOT NULL,
  role          VARCHAR(16) NOT NULL DEFAULT 'va' CHECK (role IN ('admin', 'manager', 'va')),
  platform      VARCHAR(64) NOT NULL DEFAULT 'all',
  created_at    TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

-- === INDEXES for v2 ===
CREATE INDEX IF NOT EXISTS idx_posts_perf ON posts(performance);
CREATE INDEX IF NOT EXISTS idx_posts_platform ON posts(platform);
CREATE INDEX IF NOT EXISTS idx_posts_guild ON posts(guild_id);
CREATE INDEX IF NOT EXISTS idx_daily_platform ON daily_summaries(platform);
CREATE INDEX IF NOT EXISTS idx_perms_discord ON user_permissions(discord_id);
CREATE INDEX IF NOT EXISTS idx_perms_platform ON user_permissions(platform);
CREATE INDEX IF NOT EXISTS idx_streaks_platform ON va_streaks(platform);

-- === UPDATE daily_summaries UNIQUE constraint to include platform ===
DO $$ BEGIN
  -- Check if old unique constraint exists without platform
  IF EXISTS (
    SELECT 1 FROM pg_constraint 
    WHERE conname = 'daily_summaries_va_discord_id_date_key'
    AND conrelid = 'daily_summaries'::regclass
  ) THEN
    ALTER TABLE daily_summaries DROP CONSTRAINT daily_summaries_va_discord_id_date_key;
    ALTER TABLE daily_summaries ADD CONSTRAINT daily_summaries_va_discord_id_date_platform_key 
      UNIQUE (va_discord_id, date, platform);
  END IF;
END $$;

-- === V3: ACCOUNTS TABLE (tracking per IG/Twitter account) ===
CREATE TABLE IF NOT EXISTS accounts (
  id            SERIAL PRIMARY KEY,
  username      VARCHAR(128) NOT NULL,
  platform      VARCHAR(16) NOT NULL CHECK (platform IN ('instagram', 'twitter', 'geelark', 'threads')),
  va_discord_id VARCHAR(32),
  va_name       VARCHAR(128),
  status        VARCHAR(16) NOT NULL DEFAULT 'active' CHECK (status IN ('active', 'inactive')),
  first_seen_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
  last_seen_at  TIMESTAMPTZ NOT NULL DEFAULT NOW(),
  created_at    TIMESTAMPTZ NOT NULL DEFAULT NOW(),
  UNIQUE (username, platform)
);

CREATE INDEX IF NOT EXISTS idx_accounts_platform ON accounts(platform);
CREATE INDEX IF NOT EXISTS idx_accounts_va ON accounts(va_discord_id);
CREATE INDEX IF NOT EXISTS idx_accounts_status ON accounts(status);
CREATE INDEX IF NOT EXISTS idx_accounts_last_seen ON accounts(last_seen_at);

-- === V3: Add account_id to posts ===
DO $$ BEGIN
  IF NOT EXISTS (SELECT 1 FROM information_schema.columns WHERE table_name='posts' AND column_name='account_id') THEN
    ALTER TABLE posts ADD COLUMN account_id INTEGER REFERENCES accounts(id) ON DELETE SET NULL;
  END IF;
  IF NOT EXISTS (SELECT 1 FROM information_schema.columns WHERE table_name='posts' AND column_name='account_username') THEN
    ALTER TABLE posts ADD COLUMN account_username VARCHAR(128);
  END IF;
END $$;

CREATE INDEX IF NOT EXISTS idx_posts_account ON posts(account_id);
CREATE INDEX IF NOT EXISTS idx_posts_account_username ON posts(account_username);

-- === V4: GAMIFICATION TABLES ===

-- Points earned per day per VA (10 for #1, 6 for #2, 3 for #3).
-- Aggregated weekly to declare a winner.
CREATE TABLE IF NOT EXISTS va_points (
  id             SERIAL PRIMARY KEY,
  va_discord_id  VARCHAR(32) NOT NULL,
  va_name        VARCHAR(128) NOT NULL,
  platform       VARCHAR(16) NOT NULL DEFAULT 'instagram',
  date           DATE NOT NULL,
  rank           INTEGER NOT NULL,
  points         INTEGER NOT NULL,
  total_views    BIGINT DEFAULT 0,
  created_at     TIMESTAMPTZ NOT NULL DEFAULT NOW(),
  UNIQUE (va_discord_id, date, platform)
);

CREATE INDEX IF NOT EXISTS idx_points_date ON va_points(date);
CREATE INDEX IF NOT EXISTS idx_points_va ON va_points(va_discord_id);
CREATE INDEX IF NOT EXISTS idx_points_platform ON va_points(platform);

-- Weekly winners archive. One row per (week, platform).
CREATE TABLE IF NOT EXISTS weekly_winners (
  id              SERIAL PRIMARY KEY,
  week_start      DATE NOT NULL,
  week_end        DATE NOT NULL,
  platform        VARCHAR(16) NOT NULL,
  va_discord_id   VARCHAR(32) NOT NULL,
  va_name         VARCHAR(128) NOT NULL,
  total_points    INTEGER NOT NULL,
  total_views     BIGINT DEFAULT 0,
  total_posts     INTEGER DEFAULT 0,
  announced_at    TIMESTAMPTZ NOT NULL DEFAULT NOW(),
  UNIQUE (week_start, platform)
);

CREATE INDEX IF NOT EXISTS idx_winners_platform ON weekly_winners(platform);

-- Weekly 1v1 duels. Paired randomly every Monday, resolved on Sunday.
CREATE TABLE IF NOT EXISTS duels (
  id             SERIAL PRIMARY KEY,
  week_start     DATE NOT NULL,
  week_end       DATE NOT NULL,
  platform       VARCHAR(16) NOT NULL,
  va1_discord_id VARCHAR(32) NOT NULL,
  va1_name       VARCHAR(128) NOT NULL,
  va2_discord_id VARCHAR(32) NOT NULL,
  va2_name       VARCHAR(128) NOT NULL,
  va1_views      BIGINT DEFAULT 0,
  va2_views      BIGINT DEFAULT 0,
  winner_id      VARCHAR(32),
  status         VARCHAR(16) NOT NULL DEFAULT 'active' CHECK (status IN ('active', 'resolved')),
  created_at     TIMESTAMPTZ NOT NULL DEFAULT NOW(),
  resolved_at    TIMESTAMPTZ
);

CREATE INDEX IF NOT EXISTS idx_duels_status ON duels(status);
CREATE INDEX IF NOT EXISTS idx_duels_platform ON duels(platform);
CREATE INDEX IF NOT EXISTS idx_duels_week ON duels(week_start);

-- === V5: link dashboard users to Discord IDs (needed for /me page) ===
DO $$ BEGIN
  IF NOT EXISTS (SELECT 1 FROM information_schema.columns WHERE table_name='dashboard_users' AND column_name='discord_id') THEN
    ALTER TABLE dashboard_users ADD COLUMN discord_id VARCHAR(32);
  END IF;
END $$;
CREATE INDEX IF NOT EXISTS idx_dashboard_users_discord ON dashboard_users(discord_id);

-- === V6: auto-revoke when a user leaves Discord or loses their role ===
DO $$ BEGIN
  IF NOT EXISTS (SELECT 1 FROM information_schema.columns WHERE table_name='dashboard_users' AND column_name='status') THEN
    ALTER TABLE dashboard_users ADD COLUMN status VARCHAR(16) NOT NULL DEFAULT 'active' CHECK (status IN ('active', 'revoked'));
  END IF;
  IF NOT EXISTS (SELECT 1 FROM information_schema.columns WHERE table_name='dashboard_users' AND column_name='revoked_at') THEN
    ALTER TABLE dashboard_users ADD COLUMN revoked_at TIMESTAMPTZ;
  END IF;
  IF NOT EXISTS (SELECT 1 FROM information_schema.columns WHERE table_name='dashboard_users' AND column_name='revoked_reason') THEN
    ALTER TABLE dashboard_users ADD COLUMN revoked_reason TEXT;
  END IF;
  IF NOT EXISTS (SELECT 1 FROM information_schema.columns WHERE table_name='dashboard_users' AND column_name='last_check_at') THEN
    ALTER TABLE dashboard_users ADD COLUMN last_check_at TIMESTAMPTZ;
  END IF;

  -- Migration: extend tracking_end to 7 days for posts created in the last 7
  -- days. Catches up posts that were created when the window was 72h, so we
  -- don't lose tracking on posts still in their growth phase. Only updates
  -- posts whose tracking_end is shorter than the new 7-day standard.
  UPDATE posts
  SET tracking_end = created_at + INTERVAL '7 days',
      status = 'active'
  WHERE created_at >= NOW() - INTERVAL '7 days'
    AND deleted_at IS NULL
    AND tracking_end < created_at + INTERVAL '7 days';

  -- In-app notifications. Stored per platform so the dashboard can filter on
  -- whatever the user is currently viewing. We don't need them to be very
  -- long-lived: a TTL of ~14 days is enough; older ones can be pruned.
  CREATE TABLE IF NOT EXISTS notifications (
    id          SERIAL PRIMARY KEY,
    platform    VARCHAR(16) NOT NULL,
    kind        VARCHAR(32) NOT NULL,        -- 'viral_confirmed' | 'fast_growth'
    post_id     INTEGER REFERENCES posts(id) ON DELETE CASCADE,
    va_name     VARCHAR(128),
    title       VARCHAR(255),
    body        TEXT,
    url         TEXT,
    metadata    JSONB,
    created_at  TIMESTAMPTZ NOT NULL DEFAULT NOW()
  );
  CREATE INDEX IF NOT EXISTS idx_notif_platform_time ON notifications(platform, created_at DESC);
  CREATE INDEX IF NOT EXISTS idx_notif_post ON notifications(post_id);

  -- Track which viral thresholds have already been announced in the VA's
  -- ticket channel. We only fire once per (post, threshold) so the manager
  -- isn't spammed with duplicate alerts on every scrape that confirms the
  -- view count is still above the threshold. Keyed on (post_id, threshold)
  -- so each milestone (8k, 20k, 50k, 100k) is announced exactly once.
  CREATE TABLE IF NOT EXISTS post_viral_milestones_sent (
    post_id     INTEGER NOT NULL REFERENCES posts(id) ON DELETE CASCADE,
    threshold   INTEGER NOT NULL,
    sent_at     TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    PRIMARY KEY (post_id, threshold)
  );

  -- Track which account-level alerts have been sent in the VA's ticket and
  -- with what severity, so we can re-notify only when the situation gets
  -- WORSE (e.g. dead-account: 5 failed posts → escalates to 10 failed posts).
  -- 'kind' is one of: 'dead_account', 'shadowban', 'concentrated_views'.
  -- 'severity' is a numeric measure that goes up as things deteriorate
  -- (e.g. count of failed posts, or % of views concentrated on one account).
  CREATE TABLE IF NOT EXISTS account_alerts_sent (
    account_id    INTEGER NOT NULL REFERENCES accounts(id) ON DELETE CASCADE,
    kind          VARCHAR(32) NOT NULL,
    last_severity INTEGER NOT NULL DEFAULT 0,
    sent_at       TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    PRIMARY KEY (account_id, kind)
  );

  -- VA-level alerts dedup. The account_alerts_sent table is keyed on
  -- account_id with a foreign-key to accounts(id), which means we can't
  -- piggyback on it for VA-aggregate alerts (e.g. inactivity, weekly stats
  -- reminders). This dedicated table stores the last-sent state for VA-level
  -- alert kinds, keyed on (va_discord_id, kind).
  CREATE TABLE IF NOT EXISTS va_alerts_sent (
    va_discord_id VARCHAR(64) NOT NULL,
    kind          VARCHAR(64) NOT NULL,
    last_severity INTEGER NOT NULL DEFAULT 0,
    sent_at       TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    PRIMARY KEY (va_discord_id, kind)
  );

  -- Tracks which badges each VA currently has. Keyed on (va_discord_id, kind).
  -- Earned badges expire after 14 days without earning ANY new badge — this
  -- pushes VAs to keep performing. The badges module updates last_earned_at
  -- whenever the VA gains a new badge of any type.
  -- 'kind' is one of: 'top1', 'firstViral', 'viral10', 'regularity', 'record100k'.
  CREATE TABLE IF NOT EXISTS va_badges (
    va_discord_id   VARCHAR(64) NOT NULL,
    kind            VARCHAR(32) NOT NULL,
    earned_at       TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    metadata        JSONB,                 -- e.g. { peak_views: 124000, month: '2026-05' }
    PRIMARY KEY (va_discord_id, kind)
  );

  -- One row per VA, tracking when they last earned ANY badge. Used to
  -- expire all badges if the VA has been idle for 14 days.
  CREATE TABLE IF NOT EXISTS va_badge_activity (
    va_discord_id     VARCHAR(64) PRIMARY KEY,
    last_earned_at    TIMESTAMPTZ NOT NULL DEFAULT NOW()
  );

  -- Per-account override for the "day J" calculation. By default the day is
  -- counted from the first tracked post on the account, but VAs sometimes
  -- forget to send the first link(s) — so an admin can adjust the start
  -- date here. If start_date is set, J1 is that date; J2 is the next day,
  -- and so on. NULL = use the natural first-post date.
  CREATE TABLE IF NOT EXISTS account_day_overrides (
    account_id   INTEGER PRIMARY KEY REFERENCES accounts(id) ON DELETE CASCADE,
    start_date   DATE,
    updated_by   VARCHAR(128),
    updated_at   TIMESTAMPTZ NOT NULL DEFAULT NOW()
  );

  -- Shadowban state for the daily-objectives flow. When a shadowban is detected,
  -- we mark the account "in_rest" with shadowban_at = today. The morning
  -- objective routine then says "rest day J1..J7" and from J8 onwards
  -- "ramp-up J1=1 post / J2=2 posts / J3+=3 posts (normal)".
  -- After J10 of ramp-up (= J17 since shadowban) we clear the row and
  -- the account returns to its standard day calculation.
  CREATE TABLE IF NOT EXISTS account_shadowban_state (
    account_id     INTEGER PRIMARY KEY REFERENCES accounts(id) ON DELETE CASCADE,
    shadowban_at   DATE NOT NULL,
    detected_at    TIMESTAMPTZ NOT NULL DEFAULT NOW()
  );

  -- Per-user read state. We don't want to spam every user with the same notif
  -- counter, so each dashboard user has their own "last read" timestamp per
  -- platform. Notifs newer than this timestamp count as unread.
  CREATE TABLE IF NOT EXISTS notification_reads (
    username    VARCHAR(128) NOT NULL,
    platform    VARCHAR(16) NOT NULL,
    last_read_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    PRIMARY KEY (username, platform)
  );
END $$;
CREATE INDEX IF NOT EXISTS idx_dashboard_users_status ON dashboard_users(status);

-- === V7: viral post notifications (tracks which posts we've already congratulated) ===
CREATE TABLE IF NOT EXISTS viral_notifications (
  id             SERIAL PRIMARY KEY,
  post_id        INTEGER NOT NULL REFERENCES posts(id) ON DELETE CASCADE,
  va_discord_id  VARCHAR(32) NOT NULL,
  threshold      INTEGER NOT NULL,
  views_at_notif BIGINT NOT NULL,
  notified_at    TIMESTAMPTZ NOT NULL DEFAULT NOW(),
  UNIQUE (post_id, threshold)
);
CREATE INDEX IF NOT EXISTS idx_viral_notif_post ON viral_notifications(post_id);
CREATE INDEX IF NOT EXISTS idx_viral_notif_va ON viral_notifications(va_discord_id);

-- === V8: track DM delivery status per VA ===
-- Updated each time sendVaDM() is called. Lets admins see at a glance who
-- has their Discord DMs enabled (and who needs reminding).
CREATE TABLE IF NOT EXISTS va_dm_status (
  discord_id      VARCHAR(32) PRIMARY KEY,
  va_name         VARCHAR(255),
  last_ok_at      TIMESTAMPTZ,
  last_fail_at    TIMESTAMPTZ,
  last_fail_reason TEXT,
  total_ok        INTEGER NOT NULL DEFAULT 0,
  total_fail      INTEGER NOT NULL DEFAULT 0,
  updated_at      TIMESTAMPTZ NOT NULL DEFAULT NOW()
);
`;

async function initDb() {
  try {
    await pool.query(SCHEMA);
    await pool.query(MIGRATIONS);
    logger.info('Database schema initialized (v7 viral notifications)');
  } catch (err) {
    logger.error('Database init failed', { error: err.message });
    throw err;
  }
}

if (require.main === module) {
  initDb()
    .then(function() { console.log('Database initialized (v2)'); process.exit(0); })
    .catch(function(err) { console.error('Init failed:', err.message); process.exit(1); });
}

module.exports = { pool, initDb };
