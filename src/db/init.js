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

END $$;

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
  platform      VARCHAR(16) NOT NULL DEFAULT 'all' CHECK (platform IN ('instagram', 'twitter', 'geelark', 'all')),
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
`;

async function initDb() {
  try {
    await pool.query(SCHEMA);
    await pool.query(MIGRATIONS);
    logger.info('Database schema initialized (v2 multi-platform)');
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
