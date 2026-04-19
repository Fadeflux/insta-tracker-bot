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

async function initDb() {
  try {
    await pool.query(SCHEMA);
    logger.info('Database schema initialized');
  } catch (err) {
    logger.error('Database init failed', { error: err.message });
    throw err;
  }
}

if (require.main === module) {
  initDb()
    .then(() => { console.log('Database initialized'); process.exit(0); })
    .catch((err) => { console.error('Init failed:', err.message); process.exit(1); });
}

module.exports = { pool, initDb };
