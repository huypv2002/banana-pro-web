-- D1 Database Schema for Banana Pro
DROP TABLE IF EXISTS gen_history;
DROP TABLE IF EXISTS user_cookies;
DROP TABLE IF EXISTS sessions;
DROP TABLE IF EXISTS users;

CREATE TABLE users (
  id INTEGER PRIMARY KEY AUTOINCREMENT,
  username TEXT UNIQUE NOT NULL,
  password_hash TEXT NOT NULL,
  role TEXT DEFAULT 'user' CHECK(role IN ('admin','user')),
  disabled INTEGER DEFAULT 0,
  cookie_quota INTEGER DEFAULT 5,
  plan_expires_at TEXT DEFAULT NULL,
  created_at TEXT DEFAULT (datetime('now'))
);

CREATE TABLE sessions (
  token TEXT PRIMARY KEY,
  user_id INTEGER NOT NULL,
  expires_at TEXT NOT NULL,
  FOREIGN KEY(user_id) REFERENCES users(id) ON DELETE CASCADE
);

CREATE TABLE user_cookies (
  id INTEGER PRIMARY KEY AUTOINCREMENT,
  user_id INTEGER NOT NULL,
  cookie_raw TEXT NOT NULL,
  cookie_hash TEXT NOT NULL,
  email TEXT DEFAULT '',
  status TEXT DEFAULT 'pending',
  created_at TEXT DEFAULT (datetime('now')),
  FOREIGN KEY(user_id) REFERENCES users(id) ON DELETE CASCADE,
  UNIQUE(user_id, cookie_hash)
);

CREATE TABLE gen_history (
  id INTEGER PRIMARY KEY AUTOINCREMENT,
  user_id INTEGER NOT NULL,
  job_id TEXT,
  prompt TEXT,
  model TEXT,
  image_url TEXT,
  media_url TEXT,
  media_type TEXT DEFAULT 'image',
  error TEXT,
  batch_name TEXT DEFAULT '',
  file_name TEXT DEFAULT '',
  created_at TEXT DEFAULT (datetime('now')),
  FOREIGN KEY(user_id) REFERENCES users(id) ON DELETE CASCADE
);

CREATE INDEX idx_history_user_media_job ON gen_history(user_id, media_type, job_id, id DESC);
CREATE INDEX idx_history_user_media_file ON gen_history(user_id, media_type, job_id, file_name, id DESC);
CREATE INDEX idx_history_cleanup ON gen_history(created_at);

-- Default super admin app-account: adminveo / 30102002
INSERT INTO users (username, password_hash, role) VALUES ('adminveo', 'cef380b2c74489696d94000c79718ce6da5674ca2041c002a5cedd0f27826933', 'admin');
