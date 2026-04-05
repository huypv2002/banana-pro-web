"""SQLite database for users, cookies, generation history."""
import sqlite3, os, hashlib, secrets, time

DB_PATH = os.environ.get("DB_PATH", "banana_pro.db")

def get_db():
    conn = sqlite3.connect(DB_PATH)
    conn.row_factory = sqlite3.Row
    conn.execute("PRAGMA journal_mode=WAL")
    return conn

def init_db():
    db = get_db()
    db.executescript("""
    CREATE TABLE IF NOT EXISTS users (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        username TEXT UNIQUE NOT NULL,
        password_hash TEXT NOT NULL,
        role TEXT DEFAULT 'user',
        created_at REAL DEFAULT (strftime('%s','now')),
        disabled INTEGER DEFAULT 0
    );
    CREATE TABLE IF NOT EXISTS user_cookies (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        user_id INTEGER NOT NULL,
        cookie_raw TEXT NOT NULL,
        cookie_hash TEXT NOT NULL,
        email TEXT DEFAULT '',
        status TEXT DEFAULT 'pending',
        created_at REAL DEFAULT (strftime('%s','now')),
        FOREIGN KEY(user_id) REFERENCES users(id)
    );
    CREATE TABLE IF NOT EXISTS gen_history (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        user_id INTEGER NOT NULL,
        job_id TEXT,
        prompt TEXT,
        model TEXT,
        image_url TEXT,
        error TEXT,
        created_at REAL DEFAULT (strftime('%s','now')),
        FOREIGN KEY(user_id) REFERENCES users(id)
    );
    """)
    # Create default admin if not exists
    existing = db.execute("SELECT id FROM users WHERE username='admin'").fetchone()
    if not existing:
        pw = os.environ.get("ADMIN_PASSWORD", "admin123")
        db.execute("INSERT INTO users(username,password_hash,role) VALUES(?,?,?)",
                   ("admin", hash_password(pw), "admin"))
        db.commit()
    db.close()

def hash_password(pw: str) -> str:
    salt = "banana_pro_salt_2024"
    return hashlib.sha256(f"{salt}{pw}".encode()).hexdigest()

def create_token() -> str:
    return secrets.token_hex(32)

# In-memory token store: {token: {user_id, username, role, expires}}
_tokens: dict = {}

def login(username: str, password: str):
    db = get_db()
    user = db.execute("SELECT * FROM users WHERE username=? AND disabled=0",
                      (username,)).fetchone()
    db.close()
    if not user or user["password_hash"] != hash_password(password):
        return None
    token = create_token()
    _tokens[token] = {
        "user_id": user["id"], "username": user["username"],
        "role": user["role"], "expires": time.time() + 86400 * 7
    }
    return {"token": token, "username": user["username"], "role": user["role"]}

def verify_token(token: str):
    info = _tokens.get(token)
    if not info:
        return None
    if time.time() > info["expires"]:
        _tokens.pop(token, None)
        return None
    return info

def logout(token: str):
    _tokens.pop(token, None)

# ── User CRUD ──
def list_users():
    db = get_db()
    rows = db.execute("SELECT id,username,role,created_at,disabled FROM users ORDER BY id").fetchall()
    db.close()
    return [dict(r) for r in rows]

def create_user(username: str, password: str, role: str = "user"):
    db = get_db()
    try:
        db.execute("INSERT INTO users(username,password_hash,role) VALUES(?,?,?)",
                   (username, hash_password(password), role))
        db.commit()
        return True
    except sqlite3.IntegrityError:
        return False
    finally:
        db.close()

def update_user(user_id: int, role: str = None, disabled: int = None, password: str = None):
    db = get_db()
    if role is not None:
        db.execute("UPDATE users SET role=? WHERE id=?", (role, user_id))
    if disabled is not None:
        db.execute("UPDATE users SET disabled=? WHERE id=?", (disabled, user_id))
    if password is not None:
        db.execute("UPDATE users SET password_hash=? WHERE id=?", (hash_password(password), user_id))
    db.commit()
    db.close()

def delete_user(user_id: int):
    db = get_db()
    db.execute("DELETE FROM user_cookies WHERE user_id=?", (user_id,))
    db.execute("DELETE FROM gen_history WHERE user_id=?", (user_id,))
    db.execute("DELETE FROM users WHERE id=?", (user_id,))
    db.commit()
    db.close()

# ── Cookie per user ──
def get_user_cookies(user_id: int):
    db = get_db()
    rows = db.execute("SELECT * FROM user_cookies WHERE user_id=? ORDER BY id", (user_id,)).fetchall()
    db.close()
    return [dict(r) for r in rows]

def add_user_cookie(user_id: int, cookie_raw: str, cookie_hash: str, email: str = "", status: str = "pending"):
    db = get_db()
    existing = db.execute("SELECT id FROM user_cookies WHERE user_id=? AND cookie_hash=?",
                          (user_id, cookie_hash)).fetchone()
    if existing:
        db.close()
        return None
    db.execute("INSERT INTO user_cookies(user_id,cookie_raw,cookie_hash,email,status) VALUES(?,?,?,?,?)",
               (user_id, cookie_raw, cookie_hash, email, status))
    db.commit()
    cid = db.execute("SELECT last_insert_rowid()").fetchone()[0]
    db.close()
    return cid

def update_user_cookie(cookie_id: int, email: str = None, status: str = None):
    db = get_db()
    if email is not None:
        db.execute("UPDATE user_cookies SET email=? WHERE id=?", (email, cookie_id))
    if status is not None:
        db.execute("UPDATE user_cookies SET status=? WHERE id=?", (status, cookie_id))
    db.commit()
    db.close()

def delete_user_cookie(cookie_id: int, user_id: int):
    db = get_db()
    db.execute("DELETE FROM user_cookies WHERE id=? AND user_id=?", (cookie_id, user_id))
    db.commit()
    db.close()

def delete_all_user_cookies(user_id: int):
    db = get_db()
    db.execute("DELETE FROM user_cookies WHERE user_id=?", (user_id,))
    db.commit()
    db.close()

# ── History ──
def add_history(user_id: int, job_id: str, prompt: str, model: str, image_url: str = None, error: str = None):
    db = get_db()
    db.execute("INSERT INTO gen_history(user_id,job_id,prompt,model,image_url,error) VALUES(?,?,?,?,?,?)",
               (user_id, job_id, prompt, model, image_url, error))
    db.commit()
    db.close()

def get_history(user_id: int, limit: int = 100, offset: int = 0):
    db = get_db()
    rows = db.execute(
        "SELECT * FROM gen_history WHERE user_id=? ORDER BY id DESC LIMIT ? OFFSET ?",
        (user_id, limit, offset)).fetchall()
    db.close()
    return [dict(r) for r in rows]

def get_all_history(limit: int = 100, offset: int = 0):
    db = get_db()
    rows = db.execute(
        "SELECT h.*, u.username FROM gen_history h JOIN users u ON h.user_id=u.id ORDER BY h.id DESC LIMIT ? OFFSET ?",
        (limit, offset)).fetchall()
    db.close()
    return [dict(r) for r in rows]
