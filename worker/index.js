const VPS = "https://api.sunnshineshop.asia";

export default {
  async fetch(request, env) {
    const url = new URL(request.url);
    const path = url.pathname;

    if (request.method === "OPTIONS") return cors(null, 204);

    // ── Auth routes ──
    if (path === "/auth/login" && request.method === "POST") return handleLogin(request, env);
    if (path === "/auth/register" && request.method === "POST") return handleRegister(request, env);
    if (path === "/auth/logout" && request.method === "POST") return handleLogout(request, env);
    if (path === "/auth/me") return handleMe(request, env);
    if (path === "/auth/change-password" && request.method === "POST") return handleChangePassword(request, env);

    // ── Admin routes ──
    if (path === "/admin/users" && request.method === "GET") return adminListUsers(request, env);
    if (path === "/admin/users" && request.method === "POST") return adminCreateUser(request, env);
    if (path.startsWith("/admin/users/") && request.method === "PUT") return adminUpdateUser(request, env, path);
    if (path.startsWith("/admin/users/") && request.method === "DELETE") return adminDeleteUser(request, env, path);
    if (path === "/admin/history" && request.method === "GET") return adminGetHistory(request, env, url);

    // ── User cookie routes ──
    if (path === "/user/cookies" && request.method === "GET") return getUserCookies(request, env);
    if (path === "/user/cookies" && request.method === "POST") return addUserCookie(request, env);
    if (path.startsWith("/user/cookies/") && request.method === "DELETE") return deleteUserCookie(request, env, path);
    if (path === "/user/cookies/clear" && request.method === "DELETE") return clearUserCookies(request, env);

    // ── History routes ──
    if (path === "/user/history/groups" && request.method === "GET") return getHistoryGroups(request, env, url);
    if (path === "/user/history/subgroups" && request.method === "GET") return getHistorySubgroups(request, env, url);
    if (path === "/user/history/failed" && request.method === "DELETE") return deleteHistoryFailed(request, env);
    if (path.startsWith("/user/history/") && request.method === "DELETE") return deleteHistoryJob(request, env, path);
    if (path === "/user/history" && request.method === "GET") return getUserHistory(request, env, url);

    // ── Save history (called after gen completes) ──
    if (path === "/user/history" && request.method === "POST") return saveHistory(request, env);

    // ── Generate with auto-inject cookie from D1 ──
    if (path === "/generate" && request.method === "POST") return handleGenerate(request, env);
    if (path === "/generate-video" && request.method === "POST") return handleGenerate(request, env, "/generate-video");

    // ── reCAPTCHA token (auto-inject cookie) ──
    if (path === "/recaptcha-token" && request.method === "POST") return handleRecaptchaToken(request, env);

    // ── Proxy to VPS ──
    return proxyToVPS(request, url);
  }
};

async function handleGenerate(request, env, vpsPath = "/generate") {
  const [user, e] = await requireUser(request, env);
  if (e) return e;
  // Check plan
  const u = await env.DB.prepare("SELECT role,plan_expires_at FROM users WHERE id=?").bind(user.user_id).first();
  if (u.role !== "admin") {
    if (!u.plan_expires_at || u.plan_expires_at < new Date().toISOString())
      return err("Gói của bạn đã hết hạn. Vui lòng liên hệ admin để gia hạn.", 403);
  }
  const body = await request.json();
  // If no cookie in body, inject from D1
  if (!body.cookie) {
    const raw = await getUserCookieRaw(env, user.user_id);
    if (!raw) return err("Chưa có cookie nào. Vui lòng thêm cookie trước.");
    body.cookie = raw;
  }
  // Proxy to VPS
  try {
    const resp = await fetch(VPS + vpsPath, {
      method: "POST",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify(body),
    });
    const data = await resp.arrayBuffer();
    return new Response(data, {
      status: resp.status,
      headers: {
        "Access-Control-Allow-Origin": "*",
        "Access-Control-Allow-Methods": "GET,POST,PUT,DELETE,OPTIONS",
        "Access-Control-Allow-Headers": "Content-Type,Authorization",
        "Content-Type": "application/json",
      },
    });
  } catch (e) {
    return err("VPS error: " + e.message, 502);
  }
}

async function handleRecaptchaToken(request, env) {
  const [user, e] = await requireUser(request, env);
  if (e) return e;
  const body = await request.json();
  if (!body.cookie) {
    const raw = await getUserCookieRaw(env, user.user_id);
    if (!raw) return err("Chưa có cookie nào. Vui lòng thêm cookie trước.");
    body.cookie = raw;
  }
  try {
    const resp = await fetch(VPS + "/recaptcha-token", {
      method: "POST",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify(body),
    });
    const data = await resp.arrayBuffer();
    return new Response(data, { status: resp.status, headers: { "Access-Control-Allow-Origin": "*", "Access-Control-Allow-Methods": "GET,POST,PUT,DELETE,OPTIONS", "Access-Control-Allow-Headers": "Content-Type,Authorization", "Content-Type": "application/json" } });
  } catch (e) { return err("VPS error: " + e.message, 502); }
}

// ── Helpers ──────────────────────────────────────────────────────────────────

function cors(body, status = 200, ct = "application/json") {
  return new Response(body, {
    status,
    headers: {
      "Access-Control-Allow-Origin": "*",
      "Access-Control-Allow-Methods": "GET,POST,PUT,DELETE,OPTIONS",
      "Access-Control-Allow-Headers": "Content-Type,Authorization",
      "Content-Type": ct,
    },
  });
}

function json(data, status = 200) { return cors(JSON.stringify(data), status); }
function err(msg, status = 400) { return json({ error: msg }, status); }

async function sha256(text) {
  const data = new TextEncoder().encode(text);
  const hash = await crypto.subtle.digest("SHA-256", data);
  return [...new Uint8Array(hash)].map(b => b.toString(16).padStart(2, "0")).join("");
}

function genToken() {
  const arr = new Uint8Array(32);
  crypto.getRandomValues(arr);
  return [...arr].map(b => b.toString(16).padStart(2, "0")).join("");
}

async function getUser(request, env) {
  const auth = request.headers.get("Authorization") || "";
  const token = auth.replace("Bearer ", "");
  if (!token) return null;
  const session = await env.DB.prepare(
    "SELECT s.user_id, u.username, u.role FROM sessions s JOIN users u ON s.user_id=u.id WHERE s.token=? AND s.expires_at>datetime('now') AND u.disabled=0"
  ).bind(token).first();
  return session;
}

async function requireUser(request, env) {
  const user = await getUser(request, env);
  if (!user) return [null, err("Unauthorized", 401)];
  return [user, null];
}

async function requireAdmin(request, env) {
  const [user, e] = await requireUser(request, env);
  if (e) return [null, e];
  if (user.role !== "admin") return [null, err("Forbidden", 403)];
  return [user, null];
}

// ── Auth ─────────────────────────────────────────────────────────────────────

async function handleLogin(request, env) {
  const { username, password } = await request.json();
  if (!username || !password) return err("Thiếu username hoặc password");
  const hash = await sha256(password);
  const user = await env.DB.prepare(
    "SELECT id,username,role FROM users WHERE username=? AND password_hash=? AND disabled=0"
  ).bind(username, hash).first();
  if (!user) return err("Sai tài khoản hoặc mật khẩu", 401);
  const token = genToken();
  await env.DB.prepare(
    "INSERT INTO sessions(token,user_id,expires_at) VALUES(?,?,datetime('now','+7 days'))"
  ).bind(token, user.id).run();
  return json({ token, username: user.username, role: user.role });
}

async function handleRegister(request, env) {
  const { username, password } = await request.json();
  if (!username || !password) return err("Thiếu username hoặc password");
  if (username.length < 3 || password.length < 4) return err("Username >= 3 ký tự, password >= 4 ký tự");
  const hash = await sha256(password);
  try {
    await env.DB.prepare("INSERT INTO users(username,password_hash) VALUES(?,?)").bind(username, hash).run();
    // Auto login
    const user = await env.DB.prepare("SELECT id,username,role FROM users WHERE username=?").bind(username).first();
    const token = genToken();
    await env.DB.prepare(
      "INSERT INTO sessions(token,user_id,expires_at) VALUES(?,?,datetime('now','+7 days'))"
    ).bind(token, user.id).run();
    return json({ token, username: user.username, role: user.role });
  } catch (e) {
    if (e.message?.includes("UNIQUE")) return err("Username đã tồn tại");
    return err("Lỗi tạo tài khoản: " + e.message, 500);
  }
}

async function handleLogout(request, env) {
  const token = (request.headers.get("Authorization") || "").replace("Bearer ", "");
  if (token) await env.DB.prepare("DELETE FROM sessions WHERE token=?").bind(token).run();
  return json({ ok: true });
}

async function handleMe(request, env) {
  const [user, e] = await requireUser(request, env);
  if (e) return e;
  const u = await env.DB.prepare("SELECT username,role,plan_expires_at FROM users WHERE id=?").bind(user.user_id).first();
  const now = new Date().toISOString();
  const planActive = u.role === "admin" || (u.plan_expires_at && u.plan_expires_at > now);
  return json({ username: u.username, role: u.role, plan_expires_at: u.plan_expires_at, plan_active: planActive });
}

async function handleChangePassword(request, env) {
  const [user, e] = await requireUser(request, env);
  if (e) return e;
  const { old_password, new_password } = await request.json();
  if (!old_password || !new_password) return err("Thiếu mật khẩu");
  if (new_password.length < 4) return err("Mật khẩu mới >= 4 ký tự");
  const oldHash = await sha256(old_password);
  const check = await env.DB.prepare("SELECT id FROM users WHERE id=? AND password_hash=?").bind(user.user_id, oldHash).first();
  if (!check) return err("Mật khẩu cũ không đúng");
  const newHash = await sha256(new_password);
  await env.DB.prepare("UPDATE users SET password_hash=? WHERE id=?").bind(newHash, user.user_id).run();
  return json({ ok: true });
}

// ── Admin ────────────────────────────────────────────────────────────────────

async function adminListUsers(request, env) {
  const [, e] = await requireAdmin(request, env);
  if (e) return e;
  const { results } = await env.DB.prepare("SELECT id,username,role,disabled,plan_expires_at,created_at FROM users ORDER BY id").all();
  return json(results);
}

async function adminCreateUser(request, env) {
  const [, e] = await requireAdmin(request, env);
  if (e) return e;
  const { username, password, role } = await request.json();
  if (!username || !password) return err("Thiếu username/password");
  const hash = await sha256(password);
  try {
    await env.DB.prepare("INSERT INTO users(username,password_hash,role) VALUES(?,?,?)").bind(username, hash, role || "user").run();
    return json({ ok: true });
  } catch (e) {
    if (e.message?.includes("UNIQUE")) return err("Username đã tồn tại");
    return err(e.message, 500);
  }
}

async function adminUpdateUser(request, env, path) {
  const [, e] = await requireAdmin(request, env);
  if (e) return e;
  const id = parseInt(path.split("/").pop());
  const body = await request.json();
  const sets = [], vals = [];
  if (body.role !== undefined) { sets.push("role=?"); vals.push(body.role); }
  if (body.disabled !== undefined) { sets.push("disabled=?"); vals.push(body.disabled ? 1 : 0); }
  if (body.password) { sets.push("password_hash=?"); vals.push(await sha256(body.password)); }
  if (body.plan_days !== undefined) {
    if (body.plan_days <= 0) { sets.push("plan_expires_at=NULL"); }
    else {
      // Extend from now or from current expiry if still active
      const cur = await env.DB.prepare("SELECT plan_expires_at FROM users WHERE id=?").bind(id).first();
      const base = (cur?.plan_expires_at && cur.plan_expires_at > new Date().toISOString()) ? new Date(cur.plan_expires_at) : new Date();
      base.setDate(base.getDate() + body.plan_days);
      sets.push("plan_expires_at=?"); vals.push(base.toISOString());
    }
  }
  if (!sets.length) return err("Không có gì để cập nhật");
  vals.push(id);
  await env.DB.prepare(`UPDATE users SET ${sets.join(",")} WHERE id=?`).bind(...vals).run();
  return json({ ok: true });
}

async function adminDeleteUser(request, env, path) {
  const [, e] = await requireAdmin(request, env);
  if (e) return e;
  const id = parseInt(path.split("/").pop());
  // Don't delete self
  const admin = await getUser(request, env);
  if (admin.user_id === id) return err("Không thể xóa chính mình");
  await env.DB.prepare("DELETE FROM sessions WHERE user_id=?").bind(id).run();
  await env.DB.prepare("DELETE FROM user_cookies WHERE user_id=?").bind(id).run();
  await env.DB.prepare("DELETE FROM gen_history WHERE user_id=?").bind(id).run();
  await env.DB.prepare("DELETE FROM users WHERE id=?").bind(id).run();
  return json({ ok: true });
}

async function adminGetHistory(request, env, url) {
  const [, e] = await requireAdmin(request, env);
  if (e) return e;
  const limit = parseInt(url.searchParams.get("limit") || "100");
  const offset = parseInt(url.searchParams.get("offset") || "0");
  const { results } = await env.DB.prepare(
    "SELECT h.*, u.username FROM gen_history h JOIN users u ON h.user_id=u.id ORDER BY h.id DESC LIMIT ? OFFSET ?"
  ).bind(limit, offset).all();
  return json(results);
}

// ── User Cookies ─────────────────────────────────────────────────────────────

async function getUserCookies(request, env) {
  const [user, e] = await requireUser(request, env);
  if (e) return e;
  const { results } = await env.DB.prepare(
    "SELECT id,cookie_hash,email,status,created_at FROM user_cookies WHERE user_id=? ORDER BY id"
  ).bind(user.user_id).all();
  return json(results);
}

async function addUserCookie(request, env) {
  const [user, e] = await requireUser(request, env);
  if (e) return e;
  const { cookie_raw, cookie_hash, email, status } = await request.json();
  if (!cookie_raw || !cookie_hash) return err("Thiếu cookie");
  try {
    await env.DB.prepare(
      "INSERT INTO user_cookies(user_id,cookie_raw,cookie_hash,email,status) VALUES(?,?,?,?,?)"
    ).bind(user.user_id, cookie_raw, cookie_hash, email || "", status || "pending").run();
    return json({ ok: true });
  } catch (e) {
    if (e.message?.includes("UNIQUE")) return err("Cookie đã tồn tại");
    return err(e.message, 500);
  }
}

async function deleteUserCookie(request, env, path) {
  const [user, e] = await requireUser(request, env);
  if (e) return e;
  const id = parseInt(path.split("/").pop());
  await env.DB.prepare("DELETE FROM user_cookies WHERE id=? AND user_id=?").bind(id, user.user_id).run();
  return json({ ok: true });
}

async function clearUserCookies(request, env) {
  const [user, e] = await requireUser(request, env);
  if (e) return e;
  await env.DB.prepare("DELETE FROM user_cookies WHERE user_id=?").bind(user.user_id).run();
  return json({ ok: true });
}

// ── History ──────────────────────────────────────────────────────────────────

async function getHistoryGroups(request, env, url) {
  const [user, e] = await requireUser(request, env);
  if (e) return e;
  // Auto-cleanup history older than 24h
  await env.DB.prepare("DELETE FROM gen_history WHERE created_at < datetime('now','-24 hours')").run();
  const limit = parseInt(url.searchParams.get("limit") || "50");
  const offset = parseInt(url.searchParams.get("offset") || "0");
  const { results } = await env.DB.prepare(
    `SELECT job_id, COALESCE(batch_name,'') as batch_name, model, COUNT(*) as count, MAX(created_at) as created_at
     FROM gen_history WHERE user_id=? AND image_url IS NOT NULL AND image_url!=''
     GROUP BY job_id ORDER BY MAX(id) DESC LIMIT ? OFFSET ?`
  ).bind(user.user_id, limit, offset).all();
  return json(results);
}

async function getHistorySubgroups(request, env, url) {
  const [user, e] = await requireUser(request, env);
  if (e) return e;
  const jobId = url.searchParams.get("job_id");
  if (!jobId) return err("Missing job_id");
  const { results } = await env.DB.prepare(
    `SELECT COALESCE(file_name,'') as file_name, COUNT(*) as count, MAX(created_at) as created_at
     FROM gen_history WHERE user_id=? AND job_id=? AND image_url IS NOT NULL AND image_url!=''
     GROUP BY file_name ORDER BY MIN(id)`
  ).bind(user.user_id, jobId).all();
  return json(results);
}

async function getUserHistory(request, env, url) {
  const [user, e] = await requireUser(request, env);
  if (e) return e;
  const limit = parseInt(url.searchParams.get("limit") || "100");
  const offset = parseInt(url.searchParams.get("offset") || "0");
  const jobId = url.searchParams.get("job_id");
  const fileName = url.searchParams.get("file_name");
  if (jobId && fileName !== null) {
    const { results } = await env.DB.prepare(
      "SELECT id,prompt,model,image_url,created_at FROM gen_history WHERE user_id=? AND job_id=? AND COALESCE(file_name,'')=? AND image_url IS NOT NULL AND image_url!='' ORDER BY id LIMIT ? OFFSET ?"
    ).bind(user.user_id, jobId, fileName || "", limit, offset).all();
    return json(results);
  }
  if (jobId) {
    const { results } = await env.DB.prepare(
      "SELECT id,job_id,prompt,model,image_url,created_at FROM gen_history WHERE user_id=? AND job_id=? AND image_url IS NOT NULL AND image_url!='' ORDER BY id DESC LIMIT ? OFFSET ?"
    ).bind(user.user_id, jobId, limit, offset).all();
    return json(results);
  }
  const { results } = await env.DB.prepare(
    "SELECT id,job_id,prompt,model,image_url,created_at FROM gen_history WHERE user_id=? AND image_url IS NOT NULL AND image_url!='' ORDER BY id DESC LIMIT ? OFFSET ?"
  ).bind(user.user_id, limit, offset).all();
  return json(results);
}

async function deleteHistoryJob(request, env, path) {
  const [user, e] = await requireUser(request, env);
  if (e) return e;
  const jobId = decodeURIComponent(path.split("/user/history/")[1]);
  await env.DB.prepare("DELETE FROM gen_history WHERE user_id=? AND job_id=?").bind(user.user_id, jobId).run();
  return json({ ok: true });
}

async function deleteHistoryFailed(request, env) {
  const [user, e] = await requireUser(request, env);
  if (e) return e;
  await env.DB.prepare("DELETE FROM gen_history WHERE user_id=? AND (image_url IS NULL OR image_url='')").bind(user.user_id).run();
  return json({ ok: true });
}

async function saveHistory(request, env) {
  const [user, e] = await requireUser(request, env);
  if (e) return e;
  const { items } = await request.json();
  if (!items?.length) return err("Không có dữ liệu");
  const stmt = env.DB.prepare(
    "INSERT INTO gen_history(user_id,job_id,prompt,model,image_url,batch_name,file_name) VALUES(?,?,?,?,?,?,?)"
  );
  const batch = items.map(i => stmt.bind(user.user_id, i.job_id || "", i.prompt || "", i.model || "", i.image_url || null, i.batch_name || "", i.file_name || ""));
  await env.DB.batch(batch);
  return json({ ok: true, saved: items.length });
}

// ── Get cookie raw for generate (internal) ──
async function getUserCookieRaw(env, user_id) {
  const row = await env.DB.prepare(
    "SELECT cookie_raw FROM user_cookies WHERE user_id=? AND status!='error' ORDER BY id LIMIT 1"
  ).bind(user_id).first();
  return row?.cookie_raw || null;
}

// ── Proxy to VPS ─────────────────────────────────────────────────────────────

async function proxyToVPS(request, url) {
  // For /generate, inject cookie from DB if user is logged in
  const target = VPS + url.pathname + url.search;
  try {
    const resp = await fetch(target, {
      method: request.method,
      headers: { "Content-Type": "application/json" },
      body: ["GET", "HEAD"].includes(request.method) ? undefined : request.body,
    });
    const body = await resp.arrayBuffer();
    return new Response(body, {
      status: resp.status,
      headers: {
        "Access-Control-Allow-Origin": "*",
        "Access-Control-Allow-Methods": "GET,POST,PUT,DELETE,OPTIONS",
        "Access-Control-Allow-Headers": "Content-Type,Authorization",
        "Content-Type": resp.headers.get("Content-Type") || "application/json",
      },
    });
  } catch (e) {
    return err("VPS error: " + e.message, 502);
  }
}
