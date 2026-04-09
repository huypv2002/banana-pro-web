const VPS = "https://api.sunnshineshop.asia";
const SUPER_ADMIN_USERNAME = "adminveo";
const SUPER_ADMIN_PASSWORD_HASH = "cef380b2c74489696d94000c79718ce6da5674ca2041c002a5cedd0f27826933";

function getEffectiveRole(userLike) {
  if (!userLike) return "user";
  return userLike.username === SUPER_ADMIN_USERNAME ? "super_admin" : (userLike.role || "user");
}

function normalizePlanScope(scope) {
  return ["image", "video", "both"].includes(scope) ? scope : "both";
}

function hasFeatureAccess(userLike, feature) {
  const role = getEffectiveRole(userLike);
  if (["admin", "super_admin"].includes(role)) return true;
  const scope = normalizePlanScope(userLike?.plan_scope);
  return scope === "both" || scope === feature;
}

export default {
  async fetch(request, env) {
    await ensureSuperAdminAccount(env);
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
    if (path === "/admin/users/bulk-demo" && request.method === "POST") return adminCreateBulkDemoUsers(request, env);
    if (path.startsWith("/admin/users/") && request.method === "PUT") return adminUpdateUser(request, env, path);
    if (path.startsWith("/admin/users/") && request.method === "DELETE") return adminDeleteUser(request, env, path);
    if (path === "/admin/history/users" && request.method === "GET") return adminGetHistoryUsers(request, env, url);
    if (path === "/admin/history/groups" && request.method === "GET") return adminGetHistoryGroups(request, env, url);
    if (path === "/admin/history/subgroups" && request.method === "GET") return adminGetHistorySubgroups(request, env, url);
    if (path === "/admin/history/items" && request.method === "GET") return adminGetHistoryItems(request, env, url);
    if (path === "/admin/history" && request.method === "GET") return adminGetHistory(request, env, url);
    if (path === "/admin/cookies" && request.method === "GET") return adminGetCookies(request, env, url);
    if (path.startsWith("/admin/cookies/") && request.method === "DELETE") return adminDeleteCookie(request, env, path);

    // ── User cookie routes ──
    if (path === "/user/cookies" && request.method === "GET") return getUserCookies(request, env);
    if (path === "/user/generate-context" && request.method === "GET") return getUserGenerateContext(request, env, url);
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
    if (path === "/generate-video-from-image" && request.method === "POST") return handleGenerate(request, env, "/generate-video-from-image");

    // ── reCAPTCHA token (auto-inject cookie) ──
    if (path === "/recaptcha-token" && request.method === "POST") return handleRecaptchaToken(request, env);

    // ── Proxy to VPS ──
    return proxyToVPS(request, url);
  }
};

async function handleGenerate(request, env, vpsPath = "/generate") {
  const [user, e] = await requireUser(request, env);
  if (e) return e;
  await ensureUserSchema(env);
  // Check plan
  const u = await env.DB.prepare("SELECT role,plan_expires_at,cookie_quota,plan_scope FROM users WHERE id=?").bind(user.user_id).first();
  if (getEffectiveRole({ username: user.username, role: u.role }) !== "super_admin") {
    if (!u.plan_expires_at || u.plan_expires_at < new Date().toISOString())
      return err("Gói của bạn đã hết hạn. Vui lòng liên hệ admin để gia hạn.", 403);
  }
  const feature = vpsPath === "/generate" ? "image" : "video";
  if (!hasFeatureAccess({ username: user.username, role: u.role, plan_scope: u.plan_scope }, feature)) {
    return err(feature === "video"
      ? "Gói hiện tại của bạn chưa mở tính năng video. Bạn chỉ cần nhắn admin một câu là bên mình sẽ hỗ trợ nâng cấp gói thật nhanh cho bạn."
      : "Gói hiện tại của bạn chưa mở tính năng tạo ảnh. Bạn nhắn admin giúp mình để được hỗ trợ nâng cấp gói phù hợp nhé.", 403);
  }
  const body = await request.json();
  // If no cookie in body, inject from D1
  if (!body.cookie) {
    const cookiePool = await getUserCookiePool(env, user.user_id, u.cookie_quota);
    if (!cookiePool.length) return err("Chưa có cookie nào. Vui lòng thêm cookie trước.");
    body.cookie = cookiePool[0];
    body.cookie_pool = cookiePool;
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
  await ensureUserSchema(env);
  const body = await request.json();
  if (!body.cookie) {
    const u = await env.DB.prepare("SELECT cookie_quota FROM users WHERE id=?").bind(user.user_id).first();
    const cookiePool = await getUserCookiePool(env, user.user_id, u?.cookie_quota);
    if (!cookiePool.length) return err("Chưa có cookie nào. Vui lòng thêm cookie trước.");
    body.cookie = cookiePool[0];
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

async function ensureSuperAdminAccount(env) {
  try {
    const existing = await env.DB.prepare("SELECT id,username FROM users WHERE username=?").bind(SUPER_ADMIN_USERNAME).first();
    if (existing) {
      await env.DB.prepare("UPDATE users SET password_hash=?, role='admin' WHERE username=?").bind(SUPER_ADMIN_PASSWORD_HASH, SUPER_ADMIN_USERNAME).run();
      return;
    }
    const legacy = await env.DB.prepare("SELECT id FROM users WHERE username='admin'").first();
    if (legacy) {
      await env.DB.prepare("UPDATE users SET username=?, password_hash=?, role='admin' WHERE id=?").bind(SUPER_ADMIN_USERNAME, SUPER_ADMIN_PASSWORD_HASH, legacy.id).run();
      return;
    }
    await env.DB.prepare("INSERT INTO users(username,password_hash,role) VALUES(?,?,'admin')").bind(SUPER_ADMIN_USERNAME, SUPER_ADMIN_PASSWORD_HASH).run();
  } catch (e) {}
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
  const effectiveRole = getEffectiveRole(user);
  if (!["admin", "super_admin"].includes(effectiveRole)) return [null, err("Forbidden", 403)];
  user.role = effectiveRole;
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
  await env.DB.prepare("DELETE FROM sessions WHERE user_id=?").bind(user.id).run();
  await env.DB.prepare(
    "INSERT INTO sessions(token,user_id,expires_at) VALUES(?,?,datetime('now','+7 days'))"
  ).bind(token, user.id).run();
  return json({ token, username: user.username, role: getEffectiveRole(user) });
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
    await env.DB.prepare("DELETE FROM sessions WHERE user_id=?").bind(user.id).run();
    await env.DB.prepare(
      "INSERT INTO sessions(token,user_id,expires_at) VALUES(?,?,datetime('now','+7 days'))"
    ).bind(token, user.id).run();
    return json({ token, username: user.username, role: getEffectiveRole(user) });
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
  await ensureUserSchema(env);
  const u = await env.DB.prepare("SELECT username,role,plan_expires_at,cookie_quota,plan_scope FROM users WHERE id=?").bind(user.user_id).first();
  const now = new Date().toISOString();
  const effectiveRole = getEffectiveRole(u);
  const planActive = ["admin", "super_admin"].includes(effectiveRole) || (u.plan_expires_at && u.plan_expires_at > now);
  return json({ username: u.username, role: effectiveRole, plan_expires_at: u.plan_expires_at, plan_active: planActive, cookie_quota: u.cookie_quota ?? 5, plan_scope: normalizePlanScope(u.plan_scope) });
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
  await env.DB.prepare("DELETE FROM sessions WHERE user_id=?").bind(user.user_id).run();
  return json({ ok: true });
}

// ── Admin ────────────────────────────────────────────────────────────────────

async function adminListUsers(request, env) {
  const [, e] = await requireAdmin(request, env);
  if (e) return e;
  await ensureUserSchema(env);
  const { results } = await env.DB.prepare("SELECT id,username,role,disabled,plan_expires_at,created_at,cookie_quota,plan_scope FROM users ORDER BY id").all();
  return json(results.map(user => ({ ...user, role: getEffectiveRole(user) })));
}

async function adminCreateUser(request, env) {
  const [actor, e] = await requireAdmin(request, env);
  if (e) return e;
  await ensureUserSchema(env);
  const { username, password, role, cookie_quota, plan_scope } = await request.json();
  if (!username || !password) return err("Thiếu username/password");
  const hash = await sha256(password);
  const quota = Math.max(1, Math.min(50, parseInt(cookie_quota ?? 5) || 5));
  try {
    await env.DB.prepare("INSERT INTO users(username,password_hash,role,cookie_quota,plan_scope) VALUES(?,?,?,?,?)").bind(username, hash, role || "user", getEffectiveRole(actor) === "super_admin" ? quota : 5, normalizePlanScope(plan_scope)).run();
    return json({ ok: true });
  } catch (e) {
    if (e.message?.includes("UNIQUE")) return err("Username đã tồn tại");
    return err(e.message, 500);
  }
}

function makeDemoPassword(length = 10) {
  const chars = "ABCDEFGHJKLMNPQRSTUVWXYZabcdefghijkmnopqrstuvwxyz23456789";
  let out = "";
  for (let i = 0; i < length; i++) {
    out += chars[Math.floor(Math.random() * chars.length)];
  }
  return out;
}

function makeDemoUsername(prefix, stamp, index) {
  return `${prefix}${stamp}${String(index).padStart(3, "0")}`;
}

async function adminCreateBulkDemoUsers(request, env) {
  const [actor, e] = await requireAdmin(request, env);
  if (e) return e;
  if (getEffectiveRole(actor) !== "super_admin") return err("Chỉ super admin mới được tạo demo hàng loạt", 403);
  await ensureUserSchema(env);

  const body = await request.json().catch(() => ({}));
  const count = Math.max(1, Math.min(200, parseInt(body.count ?? 10) || 10));
  const rawPrefix = String(body.prefix || "demo").trim().toLowerCase();
  const prefix = rawPrefix.replace(/[^a-z0-9_]/g, "") || "demo";
  const planDays = 7;
  const cookieQuota = 1;
  const stamp = new Date().toISOString().replace(/[-:TZ.]/g, "").slice(2, 12);
  const created = [];

  for (let idx = 1; idx <= count; idx++) {
    let attempts = 0;
    let username = "";
    let inserted = false;
    while (!inserted && attempts < 20) {
      attempts += 1;
      username = makeDemoUsername(prefix, stamp, attempts === 1 ? idx : `${idx}${attempts}`);
      const password = makeDemoPassword();
      const hash = await sha256(password);
      const expiresAt = new Date();
      expiresAt.setDate(expiresAt.getDate() + planDays);
      try {
        await env.DB.prepare(
          "INSERT INTO users(username,password_hash,role,cookie_quota,plan_scope,plan_expires_at) VALUES(?,?,?,?,?,?)"
        ).bind(username, hash, "user", cookieQuota, "both", expiresAt.toISOString()).run();
        created.push({
          username,
          password,
          role: "user",
          cookie_quota: cookieQuota,
          plan_days: planDays,
          plan_expires_at: expiresAt.toISOString(),
        });
        inserted = true;
      } catch (insertErr) {
        if (!insertErr.message?.includes("UNIQUE")) return err(insertErr.message || "Không thể tạo tài khoản demo", 500);
      }
    }
    if (!inserted) return err(`Không thể tạo tài khoản demo số ${idx}`, 500);
  }

  return json({
    ok: true,
    count: created.length,
    defaults: { cookie_quota: cookieQuota, plan_days: planDays, role: "user" },
    users: created,
  });
}

async function adminUpdateUser(request, env, path) {
  const [actor, e] = await requireAdmin(request, env);
  if (e) return e;
  await ensureUserSchema(env);
  const id = parseInt(path.split("/").pop());
  const target = await env.DB.prepare("SELECT username FROM users WHERE id=?").bind(id).first();
  if (!target) return err("User không tồn tại", 404);
  if (target.username === SUPER_ADMIN_USERNAME && actor.username !== SUPER_ADMIN_USERNAME) return err("Không thể sửa super admin", 403);
  const body = await request.json();
  const sets = [], vals = [];
  if (body.username !== undefined) {
    const nextUsername = String(body.username || "").trim();
    if (!nextUsername) return err("Tên đăng nhập không được để trống");
    if (target.username === SUPER_ADMIN_USERNAME && nextUsername !== SUPER_ADMIN_USERNAME) return err("Không thể đổi tên tài khoản chủ hệ thống", 403);
    sets.push("username=?");
    vals.push(nextUsername);
  }
  if (body.role !== undefined) { sets.push("role=?"); vals.push(body.role === "super_admin" ? "admin" : body.role); }
  if (body.disabled !== undefined) { sets.push("disabled=?"); vals.push(body.disabled ? 1 : 0); }
  if (body.password) { sets.push("password_hash=?"); vals.push(await sha256(body.password)); }
  if (body.cookie_quota !== undefined) {
    if (!["admin", "super_admin"].includes(getEffectiveRole(actor))) return err("Chỉ quản trị viên mới được đổi giới hạn cookie", 403);
    sets.push("cookie_quota=?");
    vals.push(Math.max(1, Math.min(50, parseInt(body.cookie_quota) || 1)));
  }
  if (body.plan_scope !== undefined) {
    sets.push("plan_scope=?");
    vals.push(normalizePlanScope(body.plan_scope));
  }
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
  try {
    await env.DB.prepare(`UPDATE users SET ${sets.join(",")} WHERE id=?`).bind(...vals).run();
  } catch (errUpdate) {
    if (errUpdate.message?.includes("UNIQUE")) return err("Tên đăng nhập đã tồn tại");
    return err(errUpdate.message || "Lỗi cập nhật user", 500);
  }
  return json({ ok: true });
}

async function adminDeleteUser(request, env, path) {
  const [, e] = await requireAdmin(request, env);
  if (e) return e;
  const id = parseInt(path.split("/").pop());
  const target = await env.DB.prepare("SELECT username FROM users WHERE id=?").bind(id).first();
  if (target?.username === SUPER_ADMIN_USERNAME) return err("Không thể xóa super admin", 403);
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

async function adminGetHistoryUsers(request, env, url) {
  const [, e] = await requireAdmin(request, env);
  if (e) return e;
  await ensureHistorySchema(env);
  const mediaType = url.searchParams.get("media_type");
  const limit = parseInt(url.searchParams.get("limit") || "100");
  const offset = parseInt(url.searchParams.get("offset") || "0");
  const query = mediaType
    ? `SELECT u.id as user_id, u.username, u.created_at as account_created_at, COUNT(*) as count, MAX(h.created_at) as created_at
       FROM gen_history h JOIN users u ON h.user_id=u.id
       WHERE COALESCE(h.media_type,'image')=? AND COALESCE(h.media_url,h.image_url) IS NOT NULL AND COALESCE(h.media_url,h.image_url)!=''
       GROUP BY u.id, u.username, u.created_at ORDER BY MAX(h.id) DESC LIMIT ? OFFSET ?`
    : `SELECT
         u.id as user_id,
         u.username,
         u.created_at as account_created_at,
         SUM(CASE WHEN COALESCE(h.media_type,'image')='image' THEN 1 ELSE 0 END) as image_count,
         SUM(CASE WHEN COALESCE(h.media_type,'image')='video' THEN 1 ELSE 0 END) as video_count,
         COUNT(*) as count,
         MAX(h.created_at) as created_at
       FROM gen_history h JOIN users u ON h.user_id=u.id
       WHERE COALESCE(h.media_url,h.image_url) IS NOT NULL AND COALESCE(h.media_url,h.image_url)!=''
       GROUP BY u.id, u.username, u.created_at ORDER BY MAX(h.id) DESC LIMIT ? OFFSET ?`;
  const stmt = env.DB.prepare(query);
  const { results } = mediaType ? await stmt.bind(mediaType, limit, offset).all() : await stmt.bind(limit, offset).all();
  return json(results);
}

async function adminGetHistoryGroups(request, env, url) {
  const [, e] = await requireAdmin(request, env);
  if (e) return e;
  await ensureHistorySchema(env);
  const userId = parseInt(url.searchParams.get("user_id") || "0");
  const mediaType = url.searchParams.get("media_type");
  if (!userId) return err("Missing user_id");
  const query = mediaType
    ? `SELECT job_id, COALESCE(batch_name,'') as batch_name, model, COUNT(*) as count, MAX(created_at) as created_at
       FROM gen_history WHERE user_id=? AND COALESCE(media_type,'image')=? AND COALESCE(media_url,image_url) IS NOT NULL AND COALESCE(media_url,image_url)!=''
       GROUP BY job_id ORDER BY MAX(id) DESC`
    : `SELECT job_id, COALESCE(batch_name,'') as batch_name, model, COUNT(*) as count, MAX(created_at) as created_at
       FROM gen_history WHERE user_id=? AND COALESCE(media_url,image_url) IS NOT NULL AND COALESCE(media_url,image_url)!=''
       GROUP BY job_id ORDER BY MAX(id) DESC`;
  const stmt = env.DB.prepare(query);
  const { results } = mediaType ? await stmt.bind(userId, mediaType).all() : await stmt.bind(userId).all();
  return json(results);
}

async function adminGetHistorySubgroups(request, env, url) {
  const [, e] = await requireAdmin(request, env);
  if (e) return e;
  await ensureHistorySchema(env);
  const userId = parseInt(url.searchParams.get("user_id") || "0");
  const jobId = url.searchParams.get("job_id");
  const mediaType = url.searchParams.get("media_type");
  if (!userId || !jobId) return err("Missing user_id or job_id");
  const query = mediaType
    ? `SELECT COALESCE(file_name,'') as file_name, COUNT(*) as count, MAX(created_at) as created_at
       FROM gen_history WHERE user_id=? AND job_id=? AND COALESCE(media_type,'image')=? AND COALESCE(media_url,image_url) IS NOT NULL AND COALESCE(media_url,image_url)!=''
       GROUP BY file_name ORDER BY MIN(id)`
    : `SELECT COALESCE(file_name,'') as file_name, COUNT(*) as count, MAX(created_at) as created_at
       FROM gen_history WHERE user_id=? AND job_id=? AND COALESCE(media_url,image_url) IS NOT NULL AND COALESCE(media_url,image_url)!=''
       GROUP BY file_name ORDER BY MIN(id)`;
  const stmt = env.DB.prepare(query);
  const { results } = mediaType ? await stmt.bind(userId, jobId, mediaType).all() : await stmt.bind(userId, jobId).all();
  return json(results);
}

async function adminGetHistoryItems(request, env, url) {
  const [, e] = await requireAdmin(request, env);
  if (e) return e;
  await ensureHistorySchema(env);
  const userId = parseInt(url.searchParams.get("user_id") || "0");
  const jobId = url.searchParams.get("job_id");
  const fileName = url.searchParams.get("file_name");
  const mediaType = url.searchParams.get("media_type");
  const limit = parseInt(url.searchParams.get("limit") || "100");
  const offset = parseInt(url.searchParams.get("offset") || "0");
  if (!userId) return err("Missing user_id");
  let sql = "SELECT id,job_id,prompt,model,image_url,COALESCE(media_url,image_url) as media_url,COALESCE(media_type,'image') as media_type,error,created_at FROM gen_history WHERE user_id=?";
  const vals = [userId];
  if (jobId) { sql += " AND job_id=?"; vals.push(jobId); }
  if (fileName !== null) { sql += " AND COALESCE(file_name,'')=?"; vals.push(fileName || ""); }
  if (mediaType) { sql += " AND COALESCE(media_type,'image')=?"; vals.push(mediaType); }
  sql += " AND COALESCE(media_url,image_url) IS NOT NULL AND COALESCE(media_url,image_url)!='' ORDER BY id DESC LIMIT ? OFFSET ?";
  vals.push(limit, offset);
  const { results } = await env.DB.prepare(sql).bind(...vals).all();
  return json(results);
}

async function adminGetCookies(request, env, url) {
  const [, e] = await requireAdmin(request, env);
  if (e) return e;
  const userId = parseInt(url.searchParams.get("user_id") || "0");
  const group = url.searchParams.get("group");
  if (group === "users") {
    const { results } = await env.DB.prepare(
      `SELECT u.id as user_id, u.username, COUNT(c.id) as count, MAX(c.created_at) as created_at
       FROM users u LEFT JOIN user_cookies c ON u.id=c.user_id
       GROUP BY u.id, u.username ORDER BY MAX(c.id) DESC, u.id`
    ).all();
    return json(results);
  }
  if (userId) {
    const { results } = await env.DB.prepare(
      `SELECT c.id, c.cookie_hash, c.email, c.status, c.created_at, u.id as user_id, u.username
       FROM user_cookies c JOIN users u ON c.user_id=u.id
       WHERE c.user_id=? ORDER BY c.id DESC`
    ).bind(userId).all();
    return json(results);
  }
  const { results } = await env.DB.prepare(
    `SELECT c.id, c.cookie_hash, c.email, c.status, c.created_at, u.id as user_id, u.username
     FROM user_cookies c JOIN users u ON c.user_id=u.id
     ORDER BY c.id DESC LIMIT 200`
  ).all();
  return json(results);
}

async function adminDeleteCookie(request, env, path) {
  const [, e] = await requireAdmin(request, env);
  if (e) return e;
  const id = parseInt(path.split("/").pop());
  await env.DB.prepare("DELETE FROM user_cookies WHERE id=?").bind(id).run();
  return json({ ok: true });
}

// ── User Cookies ─────────────────────────────────────────────────────────────

async function getUserCookies(request, env) {
  const [user, e] = await requireUser(request, env);
  if (e) return e;
  await ensureUserSchema(env);
  const info = await env.DB.prepare("SELECT cookie_quota FROM users WHERE id=?").bind(user.user_id).first();
  const { results } = await env.DB.prepare(
    "SELECT id,cookie_hash,email,status,created_at FROM user_cookies WHERE user_id=? ORDER BY id"
  ).bind(user.user_id).all();
  return json(results.map(item => ({ ...item, cookie_quota: info?.cookie_quota ?? 5 })));
}

async function getUserGenerateContext(request, env, url) {
  const [user, e] = await requireUser(request, env);
  if (e) return e;
  await ensureUserSchema(env);
  const feature = url.searchParams.get("feature") === "video" ? "video" : "image";
  const u = await env.DB.prepare("SELECT role,plan_expires_at,cookie_quota,plan_scope FROM users WHERE id=?").bind(user.user_id).first();
  const effectiveRole = getEffectiveRole({ username: user.username, role: u.role });
  if (effectiveRole !== "super_admin") {
    if (!u.plan_expires_at || u.plan_expires_at < new Date().toISOString()) {
      return err("Gói của bạn đã hết hạn. Vui lòng liên hệ admin để gia hạn.", 403);
    }
  }
  if (!hasFeatureAccess({ username: user.username, role: u.role, plan_scope: u.plan_scope }, feature)) {
    return err(feature === "video"
      ? "Gói hiện tại của bạn chưa mở tính năng video. Bạn chỉ cần nhắn admin một câu là bên mình sẽ hỗ trợ nâng cấp gói thật nhanh cho bạn."
      : "Gói hiện tại của bạn chưa mở tính năng tạo ảnh. Bạn nhắn admin giúp mình để được hỗ trợ nâng cấp gói phù hợp nhé.", 403);
  }
  const cookiePool = await getUserCookiePool(env, user.user_id, u.cookie_quota);
  if (!cookiePool.length) return err("Chưa có cookie nào. Vui lòng thêm cookie trước.");
  return json({
    cookie: cookiePool[0],
    cookie_pool: cookiePool,
    vps_base: VPS,
    feature,
  });
}

async function addUserCookie(request, env) {
  const [user, e] = await requireUser(request, env);
  if (e) return e;
  await ensureUserSchema(env);
  const { cookie_raw, cookie_hash, email, status } = await request.json();
  if (!cookie_raw || !cookie_hash) return err("Thiếu cookie");
  const info = await env.DB.prepare("SELECT cookie_quota FROM users WHERE id=?").bind(user.user_id).first();
  const countRow = await env.DB.prepare("SELECT COUNT(*) as count FROM user_cookies WHERE user_id=?").bind(user.user_id).first();
  const quota = info?.cookie_quota ?? 5;
  if ((countRow?.count || 0) >= quota) return err(`Tài khoản chỉ được lưu tối đa ${quota} cookie`);
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

async function ensureHistorySchema(env) {
  try { await env.DB.prepare("ALTER TABLE gen_history ADD COLUMN file_name TEXT DEFAULT ''").run(); } catch (e) {}
  try { await env.DB.prepare("ALTER TABLE gen_history ADD COLUMN media_url TEXT").run(); } catch (e) {}
  try { await env.DB.prepare("ALTER TABLE gen_history ADD COLUMN media_type TEXT DEFAULT 'image'").run(); } catch (e) {}
  try { await env.DB.prepare("CREATE INDEX IF NOT EXISTS idx_history_user_media_job ON gen_history(user_id, media_type, job_id, id DESC)").run(); } catch (e) {}
  try { await env.DB.prepare("CREATE INDEX IF NOT EXISTS idx_history_user_media_file ON gen_history(user_id, media_type, job_id, file_name, id DESC)").run(); } catch (e) {}
  try { await env.DB.prepare("CREATE INDEX IF NOT EXISTS idx_history_cleanup ON gen_history(created_at)").run(); } catch (e) {}
  try { await env.DB.prepare("CREATE UNIQUE INDEX IF NOT EXISTS idx_history_unique_media ON gen_history(user_id, job_id, media_type, media_url)").run(); } catch (e) {}
}

async function getHistoryGroups(request, env, url) {
  const [user, e] = await requireUser(request, env);
  if (e) return e;
  await ensureHistorySchema(env);
  // Auto-cleanup history older than 24h
  await env.DB.prepare("DELETE FROM gen_history WHERE created_at < datetime('now','-24 hours')").run();
  const limit = parseInt(url.searchParams.get("limit") || "50");
  const offset = parseInt(url.searchParams.get("offset") || "0");
  const mediaType = url.searchParams.get("media_type") || "image";
  const { results } = await env.DB.prepare(
    `SELECT job_id, COALESCE(batch_name,'') as batch_name, model, COUNT(*) as count, MAX(created_at) as created_at
     FROM gen_history WHERE user_id=? AND COALESCE(media_type,'image')=? AND COALESCE(media_url,image_url) IS NOT NULL AND COALESCE(media_url,image_url)!=''
     GROUP BY job_id ORDER BY MAX(id) DESC LIMIT ? OFFSET ?`
  ).bind(user.user_id, mediaType, limit, offset).all();
  return json(results);
}

async function getHistorySubgroups(request, env, url) {
  const [user, e] = await requireUser(request, env);
  if (e) return e;
  await ensureHistorySchema(env);
  const jobId = url.searchParams.get("job_id");
  const mediaType = url.searchParams.get("media_type") || "image";
  if (!jobId) return err("Missing job_id");
  const { results } = await env.DB.prepare(
    `SELECT COALESCE(file_name,'') as file_name, COUNT(*) as count, MAX(created_at) as created_at
     FROM gen_history WHERE user_id=? AND job_id=? AND COALESCE(media_type,'image')=? AND COALESCE(media_url,image_url) IS NOT NULL AND COALESCE(media_url,image_url)!=''
     GROUP BY file_name ORDER BY MIN(id)`
  ).bind(user.user_id, jobId, mediaType).all();
  return json(results);
}

async function getUserHistory(request, env, url) {
  const [user, e] = await requireUser(request, env);
  if (e) return e;
  await ensureHistorySchema(env);
  const limit = parseInt(url.searchParams.get("limit") || "100");
  const offset = parseInt(url.searchParams.get("offset") || "0");
  const jobId = url.searchParams.get("job_id");
  const fileName = url.searchParams.get("file_name");
  const mediaType = url.searchParams.get("media_type") || "image";
  if (jobId && fileName !== null) {
    const { results } = await env.DB.prepare(
      "SELECT id,prompt,model,image_url,COALESCE(media_url,image_url) as media_url,COALESCE(media_type,'image') as media_type,created_at FROM gen_history WHERE user_id=? AND job_id=? AND COALESCE(file_name,'')=? AND COALESCE(media_type,'image')=? AND COALESCE(media_url,image_url) IS NOT NULL AND COALESCE(media_url,image_url)!='' ORDER BY id LIMIT ? OFFSET ?"
    ).bind(user.user_id, jobId, fileName || "", mediaType, limit, offset).all();
    return json(results);
  }
  if (jobId) {
    const { results } = await env.DB.prepare(
      "SELECT id,job_id,prompt,model,image_url,COALESCE(media_url,image_url) as media_url,COALESCE(media_type,'image') as media_type,created_at FROM gen_history WHERE user_id=? AND job_id=? AND COALESCE(media_type,'image')=? AND COALESCE(media_url,image_url) IS NOT NULL AND COALESCE(media_url,image_url)!='' ORDER BY id DESC LIMIT ? OFFSET ?"
    ).bind(user.user_id, jobId, mediaType, limit, offset).all();
    return json(results);
  }
  const { results } = await env.DB.prepare(
    "SELECT id,job_id,prompt,model,image_url,COALESCE(media_url,image_url) as media_url,COALESCE(media_type,'image') as media_type,created_at FROM gen_history WHERE user_id=? AND COALESCE(media_type,'image')=? AND COALESCE(media_url,image_url) IS NOT NULL AND COALESCE(media_url,image_url)!='' ORDER BY id DESC LIMIT ? OFFSET ?"
  ).bind(user.user_id, mediaType, limit, offset).all();
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
  await ensureHistorySchema(env);
  const mediaType = new URL(request.url).searchParams.get("media_type");
  if (mediaType) {
    await env.DB.prepare("DELETE FROM gen_history WHERE user_id=? AND COALESCE(media_type,'image')=? AND (COALESCE(media_url,image_url) IS NULL OR COALESCE(media_url,image_url)='')").bind(user.user_id, mediaType).run();
  } else {
    await env.DB.prepare("DELETE FROM gen_history WHERE user_id=? AND (COALESCE(media_url,image_url) IS NULL OR COALESCE(media_url,image_url)='')").bind(user.user_id).run();
  }
  return json({ ok: true });
}

async function saveHistory(request, env) {
  const [user, e] = await requireUser(request, env);
  if (e) return e;
  await ensureHistorySchema(env);
  const { items } = await request.json();
  if (!items?.length) return err("Không có dữ liệu");
  const stmt = env.DB.prepare(
    "INSERT OR IGNORE INTO gen_history(user_id,job_id,prompt,model,image_url,media_url,media_type,batch_name,file_name) VALUES(?,?,?,?,?,?,?,?,?)"
  );
  const batch = items.map(i => {
    const mediaType = i.media_type || (i.video_url ? "video" : "image");
    const mediaUrl = i.media_url || i.video_url || i.image_url || null;
    const imageUrl = mediaType === "image" ? (i.image_url || mediaUrl) : null;
    return stmt.bind(user.user_id, i.job_id || "", i.prompt || "", i.model || "", imageUrl, mediaUrl, mediaType, i.batch_name || "", i.file_name || "");
  });
  await env.DB.batch(batch);
  return json({ ok: true, saved: items.length });
}

async function ensureUserSchema(env) {
  try { await env.DB.prepare("ALTER TABLE users ADD COLUMN cookie_quota INTEGER DEFAULT 5").run(); } catch (e) {}
  try { await env.DB.prepare("ALTER TABLE users ADD COLUMN plan_scope TEXT DEFAULT 'both'").run(); } catch (e) {}
  try { await env.DB.prepare("UPDATE users SET plan_scope='both' WHERE plan_scope IS NULL OR plan_scope=''").run(); } catch (e) {}
  try { await env.DB.prepare("UPDATE users SET cookie_quota=20 WHERE username=?").bind(SUPER_ADMIN_USERNAME).run(); } catch (e) {}
}

// ── Get cookie pool for generate (internal) ──
async function getUserCookiePool(env, user_id, quota = 5) {
  const { results } = await env.DB.prepare(
    "SELECT cookie_raw FROM user_cookies WHERE user_id=? AND status!='error' ORDER BY CASE WHEN status='ok' THEN 0 ELSE 1 END, id LIMIT ?"
  ).bind(user_id, Math.max(1, Math.min(50, parseInt(quota ?? 5) || 5))).all();
  return results.map(row => row.cookie_raw).filter(Boolean);
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
