const API_BASE = "https://banana-pro-api.kh431248.workers.dev";
const VPS_BASE = "https://api.sunnshineshop.asia";
const STORAGE_TOKEN_KEY = "bp_token";
const STORAGE_USER_KEY = "bp_user";
const LEGACY_TOKEN_KEYS = ["bp_token", "bp_image_token", "bp_video_token"];
const LEGACY_USER_KEYS = ["bp_user", "bp_image_user", "bp_video_user"];

// ── SweetAlert2 helpers ───────────────────────────────────────────────────────
const Toast = Swal.mixin({ toast: true, position: "top-end", showConfirmButton: false, timer: 2500, timerProgressBar: true });
function sAlert(text, icon = "info") { return Swal.fire({ text, icon, confirmButtonColor: "#16a34a" }); }
function sSuccess(text) { Toast.fire({ icon: "success", title: text }); }
function sError(text) { return Swal.fire({ text, icon: "error", confirmButtonColor: "#16a34a" }); }
async function sConfirm(text, title = "Xác nhận") { const r = await Swal.fire({ title, text, icon: "warning", showCancelButton: true, confirmButtonColor: "#16a34a", cancelButtonColor: "#6b7280", confirmButtonText: "Đồng ý", cancelButtonText: "Hủy" }); return r.isConfirmed; }
function roleLabel(role) { return role === "super_admin" ? "Chủ hệ thống" : role === "admin" ? "Quản trị viên" : "Người dùng"; }
const ADMIN_CONTACT_HTML = `
  <div style="margin-top:12px;display:flex;gap:10px;flex-wrap:wrap">
    <a href="https://zalo.me/0822922996" target="_blank" rel="noopener noreferrer" style="flex:1;min-width:170px;display:inline-flex;align-items:center;justify-content:center;gap:8px;padding:11px 14px;border-radius:14px;background:#e0f2fe;border:1px solid #7dd3fc;color:#0369a1;font-weight:700;text-decoration:none">
      <span>Zalo</span>
      <span>0822.922.996</span>
    </a>
    <a href="https://t.me/mavnhuy1" target="_blank" rel="noopener noreferrer" style="flex:1;min-width:170px;display:inline-flex;align-items:center;justify-content:center;gap:8px;padding:11px 14px;border-radius:14px;background:#eff6ff;border:1px solid #93c5fd;color:#1d4ed8;font-weight:700;text-decoration:none">
      <span>Telegram</span>
      <span>@mavnhuy1</span>
    </a>
  </div>`;
function planScopeLabel(scope) { return scope === "image" ? "Chỉ ảnh" : scope === "video" ? "Chỉ video" : "Ảnh + Video"; }
function hasFeatureAccess(feature) {
  if (!authUser) return false;
  if (["admin", "super_admin"].includes(authUser.role)) return true;
  const scope = authUser.plan_scope || "both";
  return scope === "both" || scope === feature;
}
function showUpgradePopup(feature = "video") {
  const targetLabel = feature === "video" ? "video" : "tạo ảnh";
  return Swal.fire({
    icon: "info",
    title: `Gói hiện tại chưa mở ${targetLabel}`,
    html: `<div style="text-align:left;font-size:0.92rem;line-height:1.7">
      <p>Anh/chị đang dùng gói <b>${planScopeLabel(authUser?.plan_scope || "both")}</b>, nên tính năng <b>${targetLabel}</b> tạm thời chưa khả dụng.</p>
      <p>Nếu anh/chị cần dùng thêm, chỉ cần nhắn admin giúp em một câu. Bên em sẽ hỗ trợ nâng cấp gói thật nhanh để mình dùng trọn bộ tính năng ạ.</p>
      <div style="margin-top:10px;background:#f8fafc;border:1px solid #e2e8f0;border-radius:16px;padding:12px 12px">
        <div style="font-weight:700;color:#0f172a;margin-bottom:4px">Liên hệ admin để nâng cấp nhanh</div>
        <div style="font-size:0.85rem;color:#64748b">Chọn kênh thuận tiện nhất cho anh/chị ở dưới đây.</div>
        ${ADMIN_CONTACT_HTML}
      </div>
    </div>`,
    confirmButtonColor: "#16a34a",
    confirmButtonText: "Đã hiểu",
  });
}

// ── Auth State ────────────────────────────────────────────────────────────────
let authToken = LEGACY_TOKEN_KEYS.map(k => localStorage.getItem(k)).find(Boolean) || "";
let authUser = null;
for (const key of LEGACY_USER_KEYS) {
  const raw = localStorage.getItem(key);
  if (!raw) continue;
  try { authUser = JSON.parse(raw); break; } catch (_) {}
}
if (authToken) localStorage.setItem(STORAGE_TOKEN_KEY, authToken);
if (authUser) localStorage.setItem(STORAGE_USER_KEY, JSON.stringify(authUser));
let authTab = "login";

// ── App State ─────────────────────────────────────────────────────────────────
let cookies = [];
let currentMode = "normal";
let currentJobId = null;
let pollInterval = null;
let paused = false;
let batchFiles = [];
let refImages = {};
let refDirImages = [];
let folderStructure = {};
let adminSection = "overview";
let adminHistoryMedia = "";
let adminHistoryState = { userId: null, username: "", jobId: null, batchName: "", fileName: null };
let adminCookieUserId = null;
const autoDownloadedJobIds = new Set();
const autoDownloadedImageUrls = new Set();
let autoDownloadImageGuideAcknowledged = localStorage.getItem("bp_img_dl_guide") === "1";
let autoDownloadEachImageEnabled = localStorage.getItem("bp_img_auto_each_download") !== "0";

// ── Init ──────────────────────────────────────────────────────────────────────
(async function init() {
  if (authToken && authUser) {
    try {
      const res = await apiFetch("/auth/me");
      if (res.ok) {
        const data = await res.json();
        authUser = data;
        localStorage.setItem(STORAGE_USER_KEY, JSON.stringify(data));
        showApp();
        return;
      }
    } catch (e) {}
    // Token expired
    clearAuth();
  }
  showLogin();
})();

// ── API Helper ────────────────────────────────────────────────────────────────
function apiFetch(path, opts = {}) {
  const headers = { "Content-Type": "application/json", ...(opts.headers || {}) };
  if (authToken) headers["Authorization"] = "Bearer " + authToken;
  return fetch(API_BASE + path, { ...opts, headers });
}

function vpsFetch(path, opts = {}) {
  return fetch(VPS_BASE + path, opts);
}

function mapServerErrorMessage(message, status, fallback = "Hệ thống đang gặp lỗi, vui lòng thử lại.") {
  const raw = String(message || "").trim();
  if (!raw) {
    if (status === 401) return "Phiên đăng nhập đã hết hạn, vui lòng đăng nhập lại để tiếp tục.";
    if (status === 403) return "Yêu cầu bị từ chối. Vui lòng kiểm tra quyền truy cập, gói dịch vụ hoặc cookie đang dùng.";
    if (status === 404) return "Không tìm thấy dữ liệu hoặc tính năng mà hệ thống đang cần.";
    if (status === 408) return "Máy chủ phản hồi quá lâu. Anh/chị vui lòng thử lại sau ít phút.";
    if (status === 409) return "Dữ liệu đang bị trùng hoặc xung đột, vui lòng kiểm tra lại rồi thử lại.";
    if (status === 429) return "Hệ thống đang xử lý quá nhiều yêu cầu. Anh/chị vui lòng chờ một chút rồi thử lại.";
    if (status >= 500) return "Máy chủ đang gặp sự cố tạm thời. Anh/chị vui lòng thử lại sau ít phút.";
    return fallback;
  }
  if (/^HTTP\s*\d+/i.test(raw)) return mapServerErrorMessage("", status || Number(raw.replace(/\D+/g, "")), fallback);
  if (/failed to fetch|networkerror|load failed|network request failed/i.test(raw)) return "Không thể kết nối tới máy chủ. Anh/chị vui lòng kiểm tra mạng rồi thử lại.";
  if (/unauthorized|jwt|token|phiên đăng nhập/i.test(raw)) return "Phiên đăng nhập đã hết hạn hoặc không hợp lệ. Vui lòng đăng nhập lại.";
  if (/forbidden|permission|not allowed|access denied/i.test(raw)) return "Yêu cầu bị từ chối. Tài khoản hiện chưa có quyền thực hiện thao tác này.";
  if (/not found|không tìm thấy/i.test(raw)) return raw;
  if (/too many requests|rate limit|429/i.test(raw)) return "Hệ thống đang bận vì có quá nhiều yêu cầu cùng lúc. Anh/chị vui lòng thử lại sau ít phút.";
  if (/vps error|upstream|bad gateway|gateway/i.test(raw)) return `Máy chủ xử lý đang gặp lỗi kết nối: ${raw}`;
  if (/timeout|timed out/i.test(raw)) return "Máy chủ xử lý quá lâu nên yêu cầu đã bị gián đoạn. Anh/chị vui lòng thử lại.";
  return raw;
}

async function parseApiError(res, fallback = "Hệ thống đang gặp lỗi, vui lòng thử lại.") {
  let payload = null;
  let text = "";
  try { payload = await res.clone().json(); } catch (_) {}
  if (!payload) {
    try { text = await res.clone().text(); } catch (_) {}
  }
  const message =
    payload?.detail ||
    payload?.error ||
    payload?.message ||
    payload?.msg ||
    text;
  return mapServerErrorMessage(message, res.status, fallback);
}

function parseCaughtError(error, fallback = "Hệ thống đang gặp lỗi, vui lòng thử lại.") {
  return mapServerErrorMessage(error?.message || error, 0, fallback);
}

// ── Auth ──────────────────────────────────────────────────────────────────────
function showLogin() {
  document.getElementById("loginScreen").style.display = "flex";
  document.getElementById("appScreen").style.display = "none";
}

function showApp() {
  document.getElementById("loginScreen").style.display = "none";
  document.getElementById("appScreen").style.display = "block";
  document.getElementById("userInfo").textContent = `${authUser.username} (${roleLabel(authUser.role)})`;
  // Show plan status
  const planEl = document.getElementById("planInfo");
  if (["admin", "super_admin"].includes(authUser.role)) { planEl.textContent = "Không giới hạn"; planEl.className = "plan-badge plan-active"; }
  else if (authUser.plan_active) {
    const exp = new Date(authUser.plan_expires_at);
    const days = Math.ceil((exp - new Date()) / 86400000);
    planEl.textContent = `📦 Còn ${days} ngày`;
    planEl.className = "plan-badge " + (days <= 3 ? "plan-expiring" : "plan-active");
  } else { planEl.textContent = "⛔ Hết hạn"; planEl.className = "plan-badge plan-expired"; }
  document.getElementById("navAdmin").style.display = ["admin", "super_admin"].includes(authUser.role) ? "" : "none";
  loadCookiesFromDB();
  const autoToggle = document.getElementById("autoDownloadEachImageToggle");
  if (autoToggle) autoToggle.checked = autoDownloadEachImageEnabled;
  if (autoDownloadEachImageEnabled && !autoDownloadImageGuideAcknowledged) {
    setTimeout(() => { showImageAutoDownloadGuide().catch(() => {}); }, 300);
  }
}

function switchAuthTab(tab) {
  authTab = tab;
  document.querySelectorAll(".login-tab").forEach(b => b.classList.remove("active"));
  document.querySelector(`.login-tab:nth-child(${tab === "login" ? 1 : 2})`).classList.add("active");
  document.getElementById("authSubmitBtn").textContent = tab === "login" ? "Đăng nhập" : "Đăng ký";
}

async function handleAuth(e) {
  e.preventDefault();
  const username = document.getElementById("authUsername").value.trim();
  const password = document.getElementById("authPassword").value;
  const errEl = document.getElementById("authError");
  errEl.style.display = "none";
  const endpoint = authTab === "login" ? "/auth/login" : "/auth/register";
  try {
    const res = await fetch(API_BASE + endpoint, {
      method: "POST",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify({ username, password }),
    });
    if (!res.ok) { errEl.textContent = await parseApiError(res, "Không thể đăng nhập vào hệ thống."); errEl.style.display = "block"; return false; }
    const data = await res.json();
    authToken = data.token;
    authUser = { username: data.username, role: data.role };
    localStorage.setItem(STORAGE_TOKEN_KEY, authToken);
    localStorage.setItem(STORAGE_USER_KEY, JSON.stringify(authUser));
    // Fetch plan info
    try { const me = await apiFetch("/auth/me"); if (me.ok) { authUser = await me.json(); localStorage.setItem(STORAGE_USER_KEY, JSON.stringify(authUser)); } } catch(_){}
    showApp();
  } catch (e) {
    errEl.textContent = parseCaughtError(e, "Không thể kết nối tới máy chủ để đăng nhập.");
    errEl.style.display = "block";
  }
  return false;
}

async function handleLogout() {
  await apiFetch("/auth/logout", { method: "POST" }).catch(() => {});
  clearAuth();
  showLogin();
}

function clearAuth() {
  authToken = "";
  authUser = null;
  LEGACY_TOKEN_KEYS.forEach(k => localStorage.removeItem(k));
  LEGACY_USER_KEYS.forEach(k => localStorage.removeItem(k));
}

function switchApp(url) {
  window.location.assign(url);
}

// ── Tabs ──────────────────────────────────────────────────────────────────────
function showTab(tab) {
  ["Generate", "History", "Admin"].forEach(t => {
    document.getElementById("tab" + t).style.display = t.toLowerCase() === tab ? "" : "none";
    const nav = document.getElementById("nav" + t);
    if (nav) nav.classList.toggle("active", t.toLowerCase() === tab);
  });
  if (tab === "history") loadHistory();
  if (tab === "admin" && ["admin", "super_admin"].includes(authUser?.role)) { switchAdminSection(adminSection || "overview"); }
}

// ── Cookie Management (D1) ───────────────────────────────────────────────────
function openCookieModal() { if (!hasFeatureAccess("image")) { showUpgradePopup("image"); return; } document.getElementById("cookieModal").style.display = "flex"; }
function closeCookieModal() { document.getElementById("cookieModal").style.display = "none"; }
function closeCookieModalOutside(e) { if (e.target.id === "cookieModal") closeCookieModal(); }

async function loadCookiesFromDB() {
  try {
    const res = await apiFetch("/user/cookies");
    if (res.ok) cookies = await res.json();
    renderCookieTable();
  } catch (e) {}
}

function updateCookieQuotaInfo() {
  const info = document.getElementById("cookieQuotaInfo");
  if (!info) return;
  const quota = cookies[0]?.cookie_quota ?? authUser?.cookie_quota ?? 5;
  info.textContent = `Đang dùng ${cookies.length}/${quota} cookie cho tài khoản này.`;
}

async function addCookie() {
  if (!hasFeatureAccess("image")) { showUpgradePopup("image"); return; }
  const raw = document.getElementById("newCookieInput").value.trim();
  if (!raw) return;
  const parsed = parseCookieInput(raw);
  if (!parsed || !Object.keys(parsed).length) { sAlert("Cookie không hợp lệ"); return; }
  const hash = cookieHash(parsed);
  const res = await apiFetch("/user/cookies", {
    method: "POST",
    body: JSON.stringify({ cookie_raw: raw, cookie_hash: hash }),
  });
  if (!res.ok) { sAlert(await parseApiError(res, "Không thể thêm cookie vào hệ thống."), "error"); return; }
  document.getElementById("newCookieInput").value = "";
  loadCookiesFromDB();
}

async function testCookie() {
  const raw = document.getElementById("newCookieInput").value.trim();
  if (!raw) return;
  const el = document.getElementById("cookieTestResult");
  el.textContent = "⏳ Đang test...";
  try {
    const res = await apiFetch("/test-cookie", {
      method: "POST",
      body: JSON.stringify({ cookie: raw }),
    });
    const data = await res.json();
    el.innerHTML = data.ok
      ? `<span class="status-ok">✅ Hợp lệ - ${data.email || ""}</span>`
      : `<span class="status-err">❌ ${data.error || "Không hợp lệ"}</span>`;
  } catch (e) {
    el.innerHTML = `<span class="status-err">❌ ${e.message}</span>`;
  }
}

async function deleteCookie(id) {
  await apiFetch(`/user/cookies/${id}`, { method: "DELETE" });
  loadCookiesFromDB();
}

async function clearAllCookies() {
  if (!await sConfirm("Xóa tất cả cookie?")) return;
  await apiFetch("/user/cookies/clear", { method: "DELETE" });
  loadCookiesFromDB();
}

function importCookieTxt() { document.getElementById("cookieTxtImport").click(); }
function loadCookieTxt(input) {
  const file = input.files[0];
  if (!file) return;
  const reader = new FileReader();
  reader.onload = async (e) => {
    const lines = e.target.result.split("\n").map(s => s.trim()).filter(Boolean);
    let added = 0;
    for (const line of lines) {
      const parsed = parseCookieInput(line);
      if (!parsed || !Object.keys(parsed).length) continue;
      const hash = cookieHash(parsed);
      const res = await apiFetch("/user/cookies", {
        method: "POST",
        body: JSON.stringify({ cookie_raw: line, cookie_hash: hash }),
      });
      if (res.ok) added++;
    }
    loadCookiesFromDB();
    sSuccess(`Đã thêm ${added} cookie`);
  };
  reader.readAsText(file);
}

function renderCookieTable() {
  const tbody = document.getElementById("cookieTableBody");
  if (!tbody) return;
  tbody.innerHTML = cookies.map((c, i) => `
    <tr>
      <td>${i + 1}</td>
      <td style="font-family:monospace;font-size:0.75rem">${(c.cookie_hash || "").slice(0, 12)}...</td>
      <td>${c.email || "-"}</td>
      <td><span class="status-${c.status === "ok" ? "ok" : c.status === "error" ? "err" : "pending"}">${c.status === "ok" ? "✅ OK" : c.status === "error" ? "❌ Lỗi" : "⏳"}</span></td>
      <td><button class="btn btn-red" style="padding:3px 8px;font-size:0.72rem" onclick="deleteCookie(${c.id})">Xóa</button></td>
    </tr>`).join("") || '<tr><td colspan="5" style="text-align:center;color:#6b7280;padding:16px">Chưa có cookie</td></tr>';
  updateCookieQuotaInfo();
}

// ── Mode ──────────────────────────────────────────────────────────────────────
function setMode(mode) {
  currentMode = mode;
  document.getElementById("folderStructureCard").style.display = mode === "folder" ? "block" : "none";
  document.getElementById("promptSourceCard").style.display = mode !== "folder" ? "block" : "none";
  const refDir = document.getElementById("refDirCard");
  if (refDir) refDir.style.display = mode === "normal" ? "block" : "none";
}

// ── Prompt Sources ────────────────────────────────────────────────────────────
function loadTxtFile(input) {
  const file = input.files[0]; if (!file) return;
  document.getElementById("txtFilePath").value = file.name;
  document.getElementById("folderTxtPath").value = "";
  batchFiles = [];
  const reader = new FileReader();
  reader.onload = e => {
    const prompts = e.target.result.split("\n").map(s => s.trim()).filter(Boolean);
    batchFiles.push({ name: file.name, prompts, status: "Chờ" });
    renderBatchTable();
  };
  reader.readAsText(file);
}

function loadFolderTxt(input) {
  const files = Array.from(input.files).filter(f => f.name.endsWith(".txt"));
  if (!files.length) return;
  document.getElementById("folderTxtPath").value = input.files[0].webkitRelativePath.split("/")[0];
  document.getElementById("txtFilePath").value = "";
  batchFiles = [];
  let loaded = 0;
  files.forEach(file => {
    const reader = new FileReader();
    reader.onload = e => {
      const prompts = e.target.result.split("\n").map(s => s.trim()).filter(Boolean);
      batchFiles = batchFiles.filter(f => f.name !== file.name);
      batchFiles.push({ name: file.name, prompts, status: "Chờ" });
      if (++loaded === files.length) renderBatchTable();
    };
    reader.readAsText(file);
  });
}

function clearSources() { batchFiles = []; document.getElementById("txtFilePath").value = ""; document.getElementById("folderTxtPath").value = ""; renderBatchTable(); }

function renderBatchTable() {
  const tbody = document.getElementById("batchTableBody");
  tbody.innerHTML = batchFiles.map((f, i) => `<tr id="batchRow${i}"><td>${i + 1}</td><td>${esc(f.name)}</td><td>${f.prompts.length}</td><td>${f.status}</td></tr>`).join("") || '<tr><td colspan="4" style="text-align:center;color:#6b7280;padding:12px">Chưa có file</td></tr>';
  // Auto-populate results table when files loaded (only if no active job)
  if (!currentJobId) populateResultsTable();
}

// ── Ref Images (per-row + bulk) ───────────────────────────────────────────────
let rowRefImages = {}; // {rowIndex: [base64...]}
let refImportTargetRow = -1;

function setRefFolder(type, input) {
  const files = Array.from(input.files).filter(f => /\.(jpg|jpeg|png|webp|gif)$/i.test(f.name));
  document.getElementById(`${type}Folder`).value = files.length ? input.files[0].webkitRelativePath.split("/")[0] : "";
  refImages[type] = [];
  files.forEach(file => { const r = new FileReader(); r.onload = e => refImages[type].push(e.target.result); r.readAsDataURL(file); });
}

function importRefAll() {
  refImportTargetRow = -1;
  document.getElementById("refBulkInput").click();
}
function importRefForRow(idx) {
  refImportTargetRow = idx;
  document.getElementById("refRowInput").click();
}
function handleRefBulkImport(input) {
  const files = Array.from(input.files); if (!files.length) return;
  const total = batchFiles.flatMap(f => f.prompts).length;
  let loaded = 0; const imgs = [];
  files.forEach(file => { const r = new FileReader(); r.onload = e => { imgs.push(e.target.result); if (++loaded === files.length) { for (let i = 0; i < total; i++) { if (!rowRefImages[i]) rowRefImages[i] = []; rowRefImages[i].push(...imgs); } refreshRefCells(); } }; r.readAsDataURL(file); });
  input.value = "";
}
function handleRefRowImport(input) {
  const files = Array.from(input.files); if (!files.length) return;
  const idx = refImportTargetRow; let loaded = 0;
  files.forEach(file => { const r = new FileReader(); r.onload = e => { if (!rowRefImages[idx]) rowRefImages[idx] = []; rowRefImages[idx].push(e.target.result); if (++loaded === files.length) refreshRefCells(); }; r.readAsDataURL(file); });
  input.value = "";
}
function clearRefAll() { rowRefImages = {}; refreshRefCells(); }
function refreshRefCells() {
  batchFiles.flatMap(f => f.prompts).forEach((_, i) => {
    const row = document.getElementById(`resRow${i}`); if (!row) return;
    const imgs = rowRefImages[i] || [];
    if (imgs.length) {
      const thumbs = imgs.map((s, j) =>
        `<span class="ref-wrap"><img src="${s}" class="ref-thumb" onclick="window.open(this.src)"/><span class="ref-del" onclick="removeRefImg(${i},${j})">✕</span></span>`
      ).join("");
      row.cells[2].innerHTML = `<div class="ref-cell">${thumbs}<span class="ref-add-btn" onclick="importRefForRow(${i})">+</span></div>`;
    } else {
      row.cells[2].innerHTML = `<span class="ref-add-btn" onclick="importRefForRow(${i})">+</span>`;
    }
  });
}

function removeRefImg(row, idx) {
  if (rowRefImages[row]) { rowRefImages[row].splice(idx, 1); if (!rowRefImages[row].length) delete rowRefImages[row]; }
  refreshRefCells();
}

function loadParentFolder(input) {
  const files = Array.from(input.files); folderStructure = {};
  files.forEach(file => {
    const parts = file.webkitRelativePath.split("/"); if (parts.length < 2) return;
    const sub = parts[1]; if (!folderStructure[sub]) folderStructure[sub] = { prompts: [], images: [] };
    if (file.name.endsWith(".txt")) { const r = new FileReader(); r.onload = e => { folderStructure[sub].prompts.push(...e.target.result.split("\n").map(s => s.trim()).filter(Boolean)); renderFolderStructureTable(); }; r.readAsText(file); }
    else if (/\.(jpg|jpeg|png|webp|gif)$/i.test(file.name)) { const r = new FileReader(); r.onload = e => folderStructure[sub].images.push(e.target.result); r.readAsDataURL(file); }
  });
  document.getElementById("parentFolder").value = files[0]?.webkitRelativePath.split("/")[0] || "";
}

function renderFolderStructureTable() {
  const tbody = document.getElementById("folderStructureBody");
  const entries = Object.entries(folderStructure);
  tbody.innerHTML = entries.map(([name, data], i) => `<tr><td>${i + 1}</td><td>${esc(name)}</td><td>${data.images.length}</td><td>${data.prompts.length}</td></tr>`).join("") || '<tr><td colspan="4" style="text-align:center;color:#6b7280;padding:12px">Chưa có folder</td></tr>';
}

// ── Generate ──────────────────────────────────────────────────────────────────
async function startGeneration() {
  if (!hasFeatureAccess("image")) { showUpgradePopup("image"); return; }
  if (!cookies.length) { showError("Vui lòng thêm cookie trong mục Quản lý Cookie."); return; }

  // Warn if cookie is older than 10 hours
  const oldest = cookies.reduce((min, c) => {
    const t = c.created_at ? new Date(c.created_at + "Z").getTime() : 0;
    return t && t < min ? t : min;
  }, Date.now());
  const hoursAgo = Math.floor((Date.now() - oldest) / 3600000);
  if (hoursAgo >= 10) {
    const ok = await sConfirm(`Cookie đã được thêm ${hoursAgo} giờ trước.\nCookie chỉ có hạn 10–24 giờ, có thể đã hết hạn.\nBạn nên lấy cookie mới trước khi tạo ảnh.`, "⚠️ Cookie có thể hết hạn");
    if (!ok) return;
  }

  let prompts = [], reference_images = [], folder_images = {};
  if (currentMode === "normal" || currentMode === "multiple") {
    prompts = batchFiles.flatMap(f => f.prompts);
    if (!prompts.length) { showError("Vui lòng chọn file .txt có prompts."); return; }
    reference_images = [];
    // Per-prompt ref images mapping: {"0": [base64...], "2": [base64...]}
    if (currentMode !== "multiple") {
      const perPrompt = {};
      let pi = 0;
      for (const f of batchFiles) {
        for (let j = 0; j < f.prompts.length; j++) {
          const rowIdx = batchFiles.slice(0, batchFiles.indexOf(f)).reduce((s, ff) => s + ff.prompts.length, 0) + j;
          if (rowRefImages[rowIdx] && rowRefImages[rowIdx].length) perPrompt[String(pi)] = rowRefImages[rowIdx];
          pi++;
        }
      }
      if (Object.keys(perPrompt).length) folder_images.__per_prompt_ref = perPrompt;
    }
  } else if (currentMode === "folder") {
    const entries = Object.entries(folderStructure);
    if (!entries.length) { showError("Vui lòng chọn folder cha."); return; }
    entries.forEach(([name, data]) => { prompts.push(...data.prompts); if (data.images.length) folder_images[name.toLowerCase()] = data.images; });
    if (!prompts.length) { showError("Không tìm thấy prompt nào."); return; }
  }

  const model = document.getElementById("modelSelect").value;
  const aspect_ratio = document.getElementById("aspectSelect").value;
  const variants = parseInt(document.getElementById("variantsInput").value) || 1;

  hideError(); setLoading(true); populateResultsTable(); paused = false;
  autoDownloadedImageUrls.clear();

  try {
    const resolution = document.getElementById("resolutionSelect").value;
    const ctxRes = await apiFetch("/user/generate-context?feature=image");
    if (!ctxRes.ok) throw new Error(await parseApiError(ctxRes, "Không thể lấy cấu hình chạy từ máy chủ."));
    const ctx = await ctxRes.json();

    const body = { prompts, model, aspect_ratio, variants, resolution, cookie: ctx.cookie, cookie_pool: ctx.cookie_pool || [] };
    if (reference_images.length) body.reference_images = reference_images;
    if (Object.keys(folder_images).length) body.folder_images = folder_images;

    const res = await vpsFetch("/generate", { method: "POST", headers: { "Content-Type": "application/json" }, body: JSON.stringify(body) });
    if (!res.ok) throw new Error(await parseApiError(res, "Không thể gửi yêu cầu tạo ảnh lên máy chủ."));
    const job = await res.json();
    currentJobId = job.job_id;
    updateProgress(job);
    startPolling(model);
  } catch (e) {
    showError(parseCaughtError(e, "Không thể bắt đầu tác vụ tạo ảnh."));
    setLoading(false);
  }
}

let pollFailCount = 0;

function startPolling(model) {
  pollFailCount = 0;
  if (pollInterval) {
    clearInterval(pollInterval);
    pollInterval = null;
  }
  pollInterval = setInterval(async () => {
    if (!currentJobId || paused) return;
    try {
      const res = await vpsFetch(`/jobs/${currentJobId}`);
      if (!res.ok) {
        pollFailCount++;
        if (pollFailCount >= 5) {
          clearInterval(pollInterval);
          currentJobId = null;
          setLoading(false);
          showError(await parseApiError(res, "Mất kết nối tới máy chủ trong lúc đang lấy tiến độ tạo ảnh."));
        }
        return;
      }
      pollFailCount = 0;
      const job = await res.json();
      updateProgress(job);
      updateResultsFromJob(job);
      if (autoDownloadEachImageEnabled) autoDownloadFinishedImages(job).catch(() => {});
      if (job.status === "done" || job.status === "error") {
        clearInterval(pollInterval);
        pollInterval = null;
        const finishedJobId = currentJobId;
        currentJobId = null;
        setLoading(false);
        if (job.status === "error") showError(mapServerErrorMessage(job.error, 500, "Máy chủ trả về lỗi khi đang tạo ảnh."));
        // Save history to D1 - only successful images, with source file name
        if (job.images?.length) {
          const folderName = getBatchName();
          const promptFileMap = [];
          const strip = n => n.replace(/\.txt$/i, "");
          const imgVariants = job.total > 0 && batchFiles.flatMap(f => f.prompts).length > 0
            ? Math.round(job.total / batchFiles.flatMap(f => f.prompts).length) : 1;
          batchFiles.forEach(f => f.prompts.forEach(() => {
            for (let v = 0; v < imgVariants; v++) promptFileMap.push(strip(f.name));
          }));
          const successItems = job.images.map((img, idx) => img.url ? {
            job_id: finishedJobId, prompt: img.prompt, model: model || "",
            image_url: img.url, batch_name: folderName, file_name: promptFileMap[idx] || "",
          } : null).filter(Boolean);
          if (successItems.length) apiFetch("/user/history", { method: "POST", body: JSON.stringify({ items: successItems }) }).catch(() => {});
          if (autoDownloadEachImageEnabled) {
            autoDownloadFinishedImages(job).catch(() => {});
          }
          if (finishedJobId && !autoDownloadedJobIds.has(finishedJobId)) {
            autoDownloadedJobIds.add(finishedJobId);
            autoDownloadZip(job.images).catch(() => {});
          }
        }
      }
    } catch (e) {
      pollFailCount++;
      if (pollFailCount >= 5) {
        clearInterval(pollInterval);
        pollInterval = null;
        currentJobId = null;
        setLoading(false);
        showError(parseCaughtError(e, "Mất kết nối tới máy chủ trong lúc đang lấy tiến độ tạo ảnh."));
      }
    }
  }, 2000);
}

async function autoDownloadZip(images) {
  // Show download setup guide once
  if (!localStorage.getItem("bp_dl_guide")) {
    await Swal.fire({
      title: "📥 Tự động tải ảnh",
      html: `<div style="text-align:left;font-size:0.88rem;line-height:1.6">
        <p>Ảnh sẽ tự động tải về dạng ZIP khi hoàn thành.</p>
        <p><b>Để Chrome không hỏi mỗi lần tải:</b></p>
        <ol style="padding-left:18px">
          <li>Mở <a href="#" onclick="navigator.clipboard.writeText('chrome://settings/downloads');this.textContent='Đã copy!';return false" style="cursor:pointer;color:#0369a1"><code>chrome://settings/downloads</code> (bấm để copy)</a> → dán vào thanh địa chỉ</li>
          <li>Tắt <b>"Hỏi vị trí lưu mỗi tệp trước khi tải xuống"</b></li>
          <li>Nếu Chrome chặn tải nhiều file → bấm <b>Cho phép</b> trên thanh địa chỉ</li>
        </ol>
      </div>`,
      icon: "info",
      confirmButtonColor: "#16a34a",
      confirmButtonText: "Đã hiểu!",
    });
    localStorage.setItem("bp_dl_guide", "1");
  }
  const items = images.filter(img => img && (img.url || img.upscaled));
  if (!items.length) return;
  try {
    if (!window.JSZip) {
      await new Promise((ok, fail) => { const s = document.createElement("script"); s.src = "https://cdnjs.cloudflare.com/ajax/libs/jszip/3.10.1/jszip.min.js"; s.onload = ok; s.onerror = fail; document.head.appendChild(s); });
    }
    const zip = new JSZip(); let idx = 0;
    for (const img of items) {
      try {
        const dlUrl = img.upscaled || img.url;
        const isRelative = dlUrl.startsWith("/");
        const resp = await fetch(isRelative ? VPS_BASE + dlUrl : dlUrl);
        const blob = await resp.blob();
        const ext = blob.type?.includes("png") ? "png" : "jpg";
        zip.file(`${String(++idx).padStart(3, "0")}.${ext}`, blob);
      } catch (e) { console.warn("Skip:", e); }
    }
    if (!idx) return;
    const content = await zip.generateAsync({ type: "blob" });
    const a = document.createElement("a");
    a.href = URL.createObjectURL(content);
    a.download = `${getBatchName()}_${new Date().toISOString().slice(0,10)}.zip`;
    a.click();
    URL.revokeObjectURL(a.href);
    sSuccess(`Đã tải ${idx} ảnh (ZIP)`);
  } catch (e) { console.error("Auto ZIP failed:", e); }
}

async function showImageAutoDownloadGuide() {
  await Swal.fire({
    title: "Tự động tải ảnh",
    html: `<div style="text-align:left;font-size:0.88rem;line-height:1.65">
      <p>Tính năng tự tải từng ảnh đang được bật sẵn, nên ảnh nào xong trước sẽ tự tải về ngay.</p>
      <p style="margin-top:8px"><b>Thiết lập một lần trong Chrome:</b></p>
      <ol style="padding-left:18px">
        <li>Mở <a href="chrome://settings/downloads" target="_blank" rel="noopener noreferrer" id="imageAutoDlLink" style="color:#0369a1;font-weight:700">cài đặt tải xuống</a></li>
        <li>Nếu Chrome chặn không cho website mở trang này, dán <code>chrome://settings/downloads</code> vào thanh địa chỉ</li>
        <li>Tắt <b>Hỏi vị trí lưu mỗi tệp trước khi tải xuống</b> để ảnh tự tải mượt hơn</li>
        <li>Mở tiếp <a href="chrome://settings/content/automaticDownloads" target="_blank" rel="noopener noreferrer" id="imageAutoMultiLink" style="color:#0369a1;font-weight:700">cài đặt tải nhiều tệp</a></li>
        <li>Bấm <b>Thêm</b> rồi nhập <code>https://banana-pro.liveyt.pro/</code> để cho phép website tải nhiều tệp tự động</li>
      </ol>
      <p style="margin-top:8px;color:#64748b">Chrome có thể chặn website mở trực tiếp trang <code>chrome://</code>. Nếu vậy, hệ thống sẽ copy sẵn đường dẫn để anh/chị dán nhanh.</p>
    </div>`,
    icon: "info",
    confirmButtonColor: "#16a34a",
    confirmButtonText: "Đã hiểu",
    didOpen: () => {
      const link = document.getElementById("imageAutoDlLink");
      if (link) {
        link.addEventListener("click", async () => {
          try { await navigator.clipboard.writeText("chrome://settings/downloads"); } catch (_) {}
          link.textContent = "Đã thử mở và copy sẵn đường dẫn";
        });
      }
      const multiLink = document.getElementById("imageAutoMultiLink");
      if (multiLink) {
        multiLink.addEventListener("click", async () => {
          try { await navigator.clipboard.writeText("chrome://settings/content/automaticDownloads"); } catch (_) {}
          multiLink.textContent = "Đã thử mở và copy sẵn đường dẫn";
        });
      }
    }
  });
  autoDownloadImageGuideAcknowledged = true;
  localStorage.setItem("bp_img_dl_guide", "1");
}

function toggleAutoDownloadEachImage(enabled) {
  autoDownloadEachImageEnabled = !!enabled;
  localStorage.setItem("bp_img_auto_each_download", autoDownloadEachImageEnabled ? "1" : "0");
  if (autoDownloadEachImageEnabled && !autoDownloadImageGuideAcknowledged) {
    showImageAutoDownloadGuide().catch(() => {});
  }
}

function buildImageFilename(url, index) {
  const ext = (() => {
    try {
      const pathname = new URL(url).pathname || "";
      const file = pathname.split("/").pop() || "";
      const match = file.match(/\.([a-z0-9]+)$/i);
      if (match) return match[1].toLowerCase();
    } catch (_) {}
    return "png";
  })();
  return buildPromptBasedFilename(index, getPromptTextByIndex(index), ext);
}

function sanitizeFilenamePart(value) {
  return String(value || "")
    .normalize("NFKD")
    .replace(/[^\w\s-]/g, "")
    .replace(/_/g, " ")
    .replace(/\s+/g, " ")
    .trim();
}

function compactPromptStem(prompt, maxLength = 15) {
  const clean = sanitizeFilenamePart(prompt);
  if (!clean) return "untitled";
  if (clean.length <= maxLength) return clean.replace(/\s+/g, "-");
  const before = clean.slice(0, maxLength + 1);
  const lastSpace = before.lastIndexOf(" ");
  if (lastSpace >= 8) return before.slice(0, lastSpace).replace(/\s+/g, "-");
  const afterSpace = clean.indexOf(" ", maxLength);
  if (afterSpace !== -1 && afterSpace <= maxLength + 6) {
    return clean.slice(0, afterSpace).replace(/\s+/g, "-");
  }
  return clean.slice(0, maxLength).replace(/\s+/g, "-");
}

function getPromptTextByIndex(index) {
  const allPrompts = batchFiles.flatMap(f => f.prompts);
  return allPrompts[index] || "";
}

function buildPromptBasedFilename(index, prompt, ext) {
  const stem = compactPromptStem(prompt, 15);
  return `${index + 1}_${stem}.${ext}`;
}

async function autoDownloadFinishedImages(job) {
  const items = (job.images || []).filter(img => img && (img.upscaled || img.url));
  let idx = 0;
  for (const img of items) {
    const dlUrl = img.upscaled || img.url;
    if (!dlUrl || autoDownloadedImageUrls.has(dlUrl)) continue;
    try {
      autoDownloadedImageUrls.add(dlUrl);
      const isRelative = dlUrl.startsWith("/");
      const resp = await fetch(isRelative ? VPS_BASE + dlUrl : dlUrl);
      const blob = await resp.blob();
      const objectUrl = URL.createObjectURL(blob);
      const a = document.createElement("a");
      a.href = objectUrl;
      a.download = buildImageFilename(dlUrl, idx);
      document.body.appendChild(a);
      a.click();
      a.remove();
      URL.revokeObjectURL(objectUrl);
      idx++;
      await new Promise(resolve => setTimeout(resolve, 120));
    } catch (_) {
      autoDownloadedImageUrls.delete(dlUrl);
    }
  }
}

function getBatchName() {
  const strip = n => n.replace(/\.(txt|png|jpg|jpeg|webp)$/i, "");
  if (currentMode === "folder") {
    const subs = Object.keys(folderStructure);
    if (subs.length === 1) return subs[0];
    return subs.length ? subs.join(", ") : "Folder";
  }
  const names = batchFiles.map(f => strip(f.name));
  return names.length ? names.join(", ") : "Untitled";
}

function togglePause() { paused = !paused; document.getElementById("pauseBtn").textContent = paused ? "Tiếp tục" : "Tạm dừng"; }

async function stopGeneration() {
  if (!currentJobId) return;
  clearInterval(pollInterval);
  await vpsFetch(`/jobs/${currentJobId}`, { method: "DELETE" }).catch(() => {});
  currentJobId = null;
  setLoading(false);
  document.getElementById("progressText").textContent = "Đã dừng.";
  batchFiles.forEach(f => { if (f.status.includes("🔄")) f.status = "⏹ Dừng"; });
  renderBatchTable();
}

// ── History ───────────────────────────────────────────────────────────────────
let historyPage = 0;
let historyLoading = false;
const HISTORY_LIMIT = 50;
let historyView = "folders"; // "folders" | "files" | "images"
let historyCurrentJob = null;
let historyCurrentFile = null;
let historyBreadcrumb = []; // [{label, action}]

async function loadHistory() {
  historyPage = 0;
  historyView = "folders";
  historyCurrentJob = null;
  historyCurrentFile = null;
  document.getElementById("historySelectAll").checked = false;
  await fetchHistoryPage(false);
}

async function fetchHistoryPage(append) {
  if (historyLoading) return;
  historyLoading = true;
  const grid = document.getElementById("historyGrid");
  const prevBtn = document.getElementById("historyPrev");
  const nextBtn = document.getElementById("historyNext");
  const pageInfo = document.getElementById("historyPageInfo");

  if (!append) grid.innerHTML = '<div class="empty-state">⏳ Đang tải...</div>';
  updateHistoryBreadcrumb();

  try {
    if (historyView === "folders") {
      const res = await apiFetch(`/user/history/groups?limit=${HISTORY_LIMIT}&offset=${historyPage * HISTORY_LIMIT}`);
      if (!res.ok) throw new Error(await parseApiError(res, "Không thể tải danh sách thư mục lịch sử."));
      const groups = await res.json();
      renderHistoryFolders(groups, grid, "📁", g => g.batch_name || g.job_id.slice(0, 8), g => `${g.model || ""} · ${g.count} ảnh`, g => openHistoryJob(g.job_id, g.batch_name || g.job_id), g => deleteHistoryJob(g.job_id));
      prevBtn.disabled = historyPage === 0;
      nextBtn.disabled = groups.length < HISTORY_LIMIT;
    } else if (historyView === "files") {
      const res = await apiFetch(`/user/history/subgroups?job_id=${encodeURIComponent(historyCurrentJob)}`);
      if (!res.ok) throw new Error(await parseApiError(res, "Không thể tải danh sách file trong lịch sử."));
      const subs = await res.json();
      // If only 1 file (or no file_name), skip to images directly
      if (subs.length <= 1) {
        historyView = "images";
        historyCurrentFile = subs[0]?.file_name || "";
        return fetchHistoryPage(false);
      }
      renderHistoryFolders(subs, grid, "📄", s => s.file_name || "Untitled", s => `${s.count} ảnh`, s => openHistoryFile(s.file_name), null);
      prevBtn.disabled = true; nextBtn.disabled = true;
    } else {
      const params = `job_id=${encodeURIComponent(historyCurrentJob)}&file_name=${encodeURIComponent(historyCurrentFile || "")}&limit=${HISTORY_LIMIT}&offset=${historyPage * HISTORY_LIMIT}`;
      const res = await apiFetch(`/user/history?${params}`);
      if (!res.ok) throw new Error(await parseApiError(res, "Không thể tải ảnh trong lịch sử."));
      const items = await res.json();
      renderHistoryGrid(items, grid, false);
      prevBtn.disabled = historyPage === 0;
      nextBtn.disabled = items.length < HISTORY_LIMIT;
    }
    pageInfo.textContent = `Trang ${historyPage + 1}`;
  } catch (e) {
    grid.innerHTML = `<div class="empty-state">❌ ${esc(parseCaughtError(e, "Không thể tải lịch sử lúc này."))}</div>`;
  } finally {
    historyLoading = false;
  }
}

function renderHistoryFolders(items, grid, icon, getName, getMeta, onClick, onDelete) {
  grid.innerHTML = ""; grid.classList.remove("grid-mode");
  if (!items.length) { grid.innerHTML = '<div class="empty-state"><div class="empty-icon">📜</div><div>Chưa có lịch sử</div></div>'; return; }
  items.forEach((g, i) => {
    const card = document.createElement("div");
    card.className = "hist-folder";
    const time = g.created_at ? new Date(g.created_at + "Z").toLocaleString("vi-VN") : "";
    const delId = g.job_id || g.file_name || i;
    const delBtn = onDelete ? `<button class="btn btn-red btn-sm hist-folder-del" onclick="event.stopPropagation();deleteHistoryJob('${esc(delId)}')" title="Xóa">🗑</button>` : "";
    card.innerHTML = `
      <div class="hist-folder-icon">${icon}</div>
      <div class="hist-folder-info">
        <div class="hist-folder-name">${esc(getName(g))}</div>
        <div class="hist-folder-meta">${esc(getMeta(g))} · ${time}</div>
      </div>${delBtn}`;
    card.onclick = () => onClick(g);
    grid.appendChild(card);
  });
}

function updateHistoryBreadcrumb() {
  const el = document.getElementById("historyTitle");
  const backBtn = document.getElementById("historyBack");
  if (historyView === "folders") {
    el.textContent = "📜 Lịch sử tạo ảnh";
    backBtn.style.display = "none";
  } else {
    backBtn.style.display = "";
    let parts = ["📜 Lịch sử"];
    if (historyCurrentJob) parts.push(historyBreadcrumb[0] || "");
    if (historyView === "images" && historyCurrentFile) parts.push(historyCurrentFile);
    el.textContent = parts.filter(Boolean).join(" › ");
  }
}

function openHistoryJob(jobId, name) {
  historyView = "files";
  historyCurrentJob = jobId;
  historyCurrentFile = null;
  historyBreadcrumb = [name];
  historyPage = 0;
  document.getElementById("historySelectAll").checked = false;
  fetchHistoryPage(false);
}

function openHistoryFile(fileName) {
  historyView = "images";
  historyCurrentFile = fileName;
  historyPage = 0;
  document.getElementById("historySelectAll").checked = false;
  fetchHistoryPage(false);
}

function historyGoBack() {
  if (historyView === "images" && historyCurrentFile !== null) {
    // Go back to files level (unless it was auto-skipped)
    historyView = "files";
    historyCurrentFile = null;
    historyPage = 0;
  } else {
    historyView = "folders";
    historyCurrentJob = null;
    historyCurrentFile = null;
    historyBreadcrumb = [];
    historyPage = 0;
  }
  document.getElementById("historySelectAll").checked = false;
  fetchHistoryPage(false);
}

async function deleteHistoryJob(jobId) {
  if (!await sConfirm("Xóa toàn bộ ảnh trong batch này?")) return;
  try {
    await apiFetch(`/user/history/${encodeURIComponent(jobId)}`, { method: "DELETE" });
    fetchHistoryPage(false);
  } catch (e) { sAlert("Lỗi xóa"); }
}

async function deleteHistoryFailed() {
  if (!await sConfirm("Xóa tất cả lịch sử lỗi (không có ảnh)?")) return;
  try {
    await apiFetch("/user/history/failed", { method: "DELETE" });
    fetchHistoryPage(false);
  } catch (e) { sAlert("Lỗi xóa"); }
}

function historyPrevPage() {
  if (historyPage > 0) { historyPage--; document.getElementById("historySelectAll").checked = false; fetchHistoryPage(false); }
}

function historyNextPage() {
  historyPage++;
  document.getElementById("historySelectAll").checked = false;
  fetchHistoryPage(false);
}

function renderHistoryGrid(items, grid, append) {
  if (!append) { grid.innerHTML = ""; grid.classList.add("grid-mode"); }
  if (!items.length && !append) { grid.innerHTML = '<div class="empty-state"><div class="empty-icon">📜</div><div>Chưa có ảnh</div></div>'; return; }
  items.forEach(item => {
    const card = document.createElement("div");
    card.className = "img-card";
    const time = item.created_at ? new Date(item.created_at + "Z").toLocaleString("vi-VN") : "";
    const cbHtml = item.image_url ? `<label class="hist-cb"><input type="checkbox" class="hist-select" data-url="${item.image_url}"/></label>` : "";
    if (item.image_url) {
      card.innerHTML = `${cbHtml}
        <img src="${item.image_url}" alt="${esc(item.prompt)}" loading="lazy" onerror="this.style.display='none'"/>
        <div class="img-card-body">
          <div class="img-card-prompt">${esc(item.prompt)}</div>
          <div style="font-size:0.68rem;color:var(--muted);margin-top:4px">${esc(item.model)} · ${time}</div>
        </div>
        <div class="img-card-actions">
          <a href="${item.image_url}" target="_blank">🔗 Mở</a>
        </div>`;
    } else {
      card.innerHTML = `<div class="img-card-error">❌ ${esc(item.error || "Thất bại")}</div>
        <div class="img-card-body"><div class="img-card-prompt">${esc(item.prompt)}</div><div style="font-size:0.68rem;color:var(--muted);margin-top:4px">${esc(item.model)} · ${time}</div></div>`;
    }
    grid.appendChild(card);
  });
}

function historyToggleAll(checked) {
  document.querySelectorAll("#historyGrid .hist-select").forEach(cb => cb.checked = checked);
}

function getHistoryUrls(selectedOnly) {
  if (selectedOnly) return [...document.querySelectorAll("#historyGrid .hist-select:checked")].map(cb => cb.dataset.url);
  return [...document.querySelectorAll("#historyGrid .hist-select")].map(cb => cb.dataset.url);
}

async function historyDownloadZip(urls, btnId) {
  if (!urls.length) { sAlert("Không có ảnh để tải"); return; }
  const btn = document.getElementById(btnId);
  const origText = btn?.textContent;
  if (btn) { btn.disabled = true; btn.textContent = "⏳ Đang gom..."; }
  try {
    if (!window.JSZip) {
      await new Promise((ok, fail) => { const s = document.createElement("script"); s.src = "https://cdnjs.cloudflare.com/ajax/libs/jszip/3.10.1/jszip.min.js"; s.onload = ok; s.onerror = fail; document.head.appendChild(s); });
    }
    const zip = new JSZip(); let idx = 0;
    for (const url of urls) {
      try {
        if (btn) btn.textContent = `⏳ ${idx + 1}/${urls.length}...`;
        const resp = await fetch(url); const blob = await resp.blob();
        const ext = blob.type?.includes("png") ? "png" : blob.type?.includes("webp") ? "webp" : "jpg";
        zip.file(`image_${String(++idx).padStart(3, "0")}.${ext}`, blob);
      } catch (e) { console.warn("Skip:", url, e); }
    }
    if (!idx) { sAlert("Không tải được ảnh nào"); return; }
    if (btn) btn.textContent = "⏳ Nén ZIP...";
    const content = await zip.generateAsync({ type: "blob" });
    const a = document.createElement("a"); a.href = URL.createObjectURL(content);
    a.download = `history_${new Date().toISOString().slice(0, 10)}.zip`;
    a.click(); URL.revokeObjectURL(a.href);
  } catch (e) { sAlert("Lỗi tải ZIP: " + e.message); }
  finally { if (btn) { btn.textContent = origText; btn.disabled = false; } }
}

function historyDownloadAll() { historyDownloadZip(getHistoryUrls(false), "histDlAll"); }
function historyDownloadSelected() { historyDownloadZip(getHistoryUrls(true), "histDlSel"); }

// ── Admin ─────────────────────────────────────────────────────────────────────
async function loadAdminUsers() {
  try {
    const res = await apiFetch("/admin/users");
    if (!res.ok) return;
    const users = await res.json();
    const now = new Date().toISOString();
    updateAdminDashboard(users);
    loadAdminTracking(users);
    const tbody = document.getElementById("adminUserBody");
    tbody.innerHTML = users.map(u => {
      const planActive = ["admin", "super_admin"].includes(u.role) || (u.plan_expires_at && u.plan_expires_at > now);
      const planText = ["admin", "super_admin"].includes(u.role) ? "Không giới hạn" : u.plan_expires_at ? new Date(u.plan_expires_at).toLocaleDateString("vi-VN") : "Chưa có";
      const planScopeControl = ["admin", "super_admin"].includes(authUser?.role)
        ? `<select onchange="adminSetPlanScope(${u.id}, this.value)">
            <option value="image" ${u.plan_scope === "image" ? "selected" : ""}>Chỉ ảnh</option>
            <option value="video" ${u.plan_scope === "video" ? "selected" : ""}>Chỉ video</option>
            <option value="both" ${(u.plan_scope || "both") === "both" ? "selected" : ""}>Ảnh + Video</option>
          </select>`
        : `<span>${planScopeLabel(u.plan_scope || "both")}</span>`;
      const planClass = planActive ? "status-ok" : "status-err";
      const quotaControl = ["admin", "super_admin"].includes(authUser?.role)
        ? `<input type="number" min="1" max="50" value="${u.cookie_quota ?? 5}" onchange="adminSetCookieQuota(${u.id}, this.value)" style="width:84px"/>`
        : `<span>${u.cookie_quota ?? 5}</span>`;
      return `<tr>
      <td>${u.id}</td><td>${esc(u.username)}</td>
      <td><select onchange="adminChangeRole(${u.id},this.value)" ${u.username === "adminveo" ? "disabled" : ""}>
        <option value="user" ${u.role === "user" ? "selected" : ""}>Người dùng</option>
        <option value="admin" ${u.role === "admin" ? "selected" : ""}>Quản trị viên</option>
        <option value="super_admin" ${u.role === "super_admin" ? "selected" : ""}>Chủ hệ thống</option>
      </select></td>
      <td>${quotaControl}</td>
      <td>${planScopeControl}<div style="margin-top:6px"><span class="${planClass}">${planText}</span></div>
        ${!["admin", "super_admin"].includes(u.role) ? `<div style="margin-top:4px;display:flex;gap:4px;flex-wrap:wrap">
          <button class="btn btn-ghost" style="padding:2px 6px;font-size:0.68rem" onclick="adminSetPlan(${u.id},7)">+7d</button>
          <button class="btn btn-ghost" style="padding:2px 6px;font-size:0.68rem" onclick="adminSetPlan(${u.id},30)">+30d</button>
          <button class="btn btn-ghost" style="padding:2px 6px;font-size:0.68rem" onclick="adminSetPlan(${u.id},90)">+90d</button>
          <button class="btn btn-ghost" style="padding:2px 6px;font-size:0.68rem;color:var(--error)" onclick="adminSetPlan(${u.id},0)">Hủy</button>
        </div>` : ""}
      </td>
      <td>${u.disabled ? '<span class="status-err">Đã khóa</span>' : '<span class="status-ok">Hoạt động</span>'}</td>
      <td>
        <button class="btn btn-ghost" onclick="adminEditUser(${u.id},'${esc(u.username)}',${u.cookie_quota ?? 5},'${esc(u.plan_scope || "both")}')">Sửa</button>
        <button class="btn btn-ghost" onclick="adminToggleUser(${u.id},${u.disabled ? 0 : 1})">${u.disabled ? "Mở khóa" : "Khóa"}</button>
        ${u.username !== "adminveo" ? `<button class="btn btn-red btn-sm" onclick="adminDelUser(${u.id})">Xóa</button>` : ""}
      </td>
    </tr>`;
    }).join("");
    const planBody = document.getElementById("adminPlanBody");
    if (planBody) {
      planBody.innerHTML = users.map(u => {
        const planActive = ["admin", "super_admin"].includes(u.role) || (u.plan_expires_at && u.plan_expires_at > now);
        const planText = ["admin", "super_admin"].includes(u.role) ? "Không giới hạn" : u.plan_expires_at ? new Date(u.plan_expires_at).toLocaleDateString("vi-VN") : "Chưa có";
        return `<tr>
          <td>${u.id}</td>
          <td>${esc(u.username)}</td>
          <td>${roleLabel(u.role)}</td>
          <td><select onchange="adminSetPlanScope(${u.id}, this.value)">
            <option value="image" ${u.plan_scope === "image" ? "selected" : ""}>Chỉ ảnh</option>
            <option value="video" ${u.plan_scope === "video" ? "selected" : ""}>Chỉ video</option>
            <option value="both" ${(u.plan_scope || "both") === "both" ? "selected" : ""}>Ảnh + Video</option>
          </select></td>
          <td><span class="${planActive ? "status-ok" : "status-err"}">${planText}</span></td>
          <td>
            ${!["admin", "super_admin"].includes(u.role) ? `
            <button class="btn btn-ghost" style="padding:2px 6px;font-size:0.68rem" onclick="adminSetPlan(${u.id},7)">+7 ngày</button>
            <button class="btn btn-ghost" style="padding:2px 6px;font-size:0.68rem" onclick="adminSetPlan(${u.id},30)">+30 ngày</button>
            <button class="btn btn-ghost" style="padding:2px 6px;font-size:0.68rem" onclick="adminSetPlan(${u.id},90)">+90 ngày</button>
            <button class="btn btn-ghost" style="padding:2px 6px;font-size:0.68rem;color:var(--error)" onclick="adminSetPlan(${u.id},0)">Hủy gói</button>` : "—"}
          </td>
        </tr>`;
      }).join("");
    }
  } catch (e) {}
}

async function loadAdminTracking(fallbackUsers = []) {
  const body = document.getElementById("adminTrackingBody");
  if (!body) return;
  try {
    const res = await apiFetch("/admin/users/activity-summary");
    if (!res.ok) throw new Error("activity-summary failed");
    const rows = await res.json();
    const now = new Date().toISOString();
    body.innerHTML = rows.map(row => {
      const role = roleLabel(row.role);
      const planActive = ["admin", "super_admin"].includes(row.role) || (row.plan_expires_at && row.plan_expires_at > now);
      const planText = ["admin", "super_admin"].includes(row.role)
        ? "Không giới hạn"
        : row.plan_expires_at
          ? new Date(row.plan_expires_at).toLocaleDateString("vi-VN")
          : "Chưa có";
      const createdAt = row.created_at ? new Date(row.created_at + "Z").toLocaleDateString("vi-VN") : "—";
      const lastActivity = row.last_activity_at ? new Date(row.last_activity_at + "Z").toLocaleString("vi-VN") : "Chưa phát sinh";
      return `<tr>
        <td><strong>${esc(row.username)}</strong></td>
        <td>${role}</td>
        <td>${row.image_count || 0}</td>
        <td>${row.video_count || 0}</td>
        <td>${row.cookie_count || 0} / ${row.cookie_quota ?? 0}</td>
        <td><span class="${planActive ? "status-ok" : "status-err"}">${planText}</span></td>
        <td>${createdAt}</td>
        <td>${lastActivity}</td>
      </tr>`;
    }).join("") || '<tr><td colspan="8" style="text-align:center;padding:16px">Chưa có dữ liệu</td></tr>';
  } catch (_) {
    body.innerHTML = (fallbackUsers || []).map(row => `<tr>
      <td><strong>${esc(row.username)}</strong></td>
      <td>${roleLabel(row.role)}</td>
      <td>0</td>
      <td>0</td>
      <td>0 / ${row.cookie_quota ?? 0}</td>
      <td>${row.plan_expires_at ? new Date(row.plan_expires_at).toLocaleDateString("vi-VN") : (["admin", "super_admin"].includes(row.role) ? "Không giới hạn" : "Chưa có")}</td>
      <td>${row.created_at ? new Date(row.created_at).toLocaleDateString("vi-VN") : "—"}</td>
      <td>Chưa phát sinh</td>
    </tr>`).join("") || '<tr><td colspan="8" style="text-align:center;padding:16px">Không tải được dữ liệu tracking</td></tr>';
  }
}

function updateAdminDashboard(users) {
  const now = new Date().toISOString();
  const totalUsers = users.length;
  const activePlans = users.filter(u => ["admin", "super_admin"].includes(u.role) || (u.plan_expires_at && u.plan_expires_at > now)).length;
  const lockedUsers = users.filter(u => !!u.disabled).length;
  const admins = users.filter(u => ["admin", "super_admin"].includes(u.role)).length;
  const expiringSoon = users.filter(u => {
    if (!u.plan_expires_at || ["admin", "super_admin"].includes(u.role)) return false;
    const diff = new Date(u.plan_expires_at) - new Date();
    return diff > 0 && diff <= 3 * 86400000;
  }).length;

  document.getElementById("adminStatUsers").textContent = String(totalUsers);
  document.getElementById("adminStatPlans").textContent = String(activePlans);
  document.getElementById("adminStatLocked").textContent = String(lockedUsers);
  document.getElementById("adminWorkspaceMeta").textContent = `${admins} tài khoản quản trị, ${expiringSoon} gói sắp hết hạn và ${lockedUsers} tài khoản đang bị khóa.`;

  const summary = document.getElementById("adminSummaryList");
  const items = [
    { label: "Tài khoản quản trị", value: `${admins} tài khoản`, note: "Bao gồm quản trị viên và chủ hệ thống đang có quyền điều hành." },
    { label: "Gói sắp hết hạn", value: `${expiringSoon} tài khoản`, note: "Ưu tiên gia hạn để tránh gián đoạn khi người dùng tạo ảnh." },
    { label: "Tài khoản bị khóa", value: `${lockedUsers} tài khoản`, note: lockedUsers ? "Nên rà soát lý do khóa hoặc mở lại nếu cần." : "Hiện chưa có tài khoản nào bị khóa." },
  ];
  summary.innerHTML = items.map(item => `<div class="admin-summary-item"><strong>${item.label}</strong><span>${item.value}<br>${item.note}</span></div>`).join("");
}

async function adminAddUser() {
  const username = document.getElementById("adminNewUser").value.trim();
  const password = document.getElementById("adminNewPass").value;
  const role = document.getElementById("adminNewRole").value;
  const planScope = document.getElementById("adminNewPlanScope").value;
  const cookieQuota = parseInt(document.getElementById("adminNewCookieQuota")?.value || "5") || 5;
  if (!username || !password) { sAlert("Thiếu username/password"); return; }
  const res = await apiFetch("/admin/users", { method: "POST", body: JSON.stringify({ username, password, role, cookie_quota: cookieQuota, plan_scope: planScope }) });
  if (!res.ok) { sAlert(await parseApiError(res, "Không thể tạo tài khoản mới."), "error"); return; }
  document.getElementById("adminNewUser").value = "";
  document.getElementById("adminNewPass").value = "";
  if (document.getElementById("adminNewPlanScope")) document.getElementById("adminNewPlanScope").value = "both";
  if (document.getElementById("adminNewCookieQuota")) document.getElementById("adminNewCookieQuota").value = "5";
  loadAdminUsers();
}

async function adminBulkCreateDemoUsers() {
  if (authUser?.role !== "super_admin") {
    sAlert("Chỉ super admin mới dùng được chức năng này.", "error");
    return;
  }
  const count = parseInt(document.getElementById("adminDemoCount")?.value || "20") || 20;
  const prefix = (document.getElementById("adminDemoPrefix")?.value || "demo").trim() || "demo";
  const finalCount = Math.max(1, Math.min(200, count));
  if (!await sConfirm(`Tạo ${finalCount} tài khoản demo mới?`, "Xác nhận tạo demo")) return;
  const res = await apiFetch("/admin/users/bulk-demo", {
    method: "POST",
    body: JSON.stringify({ count: finalCount, prefix }),
  });
  if (!res.ok) {
    sAlert(await parseApiError(res, "Không thể tạo tài khoản demo."), "error");
    return;
  }
  const data = await res.json().catch(() => ({}));
  const lines = (data.users || []).map((user, idx) =>
    `${idx + 1}. ${user.username} | ${user.password} | ${user.cookie_quota} cookie | ${user.plan_days} ngày`
  );
  const output = lines.join("\n");
  try { await navigator.clipboard.writeText(output); } catch (_) {}
  await Swal.fire({
    title: `Đã tạo ${data.count || lines.length} tài khoản demo`,
    html: `<div style="text-align:left">
      <p style="font-size:0.84rem;line-height:1.6;color:#475569;margin-bottom:10px">Danh sách đã được copy sẵn vào clipboard. Mặc định mỗi tài khoản có <b>1 cookie</b> và <b>7 ngày</b>.</p>
      <textarea readonly style="width:100%;min-height:320px;border:1px solid #dbe2ea;border-radius:12px;padding:12px;font:inherit;font-size:0.8rem;line-height:1.6">${output}</textarea>
    </div>`,
    width: "min(760px, 92vw)",
    confirmButtonText: "Đã hiểu",
    confirmButtonColor: "#16a34a",
  });
  loadAdminUsers();
}

async function adminChangeRole(id, role) {
  const res = await apiFetch(`/admin/users/${id}`, { method: "PUT", body: JSON.stringify({ role }) });
  if (!res.ok) { sAlert(await parseApiError(res, "Không thể cập nhật vai trò người dùng."), "error"); return; }
  loadAdminUsers();
}
async function adminToggleUser(id, disabled) {
  const res = await apiFetch(`/admin/users/${id}`, { method: "PUT", body: JSON.stringify({ disabled }) });
  if (!res.ok) { sAlert(await parseApiError(res, "Không thể thay đổi trạng thái tài khoản."), "error"); return; }
  loadAdminUsers();
}
async function adminDelUser(id) {
  if (!await sConfirm("Xóa user này?")) return;
  const res = await apiFetch(`/admin/users/${id}`, { method: "DELETE" });
  if (!res.ok) { sAlert(await parseApiError(res, "Không thể xóa người dùng này."), "error"); return; }
  loadAdminUsers();
}
async function adminEditUser(id, username, cookieQuota, planScope = "both") {
  const canEditQuota = ["admin", "super_admin"].includes(authUser?.role);
  const result = await Swal.fire({
    title: "Sửa người dùng",
    html: `
      <div style="display:grid;gap:10px;text-align:left">
        <div>
          <label style="display:block;margin-bottom:6px;font-size:0.82rem;font-weight:600">Tên đăng nhập</label>
          <input id="swalAdminUsername" class="swal2-input" value="${esc(username)}" style="margin:0;width:100%" />
        </div>
        <div>
          <label style="display:block;margin-bottom:6px;font-size:0.82rem;font-weight:600">Mật khẩu mới</label>
          <input id="swalAdminPassword" type="password" class="swal2-input" placeholder="Để trống nếu không đổi" style="margin:0;width:100%" />
        </div>
        ${canEditQuota ? `<div>
          <label style="display:block;margin-bottom:6px;font-size:0.82rem;font-weight:600">Giới hạn cookie</label>
          <input id="swalAdminCookieQuota" type="number" min="1" max="50" class="swal2-input" value="${cookieQuota || 5}" style="margin:0;width:100%" />
        </div>` : ""}
        <div>
          <label style="display:block;margin-bottom:6px;font-size:0.82rem;font-weight:600">Loại gói</label>
          <select id="swalAdminPlanScope" class="swal2-input" style="margin:0;width:100%">
            <option value="image" ${planScope === "image" ? "selected" : ""}>Chỉ ảnh</option>
            <option value="video" ${planScope === "video" ? "selected" : ""}>Chỉ video</option>
            <option value="both" ${planScope === "both" ? "selected" : ""}>Ảnh + Video</option>
          </select>
        </div>
      </div>`,
    focusConfirm: false,
    showCancelButton: true,
    confirmButtonText: "Lưu",
    cancelButtonText: "Hủy",
    confirmButtonColor: "#16a34a",
    preConfirm: () => {
      const nextUsername = document.getElementById("swalAdminUsername").value.trim();
      const nextPassword = document.getElementById("swalAdminPassword").value;
      const nextQuota = canEditQuota ? parseInt(document.getElementById("swalAdminCookieQuota").value || "5") || 5 : undefined;
      if (!nextUsername) {
        Swal.showValidationMessage("Tên đăng nhập không được để trống");
        return false;
      }
      return { username: nextUsername, password: nextPassword, cookie_quota: nextQuota, plan_scope: document.getElementById("swalAdminPlanScope").value };
    }
  });
  if (!result.isConfirmed) return;
  const payload = { username: result.value.username };
  if (result.value.password) payload.password = result.value.password;
  if (canEditQuota) payload.cookie_quota = result.value.cookie_quota;
  payload.plan_scope = result.value.plan_scope;
  const res = await apiFetch(`/admin/users/${id}`, { method: "PUT", body: JSON.stringify(payload) });
  if (!res.ok) { sAlert(await parseApiError(res, "Không thể cập nhật thông tin người dùng."), "error"); return; }
  loadAdminUsers();
}
async function adminSetPlan(id, days) {
  if (days === 0 && !await sConfirm("Hủy gói user này?")) return;
  const res = await apiFetch(`/admin/users/${id}`, { method: "PUT", body: JSON.stringify({ plan_days: days }) });
  if (!res.ok) { sAlert(await parseApiError(res, "Không thể cập nhật thời hạn gói."), "error"); return; }
  loadAdminUsers();
}

async function adminSetPlanScope(id, planScope) {
  const res = await apiFetch(`/admin/users/${id}`, { method: "PUT", body: JSON.stringify({ plan_scope: planScope }) });
  if (!res.ok) { sAlert(await parseApiError(res, "Không thể cập nhật loại gói."), "error"); return; }
  loadAdminUsers();
}

async function adminSetCookieQuota(id, quota) {
  const res = await apiFetch(`/admin/users/${id}`, { method: "PUT", body: JSON.stringify({ cookie_quota: parseInt(quota) || 1 }) });
  if (!res.ok) { sAlert(await parseApiError(res, "Không thể cập nhật giới hạn cookie."), "error"); return; }
  loadAdminUsers();
}

async function loadAdminHistory() {
  try {
    const res = await apiFetch("/admin/history?limit=50");
    if (!res.ok) return;
    const items = await res.json();
    document.getElementById("adminStatHistory").textContent = String(items.length);
    const grid = document.getElementById("adminHistoryGrid");
    grid.innerHTML = "";
    if (!items.length) { grid.innerHTML = '<div class="empty-state"><div class="empty-icon">📜</div><div>Chưa có lịch sử</div></div>'; return; }
    items.forEach(item => {
      const card = document.createElement("div");
      card.className = "img-card";
      const time = item.created_at ? new Date(item.created_at + "Z").toLocaleString("vi-VN") : "";
      if (item.image_url) {
        card.innerHTML = `<img src="${item.image_url}" loading="lazy" onerror="this.style.display='none'"/>
          <div class="img-card-body"><div class="img-card-prompt">${esc(item.prompt)}</div><div style="font-size:0.68rem;color:var(--muted);margin-top:4px">👤 ${esc(item.username)} · ${esc(item.model)} · ${time}</div></div>`;
      } else {
        card.innerHTML = `<div class="img-card-error">❌ ${esc(item.error || "Thất bại")}</div><div class="img-card-body"><div class="img-card-prompt">${esc(item.prompt)}</div><div style="font-size:0.68rem;color:var(--muted);margin-top:4px">👤 ${esc(item.username)} · ${time}</div></div>`;
      }
      grid.appendChild(card);
    });
  } catch (e) {}
}

function switchAdminSection(section) {
  adminSection = section;
  ["overview", "users", "plans", "cookies", "history"].forEach(key => {
    document.getElementById(`adminPane${key.charAt(0).toUpperCase() + key.slice(1)}`).style.display = key === section ? "" : "none";
    document.getElementById(`adminNav${key.charAt(0).toUpperCase() + key.slice(1)}`)?.classList.toggle("active", key === section);
  });
  const headerMap = {
    overview: ["Tổng quan vận hành", "Theo dõi nhanh người dùng, gói, cookie và lịch sử tạo ảnh trong cùng một không gian làm việc.", "Bức tranh toàn hệ thống"],
    users: ["Quản lý người dùng", "Kiểm soát vai trò, trạng thái tài khoản và thông tin đăng nhập ngay trong một khu làm việc rõ ràng.", "Tài khoản và phân quyền"],
    plans: ["Quản lý gói dịch vụ", "Gia hạn, hủy hoặc kiểm tra trạng thái gói của từng tài khoản mà không phải chuyển màn hình.", "Gói và thời hạn"],
    cookies: ["Quản lý kho cookie", "Theo dõi cookie theo từng tài khoản để rà lỗi nhanh và giữ nguồn chạy luôn sẵn sàng.", "Cookie theo tài khoản"],
    history: ["Lịch sử toàn hệ thống", "Mở lịch sử theo từng người dùng, từng batch và từng file để truy vết chính xác hơn.", "Lịch sử theo thư mục"],
  };
  const [title, subtitle, chip] = headerMap[section] || headerMap.overview;
  document.getElementById("adminViewTitle").textContent = title;
  document.getElementById("adminViewSubtitle").textContent = subtitle;
  document.getElementById("adminViewChip").textContent = chip;
  const bulkDemoBox = document.getElementById("adminBulkDemoBox");
  if (bulkDemoBox) bulkDemoBox.style.display = section === "users" && authUser?.role === "super_admin" ? "" : "none";
  if (section === "overview" || section === "users" || section === "plans") loadAdminUsers();
  if (section === "overview") loadAdminHistory();
  if (section === "cookies") loadAdminCookieUsers();
  if (section === "history") loadAdminHistoryUsers();
}

async function loadAdminCookieUsers() {
  const res = await apiFetch("/admin/cookies?group=users");
  if (!res.ok) return;
  const users = await res.json();
  const box = document.getElementById("adminCookieUsers");
  box.innerHTML = users.map(u => `
    <div class="hist-folder" onclick="openAdminCookieUser(${u.user_id},'${esc(u.username)}')">
      <div class="hist-folder-info">
        <div class="hist-folder-name">${esc(u.username)}</div>
        <div class="hist-folder-meta">${u.count} cookie</div>
      </div>
    </div>`).join("") || '<div class="empty-state"><div>Chưa có cookie</div></div>';
  document.getElementById("adminCookieTitle").textContent = "Kho cookie theo tài khoản";
  document.getElementById("adminCookieBody").innerHTML = '<tr><td colspan="7" style="text-align:center;padding:16px">Chọn một tài khoản để xem cookie</td></tr>';
}

async function openAdminCookieUser(userId, username) {
  adminCookieUserId = userId;
  document.getElementById("adminCookieTitle").textContent = `Kho cookie của ${username}`;
  const res = await apiFetch(`/admin/cookies?user_id=${userId}`);
  if (!res.ok) return;
  const items = await res.json();
  document.getElementById("adminCookieBody").innerHTML = items.map(c => `<tr>
    <td>${c.id}</td>
    <td>${esc(c.username)}</td>
    <td style="font-family:monospace">${esc((c.cookie_hash || "").slice(0, 16))}...</td>
    <td>${esc(c.email || "—")}</td>
    <td>${esc(c.status || "pending")}</td>
    <td>${c.created_at ? new Date(c.created_at + "Z").toLocaleString("vi-VN") : "—"}</td>
    <td><button class="btn btn-red btn-sm" onclick="adminDeleteCookie(${c.id})">Xóa</button></td>
  </tr>`).join("") || '<tr><td colspan="7" style="text-align:center;padding:16px">Tài khoản này chưa có cookie</td></tr>';
}

async function adminDeleteCookie(id) {
  if (!await sConfirm("Xóa cookie này khỏi hệ thống?")) return;
  const res = await apiFetch(`/admin/cookies/${id}`, { method: "DELETE" });
  if (!res.ok) { sAlert(await parseApiError(res, "Không thể xóa cookie khỏi hệ thống."), "error"); return; }
  if (adminCookieUserId) openAdminCookieUser(adminCookieUserId, document.getElementById("adminCookieTitle").textContent.replace("Kho cookie của ", ""));
}

async function loadAdminHistoryUsers() {
  adminHistoryState = { userId: null, username: "", jobId: null, batchName: "", fileName: null };
  document.getElementById("adminHistoryBack").style.display = "none";
  document.getElementById("adminHistoryTitle").textContent = "Lịch sử toàn hệ thống";
  document.getElementById("adminHistoryDesc").textContent = "Theo dõi từng tài khoản theo số ảnh, số video, ngày tạo và thời điểm hoạt động gần nhất.";
  const res = await apiFetch(`/admin/history/users${adminHistoryMedia ? `?media_type=${adminHistoryMedia}` : ""}`);
  if (!res.ok) return;
  const users = await res.json();
  document.getElementById("adminHistoryFolders").innerHTML = users.map(u => `
    <div class="hist-folder" onclick="openAdminHistoryUser(${u.user_id},'${esc(u.username)}')">
      <div class="hist-folder-info">
        <div class="hist-folder-name">${esc(u.username)}</div>
        <div class="hist-folder-meta">${u.count} mục${u.created_at ? ` · Hoạt động ${new Date(u.created_at + "Z").toLocaleString("vi-VN")}` : ""}</div>
        <div class="hist-folder-badges">
          ${adminHistoryMedia ? `<span class="hist-badge hist-badge-blue">${u.count} ${adminHistoryMedia === "video" ? "video" : "ảnh"}</span>` : `<span class="hist-badge hist-badge-blue">${u.image_count || 0} ảnh</span><span class="hist-badge hist-badge-green">${u.video_count || 0} video</span>`}
          ${u.account_created_at ? `<span class="hist-badge">${`Tạo ${new Date(u.account_created_at + "Z").toLocaleDateString("vi-VN")}`}</span>` : ""}
        </div>
      </div>
    </div>`).join("") || '<div class="empty-state"><div>Chưa có lịch sử</div></div>';
  document.getElementById("adminHistoryBrowser").innerHTML = '<div class="empty-state"><div class="empty-icon">📁</div><div>Chọn một tài khoản để xem lịch sử</div></div>';
}

function setAdminHistoryMedia(mediaType) {
  adminHistoryMedia = mediaType;
  loadAdminHistoryUsers();
}

async function openAdminHistoryUser(userId, username) {
  adminHistoryState = { userId, username, jobId: null, batchName: "", fileName: null };
  document.getElementById("adminHistoryTitle").textContent = `Lịch sử của ${username}`;
  document.getElementById("adminHistoryDesc").textContent = "Chọn batch để đi sâu hơn vào từng thư mục file.";
  const qs = `user_id=${userId}${adminHistoryMedia ? `&media_type=${adminHistoryMedia}` : ""}`;
  const res = await apiFetch(`/admin/history/groups?${qs}`);
  if (!res.ok) return;
  const items = await res.json();
  document.getElementById("adminHistoryBack").style.display = "";
  renderAdminHistoryFolders(items, g => g.batch_name || g.job_id, g => `${g.count} mục`, item => openAdminHistoryGroup(item.job_id, item.batch_name || item.job_id));
}

async function openAdminHistoryGroup(jobId, batchName) {
  adminHistoryState.jobId = jobId;
  adminHistoryState.batchName = batchName;
  document.getElementById("adminHistoryTitle").textContent = batchName;
  document.getElementById("adminHistoryDesc").textContent = "Chọn file để xem ảnh/video bên trong.";
  const qs = `user_id=${adminHistoryState.userId}&job_id=${encodeURIComponent(jobId)}${adminHistoryMedia ? `&media_type=${adminHistoryMedia}` : ""}`;
  const res = await apiFetch(`/admin/history/subgroups?${qs}`);
  if (!res.ok) return;
  const items = await res.json();
  if (items.length <= 1) return openAdminHistoryFile(items[0]?.file_name || "");
  renderAdminHistoryFolders(items, f => f.file_name || "Không chia file", f => `${f.count} mục`, item => openAdminHistoryFile(item.file_name || ""));
}

async function openAdminHistoryFile(fileName) {
  adminHistoryState.fileName = fileName;
  document.getElementById("adminHistoryTitle").textContent = fileName || adminHistoryState.batchName;
  document.getElementById("adminHistoryDesc").textContent = "Nội dung bên trong thư mục đã chọn.";
  const params = new URLSearchParams({
    user_id: String(adminHistoryState.userId),
    job_id: adminHistoryState.jobId,
    file_name: fileName || "",
    limit: "200",
  });
  if (adminHistoryMedia) params.set("media_type", adminHistoryMedia);
  const res = await apiFetch(`/admin/history/items?${params.toString()}`);
  if (!res.ok) return;
  const items = await res.json();
  renderAdminHistoryItems(items);
}

function adminHistoryBack() {
  if (adminHistoryState.fileName !== null) {
    adminHistoryState.fileName = null;
    openAdminHistoryGroup(adminHistoryState.jobId, adminHistoryState.batchName);
    return;
  }
  if (adminHistoryState.jobId) {
    openAdminHistoryUser(adminHistoryState.userId, adminHistoryState.username);
    adminHistoryState.jobId = null;
    adminHistoryState.batchName = "";
    return;
  }
  loadAdminHistoryUsers();
}

function renderAdminHistoryFolders(items, getName, getMeta, onClick) {
  const box = document.getElementById("adminHistoryBrowser");
  box.classList.remove("grid-mode");
  box.innerHTML = items.map(item => `
    <div class="hist-folder" onclick="${''}">
      <div class="hist-folder-info">
        <div class="hist-folder-name">${esc(getName(item))}</div>
        <div class="hist-folder-meta">${esc(getMeta(item))}</div>
      </div>
    </div>`).join("");
  [...box.children].forEach((el, idx) => { el.onclick = () => onClick(items[idx]); });
  if (!items.length) box.innerHTML = '<div class="empty-state"><div class="empty-icon">📁</div><div>Không có dữ liệu</div></div>';
}

function renderAdminHistoryItems(items) {
  const grid = document.getElementById("adminHistoryBrowser");
  grid.classList.add("grid-mode");
  if (!items.length) {
    grid.innerHTML = '<div class="empty-state"><div class="empty-icon">📁</div><div>Không có dữ liệu</div></div>';
    return;
  }
  grid.innerHTML = "";
  items.forEach(item => {
    const mediaUrl = item.media_url || item.image_url;
    const card = document.createElement("div");
    card.className = "img-card";
    const time = item.created_at ? new Date(item.created_at + "Z").toLocaleString("vi-VN") : "";
    if (item.media_type === "video") {
      card.innerHTML = `<div class="video-history-preview" onclick="window.open('${mediaUrl}','_blank')"><div class="video-history-play">▶</div><div class="video-history-label">Xem video</div></div>
      <div class="img-card-body"><div class="img-card-prompt">${esc(item.prompt)}</div><div style="font-size:0.68rem;color:var(--muted);margin-top:4px">${esc(item.model || "")} · ${time}</div></div>`;
    } else {
      card.innerHTML = `<img src="${mediaUrl}" loading="lazy"/><div class="img-card-body"><div class="img-card-prompt">${esc(item.prompt)}</div><div style="font-size:0.68rem;color:var(--muted);margin-top:4px">${esc(item.model || "")} · ${time}</div></div>`;
    }
    grid.appendChild(card);
  });
}
// ── UI Helpers ────────────────────────────────────────────────────────────────
function setLoading(on) {
  document.getElementById("runBtn").disabled = on;
  document.getElementById("pauseBtn").disabled = !on;
  document.getElementById("stopBtn").disabled = !on;
  document.getElementById("progressCard").style.display = on ? "block" : "none";
}

function updateProgress(job) {
  const pct = job.total > 0 ? Math.round((job.completed / job.total) * 100) : 0;
  document.getElementById("progressFill").style.width = pct + "%";
  
  if (job.queue_position > 0) {
    document.getElementById("progressText").textContent = `⏳ Đang xếp hàng (Còn ${job.queue_position} lượt trước bạn) - ${job.completed} / ${job.total} ảnh (${pct}%)`;
  } else {
    document.getElementById("progressText").textContent = `${job.completed} / ${job.total} ảnh (${pct}%)`;
  }
  
  const labels = { pending: "Chờ", running: "Đang chạy", done: "Hoàn thành", error: "Lỗi" };
  const badge = document.getElementById("progressBadge");
  badge.className = `badge badge-${job.status}`; badge.textContent = labels[job.status] || job.status;
  const sb = document.getElementById("statusBadge");
  sb.className = `badge badge-${job.status}`; sb.textContent = labels[job.status] || job.status;
}

function renderRunningStatus(label = "Đang chạy") {
  return `<div class="status-progress">
    <span class="status-progress-label">${esc(label)}</span>
    <span class="status-progress-track"><span class="status-progress-bar"></span></span>
  </div>`;
}

// ── Results Table ──────────────────────────────────────────────────────────────
function populateResultsTable() {
  const allPrompts = batchFiles.flatMap(f => f.prompts);
  const variants = parseInt(document.getElementById("variantsInput").value) || 1;
  const tbody = document.getElementById("resultsBody");
  const empty = document.getElementById("resultsEmpty");
  const badge = document.getElementById("promptCountBadge");

  if (!allPrompts.length) {
    tbody.innerHTML = "";
    empty.style.display = "";
    badge.style.display = "none";
    return;
  }
  empty.style.display = "none";
  badge.style.display = "";
  const totalImages = allPrompts.length * variants;
  badge.textContent = `${totalImages} ảnh (${allPrompts.length} dòng × ${variants})`;

  let html = "", globalIdx = 0;
  batchFiles.forEach(f => {
    const strip = n => n.replace(/\.(txt)$/i, "");
    const fileTotal = f.prompts.length * variants;
    html += `<tr class="file-separator"><td colspan="5">📄 ${esc(strip(f.name))} <span style="color:var(--muted);font-weight:400">(${f.prompts.length} prompt × ${variants} = ${fileTotal} ảnh)</span></td></tr>`;
    f.prompts.forEach((text, j) => {
      for (let v = 0; v < variants; v++) {
        const i = globalIdx++;
        const promptIdx = batchFiles.slice(0, batchFiles.indexOf(f)).reduce((s, ff) => s + ff.prompts.length, 0) + j;
        const imgs = rowRefImages[promptIdx] || [];
        const varLabel = variants > 1 ? ` <span style="color:var(--muted);font-size:0.7rem">[${v + 1}/${variants}]</span>` : "";
        const refCell = imgs.length
            ? `<div class="ref-cell">${imgs.map((s, ri) => `<span class="ref-wrap"><img src="${s}" class="ref-thumb" onclick="window.open(this.src)"/><span class="ref-del" onclick="removeRefImg(${promptIdx},${ri})">✕</span></span>`).join("")}${v === 0 ? `<span class="ref-add-btn" onclick="importRefForRow(${promptIdx})">+</span>` : ""}</div>`
            : `<span class="ref-add-btn" onclick="importRefForRow(${promptIdx})">+</span>`;
        html += `<tr id="resRow${i}">
          <td>${i + 1}</td>
          <td><div class="prompt-cell">${esc(text)}${varLabel}</div></td>
          <td style="text-align:center">${refCell}</td>
          <td class="status-cell">Chờ</td>
          <td style="text-align:center"><span style="color:var(--muted);font-size:0.72rem">—</span></td>
        </tr>`;
      }
    });
  });
  tbody.innerHTML = html;
}

function updateResultsFromJob(job) {
  const images = job.images || [];
  const allPrompts = batchFiles.flatMap(f => f.prompts);
  if (!allPrompts.length) return;

  const completed = job.completed || 0;
  const total = job.total || allPrompts.length;
  const variants = total > 0 && allPrompts.length > 0 ? Math.round(total / allPrompts.length) : 1;

  // Update completed rows from images array
  // images array index maps directly to resRow index (both are prompt*variants)
  images.forEach((img, idx) => {
    const row = document.getElementById(`resRow${idx}`);
    if (!row) return;
    const cells = row.cells;
    if (img.url) {
      row.className = "row-done";
      cells[3].innerHTML = '<span class="status-ok">✅ Xong</span>';
      const originalName = buildImageFilename(img.url, idx);
      const upscaleUrl = img.upscaled ? `${VPS_BASE}${img.upscaled}` : "";
      const upscaleName = img.upscaled ? buildImageFilename(upscaleUrl, idx) : "";
      cells[4].innerHTML = `<img src="${img.url}" class="result-thumb" onclick="window.open(this.src)" onerror="this.outerHTML='❌'"/>
        <div class="result-actions"><a href="${img.url}" target="_blank" download="${originalName}">🔗</a>${img.upscaled ? ` <a href="${upscaleUrl}" download="${upscaleName}">⬇${document.getElementById("resolutionSelect")?.value?.toUpperCase() || "HD"}</a>` : ""}</div>`;
    } else {
      row.className = "row-error";
      cells[3].innerHTML = '<span class="status-err">❌ Lỗi</span>';
      cells[4].innerHTML = `<span style="font-size:0.68rem;color:var(--error)">${esc(img.error || "Lỗi")}</span>`;
    }
  });

  // Reset untouched rows back to pending before marking running slots
  for (let idx = 0; idx < total; idx++) {
    const row = document.getElementById(`resRow${idx}`);
    if (!row || row.className === "row-done" || row.className === "row-error") continue;
    row.className = "";
    row.cells[3].innerHTML = "Chờ";
    row.cells[4].innerHTML = "—";
  }

  // Shared rule with video: 1 output => 3x cookie, 2 outputs => 2x cookie, 3-4 outputs => 1x cookie
  if (job.status === "running" && completed < total) {
    const cookieCount = Math.max(1, cookies.length || 1);
    const workersPerCookie = variants <= 1 ? 3 : variants === 2 ? 2 : 1;
    const runningSlots = Math.min(workersPerCookie * cookieCount, Math.max(0, total - completed));
    const pendingIndexes = [];
    for (let idx = 0; idx < total; idx++) {
      if (!images[idx]) pendingIndexes.push(idx);
    }
    pendingIndexes.slice(0, runningSlots).forEach((runIdx, pos) => {
      const row = document.getElementById(`resRow${runIdx}`);
      if (!row || row.className === "row-done" || row.className === "row-error") return;
      row.className = "row-running";
      row.cells[3].innerHTML = renderRunningStatus("Đang chạy");
      if (pos === 0) row.scrollIntoView({ behavior: "smooth", block: "center" });
    });
  }

  // Update per-file status in batch table
  // Each file has f.prompts.length * variants images in the backend
  let offset = 0;
  batchFiles.forEach((f, fi) => {
    const fileImageCount = f.prompts.length * variants;
    const fileStart = offset;
    const fileEnd = offset + fileImageCount;
    offset = fileEnd;
    const fileDone = images.slice(fileStart, fileEnd).filter(img => img).length;
    if (fileDone >= fileImageCount) {
      const allOk = images.slice(fileStart, fileEnd).every(img => img && img.url);
      f.status = allOk ? "✅ Xong" : "⚠️ Có lỗi";
    } else if (fileDone > 0 || (completed >= fileStart && completed < fileEnd)) {
      f.status = `🔄 ${fileDone}/${fileImageCount}`;
    } else if (completed >= fileEnd) {
      f.status = "✅ Xong";
    } else {
      f.status = "Chờ";
    }
  });
  renderBatchTable();
}

function clearResults() {
  document.getElementById("resultsBody").innerHTML = "";
  document.getElementById("resultsEmpty").style.display = "";
  document.getElementById("statusBadge").textContent = "";
  document.getElementById("promptCountBadge").style.display = "none";
  rowRefImages = {};
}

async function downloadAll() {
  // Collect all result image URLs from table
  const urls = [];
  document.querySelectorAll("#resultsBody .result-thumb").forEach(img => {
    if (img.src) urls.push(img.src);
  });
  if (!urls.length) { sAlert("Không có ảnh để tải"); return; }

  const btn = document.querySelector('[onclick="downloadAll()"]');
  const origText = btn.textContent;
  btn.textContent = "⏳ Đang gom ZIP...";
  btn.disabled = true;

  try {
    if (!window.JSZip) {
      await new Promise((resolve, reject) => {
        const s = document.createElement("script");
        s.src = "https://cdnjs.cloudflare.com/ajax/libs/jszip/3.10.1/jszip.min.js";
        s.onload = resolve; s.onerror = reject;
        document.head.appendChild(s);
      });
    }
    const zip = new JSZip();
    let idx = 0;
    for (const url of urls) {
      try {
        btn.textContent = `⏳ Tải ${idx + 1}/${urls.length}...`;
        const resp = await fetch(url);
        const blob = await resp.blob();
        const ext = blob.type?.includes("png") ? "png" : blob.type?.includes("webp") ? "webp" : "jpg";
        zip.file(`image_${String(idx + 1).padStart(3, "0")}.${ext}`, blob);
        idx++;
      } catch (e) { console.warn("Skip:", url, e); }
    }
    if (!idx) { sAlert("Không tải được ảnh nào"); return; }
    btn.textContent = "⏳ Đang nén ZIP...";
    const content = await zip.generateAsync({ type: "blob" });
    const a = document.createElement("a");
    a.href = URL.createObjectURL(content);
    a.download = `banana_pro_${new Date().toISOString().slice(0, 10)}.zip`;
    a.click();
    URL.revokeObjectURL(a.href);
  } catch (e) {
    sAlert("Lỗi tải ZIP: " + e.message);
  } finally {
    btn.textContent = origText;
    btn.disabled = false;
  }
}
function showError(msg) { const el = document.getElementById("errorMsg"); el.textContent = "⚠️ " + msg; el.style.display = "block"; }
function hideError() { document.getElementById("errorMsg").style.display = "none"; }
function esc(s) { return String(s ?? "").replace(/&/g, "&amp;").replace(/</g, "&lt;").replace(/>/g, "&gt;").replace(/"/g, "&quot;"); }

function parseCookieInput(raw) {
  raw = raw.trim();
  if (raw.startsWith("[")) { try { const items = JSON.parse(raw); return Object.fromEntries(items.map(c => [c.name, c.value])); } catch (e) {} }
  const obj = {};
  raw.split(";").forEach(part => { const idx = part.indexOf("="); if (idx > 0) obj[part.slice(0, idx).trim()] = part.slice(idx + 1).trim(); });
  return obj;
}

function cookieHash(obj) {
  const str = JSON.stringify(obj);
  let h = 0;
  for (let i = 0; i < str.length; i++) h = (Math.imul(31, h) + str.charCodeAt(i)) | 0;
  return Math.abs(h).toString(16).padStart(8, "0");
}
