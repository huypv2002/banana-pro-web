const API_BASE = "https://banana-pro-api.kh431248.workers.dev";
const STORAGE_TOKEN_KEY = "bp_token";
const STORAGE_USER_KEY = "bp_user";
const LEGACY_TOKEN_KEYS = ["bp_token", "bp_image_token", "bp_video_token"];
const LEGACY_USER_KEYS = ["bp_user", "bp_image_user", "bp_video_user"];

const Toast = Swal.mixin({ toast: true, position: "top-end", showConfirmButton: false, timer: 2500, timerProgressBar: true });
function sAlert(text, icon = "info") { return Swal.fire({ text, icon, confirmButtonColor: "#16a34a" }); }
function sSuccess(text) { Toast.fire({ icon: "success", title: text }); }
async function sConfirm(text, title = "Xác nhận") { const r = await Swal.fire({ title, text, icon: "warning", showCancelButton: true, confirmButtonColor: "#16a34a", cancelButtonColor: "#6b7280", confirmButtonText: "Đồng ý", cancelButtonText: "Hủy" }); return r.isConfirmed; }
function roleLabel(role) { return role === "super_admin" ? "Chủ hệ thống" : role === "admin" ? "Quản trị viên" : "Người dùng"; }

let authToken = LEGACY_TOKEN_KEYS.map(k => localStorage.getItem(k)).find(Boolean) || "";
let authUser = null;
for (const key of LEGACY_USER_KEYS) {
  const raw = localStorage.getItem(key);
  if (!raw) continue;
  try { authUser = JSON.parse(raw); break; } catch (_) {}
}
if (authToken) localStorage.setItem(STORAGE_TOKEN_KEY, authToken);
if (authUser) localStorage.setItem(STORAGE_USER_KEY, JSON.stringify(authUser));
let cookies = [];
let currentJobId = null;
let pollInterval = null;
let pollFailCount = 0;
let currentMode = "t2v";
let activeJobMode = null;
let latestJobState = null;
const modeBatchFiles = { t2v: [], i2v: [], fl: [], r2v: [] };
const modeSourcePaths = {
  t2v: { txt: "", folder: "" },
  i2v: { txt: "", folder: "" },
  fl: { txt: "", folder: "" },
  r2v: { txt: "", folder: "" },
};
// Per-mode image stores — không share giữa các tab
const modeRefImages = { t2v: {}, i2v: {}, fl: {}, r2v: {} };
const modeEndImages = { t2v: {}, i2v: {}, fl: {}, r2v: {} };
let rowRefImages = modeRefImages.t2v;
let endRowImages = modeEndImages.t2v;
let refImportTargetRow = -1;
let endImportTargetRow = -1;
let adminSection = "overview";
let adminHistoryMedia = "";
let adminHistoryState = { userId: null, username: "", jobId: null, batchName: "", fileName: null };
let adminCookieUserId = null;
const savedVideoHistoryUrls = new Set();
const autoDownloadedVideoUrls = new Set();
let autoDownloadGuideAcknowledged = localStorage.getItem("bp_video_dl_guide") === "1";
let autoDownloadEachEnabled = localStorage.getItem("bp_video_auto_each_download") === "1";
const modeReviewFlags = { t2v: {}, i2v: {}, fl: {}, r2v: {} };
const modeRerunSnapshots = { t2v: null, i2v: null, fl: null, r2v: null };

const MODE_CONFIG = {
  t2v: {
    desc: "Văn bản sang video: tạo video từ nội dung mô tả",
    refLabel: null,
    endLabel: null,
    models: [
      { group: "🌄 Landscape (16:9)", opts: [
        { v: "t2v_low_16_9",     l: "Low Fast 16:9 – 0 credits" },
        { v: "t2v_fast_16_9",    l: "Fast 16:9 – 10 credits", sel: true },
        { v: "t2v_quality_16_9", l: "Quality 16:9 – 100 credits" },
      ]},
      { group: "📱 Portrait (9:16)", opts: [
        { v: "t2v_low_9_16",     l: "Low Fast 9:16 – 0 credits" },
        { v: "t2v_fast_9_16",    l: "Fast 9:16 – 10 credits" },
        { v: "t2v_quality_9_16", l: "Quality 9:16 – 100 credits" },
      ]},
    ],
  },
  i2v: {
    desc: "Ảnh sang video: dùng ảnh đầu vào để tạo chuyển động",
    refLabel: "Ảnh đầu vào",
    endLabel: null,
    models: [
      { group: "🌄 Landscape (16:9)", opts: [
        { v: "i2v_low_16_9",     l: "Low Fast 16:9 – 0 credits" },
        { v: "i2v_fast_16_9",    l: "Fast 16:9 – 10 credits", sel: true },
        { v: "i2v_quality_16_9", l: "Quality 16:9 – 100 credits" },
      ]},
      { group: "📱 Portrait (9:16)", opts: [
        { v: "i2v_low_9_16",     l: "Low Fast 9:16 – 0 credits" },
        { v: "i2v_fast_9_16",    l: "Fast 9:16 – 10 credits" },
        { v: "i2v_quality_9_16", l: "Quality 9:16 – 100 credits" },
      ]},
    ],
  },
  fl: {
    desc: "Ảnh đầu và cuối: tạo chuyển động giữa hai khung hình",
    refLabel: "Ảnh đầu vào",
    endLabel: "Ảnh kết thúc",
    models: [
      { group: "🌄 Landscape (16:9)", opts: [
        { v: "fl_low_16_9",     l: "Low Fast 16:9 – 0 credits" },
        { v: "fl_fast_16_9",    l: "Fast 16:9 – 10 credits", sel: true },
        { v: "fl_quality_16_9", l: "Quality 16:9 – 100 credits" },
      ]},
      { group: "📱 Portrait (9:16)", opts: [
        { v: "fl_low_9_16",     l: "Low Fast 9:16 – 0 credits" },
        { v: "fl_fast_9_16",    l: "Fast 9:16 – 10 credits" },
        { v: "fl_quality_9_16", l: "Quality 9:16 – 100 credits" },
      ]},
    ],
  },
  r2v: {
    desc: "Ảnh tham chiếu: giữ phong cách hoặc nhân vật cho video",
    refLabel: "Ảnh tham chiếu",
    endLabel: null,
    models: [
      { group: "🌄 Landscape (16:9)", opts: [
        { v: "r2v_low_16_9",  l: "Low Fast 16:9 – 0 credits" },
        { v: "r2v_fast_16_9", l: "Fast 16:9 – 10 credits", sel: true },
      ]},
      { group: "📱 Portrait (9:16)", opts: [
        { v: "r2v_low_9_16",  l: "Low Fast 9:16 – 0 credits" },
        { v: "r2v_fast_9_16", l: "Fast 9:16 – 10 credits" },
      ]},
    ],
  },
};

function esc(s) { const d = document.createElement("div"); d.textContent = s || ""; return d.innerHTML; }
function getBatchFiles() { return modeBatchFiles[currentMode]; }
function setBatchFiles(files) { modeBatchFiles[currentMode] = files; }
function getSourcePaths() { return modeSourcePaths[currentMode]; }
function getReviewFlags() { return modeReviewFlags[currentMode]; }

function apiFetch(path, opts = {}) {
  const headers = { "Content-Type": "application/json", ...(opts.headers || {}) };
  if (authToken) headers["Authorization"] = `Bearer ${authToken}`;
  return fetch(API_BASE + path, { ...opts, headers });
}

// ── Auth ──
(async function init() {
  setMode("t2v"); // populate model select on load
  if (authToken && authUser) {
    try { const res = await apiFetch("/auth/me"); if (res.ok) { authUser = await res.json(); localStorage.setItem(STORAGE_USER_KEY, JSON.stringify(authUser)); showApp(); return; } } catch (e) {}
    clearAuth();
  }
  showLogin();
})();

function showLogin() { document.getElementById("loginScreen").style.display = "flex"; document.getElementById("appScreen").style.display = "none"; }
function showApp() {
  document.getElementById("loginScreen").style.display = "none";
  document.getElementById("appScreen").style.display = "block";
  document.getElementById("userInfo").textContent = `${authUser.username} (${roleLabel(authUser.role)})`;
  const planEl = document.getElementById("planInfo");
  if (["admin", "super_admin"].includes(authUser.role)) { planEl.textContent = "Không giới hạn"; planEl.className = "plan-badge plan-active"; }
  else if (authUser.plan_active) { const days = Math.ceil((new Date(authUser.plan_expires_at) - new Date()) / 86400000); planEl.textContent = `📦 Còn ${days} ngày`; planEl.className = "plan-badge " + (days <= 3 ? "plan-expiring" : "plan-active"); }
  else { planEl.textContent = "⛔ Hết hạn"; planEl.className = "plan-badge plan-expired"; }
  document.getElementById("navAdmin").style.display = ["admin", "super_admin"].includes(authUser.role) ? "" : "none";
  const autoToggle = document.getElementById("autoDownloadEachToggle");
  if (autoToggle) autoToggle.checked = autoDownloadEachEnabled;
  loadCookiesFromDB();
  showTab("generate");
}
function clearAuth() { authToken = ""; authUser = null; LEGACY_TOKEN_KEYS.forEach(k => localStorage.removeItem(k)); LEGACY_USER_KEYS.forEach(k => localStorage.removeItem(k)); }
function logout() { clearAuth(); showLogin(); }
function switchApp(url) {
  window.location.assign(url);
}
let authTab = "login";
function switchAuthTab(tab) {
  authTab = tab;
  document.querySelectorAll(".login-tab").forEach((b, i) => b.classList.toggle("active", (i === 0 && tab === "login") || (i === 1 && tab === "register")));
  document.getElementById("authSubmitBtn").textContent = tab === "login" ? "Đăng nhập" : "Đăng ký";
}
async function handleAuth(e) {
  e.preventDefault();
  const username = document.getElementById("authUsername").value.trim(), password = document.getElementById("authPassword").value;
  const errEl = document.getElementById("authError"); errEl.style.display = "none";
  try {
    const res = await fetch(API_BASE + (authTab === "login" ? "/auth/login" : "/auth/register"), { method: "POST", headers: { "Content-Type": "application/json" }, body: JSON.stringify({ username, password }) });
    const data = await res.json();
    if (!res.ok) { errEl.textContent = data.error || "Lỗi"; errEl.style.display = "block"; return false; }
    authToken = data.token; authUser = { username: data.username, role: data.role };
    localStorage.setItem(STORAGE_TOKEN_KEY, authToken); localStorage.setItem(STORAGE_USER_KEY, JSON.stringify(authUser));
    try { const me = await apiFetch("/auth/me"); if (me.ok) { authUser = await me.json(); localStorage.setItem(STORAGE_USER_KEY, JSON.stringify(authUser)); } } catch(_){}
    showApp();
  } catch (e) { errEl.textContent = "Lỗi kết nối"; errEl.style.display = "block"; }
  return false;
}

function showTab(tab) {
  document.getElementById("tabGenerate").style.display = tab === "generate" ? "" : "none";
  document.getElementById("tabHistory").style.display = tab === "history" ? "" : "none";
  document.getElementById("tabAdmin").style.display = tab === "admin" ? "" : "none";
  document.getElementById("navGenerate").classList.toggle("active", tab === "generate");
  document.getElementById("navHistory").classList.toggle("active", tab === "history");
  document.getElementById("navAdmin").classList.toggle("active", tab === "admin");
  if (tab === "history") loadHistory();
  if (tab === "admin" && ["admin", "super_admin"].includes(authUser?.role)) { switchAdminSection(adminSection || "overview"); }
  updateRetryUI();
}

// ── Video Mode ──
function setMode(mode) {
  currentMode = mode;
  // Switch sang store riêng của mode này
  rowRefImages = modeRefImages[mode];
  endRowImages = modeEndImages[mode];
  const cfg = MODE_CONFIG[mode];
  Object.keys(MODE_CONFIG).forEach(m => document.getElementById(`modeBtn_${m}`)?.classList.toggle("active", m === mode));
  document.getElementById("modeDesc").textContent = cfg.desc;
  const sel = document.getElementById("modelSelect");
  sel.innerHTML = cfg.models.map(g =>
    `<optgroup label="${g.group}">${g.opts.map(o => `<option value="${o.v}"${o.sel ? " selected" : ""}>${o.l}</option>`).join("")}</optgroup>`
  ).join("");
  updateModelDesc();
  const hasRef = !!cfg.refLabel;
  const hasEnd = !!cfg.endLabel;
  // Tính width động: prompt luôn lớn nhất, ảnh chia đều phần còn lại
  // Fixed: #=5%, status=10%, video=15% → còn 70% cho prompt+ảnh
  const imgCols = (hasRef ? 1 : 0) + (hasEnd ? 1 : 0);
  const imgPct  = imgCols === 0 ? 0 : imgCols === 1 ? 38 : 22;
  const promptPct = 85 - imgCols * imgPct;
  document.getElementById("colRefHead").style.display = hasRef ? "" : "none";
  document.getElementById("colRefHead").style.width = imgPct + "%";
  document.getElementById("colRefHead").textContent = cfg.refLabel || "Ảnh đầu vào";
  document.getElementById("colEndHead").style.display = hasEnd ? "" : "none";
  document.getElementById("colEndHead").style.width = imgPct + "%";
  document.getElementById("colEndHead").textContent = cfg.endLabel || "Ảnh kết thúc";
  document.querySelector(".col-prompt").style.width = promptPct + "%";
  document.getElementById("btnImportRef").style.display = hasRef ? "" : "none";
  document.getElementById("btnImportRef").textContent = cfg.refLabel ? `Thêm ${cfg.refLabel.toLowerCase()} cho tất cả` : "Thêm ảnh cho tất cả";
  document.getElementById("btnImportEnd").style.display = hasEnd ? "" : "none";
  syncPromptSourceUI();
  renderBatchTable();
  populateResultsTable();
  if (currentJobId && activeJobMode === currentMode && latestJobState) {
    updateProgress(latestJobState);
    updateResults(latestJobState);
    document.getElementById("progressCard").style.display = "block";
    document.getElementById("runBtn").disabled = true;
    document.getElementById("stopBtn").disabled = false;
  } else {
    clearProgressUI();
    document.getElementById("runBtn").disabled = false;
    document.getElementById("stopBtn").disabled = true;
  }
  updateRetryUI();
}

function updateModelDesc() {
  const sel = document.getElementById("modelSelect");
  const opt = sel.options[sel.selectedIndex];
  if (opt) document.getElementById("modelDesc").textContent = `Mô hình: ${opt.text}`;
}

// ── Ref/End Images ──
function importEndAll() { endImportTargetRow = -1; document.getElementById("endBulkInput").click(); }
function importRefForRow(idx) {
  refImportTargetRow = idx;
  const input = document.getElementById("refRowInput");
  input.multiple = currentMode === "r2v";
  input.click();
}
function importEndForRow(idx) { endImportTargetRow = idx; document.getElementById("endRowInput").click(); }

function handleEndBulkImport(input) {
  const files = Array.from(input.files); if (!files.length) return;
  const total = getBatchFiles().flatMap(f => f.prompts).length;
  let loaded = 0; const imgs = [];
  files.forEach(file => { const r = new FileReader(); r.onload = e => { imgs.push(e.target.result); if (++loaded === files.length) { for (let i = 0; i < total; i++) endRowImages[i] = imgs.length === 1 ? imgs[0] : (imgs[i] || imgs[imgs.length - 1]); refreshRefCells(); } }; r.readAsDataURL(file); });
  input.value = "";
}
function handleEndRowImport(input) {
  const file = input.files[0]; if (!file) return;
  const r = new FileReader(); r.onload = e => { endRowImages[endImportTargetRow] = e.target.result; refreshRefCells(); }; r.readAsDataURL(file);
  input.value = "";
}

function _renderRefCell(imgs, isR2V, addFn) {
  if (!imgs.length) return `<span class="ref-add-btn" onclick="${addFn}">+</span>`;
  const thumbs = imgs.map((src, j) =>
    `<span class="ref-wrap"><img src="${src}" class="ref-thumb" onclick="window.open(this.src)"/><span class="ref-del" onclick="removeRefImg(${j === undefined ? '' : j})">✕</span></span>`
  ).join("");
  const addBtn = isR2V && imgs.length < 15 ? `<span class="ref-add-btn" onclick="${addFn}">+</span>` : "";
  return `<div class="ref-cell">${thumbs}${addBtn}</div>`;
}

function refreshRefCells() {
  const cfg = MODE_CONFIG[currentMode];
  getBatchFiles().flatMap(f => f.prompts).forEach((_, i) => {
    const row = document.getElementById(`resRow${i}`); if (!row) return;
    if (cfg.refLabel) {
      const raw = rowRefImages[i];
      const imgs = Array.isArray(raw) ? raw : (raw ? [raw] : []);
      const isR2V = currentMode === "r2v";
      if (imgs.length) {
        const thumbs = imgs.map((src, j) =>
          `<span class="ref-wrap"><img src="${src}" class="ref-thumb" onclick="window.open(this.src)"/><span class="ref-del" onclick="removeRefImg(${i},${j})">✕</span></span>`
        ).join("");
        const addBtn = isR2V && imgs.length < 15 ? `<span class="ref-add-btn" onclick="importRefForRow(${i})">+</span>` : "";
        row.cells[2].innerHTML = `<div class="ref-cell">${thumbs}${addBtn}</div>`;
      } else {
        row.cells[2].innerHTML = `<span class="ref-add-btn" onclick="importRefForRow(${i})">+</span>`;
      }
    }
    if (cfg.endLabel) {
      const img = endRowImages[i];
      const cellIdx = cfg.refLabel ? 3 : 2;
      row.cells[cellIdx].innerHTML = img
        ? `<div class="ref-cell"><span class="ref-wrap"><img src="${img}" class="ref-thumb" onclick="window.open(this.src)"/><span class="ref-del" onclick="removeEndImg(${i})">✕</span></span></div>`
        : `<span class="ref-add-btn" onclick="importEndForRow(${i})">+</span>`;
    }
  });
}

// ── Cookies ──
async function loadCookiesFromDB() { try { const res = await apiFetch("/user/cookies"); if (res.ok) cookies = await res.json(); renderCookieTable(); } catch (e) {} }
function updateCookieQuotaInfo() {
  const info = document.getElementById("cookieQuotaInfo");
  if (!info) return;
  const quota = cookies[0]?.cookie_quota ?? authUser?.cookie_quota ?? 5;
  info.textContent = `Đang dùng ${cookies.length}/${quota} cookie cho tài khoản này.`;
}
function renderCookieTable() {
  const tbody = document.getElementById("cookieTableBody");
  tbody.innerHTML = cookies.map((c, i) => `<tr><td>${i + 1}</td><td style="font-family:monospace;font-size:0.75rem">${esc((c.cookie_hash||"").slice(0,12))}...</td><td>${esc(c.email || "—")}</td><td>${c.status}</td><td><button class="btn btn-red btn-sm" onclick="deleteCookie(${c.id})">Xóa</button></td></tr>`).join("") || '<tr><td colspan="5" style="text-align:center;padding:12px">Chưa có cookie</td></tr>';
  updateCookieQuotaInfo();
}
function parseCookieInput(raw) { try { const arr = JSON.parse(raw); if (Array.isArray(arr)) { const o = {}; arr.forEach(c => { if (c.name && c.value) o[c.name] = c.value; }); return o; } } catch(e) {} const o = {}; raw.split(";").forEach(p => { const [k,...v] = p.split("="); if (k?.trim()) o[k.trim()] = v.join("=").trim(); }); return o; }
async function addCookie() {
  const raw = document.getElementById("newCookieInput").value.trim(); if (!raw) return;
  const parsed = parseCookieInput(raw); if (!parsed || !Object.keys(parsed).length) { sAlert("Cookie không hợp lệ"); return; }
  const hash = Array.from(new Uint8Array(await crypto.subtle.digest("SHA-256", new TextEncoder().encode(raw)))).map(b => b.toString(16).padStart(2, "0")).join("").slice(0, 8);
  const res = await apiFetch("/user/cookies", { method: "POST", body: JSON.stringify({ cookie_raw: raw, cookie_hash: hash }) });
  const data = await res.json(); if (!res.ok) { sAlert(data.error || "Lỗi"); return; }
  document.getElementById("newCookieInput").value = ""; sSuccess("Đã thêm cookie"); loadCookiesFromDB();
}
async function deleteCookie(id) { await apiFetch(`/user/cookies/${id}`, { method: "DELETE" }); loadCookiesFromDB(); }
async function clearAllCookies() { if (!await sConfirm("Xóa tất cả cookie?")) return; await apiFetch("/user/cookies/clear", { method: "DELETE" }); loadCookiesFromDB(); }
function openCookieModal() { document.getElementById("cookieModal").style.display = "flex"; }
function closeCookieModal() { document.getElementById("cookieModal").style.display = "none"; }

// ── Prompt Sources ──
function loadTxtFile(input) {
  const file = input.files[0]; if (!file) return;
  const sourcePaths = getSourcePaths();
  sourcePaths.txt = file.name;
  sourcePaths.folder = "";
  syncPromptSourceUI();
  setBatchFiles([]);
  modeReviewFlags[currentMode] = {};
  modeRerunSnapshots[currentMode] = null;
  latestJobState = null;
  const reader = new FileReader();
  reader.onload = e => {
    setBatchFiles([{ name: file.name, prompts: e.target.result.split("\n").map(s => s.trim()).filter(Boolean), status: "Chờ" }]);
    renderBatchTable();
  };
  reader.readAsText(file);
  input.value = "";
}
function loadFolderTxt(input) {
  const files = Array.from(input.files).filter(f => f.name.endsWith(".txt")); if (!files.length) return;
  const sourcePaths = getSourcePaths();
  sourcePaths.folder = input.files[0].webkitRelativePath.split("/")[0];
  sourcePaths.txt = "";
  syncPromptSourceUI();
  setBatchFiles([]);
  modeReviewFlags[currentMode] = {};
  modeRerunSnapshots[currentMode] = null;
  latestJobState = null;
  let loaded = 0;
  const nextBatchFiles = [];
  files.forEach(file => {
    const r = new FileReader();
    r.onload = e => {
      nextBatchFiles.push({ name: file.name, prompts: e.target.result.split("\n").map(s => s.trim()).filter(Boolean), status: "Chờ" });
      if (++loaded === files.length) {
        setBatchFiles(nextBatchFiles);
        renderBatchTable();
      }
    };
    r.readAsText(file);
  });
  input.value = "";
}
function clearSources() {
  setBatchFiles([]);
  modeRefImages[currentMode] = {}; rowRefImages = modeRefImages[currentMode];
  modeEndImages[currentMode] = {}; endRowImages = modeEndImages[currentMode];
  modeReviewFlags[currentMode] = {};
  modeRerunSnapshots[currentMode] = null;
  modeSourcePaths[currentMode] = { txt: "", folder: "" };
  latestJobState = null;
  syncPromptSourceUI();
  renderBatchTable();
  updateRetryUI();
}
function renderBatchTable() {
  const batchFiles = getBatchFiles();
  document.getElementById("batchTableBody").innerHTML = batchFiles.map((f, i) => `<tr><td>${i + 1}</td><td>${esc(f.name)}</td><td>${f.prompts.length}</td><td>${f.status}</td></tr>`).join("") || '<tr><td colspan="4" style="text-align:center;color:#6b7280;padding:12px">Chưa có file</td></tr>';
  if (!currentJobId) populateResultsTable();
}

// ── Ref Images (per-row + bulk) ──
function importRefAll() { refImportTargetRow = -1; document.getElementById("refBulkInput").click(); }

function handleRefBulkImport(input) {
  const files = Array.from(input.files); if (!files.length) return;
  const total = getBatchFiles().flatMap(f => f.prompts).length;
  const isR2V = currentMode === "r2v";
  let loaded = 0; const imgs = [];
  files.forEach(file => {
    const r = new FileReader();
    r.onload = e => {
      imgs.push(e.target.result);
      if (++loaded !== files.length) return;
      for (let i = 0; i < total; i++) {
        const img = imgs.length === 1 ? imgs[0] : (imgs[i] || imgs[imgs.length - 1]);
        if (isR2V) {
          // R2V: append vào list hiện có, max 15
          const existing = Array.isArray(rowRefImages[i]) ? rowRefImages[i] : (rowRefImages[i] ? [rowRefImages[i]] : []);
          rowRefImages[i] = [...existing, img].slice(0, 15);
        } else {
          rowRefImages[i] = img; // I2V/FL: replace (chỉ 1 ảnh)
        }
      }
      refreshRefCells();
    };
    r.readAsDataURL(file);
  });
  input.value = "";
}

function handleRefRowImport(input) {
  const files = Array.from(input.files); if (!files.length) return;
  const idx = refImportTargetRow;
  const isR2V = currentMode === "r2v";
  let loaded = 0; const imgs = [];
  files.forEach(file => {
    const r = new FileReader();
    r.onload = e => {
      imgs.push(e.target.result);
      if (++loaded !== files.length) return;
      if (isR2V) {
        const existing = Array.isArray(rowRefImages[idx]) ? rowRefImages[idx] : (rowRefImages[idx] ? [rowRefImages[idx]] : []);
        rowRefImages[idx] = [...existing, ...imgs].slice(0, 15);
      } else {
        rowRefImages[idx] = imgs[0];
      }
      refreshRefCells();
    };
    r.readAsDataURL(file);
  });
  input.value = "";
}

function clearRefAll() {
  modeRefImages[currentMode] = {}; rowRefImages = modeRefImages[currentMode];
  modeEndImages[currentMode] = {}; endRowImages = modeEndImages[currentMode];
  modeReviewFlags[currentMode] = {};
  refreshRefCells();
  updateRetryUI();
}
function removeRefImg(idx, imgIdx = null) {
  if (imgIdx !== null && Array.isArray(rowRefImages[idx])) {
    rowRefImages[idx].splice(imgIdx, 1);
    if (!rowRefImages[idx].length) delete rowRefImages[idx];
  } else { delete rowRefImages[idx]; }
  refreshRefCells();
}
function removeEndImg(idx) { delete endRowImages[idx]; refreshRefCells(); }

function countFailedRows() {
  return (latestJobState?.videos || []).filter(v => v?.error).length;
}

function countNeedsReviewRows() {
  return Object.values(getReviewFlags()).filter(Boolean).length;
}

function updateRetryUI() {
  const failed = countFailedRows();
  const needsReview = countNeedsReviewRows();
  const rerunActive = !!modeRerunSnapshots[currentMode];
  const summaryEl = document.getElementById("retrySummary");
  const badgeEl = document.getElementById("retryStateBadge");
  const failedBtn = document.getElementById("rerunFailedBtn");
  const reviewBtn = document.getElementById("rerunNeedsReviewBtn");
  const restoreBtn = document.getElementById("restoreBatchBtn");
  if (!summaryEl || !badgeEl || !failedBtn || !reviewBtn || !restoreBtn) return;
  summaryEl.textContent = rerunActive
    ? `Đang mở danh sách rerun rút gọn. Lỗi: ${failed} dòng, chưa đẹp: ${needsReview} dòng.`
    : `Hiện có ${failed} dòng lỗi và ${needsReview} dòng đã đánh dấu chưa đẹp trong batch hiện tại.`;
  badgeEl.className = `badge ${rerunActive ? "badge-running" : (failed || needsReview ? "badge-done" : "badge-pending")}`;
  badgeEl.textContent = rerunActive ? "Rerun" : (failed || needsReview ? "Sẵn sàng" : "Chờ");
  failedBtn.disabled = !!currentJobId || failed === 0;
  reviewBtn.disabled = !!currentJobId || needsReview === 0;
  restoreBtn.disabled = !!currentJobId || !rerunActive;
}

function toggleNeedsReview(idx) {
  const flags = getReviewFlags();
  flags[idx] = !flags[idx];
  if (!flags[idx]) delete flags[idx];
  const row = document.getElementById(`resRow${idx}`);
  if (row) row.classList.toggle("row-review", !!flags[idx]);
  updateResults(latestJobState || { videos: [] });
  updateRetryUI();
}

function buildRerunSubset(filter) {
  const batchFiles = getBatchFiles();
  const reviewFlags = getReviewFlags();
  const videoResults = latestJobState?.videos || [];
  const selected = [];
  let globalIdx = 0;
  batchFiles.forEach(file => {
    const prompts = [];
    file.prompts.forEach(prompt => {
      const picked = filter === "failed" ? !!videoResults[globalIdx]?.error : !!reviewFlags[globalIdx];
      if (picked) prompts.push({ prompt, sourceIdx: globalIdx });
      globalIdx++;
    });
    if (prompts.length) selected.push({ name: file.name, prompts });
  });
  return selected;
}

function snapshotCurrentBatchForRerun() {
  if (modeRerunSnapshots[currentMode]) return;
  modeRerunSnapshots[currentMode] = {
    batchFiles: JSON.parse(JSON.stringify(getBatchFiles())),
    sourcePaths: { ...getSourcePaths() },
    refImages: JSON.parse(JSON.stringify(modeRefImages[currentMode] || {})),
    endImages: JSON.parse(JSON.stringify(modeEndImages[currentMode] || {})),
    reviewFlags: { ...(modeReviewFlags[currentMode] || {}) },
  };
}

function applyRerunSubset(subset, label) {
  snapshotCurrentBatchForRerun();
  const nextFiles = [];
  const nextRefs = {};
  const nextEnds = {};
  const nextReview = {};
  let nextIdx = 0;
  subset.forEach(file => {
    nextFiles.push({ name: `${file.name} • ${label}`, prompts: file.prompts.map(item => item.prompt), status: "Chờ" });
    file.prompts.forEach(item => {
      if (modeRefImages[currentMode]?.[item.sourceIdx] !== undefined) nextRefs[nextIdx] = JSON.parse(JSON.stringify(modeRefImages[currentMode][item.sourceIdx]));
      if (modeEndImages[currentMode]?.[item.sourceIdx] !== undefined) nextEnds[nextIdx] = JSON.parse(JSON.stringify(modeEndImages[currentMode][item.sourceIdx]));
      if (modeReviewFlags[currentMode]?.[item.sourceIdx]) nextReview[nextIdx] = true;
      nextIdx++;
    });
  });
  modeBatchFiles[currentMode] = nextFiles;
  modeRefImages[currentMode] = nextRefs;
  modeEndImages[currentMode] = nextEnds;
  modeReviewFlags[currentMode] = nextReview;
  rowRefImages = modeRefImages[currentMode];
  endRowImages = modeEndImages[currentMode];
  latestJobState = null;
  activeJobMode = null;
  syncPromptSourceUI();
  renderBatchTable();
  populateResultsTable();
  clearProgressUI();
  updateRetryUI();
}

async function runRerunSelection(filter) {
  const subset = buildRerunSubset(filter);
  const count = subset.reduce((sum, file) => sum + file.prompts.length, 0);
  if (!count) {
    sAlert(filter === "failed" ? "Không có dòng lỗi để chạy lại." : "Chưa có dòng nào được đánh dấu chưa đẹp.");
    return;
  }
  const confirmed = await sConfirm(
    filter === "failed"
      ? `Chạy lại ${count} dòng lỗi trong batch hiện tại?`
      : `Chạy lại ${count} dòng đã đánh dấu chưa đẹp trong batch hiện tại?`,
    "Chạy lại"
  );
  if (!confirmed) return;
  applyRerunSubset(subset, filter === "failed" ? "retry lỗi" : "retry chưa đẹp");
  await startGeneration();
}

function rerunFailedRows() { return runRerunSelection("failed"); }
function rerunNeedsReviewRows() { return runRerunSelection("needs_review"); }

function restoreOriginalBatchAfterRerun() {
  const snapshot = modeRerunSnapshots[currentMode];
  if (!snapshot) return;
  modeBatchFiles[currentMode] = JSON.parse(JSON.stringify(snapshot.batchFiles));
  modeSourcePaths[currentMode] = { ...snapshot.sourcePaths };
  modeRefImages[currentMode] = JSON.parse(JSON.stringify(snapshot.refImages));
  modeEndImages[currentMode] = JSON.parse(JSON.stringify(snapshot.endImages));
  modeReviewFlags[currentMode] = { ...snapshot.reviewFlags };
  rowRefImages = modeRefImages[currentMode];
  endRowImages = modeEndImages[currentMode];
  modeRerunSnapshots[currentMode] = null;
  latestJobState = null;
  activeJobMode = null;
  syncPromptSourceUI();
  renderBatchTable();
  populateResultsTable();
  clearProgressUI();
  updateRetryUI();
}

// ── Generate ──
async function startGeneration() {
  if (!cookies.length) { sAlert("Vui lòng thêm cookie."); return; }
  const prompts = getBatchFiles().flatMap(f => f.prompts);
  if (!prompts.length) { sAlert("Chọn file .txt có prompts."); return; }

  const model = document.getElementById("modelSelect").value;
  const num_videos = parseInt(document.getElementById("numVideosInput").value) || 1;

  // Validate ảnh theo mode
  if (currentMode === "i2v" || currentMode === "r2v") {
    const missing = prompts.findIndex((_, i) => !rowRefImages[i]);
    if (missing >= 0) { sAlert(`Dòng #${missing + 1} chưa có ảnh. Dùng nút thêm ảnh cho tất cả để gán nhanh.`); return; }
  }
  if (currentMode === "fl") {
    const missingStart = prompts.findIndex((_, i) => !rowRefImages[i]);
    const missingEnd   = prompts.findIndex((_, i) => !endRowImages[i]);
    if (missingStart >= 0) { sAlert(`Dòng #${missingStart + 1} chưa có ảnh đầu vào.`); return; }
    if (missingEnd >= 0)   { sAlert(`Dòng #${missingEnd + 1} chưa có ảnh kết thúc.`); return; }
  }

  document.getElementById("progressCard").style.display = "block";
  document.getElementById("runBtn").disabled = true;
  document.getElementById("stopBtn").disabled = false;
  populateResultsTable();
  updateRetryUI();

  const refMap = {}, endMap = {};
  prompts.forEach((_, i) => {
    const raw = rowRefImages[i];
    if (raw) {
      // R2V: gửi list; các mode khác: gửi string
      refMap[String(i)] = currentMode === "r2v"
        ? (Array.isArray(raw) ? raw : [raw])
        : (Array.isArray(raw) ? raw[0] : raw);
    }
    if (endRowImages[i]) endMap[String(i)] = endRowImages[i];
  });

  try {
    savedVideoHistoryUrls.clear();
    autoDownloadedVideoUrls.clear();
    const body = { mode: currentMode, model, num_videos, prompts, ref_images: refMap, end_images: endMap,
                   delay: parseInt(document.getElementById("delayInput").value) || 3 };
    const res = await apiFetch("/generate-video", { method: "POST", body: JSON.stringify(body) });
    if (!res.ok) { const e = await res.json().catch(() => ({})); throw new Error(e?.detail || e?.error || `HTTP ${res.status}`); }
    const job = await res.json();
    currentJobId = job.job_id;
    activeJobMode = currentMode;
    latestJobState = job;
    startPolling();
  } catch (e) {
    sAlert("Lỗi: " + e.message, "error");
    document.getElementById("runBtn").disabled = false;
    document.getElementById("stopBtn").disabled = true;
    document.getElementById("progressCard").style.display = "none";
  }
}

function startPolling() {
  if (pollInterval) clearInterval(pollInterval);
  pollFailCount = 0;
  pollInterval = setInterval(async () => {
    if (!currentJobId) return;
    try {
      const res = await apiFetch(`/video-jobs/${currentJobId}`);
      if (!res.ok) { if (++pollFailCount >= 5) { clearInterval(pollInterval); currentJobId = null; sAlert("Mất kết nối server", "error"); resetUI(); } return; }
      pollFailCount = 0;
      const job = await res.json();
      latestJobState = job;
      if (currentMode === activeJobMode) {
        updateProgress(job);
        updateResults(job);
      }
      persistVideoHistoryIncrementally(job, currentJobId, activeJobMode).catch(() => {});
      if (autoDownloadEachEnabled) {
        autoDownloadFinishedVideos(job).catch(() => {});
      }
      if (job.status === "done" || job.status === "error") {
        clearInterval(pollInterval);
        pollInterval = null;
        const finishedJobId = currentJobId;
        const finishedMode = activeJobMode;
        persistVideoHistoryIncrementally(job, finishedJobId, finishedMode).catch(() => {});
        if (autoDownloadEachEnabled) autoDownloadFinishedVideos(job).catch(() => {});
        currentJobId = null; resetUI();
        if (job.status === "error") sAlert(job.error || "Lỗi", "error");
        else sSuccess(`Hoàn thành ${job.completed} prompt!`);
      }
    } catch (e) { if (++pollFailCount >= 5) { clearInterval(pollInterval); currentJobId = null; sAlert("Mất kết nối", "error"); resetUI(); } }
  }, 3000);
}

async function stopGeneration() {
  if (!currentJobId) return;
  clearInterval(pollInterval);
  await apiFetch(`/jobs/${currentJobId}`, { method: "DELETE" }).catch(() => {});
  currentJobId = null; resetUI();
}

function resetUI() {
  activeJobMode = null;
  document.getElementById("runBtn").disabled = false;
  document.getElementById("stopBtn").disabled = true;
  clearProgressUI();
  updateRetryUI();
}

function clearProgressUI() {
  document.getElementById("progressCard").style.display = "none";
  document.getElementById("progressFill").style.width = "0%";
  document.getElementById("progressText").textContent = "0 / 0 prompt (0%)";
  const progressBadge = document.getElementById("progressBadge");
  progressBadge.className = "badge badge-pending";
  progressBadge.textContent = "Chờ";
  const statusBadge = document.getElementById("statusBadge");
  if (statusBadge) {
    statusBadge.className = "";
    statusBadge.textContent = "";
  }
}

function getBatchNameForMode(mode) {
  const strip = n => n.replace(/\.txt$/i, "");
  const files = modeBatchFiles[mode] || [];
  const names = files.map(f => strip(f.name));
  return names.length ? names.join(", ") : "Untitled";
}

function getPromptFileMapForMode(mode) {
  const strip = n => n.replace(/\.txt$/i, "");
  const files = modeBatchFiles[mode] || [];
  const map = [];
  files.forEach(f => f.prompts.forEach(() => map.push(strip(f.name))));
  return map;
}

async function saveVideoHistory(job, jobId, mode) {
  const promptFileMap = getPromptFileMapForMode(mode);
  const batchName = getBatchNameForMode(mode);
  const items = (job.videos || []).flatMap((video, idx) => {
    if (!video?.urls?.length) return [];
    return video.urls.filter(url => {
      const key = `${jobId}::${url}`;
      if (savedVideoHistoryUrls.has(key)) return false;
      savedVideoHistoryUrls.add(key);
      return true;
    }).map(url => ({
      job_id: jobId,
      prompt: video.prompt || "",
      model: video.model || "",
      video_url: url,
      media_type: "video",
      batch_name: batchName,
      file_name: promptFileMap[idx] || "",
    }));
  });
  if (items.length) {
    await apiFetch("/user/history", { method: "POST", body: JSON.stringify({ items }) });
  }
}

async function persistVideoHistoryIncrementally(job, jobId, mode) {
  if (!jobId || !mode || !job?.videos?.length) return;
  await saveVideoHistory(job, jobId, mode);
}

function buildVideoFilename(url, index) {
  try {
    const pathname = new URL(url).pathname || "";
    const fromPath = pathname.split("/").pop();
    if (fromPath && fromPath.includes(".")) return fromPath;
  } catch (_) {}
  const ext = url.includes(".webm") ? "webm" : url.includes(".mov") ? "mov" : "mp4";
  return `video_${String(index + 1).padStart(3, "0")}.${ext}`;
}

async function showVideoAutoDownloadGuide() {
  let linkClicked = false;
  await Swal.fire({
    title: "Tự động tải video",
    html: `<div style="text-align:left;font-size:0.88rem;line-height:1.65">
      <p>Bật tính năng này để mỗi video hoàn thành sẽ tự tải ngay về máy.</p>
      <p style="margin-top:8px"><b>Thiết lập một lần trong Chrome:</b></p>
      <ol style="padding-left:18px">
        <li>Bấm vào liên kết <a href="chrome://settings/downloads" target="_blank" rel="noopener noreferrer" id="videoAutoDlLink" style="color:#0369a1;font-weight:700">mở cài đặt tải xuống</a></li>
        <li>Nếu Chrome chặn không cho website mở trang này, dán <code>chrome://settings/downloads</code> vào thanh địa chỉ</li>
        <li>Cho phép trang tải nhiều tệp tự động để hệ thống tự tải không hỏi lại</li>
      </ol>
      <p style="margin-top:8px;color:#64748b">Chrome có thể chặn website mở trực tiếp trang <code>chrome://</code>. Nếu vậy, hệ thống sẽ tự copy sẵn đường dẫn để anh/chị dán nhanh.</p>
    </div>`,
    icon: "info",
    showConfirmButton: true,
    confirmButtonText: "Đã mở cài đặt",
    confirmButtonColor: "#16a34a",
    allowOutsideClick: false,
    allowEscapeKey: false,
    didOpen: () => {
      const confirmBtn = Swal.getConfirmButton();
      if (confirmBtn) confirmBtn.style.display = "none";
      const link = document.getElementById("videoAutoDlLink");
      if (link) {
        link.addEventListener("click", async ev => {
          try { await navigator.clipboard.writeText("chrome://settings/downloads"); } catch (_) {}
          link.textContent = "Đã thử mở và copy sẵn đường dẫn";
          if (!linkClicked && confirmBtn) {
            linkClicked = true;
            confirmBtn.style.display = "inline-flex";
          }
        });
      }
    }
  });
  autoDownloadGuideAcknowledged = true;
  localStorage.setItem("bp_video_dl_guide", "1");
}

async function toggleAutoDownloadEach(enabled) {
  if (enabled && !autoDownloadGuideAcknowledged) {
    await showVideoAutoDownloadGuide();
  }
  autoDownloadEachEnabled = !!enabled;
  localStorage.setItem("bp_video_auto_each_download", autoDownloadEachEnabled ? "1" : "0");
  const autoToggle = document.getElementById("autoDownloadEachToggle");
  if (autoToggle) autoToggle.checked = autoDownloadEachEnabled;
  if (autoDownloadEachEnabled) sSuccess("Đã bật tự tải từng video");
}

async function autoDownloadFinishedVideos(job) {
  const videos = job?.videos || [];
  let idx = 0;
  for (const video of videos) {
    const urls = video?.urls || [];
    for (const url of urls) {
      const key = `${job.job_id || currentJobId || "job"}::${url}`;
      if (autoDownloadedVideoUrls.has(key)) continue;
      autoDownloadedVideoUrls.add(key);
      try {
        const resp = await fetch(url);
        const blob = await resp.blob();
        const objectUrl = URL.createObjectURL(blob);
        const a = document.createElement("a");
        a.href = objectUrl;
        a.download = buildVideoFilename(url, idx);
        document.body.appendChild(a);
        a.click();
        a.remove();
        URL.revokeObjectURL(objectUrl);
        idx++;
        await new Promise(resolve => setTimeout(resolve, 180));
      } catch (_) {
        autoDownloadedVideoUrls.delete(key);
      }
    }
  }
}

function updateProgress(job) {
  const pct = job.total > 0 ? Math.round((job.completed / job.total) * 100) : 0;
  document.getElementById("progressFill").style.width = pct + "%";
  document.getElementById("progressText").textContent = `${job.completed} / ${job.total} prompt (${pct}%)`;
  const labels = { pending: "Chờ", running: "Đang tạo", done: "Hoàn thành", error: "Lỗi" };
  const badge = document.getElementById("progressBadge");
  badge.className = `badge badge-${job.status}`; badge.textContent = labels[job.status] || job.status;
  const sb = document.getElementById("statusBadge");
  if (sb) { sb.className = `badge badge-${job.status}`; sb.textContent = labels[job.status] || job.status; }
}

function populateResultsTable() {
  const batchFiles = getBatchFiles();
  const allPrompts = batchFiles.flatMap(f => f.prompts);
  const tbody = document.getElementById("resultsBody");
  const empty = document.getElementById("resultsEmpty");
  const badge = document.getElementById("promptCountBadge");
  const cfg = MODE_CONFIG[currentMode];
  const hasRef = !!cfg.refLabel;
  const hasEnd = !!cfg.endLabel;

  if (!allPrompts.length) { tbody.innerHTML = ""; empty.style.display = ""; badge.style.display = "none"; updateRetryUI(); return; }
  empty.style.display = "none"; badge.style.display = ""; badge.textContent = `${allPrompts.length} dòng`;

  let html = "", idx = 0;
  batchFiles.forEach(f => {
    const strip = n => n.replace(/\.txt$/i, "");
    const cols = 4 + (hasRef ? 1 : 0) + (hasEnd ? 1 : 0);
    html += `<tr class="file-separator"><td colspan="${cols}">📄 ${esc(strip(f.name))} (${f.prompts.length} prompt)</td></tr>`;
    f.prompts.forEach(text => {
      const refCell = hasRef
        ? (() => {
            const raw = rowRefImages[idx];
            const imgs = Array.isArray(raw) ? raw : (raw ? [raw] : []);
            const isR2V = currentMode === "r2v";
            if (imgs.length) {
              const thumbs = imgs.map((src, j) =>
                `<span class="ref-wrap"><img src="${src}" class="ref-thumb" onclick="window.open(this.src)"/><span class="ref-del" onclick="removeRefImg(${idx},${j})">✕</span></span>`
              ).join("");
              const addBtn = isR2V && imgs.length < 15 ? `<span class="ref-add-btn" onclick="importRefForRow(${idx})">+</span>` : "";
              return `<div class="ref-cell">${thumbs}${addBtn}</div>`;
            }
            return `<span class="ref-add-btn" onclick="importRefForRow(${idx})">+</span>`;
          })()
        : "";
      const endCell = hasEnd
        ? (endRowImages[idx]
            ? `<span class="ref-wrap"><img src="${endRowImages[idx]}" class="ref-thumb" onclick="window.open(this.src)"/><span class="ref-del" onclick="removeEndImg(${idx})">✕</span></span>`
            : `<span class="ref-add-btn" onclick="importEndForRow(${idx})">+</span>`)
        : "";
      html += `<tr id="resRow${idx}">
        <td>${idx + 1}</td>
        <td><div class="prompt-cell">${esc(text)}</div></td>
        ${hasRef ? `<td style="text-align:center">${refCell}</td>` : ""}
        ${hasEnd ? `<td style="text-align:center">${endCell}</td>` : ""}
        <td class="status-cell">Chờ</td>
        <td>—</td>
      </tr>`;
      idx++;
    });
  });
  tbody.innerHTML = html;
  Object.entries(getReviewFlags()).forEach(([idx, active]) => {
    const row = document.getElementById(`resRow${idx}`);
    if (row) row.classList.toggle("row-review", !!active);
  });
  updateRetryUI();
}

function getActiveVideoSlots() {
  const cookieCount = Math.max(1, cookies.length || 1);
  const numVideos = parseInt(document.getElementById("numVideosInput")?.value || "1") || 1;
  let workersPerCookie = 1;
  if (currentMode === "t2v") {
    if (numVideos <= 1) workersPerCookie = 3;
    else if (numVideos === 2) workersPerCookie = 2;
    else workersPerCookie = 1;
  }
  return Math.max(1, workersPerCookie * cookieCount);
}

function updateResults(job) {
  const batchFiles = getBatchFiles();
  const videos = job.videos || [];
  const cfg = MODE_CONFIG[currentMode];
  const statusCellIdx = 2 + (cfg.refLabel ? 1 : 0) + (cfg.endLabel ? 1 : 0);
  const videoCellIdx = statusCellIdx + 1;

  videos.forEach((v, idx) => {
    const row = document.getElementById(`resRow${idx}`); if (!row) return;
    if (!v) return;
    if (v.urls && v.urls.length) {
      row.className = "row-done";
      row.cells[statusCellIdx].innerHTML = `<span class="status-ok">✅ Xong</span><br><span style="font-size:0.68rem;color:var(--muted)">${modeLabel(v.mode || currentMode)}</span>`;
      const reviewActive = !!getReviewFlags()[idx];
      row.cells[videoCellIdx].innerHTML = `
        <div class="video-action-stack">
          ${v.urls.map((u, i) => `<a href="${u}" target="_blank" class="btn btn-green btn-sm" style="margin:2px">Video ${i + 1}</a>`).join("")}
          <button class="btn-chip ${reviewActive ? "active" : ""}" onclick="toggleNeedsReview(${idx})">
            ${reviewActive ? "Bỏ cờ chưa đẹp" : "Đánh dấu chưa đẹp"}
          </button>
        </div>
        <div class="retry-note">${reviewActive ? "Dòng này sẽ được gom vào nhóm chạy lại file chưa đẹp." : "Nếu chưa ưng ý, đánh dấu để gom vào lượt chạy lại tiếp theo."}</div>`;
    } else if (v.error) {
      row.className = "row-error";
      row.cells[statusCellIdx].innerHTML = '<span class="status-err">❌ Lỗi</span>';
      row.cells[videoCellIdx].innerHTML = `<span style="font-size:0.7rem;color:var(--error)">${esc(v.error)}</span><div class="retry-note">Dòng lỗi sẽ được gom vào nút chạy lại file lỗi.</div>`;
    }
  });

  // Reset untouched rows back to pending before marking running slots
  const totalRows = batchFiles.flatMap(f => f.prompts).length;
  for (let idx = 0; idx < totalRows; idx++) {
    const row = document.getElementById(`resRow${idx}`);
    if (!row || row.className === "row-done" || row.className === "row-error") continue;
    row.className = "";
    row.cells[statusCellIdx].innerHTML = "Chờ";
    row.cells[videoCellIdx].innerHTML = "—";
  }

  // Mark all currently running rows based on the active parallel slots
  if (job.status === "running" && job.completed < job.total) {
    const runningSlots = Math.min(getActiveVideoSlots(), Math.max(0, job.total - job.completed));
    const pendingIndexes = [];
    for (let idx = 0; idx < job.total; idx++) {
      if (!videos[idx]) pendingIndexes.push(idx);
    }
    pendingIndexes.slice(0, runningSlots).forEach((idx, pos) => {
      const row = document.getElementById(`resRow${idx}`);
      if (!row || row.className === "row-done" || row.className === "row-error") return;
      row.className = "row-running";
      row.cells[statusCellIdx].innerHTML = '<span style="color:#1d4ed8;font-weight:600">🔄 Đang tạo...</span>';
      if (pos === 0) row.scrollIntoView({ behavior: "smooth", block: "center" });
    });
  }
  // Update batch table status
  let offset = 0;
  batchFiles.forEach(f => {
    const end = offset + f.prompts.length;
    const done = videos.slice(offset, end).filter(v => !!v).length;
    if (done >= f.prompts.length) f.status = videos.slice(offset, end).every(v => v && v.urls?.length) ? "✅ Xong" : "⚠️ Có lỗi";
    else if (done > 0) f.status = `🔄 ${done}/${f.prompts.length}`;
    else f.status = "Chờ";
    offset = end;
  });
  document.getElementById("batchTableBody").innerHTML = batchFiles.map((f, i) => `<tr><td>${i + 1}</td><td>${esc(f.name)}</td><td>${f.prompts.length}</td><td>${f.status}</td></tr>`).join("");
  Object.entries(getReviewFlags()).forEach(([idx, active]) => {
    const row = document.getElementById(`resRow${idx}`);
    if (row) row.classList.toggle("row-review", !!active);
  });
  updateRetryUI();
}

function modeLabel(mode) {
  const labels = {
    t2v: "T2V",
    i2v: "I2V",
    fl: "FL",
    r2v: "R2V",
  };
  return labels[mode] || "Video";
}

function syncPromptSourceUI() {
  const sourcePaths = getSourcePaths();
  document.getElementById("txtFilePath").value = sourcePaths.txt || "";
  document.getElementById("folderTxtPath").value = sourcePaths.folder || "";
}

let historyPage = 0;
let historyLoading = false;
const HISTORY_LIMIT = 24;
let historyView = "folders";
let historyCurrentJob = null;
let historyCurrentFile = null;
let historyBreadcrumb = [];

async function loadHistory() {
  historyPage = 0;
  historyView = "folders";
  historyCurrentJob = null;
  historyCurrentFile = null;
  document.getElementById("historySelectAll").checked = false;
  await fetchHistoryPage();
}

async function fetchHistoryPage() {
  if (historyLoading) return;
  historyLoading = true;
  const grid = document.getElementById("historyGrid");
  const prevBtn = document.getElementById("historyPrev");
  const nextBtn = document.getElementById("historyNext");
  const pageInfo = document.getElementById("historyPageInfo");
  grid.innerHTML = '<div class="empty-state">⏳ Đang tải...</div>';
  updateHistoryBreadcrumb();
  try {
    if (historyView === "folders") {
      const res = await apiFetch(`/user/history/groups?media_type=video&limit=${HISTORY_LIMIT}&offset=${historyPage * HISTORY_LIMIT}`);
      if (!res.ok) throw new Error();
      const groups = await res.json();
      renderHistoryFolders(groups, grid, "📁", g => g.batch_name || g.job_id, g => `${g.model || ""} · ${g.count} video`, g => openHistoryJob(g.job_id, g.batch_name || g.job_id), g => deleteHistoryJob(g.job_id));
      prevBtn.disabled = historyPage === 0;
      nextBtn.disabled = groups.length < HISTORY_LIMIT;
    } else if (historyView === "files") {
      const res = await apiFetch(`/user/history/subgroups?media_type=video&job_id=${encodeURIComponent(historyCurrentJob)}`);
      if (!res.ok) throw new Error();
      const subs = await res.json();
      if (subs.length <= 1) {
        historyView = "videos";
        historyCurrentFile = subs[0]?.file_name || "";
        historyLoading = false;
        return fetchHistoryPage();
      }
      renderHistoryFolders(subs, grid, "📄", s => s.file_name || "Untitled", s => `${s.count} video`, s => openHistoryFile(s.file_name), null);
      prevBtn.disabled = true;
      nextBtn.disabled = true;
    } else {
      const qs = `media_type=video&job_id=${encodeURIComponent(historyCurrentJob)}&file_name=${encodeURIComponent(historyCurrentFile || "")}&limit=${HISTORY_LIMIT}&offset=${historyPage * HISTORY_LIMIT}`;
      const res = await apiFetch(`/user/history?${qs}`);
      if (!res.ok) throw new Error();
      const items = await res.json();
      renderVideoHistoryGrid(items, grid);
      prevBtn.disabled = historyPage === 0;
      nextBtn.disabled = items.length < HISTORY_LIMIT;
    }
    pageInfo.textContent = `Trang ${historyPage + 1}`;
  } catch (e) {
    grid.innerHTML = '<div class="empty-state"><div class="empty-icon">📜</div><div>Lỗi tải lịch sử</div></div>';
  } finally {
    historyLoading = false;
  }
}

function renderHistoryFolders(items, grid, icon, getName, getMeta, onClick, onDelete) {
  grid.innerHTML = "";
  grid.classList.remove("grid-mode");
  if (!items.length) {
    grid.innerHTML = '<div class="empty-state"><div class="empty-icon">📜</div><div>Chưa có lịch sử</div></div>';
    return;
  }
  items.forEach((item, i) => {
    const card = document.createElement("div");
    card.className = "hist-folder";
    const time = item.created_at ? new Date(item.created_at + "Z").toLocaleString("vi-VN") : "";
    const count = Number(item.count || 0);
    const delId = item.job_id || item.file_name || i;
    const delBtn = onDelete ? `<button class="btn btn-red btn-sm hist-folder-del" onclick="event.stopPropagation();deleteHistoryJob('${esc(delId)}')" title="Xóa">🗑</button>` : "";
    card.innerHTML = `
      <div class="hist-folder-icon">${icon}</div>
      <div class="hist-folder-info">
        <div class="hist-folder-name">${esc(getName(item))}</div>
        <div class="hist-folder-meta">${esc(getMeta(item))}</div>
        <div class="hist-folder-badges">
          <span class="hist-badge hist-badge-blue">Ngày tạo: ${esc(time || "Chưa rõ")}</span>
          <span class="hist-badge ${count > 0 ? "hist-badge-green" : "hist-badge-red"}">${count} video đã lưu</span>
        </div>
      </div>${delBtn}`;
    card.onclick = () => onClick(item);
    grid.appendChild(card);
  });
}

function renderVideoHistoryGrid(items, grid) {
  grid.innerHTML = "";
  grid.classList.add("grid-mode");
  if (!items.length) {
    grid.innerHTML = '<div class="empty-state"><div class="empty-icon">🎬</div><div>Chưa có video</div></div>';
    return;
  }
  items.forEach(item => {
    const url = item.media_url || item.video_url || item.image_url;
    const time = item.created_at ? new Date(item.created_at + "Z").toLocaleString("vi-VN") : "";
    const card = document.createElement("div");
    card.className = "img-card";
    card.innerHTML = `${url ? `<label class="hist-cb"><input type="checkbox" class="hist-select" data-url="${url}"/></label>` : ""}
      ${url ? `<div class="video-history-preview" onclick="openHistoryVideo('${esc(url)}')"><div class="video-history-play">▶</div><div class="video-history-label">Xem video</div></div>` : `<div class="img-card-error">❌ ${esc(item.error || "Thất bại")}</div>`}
      <div class="img-card-body">
        <div class="img-card-prompt">${esc(item.prompt)}</div>
        <div style="font-size:0.68rem;color:var(--muted);margin-top:4px">${esc(item.model || "")} · ${time}</div>
      </div>
      ${url ? `<div class="img-card-actions"><a href="${url}" target="_blank">🔗 Mở</a></div>` : ""}`;
    grid.appendChild(card);
  });
}

function openHistoryVideo(url) {
  Swal.fire({
    width: "min(960px, 92vw)",
    showConfirmButton: false,
    showCloseButton: true,
    html: `<video src="${url}" controls autoplay preload="none" style="width:100%;max-height:75vh;background:#000;border-radius:12px"></video>`,
  });
}

function updateHistoryBreadcrumb() {
  const el = document.getElementById("historyTitle");
  const backBtn = document.getElementById("historyBack");
  if (historyView === "folders") {
    el.textContent = "📜 Lịch sử tạo video";
    backBtn.style.display = "none";
  } else {
    backBtn.style.display = "";
    const parts = ["📜 Lịch sử", historyBreadcrumb[0] || "", historyView === "videos" ? (historyCurrentFile || "") : ""].filter(Boolean);
    el.textContent = parts.join(" › ");
  }
}

function openHistoryJob(jobId, name) {
  historyView = "files";
  historyCurrentJob = jobId;
  historyCurrentFile = null;
  historyBreadcrumb = [name];
  historyPage = 0;
  document.getElementById("historySelectAll").checked = false;
  fetchHistoryPage();
}

function openHistoryFile(fileName) {
  historyView = "videos";
  historyCurrentFile = fileName;
  historyPage = 0;
  document.getElementById("historySelectAll").checked = false;
  fetchHistoryPage();
}

function historyGoBack() {
  if (historyView === "videos" && historyCurrentFile !== null) {
    historyView = "files";
    historyCurrentFile = null;
  } else {
    historyView = "folders";
    historyCurrentJob = null;
    historyCurrentFile = null;
    historyBreadcrumb = [];
  }
  historyPage = 0;
  document.getElementById("historySelectAll").checked = false;
  fetchHistoryPage();
}

async function deleteHistoryJob(jobId) {
  if (!await sConfirm("Xóa toàn bộ video trong batch này?")) return;
  await apiFetch(`/user/history/${encodeURIComponent(jobId)}`, { method: "DELETE" });
  fetchHistoryPage();
}

async function deleteHistoryFailed() {
  if (!await sConfirm("Xóa lịch sử lỗi?")) return;
  await apiFetch("/user/history/failed?media_type=video", { method: "DELETE" });
  fetchHistoryPage();
}

function historyPrevPage() {
  if (historyPage > 0) {
    historyPage--;
    document.getElementById("historySelectAll").checked = false;
    fetchHistoryPage();
  }
}

function historyNextPage() {
  historyPage++;
  document.getElementById("historySelectAll").checked = false;
  fetchHistoryPage();
}

function historyToggleAll(checked) {
  document.querySelectorAll("#historyGrid .hist-select").forEach(cb => cb.checked = checked);
}

function getHistoryUrls(selectedOnly) {
  const selector = selectedOnly ? "#historyGrid .hist-select:checked" : "#historyGrid .hist-select";
  return [...document.querySelectorAll(selector)].map(cb => cb.dataset.url).filter(Boolean);
}

async function historyDownloadZip(urls, btnId) {
  if (!urls.length) { sAlert("Không có video để tải"); return; }
  const btn = document.getElementById(btnId);
  const old = btn?.textContent;
  if (btn) { btn.disabled = true; btn.textContent = "⏳ Đang gom..."; }
  try {
    if (!window.JSZip) {
      await new Promise((ok, fail) => { const s = document.createElement("script"); s.src = "https://cdnjs.cloudflare.com/ajax/libs/jszip/3.10.1/jszip.min.js"; s.onload = ok; s.onerror = fail; document.head.appendChild(s); });
    }
    const zip = new JSZip();
    let idx = 0;
    for (const url of urls) {
      try {
        const resp = await fetch(url);
        const blob = await resp.blob();
        const ext = blob.type?.includes("webm") ? "webm" : blob.type?.includes("quicktime") ? "mov" : "mp4";
        zip.file(`video_${String(++idx).padStart(3, "0")}.${ext}`, blob);
      } catch (e) {}
    }
    if (!idx) { sAlert("Không tải được video nào"); return; }
    const content = await zip.generateAsync({ type: "blob" });
    const a = document.createElement("a");
    a.href = URL.createObjectURL(content);
    a.download = `video_history_${new Date().toISOString().slice(0, 10)}.zip`;
    a.click();
    URL.revokeObjectURL(a.href);
  } catch (e) {
    sAlert("Lỗi tải ZIP: " + e.message);
  } finally {
    if (btn) { btn.disabled = false; btn.textContent = old; }
  }
}

function historyDownloadAll() { historyDownloadZip(getHistoryUrls(false), "histDlAll"); }
function historyDownloadSelected() { historyDownloadZip(getHistoryUrls(true), "histDlSel"); }

async function loadAdminUsers() {
  try {
    const res = await apiFetch("/admin/users");
    if (!res.ok) return;
    const users = await res.json();
    const now = new Date().toISOString();
    updateAdminDashboard(users);
    const tbody = document.getElementById("adminUserBody");
    tbody.innerHTML = users.map(u => {
      const planActive = ["admin", "super_admin"].includes(u.role) || (u.plan_expires_at && u.plan_expires_at > now);
      const planText = ["admin", "super_admin"].includes(u.role) ? "Không giới hạn" : u.plan_expires_at ? new Date(u.plan_expires_at).toLocaleDateString("vi-VN") : "Chưa có";
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
      <td><span class="${planClass}">${planText}</span>
        ${!["admin", "super_admin"].includes(u.role) ? `<div style="margin-top:4px;display:flex;gap:4px;flex-wrap:wrap">
          <button class="btn btn-ghost" style="padding:2px 6px;font-size:0.68rem" onclick="adminSetPlan(${u.id},7)">+7d</button>
          <button class="btn btn-ghost" style="padding:2px 6px;font-size:0.68rem" onclick="adminSetPlan(${u.id},30)">+30d</button>
          <button class="btn btn-ghost" style="padding:2px 6px;font-size:0.68rem" onclick="adminSetPlan(${u.id},90)">+90d</button>
          <button class="btn btn-ghost" style="padding:2px 6px;font-size:0.68rem;color:var(--error)" onclick="adminSetPlan(${u.id},0)">Hủy</button>
        </div>` : ""}
      </td>
      <td>${u.disabled ? '<span class="status-err">Đã khóa</span>' : '<span class="status-ok">Hoạt động</span>'}</td>
      <td>
        <button class="btn btn-ghost" onclick="adminEditUser(${u.id},'${esc(u.username)}',${u.cookie_quota ?? 5})">Sửa</button>
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
    { label: "Gói sắp hết hạn", value: `${expiringSoon} tài khoản`, note: "Ưu tiên gia hạn để tránh gián đoạn khi người dùng tạo video." },
    { label: "Tài khoản bị khóa", value: `${lockedUsers} tài khoản`, note: lockedUsers ? "Nên rà soát lý do khóa hoặc mở lại nếu cần." : "Hiện chưa có tài khoản nào bị khóa." },
  ];
  summary.innerHTML = items.map(item => `<div class="admin-summary-item"><strong>${item.label}</strong><span>${item.value}<br>${item.note}</span></div>`).join("");
}

async function adminAddUser() {
  const username = document.getElementById("adminNewUser").value.trim();
  const password = document.getElementById("adminNewPass").value;
  const role = document.getElementById("adminNewRole").value;
  const cookieQuota = parseInt(document.getElementById("adminNewCookieQuota")?.value || "5") || 5;
  if (!username || !password) { sAlert("Thiếu username/password"); return; }
  const res = await apiFetch("/admin/users", { method: "POST", body: JSON.stringify({ username, password, role, cookie_quota: cookieQuota }) });
  const data = await res.json();
  if (!res.ok) { sAlert(data.error || "Lỗi"); return; }
  document.getElementById("adminNewUser").value = "";
  document.getElementById("adminNewPass").value = "";
  if (document.getElementById("adminNewCookieQuota")) document.getElementById("adminNewCookieQuota").value = "5";
  loadAdminUsers();
}

async function adminChangeRole(id, role) { await apiFetch(`/admin/users/${id}`, { method: "PUT", body: JSON.stringify({ role }) }); loadAdminUsers(); }
async function adminToggleUser(id, disabled) { await apiFetch(`/admin/users/${id}`, { method: "PUT", body: JSON.stringify({ disabled }) }); loadAdminUsers(); }
async function adminDelUser(id) { if (!await sConfirm("Xóa user này?")) return; await apiFetch(`/admin/users/${id}`, { method: "DELETE" }); loadAdminUsers(); }
async function adminEditUser(id, username, cookieQuota) {
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
      return { username: nextUsername, password: nextPassword, cookie_quota: nextQuota };
    }
  });
  if (!result.isConfirmed) return;
  const payload = { username: result.value.username };
  if (result.value.password) payload.password = result.value.password;
  if (canEditQuota) payload.cookie_quota = result.value.cookie_quota;
  const res = await apiFetch(`/admin/users/${id}`, { method: "PUT", body: JSON.stringify(payload) });
  const data = await res.json().catch(() => ({}));
  if (!res.ok) { sAlert(data.error || "Không thể cập nhật user"); return; }
  loadAdminUsers();
}
async function adminSetPlan(id, days) {
  if (days === 0 && !await sConfirm("Hủy gói user này?")) return;
  await apiFetch(`/admin/users/${id}`, { method: "PUT", body: JSON.stringify({ plan_days: days }) });
  loadAdminUsers();
}

async function adminSetCookieQuota(id, quota) {
  await apiFetch(`/admin/users/${id}`, { method: "PUT", body: JSON.stringify({ cookie_quota: parseInt(quota) || 1 }) });
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
    if (!items.length) {
      grid.innerHTML = '<div class="empty-state"><div class="empty-icon">📜</div><div>Chưa có lịch sử</div></div>';
      return;
    }
    items.forEach(item => {
      const card = document.createElement("div");
      card.className = "img-card";
      const time = item.created_at ? new Date(item.created_at + "Z").toLocaleString("vi-VN") : "";
      const mediaUrl = item.media_url || item.video_url || item.image_url;
      if (mediaUrl) {
        const mediaBlock = item.media_type === "video"
          ? `<div class="video-history-preview" onclick="openHistoryVideo('${esc(mediaUrl)}')"><div class="video-history-play">▶</div><div class="video-history-label">Xem video</div></div>`
          : `<img src="${mediaUrl}" loading="lazy" onerror="this.style.display='none'"/>`;
        card.innerHTML = `${mediaBlock}
          <div class="img-card-body"><div class="img-card-prompt">${esc(item.prompt)}</div><div style="font-size:0.68rem;color:var(--muted);margin-top:4px">👤 ${esc(item.username)} · ${esc(item.model || "")} · ${time}</div></div>`;
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
    overview: ["Tổng quan vận hành", "Theo dõi nhanh người dùng, gói, cookie và lịch sử tạo video trong cùng một không gian làm việc.", "Bức tranh toàn hệ thống"],
    users: ["Quản lý người dùng", "Kiểm soát vai trò, trạng thái tài khoản và thông tin đăng nhập ngay trong một khu làm việc rõ ràng.", "Tài khoản và phân quyền"],
    plans: ["Quản lý gói dịch vụ", "Gia hạn, hủy hoặc kiểm tra trạng thái gói của từng tài khoản mà không phải chuyển màn hình.", "Gói và thời hạn"],
    cookies: ["Quản lý kho cookie", "Theo dõi cookie theo từng tài khoản để rà lỗi nhanh và giữ nguồn chạy video luôn sẵn sàng.", "Cookie theo tài khoản"],
    history: ["Lịch sử toàn hệ thống", "Mở lịch sử theo từng người dùng, từng batch và từng file để truy vết chính xác hơn.", "Lịch sử theo thư mục"],
  };
  const [title, subtitle, chip] = headerMap[section] || headerMap.overview;
  document.getElementById("adminViewTitle").textContent = title;
  document.getElementById("adminViewSubtitle").textContent = subtitle;
  document.getElementById("adminViewChip").textContent = chip;
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
  await apiFetch(`/admin/cookies/${id}`, { method: "DELETE" });
  if (adminCookieUserId) openAdminCookieUser(adminCookieUserId, document.getElementById("adminCookieTitle").textContent.replace("Kho cookie của ", ""));
}

async function loadAdminHistoryUsers() {
  adminHistoryState = { userId: null, username: "", jobId: null, batchName: "", fileName: null };
  document.getElementById("adminHistoryBack").style.display = "none";
  document.getElementById("adminHistoryTitle").textContent = "Lịch sử toàn hệ thống";
  document.getElementById("adminHistoryDesc").textContent = "Chọn một tài khoản bên trái để mở theo cây thư mục.";
  const res = await apiFetch(`/admin/history/users${adminHistoryMedia ? `?media_type=${adminHistoryMedia}` : ""}`);
  if (!res.ok) return;
  const users = await res.json();
  document.getElementById("adminHistoryFolders").innerHTML = users.map(u => `
    <div class="hist-folder" onclick="openAdminHistoryUser(${u.user_id},'${esc(u.username)}')">
      <div class="hist-folder-info">
        <div class="hist-folder-name">${esc(u.username)}</div>
        <div class="hist-folder-meta">${u.count} mục</div>
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
