const API_BASE = "https://banana-pro-api.kh431248.workers.dev";

// ── SweetAlert2 helpers ───────────────────────────────────────────────────────
const Toast = Swal.mixin({ toast: true, position: "top-end", showConfirmButton: false, timer: 2500, timerProgressBar: true });
function sAlert(text, icon = "info") { return Swal.fire({ text, icon, confirmButtonColor: "#16a34a" }); }
function sSuccess(text) { Toast.fire({ icon: "success", title: text }); }
function sError(text) { return Swal.fire({ text, icon: "error", confirmButtonColor: "#16a34a" }); }
async function sConfirm(text, title = "Xác nhận") { const r = await Swal.fire({ title, text, icon: "warning", showCancelButton: true, confirmButtonColor: "#16a34a", cancelButtonColor: "#6b7280", confirmButtonText: "Đồng ý", cancelButtonText: "Hủy" }); return r.isConfirmed; }

// ── Auth State ────────────────────────────────────────────────────────────────
let authToken = localStorage.getItem("bp_token") || "";
let authUser = JSON.parse(localStorage.getItem("bp_user") || "null");
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

// ── Init ──────────────────────────────────────────────────────────────────────
(async function init() {
  if (authToken && authUser) {
    try {
      const res = await apiFetch("/auth/me");
      if (res.ok) {
        const data = await res.json();
        authUser = data;
        localStorage.setItem("bp_user", JSON.stringify(data));
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

// ── Auth ──────────────────────────────────────────────────────────────────────
function showLogin() {
  document.getElementById("loginScreen").style.display = "flex";
  document.getElementById("appScreen").style.display = "none";
}

function showApp() {
  document.getElementById("loginScreen").style.display = "none";
  document.getElementById("appScreen").style.display = "block";
  document.getElementById("userInfo").textContent = `👤 ${authUser.username} (${authUser.role})`;
  // Show plan status
  const planEl = document.getElementById("planInfo");
  if (authUser.role === "admin") { planEl.textContent = "♾ Unlimited"; planEl.className = "plan-badge plan-active"; }
  else if (authUser.plan_active) {
    const exp = new Date(authUser.plan_expires_at);
    const days = Math.ceil((exp - new Date()) / 86400000);
    planEl.textContent = `📦 Còn ${days} ngày`;
    planEl.className = "plan-badge " + (days <= 3 ? "plan-expiring" : "plan-active");
  } else { planEl.textContent = "⛔ Hết hạn"; planEl.className = "plan-badge plan-expired"; }
  document.getElementById("navAdmin").style.display = authUser.role === "admin" ? "" : "none";
  loadCookiesFromDB();
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
    const data = await res.json();
    if (!res.ok) { errEl.textContent = data.error || "Lỗi"; errEl.style.display = "block"; return false; }
    authToken = data.token;
    authUser = { username: data.username, role: data.role };
    localStorage.setItem("bp_token", authToken);
    localStorage.setItem("bp_user", JSON.stringify(authUser));
    // Fetch plan info
    try { const me = await apiFetch("/auth/me"); if (me.ok) { authUser = await me.json(); localStorage.setItem("bp_user", JSON.stringify(authUser)); } } catch(_){}
    showApp();
  } catch (e) {
    errEl.textContent = "Lỗi kết nối: " + e.message;
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
  localStorage.removeItem("bp_token");
  localStorage.removeItem("bp_user");
}

// ── Tabs ──────────────────────────────────────────────────────────────────────
function showTab(tab) {
  ["Generate", "History", "Admin"].forEach(t => {
    document.getElementById("tab" + t).style.display = t.toLowerCase() === tab ? "" : "none";
    const nav = document.getElementById("nav" + t);
    if (nav) nav.classList.toggle("active", t.toLowerCase() === tab);
  });
  if (tab === "history") loadHistory();
  if (tab === "admin" && authUser?.role === "admin") { loadAdminUsers(); loadAdminHistory(); }
}

// ── Cookie Management (D1) ───────────────────────────────────────────────────
function openCookieModal() { document.getElementById("cookieModal").style.display = "flex"; }
function closeCookieModal() { document.getElementById("cookieModal").style.display = "none"; }
function closeCookieModalOutside(e) { if (e.target.id === "cookieModal") closeCookieModal(); }

async function loadCookiesFromDB() {
  try {
    const res = await apiFetch("/user/cookies");
    if (res.ok) cookies = await res.json();
    renderCookieTable();
  } catch (e) {}
}

async function addCookie() {
  const raw = document.getElementById("newCookieInput").value.trim();
  if (!raw) return;
  const parsed = parseCookieInput(raw);
  if (!parsed || !Object.keys(parsed).length) { sAlert("Cookie không hợp lệ"); return; }
  const hash = cookieHash(parsed);
  const res = await apiFetch("/user/cookies", {
    method: "POST",
    body: JSON.stringify({ cookie_raw: raw, cookie_hash: hash }),
  });
  const data = await res.json();
  if (!res.ok) { sAlert(data.error || "Lỗi"); return; }
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
  const reader = new FileReader();
  reader.onload = e => {
    const prompts = e.target.result.split("\n").map(s => s.trim()).filter(Boolean);
    batchFiles = batchFiles.filter(f => f.name !== file.name);
    batchFiles.push({ name: file.name, prompts, status: "⏳ Chờ" });
    renderBatchTable();
  };
  reader.readAsText(file);
}

function loadFolderTxt(input) {
  const files = Array.from(input.files).filter(f => f.name.endsWith(".txt"));
  if (!files.length) return;
  document.getElementById("folderTxtPath").value = input.files[0].webkitRelativePath.split("/")[0];
  let loaded = 0;
  files.forEach(file => {
    const reader = new FileReader();
    reader.onload = e => {
      const prompts = e.target.result.split("\n").map(s => s.trim()).filter(Boolean);
      batchFiles = batchFiles.filter(f => f.name !== file.name);
      batchFiles.push({ name: file.name, prompts, status: "⏳ Chờ" });
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
    row.cells[2].innerHTML = imgs.length
      ? imgs.map((s, j) => `<span class="ref-wrap"><img src="${s}" class="ref-thumb" onclick="window.open(this.src)"/><span class="ref-del" onclick="removeRefImg(${i},${j})">✕</span></span>`).join("") + `<br><span class="ref-add-btn" onclick="importRefForRow(${i})">+</span>`
      : `<span class="ref-add-btn" onclick="importRefForRow(${i})">+ ảnh</span>`;
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

  // Get first cookie raw from DB
  let cookieRaw = "";
  try {
    const res = await apiFetch("/user/cookies");
    if (res.ok) {
      const list = await res.json();
      if (list.length) {
        // Need to get raw - fetch from a dedicated endpoint or use stored
        // We'll pass cookie_id and let worker handle it
      }
    }
  } catch (e) {}

  // For now, we need the raw cookie - get it from the first cookie
  // The cookie_raw is stored in D1, we need a way to get it
  // Let's add a special generate endpoint that reads cookie from DB
  hideError(); setLoading(true); populateResultsTable(); paused = false;

  try {
    const body = { prompts, model, aspect_ratio, variants };
    if (reference_images.length) body.reference_images = reference_images;
    if (Object.keys(folder_images).length) body.folder_images = folder_images;

    const res = await apiFetch("/generate", { method: "POST", body: JSON.stringify(body) });
    if (!res.ok) { const e = await res.json().catch(() => ({})); throw new Error(e?.detail || e?.error || `HTTP ${res.status}`); }
    const job = await res.json();
    currentJobId = job.job_id;
    updateProgress(job);
    startPolling(model);
  } catch (e) {
    showError("Lỗi: " + e.message);
    setLoading(false);
  }
}

let pollFailCount = 0;

function startPolling(model) {
  pollFailCount = 0;
  pollInterval = setInterval(async () => {
    if (!currentJobId || paused) return;
    try {
      const res = await apiFetch(`/jobs/${currentJobId}`);
      if (!res.ok) {
        pollFailCount++;
        if (pollFailCount >= 5) {
          clearInterval(pollInterval);
          currentJobId = null;
          setLoading(false);
          showError("Mất kết nối tới server. Vui lòng thử lại.");
        }
        return;
      }
      pollFailCount = 0;
      const job = await res.json();
      updateProgress(job);
      updateResultsFromJob(job);
      if (job.status === "done" || job.status === "error") {
        clearInterval(pollInterval);
        const finishedJobId = currentJobId;
        currentJobId = null;
        setLoading(false);
        if (job.status === "error") showError(job.error || "Có lỗi xảy ra.");
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
        }
      }
    } catch (e) {
      pollFailCount++;
      if (pollFailCount >= 5) {
        clearInterval(pollInterval);
        currentJobId = null;
        setLoading(false);
        showError("Mất kết nối tới server. Vui lòng thử lại.");
      }
    }
  }, 2000);
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

function togglePause() { paused = !paused; document.getElementById("pauseBtn").textContent = paused ? "▶ TIẾP TỤC" : "⏸ TẠM DỪNG"; }

async function stopGeneration() {
  if (!currentJobId) return;
  clearInterval(pollInterval);
  await apiFetch(`/jobs/${currentJobId}`, { method: "DELETE" }).catch(() => {});
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
      if (!res.ok) throw new Error();
      const groups = await res.json();
      renderHistoryFolders(groups, grid, "📁", g => g.batch_name || g.job_id.slice(0, 8), g => `${g.model || ""} · ${g.count} ảnh`, g => openHistoryJob(g.job_id, g.batch_name || g.job_id), g => deleteHistoryJob(g.job_id));
      prevBtn.disabled = historyPage === 0;
      nextBtn.disabled = groups.length < HISTORY_LIMIT;
    } else if (historyView === "files") {
      const res = await apiFetch(`/user/history/subgroups?job_id=${encodeURIComponent(historyCurrentJob)}`);
      if (!res.ok) throw new Error();
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
      if (!res.ok) throw new Error();
      const items = await res.json();
      renderHistoryGrid(items, grid, false);
      prevBtn.disabled = historyPage === 0;
      nextBtn.disabled = items.length < HISTORY_LIMIT;
    }
    pageInfo.textContent = `Trang ${historyPage + 1}`;
  } catch (e) {
    grid.innerHTML = '<div class="empty-state">❌ Lỗi tải lịch sử</div>';
  } finally {
    historyLoading = false;
  }
}

function renderHistoryFolders(items, grid, icon, getName, getMeta, onClick, onDelete) {
  grid.innerHTML = ""; grid.classList.remove("grid-mode");
  if (!items.length) { grid.innerHTML = '<div class="empty-state"><div class="empty-icon">📜</div><div>Chưa có lịch sử</div></div>'; return; }
  items.forEach(g => {
    const card = document.createElement("div");
    card.className = "hist-folder";
    const time = g.created_at ? new Date(g.created_at + "Z").toLocaleString("vi-VN") : "";
    const delBtn = onDelete ? `<button class="btn btn-red btn-sm hist-folder-del" onclick="event.stopPropagation();(${onDelete.toString()})('${esc(g.job_id || g.file_name)}')" title="Xóa">🗑</button>` : "";
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
    const tbody = document.getElementById("adminUserBody");
    tbody.innerHTML = users.map(u => {
      const planActive = u.role === "admin" || (u.plan_expires_at && u.plan_expires_at > now);
      const planText = u.role === "admin" ? "♾ Unlimited" : u.plan_expires_at ? new Date(u.plan_expires_at).toLocaleDateString("vi-VN") : "Chưa có";
      const planClass = planActive ? "status-ok" : "status-err";
      return `<tr>
      <td>${u.id}</td><td>${esc(u.username)}</td>
      <td><select onchange="adminChangeRole(${u.id},this.value)" ${u.username === "admin" ? "disabled" : ""}>
        <option value="user" ${u.role === "user" ? "selected" : ""}>User</option>
        <option value="admin" ${u.role === "admin" ? "selected" : ""}>Admin</option>
      </select></td>
      <td><span class="${planClass}">${planText}</span>
        ${u.role !== "admin" ? `<div style="margin-top:4px;display:flex;gap:4px;flex-wrap:wrap">
          <button class="btn btn-ghost" style="padding:2px 6px;font-size:0.68rem" onclick="adminSetPlan(${u.id},7)">+7d</button>
          <button class="btn btn-ghost" style="padding:2px 6px;font-size:0.68rem" onclick="adminSetPlan(${u.id},30)">+30d</button>
          <button class="btn btn-ghost" style="padding:2px 6px;font-size:0.68rem" onclick="adminSetPlan(${u.id},90)">+90d</button>
          <button class="btn btn-ghost" style="padding:2px 6px;font-size:0.68rem;color:var(--error)" onclick="adminSetPlan(${u.id},0)">Hủy</button>
        </div>` : ""}
      </td>
      <td>${u.disabled ? '<span class="status-err">Khóa</span>' : '<span class="status-ok">OK</span>'}</td>
      <td>
        <button class="btn btn-ghost" onclick="adminToggleUser(${u.id},${u.disabled ? 0 : 1})">${u.disabled ? "🔓" : "🔒"}</button>
        ${u.username !== "admin" ? `<button class="btn btn-red btn-sm" onclick="adminDelUser(${u.id})">🗑</button>` : ""}
      </td>
    </tr>`;
    }).join("");
  } catch (e) {}
}

async function adminAddUser() {
  const username = document.getElementById("adminNewUser").value.trim();
  const password = document.getElementById("adminNewPass").value;
  const role = document.getElementById("adminNewRole").value;
  if (!username || !password) { sAlert("Thiếu username/password"); return; }
  const res = await apiFetch("/admin/users", { method: "POST", body: JSON.stringify({ username, password, role }) });
  const data = await res.json();
  if (!res.ok) { sAlert(data.error || "Lỗi"); return; }
  document.getElementById("adminNewUser").value = "";
  document.getElementById("adminNewPass").value = "";
  loadAdminUsers();
}

async function adminChangeRole(id, role) { await apiFetch(`/admin/users/${id}`, { method: "PUT", body: JSON.stringify({ role }) }); loadAdminUsers(); }
async function adminToggleUser(id, disabled) { await apiFetch(`/admin/users/${id}`, { method: "PUT", body: JSON.stringify({ disabled }) }); loadAdminUsers(); }
async function adminDelUser(id) { if (!await sConfirm("Xóa user này?")) return; await apiFetch(`/admin/users/${id}`, { method: "DELETE" }); loadAdminUsers(); }
async function adminSetPlan(id, days) {
  if (days === 0 && !await sConfirm("Hủy gói user này?")) return;
  await apiFetch(`/admin/users/${id}`, { method: "PUT", body: JSON.stringify({ plan_days: days }) });
  loadAdminUsers();
}

async function loadAdminHistory() {
  try {
    const res = await apiFetch("/admin/history?limit=50");
    if (!res.ok) return;
    const items = await res.json();
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
  document.getElementById("progressText").textContent = `${job.completed} / ${job.total} ảnh (${pct}%)`;
  const labels = { pending: "Chờ", running: "Đang chạy", done: "Hoàn thành", error: "Lỗi" };
  const badge = document.getElementById("progressBadge");
  badge.className = `badge badge-${job.status}`; badge.textContent = labels[job.status] || job.status;
  const sb = document.getElementById("statusBadge");
  sb.className = `badge badge-${job.status}`; sb.textContent = labels[job.status] || job.status;
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
  badge.textContent = `${totalImages} ảnh (${allPrompts.length} prompt × ${variants})`;

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
        const refCell = v === 0
          ? (imgs.length
            ? imgs.map((s, ri) => `<span class="ref-wrap"><img src="${s}" class="ref-thumb" onclick="window.open(this.src)"/><span class="ref-del" onclick="removeRefImg(${promptIdx},${ri})">✕</span></span>`).join("") + `<br><span class="ref-add-btn" onclick="importRefForRow(${promptIdx})">+</span>`
            : `<span class="ref-add-btn" onclick="importRefForRow(${promptIdx})">+ ảnh</span>`)
          : `<span style="color:var(--muted);font-size:0.7rem">—</span>`;
        html += `<tr id="resRow${i}">
          <td>${i + 1}</td>
          <td><div class="prompt-cell">${esc(text)}${varLabel}</div></td>
          <td style="text-align:center">${refCell}</td>
          <td class="status-cell">⏳ Chờ</td>
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
      cells[4].innerHTML = `<img src="${img.url}" class="result-thumb" onclick="window.open(this.src)" onerror="this.outerHTML='❌'"/>
        <div class="result-actions"><a href="${img.url}" target="_blank">🔗</a></div>`;
    } else {
      row.className = "row-error";
      cells[3].innerHTML = '<span class="status-err">❌ Lỗi</span>';
      cells[4].innerHTML = `<span style="font-size:0.68rem;color:var(--error)">${esc(img.error || "Lỗi")}</span>`;
    }
  });

  // Mark currently running row
  if (job.status === "running" && completed < total) {
    const runIdx = completed;
    const row = document.getElementById(`resRow${runIdx}`);
    if (row && row.className !== "row-done" && row.className !== "row-error") {
      row.className = "row-running";
      row.cells[3].innerHTML = '<span style="color:#1d4ed8;font-weight:600">🔄 Đang chạy</span>';
      row.scrollIntoView({ behavior: "smooth", block: "center" });
    }
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
      f.status = "⏳ Chờ";
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
function esc(s) { return (s || "").replace(/&/g, "&amp;").replace(/</g, "&lt;").replace(/>/g, "&gt;").replace(/"/g, "&quot;"); }

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
