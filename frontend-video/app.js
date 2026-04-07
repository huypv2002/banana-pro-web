const API_BASE = "https://banana-pro-api.kh431248.workers.dev";

const Toast = Swal.mixin({ toast: true, position: "top-end", showConfirmButton: false, timer: 2500, timerProgressBar: true });
function sAlert(text, icon = "info") { return Swal.fire({ text, icon, confirmButtonColor: "#16a34a" }); }
function sSuccess(text) { Toast.fire({ icon: "success", title: text }); }
async function sConfirm(text, title = "Xác nhận") { const r = await Swal.fire({ title, text, icon: "warning", showCancelButton: true, confirmButtonColor: "#16a34a", cancelButtonColor: "#6b7280", confirmButtonText: "Đồng ý", cancelButtonText: "Hủy" }); return r.isConfirmed; }

let authToken = localStorage.getItem("bp_token") || "";
let authUser = JSON.parse(localStorage.getItem("bp_user") || "null");
let cookies = [];
let batchFiles = [];
let currentJobId = null;
let pollInterval = null;
let pollFailCount = 0;
let currentMode = "t2v";
let rowRefImages = {};  // {idx: base64} — start/ref image
let endRowImages = {};  // {idx: base64} — end image (FL mode only)
let refImportTargetRow = -1;
let endImportTargetRow = -1;

const MODE_CONFIG = {
  t2v: {
    desc: "📝 Text → Video: Tạo video từ prompt văn bản",
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
    desc: "🖼 Image → Video: Ảnh làm frame đầu, video chuyển động từ đó",
    refLabel: "🖼 Start",
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
    desc: "🎞 First+Last → Video: Ảnh đầu & cuối, AI tạo chuyển động giữa",
    refLabel: "🎬 Start",
    endLabel: "🏁 End",
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
    desc: "🎨 Reference → Video: Ảnh tham chiếu phong cách/nhân vật cho video",
    refLabel: "🎨 Ref",
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

function apiFetch(path, opts = {}) {
  const headers = { "Content-Type": "application/json", ...(opts.headers || {}) };
  if (authToken) headers["Authorization"] = `Bearer ${authToken}`;
  return fetch(API_BASE + path, { ...opts, headers });
}

// ── Auth ──
(async function init() {
  setMode("t2v"); // populate model select on load
  if (authToken && authUser) {
    try { const res = await apiFetch("/auth/me"); if (res.ok) { authUser = await res.json(); localStorage.setItem("bp_user", JSON.stringify(authUser)); showApp(); return; } } catch (e) {}
    clearAuth();
  }
  showLogin();
})();

function showLogin() { document.getElementById("loginScreen").style.display = "flex"; document.getElementById("appScreen").style.display = "none"; }
function showApp() {
  document.getElementById("loginScreen").style.display = "none";
  document.getElementById("appScreen").style.display = "block";
  document.getElementById("userInfo").textContent = `👤 ${authUser.username} (${authUser.role})`;
  const planEl = document.getElementById("planInfo");
  if (authUser.role === "admin") { planEl.textContent = "♾ Unlimited"; planEl.className = "plan-badge plan-active"; }
  else if (authUser.plan_active) { const days = Math.ceil((new Date(authUser.plan_expires_at) - new Date()) / 86400000); planEl.textContent = `📦 Còn ${days} ngày`; planEl.className = "plan-badge " + (days <= 3 ? "plan-expiring" : "plan-active"); }
  else { planEl.textContent = "⛔ Hết hạn"; planEl.className = "plan-badge plan-expired"; }
  loadCookiesFromDB();
}
function clearAuth() { authToken = ""; authUser = null; localStorage.removeItem("bp_token"); localStorage.removeItem("bp_user"); }
function logout() { clearAuth(); showLogin(); }
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
    localStorage.setItem("bp_token", authToken); localStorage.setItem("bp_user", JSON.stringify(authUser));
    try { const me = await apiFetch("/auth/me"); if (me.ok) { authUser = await me.json(); localStorage.setItem("bp_user", JSON.stringify(authUser)); } } catch(_){}
    showApp();
  } catch (e) { errEl.textContent = "Lỗi kết nối"; errEl.style.display = "block"; }
  return false;
}

// ── Video Mode ──
function setMode(mode) {
  currentMode = mode;
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
  document.getElementById("colRefHead").style.display = hasRef ? "" : "none";
  document.getElementById("colRefHead").style.width = hasEnd ? "25%" : "50%";
  document.getElementById("colRefHead").textContent = cfg.refLabel || "Ảnh Ref";
  document.getElementById("colEndHead").style.display = hasEnd ? "" : "none";
  document.getElementById("colEndHead").style.width = "25%";
  document.getElementById("colEndHead").textContent = cfg.endLabel || "Ảnh End";
  document.getElementById("btnImportRef").style.display = hasRef ? "" : "none";
  document.getElementById("btnImportRef").textContent = `📷 Import ${cfg.refLabel || "Ref"} tất cả`;
  document.getElementById("btnImportEnd").style.display = hasEnd ? "" : "none";
  if (!currentJobId) populateResultsTable();
}

function updateModelDesc() {
  const sel = document.getElementById("modelSelect");
  const opt = sel.options[sel.selectedIndex];
  if (opt) document.getElementById("modelDesc").textContent = `Model: ${opt.text}`;
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
  const total = batchFiles.flatMap(f => f.prompts).length;
  let loaded = 0; const imgs = [];
  files.forEach(file => { const r = new FileReader(); r.onload = e => { imgs.push(e.target.result); if (++loaded === files.length) { for (let i = 0; i < total; i++) endRowImages[i] = imgs.length === 1 ? imgs[0] : (imgs[i] || imgs[imgs.length - 1]); refreshRefCells(); } }; r.readAsDataURL(file); });
  input.value = "";
}
function handleEndRowImport(input) {
  const file = input.files[0]; if (!file) return;
  const r = new FileReader(); r.onload = e => { endRowImages[endImportTargetRow] = e.target.result; refreshRefCells(); }; r.readAsDataURL(file);
  input.value = "";
}

function refreshRefCells() {
  const cfg = MODE_CONFIG[currentMode];
  batchFiles.flatMap(f => f.prompts).forEach((_, i) => {
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
        row.cells[2].innerHTML = `<span class="ref-add-btn" onclick="importRefForRow(${i})">+ ${cfg.refLabel}</span>`;
      }
    }
    if (cfg.endLabel) {
      const img = endRowImages[i];
      row.cells[3].innerHTML = img
        ? `<div class="ref-cell"><span class="ref-wrap"><img src="${img}" class="ref-thumb" onclick="window.open(this.src)"/><span class="ref-del" onclick="removeEndImg(${i})">✕</span></span></div>`
        : `<span class="ref-add-btn" onclick="importEndForRow(${i})">+ ${cfg.endLabel}</span>`;
    }
  });
}

// ── Cookies ──
async function loadCookiesFromDB() { try { const res = await apiFetch("/user/cookies"); if (res.ok) cookies = await res.json(); renderCookieTable(); } catch (e) {} }
function renderCookieTable() {
  const tbody = document.getElementById("cookieTableBody");
  tbody.innerHTML = cookies.map((c, i) => `<tr><td>${i + 1}</td><td style="font-family:monospace;font-size:0.75rem">${esc((c.cookie_hash||"").slice(0,12))}...</td><td>${esc(c.email || "—")}</td><td>${c.status}</td><td><button class="btn btn-red btn-sm" onclick="deleteCookie(${c.id})">Xóa</button></td></tr>`).join("") || '<tr><td colspan="5" style="text-align:center;padding:12px">Chưa có cookie</td></tr>';
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
  document.getElementById("txtFilePath").value = file.name;
  document.getElementById("folderTxtPath").value = ""; batchFiles = [];
  const reader = new FileReader();
  reader.onload = e => { batchFiles.push({ name: file.name, prompts: e.target.result.split("\n").map(s => s.trim()).filter(Boolean), status: "⏳ Chờ" }); renderBatchTable(); };
  reader.readAsText(file);
}
function loadFolderTxt(input) {
  const files = Array.from(input.files).filter(f => f.name.endsWith(".txt")); if (!files.length) return;
  document.getElementById("folderTxtPath").value = input.files[0].webkitRelativePath.split("/")[0];
  document.getElementById("txtFilePath").value = ""; batchFiles = [];
  let loaded = 0;
  files.forEach(file => { const r = new FileReader(); r.onload = e => { batchFiles.push({ name: file.name, prompts: e.target.result.split("\n").map(s => s.trim()).filter(Boolean), status: "⏳ Chờ" }); if (++loaded === files.length) renderBatchTable(); }; r.readAsText(file); });
}
function clearSources() { batchFiles = []; rowRefImages = {}; document.getElementById("txtFilePath").value = ""; document.getElementById("folderTxtPath").value = ""; renderBatchTable(); }
function renderBatchTable() {
  document.getElementById("batchTableBody").innerHTML = batchFiles.map((f, i) => `<tr><td>${i + 1}</td><td>${esc(f.name)}</td><td>${f.prompts.length}</td><td>${f.status}</td></tr>`).join("") || '<tr><td colspan="4" style="text-align:center;color:#6b7280;padding:12px">Chưa có file</td></tr>';
  if (!currentJobId) populateResultsTable();
}

// ── Ref Images (per-row + bulk) ──
function importRefAll() { refImportTargetRow = -1; document.getElementById("refBulkInput").click(); }

function handleRefBulkImport(input) {
  const files = Array.from(input.files); if (!files.length) return;
  const total = batchFiles.flatMap(f => f.prompts).length;
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

function clearRefAll() { rowRefImages = {}; endRowImages = {}; refreshRefCells(); }
function removeRefImg(idx, imgIdx = null) {
  if (imgIdx !== null && Array.isArray(rowRefImages[idx])) {
    rowRefImages[idx].splice(imgIdx, 1);
    if (!rowRefImages[idx].length) delete rowRefImages[idx];
  } else { delete rowRefImages[idx]; }
  refreshRefCells();
}
function removeEndImg(idx) { delete endRowImages[idx]; refreshRefCells(); }

// ── Generate ──
async function startGeneration() {
  if (!cookies.length) { sAlert("Vui lòng thêm cookie."); return; }
  const prompts = batchFiles.flatMap(f => f.prompts);
  if (!prompts.length) { sAlert("Chọn file .txt có prompts."); return; }

  const model = document.getElementById("modelSelect").value;
  const num_videos = parseInt(document.getElementById("numVideosInput").value) || 1;

  // Validate ảnh theo mode
  if (currentMode === "i2v" || currentMode === "r2v") {
    const missing = prompts.findIndex((_, i) => !rowRefImages[i]);
    if (missing >= 0) { sAlert(`Prompt #${missing + 1} chưa có ảnh. Dùng "Import tất cả" để gán nhanh.`); return; }
  }
  if (currentMode === "fl") {
    const missingStart = prompts.findIndex((_, i) => !rowRefImages[i]);
    const missingEnd   = prompts.findIndex((_, i) => !endRowImages[i]);
    if (missingStart >= 0) { sAlert(`Prompt #${missingStart + 1} chưa có ảnh Start.`); return; }
    if (missingEnd >= 0)   { sAlert(`Prompt #${missingEnd + 1} chưa có ảnh End.`); return; }
  }

  document.getElementById("progressCard").style.display = "block";
  document.getElementById("runBtn").disabled = true;
  document.getElementById("stopBtn").disabled = false;
  populateResultsTable();

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
    const body = { mode: currentMode, model, num_videos, prompts, ref_images: refMap, end_images: endMap };
    const res = await apiFetch("/generate-video", { method: "POST", body: JSON.stringify(body) });
    if (!res.ok) { const e = await res.json().catch(() => ({})); throw new Error(e?.detail || e?.error || `HTTP ${res.status}`); }
    const job = await res.json();
    currentJobId = job.job_id;
    startPolling();
  } catch (e) {
    sAlert("Lỗi: " + e.message, "error");
    document.getElementById("runBtn").disabled = false;
    document.getElementById("stopBtn").disabled = true;
    document.getElementById("progressCard").style.display = "none";
  }
}

function startPolling() {
  pollFailCount = 0;
  pollInterval = setInterval(async () => {
    if (!currentJobId) return;
    try {
      const res = await apiFetch(`/video-jobs/${currentJobId}`);
      if (!res.ok) { if (++pollFailCount >= 5) { clearInterval(pollInterval); currentJobId = null; sAlert("Mất kết nối server", "error"); resetUI(); } return; }
      pollFailCount = 0;
      const job = await res.json();
      updateProgress(job);
      updateResults(job);
      if (job.status === "done" || job.status === "error") {
        clearInterval(pollInterval); currentJobId = null; resetUI();
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
  document.getElementById("runBtn").disabled = false;
  document.getElementById("stopBtn").disabled = true;
  document.getElementById("progressCard").style.display = "none";
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
  const allPrompts = batchFiles.flatMap(f => f.prompts);
  const tbody = document.getElementById("resultsBody");
  const empty = document.getElementById("resultsEmpty");
  const badge = document.getElementById("promptCountBadge");
  const cfg = MODE_CONFIG[currentMode];
  const hasRef = !!cfg.refLabel;
  const hasEnd = !!cfg.endLabel;

  if (!allPrompts.length) { tbody.innerHTML = ""; empty.style.display = ""; badge.style.display = "none"; return; }
  empty.style.display = "none"; badge.style.display = ""; badge.textContent = `${allPrompts.length} prompt`;

  let html = "", idx = 0;
  batchFiles.forEach(f => {
    const strip = n => n.replace(/\.txt$/i, "");
    const cols = 3 + (hasRef ? 1 : 0) + (hasEnd ? 1 : 0);
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
            return `<span class="ref-add-btn" onclick="importRefForRow(${idx})">+ ${cfg.refLabel}</span>`;
          })()
        : "";
      const endCell = hasEnd
        ? (endRowImages[idx]
            ? `<span class="ref-wrap"><img src="${endRowImages[idx]}" class="ref-thumb" onclick="window.open(this.src)"/><span class="ref-del" onclick="removeEndImg(${idx})">✕</span></span>`
            : `<span class="ref-add-btn" onclick="importEndForRow(${idx})">+ ${cfg.endLabel}</span>`)
        : "";
      html += `<tr id="resRow${idx}">
        <td>${idx + 1}</td>
        <td><div class="prompt-cell">${esc(text)}</div></td>
        ${hasRef ? `<td style="text-align:center">${refCell}</td>` : ""}
        ${hasEnd ? `<td style="text-align:center">${endCell}</td>` : ""}
        <td class="status-cell">⏳ Chờ</td>
        <td>—</td>
      </tr>`;
      idx++;
    });
  });
  tbody.innerHTML = html;
}

function updateResults(job) {
  const videos = job.videos || [];
  videos.forEach((v, idx) => {
    const row = document.getElementById(`resRow${idx}`); if (!row) return;
    if (v.urls && v.urls.length) {
      row.className = "row-done";
      row.cells[3].innerHTML = `<span class="status-ok">✅ Xong</span><br><span style="font-size:0.68rem;color:var(--muted)">${v.mode === 'i2v' ? '🖼 I2V' : '📝 T2V'}</span>`;
      row.cells[4].innerHTML = v.urls.map((u, i) => `<a href="${u}" target="_blank" class="btn btn-green btn-sm" style="margin:2px">▶ Video ${i + 1}</a>`).join("");
    } else if (v.error) {
      row.className = "row-error";
      row.cells[3].innerHTML = '<span class="status-err">❌ Lỗi</span>';
      row.cells[4].innerHTML = `<span style="font-size:0.7rem;color:var(--error)">${esc(v.error)}</span>`;
    }
  });
  // Mark running
  if (job.status === "running" && videos.length < job.total) {
    const row = document.getElementById(`resRow${videos.length}`);
    if (row && row.className !== "row-done" && row.className !== "row-error") {
      row.className = "row-running";
      row.cells[3].innerHTML = '<span style="color:#1d4ed8;font-weight:600">🔄 Đang tạo...</span>';
      row.scrollIntoView({ behavior: "smooth", block: "center" });
    }
  }
  // Update batch table status
  let offset = 0;
  batchFiles.forEach(f => {
    const end = offset + f.prompts.length;
    const done = videos.slice(offset, end).filter(v => v).length;
    if (done >= f.prompts.length) f.status = videos.slice(offset, end).every(v => v && v.urls?.length) ? "✅ Xong" : "⚠️ Có lỗi";
    else if (done > 0) f.status = `🔄 ${done}/${f.prompts.length}`;
    offset = end;
  });
  document.getElementById("batchTableBody").innerHTML = batchFiles.map((f, i) => `<tr><td>${i + 1}</td><td>${esc(f.name)}</td><td>${f.prompts.length}</td><td>${f.status}</td></tr>`).join("");
}
