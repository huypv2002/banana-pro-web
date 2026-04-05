const API_BASE = "https://banana-pro-api.kh431248.workers.dev";

// ── State ─────────────────────────────────────────────────────────────────────
let cookies = JSON.parse(localStorage.getItem('bp_cookies') || '[]');
let currentMode = 'normal';
let currentJobId = null;
let pollInterval = null;
let paused = false;

// Prompt sources
let batchFiles = [];        // [{name, prompts}]
let refImages = {};         // {subject:[b64], scene:[b64], style:[b64]}
let refDirImages = [];      // [b64] from ref dir
let folderStructure = {};   // {folderName: {prompts:[], images:[b64]}}

// ── Init ──────────────────────────────────────────────────────────────────────
renderCookieTable();
updateUserInfo();

// ── Cookie Management ─────────────────────────────────────────────────────────
function openCookieModal() { document.getElementById('cookieModal').style.display = 'flex'; }
function closeCookieModal() { document.getElementById('cookieModal').style.display = 'none'; }
function closeCookieModalOutside(e) { if (e.target.id === 'cookieModal') closeCookieModal(); }

function addCookie() {
  const raw = document.getElementById('newCookieInput').value.trim();
  if (!raw) return;
  const parsed = parseCookieInput(raw);
  if (!parsed || !Object.keys(parsed).length) { alert('Cookie không hợp lệ'); return; }
  const hash = cookieHash(parsed);
  if (cookies.find(c => c.hash === hash)) { alert('Cookie này đã tồn tại'); return; }
  cookies.push({ raw, hash, email: '', status: 'pending' });
  saveCookies();
  renderCookieTable();
  document.getElementById('newCookieInput').value = '';
}

async function testCookie() {
  const raw = document.getElementById('newCookieInput').value.trim();
  if (!raw) return;
  const el = document.getElementById('cookieTestResult');
  el.textContent = '⏳ Đang test...';
  try {
    const res = await fetch(`${API_BASE}/test-cookie`, {
      method: 'POST', headers: {'Content-Type':'application/json'},
      body: JSON.stringify({ cookie: raw })
    });
    const data = await res.json();
    if (data.ok) {
      el.innerHTML = `<span class="status-ok">✅ Hợp lệ - ${data.email || ''} (hết hạn: ${data.expires || 'N/A'})</span>`;
    } else {
      el.innerHTML = `<span class="status-err">❌ ${data.error || 'Cookie không hợp lệ'}</span>`;
    }
  } catch(e) {
    el.innerHTML = `<span class="status-err">❌ Lỗi kết nối: ${e.message}</span>`;
  }
}

function deleteCookie(idx) {
  cookies.splice(idx, 1);
  saveCookies();
  renderCookieTable();
}

function clearAllCookies() {
  if (!confirm('Xóa tất cả cookie?')) return;
  cookies = [];
  saveCookies();
  renderCookieTable();
}

function importCookieTxt() { document.getElementById('cookieTxtImport').click(); }
function loadCookieTxt(input) {
  const file = input.files[0];
  if (!file) return;
  const reader = new FileReader();
  reader.onload = e => {
    const lines = e.target.result.split('\n').map(s => s.trim()).filter(Boolean);
    let added = 0;
    lines.forEach(line => {
      const parsed = parseCookieInput(line);
      if (!parsed || !Object.keys(parsed).length) return;
      const hash = cookieHash(parsed);
      if (!cookies.find(c => c.hash === hash)) {
        cookies.push({ raw: line, hash, email: '', status: 'pending' });
        added++;
      }
    });
    saveCookies();
    renderCookieTable();
    alert(`Đã thêm ${added} cookie`);
  };
  reader.readAsText(file);
}

function saveCookies() {
  localStorage.setItem('bp_cookies', JSON.stringify(cookies));
  closeCookieModal();
  updateUserInfo();
}

function renderCookieTable() {
  const tbody = document.getElementById('cookieTableBody');
  if (!tbody) return;
  tbody.innerHTML = cookies.map((c, i) => `
    <tr>
      <td>${i+1}</td>
      <td style="font-family:monospace;font-size:0.75rem">${c.hash.slice(0,12)}...</td>
      <td>${c.email || '-'}</td>
      <td><span class="status-${c.status === 'ok' ? 'ok' : c.status === 'error' ? 'err' : 'pending'}">${c.status === 'ok' ? '✅ OK' : c.status === 'error' ? '❌ Lỗi' : '⏳ Chưa test'}</span></td>
      <td><button class="btn btn-red" style="padding:3px 8px;font-size:0.72rem" onclick="deleteCookie(${i})">Xóa</button></td>
    </tr>`).join('') || '<tr><td colspan="5" style="text-align:center;color:#6b7280;padding:16px">Chưa có cookie nào</td></tr>';
}

function updateUserInfo() {
  const el = document.getElementById('userInfo');
  if (el) el.textContent = cookies.length ? `🍪 ${cookies.length} cookie` : '';
}

// ── Mode ──────────────────────────────────────────────────────────────────────
function setMode(mode) {
  currentMode = mode;
  document.getElementById('multipleCard').style.display = mode === 'multiple' ? 'block' : 'none';
  document.getElementById('folderStructureCard').style.display = mode === 'folder' ? 'block' : 'none';
  document.getElementById('refDirCard').style.display = mode === 'normal' ? 'block' : 'none';
  document.getElementById('promptSourceCard').style.display = mode !== 'folder' ? 'block' : 'none';
}

// ── Prompt Sources ────────────────────────────────────────────────────────────
function loadTxtFile(input) {
  const file = input.files[0];
  if (!file) return;
  document.getElementById('txtFilePath').value = file.name;
  const reader = new FileReader();
  reader.onload = e => {
    const prompts = e.target.result.split('\n').map(s => s.trim()).filter(Boolean);
    // Remove existing entry with same name
    batchFiles = batchFiles.filter(f => f.name !== file.name);
    batchFiles.push({ name: file.name, prompts, status: '⏳ Chờ' });
    renderBatchTable();
  };
  reader.readAsText(file);
}

function loadFolderTxt(input) {
  const files = Array.from(input.files).filter(f => f.name.endsWith('.txt'));
  if (!files.length) return;
  document.getElementById('folderTxtPath').value = input.files[0].webkitRelativePath.split('/')[0];
  let loaded = 0;
  files.forEach(file => {
    const reader = new FileReader();
    reader.onload = e => {
      const prompts = e.target.result.split('\n').map(s => s.trim()).filter(Boolean);
      batchFiles = batchFiles.filter(f => f.name !== file.name);
      batchFiles.push({ name: file.name, prompts, status: '⏳ Chờ' });
      loaded++;
      if (loaded === files.length) renderBatchTable();
    };
    reader.readAsText(file);
  });
}

function clearSources() {
  batchFiles = [];
  document.getElementById('txtFilePath').value = '';
  document.getElementById('folderTxtPath').value = '';
  renderBatchTable();
}

function renderBatchTable() {
  const tbody = document.getElementById('batchTableBody');
  tbody.innerHTML = batchFiles.map((f, i) => `
    <tr onclick="selectBatchRow(${i})" id="batchRow${i}">
      <td>${i+1}</td>
      <td>${esc(f.name)}</td>
      <td>${f.prompts.length}</td>
      <td>${f.status}</td>
    </tr>`).join('') || '<tr><td colspan="4" style="text-align:center;color:#6b7280;padding:12px">Chưa có file nào</td></tr>';
}

function selectBatchRow(i) {
  document.querySelectorAll('.batch-table tr').forEach(r => r.classList.remove('active-row'));
  const row = document.getElementById(`batchRow${i}`);
  if (row) row.classList.add('active-row');
}

// ── Ref Folders (Multiple-to-Image) ──────────────────────────────────────────
function setRefFolder(type, input) {
  const files = Array.from(input.files).filter(f => /\.(jpg|jpeg|png|webp|gif)$/i.test(f.name));
  document.getElementById(`${type}Folder`).value = files.length ? input.files[0].webkitRelativePath.split('/')[0] : '';
  refImages[type] = [];
  files.forEach(file => {
    const reader = new FileReader();
    reader.onload = e => refImages[type].push(e.target.result);
    reader.readAsDataURL(file);
  });
}

// ── Ref Dir ───────────────────────────────────────────────────────────────────
function loadRefDir(input) {
  const files = Array.from(input.files).filter(f => /\.(jpg|jpeg|png|webp|gif)$/i.test(f.name));
  document.getElementById('refDirPath').value = files.length ? input.files[0].webkitRelativePath.split('/')[0] : '';
  refDirImages = [];
  const preview = document.getElementById('refImgPreview');
  preview.innerHTML = '';
  files.slice(0, 10).forEach(file => {
    const reader = new FileReader();
    reader.onload = e => {
      refDirImages.push(e.target.result);
      const img = document.createElement('img');
      img.src = e.target.result;
      preview.appendChild(img);
    };
    reader.readAsDataURL(file);
  });
}

// ── Folder Structure ──────────────────────────────────────────────────────────
function loadParentFolder(input) {
  const files = Array.from(input.files);
  folderStructure = {};
  files.forEach(file => {
    const parts = file.webkitRelativePath.split('/');
    if (parts.length < 2) return;
    const subFolder = parts[1];
    if (!folderStructure[subFolder]) folderStructure[subFolder] = { prompts: [], images: [] };
    if (file.name.endsWith('.txt')) {
      const reader = new FileReader();
      reader.onload = e => {
        folderStructure[subFolder].prompts.push(...e.target.result.split('\n').map(s => s.trim()).filter(Boolean));
        renderFolderStructureTable();
      };
      reader.readAsText(file);
    } else if (/\.(jpg|jpeg|png|webp|gif)$/i.test(file.name)) {
      const reader = new FileReader();
      reader.onload = e => { folderStructure[subFolder].images.push(e.target.result); };
      reader.readAsDataURL(file);
    }
  });
  document.getElementById('parentFolder').value = files[0]?.webkitRelativePath.split('/')[0] || '';
}

function renderFolderStructureTable() {
  const tbody = document.getElementById('folderStructureBody');
  const entries = Object.entries(folderStructure);
  tbody.innerHTML = entries.map(([name, data], i) => `
    <tr><td>${i+1}</td><td>${esc(name)}</td><td>${data.images.length}</td><td>${data.prompts.length}</td></tr>
  `).join('') || '<tr><td colspan="4" style="text-align:center;color:#6b7280;padding:12px">Chưa có folder nào</td></tr>';
}

// ── Generate ──────────────────────────────────────────────────────────────────
async function startGeneration() {
  if (!cookies.length) { showError('Vui lòng thêm cookie trong mục Quản lý Cookie.'); return; }

  let prompts = [];
  let reference_images = [];
  let folder_images = {};

  if (currentMode === 'normal' || currentMode === 'multiple') {
    prompts = batchFiles.flatMap(f => f.prompts);
    if (!prompts.length) { showError('Vui lòng chọn file .txt có prompts.'); return; }
    if (currentMode === 'multiple') {
      reference_images = [...(refImages.subject||[]), ...(refImages.scene||[]), ...(refImages.style||[])];
    } else {
      reference_images = refDirImages;
    }
  } else if (currentMode === 'folder') {
    const entries = Object.entries(folderStructure);
    if (!entries.length) { showError('Vui lòng chọn folder cha.'); return; }
    entries.forEach(([name, data]) => {
      prompts.push(...data.prompts);
      if (data.images.length) folder_images[name.toLowerCase()] = data.images;
    });
    if (!prompts.length) { showError('Không tìm thấy prompt nào.'); return; }
  }

  const model = document.getElementById('modelSelect').value;
  const aspect_ratio = document.getElementById('aspectSelect').value;
  const variants = parseInt(document.getElementById('variantsInput').value) || 1;

  hideError();
  setLoading(true);
  clearResults();
  paused = false;

  try {
    const body = { cookie: cookies[0].raw, prompts, model, aspect_ratio, variants };
    if (reference_images.length) body.reference_images = reference_images;
    if (Object.keys(folder_images).length) body.folder_images = folder_images;

    const res = await fetch(`${API_BASE}/generate`, {
      method: 'POST', headers: {'Content-Type':'application/json'},
      body: JSON.stringify(body)
    });
    if (!res.ok) { const e = await res.json().catch(()=>{}); throw new Error(e?.detail || `HTTP ${res.status}`); }
    const job = await res.json();
    currentJobId = job.job_id;
    updateProgress(job);
    startPolling();
  } catch(e) {
    showError('Lỗi: ' + e.message);
    setLoading(false);
  }
}

function startPolling() {
  pollInterval = setInterval(async () => {
    if (!currentJobId || paused) return;
    try {
      const res = await fetch(`${API_BASE}/jobs/${currentJobId}`);
      if (!res.ok) return;
      const job = await res.json();
      updateProgress(job);
      renderImages(job.images);
      // Update batch table status
      if (job.status === 'running') {
        batchFiles.forEach(f => { if (f.status === '⏳ Chờ') f.status = '🔄 Đang chạy'; });
        renderBatchTable();
      }
      if (job.status === 'done' || job.status === 'error') {
        clearInterval(pollInterval);
        setLoading(false);
        batchFiles.forEach(f => f.status = job.status === 'done' ? '✅ Xong' : '❌ Lỗi');
        renderBatchTable();
        if (job.status === 'error') showError(job.error || 'Có lỗi xảy ra.');
      }
    } catch(e) {}
  }, 2000);
}

function togglePause() {
  paused = !paused;
  document.getElementById('pauseBtn').textContent = paused ? '▶ TIẾP TỤC' : '⏸ TẠM DỪNG';
}

async function stopGeneration() {
  if (!currentJobId) return;
  clearInterval(pollInterval);
  await fetch(`${API_BASE}/jobs/${currentJobId}`, { method: 'DELETE' }).catch(()=>{});
  setLoading(false);
  document.getElementById('progressText').textContent = 'Đã dừng.';
  batchFiles.forEach(f => { if (f.status === '🔄 Đang chạy') f.status = '⏹ Dừng'; });
  renderBatchTable();
}

// ── UI Helpers ────────────────────────────────────────────────────────────────
function setLoading(on) {
  document.getElementById('runBtn').disabled = on;
  document.getElementById('pauseBtn').disabled = !on;
  document.getElementById('stopBtn').disabled = !on;
  document.getElementById('progressCard').style.display = on ? 'block' : 'none';
}

function updateProgress(job) {
  const pct = job.total > 0 ? Math.round((job.completed / job.total) * 100) : 0;
  document.getElementById('progressFill').style.width = pct + '%';
  document.getElementById('progressText').textContent = `${job.completed} / ${job.total} ảnh (${pct}%)`;
  const badge = document.getElementById('progressBadge');
  const labels = { pending:'Chờ', running:'Đang chạy', done:'Hoàn thành', error:'Lỗi' };
  badge.className = `badge badge-${job.status}`;
  badge.textContent = labels[job.status] || job.status;
  const sb = document.getElementById('statusBadge');
  sb.className = `badge badge-${job.status}`;
  sb.textContent = labels[job.status] || job.status;
}

function renderImages(images) {
  if (!images?.length) return;
  const grid = document.getElementById('resultsGrid');
  grid.innerHTML = '';
  images.forEach(img => {
    const card = document.createElement('div');
    card.className = 'img-card';
    if (img.url) {
      card.innerHTML = `
        <img src="${img.url}" alt="${esc(img.prompt)}" loading="lazy" onerror="this.style.display='none'"/>
        <div class="img-card-body"><div class="img-card-prompt">${esc(img.prompt)}</div></div>
        <div class="img-card-actions">
          <a href="${img.url}" download target="_blank">⬇ Tải</a>
          <a href="${img.url}" target="_blank">🔗 Mở</a>
        </div>`;
    } else {
      card.innerHTML = `<div class="img-card-error">❌ ${esc(img.error||'Thất bại')}</div><div class="img-card-body"><div class="img-card-prompt">${esc(img.prompt)}</div></div>`;
    }
    grid.appendChild(card);
  });
}

function clearResults() {
  document.getElementById('resultsGrid').innerHTML = '<div class="empty-state"><div class="empty-icon">🖼️</div><div>Ảnh sẽ hiển thị ở đây sau khi tạo xong</div></div>';
  document.getElementById('statusBadge').textContent = '';
}

function downloadAll() {
  document.querySelectorAll('.img-card a[download]').forEach(a => a.click());
}

function showError(msg) { const el = document.getElementById('errorMsg'); el.textContent = '⚠️ ' + msg; el.style.display = 'block'; }
function hideError() { document.getElementById('errorMsg').style.display = 'none'; }
function esc(s) { return (s||'').replace(/&/g,'&amp;').replace(/</g,'&lt;').replace(/>/g,'&gt;').replace(/"/g,'&quot;'); }

// ── Cookie Utils ──────────────────────────────────────────────────────────────
function parseCookieInput(raw) {
  raw = raw.trim();
  if (raw.startsWith('[')) {
    try { const items = JSON.parse(raw); return Object.fromEntries(items.map(c => [c.name, c.value])); } catch(e) {}
  }
  // name=value; name2=value2
  const obj = {};
  raw.split(';').forEach(part => {
    const idx = part.indexOf('=');
    if (idx > 0) obj[part.slice(0,idx).trim()] = part.slice(idx+1).trim();
  });
  return obj;
}

function cookieHash(obj) {
  const str = JSON.stringify(obj);
  let h = 0;
  for (let i = 0; i < str.length; i++) h = (Math.imul(31, h) + str.charCodeAt(i)) | 0;
  return Math.abs(h).toString(16).padStart(8,'0');
}
