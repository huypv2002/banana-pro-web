# 🍌 Hướng dẫn Setup Banana Pro Web App

## Tổng quan kiến trúc

```
[User]  →  [Cloudflare Pages]  →  [VPS Windows :8088]  →  [labs.google API]
              (frontend)              (FastAPI backend)
```

---

## PHẦN 1 – Setup Backend trên VPS Windows

### Bước 1: Cài Python (nếu chưa có)

Tải Python 3.11+ tại https://python.org/downloads  
Khi cài nhớ tick **"Add Python to PATH"**

Kiểm tra:
```bat
python --version
```

---

### Bước 2: Cài Google Chrome (cho reCAPTCHA)

Tải tại https://google.com/chrome  
Cài bình thường, không cần cấu hình thêm.

---

### Bước 3: Copy code lên VPS

Copy toàn bộ thư mục dự án lên VPS, ví dụ vào:
```
C:\BananaPro\
├── gui_app_clone\        ← code gốc (chứa complete_flow.py)
└── web_app\
    ├── backend\
    └── frontend\
```

---

### Bước 4: Cài dependencies

Mở **Command Prompt** hoặc **PowerShell**, chạy:

```bat
cd C:\BananaPro\web_app\backend

pip install -r requirements.txt
pip install -r ..\..\gui_app_clone\requirements.txt
```

---

### Bước 5: Mở port 8088 trong Windows Firewall

Mở **PowerShell với quyền Admin**, chạy:

```powershell
New-NetFirewallRule -DisplayName "BananaPro API" -Direction Inbound -Protocol TCP -LocalPort 8088 -Action Allow
```

---

### Bước 6: Chạy backend server

```bat
cd C:\BananaPro\web_app\backend

set ALLOWED_ORIGINS=https://your-app.pages.dev
set AUTO_RECAPTCHA=1
set SELENIUM_HEADLESS=1

uvicorn main:app --host 0.0.0.0 --port 8088 --workers 2
```

> Thay `https://your-app.pages.dev` bằng domain Cloudflare Pages thật sau khi deploy frontend.

Nếu thành công sẽ thấy:
```
INFO:     Uvicorn running on http://0.0.0.0:8088
```

**Kiểm tra:** Mở trình duyệt trên VPS, vào `http://localhost:8088/health`  
Phải thấy: `{"ok": true}`

---

### Bước 7 (Tùy chọn): Chạy tự động khi khởi động Windows

Tạo file `C:\BananaPro\start_server.bat`:

```bat
@echo off
cd C:\BananaPro\web_app\backend
set ALLOWED_ORIGINS=https://your-app.pages.dev
set AUTO_RECAPTCHA=1
set SELENIUM_HEADLESS=1
uvicorn main:app --host 0.0.0.0 --port 8088 --workers 2
```

Sau đó tạo Task trong **Task Scheduler**:
- Action: chạy `start_server.bat`
- Trigger: At startup
- Run as: Administrator

---

## PHẦN 2 – Deploy Frontend lên Cloudflare Pages

### Bước 1: Sửa API_BASE trong index.html

Mở file `web_app/frontend/index.html`, tìm dòng:

```js
const API_BASE = window.API_BASE || "http://YOUR_VPS_IP:8088";
```

Thay `YOUR_VPS_IP` bằng **IP public của VPS**, ví dụ:

```js
const API_BASE = window.API_BASE || "http://123.456.789.0:8088";
```

> Lấy IP public VPS: vào VPS, mở trình duyệt, tìm "what is my ip"

---

### Bước 2: Sửa _redirects (nếu muốn ẩn IP VPS)

Mở `web_app/frontend/_redirects`, thay `YOUR_VPS_IP`:

```
[[redirects]]
  from = "/api/*"
  to = "http://123.456.789.0:8088/:splat"
  status = 200
  force = true
```

Nếu dùng cách này, sửa `index.html` thành:
```js
const API_BASE = "";
```

---

### Bước 3: Upload lên Cloudflare Pages

1. Vào https://pages.cloudflare.com → **Create a project**
2. Chọn **"Upload assets"** (không cần GitHub)
3. Kéo thả toàn bộ nội dung trong thư mục `frontend/` vào
4. Đặt tên project, ví dụ: `banana-pro`
5. Click **Deploy**

Sau khi deploy xong sẽ có domain dạng: `https://banana-pro.pages.dev`

---

### Bước 4: Cập nhật ALLOWED_ORIGINS trên VPS

Quay lại VPS, dừng server (Ctrl+C), chạy lại với domain thật:

```bat
set ALLOWED_ORIGINS=https://banana-pro.pages.dev
uvicorn main:app --host 0.0.0.0 --port 8088 --workers 2
```

---

## PHẦN 3 – Cách user sử dụng

1. Cài extension **Cookie Editor** trên Chrome:  
   https://chrome.google.com/webstore/detail/cookie-editor/hlkenndednhfkekhgcdicdfddnkalmdm

2. Vào https://labs.google → đăng nhập tài khoản Google

3. Click icon Cookie Editor → **Export** → **Export as Header String** → Copy

4. Mở web app → dán cookie vào ô **Cookie Google Labs**

5. Nhập prompts (mỗi dòng 1 prompt), chọn model, tỉ lệ → **Tạo Ảnh**

---

## Xử lý lỗi thường gặp

| Lỗi | Nguyên nhân | Cách sửa |
|-----|-------------|----------|
| `Failed to fetch` | Frontend không kết nối được backend | Kiểm tra IP VPS, port 8088 đã mở chưa |
| `Cookie không hợp lệ` | Cookie sai format | Export lại bằng Cookie Editor, chọn "Header String" |
| `Không thể lấy access token` | Cookie hết hạn | Đăng nhập lại labs.google, lấy cookie mới |
| `Cannot get reCAPTCHA token` | Chrome chưa cài hoặc bị block | Kiểm tra Chrome đã cài trên VPS |
| `CORS error` | ALLOWED_ORIGINS chưa đúng | Cập nhật domain Cloudflare Pages vào biến môi trường |
