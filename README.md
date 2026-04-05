# 🍌 Banana Pro Web App

## Kiến trúc

```
[User Browser]
     │  nhập cookie + prompt
     ▼
[Cloudflare Pages]  ← frontend/index.html
     │  POST /generate
     ▼
[VPS Windows :8000]  ← backend/main.py (FastAPI)
     │  LabsFlowClient + reCAPTCHA (Chrome headless)
     ▼
[labs.google API]
```

---

## 1. Deploy Backend (VPS Windows)

```bat
cd web_app\backend

pip install -r requirements.txt
pip install -r ..\..\gui_app_clone\requirements.txt

:: Chạy server
set ALLOWED_ORIGINS=https://your-app.pages.dev
set AUTO_RECAPTCHA=1
set SELENIUM_HEADLESS=1
uvicorn main:app --host 0.0.0.0 --port 8088 --workers 2
```

> **Lưu ý**: Mở port 8000 trong Windows Firewall.

---

## 2. Deploy Frontend (Cloudflare Pages)

1. Vào [Cloudflare Pages](https://pages.cloudflare.com)
2. **Create project** → Upload `frontend/` folder
3. Sau khi deploy xong, lấy domain (vd: `https://banana-pro.pages.dev`)
4. Sửa `index.html` dòng:
   ```js
   const API_BASE = "http://YOUR_VPS_IP:8088";
   ```
   Thay `YOUR_VPS_IP` bằng IP thật của VPS.

5. Hoặc dùng `_redirects` để proxy qua Cloudflare (ẩn IP VPS):
   - Sửa `_redirects`: thay `YOUR_VPS_IP` bằng IP VPS
   - Sửa `index.html`: `const API_BASE = ""` (để trống = dùng relative path)

---

## 3. Cách user dùng

1. Cài [Cookie Editor](https://chrome.google.com/webstore/detail/cookie-editor/hlkenndednhfkekhgcdicdfddnkalmdm)
2. Vào `labs.google` → đăng nhập
3. Mở Cookie Editor → **Export** → Copy
4. Dán vào ô Cookie trên web
5. Nhập prompts → Chọn model → **Tạo Ảnh**

---

## Bảo mật (khuyến nghị)

- Thêm API key đơn giản vào backend để tránh abuse:
  ```python
  # main.py - thêm header check
  from fastapi import Header
  API_KEY = os.environ.get("API_KEY", "")
  async def verify_key(x_api_key: str = Header("")):
      if API_KEY and x_api_key != API_KEY:
          raise HTTPException(401, "Unauthorized")
  ```
- Dùng HTTPS cho VPS (Nginx + Let's Encrypt hoặc Cloudflare Tunnel)
