# Chạy backend trên Windows VPS
# Lưu file này vào: web_app/backend/

# 1. Cài dependencies
pip install -r requirements.txt

# 2. Cài thêm dependencies của complete_flow.py (từ gui_app_clone)
pip install -r ../../gui_app_clone/requirements.txt

# 3. Chạy server (port 8000)
uvicorn main:app --host 0.0.0.0 --port 8088 --workers 2

# ── Biến môi trường (tùy chỉnh) ──────────────────────────────────────────────
# ALLOWED_ORIGINS=https://your-app.pages.dev   ← domain Cloudflare Pages
# AUTO_RECAPTCHA=1
# SELENIUM_HEADLESS=1
# SELENIUM_BROWSER_PATH=C:\Program Files\Google\Chrome\Application\chrome.exe
