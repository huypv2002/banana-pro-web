@echo off
:: ── Mở firewall port 8088 ──────────────────────────────────────────────────
netsh advfirewall firewall add rule name="BananaPro API 8088" dir=in action=allow protocol=TCP localport=8088

:: ── Chạy backend ───────────────────────────────────────────────────────────
cd /d C:\BananaPro\banana-pro-web\backend

set ALLOWED_ORIGINS=*
set AUTO_RECAPTCHA=1
set SELENIUM_HEADLESS=1

uvicorn main:app --host 0.0.0.0 --port 8088 --workers 2
