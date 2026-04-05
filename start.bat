@echo off
echo ========================================
echo   Banana Pro Web - Start Server
echo ========================================

:: Start uvicorn in new window
echo [1/2] Starting FastAPI server on port 8088...
start "BananaPro-Server" cmd /k "cd /d C:\BananaPro\banana-pro-web\backend && set CHROME_PROFILE_PATH=C:\BananaPro\chrome_profile && set AUTO_RECAPTCHA=1 && set SELENIUM_HEADLESS=0 && uvicorn main:app --host 127.0.0.1 --port 8088"

:: Wait for server to start
echo Waiting for server to start...
timeout /t 6 /nobreak >nul

:: Start cloudflare tunnel (named - stable domain)
echo [2/2] Starting Cloudflare Tunnel (api.sunnshineshop.asia)...
echo.
cloudflared tunnel --config C:\BananaPro\banana-pro-web\tunnel-config.yml run banana-pro
