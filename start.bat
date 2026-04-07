@echo off
echo ========================================
echo   Banana Pro Web - Start Server
echo ========================================

:: Start Profile Manager GUI
echo [0/3] Starting Profile Manager...
start "BananaPro-ProfileManager" cmd /c "cd /d C:\BananaPro\banana-pro-web\backend && set PROFILES_DIR=C:\BananaPro\chrome_profiles && python profile_manager.py"

:: Wait a moment
timeout /t 2 /nobreak >nul

:: Start uvicorn in new window
echo [1/3] Starting FastAPI server on port 8088...
start "BananaPro-Server" cmd /k "cd /d C:\BananaPro\banana-pro-web\backend && set PROFILES_DIR=C:\BananaPro\chrome_profiles && set AUTO_RECAPTCHA=1 && set SELENIUM_HEADLESS=0 && uvicorn main:app --host 127.0.0.1 --port 8088"

:: Wait for server to start
echo Waiting for server to start...
timeout /t 6 /nobreak >nul

:: Start cloudflare tunnel (named - stable domain)
echo [2/3] Starting Cloudflare Tunnel (api.sunnshineshop.asia)...
echo.
cloudflared tunnel --config C:\BananaPro\banana-pro-web\tunnel-config.yml run banana-pro
