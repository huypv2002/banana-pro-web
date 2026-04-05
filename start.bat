@echo off
echo ========================================
echo   Banana Pro Web - Start Server
echo ========================================

:: Kill old processes on port 8088
for /f "tokens=5" %%a in ('netstat -ano ^| findstr :8088 ^| findstr LISTENING') do taskkill /PID %%a /F >nul 2>&1

:: Set env
set CHROME_PROFILE_PATH=C:\BananaPro\chrome_profile
set AUTO_RECAPTCHA=1
set RECAPTCHA_MODE=selenium
set SELENIUM_HEADLESS=0
set ALLOWED_ORIGINS=*

:: Start uvicorn in background
echo [1/2] Starting FastAPI server on port 8088...
start "BananaPro-Server" cmd /k "cd /d C:\BananaPro\banana-pro-web\backend && set CHROME_PROFILE_PATH=C:\BananaPro\chrome_profile && set AUTO_RECAPTCHA=1 && set SELENIUM_HEADLESS=0 && uvicorn main:app --host 127.0.0.1 --port 8088"

:: Wait for server to start
echo Waiting for server...
timeout /t 5 /nobreak >nul

:: Start cloudflare tunnel
echo [2/2] Starting Cloudflare Tunnel...
echo.
echo >>> Copy the trycloudflare.com URL and send to Kiro to update Worker <<<
echo.
cloudflared tunnel --url http://127.0.0.1:8088
