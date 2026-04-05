"""
Chạy file này trên VPS để mở Chrome với profile riêng.
Đăng nhập labs.google xong, nhấn Enter để lưu và thoát.
"""
import os
import subprocess
import sys

PROFILE_DIR = r"C:\BananaPro\chrome_profile"
os.makedirs(PROFILE_DIR, exist_ok=True)

# Tìm Chrome
chrome_paths = [
    r"C:\Program Files\Google\Chrome\Application\chrome.exe",
    r"C:\Program Files (x86)\Google\Chrome\Application\chrome.exe",
    os.path.expandvars(r"%LOCALAPPDATA%\Google\Chrome\Application\chrome.exe"),
]
chrome = next((p for p in chrome_paths if os.path.exists(p)), None)
if not chrome:
    print("❌ Không tìm thấy Chrome. Hãy cài Chrome trước.")
    input("Nhấn Enter để thoát...")
    sys.exit(1)

print(f"✅ Tìm thấy Chrome: {chrome}")
print(f"📂 Profile sẽ lưu tại: {PROFILE_DIR}")
print()
print("🌐 Đang mở Chrome... Hãy đăng nhập vào https://labs.google")
print("   Sau khi đăng nhập xong, quay lại đây và nhấn Enter.")
print()

proc = subprocess.Popen([
    chrome,
    f"--user-data-dir={PROFILE_DIR}",
    "--no-first-run",
    "--no-default-browser-check",
    "https://labs.google/fx/tools/flow",
])

input("✅ Đã đăng nhập xong? Nhấn Enter để lưu profile và thoát Chrome...")

proc.terminate()
try:
    proc.wait(timeout=5)
except Exception:
    proc.kill()

print()
print(f"✅ Profile đã lưu tại: {PROFILE_DIR}")
print()
print("Bây giờ chạy server với lệnh:")
print(f'  set CHROME_PROFILE_PATH={PROFILE_DIR}')
print(f'  set AUTO_RECAPTCHA=1')
print(f'  uvicorn main:app --host 0.0.0.0 --port 8088 --workers 1')
input("\nNhấn Enter để thoát...")
