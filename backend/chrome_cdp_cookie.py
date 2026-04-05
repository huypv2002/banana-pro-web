"""
Chrome CDP Cookie Module - Lấy cookie từ Chrome thật qua Chrome DevTools Protocol.

Tham khảo:
- https://github.com/Moustachauve/cookie-editor (Cookie Editor extension format)
- https://github.com/Christian93111/cookie-stealer (CDP cookie extraction via websocket)

Flow:
1. Launch Chrome thật với --remote-debugging-port
2. Kết nối qua CDP WebSocket
3. Navigate tới labs.google
4. Login nếu cần (visible mode)
5. Lấy cookies qua Network.getAllCookies
6. Filter 3 cookie cần thiết, output format giống Cookie Editor extension
"""

import asyncio
import json
import os
import platform
import shutil
import signal
import subprocess
import time
import threading
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple

try:
    import requests
except ImportError:
    requests = None

try:
    import websocket as ws_module
except ImportError:
    ws_module = None


def _get_ws_module():
    """Lazy import websocket module."""
    global ws_module
    if ws_module is None:
        try:
            import websocket as _ws
            ws_module = _ws
        except ImportError:
            raise ImportError(
                "websocket-client chưa được cài đặt. Chạy: pip install websocket-client"
            )
    return ws_module


# ═══════════════════════════════════════════════════════════════
# Constants
# ═══════════════════════════════════════════════════════════════

REQUIRED_COOKIE_NAMES = {
    "__Host-next-auth.csrf-token",
    "__Secure-next-auth.callback-url",
    "__Secure-next-auth.session-token",
}

LABS_URL = "https://labs.google/fx/tools/image-to-video"
LABS_DOMAIN = "labs.google"

# Default profiles directory
PROFILES_DIR = Path(os.path.dirname(os.path.abspath(__file__))) / "chrome_profiles"


# ═══════════════════════════════════════════════════════════════
# Chrome Binary Discovery
# ═══════════════════════════════════════════════════════════════

def find_chrome_binary() -> Optional[str]:
    """Tìm Chrome binary trên hệ thống."""
    system = platform.system()
    candidates = []

    if system == "Darwin":
        candidates = [
            "/Applications/Google Chrome.app/Contents/MacOS/Google Chrome",
            "/Applications/Chromium.app/Contents/MacOS/Chromium",
            os.path.expanduser("~/Applications/Google Chrome.app/Contents/MacOS/Google Chrome"),
        ]
    elif system == "Windows":
        candidates = [
            os.path.expandvars(r"%ProgramFiles%\Google\Chrome\Application\chrome.exe"),
            os.path.expandvars(r"%ProgramFiles(x86)%\Google\Chrome\Application\chrome.exe"),
            os.path.expandvars(r"%LocalAppData%\Google\Chrome\Application\chrome.exe"),
        ]
    else:
        candidates = [
            "/usr/bin/google-chrome",
            "/usr/bin/google-chrome-stable",
            "/usr/bin/chromium-browser",
            "/usr/bin/chromium",
        ]

    for path in candidates:
        if os.path.isfile(path):
            return path

    for name in ["google-chrome", "google-chrome-stable", "chromium-browser", "chromium", "chrome"]:
        found = shutil.which(name)
        if found:
            return found

    return None


# ═══════════════════════════════════════════════════════════════
# Profile Utilities
# ═══════════════════════════════════════════════════════════════

def get_profile_dir(email: str, base_dir: Optional[Path] = None) -> Path:
    """Tạo profile directory path cho email."""
    base = base_dir or PROFILES_DIR
    safe_email = email.replace("@", "_at_").replace(".", "_")
    return base / safe_email


def check_profile_has_cookies(profile_path: str) -> bool:
    """Check xem profile đã có cookies chưa."""
    try:
        profile = Path(profile_path)
        local_state = profile / "Local State"
        if not local_state.exists():
            return False
        default_folder = profile / "Default"
        if not default_folder.exists():
            return False
        for cookies_path in [
            default_folder / "Network" / "Cookies",
            default_folder / "Cookies",
        ]:
            if cookies_path.exists() and cookies_path.stat().st_size > 1000:
                return True
        return False
    except Exception:
        return False


def kill_chrome_for_profile(profile_path: str):
    """Kill Chrome processes đang dùng profile này và xóa lock files."""
    # Xóa lock files
    for lock_name in ["SingletonLock", "SingletonSocket", "SingletonCookie"]:
        lock_file = Path(profile_path) / lock_name
        try:
            if lock_file.exists():
                lock_file.unlink()
        except Exception:
            pass

    # Kill processes
    system = platform.system()
    profile_name = Path(profile_path).name
    try:
        if system == "Windows":
            # Dùng wmic để tìm Chrome process theo command line chứa profile path
            try:
                result = subprocess.run(
                    ['wmic', 'process', 'where', "name='chrome.exe'", 'get', 'processid,commandline'],
                    capture_output=True, text=True, timeout=10,
                    creationflags=0x08000000,  # CREATE_NO_WINDOW
                )
                for line in result.stdout.split('\n'):
                    # Match cả full path lẫn tên folder (tăng khả năng kill đúng)
                    if profile_name in line or profile_path in line:
                        parts = line.strip().split()
                        if parts:
                            try:
                                pid = int(parts[-1])
                                subprocess.run(
                                    ['taskkill', '/F', '/PID', str(pid)],
                                    capture_output=True, timeout=5,
                                    creationflags=0x08000000,
                                )
                            except (ValueError, Exception):
                                pass
            except Exception:
                # Fallback: taskkill theo window title (ít tin cậy hơn)
                subprocess.run(
                    ["taskkill", "/F", "/FI", f"WINDOWTITLE eq *{profile_name}*"],
                    stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL, timeout=5,
                    creationflags=0x08000000,
                )
        elif system == "Darwin":
            result = subprocess.run(
                ["pgrep", "-f", f"--user-data-dir={profile_path}"],
                capture_output=True, text=True, timeout=5,
            )
            for pid_str in result.stdout.strip().split("\n"):
                pid_str = pid_str.strip()
                if pid_str.isdigit():
                    try:
                        os.kill(int(pid_str), signal.SIGTERM)
                    except ProcessLookupError:
                        pass
    except Exception:
        pass


# ═══════════════════════════════════════════════════════════════
# Chrome CDP Session
# ═══════════════════════════════════════════════════════════════

class ChromeCDPSession:
    """Quản lý 1 Chrome instance qua CDP cho 1 account.
    
    Dùng Chrome thật + remote-debugging-port, kết nối qua websocket
    để điều khiển browser và lấy cookies.
    """

    def __init__(
        self,
        profile_path: str,
        port: int = 0,
        headless: bool = True,
        chrome_path: Optional[str] = None,
        window_pos: Tuple[int, int] = (0, 0),
        window_size: Tuple[int, int] = (500, 600),
        log_fn=None,
        proxy_server: Optional[str] = None,
        proxy_username: Optional[str] = None,
        proxy_password: Optional[str] = None,
    ):
        self.profile_path = profile_path
        self.port = port or self._find_free_port()
        self.headless = headless
        self.chrome_path = chrome_path or find_chrome_binary()
        self.window_pos = window_pos
        self.window_size = window_size
        self.log_fn = log_fn or print
        self.proxy_server = proxy_server  # e.g. "http://host:port" hoặc "socks5://host:port"
        self.proxy_username = proxy_username or ""
        self.proxy_password = proxy_password or ""
        self.process: Optional[subprocess.Popen] = None
        self._ws = None
        self._msg_id = 0
        self._proxy_auth_listener_active = False

    @staticmethod
    def _find_free_port() -> int:
        import socket
        with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
            s.bind(("127.0.0.1", 0))
            return s.getsockname()[1]

    # ─── Launch / Close ───────────────────────────────────────

    def launch(self):
        """Launch Chrome với remote debugging."""
        if not self.chrome_path:
            raise RuntimeError("Không tìm thấy Chrome binary!")

        Path(self.profile_path).mkdir(parents=True, exist_ok=True)

        cmd = [
            self.chrome_path,
            f"--remote-debugging-port={self.port}",
            f"--user-data-dir={self.profile_path}",
            "--remote-allow-origins=*",
            "--no-first-run",
            "--no-default-browser-check",
            "--disable-extensions",
            "--disable-infobars",
            "--disable-sync",
            "--disable-signin-promo",
            "--disable-features=Translate,OptimizationGuideModelDownloading",
            "--password-store=basic",
            "--use-mock-keychain",
            "--hide-crash-restore-bubble",
            "--restore-last-session",
            f"--window-size={self.window_size[0]},{self.window_size[1]}",
            f"--window-position={self.window_pos[0]},{self.window_pos[1]}",
        ]

        if self.headless:
            cmd.extend(["--headless=new", "--disable-gpu", "--no-sandbox"])

        # Proxy support: --proxy-server cho Chrome
        if self.proxy_server:
            # Chrome chỉ nhận server (không auth trong URL), auth qua CDP Fetch
            proxy_arg = self.proxy_server
            # Strip auth từ URL nếu có (Chrome --proxy-server không hỗ trợ auth trong URL)
            if "@" in proxy_arg:
                try:
                    from urllib.parse import urlparse
                    parsed = urlparse(proxy_arg)
                    if parsed.username and not self.proxy_username:
                        self.proxy_username = parsed.username
                    if parsed.password and not self.proxy_password:
                        self.proxy_password = parsed.password
                    # Rebuild URL without auth
                    proxy_arg = f"{parsed.scheme}://{parsed.hostname}:{parsed.port}" if parsed.port else f"{parsed.scheme}://{parsed.hostname}"
                except Exception:
                    pass
            cmd.append(f"--proxy-server={proxy_arg}")
            self.log_fn(f"   🌐 Chrome proxy: {proxy_arg}")

        self.log_fn(f"   🚀 Launch Chrome CDP port={self.port} headless={self.headless}")
        self.process = subprocess.Popen(
            cmd,
            stdout=subprocess.DEVNULL,
            stderr=subprocess.DEVNULL,
        )

        # Đợi Chrome sẵn sàng
        if not self._wait_for_cdp_ready(timeout=15):
            raise RuntimeError(f"Chrome không khởi động được trên port {self.port}")

        # Setup proxy auth qua CDP Fetch nếu proxy cần authentication
        if self.proxy_server and self.proxy_username and self.proxy_password:
            self._setup_proxy_auth()

    def _setup_proxy_auth(self):
        """Setup proxy authentication qua CDP Fetch domain.
        
        Chrome 146+ hỗ trợ Fetch.enable để intercept proxy auth challenges.
        Khi proxy yêu cầu auth (407), CDP sẽ gửi Fetch.authRequired event,
        ta respond bằng Fetch.continueWithAuth với credentials.
        
        Approach: Dùng dedicated WebSocket connection cho proxy auth listener
        để không conflict với _send_cdp trên main WebSocket.
        """
        try:
            # Dùng separate WebSocket cho auth listener
            ws_url = self._get_ws_url()
            if not ws_url:
                self.log_fn(f"   ⚠️ Không lấy được WS URL cho proxy auth")
                return
            
            self._proxy_auth_ws = _get_ws_module().create_connection(ws_url, timeout=10)
            
            # Enable Fetch domain trên auth WS
            auth_msg_id = 99990
            enable_msg = {
                "id": auth_msg_id,
                "method": "Fetch.enable",
                "params": {"handleAuthRequests": True}
            }
            self._proxy_auth_ws.send(json.dumps(enable_msg))
            
            # Wait for response
            deadline = time.time() + 5
            while time.time() < deadline:
                try:
                    self._proxy_auth_ws.settimeout(1.0)
                    raw = self._proxy_auth_ws.recv()
                    data = json.loads(raw)
                    if data.get("id") == auth_msg_id:
                        break
                except Exception:
                    continue
            
            self._proxy_auth_listener_active = True
            self.log_fn(f"   🔐 Proxy auth via CDP Fetch enabled (user: {self.proxy_username[:20]}...)")
            
            # Start background thread để listen cho auth events
            import threading
            self._proxy_auth_thread = threading.Thread(
                target=self._proxy_auth_listener,
                daemon=True,
                name="proxy-auth-listener"
            )
            self._proxy_auth_thread.start()
        except Exception as e:
            self.log_fn(f"   ⚠️ Không thể setup proxy auth via CDP: {e}")

    def _proxy_auth_listener(self):
        """Background listener cho Fetch.authRequired và Fetch.requestPaused events.
        Chạy trên dedicated WebSocket để không conflict với main _send_cdp."""
        auth_msg_counter = 99991
        while self._proxy_auth_listener_active and self._proxy_auth_ws:
            try:
                self._proxy_auth_ws.settimeout(2.0)
                raw = self._proxy_auth_ws.recv()
                data = json.loads(raw)
                method = data.get("method", "")
                
                if method == "Fetch.authRequired":
                    # Proxy yêu cầu auth → respond với credentials
                    params = data.get("params", {})
                    request_id = params.get("requestId", "")
                    if request_id:
                        auth_msg_counter += 1
                        auth_response = {
                            "id": auth_msg_counter,
                            "method": "Fetch.continueWithAuth",
                            "params": {
                                "requestId": request_id,
                                "authChallengeResponse": {
                                    "response": "ProvideCredentials",
                                    "username": self.proxy_username,
                                    "password": self.proxy_password,
                                }
                            }
                        }
                        self._proxy_auth_ws.send(json.dumps(auth_response))
                
                elif method == "Fetch.requestPaused":
                    # Request bị paused (không phải auth) → continue bình thường
                    params = data.get("params", {})
                    request_id = params.get("requestId", "")
                    if request_id:
                        auth_msg_counter += 1
                        msg = {"id": auth_msg_counter, "method": "Fetch.continueRequest",
                               "params": {"requestId": request_id}}
                        self._proxy_auth_ws.send(json.dumps(msg))
                        
            except Exception:
                if not self._proxy_auth_listener_active:
                    break
                continue

    def _wait_for_cdp_ready(self, timeout: int = 15) -> bool:
        """Đợi Chrome CDP endpoint sẵn sàng."""
        deadline = time.time() + timeout
        while time.time() < deadline:
            try:
                resp = requests.get(
                    f"http://127.0.0.1:{self.port}/json/version",
                    timeout=2,
                )
                if resp.status_code == 200:
                    info = resp.json()
                    self.log_fn(f"   ✅ Chrome CDP ready: {info.get('Browser', 'unknown')}")
                    return True
            except Exception:
                pass
            time.sleep(0.5)
        return False

    def close(self):
        """Đóng Chrome process và cleanup."""
        # Stop proxy auth listener
        self._proxy_auth_listener_active = False
        
        # Close proxy auth WebSocket
        if hasattr(self, '_proxy_auth_ws') and self._proxy_auth_ws:
            try:
                disable_msg = {"id": 99999, "method": "Fetch.disable"}
                self._proxy_auth_ws.send(json.dumps(disable_msg))
            except Exception:
                pass
            try:
                self._proxy_auth_ws.close()
            except Exception:
                pass
            self._proxy_auth_ws = None
        
        if self._ws:
            try:
                self._ws.close()
            except Exception:
                pass
            self._ws = None

        if self.process:
            try:
                self.process.terminate()
                self.process.wait(timeout=5)
            except Exception:
                try:
                    self.process.kill()
                except Exception:
                    pass
            self.process = None

    # ─── CDP WebSocket Communication ─────────────────────────

    def _get_ws_url(self) -> Optional[str]:
        """Lấy WebSocket debugger URL từ CDP endpoint."""
        try:
            resp = requests.get(f"http://127.0.0.1:{self.port}/json", timeout=5)
            pages = resp.json()
            for page in pages:
                if "webSocketDebuggerUrl" in page:
                    return page["webSocketDebuggerUrl"]
        except Exception:
            pass

        # Fallback: dùng browser-level endpoint
        try:
            resp = requests.get(f"http://127.0.0.1:{self.port}/json/version", timeout=5)
            info = resp.json()
            return info.get("webSocketDebuggerUrl")
        except Exception:
            pass
        return None

    def _connect_ws(self):
        """Kết nối WebSocket tới CDP."""
        if self._ws:
            return

        ws_url = self._get_ws_url()
        if not ws_url:
            raise RuntimeError("Không lấy được WebSocket URL từ Chrome CDP")

        self._ws = _get_ws_module().create_connection(ws_url, timeout=30)

    def _send_cdp(self, method: str, params: Optional[dict] = None, timeout: int = 30) -> dict:
        """Gửi CDP command và nhận response."""
        if not self._ws:
            self._connect_ws()

        self._msg_id += 1
        msg = {"id": self._msg_id, "method": method}
        if params:
            msg["params"] = params

        self._ws.send(json.dumps(msg))

        deadline = time.time() + timeout
        while time.time() < deadline:
            try:
                self._ws.settimeout(max(1, deadline - time.time()))
                raw = self._ws.recv()
                data = json.loads(raw)
                if data.get("id") == self._msg_id:
                    if "error" in data:
                        raise RuntimeError(f"CDP error: {data['error']}")
                    return data.get("result", {})
            except Exception as e:
                if "timed out" in str(e).lower() or "timeout" in type(e).__name__.lower():
                    continue
                raise

        raise TimeoutError(f"CDP command '{method}' timed out after {timeout}s")

    # ─── Page Navigation ─────────────────────────────────────

    def _get_page_target(self) -> Optional[str]:
        """Lấy target ID của page tab đầu tiên."""
        try:
            resp = requests.get(f"http://127.0.0.1:{self.port}/json", timeout=5)
            pages = resp.json()
            for page in pages:
                if page.get("type") == "page":
                    return page.get("id")
        except Exception:
            pass
        return None

    def _connect_to_page(self, target_id: str):
        """Kết nối WebSocket tới 1 page target cụ thể."""
        if self._ws:
            try:
                self._ws.close()
            except Exception:
                pass
            self._ws = None

        ws_url = f"ws://127.0.0.1:{self.port}/devtools/page/{target_id}"
        self._ws = _get_ws_module().create_connection(ws_url, timeout=30)
        self._msg_id = 0

    def navigate(self, url: str, wait_seconds: float = 3):
        """Navigate tới URL."""
        self._send_cdp("Page.enable")
        self._send_cdp("Page.navigate", {"url": url})
        time.sleep(wait_seconds)

    def evaluate_js(self, expression: str, timeout: int = 10) -> Any:
        """Evaluate JavaScript trên page."""
        result = self._send_cdp(
            "Runtime.evaluate",
            {"expression": expression, "returnByValue": True},
            timeout=timeout,
        )
        remote_obj = result.get("result", {})
        return remote_obj.get("value")

    # ─── Cookie Operations ───────────────────────────────────

    def get_all_cookies(self) -> List[dict]:
        """Lấy tất cả cookies qua CDP Network.getAllCookies."""
        result = self._send_cdp("Network.getAllCookies")
        return result.get("cookies", [])

    def get_labs_cookies(self) -> List[dict]:
        """Lấy cookies cho labs.google domain, filter 3 cookie cần thiết.
        
        Output format giống Cookie Editor extension:
        [
            {
                "domain": "labs.google",
                "hostOnly": true,
                "httpOnly": true,
                "name": "__Host-next-auth.csrf-token",
                "path": "/",
                "sameSite": "lax",
                "secure": true,
                "session": true,
                "storeId": null,
                "value": "..."
            },
            ...
        ]
        """
        all_cookies = self.get_all_cookies()
        labs_cookies = []

        for c in all_cookies:
            name = c.get("name", "")
            domain = c.get("domain", "")

            if name not in REQUIRED_COOKIE_NAMES:
                continue
            if LABS_DOMAIN not in domain:
                continue

            expires = c.get("expires", -1)
            # CDP returns expires as epoch seconds, -1 means session cookie
            is_session = (expires is None) or (expires <= 0)

            cookie_out = {
                "domain": LABS_DOMAIN,
            }
            if not is_session:
                cookie_out["expirationDate"] = float(expires)
            cookie_out["hostOnly"] = True
            cookie_out["httpOnly"] = bool(c.get("httpOnly", False))
            cookie_out["name"] = name
            cookie_out["path"] = c.get("path", "/")
            cookie_out["sameSite"] = (c.get("sameSite", "Lax") or "Lax").lower()
            if cookie_out["sameSite"] == "none":
                cookie_out["sameSite"] = "no_restriction"
            cookie_out["secure"] = bool(c.get("secure", False))
            cookie_out["session"] = is_session
            cookie_out["storeId"] = None
            cookie_out["value"] = c.get("value", "")

            labs_cookies.append(cookie_out)

        return labs_cookies

    def set_cookies(self, cookies: List[dict]):
        """Inject cookies vào browser qua CDP Network.setCookie.
        
        Xử lý đúng __Host- và __Secure- prefix cookies:
        - __Host-: không set domain, dùng url thay thế
        - __Secure-: phải có secure=True
        """
        for c in cookies:
            name = c.get("name", "")
            params = {
                "name": name,
                "value": c.get("value", ""),
                "path": c.get("path", "/"),
                "secure": c.get("secure", True),
                "httpOnly": c.get("httpOnly", True),
            }
            
            # __Host- cookies: KHÔNG được set domain, dùng url
            if name.startswith("__Host-"):
                params["url"] = f"https://{LABS_DOMAIN}/fx/tools/flow"
            else:
                params["domain"] = c.get("domain", LABS_DOMAIN)
                params["url"] = f"https://{LABS_DOMAIN}/fx/tools/flow"
            
            expires = c.get("expirationDate") or c.get("expires")
            if expires and float(expires) > 0:
                params["expires"] = float(expires)
            self._send_cdp("Network.setCookie", params)

    # ─── Google Login via CDP ────────────────────────────────

    def do_google_login(self, email: str, password: str, max_wait_captcha: int = 300) -> bool:
        """Auto-login Google qua CDP. Đợi user giải captcha nếu có.
        
        Returns True nếu login thành công.
        """
        try:
            self.log_fn(f"   🔐 Đăng nhập Google qua CDP...")

            # Navigate to Google login
            self.navigate("https://accounts.google.com/signin", wait_seconds=5)

            # Check if already logged in
            current_url = self.evaluate_js("window.location.href") or ""
            if "myaccount.google.com" in current_url:
                self.log_fn(f"   ✅ Đã đăng nhập sẵn!")
                return True

            # Enter email - dùng CDP Input để simulate typing thực sự
            # (input.value = ... không trigger React/Material state của Google)
            self.log_fn(f"   📧 Nhập email...")
            
            # Focus vào input email
            self._send_cdp("Runtime.evaluate", {
                "expression": """
                    (function() {
                        let input = document.querySelector('input[type="email"]');
                        if (input) {
                            input.focus();
                            input.value = '';
                        }
                    })()
                """
            })
            time.sleep(0.3)
            
            # Dùng Input.insertText để nhập email (trigger đúng input events)
            self._send_cdp("Input.insertText", {"text": email})
            time.sleep(0.5)
            
            # Dispatch input + change events để đảm bảo framework JS nhận giá trị
            self._send_cdp("Runtime.evaluate", {
                "expression": """
                    (function() {
                        let input = document.querySelector('input[type="email"]');
                        if (input) {
                            input.dispatchEvent(new Event('input', {bubbles: true}));
                            input.dispatchEvent(new Event('change', {bubbles: true}));
                        }
                    })()
                """
            })
            time.sleep(0.5)

            # Click Next
            self._send_cdp("Runtime.evaluate", {
                "expression": """
                    (function() {
                        let btn = document.querySelector('#identifierNext');
                        if (btn) btn.click();
                    })()
                """
            })
            time.sleep(4)

            # Enter password
            self.log_fn(f"   🔑 Nhập password...")
            
            # Focus vào input password
            self._send_cdp("Runtime.evaluate", {
                "expression": """
                    (function() {
                        let input = document.querySelector('input[type="password"]');
                        if (input) {
                            input.focus();
                            input.value = '';
                        }
                    })()
                """
            })
            time.sleep(0.3)
            
            # Dùng Input.insertText để nhập password
            self._send_cdp("Input.insertText", {"text": password})
            time.sleep(0.5)
            
            # Dispatch events
            self._send_cdp("Runtime.evaluate", {
                "expression": """
                    (function() {
                        let input = document.querySelector('input[type="password"]');
                        if (input) {
                            input.dispatchEvent(new Event('input', {bubbles: true}));
                            input.dispatchEvent(new Event('change', {bubbles: true}));
                        }
                    })()
                """
            })
            time.sleep(0.5)

            # Click Next
            self._send_cdp("Runtime.evaluate", {
                "expression": """
                    (function() {
                        let btn = document.querySelector('#passwordNext');
                        if (btn) btn.click();
                    })()
                """
            })
            time.sleep(3)

            # Đợi login thành công hoặc captcha
            self.log_fn(f"   ⏳ Đợi xử lý captcha (nếu có)...")
            start_time = time.time()

            while time.time() - start_time < max_wait_captcha:
                try:
                    current_url = self.evaluate_js("window.location.href") or ""

                    if "myaccount.google.com" in current_url or "accounts.google.com/b/" in current_url:
                        self.log_fn(f"   ✅ Đăng nhập thành công!")
                        return True

                    # Check lỗi login
                    page_text = self.evaluate_js("document.body.innerText.toLowerCase()") or ""
                    error_indicators = ["couldn't sign you in", "wrong password", "account disabled"]
                    if any(ind in page_text for ind in error_indicators) and "captcha" not in page_text:
                        self.log_fn(f"   ❌ Lỗi đăng nhập!")
                        return False

                    elapsed = int(time.time() - start_time)
                    if elapsed % 15 == 0 and elapsed > 0:
                        self.log_fn(f"   ⏳ Đợi captcha... ({elapsed}s/{max_wait_captcha}s)")

                except Exception:
                    pass

                time.sleep(2)

            self.log_fn(f"   ⚠️ Timeout đợi captcha ({max_wait_captcha}s)")
            return False

        except Exception as e:
            self.log_fn(f"   ❌ Login error: {str(e)[:80]}")
            return False

    # ─── Full Cookie Extraction Flow ─────────────────────────

    def extract_cookies(
        self,
        email: str = "",
        password: str = "",
        force_login: bool = False,
    ) -> List[dict]:
        """Full flow: launch Chrome → login nếu cần → navigate labs → lấy cookies.
        
        Returns list of cookie dicts (Cookie Editor format) hoặc [] nếu thất bại.
        """
        try:
            # Bước 1: Kill Chrome cũ cho profile này
            kill_chrome_for_profile(self.profile_path)
            time.sleep(1)

            # Bước 2: Check cần login không
            need_login = force_login or not check_profile_has_cookies(self.profile_path)

            if need_login and not password:
                self.log_fn(f"   ⚠️ Cần login nhưng không có password → skip")
                return []

            # Bước 3: Launch Chrome
            # Nếu cần login → visible (headless=False) để user giải captcha
            # Nếu không cần → headless
            self.headless = not need_login
            self.launch()

            # Bước 4: Kết nối CDP tới page
            time.sleep(2)
            target_id = self._get_page_target()
            if not target_id:
                # Tạo tab mới
                try:
                    resp = requests.get(
                        f"http://127.0.0.1:{self.port}/json/new?{LABS_URL}",
                        timeout=5,
                    )
                    target_id = resp.json().get("id")
                except Exception:
                    pass

            if not target_id:
                self.log_fn(f"   ❌ Không tìm thấy page target!")
                return []

            self._connect_to_page(target_id)

            # Bước 5: Login nếu cần
            if need_login:
                self.log_fn(f"   🔐 Cần đăng nhập (browser hiển thị để giải captcha)...")
                login_ok = self.do_google_login(email, password)
                if not login_ok:
                    self.log_fn(f"   ❌ Login thất bại!")
                    return []

            # Bước 6: Navigate tới Labs để lấy session cookie
            self.log_fn(f"   🌍 Vào Labs để lấy session cookie...")
            self.navigate(LABS_URL, wait_seconds=5)

            # Handle popups
            self._dismiss_popups()

            # Đợi thêm để cookies được set
            time.sleep(3)

            # Bước 7: Lấy cookies
            labs_cookies = self.get_labs_cookies()

            if not labs_cookies:
                self.log_fn(f"   ⚠️ Không tìm thấy labs.google cookies, thử lại...")
                time.sleep(5)
                labs_cookies = self.get_labs_cookies()

            # Validate
            has_session = any(
                c.get("name") == "__Secure-next-auth.session-token"
                for c in labs_cookies
            )

            # Nếu thiếu session-token → kiểm tra xem có bị redirect về login không
            if not has_session:
                current_url = ""
                try:
                    current_url = self.evaluate_js("window.location.href") or ""
                except Exception:
                    pass

                session_expired = (
                    "accounts.google" in current_url
                    or "signin" in current_url.lower()
                    or not current_url.startswith("https://labs.google")
                )

                if session_expired and email and password:
                    self.log_fn(f"   ⚠️ Session hết hạn (URL: {current_url[:80]}), thử re-login...")
                    # Đóng Chrome headless, mở lại visible để login
                    self.close()
                    time.sleep(1)
                    kill_chrome_for_profile(self.profile_path)
                    time.sleep(1)

                    self.headless = False
                    self.port = self._find_free_port()
                    self.launch()
                    time.sleep(2)

                    target_id = self._get_page_target()
                    if not target_id:
                        try:
                            resp = requests.get(
                                f"http://127.0.0.1:{self.port}/json/new?{LABS_URL}",
                                timeout=5,
                            )
                            target_id = resp.json().get("id")
                        except Exception:
                            pass

                    if target_id:
                        self._connect_to_page(target_id)
                        self.log_fn(f"   🔐 Re-login do session hết hạn...")
                        login_ok = self.do_google_login(email, password)
                        if login_ok:
                            self.log_fn(f"   🌍 Vào lại Labs sau re-login...")
                            self.navigate(LABS_URL, wait_seconds=5)
                            self._dismiss_popups()
                            time.sleep(3)
                            labs_cookies = self.get_labs_cookies()
                            has_session = any(
                                c.get("name") == "__Secure-next-auth.session-token"
                                for c in labs_cookies
                            )
                        else:
                            self.log_fn(f"   ❌ Re-login thất bại!")
                    else:
                        self.log_fn(f"   ❌ Không tìm thấy page target sau re-launch!")
                else:
                    # Thử đợi thêm lần cuối (có thể trang load chậm)
                    self.log_fn(f"   ⏳ Đợi thêm 5s cho cookies...")
                    time.sleep(5)
                    labs_cookies = self.get_labs_cookies()
                    has_session = any(
                        c.get("name") == "__Secure-next-auth.session-token"
                        for c in labs_cookies
                    )

            if has_session:
                cookie_names = [c["name"] for c in labs_cookies]
                self.log_fn(f"   ✅ Lấy được {len(labs_cookies)} cookies: {', '.join(cookie_names)}")
            else:
                self.log_fn(f"   ❌ Thiếu session-token trong cookies!")
                labs_cookies = []

            return labs_cookies

        except Exception as e:
            self.log_fn(f"   ❌ Extract cookies error: {str(e)[:100]}")
            return []

    def _dismiss_popups(self):
        """Đóng các popup trên Labs page."""
        popup_scripts = [
            """(function() {
                let btns = document.querySelectorAll('button');
                for (let b of btns) {
                    let text = b.textContent.toLowerCase();
                    if (text.includes('not now') || text.includes('no thanks') || text.includes('dismiss')) {
                        b.click(); return true;
                    }
                }
                return false;
            })()""",
            """(function() {
                let btn = document.querySelector('#card-button, button[aria-label="Close"], .close-button');
                if (btn) { btn.click(); return true; }
                return false;
            })()""",
        ]
        for script in popup_scripts:
            try:
                self._send_cdp("Runtime.evaluate", {"expression": script})
                time.sleep(0.5)
            except Exception:
                pass

        # Press Escape
        try:
            self._send_cdp("Input.dispatchKeyEvent", {
                "type": "keyDown", "key": "Escape", "code": "Escape",
                "windowsVirtualKeyCode": 27, "nativeVirtualKeyCode": 27,
            })
            self._send_cdp("Input.dispatchKeyEvent", {
                "type": "keyUp", "key": "Escape", "code": "Escape",
                "windowsVirtualKeyCode": 27, "nativeVirtualKeyCode": 27,
            })
        except Exception:
            pass


# ═══════════════════════════════════════════════════════════════
# Helper: Convert cookies to JSON string (for compatibility)
# ═══════════════════════════════════════════════════════════════

def cookies_to_json_string(cookies: List[dict]) -> str:
    """Convert cookie list thành JSON string (Cookie Editor format).
    
    Đảm bảo session-token có expirationDate nếu thiếu.
    """
    result = []
    for c in cookies:
        out = dict(c)  # shallow copy
        # Đảm bảo session-token có expiration
        if out.get("name") == "__Secure-next-auth.session-token":
            if out.get("session", True) and "expirationDate" not in out:
                out["expirationDate"] = time.time() + (365 * 24 * 3600)
                out["session"] = False
        result.append(out)
    return json.dumps(result, indent=4, ensure_ascii=False)


def parse_cookie_editor_json(text: str) -> List[List[dict]]:
    """Parse text chứa cookies (Cookie Editor format) thành list of accounts.
    
    Hỗ trợ:
    - 1 JSON array = 1 account
    - Nhiều JSON arrays cách nhau bởi blank line = nhiều accounts
    - 1 JSON array lớn chứa nhiều nhóm 3 cookies
    
    Returns list of [cookie_list_per_account].
    """
    import re

    text = text.strip()
    if not text:
        return []

    accounts = []

    # Method 1: Tách theo blank lines
    if "\n\n" in text:
        blocks = re.split(r'\n\s*\n+', text)
        for block in blocks:
            block = block.strip()
            if not block:
                continue
            try:
                data = json.loads(block)
                if isinstance(data, list) and len(data) > 0:
                    accounts.append(data)
            except Exception:
                pass

    # Method 2: Parse toàn bộ
    if not accounts:
        try:
            data = json.loads(text)
            if isinstance(data, list):
                if len(data) > 0 and isinstance(data[0], list):
                    accounts = data
                elif len(data) >= 3:
                    for i in range(0, len(data), 3):
                        group = data[i:i + 3]
                        if len(group) >= 3:
                            accounts.append(group)
                else:
                    accounts = [data]
        except Exception:
            pass

    # Validate: mỗi account phải có session-token
    valid_accounts = []
    for cookie_list in accounts:
        has_session = any(
            isinstance(c, dict) and "__Secure-next-auth.session-token" in c.get("name", "")
            for c in cookie_list
        )
        if has_session:
            valid_accounts.append(cookie_list)

    return valid_accounts
