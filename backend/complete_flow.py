#!/usr/bin/env python3
"""
Complete Google Labs Flow Video Generation Script

This script:
1. Fetches access_token from labs session using cookies
2. Uses cookies + token to execute the full video generation flow
3. Polls for completion and displays results

Usage:
    python complete_flow.py --cookies-file cookie.txt --prompt "cat in beach"
    python complete_flow.py --cookies "cookie_header_string" --prompt "your prompt"
"""

import argparse
import asyncio
import base64
import json
import os
import queue
import sys
import threading
import time
import uuid
from contextlib import contextmanager
from pathlib import Path
from typing import Dict, List, Optional, Any, Tuple

import requests
from PIL import Image

# ✅ Import Browser async từ cookiauto.py để dùng chung 1 Browser global cho reCAPTCHA
try:
    from cookiauto import _get_global_browser_async
except ImportError:
    _get_global_browser_async = None

try:
    from dotenv import load_dotenv  # type: ignore
    load_dotenv()
except Exception:
    pass


def _env(name: str, default: Optional[str] = None) -> Optional[str]:
    value = os.getenv(name)
    return value if value is not None and value != "" else default


def _read_file(path: Optional[str]) -> Optional[str]:
    if not path:
        return None
    try:
        with open(path, "r", encoding="utf-8") as f:
            return f.read()
    except Exception:
        return None


def _parse_cookie_string(cookie_string: str) -> Dict[str, str]:
    cookies: Dict[str, str] = {}
    if not cookie_string:
        return cookies
    
    text = cookie_string.strip()
    # Support JSON export format: a list of cookie objects with name/value
    if (text.startswith('[') and text.endswith(']')) or (text.startswith('{') and text.endswith('}')):
        try:
            data = json.loads(text)
            if isinstance(data, list):
                for item in data:
                    if isinstance(item, dict) and "name" in item and "value" in item:
                        cookies[str(item["name"])] = str(item["value"])
                if cookies:
                    return cookies
            elif isinstance(data, dict):
                # Occasionally JSON may be a mapping of name->value
                for k, v in data.items():
                    cookies[str(k)] = str(v)
                if cookies:
                    return cookies
        except Exception:
            # Fall back to header parsing
            pass
    
    # Fallback: standard Cookie header string: key=value; key2=value2
    parts = [p.strip() for p in cookie_string.split(';') if p.strip()]
    for part in parts:
        if '=' in part:
            key, val = part.split('=', 1)
            cookies[key.strip()] = val.strip()
    return cookies


def _extract_bearer_like(value: Any) -> Optional[str]:
    """Search arbitrarily nested JSON-like data for a token that looks like ya29.*
    
    Returns the token string without a leading 'Bearer '.
    """
    try:
        if value is None:
            return None
        if isinstance(value, str):
            lower = value.lower()
            if lower.startswith("bearer ya29."):
                return value.split(None, 1)[1]
            if value.startswith("ya29."):
                return value
            return None
        if isinstance(value, dict):
            for v in value.values():
                found = _extract_bearer_like(v)
                if found:
                    return found
            return None
        if isinstance(value, (list, tuple)):
            for item in value:
                found = _extract_bearer_like(item)
                if found:
                    return found
            return None
    except Exception:
        return None
    return None


def _normalize_bearer(token_like: Optional[str]) -> Optional[str]:
    """Return a clean ya29.* token if input looks like either 'ya29.*' or 'Bearer ya29.*'."""
    if not token_like:
        return None
    try:
        s = str(token_like).strip()
        if s.lower().startswith("bearer ya29."):
            parts = s.split(None, 1)
            return parts[1] if len(parts) > 1 else None
        if s.startswith("ya29."):
            return s
    except Exception:
        return None
    return None


class LabsFlowClient:
    """Complete Google Labs Flow client with automatic token extraction.
    
    ✅ PARALLEL TOKEN: Mỗi prompt/API call cần 1 reCAPTCHA token riêng.
    Extension xử lý nhiều request song song (không cần global lock).
    """
    
    # ✅ Lock giữa các prompt trong cùng cookie (các prompt trong cookie phải chờ nhau - nối đuôi)
    # KHÔNG còn global lock giữa các cookie - cho phép các cookie chạy song song
    _cookie_locks: Dict[str, threading.Lock] = {}  # Lock riêng cho mỗi cookie (các prompt trong cookie phải chờ nhau)
    _cookie_locks_lock = threading.Lock()  # Lock để bảo vệ _cookie_locks dictionary
    
    # ✅ API call lock (chỉ để bảo vệ shared state, không chặn song song giữa cookie)
    _api_call_lock = threading.Lock()  # Lock để bảo vệ shared state nếu cần (hiện tại không dùng nhiều)
    
    # ✅ PROXY POOL cho reCAPTCHA - user tự thêm qua dialog "Thêm Proxy"
    _use_proxy_pool: bool = False  # Mặc định TẮT, user tự bật khi thêm proxy
    _proxy_pool: List[Dict[str, str]] = []  # Mặc định rỗng, user tự thêm
    _proxy_pool_index: int = 0  # Index hiện tại trong proxy pool
    _proxy_pool_lock = threading.Lock()  # Lock để bảo vệ proxy pool
    _cookie_proxy_map: Dict[str, int] = {}  # {cookie_hash: proxy_index} - map cookie với proxy đang dùng
    _cookie_using_proxy: Dict[str, bool] = {}  # {cookie_hash: bool} - cookie có đang dùng proxy không
    _proxy_live_status: Dict[int, bool] = {}  # {proxy_index: is_live} - cache trạng thái live của proxy
    _proxy_live_check_time: Dict[int, float] = {}  # {proxy_index: timestamp} - thời gian check gần nhất
    _proxy_live_cache_ttl: float = 300.0  # Cache live status trong 5 phút
    
    @classmethod
    def _check_proxy_live(cls, proxy: Dict[str, str], timeout: float = 10.0) -> bool:
        """Kiểm tra proxy có live không bằng cách request đến httpbin.org/ip
        
        Hỗ trợ HTTP, HTTPS, SOCKS5 (WARP, Tor).
        
        Args:
            proxy: Dict với server, username, password
            timeout: Timeout cho request (giây)
        
        Returns:
            True nếu proxy live, False nếu không
        """
        import requests
        try:
            proxy_url = proxy.get("server", "")
            username = proxy.get("username", "")
            password = proxy.get("password", "")
            
            if not proxy_url:
                return False
            
            # Build proxy URL với auth
            from urllib.parse import quote
            if username and password:
                if "://" in proxy_url:
                    scheme, rest = proxy_url.split("://", 1)
                else:
                    scheme, rest = "http", proxy_url
                proxy_with_auth = f"{scheme}://{quote(username)}:{quote(password)}@{rest}"
            else:
                proxy_with_auth = proxy_url if "://" in proxy_url else f"http://{proxy_url}"
            
            proxies = {
                "http": proxy_with_auth,
                "https": proxy_with_auth,
            }
            
            # Test với httpbin.org/ip
            resp = requests.get("http://httpbin.org/ip", proxies=proxies, timeout=timeout)
            if resp.status_code == 200:
                ip_info = resp.json()
                print(f"  ✅ [Proxy Check] Proxy live! IP: {ip_info.get('origin', 'unknown')}")
                return True
            else:
                print(f"  ❌ [Proxy Check] Proxy trả về status {resp.status_code}")
                return False
        except requests.exceptions.Timeout:
            print(f"  ❌ [Proxy Check] Proxy timeout sau {timeout}s")
            return False
        except Exception as e:
            print(f"  ❌ [Proxy Check] Proxy lỗi: {str(e)[:50]}")
            return False
    
    @classmethod
    def _get_random_live_proxy(cls, max_attempts: int = 5) -> Optional[Dict[str, str]]:
        """Lấy ngẫu nhiên một proxy live từ pool.
        
        Args:
            max_attempts: Số lần thử tối đa
        
        Returns:
            Proxy dict nếu tìm được proxy live, None nếu không
        """
        import random
        import time
        
        if not cls._use_proxy_pool or not cls._proxy_pool:
            return None
        
        # Shuffle indices để lấy ngẫu nhiên
        indices = list(range(len(cls._proxy_pool)))
        random.shuffle(indices)
        
        current_time = time.time()
        
        for attempt, idx in enumerate(indices[:max_attempts]):
            proxy = cls._proxy_pool[idx]
            
            # Kiểm tra cache
            with cls._proxy_pool_lock:
                last_check = cls._proxy_live_check_time.get(idx, 0)
                cached_status = cls._proxy_live_status.get(idx)
                
                # Nếu đã check gần đây và live, dùng luôn
                if cached_status is True and (current_time - last_check) < cls._proxy_live_cache_ttl:
                    print(f"  ℹ️ [Proxy Pool] Dùng proxy #{idx} từ cache (live)")
                    return proxy
                
                # Nếu đã check gần đây và dead, skip
                if cached_status is False and (current_time - last_check) < 60:  # Cache dead 1 phút
                    continue
            
            # Check live
            print(f"  🔍 [Proxy Pool] Checking proxy #{idx} ({attempt+1}/{max_attempts})...")
            is_live = cls._check_proxy_live(proxy, timeout=8.0)
            
            # Update cache
            with cls._proxy_pool_lock:
                cls._proxy_live_status[idx] = is_live
                cls._proxy_live_check_time[idx] = current_time
            
            if is_live:
                return proxy
        
        print(f"  ⚠️ [Proxy Pool] Không tìm được proxy live sau {max_attempts} lần thử")
        return None
    
    @classmethod
    def _get_next_proxy(cls) -> Optional[Dict[str, str]]:
        """Lấy proxy tiếp theo từ pool (round-robin). Trả về None nếu proxy pool bị tắt."""
        if not cls._use_proxy_pool:
            return None
        with cls._proxy_pool_lock:
            proxy = cls._proxy_pool[cls._proxy_pool_index]
            cls._proxy_pool_index = (cls._proxy_pool_index + 1) % len(cls._proxy_pool)
            return proxy
    
    @classmethod
    def _get_proxy_for_cookie(cls, cookie_hash: str) -> Optional[Dict[str, str]]:
        """Lấy proxy cho cookie, nếu chưa có thì assign proxy mới. Trả về None nếu proxy pool bị tắt."""
        if not cls._use_proxy_pool:
            return None
        with cls._proxy_pool_lock:
            if cookie_hash not in cls._cookie_proxy_map:
                cls._cookie_proxy_map[cookie_hash] = cls._proxy_pool_index
                cls._proxy_pool_index = (cls._proxy_pool_index + 1) % len(cls._proxy_pool)
            return cls._proxy_pool[cls._cookie_proxy_map[cookie_hash]]
    
    @classmethod
    def _rotate_proxy_for_cookie(cls, cookie_hash: str) -> Optional[Dict[str, str]]:
        """Xoay sang proxy tiếp theo cho cookie (khi bị 403). Trả về None nếu proxy pool bị tắt.
        
        Nếu cookie chưa có proxy (lần đầu bị 403), assign proxy đầu tiên.
        Nếu cookie đã có proxy, xoay sang proxy tiếp theo.
        
        Enhanced với:
        - Proxy health tracking (đánh dấu proxy xấu khi liên tục lỗi)
        - Tự động bỏ qua proxy xấu khi xoay
        """
        if not cls._use_proxy_pool:
            print(f"  ℹ️ [Proxy Pool] Proxy pool đang TẮT, không xoay proxy")
            return None
        
        with cls._proxy_pool_lock:
            # Khởi tạo proxy health tracking nếu chưa có
            if not hasattr(cls, '_proxy_health_status'):
                cls._proxy_health_status: Dict[int, Dict[str, Any]] = {}
                for idx, proxy in enumerate(cls._proxy_pool):
                    cls._proxy_health_status[idx] = {
                        'consecutive_errors': 0,
                        'total_errors': 0,
                        'last_error_time': 0,
                        'is_bad': False,
                    }
            
            # Tìm proxy tốt tiếp theo (không bị đánh dấu xấu)
            def get_next_good_proxy(start_idx: int) -> Optional[tuple]:
                """Tìm proxy tốt tiếp theo, bỏ qua proxy xấu."""
                pool_size = len(cls._proxy_pool)
                for offset in range(pool_size):
                    idx = (start_idx + offset) % pool_size
                    if not cls._proxy_health_status.get(idx, {}).get('is_bad', False):
                        return idx, cls._proxy_pool[idx]
                return None  # Tất cả proxy đều xấu
            
            if cookie_hash not in cls._cookie_proxy_map:
                # Lần đầu bị 403, assign proxy đầu tiên tốt
                start_idx = cls._proxy_pool_index
                result = get_next_good_proxy(start_idx)
                if result is None:
                    print(f"  ⚠️ [Proxy Pool] Tất cả proxy đều bị đánh dấu xấu, không assign được")
                    return None
                    
                new_idx, new_proxy = result
                cls._proxy_pool_index = (new_idx + 1) % len(cls._proxy_pool)
                cls._cookie_proxy_map[cookie_hash] = new_idx
                print(f"  🌐 [Proxy Pool] Đã assign proxy #{new_idx} cho cookie {cookie_hash[:8]}... (lần đầu bị 403)")
                print(f"     → Proxy session: {new_proxy['username'][:30]}...")
            else:
                # Đánh dấu proxy hiện tại là xấu nếu đây là lỗi liên tiếp
                current_idx = cls._cookie_proxy_map.get(cookie_hash, -1)
                if current_idx >= 0:
                    health = cls._proxy_health_status.get(current_idx, {})
                    health['consecutive_errors'] = health.get('consecutive_errors', 0) + 1
                    health['total_errors'] = health.get('total_errors', 0) + 1
                    health['last_error_time'] = time.time()
                    
                    # Đánh dấu xấu nếu > 3 lỗi liên tiếp
                    if health['consecutive_errors'] > 3:
                        health['is_bad'] = True
                        print(f"  🚫 [Proxy Pool] Proxy #{current_idx} bị đánh dấu xấu sau {health['consecutive_errors']} lỗi liên tiếp")
                
                # Xoay sang proxy tốt tiếp theo
                result = get_next_good_proxy(current_idx + 1)
                if result is None:
                    print(f"  ⚠️ [Proxy Pool] Không tìm được proxy tốt để xoay")
                    return None
                    
                new_idx, new_proxy = result
                cls._cookie_proxy_map[cookie_hash] = new_idx
                print(f"  🔄 [Proxy Pool] Cookie {cookie_hash[:8]}... xoay proxy: #{current_idx} → #{new_idx}")
                print(f"     → New proxy session: {new_proxy['username'][:30]}...")
            return new_proxy
    
    @classmethod
    def _reset_proxy_health_for_cookie(cls, cookie_hash: str):
        """Reset proxy health counter khi request thành công."""
        with cls._proxy_pool_lock:
            if hasattr(cls, '_cookie_proxy_map') and cookie_hash in cls._cookie_proxy_map:
                idx = cls._cookie_proxy_map[cookie_hash]
                if hasattr(cls, '_proxy_health_status') and idx in cls._proxy_health_status:
                    health = cls._proxy_health_status[idx]
                    health['consecutive_errors'] = 0
                    # Giảm total_errors để proxy có cơ hội phục hồi
                    health['total_errors'] = max(0, health.get('total_errors', 1) - 1)
                    # Reset is_bad nếu đã phục hồi đủ (ít lỗi hơn ngưỡng)
                    if health['consecutive_errors'] == 0 and health.get('total_errors', 0) < 3:
                        if health.get('is_bad'):
                            health['is_bad'] = False
                            print(f"  ✅ [Proxy Pool] Proxy #{idx} đã phục hồi (is_bad = False)")
    
    # ✅ SINGLE PROCESS ARCHITECTURE: Thread-local Browser instances (Playwright)
    # Mỗi thread có Browser instance riêng (thread-safe với Playwright sync API)
    # Mỗi cookie/thread tạo BrowserContext riêng từ Browser của thread đó
    # Tất cả browsers có cùng AppUserModelID để Windows gom icon trên taskbar thành 1
    _thread_browsers: Dict[int, Any] = {}  # {thread_id: Browser} - mỗi thread có browser riêng
    _thread_playwrights: Dict[int, Any] = {}  # {thread_id: Playwright} - mỗi thread có playwright riêng
    _browser_lock = threading.Lock()  # Lock để bảo vệ browser initialization
    _browser_contexts: Dict[str, Any] = {}  # {cookie_hash: BrowserContext} - mỗi cookie có context riêng
    _cookies_injected_contexts: Dict[str, bool] = {}  # {cookie_hash: bool} - đánh dấu cookies đã inject vào context
    _app_id_set: bool = False  # Flag để chỉ set AppUserModelID 1 lần
    
    # ✅ Flag để đánh dấu context cần reset (worker thread sẽ tự reset)
    _contexts_need_reset: Dict[str, bool] = {}  # {cookie_hash: True} - đánh dấu context cần reset
    _contexts_need_reset_lock = threading.Lock()  # Lock để bảo vệ flag
    
    # ✅ Headless mode cho reCAPTCHA browser (mặc định False = hiện browser)
    _global_headless_mode: bool = False
    
    
    # ✅ reCAPTCHA Worker Thread Architecture: 1 worker thread chuyên reCAPTCHA, mỗi cookie có BrowserContext riêng (không giới hạn số cookie)
    _recaptcha_worker_thread: Optional[threading.Thread] = None
    _recaptcha_request_queue: queue.Queue = queue.Queue()  # Queue: (request_id, payload_dict)
    _recaptcha_results: Dict[str, Dict[str, Any]] = {}  # {request_id: {"token": str, "error": str}}
    _recaptcha_results_lock = threading.Lock()
    _recaptcha_worker_started = False
    _recaptcha_worker_browser: Optional[Any] = None  # Browser instance của worker thread (sync Playwright)
    # Mỗi cookie có tối đa 3 tab (Page) trong 1 BrowserContext
    _recaptcha_worker_pages: Dict[str, List[Any]] = {}  # {cookie_hash: [Page, ...]}
    _recaptcha_worker_page_index: Dict[str, int] = {}   # {cookie_hash: next_index} dùng round-robin trên tối đa 3 tab
    # ✅ Callback registry để lấy cookie mới khi bị chặn: {cookie_hash: callback_function}
    _recaptcha_renew_cookie_callbacks: Dict[str, Any] = {}  # {cookie_hash: callback(cookie_hash, old_cookies) -> new_cookies}
    # ✅ Flag để track cookie bị chặn từ API calls (403/429): {cookie_hash: True/False}
    _recaptcha_cookie_blocked_flags: Dict[str, bool] = {}  # {cookie_hash: is_blocked}
    _recaptcha_cookie_blocked_lock = threading.Lock()  # Lock để bảo vệ flags
    
    # ═══════════════════════════════════════════════════════════════════════
    # ✅ CHROME CDP - Primary token source (Chrome thật + CDP protocol)
    # Dùng Chrome thật thay vì zendriver để có trust score cao hơn
    # ═══════════════════════════════════════════════════════════════════════
    _chrome_cdp_available: bool = False       # True nếu tìm thấy Chrome binary
    _chrome_cdp_started: bool = False         # True nếu Chrome process đang chạy
    _chrome_cdp_process: Optional[Any] = None # subprocess.Popen instance
    _chrome_cdp_port: int = 9222             # Remote debugging port
    _chrome_cdp_lock = threading.Lock()       # Lock bảo vệ Chrome init
    _chrome_cdp_pages: Dict[str, str] = {}   # {cookie_hash: ws_url} - WebSocket URL per cookie tab
    _chrome_cdp_cookies_injected: Dict[str, bool] = {}  # {cookie_hash: True}
    _chrome_cdp_tab_ids: Dict[str, str] = {} # {cookie_hash: tab_id}
    _chrome_cdp_ws_conns: Dict[str, Any] = {}  # {cookie_hash: websocket connection}
    _chrome_cdp_ws_msg_ids: Dict[str, int] = {}  # {cookie_hash: next msg_id}
    _chrome_cdp_page_ready: Dict[str, bool] = {}  # {cookie_hash: True nếu page đã load xong}
    
    # ✅ Giữ lại zendriver variables cho backward compat (sẽ không dùng nữa)
    _zendriver_available: bool = False
    _zendriver_started: bool = False
    _zendriver_loop: Optional[Any] = None
    _zendriver_thread: Optional[threading.Thread] = None
    _zendriver_browser: Optional[Any] = None
    _zendriver_pages: Dict[str, Any] = {}
    _zendriver_lock = threading.Lock()
    _zendriver_cookies_injected: Dict[str, bool] = {}
    
    # Token source tracking: "chrome_cdp" hoặc "playwright"
    _last_token_source: Dict[str, str] = {}           # {cookie_hash: source}
    _zendriver_consecutive_403: Dict[str, int] = {}    # {cookie_hash: count} - giữ cho compat
    _chrome_cdp_consecutive_403: Dict[str, int] = {}   # {cookie_hash: count}
    _playwright_consecutive_403: Dict[str, int] = {}   # {cookie_hash: count}
    MAX_ZENDRIVER_403 = 3   # Giữ cho compat
    MAX_CHROME_CDP_403 = 3  # Sau 3 lần 403 liên tiếp từ Chrome CDP → chuyển sang playwright
    MAX_PLAYWRIGHT_403 = 3  # Sau 3 lần 403 liên tiếp từ playwright → reset cookie
    
    @classmethod
    def set_use_proxy_pool(cls, enabled: bool):
        """Enable/disable proxy pool và sync dữ liệu từ ProxyManager.
        
        Khi enabled=True, đọc proxy list từ ProxyManager singleton
        và sync vào cls._proxy_pool để LabsFlowClient sử dụng.
        Khi enabled=False, tắt proxy pool nhưng giữ nguyên data.
        """
        cls._use_proxy_pool = enabled
        
        if enabled:
            try:
                from proxy_manager import ProxyManager
                pm = ProxyManager.get_instance()
                # Sync proxy list từ ProxyManager → LabsFlowClient._proxy_pool
                proxies = pm.get_all_proxies()
                with cls._proxy_pool_lock:
                    cls._proxy_pool = [p.to_playwright_proxy() for p in proxies]
                    # Reset index nếu vượt quá pool size
                    if cls._proxy_pool:
                        cls._proxy_pool_index = cls._proxy_pool_index % len(cls._proxy_pool)
                    else:
                        cls._proxy_pool_index = 0
                print(f"  ✅ [Proxy Pool] Đã BẬT proxy pool ({len(cls._proxy_pool)} proxies)")
            except Exception as e:
                print(f"  ⚠️ [Proxy Pool] Lỗi sync proxy pool: {e}")
                cls._proxy_pool = []
        else:
            print(f"  ℹ️ [Proxy Pool] Đã TẮT proxy pool")
    
    @classmethod
    def sync_proxy_pool(cls):
        """Sync lại proxy list từ ProxyManager (gọi khi user thêm/xóa proxy)."""
        if not cls._use_proxy_pool:
            return
        try:
            from proxy_manager import ProxyManager
            pm = ProxyManager.get_instance()
            proxies = pm.get_all_proxies()
            with cls._proxy_pool_lock:
                cls._proxy_pool = [p.to_playwright_proxy() for p in proxies]
                if cls._proxy_pool:
                    cls._proxy_pool_index = cls._proxy_pool_index % len(cls._proxy_pool)
                else:
                    cls._proxy_pool_index = 0
                # Clear cookie-proxy mapping khi pool thay đổi
                cls._cookie_proxy_map.clear()
                cls._cookie_using_proxy.clear()
                cls._proxy_live_status.clear()
                cls._proxy_live_check_time.clear()
            print(f"  🔄 [Proxy Pool] Đã sync lại proxy pool ({len(cls._proxy_pool)} proxies)")
        except Exception as e:
            print(f"  ⚠️ [Proxy Pool] Lỗi sync proxy pool: {e}")
    
    @classmethod
    def set_headless_mode(cls, headless: bool):
        """Đặt chế độ headless cho reCAPTCHA browser."""
        cls._global_headless_mode = headless
        mode_str = "HEADLESS" if headless else "OFF-SCREEN"
        print(f"  ✅ reCAPTCHA mode: LOCAL BROWSER ({mode_str})")
    
    # ═══════════════════════════════════════════════════════════════════════
    # ✅ AUTO COOKIE RENEWAL - Tự động lấy cookie mới khi bị 403
    # Lưu thông tin account (email, password, profile_path) cho mỗi cookie_hash
    # Khi bị 403 liên tiếp → tự động headless login lại để lấy cookie mới
    # ═══════════════════════════════════════════════════════════════════════
    _cookie_account_info: Dict[str, Dict[str, str]] = {}  # {cookie_hash: {email, password, profile_path}}
    _cookie_auto_renew_lock = threading.Lock()  # Lock bảo vệ auto-renew (tránh nhiều thread cùng renew)
    _cookie_renewing: Dict[str, bool] = {}  # {cookie_hash: True} - đánh dấu đang renew
    
    @classmethod
    def register_account_info(cls, cookie_hash: str, email: str, password: str, profile_path: str):
        """Đăng ký thông tin account cho cookie để auto-renew khi bị 403.
        
        Args:
            cookie_hash: Hash của cookie
            email: Email Google account
            password: Password Google account
            profile_path: Đường dẫn profile browser
        """
        cls._cookie_account_info[cookie_hash] = {
            "email": email,
            "password": password,
            "profile_path": profile_path,
        }
        print(f"  ✅ [Auto Renew] Đã đăng ký account info cho cookie {cookie_hash[:8]}... (email: {email})")
    
    @classmethod
    def register_account_info_from_db(cls):
        """Đọc tất cả accounts từ DB và đăng ký auto-renew cho mỗi cookie.
        
        Gọi hàm này khi khởi động app hoặc khi cookies_list thay đổi.
        """
        try:
            from cookiauto import db_get_all_accounts, db_get_account_cookies
            from complete_flow import _parse_cookie_string
            
            accounts = db_get_all_accounts()
            registered = 0
            for acc in accounts:
                email = acc.get("email", "")
                password = acc.get("password", "")
                profile_path = acc.get("profile_path", "")
                
                if not email or not profile_path:
                    continue
                
                # Lấy cookies từ DB để tính cookie_hash
                try:
                    cookies_json = db_get_account_cookies(email)
                    if cookies_json:
                        import json
                        cookies_list = json.loads(cookies_json)
                        # Chuyển list of cookie objects → dict {name: value}
                        cookies_dict: Dict[str, str] = {}
                        for c in cookies_list:
                            if isinstance(c, dict):
                                cookies_dict[c.get("name", "")] = c.get("value", "")
                        
                        if cookies_dict:
                            ch = cls._get_cookie_hash(cookies_dict)
                            cls.register_account_info(ch, email, password, profile_path)
                            registered += 1
                except Exception:
                    pass
            
            if registered > 0:
                print(f"  ✅ [Auto Renew] Đã đăng ký {registered} accounts từ DB cho auto-renew")
        except Exception as e:
            print(f"  ⚠️ [Auto Renew] Lỗi đọc accounts từ DB: {e}")
    
    @classmethod
    def register_account_info_for_cookie_str(cls, cookie_str: str, email: str, password: str, profile_path: str):
        """Đăng ký account info cho cookie string (tiện dùng từ GUI).
        
        Args:
            cookie_str: Cookie string (format: "name=value; name2=value2; ...")
            email: Email Google account
            password: Password Google account
            profile_path: Đường dẫn profile browser
        """
        try:
            cookies = _parse_cookie_string(cookie_str)
            if cookies:
                ch = cls._get_cookie_hash(cookies)
                cls.register_account_info(ch, email, password, profile_path)
        except Exception as e:
            print(f"  ⚠️ [Auto Renew] Lỗi đăng ký account info: {e}")
    
    @classmethod
    def register_renew_cookie_callback(cls, cookie_hash: str, callback: Any):
        """
        Đăng ký callback để lấy cookie mới khi bị chặn.
        
        Args:
            cookie_hash: Hash của cookie để identify
            callback: Function(cookie_hash: str, old_cookies: Dict[str, str]) -> Optional[Dict[str, str]]
                      Trả về cookie mới nếu thành công, None nếu không thể renew
        """
        if not hasattr(cls, '_recaptcha_renew_cookie_callbacks'):
            cls._recaptcha_renew_cookie_callbacks = {}
        cls._recaptcha_renew_cookie_callbacks[cookie_hash] = callback
        print(f"  ✅ [reCAPTCHA] Đã đăng ký callback renew cookie cho: {cookie_hash[:8]}...")
    
    @classmethod
    def unregister_renew_cookie_callback(cls, cookie_hash: str):
        """Hủy đăng ký callback cho cookie hash"""
        if hasattr(cls, '_recaptcha_renew_cookie_callbacks'):
            cls._recaptcha_renew_cookie_callbacks.pop(cookie_hash, None)
    
    def __init__(self, cookies: Dict[str, str], session: Optional[requests.Session] = None, profile_path: Optional[str] = None, proxy_config: Optional[Dict[str, str]] = None):
        self.session = session or requests.Session()
        self.cookies = cookies
        self.profile_path = profile_path
        # ✅ Proxy configuration: ProxyConfig dict hoặc legacy {server, username, password} hoặc None
        self.proxy_config: Optional[Dict[str, str]] = proxy_config
        self.access_token: Optional[str] = None
        self.last_error_detail: Optional[str] = None
        # ✅ Luôn khởi tạo last_error để tránh AttributeError: 'LabsFlowClient' has no attribute 'last_error'
        self.last_error: Optional[str] = None
        
        # ✅ Áp dụng proxy vào session ngay khi khởi tạo (nếu có)
        if proxy_config:
            self._apply_proxy_to_session(proxy_config)
        self.user_agent = _env(
            "USER_AGENT",
            "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
            "(KHTML, like Gecko) Chrome/139.0.0.0 Safari/537.36",
        )
        self.flow_project_id = _env("FLOW_PROJECT_ID", "b7974022-eba1-489c-8228-eb02442e2a6a")

        # --- reCAPTCHA mode: Selenium Driver (mặc định) hoặc Extension ---
        # Enable by setting env AUTO_RECAPTCHA=1 (recommended for GUI)
        auto_flag = _env("AUTO_RECAPTCHA", "0") or "0"
        self.auto_recaptcha: bool = str(auto_flag) in ("1", "true", "True", "YES", "yes")
        
        # ✅ Toggle giữa Selenium Driver và Extension
        # RECAPTCHA_MODE có thể là: "selenium", "browser", "driver" (mặc định) hoặc "extension", "bridge"
        recaptcha_mode = _env("RECAPTCHA_MODE", "selenium").lower()
        
        # ✅ Mặc định dùng Selenium Driver (Trình duyệt)
        if recaptcha_mode in ("selenium", "browser", "driver", "trinhduyet"):
            self.use_selenium_recaptcha = True
            self.use_extension_recaptcha = False
        elif recaptcha_mode in ("extension", "bridge", "ext"):
            self.use_selenium_recaptcha = False
            self.use_extension_recaptcha = True
        else:
            # Fallback: mặc định Selenium
            self.use_selenium_recaptcha = True
            self.use_extension_recaptcha = False
        
        # ✅ Extension mode settings (chỉ dùng khi use_extension_recaptcha = True)
        self.captcha_bridge_url: str = _env("CAPTCHA_BRIDGE_URL", "http://localhost:3000") or "http://localhost:3000"
        
        # ✅ Selenium driver settings (chỉ dùng khi use_selenium_recaptcha = True)
        self.selenium_headless: bool = str(_env("SELENIUM_HEADLESS", "0") or "0") in ("1", "true", "True", "YES", "yes")
        self.selenium_browser_path: Optional[str] = _env("SELENIUM_BROWSER_PATH")
        
        # ✅ Log mode đang dùng
        if self.auto_recaptcha:
            if self.use_selenium_recaptcha:
                mode_str = "Selenium Driver (Trình duyệt)"
                if self.selenium_headless:
                    mode_str += " [Headless]"
            else:
                mode_str = f"Extension (Bridge: {self.captcha_bridge_url})"
            print(f"✓ reCAPTCHA mode: {mode_str}")
        
        # ✅ Generate cookie hash để identify cookie và tạo file token riêng
        self._cookie_hash = self._get_cookie_hash(cookies)
        
        # ✅ Debug: Log cookie hash và số lượng cookies để kiểm tra
        cookie_count = len(cookies) if cookies else 0
        cookie_names = list(cookies.keys())[:5] if cookies else []  # Lấy 5 cookie đầu tiên để debug
        print(f"  🔍 Cookie hash: {self._cookie_hash[:8]}... (Tổng: {cookie_count} cookies, ví dụ: {', '.join(cookie_names[:3])})")
        
        # ✅ Selenium driver instances theo cookie hash (SHARED giữa tất cả instances)
        # Dùng class variable để share drivers giữa các instances cùng cookie
        if not hasattr(LabsFlowClient, '_shared_selenium_drivers'):
            LabsFlowClient._shared_selenium_drivers: Dict[str, Any] = {}
        if not hasattr(LabsFlowClient, '_shared_cookies_injected'):
            LabsFlowClient._shared_cookies_injected: Dict[str, bool] = {}
        
        # ✅ Playwright BrowserContext instances (thay thế Selenium)
        # Mỗi cookie có BrowserContext riêng từ global Browser
        if not hasattr(LabsFlowClient, '_browser_contexts'):
            LabsFlowClient._browser_contexts: Dict[str, Any] = {}
        if not hasattr(LabsFlowClient, '_cookies_injected_contexts'):
            LabsFlowClient._cookies_injected_contexts: Dict[str, bool] = {}
        
        # ✅ Counter đếm số lần 403 liên tiếp cho mỗi cookie (SHARED)
        if not hasattr(LabsFlowClient, '_shared_403_counters'):
            LabsFlowClient._shared_403_counters: Dict[str, int] = {}
        
        # ✅ Counter đếm số lần 429 liên tiếp cho mỗi cookie (SHARED)
        if not hasattr(LabsFlowClient, '_shared_429_counters'):
            LabsFlowClient._shared_429_counters: Dict[str, int] = {}
        
        # ✅ Counter đếm tổng số lỗi cho mỗi cookie (SHARED)
        if not hasattr(LabsFlowClient, '_shared_total_error_counters'):
            LabsFlowClient._shared_total_error_counters: Dict[str, int] = {}
        
        # ✅ Tracking cookie source: "profile" (đăng nhập) hoặc "import" (import trực tiếp)
        if not hasattr(LabsFlowClient, '_cookie_sources'):
            LabsFlowClient._cookie_sources: Dict[str, str] = {}
        
        # ✅ Tracking profile path cho mỗi cookie (để lấy cookie mới)
        if not hasattr(LabsFlowClient, '_cookie_profile_paths'):
            LabsFlowClient._cookie_profile_paths: Dict[str, str] = {}
        
        # ✅ Lưu cookie source và profile path cho cookie này
        if profile_path:
            LabsFlowClient._cookie_sources[self._cookie_hash] = "profile"
            LabsFlowClient._cookie_profile_paths[self._cookie_hash] = profile_path
        else:
            LabsFlowClient._cookie_sources[self._cookie_hash] = "import"
        
        # ✅ Rate limiting per cookie (instance-level) - cho phép các cookie chạy song song
        self._last_api_call_time: float = 0
        self._api_call_count: int = 0
        self._min_api_call_interval = 0.3  # Minimum 0.3s giữa các API calls của cookie này
        
        # ✅ Token freshness tracking - reCAPTCHA token hết hạn sau 120s (Google docs)
        # Lưu timestamp khi token được generate để kiểm tra trước khi gọi API
        if not hasattr(LabsFlowClient, '_token_timestamps'):
            LabsFlowClient._token_timestamps: Dict[str, float] = {}
        # Token được coi là "fresh" nếu < TOKEN_MAX_AGE_SECONDS
        self.TOKEN_MAX_AGE_SECONDS = 90  # 90s (buffer 30s trước khi hết hạn 120s)
    
    @classmethod
    def cleanup_selenium_driver(cls):
        """Đóng tất cả Selenium drivers và Chrome CDP process."""
        if hasattr(cls, '_shared_selenium_drivers'):
            for cookie_hash, driver in list(cls._shared_selenium_drivers.items()):
                try:
                    driver.quit()
                    print(f"  ✓ Đã đóng Selenium driver (cookie: {cookie_hash[:8]}...)")
                except Exception:
                    pass
            cls._shared_selenium_drivers.clear()
        if hasattr(cls, '_shared_cookies_injected'):
            cls._shared_cookies_injected.clear()
        
        # ✅ Cleanup Chrome CDP process
        cls._cleanup_chrome_cdp()
    
    @classmethod
    def _cleanup_chrome_cdp(cls):
        """Đóng Chrome CDP process và cleanup resources."""
        # Close tất cả persistent WebSocket connections
        for ch, ws_conn in list(cls._chrome_cdp_ws_conns.items()):
            try:
                ws_conn.close()
            except Exception:
                pass
        cls._chrome_cdp_ws_conns.clear()
        cls._chrome_cdp_ws_msg_ids.clear()
        cls._chrome_cdp_page_ready.clear()
        
        # Close tất cả tabs
        if cls._chrome_cdp_started:
            for cookie_hash, tab_id in list(cls._chrome_cdp_tab_ids.items()):
                try:
                    requests.get(
                        f"http://127.0.0.1:{cls._chrome_cdp_port}/json/close/{tab_id}",
                        timeout=2,
                    )
                except Exception:
                    pass
        
        cls._chrome_cdp_pages.clear()
        cls._chrome_cdp_tab_ids.clear()
        cls._chrome_cdp_cookies_injected.clear()
        
        # Kill Chrome process
        if cls._chrome_cdp_process:
            try:
                cls._chrome_cdp_process.terminate()
                cls._chrome_cdp_process.wait(timeout=5)
                print("  ✓ [Chrome CDP] Chrome process terminated")
            except Exception:
                try:
                    cls._chrome_cdp_process.kill()
                except Exception:
                    pass
            cls._chrome_cdp_process = None
        
        cls._chrome_cdp_started = False
        
        # Cleanup temp user-data-dir
        if hasattr(cls, '_chrome_cdp_user_data_dir') and cls._chrome_cdp_user_data_dir:
            import shutil
            try:
                shutil.rmtree(cls._chrome_cdp_user_data_dir, ignore_errors=True)
            except Exception:
                pass
            cls._chrome_cdp_user_data_dir = None
    
    @classmethod
    def _get_global_browser(cls, headless: bool = False, browser_path: Optional[str] = None) -> Any:
        """
        Lấy hoặc khởi tạo Browser instance (Playwright) cho thread hiện tại.
        Thread-local Browser: Mỗi thread có Browser riêng (thread-safe với Playwright sync API).
        
        ✅ THREAD-SAFE: Mỗi thread có browser instance riêng để tránh "Cannot switch to a different thread"
        ✅ ICON GROUPING: Tất cả browsers có cùng AppUserModelID để Windows gom icon trên taskbar thành 1
        
        Args:
            headless: Chạy headless mode
            browser_path: Đường dẫn đến Chrome executable (optional)
        
        Returns:
            Browser instance (thread-local)
        """
        import platform
        import threading
        import time
        import random
        
        # ✅ THREAD-LOCAL: Mỗi thread có browser instance riêng
        # Đảm bảo thread safety (tránh "Cannot switch to a different thread")
        thread_id = threading.current_thread().ident
        
        # Kiểm tra xem thread này đã có browser chưa
        if not hasattr(cls, '_thread_browsers'):
            cls._thread_browsers: Dict[int, Any] = {}
        if not hasattr(cls, '_thread_playwrights'):
            cls._thread_playwrights: Dict[int, Any] = {}
        
        with cls._browser_lock:
            # Kiểm tra thread-local browser
            if thread_id in cls._thread_browsers:
                browser = cls._thread_browsers[thread_id]
                # Test xem browser còn hoạt động không
                try:
                    # Test bằng cách lấy contexts
                    _ = browser.contexts
                    return browser
                except Exception:
                    # Browser đã bị đóng, xóa và tạo lại
                    cls._thread_browsers.pop(thread_id, None)
                    cls._thread_playwrights.pop(thread_id, None)
            
            # Tạo browser mới cho thread này
            try:
                from playwright.sync_api import sync_playwright
                
                # ✅ Thêm delay ngẫu nhiên để giãn tải khi khởi tạo (tránh nghẽn mạng)
                time.sleep(random.uniform(3.0, 5.0))
                
                print(f"  🚀 Khởi tạo Browser instance (Playwright) cho thread {thread_id}...")
                playwright = sync_playwright().start()
                
                # Browser launch args
                launch_args = [
                    '--no-first-run',
                    '--no-default-browser-check',
                    '--disable-extensions',
                    '--disable-infobars',
                    '--disable-sync',
                    '--disable-signin-promo',
                    '--disable-features=Translate,OptimizationGuideModelDownloading,OptimizationHints,InteractiveWindowOcclusion',
                    '--password-store=basic',
                    '--use-mock-keychain',
                    '--hide-crash-restore-bubble',
                ]
                
                # Windows-specific: Set AppUserModelID để gom icon trên taskbar
                # ✅ CÙNG AppUserModelID cho tất cả threads để Windows gom icon thành 1
                if platform.system() == 'Windows':
                    app_id = "GetCookieVeo3"  # Cùng ID với cookiauto.py
                    launch_args.append(f'--app-id={app_id}')
                    # Set AppUserModelID cho process (chỉ set 1 lần)
                    if not cls._app_id_set:
                        try:
                            import ctypes
                            ctypes.windll.shell32.SetCurrentProcessExplicitAppUserModelID(app_id)
                            cls._app_id_set = True
                            print(f"  ✅ Đã set AppUserModelID: {app_id} (gom icon trên taskbar)")
                        except Exception as e:
                            print(f"  ⚠️ Không thể set AppUserModelID: {e}")
                
                # Launch browser
                browser = playwright.chromium.launch(
                    channel="chrome",
                    headless=headless,
                    executable_path=browser_path,
                    args=launch_args,
                )
                
                # Lưu browser cho thread này
                cls._thread_browsers[thread_id] = browser
                cls._thread_playwrights[thread_id] = playwright
                print(f"  ✅ Browser instance đã khởi tạo thành công cho thread {thread_id} (cùng AppUserModelID để gom icon)")
                
            except ImportError:
                raise RuntimeError("playwright chưa được cài đặt. Chạy: pip install playwright && playwright install chromium")
            except Exception as e:
                raise RuntimeError(f"Không thể khởi tạo Browser: {str(e)}")
        
        return cls._thread_browsers[thread_id]
    
    @classmethod
    def cleanup_playwright_browser(cls):
        """Đóng tất cả Browser instances và contexts (tất cả threads)."""
        with cls._browser_lock:
            # Đóng tất cả contexts
            if hasattr(cls, '_browser_contexts'):
                for cookie_hash, context in list(cls._browser_contexts.items()):
                    try:
                        context.close()
                        print(f"  ✓ Đã đóng BrowserContext (cookie: {cookie_hash[:8]}...)")
                    except Exception:
                        pass
                cls._browser_contexts.clear()
            
            # Đóng tất cả browsers (tất cả threads)
            if hasattr(cls, '_thread_browsers'):
                for thread_id, browser in list(cls._thread_browsers.items()):
                    try:
                        browser.close()
                        print(f"  ✓ Đã đóng Browser (thread: {thread_id})")
                    except Exception:
                        pass
                cls._thread_browsers.clear()
            
            # Đóng tất cả playwright instances
            if hasattr(cls, '_thread_playwrights'):
                for thread_id, playwright in list(cls._thread_playwrights.items()):
                    try:
                        playwright.stop()
                        print(f"  ✓ Đã đóng Playwright (thread: {thread_id})")
                    except Exception:
                        pass
                cls._thread_playwrights.clear()
            
            # Clear flags
            if hasattr(cls, '_cookies_injected_contexts'):
                cls._cookies_injected_contexts.clear()
            
            # ✅ Cleanup reCAPTCHA context
            if hasattr(cls, '_recaptcha_context') and cls._recaptcha_context:
                try:
                    cls._recaptcha_context.close()
                except:
                    pass
                cls._recaptcha_context = None
            cls._recaptcha_page = None
    
    def _restart_selenium_driver_for_cookie(self) -> bool:
        """
        Restart Chrome driver cho cookie hiện tại (chỉ driver của cookie này).
        Đóng driver cũ, khởi tạo lại driver mới với cùng cấu hình và vị trí.
        
        Returns:
            True nếu restart thành công, False nếu không thể restart
        """
        if not self.use_selenium_recaptcha:
            return False
        
        cookie_hash = self._cookie_hash
        import platform
        
        try:
            from selenium import webdriver
            from selenium.webdriver.chrome.options import Options as ChromeOptions
            from selenium.webdriver.chrome.service import Service as ChromeService
        except ImportError:
            print(f"  ✗ Không thể restart driver: selenium chưa được cài đặt")
            return False
        
        # ✅ Lấy driver cũ và lưu vị trí cửa sổ
        old_driver = None
        old_window_position = None
        old_window_size = None
        
        if hasattr(LabsFlowClient, '_shared_selenium_drivers'):
            old_driver = LabsFlowClient._shared_selenium_drivers.get(cookie_hash)
            if old_driver:
                try:
                    # Lưu vị trí và kích thước cửa sổ cũ
                    old_window_position = old_driver.get_window_position()
                    old_window_size = old_driver.get_window_size()
                    print(f"  → Đang đóng Chrome driver cũ của cookie {cookie_hash[:8]}...")
                    old_driver.quit()
                    print(f"  ✓ Đã đóng driver cũ")
                except Exception as quit_err:
                    print(f"  ⚠️ Lỗi khi đóng driver cũ: {quit_err}")
        
        # ✅ Xóa driver khỏi shared dictionary
        if hasattr(LabsFlowClient, '_shared_selenium_drivers'):
            LabsFlowClient._shared_selenium_drivers.pop(cookie_hash, None)
        
        # ✅ Reset flag cookies injected
        if hasattr(LabsFlowClient, '_shared_cookies_injected'):
            LabsFlowClient._shared_cookies_injected.pop(cookie_hash, None)
        
        # ✅ Khởi tạo lại driver mới với cùng cấu hình
        print(f"  → Khởi tạo lại Chrome driver mới cho cookie {cookie_hash[:8]}...")
        try:
            chrome_options = ChromeOptions()
            
            # ✅ Essential options để tránh crash
            chrome_options.add_argument('--no-sandbox')
            chrome_options.add_argument('--disable-dev-shm-usage')
            chrome_options.add_argument('--disable-gpu')
            chrome_options.add_argument('--disable-software-rasterizer')
            chrome_options.add_argument('--disable-extensions')
            chrome_options.add_argument('--disable-logging')
            chrome_options.add_argument('--disable-web-security')
            chrome_options.add_argument('--allow-running-insecure-content')

            # ✅ Use persistent profile if available
            if self.profile_path:
                chrome_options.add_argument(f"--user-data-dir={self.profile_path}")
                print(f"  → Dùng profile: {self.profile_path}")
            
            # Headless mode
            if self.selenium_headless:
                chrome_options.add_argument('--headless=new')
                chrome_options.add_argument('--window-size=1920,1080')
            
            # Anti-detection
            chrome_options.add_experimental_option("excludeSwitches", ["enable-automation", "enable-logging"])
            chrome_options.add_experimental_option('useAutomationExtension', False)
            
            # Prefs
            prefs = {
                "profile.default_content_setting_values.notifications": 2,
                "profile.default_content_settings.popups": 0,
                "profile.managed_default_content_settings.images": 1,
            }
            chrome_options.add_experimental_option("prefs", prefs)
            
            # User agent
            chrome_options.add_argument(f'user-agent={self.user_agent}')
            
            # ✅ Windows-specific options
            if platform.system() == 'Windows':
                chrome_options.add_argument('--disable-crash-reporter')
                chrome_options.add_argument('--disable-breakpad')
                chrome_options.add_argument('--disable-background-networking')
                chrome_options.add_argument('--disable-background-timer-throttling')
                chrome_options.add_argument('--disable-backgrounding-occluded-windows')
                chrome_options.add_argument('--disable-component-update')
                chrome_options.add_argument('--disable-default-apps')
                chrome_options.add_argument('--disable-features=TranslateUI')
                chrome_options.add_argument('--disable-hang-monitor')
                chrome_options.add_argument('--disable-ipc-flooding-protection')
                chrome_options.add_argument('--disable-prompt-on-repost')
                chrome_options.add_argument('--disable-renderer-backgrounding')
                chrome_options.add_argument('--disable-sync')
                chrome_options.add_argument('--disable-translate')
                chrome_options.add_argument('--metrics-recording-only')
                chrome_options.add_argument('--no-first-run')
                chrome_options.add_argument('--safebrowsing-disable-auto-update')
                chrome_options.add_argument('--enable-automation')
                chrome_options.add_argument('--password-store=basic')
                chrome_options.add_argument('--use-mock-keychain')
            
            # Khởi tạo driver mới
            if self.selenium_browser_path:
                service = ChromeService(executable_path=self.selenium_browser_path)
                new_driver = webdriver.Chrome(service=service, options=chrome_options)
            else:
                new_driver = webdriver.Chrome(options=chrome_options)
            
            # ✅ Test driver hoạt động
            new_driver.set_page_load_timeout(10)
            new_driver.get("about:blank")
            
            # ✅ Đặt kích thước và vị trí cửa sổ - TẤT CẢ DRIVER CÙNG VỊ TRÍ (0, 0)
            window_width = 400
            window_height = 300
            window_x = 0
            window_y = 0
            
            try:
                new_driver.set_window_size(window_width, window_height)
                new_driver.set_window_position(window_x, window_y)
                print(f"  → Đã đặt cửa sổ: {window_width}x{window_height} tại vị trí ({window_x}, {window_y})")
            except Exception as pos_err:
                print(f"  ⚠️ Không thể đặt vị trí cửa sổ: {pos_err}")
            
            # ✅ Lưu driver mới vào shared dictionary
            if not hasattr(LabsFlowClient, '_shared_selenium_drivers'):
                LabsFlowClient._shared_selenium_drivers = {}
            LabsFlowClient._shared_selenium_drivers[cookie_hash] = new_driver
            
            print(f"  ✅ Đã restart Chrome driver thành công cho cookie {cookie_hash[:8]}...")
            print(f"    → Driver mới đã sẵn sàng, sẽ inject cookies và lấy token mới")
            return True
            
        except Exception as restart_err:
            print(f"  ✗ Lỗi khi restart driver: {restart_err}")
            # Xóa driver khỏi dictionary nếu có lỗi
            if hasattr(LabsFlowClient, '_shared_selenium_drivers'):
                LabsFlowClient._shared_selenium_drivers.pop(cookie_hash, None)
            return False
    
    def _hard_reset_driver(self, cookie_hash: str) -> None:
        """
        Hard reset: Xóa sạch dữ liệu, đóng Chrome, xóa cache driver.
        Để lần tới chạy sẽ khởi tạo Chrome mới hoàn toàn.
        """
        if not self.use_selenium_recaptcha:
            return

        print(f"  🔄 HARD RESET: Đóng Chrome, xóa dữ liệu, force new session cho cookie {cookie_hash[:8]}...")
        
        # 1. Lấy driver cũ
        old_driver = None
        if hasattr(LabsFlowClient, '_shared_selenium_drivers'):
            old_driver = LabsFlowClient._shared_selenium_drivers.get(cookie_hash)
        
        if old_driver:
            try:
                # 2. Xóa dữ liệu duyệt web (best effort)
                # ✅ KHÔNG xóa data nếu đang dùng persistent profile (để tránh logout)
                if self.profile_path:
                     print(f"  ⚠️ Persistent profile detect: SKIP deleting cookies/storage to avoid logout.")
                else:
                    print(f"  → Xóa toàn bộ dữ liệu duyệt web...")
                    try:
                        old_driver.delete_all_cookies()
                        old_driver.execute_script("""
                            try { localStorage.clear(); } catch(e) {}
                            try { sessionStorage.clear(); } catch(e) {}
                            if ('indexedDB' in window) {
                                try {
                                    indexedDB.databases().then(dbs => {
                                        dbs.forEach(db => {
                                            if (db.name) indexedDB.deleteDatabase(db.name);
                                        });
                                    });
                                } catch(e) {}
                            }
                            if ('caches' in window) {
                                try {
                                    caches.keys().then(names => {
                                        names.forEach(name => caches.delete(name));
                                    });
                                } catch(e) {}
                            }
                        """)
                    except Exception as clear_err:
                        print(f"  ⚠️ Lỗi khi xóa data (không nghiêm trọng): {clear_err}")
                
                # 3. Đóng Chrome
                print(f"  → Đóng Chrome cũ...")
                old_driver.quit()
                print(f"  ✓ Đã đóng Chrome cũ của cookie {cookie_hash[:8]}...")
            except Exception as quit_err:
                print(f"  ⚠️ Lỗi khi đóng Chrome: {quit_err}")
        
        # 4. Xóa khỏi dictionary để lần sau tạo mới
        if hasattr(LabsFlowClient, '_shared_selenium_drivers'):
            LabsFlowClient._shared_selenium_drivers.pop(cookie_hash, None)
            
        # 5. Reset flags injected
        if hasattr(LabsFlowClient, '_shared_cookies_injected'):
            LabsFlowClient._shared_cookies_injected.pop(cookie_hash, None)
            
        # 6. Reset counters cho cookie này
        if hasattr(LabsFlowClient, '_shared_403_counters'):
            LabsFlowClient._shared_403_counters[cookie_hash] = 0
            
        print(f"  ✓ Đã reset hoàn toàn, driver mới sẽ được tạo ở request tiếp theo.")
        time.sleep(1.5)  # Đợi OS giải phóng resource

    def check_live_status(self) -> bool:
        """Check if cookie is live (MUST set model key successfully)."""
        try:
            # ✅ User request: "check cookie set được model mới tính là live"
            # Prioritize validation by setting model key
            if not self.set_video_model_key("veo_3_1_t2v_fast_ultra"):
                print(f"  ❌ Cookie check failed: Unable to set model key.")
                return False
                
            # ✅ Also verify we can get an access token (needed for generation)
            if not self.fetch_access_token():
                print(f"  ❌ Cookie check failed: Unable to fetch access token.")
                return False
                
            return True
        except Exception as e:
            print(f"  ❌ Cookie check error: {e}")
            return False

    def _handle_error_with_reset_logic(self, status_code: int, error_msg: str) -> bool:
        """
        Xử lý lỗi 400, 401, 429, 403:
        - Tăng counter lỗi cho cookie hiện tại.
        - Nếu >= 6 lần liên tiếp: HARD RESET driver.
        - Trả về True nếu nên retry (sau reset), False nếu chưa đến ngưỡng hoặc lỗi khác.
        """
        # Chỉ xử lý các lỗi HTTP cụ thể
        target_codes = [400, 401, 403, 429]
        if status_code not in target_codes:
            return False

        cookie_hash = self._cookie_hash
        
        # Init counter dict nếu chưa có (dùng chung cho mọi lỗi cần reset)
        if not hasattr(LabsFlowClient, '_shared_error_reset_counters'):
            LabsFlowClient._shared_error_reset_counters = {}
            
        # Key unique cho cookie + loại lỗi (hoặc gộp chung nếu muốn reset cho bất kỳ lỗi nào)
        # Ở đây gộp chung: nếu 1 cookie gặp lỗi liên tiếp (bất kể 400 hay 429) -> reset.
        current_count = LabsFlowClient._shared_error_reset_counters.get(cookie_hash, 0) + 1
        LabsFlowClient._shared_error_reset_counters[cookie_hash] = current_count
        
        print(f"  ⚠️ Gặp lỗi {status_code} (lần thứ {current_count}) cho cookie {cookie_hash[:8]}...")
        
        if current_count >= 6:
            print(f"  🚨 Đã đạt ngưỡng 6 lần lỗi liên tiếp ({status_code}). Thực hiện HARD RESET driver...")
            self._hard_reset_driver(cookie_hash)
            
            # Reset counter sau khi hard reset
            LabsFlowClient._shared_error_reset_counters[cookie_hash] = 0
            return True # Signal caller to retry with fresh driver
            
        return False

    def _handle_403_recaptcha_error(
        self,
        payload: Dict[str, Any],
        attempt: int,
        max_retries: int,
        recaptcha_action: str = "VIDEO_GENERATION",  # ✅ Thêm parameter action
    ) -> bool:
        """
        Xử lý 403 reCAPTCHA error:
        - Vẫn dùng logic retry, nhưng đếm số lần.
        - Nếu đạt ngưỡng: Reset BrowserContext (lấy cookie mới nếu có profile).
        """
        if attempt >= max_retries - 1:
            return False
        
        cookie_hash = self._cookie_hash
        
        # ✅ FIX: Reset token timestamp để buộc lấy token hoàn toàn mới
        LabsFlowClient._token_timestamps.pop(cookie_hash, None)
        
        # ✅ Dùng unified error handler để kiểm tra và reset nếu cần
        should_retry_fresh = self._handle_error_and_maybe_reset(403, "403 Forbidden/Captcha")
        
        if should_retry_fresh:
            print(f"  🔄 Đã reset BrowserContext do lỗi 403 liên tiếp. Retry với context mới...")
        else:
            # Chưa đến ngưỡng reset: Retry bình thường
            count_403 = LabsFlowClient._shared_403_counters.get(cookie_hash, 0) if hasattr(LabsFlowClient, '_shared_403_counters') else 0
            print(f"  → Chưa đạt ngưỡng reset (403 count: {count_403}/{self.MAX_403_BEFORE_RESET}). Retry nhẹ...")

        # Retry lấy token (sẽ dùng context cũ hoặc mới tạo tùy logic trên)
        try:
            self._maybe_inject_recaptcha(
                payload["clientContext"],
                raise_on_fail=True,
                acquire_lock=False,
                recaptcha_action=recaptcha_action,  # ✅ Truyền action
            )
            time.sleep(0.1)
            if not self._verify_token_before_api_call(payload):
                return False
            print(f"  → Retry với token mới (attempt {attempt + 2}/{max_retries})...")
            return True
        except RuntimeError as e:
            print(f"  ✗ Không thể lấy token mới: {e}")
            return False

    def _reset_403_counter_for_cookie(self):
        """Reset counter lỗi cho cookie này (gọi khi thành công)."""
        # ✅ Dùng function mới để reset tất cả counters
        self._reset_all_error_counters()

    @staticmethod
    def _parse_google_error_details(response_text: str) -> Dict[str, Any]:
        """Parse detailed error information from Google API error response.
        
        Trích xuất thông tin lỗi chi tiết từ Google API response để xác định:
        - Error reason (vd: RECAPTCHA_INVALID, TOKEN_EXPIRED, etc.)
        - Error domain
        - Details field
        
        Args:
            response_text: JSON response text from Google API
            
        Returns:
            Dict với các thông tin: 'reason', 'domain', 'details', 'is_recaptcha_error'
        """
        import json
        result = {
            'reason': None,
            'domain': None,
            'details': [],
            'is_recaptcha_error': False,
            'raw_error': None,
        }
        
        try:
            error_data = json.loads(response_text)
            result['raw_error'] = error_data
            
            # Google API error format
            if 'error' in error_data:
                error_info = error_data['error']
                result['domain'] = error_info.get('domain')
                
                # Trích xuất reason trực tiếp
                result['reason'] = error_info.get('reason')
                
                # Trích xuất từ details array
                details = error_info.get('details', [])
                for detail in details:
                    if isinstance(detail, dict):
                        # Lấy reason từ detail
                        if not result['reason'] and detail.get('reason'):
                            result['reason'] = detail.get('reason')
                        
                        # Trích xuất metadata
                        if '@type' in detail:
                            detail_type = detail['@type']
                            result['details'].append({
                                'type': detail_type,
                                'reason': detail.get('reason'),
                                'metadata': detail.get('metadata', {}),
                            })
                            
                            # Kiểm tra recaptcha error
                            if 'Recaptcha' in detail_type or 'recaptcha' in detail_type.lower():
                                result['is_recaptcha_error'] = True
                            if detail.get('reason') in ['RECAPTCHA_INVALID', 'RECAPTCHA_UNAVAILABLE', 'TOKEN_INVALID']:
                                result['is_recaptcha_error'] = True
                                
        except json.JSONDecodeError:
            # Không phải JSON, có thể là text thường
            response_lower = response_text.lower()
            if 'recaptcha' in response_lower or 'captcha' in response_lower:
                result['is_recaptcha_error'] = True
                result['reason'] = 'RECAPTCHA_ERROR'
            if '403' in response_text:
                result['reason'] = 'FORBIDDEN'
                
        return result

    @staticmethod
    def _get_cookie_hash(cookies: Dict[str, str]) -> str:
        """Generate a hash for cookie dict to identify unique cookies."""
        import hashlib
        # Sort cookies by key for consistent hashing
        sorted_items = sorted(cookies.items())
        # ✅ Dùng toàn bộ giá trị cookie (không truncate) để đảm bảo hash chính xác
        cookie_str = "|".join(f"{k}={v}" for k, v in sorted_items)
        # ✅ Dùng MD5 để đảm bảo hash unique và consistent
        hash_obj = hashlib.md5(cookie_str.encode('utf-8'))
        return hash_obj.hexdigest()[:12]  # 12-character hex hash
    
    # ============================================================================
    # ✅ UNIFIED ERROR HANDLING & BROWSER CONTEXT RESET
    # ============================================================================
    
    # Ngưỡng để reset BrowserContext
    MAX_403_BEFORE_RESET = 3  # Số lần 403 liên tiếp trước khi reset
    MAX_429_BEFORE_RESET = 3  # Số lần 429 liên tiếp trước khi reset
    MAX_TOTAL_ERRORS_BEFORE_RESET = 5  # Tổng lỗi trước khi reset
    DEFAULT_MAX_RETRIES = 3  # Retry mặc định cho mỗi request
    
    def _increment_error_counter(self, error_code: int) -> Tuple[int, int, int]:
        """
        Tăng counter lỗi cho cookie này.
        
        Returns:
            (count_403, count_429, total_errors)
        """
        cookie_hash = self._cookie_hash
        
        # Khởi tạo counters nếu chưa có
        if not hasattr(LabsFlowClient, '_shared_403_counters'):
            LabsFlowClient._shared_403_counters = {}
        if not hasattr(LabsFlowClient, '_shared_429_counters'):
            LabsFlowClient._shared_429_counters = {}
        if not hasattr(LabsFlowClient, '_shared_total_error_counters'):
            LabsFlowClient._shared_total_error_counters = {}
        
        # Tăng counter tương ứng
        if error_code == 403:
            LabsFlowClient._shared_403_counters[cookie_hash] = LabsFlowClient._shared_403_counters.get(cookie_hash, 0) + 1
        elif error_code == 429:
            LabsFlowClient._shared_429_counters[cookie_hash] = LabsFlowClient._shared_429_counters.get(cookie_hash, 0) + 1
        
        # Tăng tổng lỗi
        LabsFlowClient._shared_total_error_counters[cookie_hash] = LabsFlowClient._shared_total_error_counters.get(cookie_hash, 0) + 1
        
        return (
            LabsFlowClient._shared_403_counters.get(cookie_hash, 0),
            LabsFlowClient._shared_429_counters.get(cookie_hash, 0),
            LabsFlowClient._shared_total_error_counters.get(cookie_hash, 0),
        )
    
    def _reset_all_error_counters(self):
        """Reset tất cả counter lỗi cho cookie này (gọi khi thành công hoặc sau reset context)."""
        cookie_hash = self._cookie_hash
        
        if hasattr(LabsFlowClient, '_shared_403_counters'):
            LabsFlowClient._shared_403_counters[cookie_hash] = 0
        if hasattr(LabsFlowClient, '_shared_429_counters'):
            LabsFlowClient._shared_429_counters[cookie_hash] = 0
        if hasattr(LabsFlowClient, '_shared_total_error_counters'):
            LabsFlowClient._shared_total_error_counters[cookie_hash] = 0
        if hasattr(LabsFlowClient, '_shared_error_reset_counters'):
            LabsFlowClient._shared_error_reset_counters[cookie_hash] = 0
        # ✅ Reset proxy usage flag khi thành công
        if hasattr(LabsFlowClient, '_cookie_using_proxy'):
            LabsFlowClient._cookie_using_proxy[cookie_hash] = False
        # ✅ Xóa proxy khỏi session khi thành công (trở về direct connection)
        self._remove_proxy_from_session()
    
    def _should_reset_browser_context(self) -> bool:
        """
        Kiểm tra xem có nên reset BrowserContext không dựa trên số lỗi.
        
        Returns:
            True nếu nên reset
        """
        cookie_hash = self._cookie_hash
        
        count_403 = LabsFlowClient._shared_403_counters.get(cookie_hash, 0) if hasattr(LabsFlowClient, '_shared_403_counters') else 0
        count_429 = LabsFlowClient._shared_429_counters.get(cookie_hash, 0) if hasattr(LabsFlowClient, '_shared_429_counters') else 0
        total_errors = LabsFlowClient._shared_total_error_counters.get(cookie_hash, 0) if hasattr(LabsFlowClient, '_shared_total_error_counters') else 0
        
        if count_403 >= self.MAX_403_BEFORE_RESET:
            print(f"  ⚠️ Cookie {cookie_hash[:8]}... đã gặp {count_403} lỗi 403 liên tiếp → Cần reset BrowserContext")
            return True
        if count_429 >= self.MAX_429_BEFORE_RESET:
            print(f"  ⚠️ Cookie {cookie_hash[:8]}... đã gặp {count_429} lỗi 429 liên tiếp → Cần reset BrowserContext")
            return True
        if total_errors >= self.MAX_TOTAL_ERRORS_BEFORE_RESET:
            print(f"  ⚠️ Cookie {cookie_hash[:8]}... đã gặp {total_errors} lỗi tổng cộng → Cần reset BrowserContext")
            return True
        
        return False
    
    def _apply_proxy_to_session(self, proxy: Dict[str, str]) -> None:
        """
        Áp dụng proxy vào requests.Session cho API HTTP calls.
        
        Hỗ trợ tất cả loại proxy:
        - HTTP/HTTPS proxy: http://host:port
        - SOCKS5 proxy: socks5://host:port (WARP, Tor)
        - Proxy với auth: user:pass@host:port
        
        ✅ QUAN TRỌNG: Proxy chỉ dùng cho API calls (requests.post/get),
        KHÔNG dùng cho BrowserContext (Playwright navigation).
        
        Args:
            proxy: Dict với server, username, password
                   HOẶC ProxyConfig dict với proxy_type, static_server, etc.
        """
        try:
            # Hỗ trợ cả ProxyConfig dict (proxy_type) và legacy dict (server)
            proxy_type = proxy.get("proxy_type", "")
            
            if proxy_type and proxy_type != "none":
                # New ProxyConfig format
                from proxy_manager import ProxyConfig
                config = ProxyConfig.from_dict(proxy)
                entry = config.get_active_proxy()
                if not entry:
                    print(f"  ⚠️ [Proxy Session] ProxyConfig type={proxy_type} nhưng không có proxy active")
                    return
                proxies = entry.to_requests_proxy()
                self.session.proxies = proxies
                print(f"  🌐 [Proxy Session] Áp dụng {proxy_type} proxy: {entry.server}")
                if entry.username:
                    print(f"     → Auth: {entry.username[:30]}...")
                return
            
            # Legacy format: {server, username, password}
            proxy_server = proxy.get("server", "")
            username = proxy.get("username", "")
            password = proxy.get("password", "")
            
            if not proxy_server:
                print(f"  ⚠️ [Proxy Session] Proxy server rỗng, bỏ qua")
                return
            
            # Build proxy URL với auth
            from urllib.parse import quote
            parts = proxy_server
            if username and password:
                if "://" in parts:
                    scheme, rest = parts.split("://", 1)
                else:
                    scheme, rest = "http", parts
                proxy_url = f"{scheme}://{quote(username)}:{quote(password)}@{rest}"
            else:
                proxy_url = parts if "://" in parts else f"http://{parts}"
            
            self.session.proxies = {
                "http": proxy_url,
                "https": proxy_url,
            }
            
            print(f"  🌐 [Proxy Session] Đã áp dụng proxy cho API calls: {proxy_server}")
            if username:
                print(f"     → Auth: {username[:30]}...")
        except Exception as e:
            print(f"  ⚠️ [Proxy Session] Lỗi áp dụng proxy: {e}")
    
    def _remove_proxy_from_session(self) -> None:
        """Xóa proxy khỏi requests.Session (trở về direct connection)."""
        if hasattr(self.session, 'proxies') and self.session.proxies:
            self.session.proxies = {}
            print(f"  🔄 [Proxy Session] Đã xóa proxy, trở về direct connection")
    
    def _get_cookie_source(self) -> str:
        """Lấy source của cookie: 'profile' hoặc 'import'."""
        if hasattr(LabsFlowClient, '_cookie_sources'):
            return LabsFlowClient._cookie_sources.get(self._cookie_hash, "import")
        return "import"
    
    def _get_profile_path_for_cookie(self) -> Optional[str]:
        """Lấy profile path cho cookie này (nếu có)."""
        if hasattr(LabsFlowClient, '_cookie_profile_paths'):
            return LabsFlowClient._cookie_profile_paths.get(self._cookie_hash)
        return self.profile_path
    
    def _refresh_cookies_from_profile(self) -> Optional[Dict[str, str]]:
        """
        Lấy cookie mới từ profile đã đăng nhập (chỉ cho cookie có profile).
        
        Returns:
            Dict cookie mới hoặc None nếu không thể lấy
        """
        cookie_hash = self._cookie_hash
        profile_path = self._get_profile_path_for_cookie()
        
        if not profile_path:
            print(f"  ⚠️ Cookie {cookie_hash[:8]}... không có profile path, không thể lấy cookie mới")
            return None
        
        if self._get_cookie_source() != "profile":
            print(f"  ⚠️ Cookie {cookie_hash[:8]}... là import trực tiếp, không thể lấy cookie mới từ profile")
            return None
        
        print(f"  🔄 [Cookie Refresh] Đang lấy cookie mới từ profile: {profile_path}")
        
        try:
            from playwright.sync_api import sync_playwright
            from pathlib import Path
            
            profile_dir = Path(profile_path)
            if not profile_dir.exists():
                print(f"  ❌ Profile path không tồn tại: {profile_path}")
                return None
            
            with sync_playwright() as p:
                # Mở profile bằng persistent context (headless)
                context = p.chromium.launch_persistent_context(
                    user_data_dir=str(profile_dir),
                    headless=False,
                    channel="chrome",
                    args=[
                        '--no-first-run',
                        '--no-default-browser-check',
                    ],
                )
                
                try:
                    # Mở trang Google Labs để lấy cookies
                    page = context.new_page()
                    page.goto("https://labs.google/fx/tools/flow", wait_until="domcontentloaded", timeout=30000)
                    
                    # Đợi một chút để cookies được load
                    time.sleep(2)
                    
                    # Lấy tất cả cookies
                    all_cookies = context.cookies()
                    
                    # Filter cookies cho domain google
                    google_cookies: Dict[str, str] = {}
                    for cookie in all_cookies:
                        domain = cookie.get("domain", "")
                        if "google" in domain:
                            google_cookies[cookie["name"]] = cookie["value"]
                    
                    page.close()
                    
                    if google_cookies:
                        print(f"  ✅ [Cookie Refresh] Đã lấy {len(google_cookies)} cookies mới từ profile")
                        return google_cookies
                    else:
                        print(f"  ⚠️ [Cookie Refresh] Không tìm thấy cookies Google trong profile")
                        return None
                        
                finally:
                    context.close()
                    
        except Exception as e:
            print(f"  ❌ [Cookie Refresh] Lỗi khi lấy cookie từ profile: {e}")
            import traceback
            print(traceback.format_exc())
            return None
    
    def _auto_renew_cookies_on_403(self) -> bool:
        """Tự động lấy cookie mới khi bị 403 - KHÔNG cần user thao tác.
        
        Flow ưu tiên (hỗ trợ proxy per-account):
        1. Thử lấy cookie mới từ profile (headless, nhanh nhất)
        2. Nếu fail → thử headless re-login với email/password từ DB
        3. Áp dụng proxy per-account (nếu có) vào session
        4. Update cookies mới vào instance, DB, và tất cả browser contexts
        5. Reset tất cả error counters và token timestamps
        
        Returns:
            True nếu lấy được cookie mới, False nếu thất bại
        """
        cookie_hash = self._cookie_hash
        
        # Tránh nhiều thread cùng renew cho 1 cookie
        with LabsFlowClient._cookie_auto_renew_lock:
            if LabsFlowClient._cookie_renewing.get(cookie_hash, False):
                print(f"  ⏳ [Auto Renew] Cookie {cookie_hash[:8]}... đang được renew bởi thread khác, đợi...")
                for _ in range(60):
                    time.sleep(1)
                    if not LabsFlowClient._cookie_renewing.get(cookie_hash, False):
                        if self.cookies != self._original_cookies_before_renew:
                            print(f"  ✅ [Auto Renew] Cookie đã được renew bởi thread khác")
                            return True
                        break
                return False
            LabsFlowClient._cookie_renewing[cookie_hash] = True
        
        self._original_cookies_before_renew = dict(self.cookies)
        
        try:
            new_cookies = None
            account_info = LabsFlowClient._cookie_account_info.get(cookie_hash)
            profile_path = self._get_profile_path_for_cookie()
            
            # ═══════════════════════════════════════════════════════════════
            # BƯỚC 0: Clear session cũ + áp dụng proxy per-account
            # ═══════════════════════════════════════════════════════════════
            print(f"  🧹 [Auto Renew] Clear session cũ...")
            self._remove_proxy_from_session()
            self.session.cookies.clear()
            
            # Load proxy config per-account
            account_proxy_config = None
            if account_info and account_info.get("email"):
                try:
                    from cookiauto import db_get_account_proxy_config
                    account_proxy_config = db_get_account_proxy_config(account_info["email"])
                    if account_proxy_config:
                        proxy_type = account_proxy_config.get("proxy_type", account_proxy_config.get("server", ""))
                        print(f"  🌐 [Auto Renew] Proxy per-account: {proxy_type}")
                except Exception:
                    pass
            
            # ═══════════════════════════════════════════════════════════════
            # BƯỚC 1: Thử lấy cookie mới từ profile (headless, nhanh nhất)
            # ═══════════════════════════════════════════════════════════════
            if profile_path and self._get_cookie_source() == "profile":
                print(f"  🔄 [Auto Renew] BƯỚC 1: Thử lấy cookie từ profile (headless)...")
                new_cookies = self._refresh_cookies_from_profile()
                
                if new_cookies and self._verify_new_cookies(new_cookies):
                    print(f"  ✅ [Auto Renew] BƯỚC 1 thành công - Lấy được cookie mới từ profile")
                    self._apply_new_cookies(new_cookies, cookie_hash, account_info)
                    # Áp dụng proxy per-account vào session
                    if account_proxy_config:
                        self._apply_proxy_to_session(account_proxy_config)
                        self.proxy_config = account_proxy_config
                    return True
                else:
                    print(f"  ⚠️ [Auto Renew] BƯỚC 1 thất bại - Cookie từ profile không hợp lệ hoặc đã hết hạn")
            
            # ═══════════════════════════════════════════════════════════════
            # BƯỚC 2: Headless re-login với email/password
            # ═══════════════════════════════════════════════════════════════
            if account_info and account_info.get("email") and account_info.get("password"):
                email = account_info["email"]
                password = account_info["password"]
                acc_profile = account_info.get("profile_path", profile_path or "")
                
                print(f"  🔄 [Auto Renew] BƯỚC 2: Headless re-login cho {email}...")
                new_cookies = self._headless_relogin(email, password, acc_profile)
                
                if new_cookies and self._verify_new_cookies(new_cookies):
                    print(f"  ✅ [Auto Renew] BƯỚC 2 thành công - Đã re-login và lấy cookie mới cho {email}")
                    self._apply_new_cookies(new_cookies, cookie_hash, account_info)
                    # Áp dụng proxy per-account vào session
                    if account_proxy_config:
                        self._apply_proxy_to_session(account_proxy_config)
                        self.proxy_config = account_proxy_config
                    return True
                else:
                    print(f"  ⚠️ [Auto Renew] BƯỚC 2 thất bại - Không thể re-login cho {email}")
            else:
                print(f"  ⚠️ [Auto Renew] Không có account info (email/password) cho cookie {cookie_hash[:8]}...")
                print(f"  💡 Tip: Đăng ký account info bằng LabsFlowClient.register_account_info()")
            
            # ═══════════════════════════════════════════════════════════════
            # BƯỚC 3: Fallback - thử callback renew (nếu có)
            # ═══════════════════════════════════════════════════════════════
            callback = LabsFlowClient._recaptcha_renew_cookie_callbacks.get(cookie_hash)
            if callback:
                print(f"  🔄 [Auto Renew] BƯỚC 3: Thử callback renew...")
                try:
                    new_cookies = callback(cookie_hash, self.cookies)
                    if new_cookies and isinstance(new_cookies, dict) and len(new_cookies) > 0:
                        print(f"  ✅ [Auto Renew] BƯỚC 3 thành công - Callback trả về cookie mới")
                        self._apply_new_cookies(new_cookies, cookie_hash, account_info)
                        if account_proxy_config:
                            self._apply_proxy_to_session(account_proxy_config)
                            self.proxy_config = account_proxy_config
                        return True
                except Exception as e:
                    print(f"  ⚠️ [Auto Renew] BƯỚC 3 thất bại: {e}")
            
            print(f"  ❌ [Auto Renew] Tất cả các bước đều thất bại cho cookie {cookie_hash[:8]}...")
            return False
            
        except Exception as e:
            print(f"  ❌ [Auto Renew] Lỗi không mong đợi: {e}")
            import traceback
            print(traceback.format_exc())
            return False
        finally:
            with LabsFlowClient._cookie_auto_renew_lock:
                LabsFlowClient._cookie_renewing[cookie_hash] = False
    
    def _verify_new_cookies(self, new_cookies: Dict[str, str]) -> bool:
        """Kiểm tra cookies mới có hợp lệ không bằng cách thử fetch access token.
        
        Returns:
            True nếu cookies hợp lệ (fetch AT thành công)
        """
        try:
            import requests
            session = requests.Session()
            url = "https://labs.google/fx/api/auth/session"
            headers = self._labs_headers()
            
            resp = session.get(url, headers=headers, cookies=new_cookies, timeout=30)
            if resp.status_code == 200:
                data = resp.json()
                token = data.get("access_token") if isinstance(data, dict) else None
                if token:
                    print(f"  ✅ [Verify] Cookie mới hợp lệ - AT: {token[:20]}...")
                    return True
                else:
                    print(f"  ⚠️ [Verify] Cookie mới không có access_token trong response")
                    return False
            else:
                print(f"  ⚠️ [Verify] Cookie mới trả về status {resp.status_code}")
                return False
        except Exception as e:
            print(f"  ⚠️ [Verify] Lỗi verify cookie: {e}")
            return False
    
    def _apply_new_cookies(self, new_cookies: Dict[str, str], cookie_hash: str, account_info: Optional[Dict[str, str]] = None):
        """Áp dụng cookies mới vào tất cả các nơi cần thiết.
        
        1. Update instance cookies
        2. Update session cookies
        3. Fetch access token mới
        4. Reset tất cả browser contexts
        5. Reset error counters
        6. Update DB (nếu có account info)
        """
        # 1. Update instance
        self.cookies = new_cookies
        new_hash = self._get_cookie_hash(new_cookies)
        print(f"  🔄 [Apply] Cookie hash: {cookie_hash[:8]}... → {new_hash[:8]}...")
        
        # 2. Update session cookies
        self.session.cookies.clear()
        for name, value in new_cookies.items():
            self.session.cookies.set(name, value)
        
        # 3. Fetch access token mới
        try:
            if self.fetch_access_token():
                print(f"  ✅ [Apply] Đã fetch access token mới")
            else:
                print(f"  ⚠️ [Apply] Không fetch được access token mới")
        except Exception as e:
            print(f"  ⚠️ [Apply] Lỗi fetch AT: {e}")
        
        # 4. Reset browser contexts
        self._reset_browser_context_for_cookie(new_cookies)
        
        # Reset zendriver
        LabsFlowClient._zendriver_reset_page(cookie_hash)
        LabsFlowClient._zendriver_cookies_injected.pop(cookie_hash, None)
        
        # 5. Reset error counters
        LabsFlowClient._token_timestamps.pop(cookie_hash, None)
        self._reset_all_error_counters()
        LabsFlowClient._zendriver_consecutive_403[cookie_hash] = 0
        LabsFlowClient._playwright_consecutive_403[cookie_hash] = 0
        if hasattr(self, '_403_refresh_retries'):
            self._403_refresh_retries[cookie_hash] = 0
        
        # Clear blocked flag
        with LabsFlowClient._recaptcha_cookie_blocked_lock:
            if hasattr(LabsFlowClient, '_recaptcha_cookie_blocked_flags'):
                LabsFlowClient._recaptcha_cookie_blocked_flags[cookie_hash] = False
        
        # 6. Update DB (nếu có account info)
        if account_info and account_info.get("email"):
            try:
                from cookiauto import db_update_account_cookies
                import json
                # Chuyển dict → list of cookie objects (format DB)
                cookies_list = [{"name": k, "value": v, "domain": ".google.com"} for k, v in new_cookies.items()]
                db_update_account_cookies(account_info["email"], json.dumps(cookies_list))
                print(f"  ✅ [Apply] Đã update cookies mới vào DB cho {account_info['email']}")
            except Exception as e:
                print(f"  ⚠️ [Apply] Lỗi update DB: {e}")
        
        # 7. Đăng ký lại account info cho cookie_hash mới (nếu hash thay đổi)
        if new_hash != cookie_hash and account_info:
            LabsFlowClient._cookie_account_info[new_hash] = account_info
            # Giữ cả mapping cũ để tránh mất reference
        
        print(f"  ✅ [Apply] Đã áp dụng cookies mới hoàn tất")
    
    def _headless_relogin(self, email: str, password: str, profile_path: str) -> Optional[Dict[str, str]]:
        """Headless re-login để lấy cookie mới - KHÔNG cần user thao tác.
        
        Sử dụng Playwright persistent context với profile đã có để:
        1. Mở browser headless
        2. Navigate đến Google Labs
        3. Nếu cần login → tự động nhập email/password
        4. Lấy cookies mới
        
        Returns:
            Dict cookies mới hoặc None nếu thất bại
        """
        from pathlib import Path
        
        print(f"  🔐 [Headless Login] Bắt đầu re-login cho {email}...")
        
        try:
            from playwright.sync_api import sync_playwright
            
            profile_dir = Path(profile_path)
            profile_dir.mkdir(parents=True, exist_ok=True)
            
            with sync_playwright() as p:
                # Mở browser headless với profile
                context = p.chromium.launch_persistent_context(
                    user_data_dir=str(profile_dir),
                    headless=False,
                    channel="chrome",
                    args=[
                        '--no-first-run',
                        '--no-default-browser-check',
                        '--disable-blink-features=AutomationControlled',
                        '--disable-extensions',
                        '--disable-infobars',
                        '--disable-sync',
                    ],
                    viewport={"width": 1280, "height": 720},
                )
                
                try:
                    page = context.pages[0] if context.pages else context.new_page()
                    
                    # Navigate đến Google Labs
                    print(f"  🌍 [Headless Login] Đang vào Google Labs...")
                    try:
                        page.goto("https://labs.google/fx/tools/flow", wait_until="domcontentloaded", timeout=30000)
                    except Exception:
                        pass  # Timeout OK, tiếp tục
                    time.sleep(3)
                    
                    # Kiểm tra xem đã login chưa bằng cách check cookies
                    cookies = context.cookies()
                    session_cookies = [c for c in cookies if c.get("name") == "__Secure-next-auth.session-token" and "labs.google" in c.get("domain", "")]
                    
                    if session_cookies:
                        print(f"  ✅ [Headless Login] Đã có session token - không cần login lại")
                    else:
                        # Cần login - navigate đến Google signin
                        print(f"  🔐 [Headless Login] Chưa có session - bắt đầu login...")
                        
                        # Thử click Sign In trên Labs page
                        try:
                            page.evaluate("""
                                () => {
                                    const elements = document.querySelectorAll('button, a, [role="button"]');
                                    for (const el of elements) {
                                        const text = (el.innerText || '').toLowerCase();
                                        if (text.includes('sign in') || text.includes('đăng nhập')) {
                                            el.click(); return true;
                                        }
                                    }
                                    return false;
                                }
                            """)
                            time.sleep(3)
                        except Exception:
                            pass
                        
                        # Nếu vẫn chưa ở trang login, navigate trực tiếp
                        if "accounts.google.com" not in page.url:
                            page.goto("https://accounts.google.com/signin", wait_until="networkidle", timeout=30000)
                            time.sleep(3)
                        
                        # Kiểm tra nếu đã login sẵn (redirect về myaccount)
                        if "myaccount.google.com" in page.url or "accounts.google.com/b/" in page.url:
                            print(f"  ✅ [Headless Login] Đã login sẵn trong profile")
                        else:
                            # Nhập email
                            print(f"  📧 [Headless Login] Nhập email: {email}...")
                            try:
                                page.wait_for_selector('input[type="email"]', state="visible", timeout=10000)
                                time.sleep(0.5)
                                page.fill('input[type="email"]', email)
                                page.click("#identifierNext")
                                time.sleep(4)
                            except Exception as e:
                                print(f"  ⚠️ [Headless Login] Email step failed: {e}")
                                return None
                            
                            # Nhập password
                            print(f"  🔑 [Headless Login] Nhập password...")
                            try:
                                page.wait_for_selector('input[type="password"]', state="visible", timeout=15000)
                                time.sleep(1)
                                page.fill('input[type="password"]', password)
                                time.sleep(0.5)
                                page.click("#passwordNext")
                                time.sleep(5)
                            except Exception as e:
                                print(f"  ⚠️ [Headless Login] Password step failed: {e}")
                                return None
                            
                            # Kiểm tra captcha/2FA - nếu có thì fail (headless không giải được)
                            page_text = page.evaluate("() => document.body.innerText.toLowerCase()")
                            captcha_indicators = ["challenge", "captcha", "recaptcha", "verify", "unusual activity"]
                            if any(ind in page_text for ind in captcha_indicators):
                                print(f"  ⚠️ [Headless Login] Phát hiện captcha/2FA - không thể tự động giải")
                                print(f"  💡 Tip: Mở tool Cookie để login thủ công 1 lần, sau đó auto-renew sẽ hoạt động")
                                return None
                            
                            # Kiểm tra login thành công
                            if "myaccount.google.com" not in page.url and "accounts.google.com/b/" not in page.url:
                                # Đợi thêm
                                time.sleep(5)
                                if "myaccount.google.com" not in page.url and "accounts.google.com/b/" not in page.url:
                                    print(f"  ⚠️ [Headless Login] Login có thể chưa thành công, URL: {page.url[:80]}")
                                    # Vẫn tiếp tục thử lấy cookies
                        
                        # Sau khi login, navigate lại Labs để lấy session cookie
                        print(f"  🌍 [Headless Login] Navigate lại Labs để lấy session cookie...")
                        try:
                            page.goto("https://labs.google/fx/tools/flow", wait_until="domcontentloaded", timeout=30000)
                        except Exception:
                            pass
                        time.sleep(5)
                        
                        # Click "Create with Flow" nếu có
                        try:
                            page.evaluate("""
                                () => {
                                    const buttons = document.querySelectorAll('button, a, div[role="button"]');
                                    for (const btn of buttons) {
                                        const text = (btn.innerText || '').trim().toLowerCase();
                                        if (text.includes('create with flow') || text === 'create') {
                                            btn.click(); return true;
                                        }
                                    }
                                    return false;
                                }
                            """)
                            time.sleep(5)
                        except Exception:
                            pass
                    
                    # Lấy tất cả cookies
                    all_cookies = context.cookies()
                    
                    # Filter cookies cho Google domains
                    google_cookies: Dict[str, str] = {}
                    has_session = False
                    for cookie in all_cookies:
                        domain = cookie.get("domain", "")
                        if "google" in domain or "youtube" in domain:
                            google_cookies[cookie["name"]] = cookie["value"]
                            if cookie["name"] == "__Secure-next-auth.session-token":
                                has_session = True
                    
                    page.close()
                    
                    if has_session and google_cookies:
                        print(f"  ✅ [Headless Login] Đã lấy {len(google_cookies)} cookies (có session token)")
                        return google_cookies
                    elif google_cookies:
                        print(f"  ⚠️ [Headless Login] Có {len(google_cookies)} cookies nhưng KHÔNG có session token")
                        # Vẫn trả về cookies, _verify_new_cookies sẽ kiểm tra
                        return google_cookies
                    else:
                        print(f"  ❌ [Headless Login] Không lấy được cookies")
                        return None
                        
                finally:
                    try:
                        context.close()
                    except Exception:
                        pass
                    
        except Exception as e:
            print(f"  ❌ [Headless Login] Lỗi: {e}")
            import traceback
            print(traceback.format_exc())
            return None
    
    def _reset_browser_context_for_cookie(self, new_cookies: Optional[Dict[str, str]] = None) -> bool:
        """
        Reset BrowserContext cho cookie này:
        1. Đánh dấu flag để worker thread tự reset context
        2. Worker thread sẽ tự đóng context cũ và tạo context mới khi cần
        
        ⚠️ QUAN TRỌNG: Playwright sync API không cho phép gọi context.close() từ thread khác
        với thread đã tạo context. Vì vậy ta chỉ đánh dấu flag và để worker thread tự xử lý.
        
        Args:
            new_cookies: Cookie mới để inject (nếu None thì dùng cookie cũ)
        
        Returns:
            True nếu reset thành công
        """
        cookie_hash = self._cookie_hash
        cookies_to_use = new_cookies or self.cookies
        
        print(f"  🔄 [Context Reset] Đánh dấu reset BrowserContext cho cookie {cookie_hash[:8]}...")
        
        try:
            # ⚠️ KHÔNG xóa reference trực tiếp vì có thể gây race condition với worker thread
            # Chỉ đánh dấu flag để worker thread tự xử lý
            
            # 1. Đánh dấu flag cần reset (worker thread sẽ tự đóng và tạo mới)
            with LabsFlowClient._contexts_need_reset_lock:
                LabsFlowClient._contexts_need_reset[cookie_hash] = True
            
            # 2. Clear flag bị chặn
            with LabsFlowClient._recaptcha_cookie_blocked_lock:
                if hasattr(LabsFlowClient, '_recaptcha_cookie_blocked_flags'):
                    LabsFlowClient._recaptcha_cookie_blocked_flags[cookie_hash] = False
            
            # 3. Update cookies trong instance nếu có cookie mới
            if new_cookies:
                self.cookies = new_cookies
                print(f"  ✅ [Context Reset] Đã update cookies mới cho instance")
            
            # 4. Reset error counters (CHỈ counters, KHÔNG reset proxy flag)
            # ✅ Proxy flag được quản lý bởi _handle_error_and_maybe_reset
            cookie_hash_local = self._cookie_hash
            if hasattr(LabsFlowClient, '_shared_403_counters'):
                LabsFlowClient._shared_403_counters[cookie_hash_local] = 0
            if hasattr(LabsFlowClient, '_shared_429_counters'):
                LabsFlowClient._shared_429_counters[cookie_hash_local] = 0
            if hasattr(LabsFlowClient, '_shared_total_error_counters'):
                LabsFlowClient._shared_total_error_counters[cookie_hash_local] = 0
            
            # 5. Đợi một chút để worker thread có thời gian nhận flag
            time.sleep(0.2)
            
            print(f"  ✅ [Context Reset] Đã đánh dấu reset cho cookie {cookie_hash[:8]}...")
            print(f"  ℹ️ [Context Reset] Worker thread sẽ tự reset BrowserContext khi xử lý request tiếp theo")
            return True
            
        except Exception as e:
            print(f"  ❌ [Context Reset] Lỗi khi đánh dấu reset: {e}")
            import traceback
            print(traceback.format_exc())
            return False
    
    def _handle_error_and_maybe_reset(self, error_code: int, error_message: str) -> bool:
        """
        Xử lý lỗi 403 và reset BrowserContext nếu cần.
        
        SMART FLOW (hỗ trợ proxy per-account):
        1. Tăng counter lỗi
        2. Nếu đạt ngưỡng (3 lần 403 liên tiếp):
           a. Clear session + cookies cũ
           b. Lấy proxy config từ account (hỗ trợ static/rotating/warp/tor)
           c. Áp dụng proxy vào session
           d. Re-login lấy cookie mới (qua Chrome CDP với proxy)
           e. Reset BrowserContext
        
        Args:
            error_code: HTTP error code (400, 403, 429, 500)
            error_message: Error message để log
        
        Returns:
            True nếu đã reset context (nên retry), False nếu không reset
        """
        cookie_hash = self._cookie_hash
        
        # 1. Tăng counter lỗi
        count_403, count_429, total_errors = self._increment_error_counter(error_code)
        print(f"  📊 [Error Counter] Cookie {cookie_hash[:8]}...: 403={count_403}, 429={count_429}, total={total_errors}")
        
        # 2. Kiểm tra có cần reset không
        if not self._should_reset_browser_context():
            return False
        
        # ═══════════════════════════════════════════════════════════════════
        # ✅ SMART 403 HANDLING - Clear session + proxy + re-login
        # ═══════════════════════════════════════════════════════════════════
        
        print(f"  🔄 [Smart 403] Bắt đầu xử lý 403 cho cookie {cookie_hash[:8]}...")
        
        # Bước 1: Clear session hiện tại
        print(f"  🧹 [Smart 403] Clear session + cookies cũ...")
        self._remove_proxy_from_session()
        self.session.cookies.clear()
        
        # Bước 2: Lấy proxy config từ account (per-account proxy)
        account_info = LabsFlowClient._cookie_account_info.get(cookie_hash)
        account_proxy_config = None
        
        if account_info:
            email = account_info.get("email", "")
            if email:
                try:
                    from cookiauto import db_get_account_proxy_config
                    account_proxy_config = db_get_account_proxy_config(email)
                    if account_proxy_config:
                        print(f"  🌐 [Smart 403] Tìm thấy proxy config cho {email}: type={account_proxy_config.get('proxy_type', account_proxy_config.get('server', 'N/A'))}")
                except Exception as e:
                    print(f"  ⚠️ [Smart 403] Lỗi load proxy config: {e}")
        
        # Bước 3: Áp dụng proxy vào session (nếu có)
        should_use_proxy = False
        new_proxy = None
        
        if account_proxy_config:
            proxy_type = account_proxy_config.get("proxy_type", "")
            if proxy_type and proxy_type != "none":
                # New ProxyConfig format (static/rotating/warp/tor)
                self._apply_proxy_to_session(account_proxy_config)
                should_use_proxy = True
                new_proxy = account_proxy_config
                LabsFlowClient._cookie_using_proxy[cookie_hash] = True
            elif account_proxy_config.get("server"):
                # Legacy format
                self._apply_proxy_to_session(account_proxy_config)
                should_use_proxy = True
                new_proxy = account_proxy_config
                LabsFlowClient._cookie_using_proxy[cookie_hash] = True
        
        # Nếu không có per-account proxy → thử proxy pool
        if not should_use_proxy:
            is_using_proxy = LabsFlowClient._cookie_using_proxy.get(cookie_hash, False)
            if not is_using_proxy:
                print(f"  ℹ️ [Smart 403] Thử lại với cookie mới + NO PROXY trước")
                LabsFlowClient._cookie_using_proxy[cookie_hash] = True
            else:
                if LabsFlowClient._use_proxy_pool and LabsFlowClient._proxy_pool:
                    new_proxy = LabsFlowClient._rotate_proxy_for_cookie(cookie_hash)
                    if new_proxy:
                        should_use_proxy = True
                        self._apply_proxy_to_session(new_proxy)
                        print(f"  🌐 [Smart 403] Áp dụng proxy pool: {new_proxy.get('server', 'unknown')}")
                    else:
                        print(f"  ⚠️ [Smart 403] Không có proxy khả dụng trong pool")
                else:
                    print(f"  ⚠️ [Smart 403] Proxy pool rỗng hoặc đã tắt")
        
        # Bước 4: Lấy cookie mới (re-login với proxy nếu có)
        cookie_source = self._get_cookie_source()
        new_cookies = None
        
        if cookie_source == "profile":
            print(f"  🔄 [Smart 403] Đang lấy cookie mới từ profile...")
            new_cookies = self._refresh_cookies_from_profile()
            if new_cookies:
                print(f"  ✅ [Smart 403] Đã lấy được cookie mới từ profile")
            else:
                print(f"  ⚠️ [Smart 403] Không lấy được cookie mới từ profile")
                # Thử re-login
                if account_info and account_info.get("email") and account_info.get("password"):
                    email = account_info["email"]
                    password = account_info["password"]
                    profile_path = account_info.get("profile_path", self._get_profile_path_for_cookie() or "")
                    print(f"  🔐 [Smart 403] Thử re-login cho {email}...")
                    new_cookies = self._headless_relogin(email, password, profile_path)
                    if new_cookies:
                        print(f"  ✅ [Smart 403] Re-login thành công, có cookie mới")
                    else:
                        print(f"  ⚠️ [Smart 403] Re-login thất bại")
        else:
            print(f"  ℹ️ [Smart 403] Cookie là import, không thể lấy cookie mới")
        
        # Bước 5: Reset BrowserContext
        reset_success = self._reset_browser_context_for_cookie(new_cookies)
        
        if reset_success:
            if should_use_proxy and new_proxy:
                self.proxy_config = new_proxy
            else:
                self.proxy_config = None
                self._remove_proxy_from_session()
            
            proxy_info = "proxy ON" if should_use_proxy else "NO proxy"
            cookie_info = "cookie MỚI" if new_cookies else "cookie cũ"
            print(f"  ✅ [Smart 403] Đã reset: {cookie_info} + {proxy_info}, sẽ retry request")
            return True
        else:
            print(f"  ❌ [Smart 403] Không thể reset BrowserContext")
            return False
    
    def _labs_headers(self) -> Dict[str, str]:
        return {
            "accept": "*/*",
            "accept-language": _env("ACCEPT_LANGUAGE", "vi-VN,vi;q=0.9,fr-FR;q=0.8,fr;q=0.7,en-US;q=0.6,en;q=0.5"),
            "content-type": "application/json",
            "origin": "https://labs.google",
            "priority": "u=1, i",
            "referer": _env("LABS_REFERER", "https://labs.google/fx/tools/flow"),
            "sec-ch-ua": _env("SEC_CH_UA", '"Not;A=Brand";v="99", "Google Chrome";v="139", "Chromium";v="139"'),
            "sec-ch-ua-mobile": "?0",
            "sec-ch-ua-platform": _env("SEC_CH_UA_PLATFORM", '"Windows"'),
            "sec-fetch-dest": "empty",
            "sec-fetch-mode": "cors",
            "sec-fetch-site": "same-origin",
            "user-agent": self.user_agent,
        }
    
    def _aisandbox_headers(self) -> Dict[str, str]:
        if not self.access_token:
            raise ValueError("Missing access token for aisandbox API.")
        headers = {
            "accept": "*/*",
            "accept-language": _env("ACCEPT_LANGUAGE", "vi-VN,vi;q=0.9,fr-FR;q=0.8,fr;q=0.7,en-US;q=0.6,en;q=0.5"),
            "authorization": f"Bearer {self.access_token}",
            "content-type": "text/plain;charset=UTF-8",
            "origin": "https://labs.google",
            "priority": "u=1, i",
            "referer": "https://labs.google/",
            "sec-ch-ua": _env("SEC_CH_UA", '"Chromium";v="146", "Not-A.Brand";v="24", "Google Chrome";v="146"'),
            "sec-ch-ua-mobile": "?0",
            "sec-ch-ua-platform": _env("SEC_CH_UA_PLATFORM", '"Windows"'),
            "sec-fetch-dest": "empty",
            "sec-fetch-mode": "cors",
            "sec-fetch-site": "cross-site",
            "user-agent": _env("USER_AGENT", "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/146.0.0.0 Safari/537.36"),
            "x-browser-channel": _env("X_BROWSER_CHANNEL", "stable"),
            "x-browser-copyright": _env(
                "X_BROWSER_COPYRIGHT",
                "Copyright 2026 Google LLC. All Rights reserved.",
            ),
            "x-browser-year": _env("X_BROWSER_YEAR", "2026"),
            "x-client-data": _env("X_CLIENT_DATA", "CIq2yQEIprbJAQipncoBCNDiygEIlaHLAQiGoM0BGLGKzwEY57HPAQ=="),
            "x-browser-validation": "lYo6cDWNH/3Bt+JG4mYU+Q3kh6s=",
        }
        # Optional validation header if you have one
        x_validation = _env("X_BROWSER_VALIDATION")
        if x_validation:
            headers["x-browser-validation"] = x_validation
        return headers

    @classmethod
    def _get_cookie_lock(cls, cookie_hash: str) -> threading.Lock:
        """Lấy lock cho cookie - tạo mới nếu chưa có."""
        with cls._cookie_locks_lock:
            if cookie_hash not in cls._cookie_locks:
                cls._cookie_locks[cookie_hash] = threading.Lock()
            return cls._cookie_locks[cookie_hash]

    @contextmanager
    def _token_and_api_with_lock(self):
        """Context manager để giữ lock liên tục từ khi request token đến khi gọi API xong.
        Đảm bảo mỗi cookie chỉ xử lý 1 prompt tại một thời điểm (nối đuôi hoàn toàn).
        Lock được giữ trong suốt quá trình: request token -> đợi token -> inject token -> gọi API.
        ✅ CÁC COOKIE KHÁC NHAU CÓ THỂ CHẠY SONG SONG - chỉ lock trong cùng cookie.
        """
        cookie_hash = self._cookie_hash
        cookie_lock = self._get_cookie_lock(cookie_hash)
        
        # ✅ Lock giữa các prompt trong cùng cookie - giữ lock liên tục từ đầu đến cuối
        # ✅ KHÔNG còn global lock - các cookie khác nhau có thể chạy song song
        with cookie_lock:
            # ✅ Lock được giữ trong suốt quá trình (yield để code bên ngoài chạy)
            yield

    @contextmanager
    def _api_call_with_lock(self):
        """Context manager để giữ lock trong suốt quá trình gọi API.
        Đảm bảo mỗi cookie chỉ gọi 1 API tại một thời điểm (nối đuôi).
        ✅ CÁC COOKIE KHÁC NHAU CÓ THỂ CHẠY SONG SONG - chỉ lock trong cùng cookie.
        """
        cookie_hash = self._cookie_hash
        cookie_lock = self._get_cookie_lock(cookie_hash)
        
        # ✅ Lock giữa các prompt trong cùng cookie - giữ lock trong suốt quá trình gọi API
        # ✅ KHÔNG còn global lock - các cookie khác nhau có thể chạy song song
        with cookie_lock:
            # ✅ Rate limiting per cookie (instance-level)
            current_time = time.time()
            elapsed = current_time - self._last_api_call_time
            
            # ✅ Warm-up delay cho 10 request đầu tiên của cookie này (tránh 403 khi khởi động)
            if self._api_call_count < 10:
                warmup_delay = 2.0  # 2 giây cho các request đầu tiên
                total_wait = warmup_delay
                if elapsed < self._min_api_call_interval:
                    total_wait = max(warmup_delay, self._min_api_call_interval - elapsed)
                if total_wait > 0:
                    print(f"  ⏳ Warm-up delay {total_wait:.1f}s (cookie: {cookie_hash[:8]}..., request #{self._api_call_count + 1})...")
                    time.sleep(total_wait)
                self._api_call_count += 1
            else:
                # Sau 10 request đầu tiên, chỉ dùng interval bình thường
                if elapsed < self._min_api_call_interval:
                    wait_time = self._min_api_call_interval - elapsed
                    time.sleep(wait_time)
                self._api_call_count += 1
            
            self._last_api_call_time = time.time()
            
            # ✅ Lock được giữ trong suốt quá trình gọi API (yield)
            yield

    def _rate_limit_api_call(self):
        """Đơn giản hoá rate limiting cho các API khác nhau để tránh 429/HIGH_TRAFFIC.

        ✅ Rate limiting per cookie (instance-level) - mỗi cookie có rate limit riêng.
        Các cookie khác nhau có thể chạy song song với rate limit độc lập.
        Hàm này KHÔNG giữ cookie_lock, chỉ rate limit cho cookie hiện tại.
        """
        current_time = time.time()
        elapsed = current_time - self._last_api_call_time

        # Warm-up cho ~10 request đầu của cookie này: delay dài hơn một chút
        if self._api_call_count < 10:
            warmup_delay = 0.3  # đã giảm so với version cũ để nhanh hơn
            total_wait = warmup_delay
            if elapsed < self._min_api_call_interval:
                total_wait = max(warmup_delay, self._min_api_call_interval - elapsed)
            if total_wait > 0:
                print(f"  ⏳ Rate limit delay {total_wait:.2f}s (cookie: {self._cookie_hash[:8]}..., request #{self._api_call_count + 1})...")
                time.sleep(total_wait)
            self._api_call_count += 1
        else:
            # Sau khi qua warm-up, chỉ đảm bảo khoảng cách tối thiểu
            if elapsed < self._min_api_call_interval:
                wait_time = self._min_api_call_interval - elapsed
                time.sleep(wait_time)
            self._api_call_count += 1

        self._last_api_call_time = time.time()

    # region reCAPTCHA Playwright Worker Thread Architecture: ĐƠN GIẢN HÓA
    @classmethod
    def _ensure_recaptcha_worker(cls):
        """Đảm bảo reCAPTCHA worker thread đã được khởi động."""
        if cls._recaptcha_worker_started:
            return
        
        with cls._browser_lock:
            # Double-check sau khi acquire lock
            if cls._recaptcha_worker_started:
                return
            
            cls._recaptcha_worker_started = True
            
            def worker_loop():
                """Worker thread loop: xử lý reCAPTCHA requests từ queue."""
                print("  🚀 [reCAPTCHA Worker] Khởi động worker thread...")
                
                # Khởi tạo Browser mới (không dùng profile Chrome đang chạy)
                try:
                    from playwright.sync_api import sync_playwright
                    import os
                    import platform
                    
                    playwright = sync_playwright().start()
                    
                    # Lấy headless mode từ class variable
                    headless = cls._global_headless_mode
                    
                    launch_args = [
                        '--no-sandbox',
                        '--disable-dev-shm-usage',
                    ]
                    
                    # ✅ Nếu không headless: đặt cửa sổ off-screen để tránh hiện popup trắng
                    # Tất cả browser contexts sẽ chồng lên nhau cùng vị trí
                    if not headless:
                        launch_args.extend([
                            '--window-position=-3000,-3000',  # Off-screen để không hiện popup trắng
                            '--window-size=200,150',          # Kích thước nhỏ nhất
                        ])
                    
                    # ✅ Tìm Chrome đã cài trên máy (fallback nếu Playwright browsers chưa cài)
                    chrome_path = None
                    if platform.system() == "Windows":
                        possible_paths = [
                            os.path.expandvars(r"%ProgramFiles%\Google\Chrome\Application\chrome.exe"),
                            os.path.expandvars(r"%ProgramFiles(x86)%\Google\Chrome\Application\chrome.exe"),
                            os.path.expandvars(r"%LocalAppData%\Google\Chrome\Application\chrome.exe"),
                        ]
                        for p in possible_paths:
                            if os.path.exists(p):
                                chrome_path = p
                                break
                    elif platform.system() == "Darwin":  # macOS
                        mac_chrome = "/Applications/Google Chrome.app/Contents/MacOS/Google Chrome"
                        if os.path.exists(mac_chrome):
                            chrome_path = mac_chrome
                    
                    mode_str = "HEADLESS" if headless else "VISIBLE (góc trên trái, 200x150)"
                    
                    # ✅ Thử launch với Chrome có sẵn trước, fallback sang Chromium
                    browser = None
                    if chrome_path:
                        try:
                            print(f"  → Mở Chrome có sẵn ({mode_str})...")
                            browser = playwright.chromium.launch(
                                headless=headless,
                                executable_path=chrome_path,
                                args=launch_args,
                            )
                            print(f"  ✅ Chrome đã khởi tạo ({mode_str})")
                        except Exception as chrome_err:
                            print(f"  ⚠️ Không thể dùng Chrome: {chrome_err}")
                            browser = None
                    
                    # Fallback sang Chromium của Playwright
                    if not browser:
                        try:
                            print(f"  → Mở Chromium Playwright ({mode_str})...")
                            browser = playwright.chromium.launch(
                                headless=headless,
                                args=launch_args,
                            )
                            print(f"  ✅ Chromium đã khởi tạo ({mode_str})")
                        except Exception as chromium_err:
                            print(f"  ✗ Không thể mở Chromium: {chromium_err}")
                            print(f"  💡 Hãy chạy: playwright install chromium")
                            raise chromium_err
                    
                    cls._recaptcha_worker_browser = browser
                    cls._recaptcha_playwright = playwright
                    
                except Exception as e:
                    print(f"  ✗ Lỗi khởi tạo Browser: {e}")
                    import traceback
                    traceback.print_exc()
                    cls._recaptcha_worker_browser = None
                    cls._recaptcha_worker_started = False
                    return
                
                # Worker loop: xử lý requests từ queue
                while True:
                    try:
                        # Đợi request từ queue (blocking)
                        request_id, payload = cls._recaptcha_request_queue.get()
                        
                        # Exit signal
                        if request_id is None:
                            print("  🛑 [reCAPTCHA Worker] Nhận exit signal, đóng worker...")
                            break
                        
                        cookie_hash = payload.get("cookie_hash", "")
                        
                        # ✅ Kiểm tra flag reset context trước khi xử lý
                        need_reset = False
                        with cls._contexts_need_reset_lock:
                            need_reset = cls._contexts_need_reset.get(cookie_hash, False)
                            if need_reset:
                                cls._contexts_need_reset[cookie_hash] = False  # Clear flag
                        
                        if need_reset:
                            print(f"  🔄 [reCAPTCHA Worker] Cookie {cookie_hash[:8]}... cần reset context...")
                            # Đóng page cũ nếu có
                            if hasattr(cls, '_recaptcha_page') and cls._recaptcha_page:
                                try:
                                    cls._recaptcha_page.close()
                                except:
                                    pass
                                cls._recaptcha_page = None
                            # ✅ Đóng context cũ để tránh hiện popup trắng khi tạo lại
                            if hasattr(cls, '_recaptcha_context') and cls._recaptcha_context:
                                try:
                                    cls._recaptcha_context.close()
                                except:
                                    pass
                                cls._recaptcha_context = None
                            print(f"  ✅ [reCAPTCHA Worker] Đã reset context cho cookie {cookie_hash[:8]}...")
                        
                        # Xử lý request - ĐƠN GIẢN HÓA
                        token = None
                        error = None
                        
                        try:
                            token = cls._get_recaptcha_token_with_playwright_worker(
                                browser=cls._recaptcha_worker_browser,
                                cookie_hash=payload["cookie_hash"],
                                cookies=payload["cookies"],
                                proxy_config=payload.get("proxy_config"),
                                user_agent=payload.get("user_agent", ""),
                                timeout_s=payload.get("timeout_s", 90),
                                recaptcha_action=payload.get("recaptcha_action", "VIDEO_GENERATION"),
                            )
                        except Exception as e:
                            error = str(e)
                            print(f"  ✗ Lỗi lấy token: {e}")

                        if token:
                            result = {"token": token, "error": None}
                        else:
                            result = {"token": None, "error": error or "Không thể lấy token"}

                        # Lưu kết quả
                        with cls._recaptcha_results_lock:
                            cls._recaptcha_results[request_id] = result

                        # Báo hiệu đã xong
                        payload["event"].set()

                    except Exception as e:
                        print(f"  ✗ Lỗi trong worker loop: {e}")
                        if "request_id" in locals() and "payload" in locals():
                            with cls._recaptcha_results_lock:
                                cls._recaptcha_results[request_id] = {"token": None, "error": str(e)}
                            payload["event"].set()

                # Cleanup khi worker exit
                try:
                    if cls._recaptcha_worker_browser:
                        cls._recaptcha_worker_browser.close()
                    if hasattr(cls, '_recaptcha_playwright') and cls._recaptcha_playwright:
                        cls._recaptcha_playwright.stop()
                except Exception:
                    pass
                print("  ✅ Worker thread đã dừng")
            
            # Start worker thread
            worker_thread = threading.Thread(target=worker_loop, daemon=True, name="reCAPTCHA-Worker")
            cls._recaptcha_worker_thread = worker_thread
            worker_thread.start()
            print("  ✅ Worker thread đã khởi động")
    
    @classmethod
    def _renew_cookie_and_restart_context(
        cls,
        browser: Any,
        cookie_hash: str,
        old_cookies: Dict[str, str],
        proxy_config: Optional[Dict[str, str]],
        user_agent: str,
        get_new_cookies_callback: Optional[Any] = None,
    ) -> Optional[Dict[str, str]]:
        """
        Renew cookie và restart BrowserContext khi bị chặn.
        
        Flow:
        1. Gọi callback để lấy cookie mới từ DB/server
        2. Clear hết dữ liệu trong BrowserContext cũ (đóng tất cả pages, clear storage)
        3. Đóng BrowserContext cũ
        4. Tạo BrowserContext mới
        5. Inject cookie mới vào context
        6. Trả về cookie mới để sử dụng
        
        Returns:
            Dict[str, str]: Cookie mới nếu thành công, None nếu không thể renew
        """
        from playwright.sync_api import BrowserContext
        
        print(f"  🔄 [reCAPTCHA Worker] Bắt đầu renew cookie và restart context cho cookie: {cookie_hash[:8]}...")
        
        # 1. Lấy cookie mới từ callback hoặc DB
        new_cookies = None
        if get_new_cookies_callback:
            try:
                new_cookies = get_new_cookies_callback(cookie_hash, old_cookies)
                if new_cookies and isinstance(new_cookies, dict) and len(new_cookies) > 0:
                    print(f"  ✅ [reCAPTCHA Worker] Đã lấy cookie mới từ callback (có {len(new_cookies)} cookies)")
                else:
                    print(f"  ⚠️ [reCAPTCHA Worker] Callback không trả về cookie mới hợp lệ")
                    new_cookies = None
            except Exception as e:
                print(f"  ⚠️ [reCAPTCHA Worker] Lỗi gọi callback lấy cookie mới: {e}")
                new_cookies = None
        
        # Nếu không có callback hoặc callback fail, thử lấy từ DB dựa trên cookie_hash
        if not new_cookies:
            try:
                from cookiauto import db_get_account_cookies
                import json
                
                # Tìm email từ cookie_hash (cần mapping, tạm thời skip nếu không có)
                # TODO: Cần cách để map cookie_hash -> email
                # Tạm thời: nếu không có callback và không có mapping, return None
                print(f"  ⚠️ [reCAPTCHA Worker] Không có callback và không thể map cookie_hash -> email, skip renew")
                return None
            except Exception as e:
                print(f"  ⚠️ [reCAPTCHA Worker] Lỗi lấy cookie từ DB: {e}")
                return None
        
        # 2. Clear và đóng BrowserContext cũ
        old_context = None
        if hasattr(cls, '_browser_contexts'):
            old_context = cls._browser_contexts.get(cookie_hash)
        
        if old_context:
            try:
                print(f"  → [reCAPTCHA Worker] Đang clear và đóng BrowserContext cũ...")
                
                # Đóng tất cả pages
                for p in old_context.pages:
                    try:
                        p.close()
                    except:
                        pass
                
                # Clear storage (cookies, localStorage, sessionStorage)
                try:
                    old_context.clear_cookies()
                except:
                    pass
                
                # Đóng context
                old_context.close()
                
                # Xóa khỏi shared dict
                if hasattr(cls, '_browser_contexts'):
                    cls._browser_contexts.pop(cookie_hash, None)
                if hasattr(cls, '_cookies_injected_contexts'):
                    cls._cookies_injected_contexts.pop(cookie_hash, None)
                if hasattr(cls, '_recaptcha_worker_pages'):
                    cls._recaptcha_worker_pages.pop(cookie_hash, None)
                if hasattr(cls, '_recaptcha_worker_page_index'):
                    cls._recaptcha_worker_page_index.pop(cookie_hash, None)
                
                print(f"  ✅ [reCAPTCHA Worker] Đã clear và đóng BrowserContext cũ")
            except Exception as e:
                print(f"  ⚠️ [reCAPTCHA Worker] Lỗi clear context cũ: {e}")
        
        # 3. Tạo BrowserContext mới
        try:
            print(f"  → [reCAPTCHA Worker] Tạo BrowserContext mới với cookie mới...")
            
            context_options: Dict[str, Any] = {
                "viewport": {"width": 200, "height": 150},
                "user_agent": user_agent or "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36",
                "ignore_https_errors": True,
            }
            
            # ✅ KHÔNG dùng proxy cho BrowserContext (navigation bị timeout qua proxy)
            # Proxy chỉ áp dụng cho API HTTP calls (requests.Session)
            
            new_context = browser.new_context(**context_options)
            
            # Test context
            test_page = new_context.new_page()
            test_page.goto("about:blank", timeout=10000)
            test_page.close()
            
            # Lưu context mới vào shared dict
            if not hasattr(cls, '_browser_contexts'):
                cls._browser_contexts = {}
            cls._browser_contexts[cookie_hash] = new_context
            
            # 4. Inject cookie mới vào context
            BASE_URL = "https://labs.google"
            try:
                cookies_input = []
                failed_cookies = []
                for name, value in new_cookies.items():
                    cd: Dict[str, Any] = {
                        "name": name,
                        "value": value,
                        "domain": ".google.com",
                        "path": "/",
                    }
                    
                    # ✅ Xử lý đặc biệt cho cookies prefix
                    if name.startswith("__Host-"):
                        cd.pop("domain", None)
                        cd["secure"] = True
                        cd["sameSite"] = "Lax"
                    elif name.startswith("__Secure-"):
                        cd["secure"] = True
                        cd["sameSite"] = "Lax"
                    
                    cookies_input.append(cd)
                
                if cookies_input:
                    # ✅ Inject từng cookie riêng lẻ
                    for cookie in cookies_input:
                        try:
                            new_context.add_cookies([cookie])
                        except Exception as cookie_err:
                            failed_cookies.append(f"{cookie['name']}: {str(cookie_err)[:50]}")
                    
                    if failed_cookies:
                        print(f"  ⚠️ Cookies không inject được ({len(failed_cookies)}): {', '.join(failed_cookies)}")
                    
                    if not hasattr(cls, '_cookies_injected_contexts'):
                        cls._cookies_injected_contexts = {}
                    cls._cookies_injected_contexts[cookie_hash] = True
                    print(f"  ✅ [reCAPTCHA Worker] Đã inject {len(cookies_input) - len(failed_cookies)}/{len(cookies_input)} cookie mới vào context")
            except Exception as e:
                print(f"  ⚠️ [reCAPTCHA Worker] Lỗi inject cookie mới: {e}")
            
            print(f"  ✅ [reCAPTCHA Worker] Đã renew cookie và restart context thành công")
            return new_cookies
            
        except Exception as e:
            print(f"  ✗ [reCAPTCHA Worker] Lỗi tạo context mới: {e}")
            import traceback
            print(traceback.format_exc())
            return None
                    
    @classmethod
    def _get_recaptcha_token_with_playwright_worker(
        cls,
        browser: Any,
        cookie_hash: str,
        cookies: Dict[str, str],
        proxy_config: Optional[Dict[str, str]],
        user_agent: str,
        timeout_s: int = 90,
        get_new_cookies_callback: Optional[Any] = None,
        max_retries_on_blocked: int = 2,
        recaptcha_action: str = "VIDEO_GENERATION",
    ) -> Optional[str]:
        """
        ĐƠN GIẢN HÓA: Mở profile đã đăng nhập và lấy reCAPTCHA token.
        Không inject cookies phức tạp - dùng profile có sẵn.
        
        Args:
            recaptcha_action: "VIDEO_GENERATION" cho video, "IMAGE_GENERATION" cho image
        """
        from playwright.sync_api import Page

        SITE_KEY = "6LdsFiUsAAAAAIjVDZcuLhaHiDn5nnHVXVRQGeMV"
        TARGET_URL = "https://labs.google/fx/tools/flow"
        
        print(f"  📡 [reCAPTCHA] Lấy token (action={recaptcha_action})...")
        
        # ✅ ĐƠN GIẢN: Tái sử dụng page nếu có, không tạo context phức tạp
        page: Optional[Page] = None
        
        # Lấy page đã có hoặc tạo mới
        if not hasattr(cls, '_recaptcha_page') or cls._recaptcha_page is None:
            # ✅ Kiểm tra page cũ có bị closed không (tránh tạo page mới thừa)
            need_new_page = True
            if hasattr(cls, '_recaptcha_page') and cls._recaptcha_page is not None:
                try:
                    _ = cls._recaptcha_page.url
                    need_new_page = False
                    page = cls._recaptcha_page
                except Exception:
                    cls._recaptcha_page = None
            
            if need_new_page:
                try:
                    # ✅ Tạo context riêng với viewport nhỏ để tránh hiện popup trắng
                    if not hasattr(cls, '_recaptcha_context') or cls._recaptcha_context is None:
                        cls._recaptcha_context = browser.new_context(
                            viewport={"width": 200, "height": 150},
                            ignore_https_errors=True,
                        )
                    page = cls._recaptcha_context.new_page()
                    cls._recaptcha_page = page
                    
                    # Navigate đến trang Flow
                    print(f"  → Navigate đến {TARGET_URL}...")
                    page.goto(TARGET_URL, wait_until="domcontentloaded", timeout=60000)
                    
                    # Đợi trang load
                    time.sleep(2)
                    
                except Exception as e:
                    print(f"  ✗ Lỗi tạo page: {e}")
                    raise RuntimeError(f"Không thể tạo page: {e}")
        else:
            page = cls._recaptcha_page
            
            # Reload trang để lấy token mới
            try:
                print(f"  → Reload trang để lấy token mới...")
                page.reload(wait_until="domcontentloaded", timeout=60000)
            except Exception as e:
                print(f"  ⚠️ Reload lỗi: {e}, thử navigate lại...")
                try:
                    page.goto(TARGET_URL, wait_until="domcontentloaded", timeout=60000)
                except:
                    pass
        
        # Đợi grecaptcha load
        print("  → Đợi grecaptcha load...")
        start_wait = time.time()
        gre_ready = None
        
        while time.time() - start_wait < timeout_s:
            try:
                gre_ready = page.evaluate("""
                    () => {
                        if (typeof window.grecaptcha !== 'undefined') {
                            if (window.grecaptcha.enterprise && 
                                typeof window.grecaptcha.enterprise.execute === 'function') {
                                return 'enterprise';
                            }
                            if (typeof window.grecaptcha.execute === 'function') {
                                return 'classic';
                            }
                        }
                        return null;
                    }
                """)
                if gre_ready:
                    break
            except Exception:
                pass
            time.sleep(0.3)
        
        if not gre_ready:
            raise RuntimeError("grecaptcha không load được - có thể chưa đăng nhập")
        
        print(f"  ✓ grecaptcha sẵn sàng (mode={gre_ready})")
        
        # Lấy token
        print(f"  → Thực thi reCAPTCHA (action={recaptcha_action})...")
        token = page.evaluate(
            """
            async ([siteKey, action]) => {
                try {
                    if (typeof grecaptcha === 'undefined' || !grecaptcha.enterprise) {
                        return {error: 'grecaptcha chưa load'};
                    }
                    
                    const token = await grecaptcha.enterprise.execute(siteKey, {action: action});
                    
                    if (token && token.length > 0) {
                        return {token: token};
                    }
                    return {error: 'Token rỗng'};
                } catch (e) {
                    return {error: e.toString()};
                }
            }
            """,
            [SITE_KEY, recaptcha_action],
        )
        
        if isinstance(token, dict):
            if token.get("token"):
                print(f"  ✅ Lấy token thành công (len={len(token['token'])})")
                return token["token"]
            else:
                raise RuntimeError(f"Lỗi lấy token: {token.get('error', 'unknown')}")
        
        raise RuntimeError("Token không hợp lệ")

    def _get_recaptcha_token_with_playwright(
        self,
        timeout_s: int = 90,
        max_retries_on_403: int = 3,  # giữ tham số cho backward-compat
        acquire_lock: bool = True,
        recaptcha_action: str = "VIDEO_GENERATION",  # ✅ Thêm parameter action
    ) -> Optional[str]:
        """
        Client: gửi request vào reCAPTCHA worker thread và đợi kết quả.
        
        - Đảm bảo worker thread đã khởi động.
        - LOCK THEO COOKIE: mỗi cookie chỉ 1 request token tại 1 thời điểm (nối đuôi).
        - Gửi request vào queue, đợi worker xử lý và trả về token.
        - Worker thread tạo BrowserContext riêng cho mỗi cookie (không giới hạn số cookie).
        
        Args:
            recaptcha_action: "VIDEO_GENERATION" cho video, "IMAGE_GENERATION" cho image
        """
        cookie_hash = self._cookie_hash

        # ✅ LOCK THEO COOKIE: Mỗi cookie chỉ request 1 token tại một thời điểm (nối đuôi)
        if acquire_lock:
            cookie_lock = self._get_cookie_lock(cookie_hash)
            lock_context = cookie_lock
        else:
            from contextlib import nullcontext
            lock_context = nullcontext()

        with lock_context:
            # Đảm bảo worker đã khởi động
            LabsFlowClient._ensure_recaptcha_worker()
            
            # Đợi browser của worker sẵn sàng (tránh race: worker thread chưa kịp launch browser)
            wait_start = time.time()
            while LabsFlowClient._recaptcha_worker_browser is None and LabsFlowClient._recaptcha_worker_started:
                if time.time() - wait_start > 15:  # tối đa 15s để khởi tạo browser
                    break
                time.sleep(0.1)
            
            if not LabsFlowClient._recaptcha_worker_browser:
                self.last_error_detail = "reCAPTCHA worker browser chưa khởi tạo"
                print("  ✗ [reCAPTCHA Client] Worker browser chưa khởi tạo (sau khi chờ)")
                return None

            # Tạo request ID và event để đợi kết quả
            request_id = f"{cookie_hash}_{uuid.uuid4().hex[:8]}"
            event = threading.Event()
            
            # Lấy callback để renew cookie nếu có
            get_new_cookies_callback = None
            if hasattr(LabsFlowClient, '_recaptcha_renew_cookie_callbacks'):
                get_new_cookies_callback = LabsFlowClient._recaptcha_renew_cookie_callbacks.get(cookie_hash)
            
            payload = {
                "cookie_hash": cookie_hash,
                "cookies": self.cookies,
                "proxy_config": None,  # ✅ KHÔNG dùng proxy cho BrowserContext (navigation bị timeout qua proxy)
                "user_agent": self.user_agent,
                "timeout_s": timeout_s,
                "max_retries_on_blocked": 2,  # Retry tối đa 2 lần với cookie mới
                "get_new_cookies_callback": get_new_cookies_callback,
                "event": event,
                "recaptcha_action": recaptcha_action,  # ✅ Truyền action vào payload
            }

            # Gửi request vào queue
            try:
                LabsFlowClient._recaptcha_request_queue.put((request_id, payload))
                print(f"  📤 [reCAPTCHA Client] Đã gửi request vào queue (req_id={request_id[:12]}..., cookie={cookie_hash[:8]}...)")
            except Exception as e:
                self.last_error_detail = f"Không thể gửi request vào queue: {e}"
                print(f"  ✗ [reCAPTCHA Client] Lỗi gửi request: {e}")
                return None

            # Đợi kết quả từ worker (với timeout)
            wait_timeout = timeout_s + 30  # margin
            if not event.wait(timeout=wait_timeout):
                self.last_error_detail = f"Timeout đợi reCAPTCHA worker (req_id={request_id[:12]}...)"
                print(f"  ✗ [reCAPTCHA Client] Timeout đợi worker (req_id={request_id[:12]}...)")
                return None

            # Lấy kết quả từ shared dict
            with LabsFlowClient._recaptcha_results_lock:
                result = LabsFlowClient._recaptcha_results.pop(request_id, None)

            if result is None:
                self.last_error_detail = "Không nhận được kết quả từ reCAPTCHA worker"
                print(f"  ✗ [reCAPTCHA Client] Không nhận được kết quả (req_id={request_id[:12]}...)")
                return None

            if result.get("error"):
                error_msg = result["error"]
                self.last_error_detail = f"reCAPTCHA worker error: {error_msg}"
                print(f"  ✗ [reCAPTCHA Client] Worker error: {error_msg[:200]}")
                return None

            token = result.get("token")
            if token and isinstance(token, str) and len(token.strip()) > 0:
                print(f"  ✅ [reCAPTCHA Client] Nhận token thành công (len={len(token)})")
                return token

            self.last_error_detail = "Token reCAPTCHA không hợp lệ từ worker"
            print(f"  ✗ [reCAPTCHA Client] Token không hợp lệ")
            return None
    
    # endregion reCAPTCHA Playwright Worker Thread Architecture
    
    # region OLD extension/bridge methods (DISABLED - Không dùng nữa)
    def _restart_playwright_context_for_cookie_OLD(self) -> bool:
        """
        Restart BrowserContext cho cookie hiện tại (chỉ context của cookie này).
        Đóng context cũ, khởi tạo lại context mới từ global Browser.
        
        Returns:
            True nếu restart thành công, False nếu không thể restart
        """
        if not self.use_selenium_recaptcha:
            return False
        
        cookie_hash = self._cookie_hash
        
        try:
            # Lấy thread-local Browser
            browser = self._get_global_browser(
                headless=self.selenium_headless,
                browser_path=self.selenium_browser_path
            )
            
            # Đóng context cũ
            if hasattr(LabsFlowClient, '_browser_contexts'):
                old_context = LabsFlowClient._browser_contexts.get(cookie_hash)
                if old_context:
                    try:
                        old_context.close()
                        print(f"  ✓ Đã đóng BrowserContext cũ của cookie {cookie_hash[:8]}...")
                    except Exception:
                        pass
                    LabsFlowClient._browser_contexts.pop(cookie_hash, None)
            
            # Reset cookies injected flag
            if hasattr(LabsFlowClient, '_cookies_injected_contexts'):
                LabsFlowClient._cookies_injected_contexts.pop(cookie_hash, None)
            
            # Tạo context mới từ thread-local browser
            context_options = {
                'viewport': {'width': 200, 'height': 150},
                'user_agent': self.user_agent,
                'ignore_https_errors': True,
            }
            
            # ✅ Proxy configuration: Thêm proxy nếu có
            if self.proxy_config and self.proxy_config.get('server'):
                proxy_server = self.proxy_config['server']
                proxy_username = self.proxy_config.get('username', '')
                proxy_password = self.proxy_config.get('password', '')
                
                proxy_dict = {'server': proxy_server}
                if proxy_username:
                    proxy_dict['username'] = proxy_username
                if proxy_password:
                    proxy_dict['password'] = proxy_password
                
                context_options['proxy'] = proxy_dict
                print(f"  → Dùng proxy: {proxy_server}")
            
            if self.profile_path:
                context_options['storage_state'] = None
                print(f"  → Dùng profile: {self.profile_path}")
            
            new_context = browser.new_context(**context_options)
            
            # Lưu context mới
            if not hasattr(LabsFlowClient, '_browser_contexts'):
                LabsFlowClient._browser_contexts = {}
            LabsFlowClient._browser_contexts[cookie_hash] = new_context
            
            print(f"  ✓ Đã restart BrowserContext thành công cho cookie {cookie_hash[:8]}...")
            return True
            
        except Exception as e:
            print(f"  ✗ Lỗi restart BrowserContext: {str(e)[:100]}")
            return False
    
    def _request_recaptcha_token_from_bridge_OLD(self, timeout_s: int = 90, acquire_lock: bool = True) -> Optional[str]:
        """
        ✅ SEQUENTIAL PER COOKIE: Mỗi cookie chỉ request 1 token tại một thời điểm.
        Các prompt trong cùng cookie phải chờ nhau (nối đuôi).
        Lock theo cookie để đảm bảo tuần tự.
        
        Args:
            timeout_s: Timeout để đợi token
            acquire_lock: Nếu True, tự acquire lock. Nếu False, giả định lock đã được acquire từ bên ngoài.
        """
        server_url = (self.captcha_bridge_url or "").rstrip("/")
        if not server_url:
            self.last_error_detail = "AUTO_RECAPTCHA enabled but CAPTCHA_BRIDGE_URL is empty"
            return None

        cookie_hash = self._cookie_hash
        
        # ✅ LOCK THEO COOKIE: Mỗi cookie chỉ request 1 token tại một thời điểm
        # Lock được giữ từ khi request token đến khi nhận được token
        if acquire_lock:
            cookie_lock = self._get_cookie_lock(cookie_hash)
            lock_context = cookie_lock
        else:
            # Lock đã được acquire từ bên ngoài, không cần acquire lại
            from contextlib import nullcontext
            lock_context = nullcontext()
        
        with lock_context:
            # ✅ Tạo request_id duy nhất cho mỗi prompt
            request_id = f"{cookie_hash}_{int(time.time() * 1000)}_{id(self) % 10000}"
            print(f"  📡 [1/2] Request token NỐI ĐUÔI (cookie: {cookie_hash[:8]}..., req: {request_id[-12:]}...)...")
            
            try:
                resp = self.session.post(
                    f"{server_url}/request-token",
                    json={"cookie_hash": cookie_hash, "request_id": request_id},
                    timeout=10
                )
                resp.raise_for_status()
            except Exception as e:
                self.last_error_detail = f"Cannot reach captcha bridge: {e}"
                print(f"  ✗ Không thể kết nối đến server: {e}")
                return None

            # ✅ HTTP polling - Đợi token cho request_id này (LOCK ĐƯỢC GIỮ TRONG SUỐT QUÁ TRÌNH)
            # Lock được giữ từ khi request token đến khi nhận được token -> đảm bảo tuần tự
            deadline = time.time() + timeout_s
            last_err = None
            poll_count = 0
            start_time = time.time()
            
            print(f"  ⏳ [2/2] Đang chờ token cho request_id={request_id[-12:]}... (timeout: {timeout_s}s)")
            
            while time.time() < deadline:
                poll_count += 1
                elapsed = int(time.time() - start_time)
                
                try:
                    # ✅ CHỈ LẤY TOKEN VỚI REQUEST_ID CỤ THỂ - không lấy token của request khác
                    r = self.session.get(
                        f"{server_url}/get-captcha?clear=0&cookie_hash={cookie_hash}&request_id={request_id}", 
                        timeout=5
                    )
                    r.raise_for_status()
                    data = r.json() if r.content else {}
                    if isinstance(data, dict):
                        token = data.get("token")
                        returned_request_id = data.get("request_id")  # Verify request_id nếu server trả về
                        
                        # ✅ VERIFY: Token phải đúng request_id
                        if token:
                            if returned_request_id and returned_request_id != request_id:
                                # Token không đúng request_id - bỏ qua và tiếp tục poll
                                print(f"  ⚠️ Token nhận được nhưng request_id không khớp (expect: {request_id[-12:]}..., got: {returned_request_id[-12:]}...), tiếp tục đợi...")
                                time.sleep(0.1)  # Giảm delay vì nối đuôi
                                continue
                            
                            # ✅ Token đúng request_id - verify token không rỗng
                            if len(token.strip()) > 0:
                                print(f"  ✅ Nhận token sau {elapsed}s! (cookie: {cookie_hash[:8]}..., req: {request_id[-12:]}..., token length: {len(token)})")
                                
                                # Clear token sau khi lấy
                                try:
                                    self.session.get(
                                        f"{server_url}/get-captcha?clear=1&cookie_hash={cookie_hash}&request_id={request_id}", 
                                        timeout=2
                                    )
                                except:
                                    pass
                                
                                # ✅ ĐỢI THÊM 0.05s để đảm bảo token sẵn sàng (giảm vì nối đuôi)
                                time.sleep(0.05)
                                return token
                            else:
                                print(f"  ⚠️ Token rỗng, tiếp tục đợi... (req: {request_id[-12:]}...)")
                except Exception as e:
                    last_err = e
                
                # Log mỗi 5 giây (poll_count % 50 vì polling interval giảm xuống 0.1s)
                if poll_count % 50 == 0:
                    print(f"  ⏳ Đang chờ token cho request_id={request_id[-12:]}... ({elapsed}s/{timeout_s}s)")
                
                time.sleep(0.1)  # Giảm polling interval từ 0.3s xuống 0.1s để nhận token nhanh hơn

            # Timeout (ngoài while loop)
            self.last_error_detail = f"Timeout ({timeout_s}s) waiting for token. Request: {request_id[-12:]}..."
            print(f"  ✗ Timeout sau {timeout_s}s - không nhận được token")
            return None

    # ═══════════════════════════════════════════════════════════════════════
    # ✅ ZENDRIVER TOKEN FETCHING - Primary source, fallback to Playwright
    # ═══════════════════════════════════════════════════════════════════════
    
    @classmethod
    def _check_zendriver_available(cls) -> bool:
        """Kiểm tra Chrome CDP có sẵn không (tìm Chrome binary)."""
        chrome_path = cls._find_chrome_binary()
        if chrome_path:
            cls._zendriver_available = True  # compat flag
            cls._chrome_cdp_available = True
            return True
        cls._zendriver_available = False
        cls._chrome_cdp_available = False
        return False
    
    @classmethod
    def _find_chrome_binary(cls) -> Optional[str]:
        """Tìm Chrome binary trên hệ thống."""
        import platform
        import shutil
        
        system = platform.system()
        candidates = []
        
        if system == "Darwin":  # macOS
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
        else:  # Linux
            candidates = [
                "/usr/bin/google-chrome",
                "/usr/bin/google-chrome-stable",
                "/usr/bin/chromium-browser",
                "/usr/bin/chromium",
            ]
        
        for path in candidates:
            if os.path.isfile(path):
                return path
        
        # Fallback: tìm trong PATH
        for name in ["google-chrome", "google-chrome-stable", "chromium-browser", "chromium", "chrome"]:
            found = shutil.which(name)
            if found:
                return found
        
        return None
    
    @classmethod
    def _ensure_zendriver_worker(cls, profile_path: str = None):
        """Khởi động Chrome thật với --remote-debugging-port cho CDP.
        
        Nếu có profile_path (profile đã đăng nhập), sẽ copy profile vào temp dir
        để Chrome mở với session đã đăng nhập (tránh bị redirect to login).
        """
        # ✅ Nếu profile_path thay đổi so với lần trước, cần restart Chrome
        current_profile = getattr(cls, '_chrome_cdp_current_profile', None)
        if profile_path and current_profile != profile_path and cls._chrome_cdp_started:
            print(f"  🔄 [Chrome CDP] Profile thay đổi ({current_profile} → {profile_path}), restart Chrome...")
            cls._cleanup_chrome_cdp()
        
        if cls._chrome_cdp_started:
            # Verify Chrome process vẫn đang chạy
            if cls._chrome_cdp_process and cls._chrome_cdp_process.poll() is None:
                return
            # Process đã chết, reset
            cls._chrome_cdp_started = False
            cls._chrome_cdp_process = None
        
        with cls._chrome_cdp_lock:
            if cls._chrome_cdp_started and cls._chrome_cdp_process and cls._chrome_cdp_process.poll() is None:
                return
            
            # ✅ Kiểm tra xem có Chrome CDP nào đang chạy sẵn trên port mặc định không
            # (từ session trước chưa cleanup, hoặc user tự chạy)
            port = cls._chrome_cdp_port
            try:
                resp = requests.get(f"http://127.0.0.1:{port}/json/version", timeout=2)
                if resp.status_code == 200:
                    version_info = resp.json()
                    print(f"  ✅ [Chrome CDP] Reuse Chrome đang chạy trên port {port}: {version_info.get('Browser', 'unknown')}")
                    cls._chrome_cdp_started = True
                    cls._chrome_cdp_process = None  # Không quản lý process (external)
                    # Chỉ return nếu profile hiện tại đã match
                    if not profile_path or getattr(cls, '_chrome_cdp_current_profile', None) == profile_path:
                        return
                    print(f"  🔄 [Chrome CDP] Profile thay đổi, cần khởi động lại...")
                    cls._cleanup_chrome_cdp()
            except Exception:
                pass
            
            chrome_path = cls._find_chrome_binary()
            if not chrome_path:
                print("  ⚠️ [Chrome CDP] Không tìm thấy Chrome binary")
                return
            
            import subprocess
            import tempfile
            
            # ✅ Dùng profile thật (copy) nếu có, fallback temp dir trống
            current_data_dir = getattr(cls, '_chrome_cdp_user_data_dir', None)
            current_profile_saved = getattr(cls, '_chrome_cdp_current_profile', None)
            
            need_new_dir = (
                current_data_dir is None
                or (profile_path and current_profile_saved != profile_path and current_profile_saved is None)
            )

            if need_new_dir:
                if current_data_dir and current_profile_saved is None:
                    import shutil
                    try:
                        shutil.rmtree(current_data_dir, ignore_errors=True)
                    except Exception:
                        pass
                    cls._chrome_cdp_user_data_dir = None
                    
                if profile_path and os.path.exists(profile_path):
                    import shutil, subprocess
                    temp_dir = tempfile.mkdtemp(prefix="chrome_cdp_profile_")
                    try:
                        # ✅ Dùng robocopy /B (backup mode) để copy kể cả file bị Chrome lock
                        result = subprocess.run(
                            ["robocopy", profile_path, temp_dir, "/E", "/B", "/NFL", "/NDL", "/NJH", "/NJS", "/NC", "/NS"],
                            capture_output=True, timeout=30
                        )
                        # robocopy exit code < 8 là thành công
                        if result.returncode >= 8:
                            # Fallback sang shutil nếu robocopy fail
                            shutil.copytree(profile_path, temp_dir, dirs_exist_ok=True, ignore_dangling_symlinks=True)
                    except Exception:
                        try:
                            shutil.copytree(profile_path, temp_dir, dirs_exist_ok=True, ignore_dangling_symlinks=True)
                        except Exception as e2:
                            print(f"  ⚠️ [Chrome CDP] Copy profile lỗi: {e2}")
                    
                    # Xóa lock files
                    for lock_file in ["SingletonLock", "SingletonSocket", "SingletonCookie"]:
                        for d in [temp_dir, os.path.join(temp_dir, "Default")]:
                            lp = os.path.join(d, lock_file)
                            if os.path.exists(lp):
                                try: os.remove(lp)
                                except: pass
                    for lock_file in ["LOCK", "lockfile"]:
                        lp = os.path.join(temp_dir, "Default", "Network", lock_file)
                        if os.path.exists(lp):
                            try: os.remove(lp)
                            except: pass

                    cookies_db = os.path.join(temp_dir, "Default", "Network", "Cookies")
                    cookies_db_alt = os.path.join(temp_dir, "Default", "Cookies")
                    if os.path.exists(cookies_db):
                        print(f"  📂 [Chrome CDP] Dùng profile thật (copy): {profile_path}")
                        print(f"  ✅ [Chrome CDP] Cookies DB found: {cookies_db} ({os.path.getsize(cookies_db)} bytes)")
                    elif os.path.exists(cookies_db_alt):
                        print(f"  📂 [Chrome CDP] Dùng profile thật (copy): {profile_path}")
                        print(f"  ✅ [Chrome CDP] Cookies DB found (alt): {cookies_db_alt} ({os.path.getsize(cookies_db_alt)} bytes)")
                    else:
                        print(f"  📂 [Chrome CDP] Dùng profile thật (copy): {profile_path}")
                        print(f"  ⚠️ [Chrome CDP] Cookies DB KHÔNG tìm thấy trong profile copy!")

                    cls._chrome_cdp_user_data_dir = temp_dir
                    cls._chrome_cdp_current_profile = profile_path
                else:
                    cls._chrome_cdp_user_data_dir = tempfile.mkdtemp(prefix="chrome_cdp_recaptcha_")
                    cls._chrome_cdp_current_profile = None
                    if profile_path:
                        print(f"  ⚠️ [Chrome CDP] Profile path không tồn tại: {profile_path}, dùng temp dir trống")
            
            # Tìm port trống (bắt đầu từ port hiện tại)
            import socket
            for try_port in range(port, port + 20):
                try:
                    sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
                    sock.settimeout(0.5)
                    result = sock.connect_ex(('127.0.0.1', try_port))
                    sock.close()
                    if result != 0:  # Port trống
                        port = try_port
                        break
                except Exception:
                    pass
            cls._chrome_cdp_port = port
            
            chrome_args = [
                chrome_path,
                f"--remote-debugging-port={port}",
                f"--user-data-dir={cls._chrome_cdp_user_data_dir}",
                "--no-first-run",
                "--no-default-browser-check",
                "--disable-default-apps",
                "--disable-extensions",
                "--disable-sync",
                "--disable-translate",
                "--disable-background-networking",
                "--disable-popup-blocking",
                "--metrics-recording-only",
                "--no-service-autorun",
                # Off-screen window
                "--window-position=-3000,-3000",
                "--window-size=400,300",
            ]
            
            try:
                proc = subprocess.Popen(
                    chrome_args,
                    stdout=subprocess.DEVNULL,
                    stderr=subprocess.DEVNULL,
                )
                cls._chrome_cdp_process = proc
                cls._chrome_cdp_started = True
                print(f"  🚀 [Chrome CDP] Chrome launched (PID={proc.pid}, port={port})")
                
                # Đợi Chrome sẵn sàng (CDP endpoint)
                wait_start = time.time()
                while time.time() - wait_start < 15:
                    try:
                        resp = requests.get(f"http://127.0.0.1:{port}/json/version", timeout=2)
                        if resp.status_code == 200:
                            version_info = resp.json()
                            print(f"  ✅ [Chrome CDP] Chrome sẵn sàng: {version_info.get('Browser', 'unknown')}")
                            return
                    except Exception:
                        pass
                    time.sleep(0.3)
                
                print("  ⚠️ [Chrome CDP] Timeout chờ Chrome khởi động")
                
            except Exception as e:
                print(f"  ❌ [Chrome CDP] Lỗi launch Chrome: {e}")
                cls._chrome_cdp_started = False
                cls._chrome_cdp_process = None
    
    @classmethod
    def _zendriver_reset_page(cls, cookie_hash: str):
        """Reset page/tab cho cookie (khi cần re-inject cookies)."""
        # ✅ Close persistent WebSocket connection
        old_ws = cls._chrome_cdp_ws_conns.pop(cookie_hash, None)
        if old_ws:
            try:
                old_ws.close()
            except Exception:
                pass
        cls._chrome_cdp_ws_msg_ids.pop(cookie_hash, None)
        cls._chrome_cdp_page_ready.pop(cookie_hash, None)
        
        # ✅ Close CDP tab nếu có
        tab_id = cls._chrome_cdp_tab_ids.pop(cookie_hash, None)
        if tab_id and cls._chrome_cdp_started:
            try:
                requests.get(
                    f"http://127.0.0.1:{cls._chrome_cdp_port}/json/close/{tab_id}",
                    timeout=3,
                )
            except Exception:
                pass
        cls._chrome_cdp_pages.pop(cookie_hash, None)
        cls._chrome_cdp_cookies_injected.pop(cookie_hash, None)
        # Compat: clear zendriver caches too
        cls._zendriver_pages.pop(cookie_hash, None)
        cls._zendriver_cookies_injected.pop(cookie_hash, None)
    
    def _get_recaptcha_token_zendriver(
        self,
        timeout_s: int = 60,
        recaptcha_action: str = "VIDEO_GENERATION",
    ) -> Optional[str]:
        """
        Lấy reCAPTCHA token qua Chrome thật + CDP protocol.
        Chrome thật cho trust score cao hơn zendriver/playwright.
        
        ✅ Improvements:
        - Persistent WebSocket connection (không open/close mỗi lần)
        - Không reload page nếu đã load sẵn → execute grecaptcha trực tiếp
        - Dùng GET thay PUT cho /json/new (đúng Chrome DevTools spec)
        - Robust cdp_send với per-command timeout
        - Auto-recovery khi WebSocket bị stale
        """
        cookie_hash = self._cookie_hash
        
        # Đảm bảo Chrome đã khởi động - truyền profile_path nếu có
        profile_path = self._get_profile_path_for_cookie()
        LabsFlowClient._ensure_zendriver_worker(profile_path=profile_path)
        
        if not LabsFlowClient._chrome_cdp_started:
            print("  ⚠️ [Chrome CDP] Chrome chưa sẵn sàng")
            return None
        
        port = LabsFlowClient._chrome_cdp_port
        SITE_KEY = "6LdsFiUsAAAAAIjVDZcuLhaHiDn5nnHVXVRQGeMV"
        TARGET_URL = "https://labs.google/fx/tools/flow"
        
        def _create_new_tab() -> Optional[tuple]:
            """Tạo tab mới, trả về (ws_url, tab_id) hoặc None."""
            # Chrome DevTools Protocol dùng GET (hoặc PUT) cho /json/new
            # Thử GET trước (phổ biến hơn), fallback PUT
            for method_fn in [requests.get, requests.put]:
                try:
                    resp = method_fn(
                        f"http://127.0.0.1:{port}/json/new?about:blank",
                        timeout=5,
                    )
                    if resp.status_code == 200:
                        tab_info = resp.json()
                        ws = tab_info.get("webSocketDebuggerUrl")
                        tid = tab_info.get("id")
                        if ws:
                            return (ws, tid)
                except Exception:
                    continue
            return None
        
        def _get_or_create_ws(ws_url: str) -> Optional[Any]:
            """Lấy persistent WS connection hoặc tạo mới."""
            import websockets.sync.client as ws_sync
            
            existing = LabsFlowClient._chrome_cdp_ws_conns.get(cookie_hash)
            if existing is not None:
                # Kiểm tra connection còn sống không
                try:
                    existing.ping()
                    return existing
                except Exception:
                    # Connection đã chết, cleanup
                    try:
                        existing.close()
                    except Exception:
                        pass
                    LabsFlowClient._chrome_cdp_ws_conns.pop(cookie_hash, None)
            
            # Tạo connection mới
            try:
                conn = ws_sync.connect(ws_url, close_timeout=5, open_timeout=10)
                LabsFlowClient._chrome_cdp_ws_conns[cookie_hash] = conn
                LabsFlowClient._chrome_cdp_ws_msg_ids[cookie_hash] = 1
                return conn
            except Exception as e:
                print(f"  ⚠️ [Chrome CDP] WS connect failed: {e}")
                return None
        
        def _cdp_send(ws, method: str, params: dict = None, cmd_timeout: float = 30) -> dict:
            """Gửi CDP command và nhận response. Per-command timeout."""
            msg_id = LabsFlowClient._chrome_cdp_ws_msg_ids.get(cookie_hash, 1)
            payload = {"id": msg_id, "method": method}
            if params:
                payload["params"] = params
            LabsFlowClient._chrome_cdp_ws_msg_ids[cookie_hash] = msg_id + 1
            
            ws.send(json.dumps(payload))
            
            # Đọc response (bỏ qua CDP events, chỉ lấy response có id match)
            deadline = time.time() + cmd_timeout
            while time.time() < deadline:
                remaining = max(0.1, deadline - time.time())
                try:
                    raw = ws.recv(timeout=remaining)
                except TimeoutError:
                    break
                except Exception:
                    break
                data = json.loads(raw)
                if data.get("id") == msg_id:
                    if "error" in data:
                        err = data["error"]
                        print(f"  ⚠️ [CDP] {method} error: {err.get('message', err)}")
                    return data
            return {}
        
        try:
            # ═══ Bước 1: Lấy hoặc tạo tab cho cookie này ═══
            ws_url = LabsFlowClient._chrome_cdp_pages.get(cookie_hash)
            need_navigate = False
            page_ready = LabsFlowClient._chrome_cdp_page_ready.get(cookie_hash, False)
            
            if ws_url is None:
                result = _create_new_tab()
                if not result:
                    print("  ⚠️ [Chrome CDP] Không tạo được tab mới")
                    return None
                ws_url, tab_id = result
                LabsFlowClient._chrome_cdp_pages[cookie_hash] = ws_url
                LabsFlowClient._chrome_cdp_tab_ids[cookie_hash] = tab_id
                LabsFlowClient._chrome_cdp_cookies_injected[cookie_hash] = False
                LabsFlowClient._chrome_cdp_page_ready[cookie_hash] = False
                need_navigate = True
                page_ready = False
                print(f"  📄 [Chrome CDP] Tạo tab mới cho cookie {cookie_hash[:8]}...")
            
            # ═══ Bước 2: Lấy persistent WebSocket connection ═══
            ws = _get_or_create_ws(ws_url)
            if ws is None:
                # WS connection failed → tab có thể đã bị đóng, tạo lại
                print("  🔄 [Chrome CDP] WS stale, tạo tab mới...")
                LabsFlowClient._zendriver_reset_page(cookie_hash)
                LabsFlowClient._chrome_cdp_ws_conns.pop(cookie_hash, None)
                
                result = _create_new_tab()
                if not result:
                    print("  ⚠️ [Chrome CDP] Không tạo được tab mới (retry)")
                    return None
                ws_url, tab_id = result
                LabsFlowClient._chrome_cdp_pages[cookie_hash] = ws_url
                LabsFlowClient._chrome_cdp_tab_ids[cookie_hash] = tab_id
                LabsFlowClient._chrome_cdp_cookies_injected[cookie_hash] = False
                LabsFlowClient._chrome_cdp_page_ready[cookie_hash] = False
                need_navigate = True
                page_ready = False
                
                ws = _get_or_create_ws(ws_url)
                if ws is None:
                    return None
            
            # ═══ Bước 3: Nếu dùng profile copy, navigate trước để dùng cookies từ profile DB ═══
            has_profile = getattr(LabsFlowClient, '_chrome_cdp_current_profile', None) is not None
            profile_cookies_ok = False
            
            if has_profile and need_navigate and not LabsFlowClient._chrome_cdp_cookies_injected.get(cookie_hash, False):
                # Profile đã copy → Chrome đã load cookies từ SQLite DB
                # Navigate trước để check xem profile cookies còn hợp lệ không
                print(f"  🌐 [Chrome CDP] Navigate (profile mode) đến {TARGET_URL}...")
                _cdp_send(ws, "Page.enable", cmd_timeout=5)
                _cdp_send(ws, "Page.navigate", {"url": TARGET_URL}, cmd_timeout=15)
                start_load = time.time()
                while time.time() - start_load < 15:
                    try:
                        rs = _cdp_send(ws, "Runtime.evaluate", {
                            "expression": "document.readyState",
                            "returnByValue": True,
                        }, cmd_timeout=5)
                        state = rs.get("result", {}).get("result", {}).get("value", "")
                        if state in ("complete", "interactive"):
                            break
                    except Exception:
                        pass
                    time.sleep(0.5)
                
                # Check URL - nếu không bị redirect thì profile cookies OK
                try:
                    loc_result = _cdp_send(ws, "Runtime.evaluate", {
                        "expression": "window.location.href",
                        "returnByValue": True,
                    }, cmd_timeout=5)
                    current_url = loc_result.get("result", {}).get("result", {}).get("value", "")
                    if "accounts.google" not in current_url and "signin" not in current_url.lower():
                        print(f"  ✅ [Chrome CDP] Profile cookies hợp lệ, không cần inject CDP cookies")
                        profile_cookies_ok = True
                        LabsFlowClient._chrome_cdp_cookies_injected[cookie_hash] = True
                        need_navigate = False  # Đã navigate rồi
                        page_ready = False  # Cần check grecaptcha
                    else:
                        print(f"  ⚠️ [Chrome CDP] Profile cookies expired, sẽ inject CDP cookies...")
                except Exception:
                    pass
            
            # ═══ Bước 3b: Inject cookies nếu chưa (profile cookies failed hoặc không có profile) ═══
            if not profile_cookies_ok and not LabsFlowClient._chrome_cdp_cookies_injected.get(cookie_hash, False):
                _cdp_send(ws, "Network.enable", cmd_timeout=10)
                
                # ✅ Inject cookies với xử lý đúng cho __Host- và __Secure- prefix
                # __Host- cookies: PHẢI có secure=True, path="/", KHÔNG được set domain
                # __Secure- cookies: PHẢI có secure=True, domain phải match
                # Cần set url để Chrome biết context cho cookie
                inject_success = 0
                inject_fail = 0
                for name, value in self.cookies.items():
                    try:
                        if name.startswith("__Host-"):
                            # __Host- prefix: không set domain, phải dùng url
                            result = _cdp_send(ws, "Network.setCookie", {
                                "name": name,
                                "value": value,
                                "url": "https://labs.google/fx/tools/flow",
                                "path": "/",
                                "secure": True,
                                "httpOnly": True,
                            }, cmd_timeout=5)
                        elif name.startswith("__Secure-"):
                            # __Secure- prefix: set domain .labs.google
                            result = _cdp_send(ws, "Network.setCookie", {
                                "name": name,
                                "value": value,
                                "domain": ".labs.google",
                                "url": "https://labs.google/fx/tools/flow",
                                "path": "/",
                                "secure": True,
                                "httpOnly": True,
                            }, cmd_timeout=5)
                        else:
                            # Regular cookies: inject cho cả 2 domains
                            for domain, url in [
                                (".labs.google", "https://labs.google/fx/tools/flow"),
                                (".google.com", "https://accounts.google.com"),
                            ]:
                                result = _cdp_send(ws, "Network.setCookie", {
                                    "name": name,
                                    "value": value,
                                    "domain": domain,
                                    "url": url,
                                    "path": "/",
                                    "secure": True,
                                    "httpOnly": True,
                                }, cmd_timeout=5)
                        
                        # Check if setCookie succeeded
                        success = result.get("result", {}).get("success", True) if result else False
                        if success and "error" not in result:
                            inject_success += 1
                        else:
                            inject_fail += 1
                    except Exception:
                        inject_fail += 1
                
                # ✅ Verify cookies were actually set
                try:
                    verify_result = _cdp_send(ws, "Network.getCookies", {
                        "urls": ["https://labs.google/fx/tools/flow"]
                    }, cmd_timeout=5)
                    actual_cookies = verify_result.get("result", {}).get("cookies", [])
                    session_found = any(c.get("name") == "__Secure-next-auth.session-token" for c in actual_cookies)
                    if not session_found:
                        print(f"  ⚠️ [Chrome CDP] Session token KHÔNG có trong browser sau inject! Thử lại...")
                        # Retry với url-based approach
                        for name, value in self.cookies.items():
                            _cdp_send(ws, "Network.setCookie", {
                                "name": name,
                                "value": value,
                                "url": "https://labs.google/fx/tools/flow",
                                "secure": True,
                                "httpOnly": True,
                            }, cmd_timeout=5)
                    else:
                        print(f"  ✅ [Chrome CDP] Verified: session token có trong browser ({len(actual_cookies)} cookies)")
                except Exception as e:
                    print(f"  ⚠️ [Chrome CDP] Không verify được cookies: {e}")
                
                LabsFlowClient._chrome_cdp_cookies_injected[cookie_hash] = True
                need_navigate = True
                page_ready = False
                print(f"  🍪 [Chrome CDP] Đã inject {inject_success} cookies OK, {inject_fail} failed")
            
            # ═══ Bước 4: Navigate nếu cần, KHÔNG reload nếu page đã sẵn sàng ═══
            if need_navigate:
                print(f"  🌐 [Chrome CDP] Navigate đến {TARGET_URL}...")
                _cdp_send(ws, "Page.enable", cmd_timeout=5)
                _cdp_send(ws, "Page.navigate", {"url": TARGET_URL}, cmd_timeout=15)
                # Đợi page load bằng cách poll document.readyState
                start_load = time.time()
                while time.time() - start_load < 15:
                    try:
                        rs = _cdp_send(ws, "Runtime.evaluate", {
                            "expression": "document.readyState",
                            "returnByValue": True,
                        }, cmd_timeout=5)
                        state = rs.get("result", {}).get("result", {}).get("value", "")
                        if state in ("complete", "interactive"):
                            break
                    except Exception:
                        pass
                    time.sleep(0.5)
                page_ready = False  # Cần check grecaptcha lại
            elif page_ready:
                # ✅ Page đã load sẵn, grecaptcha đã có → execute trực tiếp (NHANH)
                print(f"  ⚡ [Chrome CDP] Page sẵn sàng, execute trực tiếp...")
            else:
                # Page chưa ready (lần đầu sau navigate) → cần check grecaptcha
                pass
            
            # ═══ Bước 5: Check URL (redirect to login?) ═══
            try:
                loc_result = _cdp_send(ws, "Runtime.evaluate", {
                    "expression": "window.location.href",
                    "returnByValue": True,
                }, cmd_timeout=5)
                current_url = loc_result.get("result", {}).get("result", {}).get("value", "")
                if "accounts.google" in current_url or "signin" in current_url.lower():
                    print(f"  ⚠️ [Chrome CDP] Redirected to login - cookie expired")
                    LabsFlowClient._zendriver_reset_page(cookie_hash)
                    LabsFlowClient._chrome_cdp_page_ready.pop(cookie_hash, None)
                    # Close WS connection
                    LabsFlowClient._chrome_cdp_ws_conns.pop(cookie_hash, None)
                    try:
                        ws.close()
                    except Exception:
                        pass
                    return None
            except Exception:
                pass
            
            # ═══ Bước 6: Đợi grecaptcha load (skip nếu page_ready) ═══
            if not page_ready:
                print("  ⏳ [Chrome CDP] Đợi grecaptcha load...")
                start_wait = time.time()
                gre_loaded = False
                
                while time.time() - start_wait < timeout_s:
                    try:
                        check_result = _cdp_send(ws, "Runtime.evaluate", {
                            "expression": """(() => {
                                if (typeof window.grecaptcha !== 'undefined') {
                                    if (window.grecaptcha.enterprise && typeof window.grecaptcha.enterprise.execute === 'function') return 'enterprise';
                                    if (typeof window.grecaptcha.execute === 'function') return 'classic';
                                }
                                return null;
                            })()""",
                            "returnByValue": True,
                        }, cmd_timeout=10)
                        val = check_result.get("result", {}).get("result", {}).get("value")
                        if val:
                            gre_loaded = True
                            print(f"  ✓ [Chrome CDP] grecaptcha sẵn sàng (mode={val}, {time.time()-start_wait:.1f}s)")
                            break
                    except Exception:
                        pass
                    time.sleep(0.5)
                
                if not gre_loaded:
                    print(f"  ⚠️ [Chrome CDP] grecaptcha not loaded after {time.time()-start_wait:.0f}s")
                    LabsFlowClient._chrome_cdp_page_ready[cookie_hash] = False
                    return None
            
            # ═══ Bước 7: Execute reCAPTCHA ═══
            exec_js = """(async () => {
                try {
                    const siteKey = '%s';
                    let token = null;
                    if (typeof grecaptcha !== 'undefined' && grecaptcha.enterprise && typeof grecaptcha.enterprise.execute === 'function') {
                        token = await grecaptcha.enterprise.execute(siteKey, {action: '%s'});
                    } else if (typeof grecaptcha !== 'undefined' && typeof grecaptcha.execute === 'function') {
                        token = await grecaptcha.execute(siteKey, {action: '%s'});
                    }
                    return (token && token.length > 0) ? token : 'ERROR:Empty';
                } catch (e) {
                    return 'ERROR:' + e.toString();
                }
            })()""" % (SITE_KEY, recaptcha_action, recaptcha_action)
            
            print(f"  🔑 [Chrome CDP] Executing reCAPTCHA (action={recaptcha_action})...")
            exec_result = _cdp_send(ws, "Runtime.evaluate", {
                "expression": exec_js,
                "awaitPromise": True,
                "returnByValue": True,
            }, cmd_timeout=30)
            
            val = exec_result.get("result", {}).get("result", {}).get("value")
            if isinstance(val, str) and not val.startswith("ERROR:") and len(val) > 20:
                # ✅ Thành công → đánh dấu page_ready để lần sau không cần reload
                LabsFlowClient._chrome_cdp_page_ready[cookie_hash] = True
                print(f"  ✅ [Chrome CDP] Token OK (len={len(val)})")
                return val
            elif isinstance(val, str) and val.startswith("ERROR:"):
                print(f"  ⚠️ [Chrome CDP] reCAPTCHA error: {val[6:]}")
                # Error có thể do page state bị stale → reset page_ready
                LabsFlowClient._chrome_cdp_page_ready[cookie_hash] = False
            else:
                print(f"  ⚠️ [Chrome CDP] Unexpected result: {val}")
                LabsFlowClient._chrome_cdp_page_ready[cookie_hash] = False
            
            return None
        
        except Exception as e:
            print(f"  ⚠️ [Chrome CDP] Error: {e}")
            import traceback
            traceback.print_exc()
            # Connection có thể bị hỏng → cleanup WS để lần sau tạo mới
            old_ws = LabsFlowClient._chrome_cdp_ws_conns.pop(cookie_hash, None)
            if old_ws:
                try:
                    old_ws.close()
                except Exception:
                    pass
            LabsFlowClient._chrome_cdp_page_ready.pop(cookie_hash, None)
            return None
    
    def _record_token_source(self, source: str):
        """Ghi nhận nguồn token vừa dùng."""
        LabsFlowClient._last_token_source[self._cookie_hash] = source

    @staticmethod
    def calculate_retry_delay(
        attempt: int,
        error_code: int,
        base_delay: float = 1.0,
        max_delay: float = 60.0,
        use_jitter: bool = True,
    ) -> float:
        """Tính toán retry delay với exponential backoff và jitter.
        
        Args:
            attempt: Số attempt hiện tại (bắt đầu từ 0)
            error_code: HTTP error code (403, 429, 500, etc.)
            base_delay: Delay cơ bản (giây)
            max_delay: Delay tối đa (giây)
            use_jitter: Có thêm random jitter không
            
        Returns:
            Số giây để đợi trước khi retry
        """
        import random
        
        # Exponential backoff: base_delay * 2^attempt
        delay = base_delay * (2 ** attempt)
        
        # Điều chỉnh theo loại lỗi
        if error_code == 429:
            # Rate limit - đợi lâu hơn
            delay *= 2
        elif error_code == 403:
            # 403 - đợi ngắn hơn vì cần refresh token nhanh
            delay *= 0.5
        elif error_code == 500:
            # Server error - đợi vừa phải
            delay *= 1.5
        
        # Cap at max_delay
        delay = min(delay, max_delay)
        
        # Thêm jitter (random ±25%) để tránh thundering herd
        if use_jitter:
            jitter = delay * 0.25
            delay = delay + random.uniform(-jitter, jitter)
        
        return max(0.1, delay)  # Minimum 0.1 giây

    def _switch_token_source_on_error(self, current_source: str, error_code: int) -> str:
        """Tự động chuyển đổi nguồn token khi gặp lỗi.
        
        Flow:
        - Nếu đang dùng Chrome CDP mà bị 403 nhiều → chuyển sang playwright
        - Nếu đang dùng playwright mà bị 403 nhiều → chuyển sang Chrome CDP
        """
        cookie_hash = self._cookie_hash
        
        if error_code == 403:
            cdp_403 = LabsFlowClient._chrome_cdp_consecutive_403.get(cookie_hash, 0)
            pw_403 = LabsFlowClient._playwright_consecutive_403.get(cookie_hash, 0)
            
            if current_source in ("chrome_cdp", "zendriver") and cdp_403 >= self.MAX_CHROME_CDP_403:
                print(f"  🔄 [Token Source] Chrome CDP đạt ngưỡng {self.MAX_CHROME_CDP_403} lỗi 403 → Chuyển sang Playwright")
                return "playwright"
            elif current_source == "playwright" and pw_403 >= self.MAX_PLAYWRIGHT_403:
                print(f"  🔄 [Token Source] Playwright đạt ngưỡng {self.MAX_PLAYWRIGHT_403} lỗi 403 → Chuyển sang Chrome CDP")
                return "chrome_cdp"
        
        return current_source
    
    def _refresh_cookie_on_403(self) -> bool:
        """Refresh cookie khi bị 403 - xóa cookie cũ, reload trang để lấy cookie mới.
        
        ✅ Dùng Chrome CDP thay vì zendriver.
        ✅ Reset token timestamp để buộc lấy token mới.
        
        Returns:
            True nếu refresh thành công, False nếu thất bại
        """
        cookie_hash = self._cookie_hash
        print(f"  🔄 [403 Handler] Cookie {cookie_hash[:8]}... bị 403 - Đang reload trang để lấy cookie mới...")
        
        # ✅ Reset token timestamp để buộc lấy token mới ở attempt tiếp theo
        LabsFlowClient._token_timestamps.pop(cookie_hash, None)
        
        # ✅ Reset Chrome CDP page/tab
        LabsFlowClient._zendriver_reset_page(cookie_hash)
        
        # ✅ Reset playwright context để lấy cookie mới
        # CHỈ set None reference, KHÔNG gọi .close() vì context có thể được tạo trong worker thread
        if hasattr(LabsFlowClient, '_browser_contexts') and cookie_hash in LabsFlowClient._browser_contexts:
            LabsFlowClient._browser_contexts.pop(cookie_hash, None)
            if hasattr(LabsFlowClient, '_cookies_injected_contexts'):
                LabsFlowClient._cookies_injected_contexts.pop(cookie_hash, None)
        
        # ✅ Reset playwright recaptcha page - CHỈ đánh dấu flag, KHÔNG gọi .close() từ thread này
        # Playwright sync API dùng greenlet bị ràng buộc vào worker thread đã tạo nó.
        # Gọi .close() từ thread khác sẽ gây lỗi "greenlet.error: Cannot switch to a different thread".
        # Worker thread sẽ tự close page/context cũ khi xử lý request tiếp theo (qua _contexts_need_reset flag).
        with LabsFlowClient._contexts_need_reset_lock:
            LabsFlowClient._contexts_need_reset[cookie_hash] = True
        
        # ✅ Reset all error counters cho cookie này
        self._reset_all_error_counters()
        LabsFlowClient._zendriver_consecutive_403[cookie_hash] = 0
        LabsFlowClient._chrome_cdp_consecutive_403[cookie_hash] = 0
        LabsFlowClient._playwright_consecutive_403[cookie_hash] = 0
        
        print(f"  ✅ [403 Handler] Đã refresh cookie {cookie_hash[:8]}... - tab mới sẽ được tạo ở attempt tiếp theo")
        return True
    
    def _handle_401_refresh_token(self) -> bool:
        """Xử lý 401 thông minh: re-fetch access token, nếu token không đổi thì refresh cookies.
        
        Returns:
            True nếu có token mới (nên retry), False nếu không thể fix
        """
        cookie_hash = self._cookie_hash
        old_token = self.access_token
        
        # Bước 1: Re-fetch access token
        if self.fetch_access_token():
            if self.access_token != old_token:
                print(f"  ✅ [401 Handler] Access token ĐÃ THAY ĐỔI → retry")
                return True
            
            # Token không đổi → session có thể expired
            same_count = getattr(self, '_same_token_count', 0)
            print(f"  ⚠️ [401 Handler] Token KHÔNG ĐỔI (lần {same_count})")
            
            if same_count >= 2:
                # Thử refresh cookies từ profile
                print(f"  🔄 [401 Handler] Thử refresh cookies từ profile...")
                new_cookies = self._refresh_cookies_from_profile()
                if new_cookies:
                    self._apply_new_cookies(new_cookies, cookie_hash)
                    # Re-fetch access token với cookies mới
                    if self.fetch_access_token():
                        print(f"  ✅ [401 Handler] Token mới sau refresh cookies: {self.access_token[:20]}...")
                        return True
                    else:
                        print(f"  ❌ [401 Handler] Không thể lấy token sau refresh cookies")
                        return False
                else:
                    # Không có profile → thử renew callback
                    renew_cb = LabsFlowClient._recaptcha_renew_cookie_callbacks.get(cookie_hash) if hasattr(LabsFlowClient, '_recaptcha_renew_cookie_callbacks') else None
                    if renew_cb:
                        print(f"  🔄 [401 Handler] Thử renew cookie callback...")
                        new_cookies = renew_cb(cookie_hash, self.cookies)
                        if new_cookies:
                            self._apply_new_cookies(new_cookies, cookie_hash)
                            if self.fetch_access_token():
                                print(f"  ✅ [401 Handler] Token mới sau renew callback: {self.access_token[:20]}...")
                                return True
                    print(f"  ⚠️ [401 Handler] Không thể refresh cookies")
                    return False
            
            # Chưa đến ngưỡng, vẫn retry với token hiện tại
            return True
        else:
            print(f"  ❌ [401 Handler] Không thể fetch access token")
            return False
    
    def _on_api_success(self):
        """Gọi khi API call thành công - reset counters."""
        cookie_hash = self._cookie_hash
        LabsFlowClient._zendriver_consecutive_403[cookie_hash] = 0
        LabsFlowClient._chrome_cdp_consecutive_403[cookie_hash] = 0
        LabsFlowClient._playwright_consecutive_403[cookie_hash] = 0
        self._reset_all_error_counters()
        # ✅ Reset 403 refresh retries khi thành công
        if hasattr(self, '_403_refresh_retries'):
            self._403_refresh_retries[cookie_hash] = 0
    
    def _on_api_403(self):
        """Gọi khi API trả về 403 - tăng counter cho source đã dùng."""
        cookie_hash = self._cookie_hash
        source = LabsFlowClient._last_token_source.get(cookie_hash, "playwright")
        
        if source == "chrome_cdp":
            count = LabsFlowClient._chrome_cdp_consecutive_403.get(cookie_hash, 0) + 1
            LabsFlowClient._chrome_cdp_consecutive_403[cookie_hash] = count
            # Compat: cũng update zendriver counter
            LabsFlowClient._zendriver_consecutive_403[cookie_hash] = count
            print(f"  📊 [Token Source] Chrome CDP 403 count: {count}/{self.MAX_CHROME_CDP_403}")
        elif source == "zendriver":
            # Backward compat
            count = LabsFlowClient._zendriver_consecutive_403.get(cookie_hash, 0) + 1
            LabsFlowClient._zendriver_consecutive_403[cookie_hash] = count
            LabsFlowClient._chrome_cdp_consecutive_403[cookie_hash] = count
            print(f"  📊 [Token Source] Chrome CDP 403 count: {count}/{self.MAX_CHROME_CDP_403}")
        else:
            count = LabsFlowClient._playwright_consecutive_403.get(cookie_hash, 0) + 1
            LabsFlowClient._playwright_consecutive_403[cookie_hash] = count
            print(f"  📊 [Token Source] Playwright 403 count: {count}/{self.MAX_PLAYWRIGHT_403}")
    
    def _should_use_zendriver(self) -> bool:
        """Quyết định có nên dùng Chrome CDP không (thay thế zendriver)."""
        # Kiểm tra Chrome có sẵn không (chỉ check 1 lần)
        if not LabsFlowClient._chrome_cdp_available:
            LabsFlowClient._check_zendriver_available()
        if not LabsFlowClient._chrome_cdp_available:
            return False
        cookie_hash = self._cookie_hash
        # Nếu Chrome CDP bị 403 quá nhiều → chuyển sang playwright
        cdp_403 = LabsFlowClient._chrome_cdp_consecutive_403.get(cookie_hash, 0)
        if cdp_403 >= self.MAX_CHROME_CDP_403:
            return False
        return True

    def _maybe_inject_recaptcha(
        self, 
        client_context: Dict[str, Any], 
        raise_on_fail: bool = True, 
        acquire_lock: bool = True,
        recaptcha_action: str = "VIDEO_GENERATION",
    ) -> bool:
        """If enabled, fetch reCAPTCHA token and inject into clientContext.
        
        ✅ TOKEN SOURCE PRIORITY:
        1. Zendriver (headed, undetected) → trust score cao
        2. Playwright (fallback) → sync API worker thread
        
        Args:
            raise_on_fail: If True, raise exception when token not available
            acquire_lock: Nếu True, tự acquire lock
            recaptcha_action: "VIDEO_GENERATION" cho video, "IMAGE_GENERATION" cho image
        Returns:
            True if token injected, False otherwise
        """
        if not self.auto_recaptcha:
            return False

        cookie_hash = self._cookie_hash
        print(f"  🔑 [reCAPTCHA] Cookie {cookie_hash[:8]}... đang yêu cầu token (action={recaptcha_action})...")
        
        token = None
        token_generated_at = None
        
        # ✅ SOURCE 1: Chrome CDP (ưu tiên - Chrome thật, trust score cao)
        if self._should_use_zendriver():
            print(f"  🔵 [Token] Thử Chrome CDP trước (Chrome thật, off-screen)...")
            try:
                token = self._get_recaptcha_token_zendriver(
                    timeout_s=60,
                    recaptcha_action=recaptcha_action,
                )
                if token and len(token.strip()) > 0:
                    token_generated_at = time.time()
                    self._record_token_source("chrome_cdp")
                    client_context["recaptchaToken"] = token
                    # ✅ Track token timestamp để kiểm tra freshness trước khi gọi API
                    LabsFlowClient._token_timestamps[cookie_hash] = token_generated_at
                    print(f"  ✅ [Chrome CDP] Token injected (len={len(token)}, ts={token_generated_at:.0f})")
                    return True
                else:
                    print(f"  ⚠️ [Chrome CDP] Không lấy được token, fallback Playwright...")
            except Exception as e:
                print(f"  ⚠️ [Chrome CDP] Error: {e}, fallback Playwright...")
        
        # ✅ SOURCE 2: Playwright (fallback)
        print(f"  🟡 [Token] Dùng Playwright...")
        token = self._get_recaptcha_token_with_playwright(
            timeout_s=90, 
            max_retries_on_403=3, 
            acquire_lock=acquire_lock,
            recaptcha_action=recaptcha_action,
        )
        
        if token and len(token.strip()) > 0:
            token_generated_at = time.time()
            self._record_token_source("playwright")
            client_context["recaptchaToken"] = token
            LabsFlowClient._token_timestamps[cookie_hash] = token_generated_at
            print(f"  ✅ [Playwright] Token injected (len={len(token)}, ts={token_generated_at:.0f})")
            return True
        
        # ✅ SOURCE 3: VPS API (remote fallback)
        vps_url = _env("VPS_RECAPTCHA_URL")
        if vps_url:
            print(f"  🔴 [Token] Fallback VPS API: {vps_url}...")
            try:
                cookie_str = "; ".join(f"{k}={v}" for k, v in self.cookies.items())
                resp = self.session.post(vps_url, json={"cookie": cookie_str, "action": recaptcha_action}, timeout=120)
                if resp.status_code == 200:
                    data = resp.json()
                    if data.get("ok") and data.get("token"):
                        token = data["token"]
                        token_generated_at = time.time()
                        self._record_token_source("vps_api")
                        client_context["recaptchaToken"] = token
                        LabsFlowClient._token_timestamps[cookie_hash] = token_generated_at
                        print(f"  ✅ [VPS API] Token injected (len={len(token)}, ts={token_generated_at:.0f})")
                        return True
                    else:
                        print(f"  ⚠️ [VPS API] Failed: {data.get('error', 'unknown')}")
                else:
                    print(f"  ⚠️ [VPS API] HTTP {resp.status_code}")
            except Exception as e:
                print(f"  ⚠️ [VPS API] Error: {e}")
        
        # Tất cả source đều fail
        if raise_on_fail:
            error_msg = f"Cannot get reCAPTCHA token from all sources (CDP, Playwright, VPS). {self.last_error_detail or ''}"
            self.last_error_detail = error_msg
            print(f"  ✗ Không thể lấy token từ tất cả source, raise exception...")
            raise RuntimeError(error_msg)
        
        return False
    
    def _convert_to_recaptcha_context(self, client_context: Dict[str, Any]) -> None:
        """Convert flat recaptchaToken → nested recaptchaContext format.
        
        Flow image API (batchGenerateImages) yêu cầu format:
            "recaptchaContext": {"token": "...", "applicationType": "RECAPTCHA_APPLICATION_TYPE_WEB"}
        
        Trong khi _maybe_inject_recaptcha inject dạng flat:
            "recaptchaToken": "..."
        """
        token = client_context.pop("recaptchaToken", None)
        if token:
            client_context["recaptchaContext"] = {
                "token": token,
                "applicationType": "RECAPTCHA_APPLICATION_TYPE_WEB",
            }

    def _verify_token_before_api_call(self, payload: Dict[str, Any]) -> bool:
        """Verify token có trong payload trước khi gọi API.
        
        Supports both flat format (recaptchaToken) and nested format (recaptchaContext.token).
        
        Returns:
            True nếu token OK, False nếu không có token (và set last_error_detail)
        """
        if not self.auto_recaptcha:
            return True  # Không cần verify nếu không dùng auto_recaptcha
        
        client_context = payload.get("clientContext") or payload.get("json", {}).get("clientContext") or payload
        token = None
        if isinstance(client_context, dict):
            # Check flat format first
            token = client_context.get("recaptchaToken")
            # Then check nested format
            if not token:
                recaptcha_ctx = client_context.get("recaptchaContext")
                if isinstance(recaptcha_ctx, dict):
                    token = recaptcha_ctx.get("token")
        
        if not token or len(token.strip()) == 0:
            self.last_error_detail = "Token không có trong payload trước khi gọi API"
            self.last_error = self.last_error_detail
            print(f"  ✗ {self.last_error_detail}")
            return False
        
        print(f"  ✅ Verified: Token có trong payload (length: {len(token)}) trước khi gọi API")
        return True
    
    def _is_token_fresh(self) -> bool:
        """Kiểm tra token hiện tại còn fresh không (< TOKEN_MAX_AGE_SECONDS).
        
        reCAPTCHA v3 token hết hạn sau 120s theo Google docs.
        Trả về False nếu token đã quá cũ hoặc chưa có timestamp.
        """
        cookie_hash = self._cookie_hash
        ts = LabsFlowClient._token_timestamps.get(cookie_hash)
        if ts is None:
            return False
        age = time.time() - ts
        is_fresh = age < self.TOKEN_MAX_AGE_SECONDS
        if not is_fresh:
            print(f"  ⏰ [Token Freshness] Token đã {age:.0f}s (max {self.TOKEN_MAX_AGE_SECONDS}s) → EXPIRED, cần lấy mới")
        return is_fresh
    
    def _ensure_fresh_token(
        self,
        client_context: Dict[str, Any],
        recaptcha_action: str = "VIDEO_GENERATION",
        acquire_lock: bool = False,
    ) -> bool:
        """Đảm bảo token trong client_context còn fresh trước khi gọi API.
        
        Nếu token đã expired (> TOKEN_MAX_AGE_SECONDS), tự động lấy token mới.
        Hàm này nên được gọi NGAY TRƯỚC khi gọi API (sau tất cả delay/rate-limit).
        
        Returns:
            True nếu token fresh (hoặc đã lấy mới thành công), False nếu fail.
        """
        if not self.auto_recaptcha:
            return True
        
        if self._is_token_fresh():
            return True
        
        # Token expired → lấy mới
        print(f"  🔄 [Token Freshness] Lấy token mới vì token cũ đã expired...")
        # Xóa token cũ khỏi client_context
        client_context.pop("recaptchaToken", None)
        client_context.pop("recaptchaContext", None)
        
        try:
            result = self._maybe_inject_recaptcha(
                client_context,
                raise_on_fail=True,
                acquire_lock=acquire_lock,
                recaptcha_action=recaptcha_action,
            )
            return result
        except RuntimeError as e:
            self.last_error_detail = f"Không thể lấy token mới (freshness check): {e}"
            print(f"  ✗ {self.last_error_detail}")
            return False
    
    def _generate_session_id(self) -> str:
        """Generate sessionId theo format flow2api: ';{timestamp_ms}'.
        
        flow2api luôn gửi sessionId trong clientContext. Format: ";{unix_timestamp_ms}"
        Điều này giúp Google Labs tracking session và có thể ảnh hưởng đến reCAPTCHA trust score.
        """
        return f";{int(time.time() * 1000)}"
    
    def _notify_captcha_error_self_heal(self, error_code: int, error_msg: str) -> None:
        """Thông báo cho browser captcha service về lỗi để tự phục hồi.
        
        Tham khảo flow2api: _notify_browser_captcha_error - khi gặp 403/429,
        thông báo cho browser captcha service để nó có thể:
        - Reset browser context
        - Xoay proxy
        - Refresh cookies
        
        Trong implementation của chúng ta, trigger zendriver page reset khi gặp 403.
        """
        cookie_hash = self._cookie_hash
        try:
            if error_code == 403:
                # Reset zendriver page cho cookie này để lần sau lấy token mới từ page sạch
                if cookie_hash in LabsFlowClient._zendriver_pages:
                    print(f"  🔄 [Self-Heal] Đánh dấu zendriver page cần reset cho cookie {cookie_hash[:8]}...")
                    LabsFlowClient._zendriver_reset_page(cookie_hash)
                
                # Đánh dấu playwright context cần reset
                with LabsFlowClient._contexts_need_reset_lock:
                    LabsFlowClient._contexts_need_reset[cookie_hash] = True
                    print(f"  🔄 [Self-Heal] Đánh dấu playwright context cần reset cho cookie {cookie_hash[:8]}...")
            
            elif error_code == 429:
                # Xoay proxy nếu có proxy pool
                if LabsFlowClient._use_proxy_pool:
                    new_proxy = LabsFlowClient._rotate_proxy_for_cookie(cookie_hash)
                    if new_proxy:
                        self._apply_proxy_to_session(new_proxy)
                        print(f"  🔄 [Self-Heal] Đã xoay proxy cho cookie {cookie_hash[:8]}... sau 429")
        except Exception as e:
            print(f"  ⚠️ [Self-Heal] Lỗi khi tự phục hồi: {e}")
    
    def _should_use_simple_prompt_format(self) -> bool:
        """Kiểm tra xem có nên dùng format prompt đơn giản không.
        
        flow2api dùng format đơn giản: {"textInput": {"prompt": "..."}}
        Chúng ta dùng format phức tạp: {"textInput": {"structuredPrompt": {"parts": [{"text": "..."}]}}}
        
        Khi gặp 403 liên tiếp, thử chuyển sang format đơn giản như flow2api.
        """
        cookie_hash = self._cookie_hash
        counter_403 = LabsFlowClient._shared_403_counters.get(cookie_hash, 0)
        # Sau 2 lần 403 liên tiếp, thử format đơn giản
        return counter_403 >= 2
    
    @staticmethod
    def _map_image_aspect(aspect: Optional[str]) -> str:
        """Map human aspect inputs to IMAGE_ASPECT_RATIO_* constants."""
        try:
            if not aspect:
                return "IMAGE_ASPECT_RATIO_LANDSCAPE"
            s = str(aspect).strip()
            if s.startswith("IMAGE_ASPECT_RATIO_"):
                return s
            if s in {"16:9", "16X9", "LANDSCAPE"}:
                return "IMAGE_ASPECT_RATIO_LANDSCAPE"
            if s in {"9:16", "9X16", "PORTRAIT"}:
                return "IMAGE_ASPECT_RATIO_PORTRAIT"
            if s in {"1:1", "1X1", "SQUARE"}:
                return "IMAGE_ASPECT_RATIO_SQUARE"
            if s in {"4:3", "4X3", "LANDSCAPE_FOUR_THREE", "FOUR_THREE"}:
                return "IMAGE_ASPECT_RATIO_LANDSCAPE_FOUR_THREE"
            if s in {"3:4", "3X4", "PORTRAIT_THREE_FOUR", "THREE_FOUR"}:
                return "IMAGE_ASPECT_RATIO_PORTRAIT_THREE_FOUR"
        except Exception:
            pass
        return "IMAGE_ASPECT_RATIO_LANDSCAPE"

    @staticmethod
    def _map_video_aspect(aspect: Optional[str]) -> str:
        """Map human aspect inputs to VIDEO_ASPECT_RATIO_* constants."""
        try:
            if not aspect:
                return "VIDEO_ASPECT_RATIO_LANDSCAPE"
            s = str(aspect).strip()
            if s.startswith("VIDEO_ASPECT_RATIO_"):
                return s
            if s in {"16:9", "16X9", "LANDSCAPE"}:
                return "VIDEO_ASPECT_RATIO_LANDSCAPE"
            if s in {"9:16", "9X16", "PORTRAIT"}:
                return "VIDEO_ASPECT_RATIO_PORTRAIT"
        except Exception:
            pass
        return "VIDEO_ASPECT_RATIO_LANDSCAPE"

    @staticmethod
    def _get_effective_model(base_key: str, aspect_ratio: str) -> str:
        """Return model key - override portrait/landscape tương ứng."""
        try:
            mapped_aspect = LabsFlowClient._map_video_aspect(aspect_ratio)
            
            # Detect mode: relaxed, fast, quality
            is_relaxed = "relaxed" in base_key
            is_fl = "_fl" in base_key  # start-end mode
            
            # Portrait (9:16) → map sang model portrait
            if mapped_aspect == "VIDEO_ASPECT_RATIO_PORTRAIT":
                # Nếu đã có portrait trong key → giữ nguyên
                if "portrait" in base_key:
                    return base_key
                
                # R2V models
                if "r2v" in base_key:
                    if is_relaxed:
                        return "veo_3_1_r2v_fast_portrait_ultra_relaxed"
                    else:
                        return "veo_3_1_r2v_fast_portrait_ultra"
                
                # I2V start-end models
                if "i2v" in base_key and is_fl:
                    if is_relaxed:
                        return "veo_3_1_i2v_s_fast_portrait_fl_ultra_relaxed"
                    elif "fast" in base_key:
                        return "veo_3_1_i2v_s_fast_portrait_ultra_fl"
                    else:
                        return "veo_3_1_i2v_s_portrait_fl"
                
                # I2V single image models
                if "i2v" in base_key:
                    if is_relaxed:
                        return "veo_3_1_i2v_s_fast_portrait_ultra_relaxed"
                    elif "fast" in base_key:
                        return "veo_3_1_i2v_s_fast_portrait_ultra"
                    else:
                        return "veo_3_1_i2v_s_portrait"
                
                # T2V models
                if "t2v" in base_key:
                    if is_relaxed:
                        return "veo_3_1_t2v_fast_portrait_ultra_relaxed"
                    elif "fast" in base_key:
                        return "veo_3_1_t2v_fast_portrait_ultra"
                    else:
                        return "veo_3_1_t2v_portrait"
                
                # Fallback: giữ nguyên
                return base_key
            
            # Landscape (16:9) → map sang model landscape
            if mapped_aspect == "VIDEO_ASPECT_RATIO_LANDSCAPE":
                # Nếu đã có landscape trong key → giữ nguyên
                if "landscape" in base_key:
                    return base_key
                
                # R2V models
                if "r2v" in base_key:
                    if is_relaxed:
                        return "veo_3_1_r2v_fast_landscape_ultra_relaxed"
                    else:
                        return "veo_3_1_r2v_fast_landscape_ultra"
                
                # I2V start-end models
                if "i2v" in base_key and is_fl:
                    if is_relaxed:
                        return "veo_3_1_i2v_s_fast_landscape_fl_ultra_relaxed"
                    elif "fast" in base_key:
                        return "veo_3_1_i2v_s_fast_landscape_ultra_fl"
                    else:
                        return "veo_3_1_i2v_s_landscape_fl"
                
                # I2V single image models - KHÔNG thêm "_landscape" suffix (theo curl example)
                if "i2v" in base_key:
                    if is_relaxed:
                        return "veo_3_1_i2v_s_fast_ultra_relaxed"
                    elif "fast" in base_key:
                        return "veo_3_1_i2v_s_fast_ultra"
                    else:
                        return "veo_3_1_i2v_s"
                
                # T2V models - Landscape: KHÔNG có "landscape" trong tên
                if "t2v" in base_key:
                    if is_relaxed:
                        return "veo_3_1_t2v_fast_ultra_relaxed"
                    elif "fast" in base_key:
                        return "veo_3_1_t2v_fast_ultra"
                    else:
                        return "veo_3_1_t2v"
                
                # Fallback: giữ nguyên
                return base_key
            
            # Fallback: giữ nguyên
            return base_key
            
        except Exception:
            return base_key
    
    def fetch_access_token(self) -> bool:
        """Fetch access token from labs session. Returns True if successful."""
        try:
            print("→ Fetching access token from labs session...")
            url = "https://labs.google/fx/api/auth/session"
            
            # ✅ Lưu token cũ để detect token không thay đổi
            old_token = getattr(self, 'access_token', None)
            
            resp = self.session.get(
                url,
                headers=self._labs_headers(),
                cookies=self.cookies,
                timeout=60,
            )
            resp.raise_for_status()
            
            if resp.status_code != 200:
                print(f"  ✗ Session returned status {resp.status_code}")
                return False
            
            try:
                data = resp.json()
            except Exception:
                print("  ✗ Session response is not JSON")
                return False
            
            # Try direct access_token field first
            token = None
            if isinstance(data, dict):
                token = data.get("access_token")
                # ✅ Log thêm thông tin session để debug 401
                expires_str = data.get("expires") or data.get("accessTokenExpires") or data.get("exp")
                if expires_str:
                    print(f"  📋 [Session] Token expires: {expires_str}")
                # Log session user info nếu có
                user_info = data.get("user", {})
                if user_info:
                    print(f"  📋 [Session] User: {user_info.get('email', user_info.get('name', 'unknown'))}")
                if not token:
                    token = _extract_bearer_like(data)
            
            if not token:
                print("  ✗ No access_token found in session response")
                print(f"  Response: {json.dumps(data, indent=2)}")
                # Fallbacks: env vars and local files
                # 1) Environment variables
                env_token = _normalize_bearer(_env("ACCESS_TOKEN") or _env("BEARER_TOKEN"))
                if env_token:
                    self.access_token = env_token
                    print("  ✓ Access token from environment variables")
                    return True
                # 2) Local files commonly used to store bearer tokens
                candidate_files = [
                    _env("BEARER_TOKEN_FILE"),
                    "bearer.token",
                    "access_token.txt",
                    "token.txt",
                ]
                for fp in candidate_files:
                    if not fp:
                        continue
                    content = _read_file(fp)
                    token_from_file = _normalize_bearer(content)
                    if token_from_file:
                        self.access_token = token_from_file
                        print(f"  ✓ Access token loaded from file: {fp}")
                        return True
                return False
            
            self.access_token = token
            
            # ✅ Detect token không thay đổi → có thể session expired
            if old_token and token == old_token:
                if not hasattr(self, '_same_token_count'):
                    self._same_token_count = 0
                self._same_token_count += 1
                print(f"  ⚠️ [Token] Cùng token sau {self._same_token_count} lần fetch (có thể session expired)")
                # Log full response khi token không đổi nhiều lần
                if self._same_token_count >= 2:
                    print(f"  📋 [Token Debug] Full session response keys: {list(data.keys()) if isinstance(data, dict) else type(data)}")
                    if isinstance(data, dict):
                        # Log token length và expiry info
                        print(f"  📋 [Token Debug] Token length: {len(token)}, starts: {token[:30]}..., ends: ...{token[-20:]}")
            else:
                self._same_token_count = 0
            
            print(f"  ✓ Access token retrieved: {token[:20]}...")
            return True
            
        except Exception as e:
            print(f"  ✗ Failed to fetch access token: {e}")
            return False
    
    def set_video_model_key(self, model_key: str) -> bool:
        """Bỏ qua bước set model key - không cần gọi API setLastSelectedVideoModelKey nữa."""
        print(f"  ✓ Skip setLastSelectedVideoModelKey (không cần thiết), model: {model_key}")
        return True
    
    def submit_batch_log(self, tool: str) -> bool:
        """Submit batch log event."""
        try:
            print("→ Submitting batch log...")
            url = "https://labs.google/fx/api/trpc/general.submitBatchLog"
            app_events = [{
                "event": "VIDEOFX_CREATE_VIDEO",
                "eventProperties": [
                    {"key": "TOOL_NAME", "stringValue": tool},
                    {"key": "QUERY_ID", "stringValue": f"PINHOLE_MAIN_VIDEO_GENERATION_CACHE_ID{uuid.uuid4()}"},
                    {"key": "USER_AGENT", "stringValue": self.user_agent},
                    {"key": "IS_DESKTOP"},
                ],
                "activeExperiments": [],
                "eventMetadata": {"sessionId": f";{int(time.time()*1000)}"},
                "eventTime": time.strftime("%Y-%m-%dT%H:%M:%S.000Z", time.gmtime()),
            }]
            payload = {"json": {"appEvents": app_events}}
            resp = self.session.post(
                url,
                headers=self._labs_headers(),
                cookies=self.cookies,
                data=json.dumps(payload),
                timeout=60,
            )
            resp.raise_for_status()
            print("  ✓ Batch log submitted")
            return True
        except Exception as e:
            print(f"  ✗ Failed to submit batch log: {e}")
            return False
    
    def submit_flow_image_log(
        self,
        session_id: Optional[str],
        tool_name: str = "PINHOLE",
        paygate_tier: str = "PAYGATE_TIER_TWO",
        prompt_box_mode: str = "IMAGE_GENERATION",
    ) -> bool:
        """Submit Flow image generation batch log (similar to Whisk tab logging)."""
        try:
            url = "https://labs.google/fx/api/trpc/general.submitBatchLog"
            effective_session = session_id or f";{int(time.time() * 1000)}"
            event_time = time.strftime("%Y-%m-%dT%H:%M:%S.000Z", time.gmtime())
            payload = {
                "json": {
                    "appEvents": [{
                        "event": "PINHOLE_GENERATE_IMAGE",
                        "eventMetadata": {"sessionId": effective_session},
                        "eventProperties": [
                            {"key": "TOOL_NAME", "stringValue": tool_name},
                            {"key": "G1_PAYGATE_TIER", "stringValue": paygate_tier},
                            {"key": "PINHOLE_PROMPT_BOX_MODE", "stringValue": prompt_box_mode},
                            {"key": "USER_AGENT", "stringValue": self.user_agent},
                            {"key": "IS_DESKTOP"},
                        ],
                        "activeExperiments": [],
                        "eventTime": event_time,
                    }]
                }
            }
            resp = self.session.post(
                url,
                headers=self._labs_headers(),
                cookies=self.cookies,
                data=json.dumps(payload),
                timeout=60,
            )
            resp.raise_for_status()
            print("  ✓ Flow batch log submitted")
            return True
        except Exception as e:
            print(f"  ⚠️ Failed to submit flow batch log: {e}")
            return False

    def generate_videos(
        self,
        project_id: str,
        tool: str,
        user_tier: str,
        prompt: str,
        model_key: str,
        num_videos: int = 4,
        aspect_ratio: str = "VIDEO_ASPECT_RATIO_LANDSCAPE",
        fixed_seed: Optional[int] = None,
    ) -> Optional[List[Dict[str, Any]]]:
        """Generate videos using batch async API with HIGH_TRAFFIC retry."""
        # ✅ Log cookie hash để debug - xem cookie nào đang generate video
        cookie_hash = self._cookie_hash
        print(f"  🎬 [Generate Video] Cookie {cookie_hash[:8]}... đang generate video: '{prompt[:50]}...'")
        
        url = "https://aisandbox-pa.googleapis.com/v1/video:batchAsyncGenerateVideoText"
        
        # Generate scene IDs and seeds - use fixed seed if provided, otherwise random
        scene_ids = [str(uuid.uuid4()) for _ in range(num_videos)]
        if fixed_seed is not None:
            seeds = [fixed_seed] * num_videos
        else:
            seeds = [int(time.time() * 1000000 + i) % 100000 for i in range(num_videos)]
        
        # Map aspect ratio and adjust model if needed
        mapped_aspect = self._map_video_aspect(aspect_ratio)
        effective_model = self._get_effective_model(model_key, mapped_aspect)
        
        # Bỏ check live status - chạy trực tiếp
        requests_body = []
        for i in range(num_videos):
            requests_body.append({
                "aspectRatio": mapped_aspect,
                "seed": seeds[i],
                "textInput": {"structuredPrompt": {"parts": [{"text": prompt}]}},
                "videoModelKey": effective_model,
                "metadata": {},
            })
        
        # ✅ Payload khớp WebUI: thêm mediaGenerationContext và useV2ModelConfig
        # ✅ FIX (flow2api fallback): Thêm sessionId vào clientContext - flow2api luôn gửi sessionId
        batch_id = str(uuid.uuid4())
        session_id = self._generate_session_id()
        payload = {
            "mediaGenerationContext": {"batchId": batch_id},
            "clientContext": {
                "sessionId": session_id,
                "projectId": project_id,
                "tool": tool,
                "userPaygateTier": user_tier,
            },
            "requests": requests_body,
            "useV2ModelConfig": True,
        }
        
        # ✅ Lock được giữ liên tục từ khi request token đến khi gọi API xong (nối đuôi hoàn toàn)
        # Đảm bảo không có khoảng trống giữa các bước
        with self._token_and_api_with_lock():
            # ✅ Retry logic: KHÔNG retry khi gặp 429 (để GUI xử lý đổi cookie)
            # Chỉ retry cho các lỗi khác (HIGH_TRAFFIC 500, network errors, etc.)
            max_retries = 7 # Tăng lên 7 để đủ logic 6 lần lỗi -> reset driver
            for attempt in range(max_retries):
                # ✅ MỖI ATTEMPT LẤY TOKEN MỚI – tránh dùng lại token cũ dễ gây 403
                try:
                    self._maybe_inject_recaptcha(
                        payload["clientContext"],
                        raise_on_fail=True,
                        acquire_lock=False,
                        recaptcha_action="VIDEO_GENERATION",  # ✅ Dùng VIDEO_GENERATION cho video
                    )
                    # ✅ Delay nhỏ sau khi có token để Google Labs validate token (giảm vì nối đuôi)
                    time.sleep(0.1)
                except RuntimeError as e:
                    self.last_error_detail = str(e)
                    self.last_error = str(e)
                    print(f"  ✗ Không thể lấy reCAPTCHA token: {e}")
                    return None
                
                # ✅ VERIFY: Đảm bảo token đã được inject vào payload trước khi gọi API
                if not self._verify_token_before_api_call(payload):
                    return None
                
                # ✅ CRITICAL: Chuyển recaptchaToken → recaptchaContext (theo format chuẩn cho tất cả video APIs)
                self._convert_to_recaptcha_context(payload["clientContext"])
                
                try:
                    # ✅ Rate limiting và warm-up delay per cookie (chỉ cho attempt đầu tiên)
                    # ✅ Rate limiting instance-level - mỗi cookie có rate limit riêng
                    if attempt == 0:
                        current_time = time.time()
                        elapsed = current_time - self._last_api_call_time
                        
                        # ✅ Warm-up delay cho 10 request đầu tiên của cookie này
                        if self._api_call_count < 10:
                            warmup_delay = 0.3  # 0.3 giây cho các request đầu tiên (giảm từ 2.0s)
                            total_wait = warmup_delay
                            if elapsed < self._min_api_call_interval:
                                total_wait = max(warmup_delay, self._min_api_call_interval - elapsed)
                            if total_wait > 0:
                                print(f"  ⏳ Warm-up delay {total_wait:.1f}s (cookie: {self._cookie_hash[:8]}..., request #{self._api_call_count + 1})...")
                                time.sleep(total_wait)
                            self._api_call_count += 1
                        else:
                            # Sau 10 request đầu tiên, chỉ dùng interval bình thường
                            if elapsed < self._min_api_call_interval:
                                wait_time = self._min_api_call_interval - elapsed
                                time.sleep(wait_time)
                            self._api_call_count += 1
                        
                        self._last_api_call_time = time.time()
                    
                    print(f"→ Generating {num_videos} videos (attempt {attempt + 1}/{max_retries}): '{prompt[:50]}...'")
                    
                    # ✅ FIX: Freshness check ngay trước khi gọi API - nếu token expired thì lấy mới
                    if not self._ensure_fresh_token(payload["clientContext"], recaptcha_action="VIDEO_GENERATION", acquire_lock=False):
                        self.last_error_detail = "Token expired và không thể lấy mới trước khi gọi API"
                        return None
                    # ✅ Convert lại format nếu token mới được inject
                    self._convert_to_recaptcha_context(payload["clientContext"])
                    
                    resp = self.session.post(
                        url,
                        headers=self._aisandbox_headers(),
                        data=json.dumps(payload),
                        timeout=120,
                    )
                    
                    # ✅ Check 429/400/401 using unified error handler
                    if resp.status_code in [400, 401, 429]:
                        if resp.status_code == 401:
                            print(f"  ⚠️ Lỗi 401 Debug Payload: {json.dumps(payload)}")
                            # ✅ FIX: Xử lý 401 thông minh - refresh token + cookies nếu cần
                            self._handle_401_refresh_token()
                        # Set flag bị chặn cho 429 (rate limit/IP blocked)
                        if resp.status_code == 429:
                            cookie_hash = self._cookie_hash
                            # ✅ FIX (flow2api): Thông báo cho captcha service tự phục hồi
                            self._notify_captcha_error_self_heal(429, resp.text[:200])
                            with LabsFlowClient._recaptcha_cookie_blocked_lock:
                                if not hasattr(LabsFlowClient, '_recaptcha_cookie_blocked_flags'):
                                    LabsFlowClient._recaptcha_cookie_blocked_flags = {}
                                LabsFlowClient._recaptcha_cookie_blocked_flags[cookie_hash] = True
                                print(f"  ⚠️ [API] Đã set flag bị chặn cho cookie: {cookie_hash[:8]}... (429 Rate Limit)")
                        
                        error_msg = f"{resp.status_code} Client Error: {resp.text[:200]}"
                        print(f"  ⚠️ {resp.status_code} Error - Checking retry logic...")
                        
                        # ✅ Dùng unified error handler để xử lý lỗi
                        if self._handle_error_and_maybe_reset(resp.status_code, error_msg):
                            print(f"  🔄 [T2V] Đã reset BrowserContext, retry với context mới...")
                            continue  # Retry với context mới
                        
                        # Chưa đến ngưỡng reset, retry với exponential backoff
                        if resp.status_code == 429:
                            wait_time = LabsFlowClient.calculate_retry_delay(attempt, 429, base_delay=5.0)
                            print(f"  ⚠️ 429 Rate Limit - Waiting {wait_time:.1f}s before retry...")
                            time.sleep(wait_time)
                            continue
                        
                        # For 400/401, retry với exponential backoff
                        print(f"  ⚠️ {resp.status_code} - Retry info: {resp.text[:100]}")
                        if attempt < max_retries - 1:
                            wait_time = LabsFlowClient.calculate_retry_delay(attempt, resp.status_code, base_delay=2.0)
                            time.sleep(wait_time)
                            continue

                    # ✅ Check 403 - Token score thấp, cần lấy token mới
                    if resp.status_code == 403:
                        # ✅ Track token source 403
                        self._on_api_403()
                        
                        # ✅ FIX (flow2api): Thông báo cho captcha service tự phục hồi
                        self._notify_captcha_error_self_heal(403, resp.text[:200])
                        
                        # Set flag bị chặn để worker thread có thể renew cookie
                        cookie_hash = self._cookie_hash
                        with LabsFlowClient._recaptcha_cookie_blocked_lock:
                            if not hasattr(LabsFlowClient, '_recaptcha_cookie_blocked_flags'):
                                LabsFlowClient._recaptcha_cookie_blocked_flags = {}
                            LabsFlowClient._recaptcha_cookie_blocked_flags[cookie_hash] = True
                            print(f"  ⚠️ [API] Đã set flag bị chặn cho cookie: {cookie_hash[:8]}... (403)")
                        
                        # ✅ FIX (flow2api fallback): Sau 2 lần 403, thử format prompt đơn giản
                        if self._should_use_simple_prompt_format():
                            print(f"  🔄 [T2V Fallback] Chuyển sang format prompt đơn giản (flow2api style)...")
                            for req in payload.get("requests", []):
                                text_input = req.get("textInput", {})
                                # Lấy text từ structuredPrompt nếu có
                                sp = text_input.get("structuredPrompt", {})
                                parts = sp.get("parts", [])
                                if parts and isinstance(parts[0], dict):
                                    original_text = parts[0].get("text", prompt)
                                    req["textInput"] = {"prompt": original_text}
                        
                        # ✅ XỬ LÝ 403: Refresh cookie - xóa cookie cũ, reload để lấy cookie mới
                        # Retry tối đa 3 lần như yêu cầu
                        max_403_retries = 3
                        if not hasattr(self, '_403_refresh_retries'):
                            self._403_refresh_retries = {}
                        current_403_retries = self._403_refresh_retries.get(cookie_hash, 0)
                        
                        if current_403_retries < max_403_retries:
                            self._403_refresh_retries[cookie_hash] = current_403_retries + 1
                            print(f"  🔄 [T2V] 403 # {current_403_retries + 1}/{max_403_retries} - Refresh cookie & retry...")
                            
                            # Gọi hàm refresh cookie
                            self._refresh_cookie_on_403()
                            
                            # Đợi 1 giây trước khi retry
                            time.sleep(1)
                            continue  # Retry với cookie mới
                        else:
                            # Đã retry 3 lần mà vẫn 403 → báo fail
                            self._403_refresh_retries[cookie_hash] = 0  # Reset for next time
                            self.last_error_detail = f"403 Forbidden sau {max_403_retries} lần refresh cookie"
                            self.last_error = self.last_error_detail
                            print(f"  ❌ [T2V] 403 sau {max_403_retries} lần refresh cookie - Bỏ qua task này")
                            return None
                    
                    # Check for HIGH_TRAFFIC error (500)
                    if resp.status_code == 500:
                        try:
                            error_data = resp.json()
                            error_msg = json.dumps(error_data)
                            if "PUBLIC_ERROR_HIGH_TRAFFIC" in error_msg or "HIGH_TRAFFIC" in error_msg:
                                if attempt < max_retries - 1:
                                    wait_time = LabsFlowClient.calculate_retry_delay(attempt, 500, base_delay=5.0)
                                    print(f"  ⚠️ VEO 3 quá tải, chờ {wait_time:.1f}s và thử lại...")
                                    time.sleep(wait_time)
                                    continue
                        except:
                            pass
                    
                    resp.raise_for_status()
                    result = resp.json()
                    print(f"  ✓ Video generation started")
                    
                    # ✅ Reset 403 counter khi thành công (reset cả counter chung)
                    self._reset_403_counter_for_cookie()
                    self._on_api_success()  # ✅ Reset zendriver/playwright 403 counters
                    
                    # Extract operations for status checking
                    operations = []
                    if isinstance(result, dict) and "operations" in result:
                        for i, op in enumerate(result["operations"]):
                            operations.append({
                                "operation": {"name": op.get("operation", {}).get("name", "")},
                                "sceneId": scene_ids[i],
                                "status": "MEDIA_GENERATION_STATUS_PENDING",
                            })
                    else:
                        for scene_id in scene_ids:
                            operations.append({
                                "operation": {"name": str(uuid.uuid4()).replace('-', '')},
                                "sceneId": scene_id,
                                "status": "MEDIA_GENERATION_STATUS_PENDING",
                            })
                    
                    return operations
                
                except Exception as e:
                    error_str = str(e)
                    # ✅ Check 429 in exception với unified error handler
                    if "429" in error_str or "Too Many Requests" in error_str:
                         print(f"  ⚠️ 429 Rate Limit in exception...")
                         if self._handle_error_and_maybe_reset(429, error_str):
                             print(f"  🔄 [T2V] Đã reset BrowserContext, retry với context mới...")
                             continue
                         # If not reset yet, retry backoff
                         if attempt < max_retries - 1:
                            time.sleep(5)
                            continue

                    # ✅ Các lỗi khác: retry như bình thường
                    if attempt < max_retries - 1:
                        wait_time = LabsFlowClient.calculate_retry_delay(attempt, 0, base_delay=5.0)
                        print(f"  ⚠️ Lỗi (attempt {attempt + 1}): {error_str[:100]}, retry sau {wait_time:.1f}s...")
                        time.sleep(wait_time)
                    else:
                        print(f"  ✗ Failed after {max_retries} attempts: {e}")
                        # Store error detail for GUI
                        self.last_error_detail = error_str
                        self.last_error = error_str
                        return None
        
        return None
    
    def check_video_status(self, operations: List[Dict[str, Any]]) -> Optional[Dict[str, Any]]:
        """Check the status of video generation operations."""
        try:
            url = "https://aisandbox-pa.googleapis.com/v1/video:batchCheckAsyncVideoGenerationStatus"
            payload = {"operations": operations}
            
            resp = self.session.post(
                url,
                headers=self._aisandbox_headers(),
                data=json.dumps(payload),
                timeout=120,
            )
            
            # ✅ FIX: Re-fetch access token khi gặp 401 và retry 1 lần
            if resp.status_code == 401:
                print(f"  ⚠️ [Poll] 401 - Re-fetch access token...")
                if self.fetch_access_token():
                    print(f"  ✅ [Poll] Access token refreshed, retry polling...")
                    resp = self.session.post(
                        url,
                        headers=self._aisandbox_headers(),
                        data=json.dumps(payload),
                        timeout=120,
                    )
            
            resp.raise_for_status()
            return resp.json()
            
        except Exception as e:
            print(f"  ✗ Failed to check video status: {e}")
            return None

    def upload_image(self, image_path: str, max_retries: int = 3) -> Optional[str]:
        """Upload image and return media ID.
        
        Args:
            image_path: Path to image file
            max_retries: Maximum number of retry attempts (default: 3)
        
        Returns:
            Media ID string or None on failure
        """
        for attempt in range(max_retries):
            try:
                if attempt > 0:
                    print(f"→ Uploading image (attempt {attempt + 1}/{max_retries}): {image_path}")
                else:
                    print(f"→ Uploading image: {image_path}")
                self.last_error_detail = None
                
                # First submit batch log for image upload
                url = "https://labs.google/fx/api/trpc/general.submitBatchLog"
                app_events = [{
                    "event": "PINHOLE_UPLOAD_IMAGE",
                    "eventProperties": [
                        {"key": "TOOL_NAME", "stringValue": "BACKBONE"},
                        {"key": "USER_AGENT", "stringValue": self.user_agent},
                        {"key": "IS_DESKTOP"},
                    ],
                    "activeExperiments": [],
                    "eventMetadata": {"sessionId": f";{int(time.time()*1000)}"},
                    "eventTime": time.strftime("%Y-%m-%dT%H:%M:%S.000Z", time.gmtime()),
                }]
                payload = {"json": {"appEvents": app_events}}
                resp = self.session.post(
                    url,
                    headers=self._labs_headers(),
                    cookies=self.cookies,
                    data=json.dumps(payload),
                    timeout=60,
                )
                resp.raise_for_status()
                print("  ✓ Upload batch log submitted")
                
                # Load and encode image
                with open(image_path, 'rb') as f:
                    image_data = f.read()
                
                # Convert to base64
                image_b64 = base64.b64encode(image_data).decode('utf-8')
                
                # Get image dimensions for aspect ratio
                with Image.open(image_path) as img:
                    width, height = img.size
                    if width > height:
                        aspect_ratio = "IMAGE_ASPECT_RATIO_LANDSCAPE"
                    elif height > width:
                        aspect_ratio = "IMAGE_ASPECT_RATIO_PORTRAIT"
                    else:
                        aspect_ratio = "IMAGE_ASPECT_RATIO_SQUARE"
                
                # Upload image via Flow endpoint (returns media.name UUID)
                upload_url = "https://aisandbox-pa.googleapis.com/v1/flow/uploadImage"
                # Detect mime type
                mime = "image/jpeg"
                if image_path.lower().endswith(".png"):
                    mime = "image/png"
                elif image_path.lower().endswith(".webp"):
                    mime = "image/webp"
                upload_payload = {
                    "clientContext": {
                        "projectId": self.flow_project_id,
                        "tool": "PINHOLE",
                    },
                    "imageBytes": image_b64,
                    "isUserUploaded": True,
                    "isHidden": False,
                    "mimeType": mime,
                    "fileName": os.path.basename(image_path),
                }
                
                resp = self.session.post(
                    upload_url,
                    headers=self._aisandbox_headers(),
                    data=json.dumps(upload_payload),
                    timeout=120,
                )
                
                # ✅ Handle upload errors with retry and unified error handler
                if resp.status_code != 200:
                    # ✅ FIX: Re-fetch access token khi gặp 401 (token expired)
                    if resp.status_code == 401:
                        print(f"  🔄 [Upload] 401 - Re-fetch access token...")
                        if self.fetch_access_token():
                            print(f"  ✅ [Upload] Access token refreshed: {self.access_token[:20]}...")
                        else:
                            print(f"  ❌ [Upload] Không thể refresh access token")
                        if attempt < max_retries - 1:
                            time.sleep(1)
                            continue
                    
                    try:
                        err_json = resp.json()
                        error_detail = json.dumps(err_json, ensure_ascii=False)
                    except Exception:
                        error_detail = f"status={resp.status_code} text={resp.text[:400]}"
                    
                    self.last_error_detail = f"Upload image failed: {error_detail}"
                    print(f"  ⚠️ Upload image failed (attempt {attempt + 1}/{max_retries}): {error_detail[:200]}")
                    
                    # ✅ Gọi unified error handler để track lỗi và reset context nếu cần
                    if resp.status_code in (403, 429):
                        did_reset = self._handle_error_and_maybe_reset(resp.status_code, error_detail)
                        if did_reset:
                            print(f"  🔄 [Upload] Đã reset BrowserContext, retry upload...")
                    
                    # ✅ Retry on 5xx server errors or network errors
                    if resp.status_code >= 500 and attempt < max_retries - 1:
                        wait_time = LabsFlowClient.calculate_retry_delay(attempt, 500, base_delay=3.0)
                        print(f"  → Server error, retry sau {wait_time:.1f}s...")
                        time.sleep(wait_time)
                        continue
                    elif resp.status_code == 429 and attempt < max_retries - 1:
                        wait_time = LabsFlowClient.calculate_retry_delay(attempt, 429, base_delay=5.0)
                        print(f"  → Rate limit, retry sau {wait_time:.1f}s...")
                        time.sleep(wait_time)
                        continue
                    elif resp.status_code == 403 and attempt < max_retries - 1:
                        wait_time = LabsFlowClient.calculate_retry_delay(attempt, 403, base_delay=3.0)
                        print(f"  → 403 Forbidden, retry sau {wait_time:.1f}s...")
                        time.sleep(wait_time)
                        continue
                    elif attempt < max_retries - 1:
                        wait_time = LabsFlowClient.calculate_retry_delay(attempt, resp.status_code, base_delay=2.0)
                        print(f"  → Retry sau {wait_time:.1f}s...")
                        time.sleep(wait_time)
                        continue
                    else:
                        print(f"  ✗ Upload image failed sau {max_retries} attempts")
                        return None
                
                result = resp.json()
                print("  ✓ Image uploaded successfully")
                
                # ✅ Reset error counters khi upload thành công
                self._reset_all_error_counters()
                
                print(f"  Response: {json.dumps(result, indent=2)}")
                
                # Extract media ID from response
                media_id = None
                if isinstance(result, dict):
                    # Try media.name (new response format)
                    media_obj = result.get("media")
                    if isinstance(media_obj, dict):
                        media_id = media_obj.get("name")
                    if not media_id:
                        # Try direct mediaId
                        media_id = result.get("mediaId")
                    if not media_id:
                        # Try mediaGenerationId structure
                        media_gen = result.get("mediaGenerationId", {})
                        if isinstance(media_gen, dict):
                            media_id = media_gen.get("mediaGenerationId")
                    if not media_id:
                        # Try nested imageData paths
                        image_data = result.get("imageData", {})
                        media_id = image_data.get("mediaId") or image_data.get("mediaGenerationId")
                
                if media_id:
                    print(f"  ✓ Media ID: {media_id}")
                    return media_id
                else:
                    print("  ✗ No media ID found in response")
                    print(f"  Available keys: {list(result.keys()) if isinstance(result, dict) else 'Not a dict'}")
                    # ✅ Retry if no media ID found
                    if attempt < max_retries - 1:
                        wait_time = (attempt + 1) * 2
                        print(f"  → Retry sau {wait_time}s...")
                        time.sleep(wait_time)
                        continue
                    return None
                    
            except Exception as e:
                print(f"  ⚠️ Upload image error (attempt {attempt + 1}/{max_retries}): {e}")
                if attempt < max_retries - 1:
                    wait_time = (attempt + 1) * 2
                    print(f"  → Retry sau {wait_time}s...")
                    time.sleep(wait_time)
                    continue
                else:
                    print(f"  ✗ Failed to upload image sau {max_retries} attempts: {e}")
                    return None
        
        return None

    def generate_videos_from_image(
        self,
        project_id: str,
        tool: str,
        user_tier: str,
        prompt: str,
        media_id: str,
        model_key: str = "veo_3_1_i2v_s_fast_ultra",
        num_videos: int = 4,
        aspect_ratio: str = "VIDEO_ASPECT_RATIO_LANDSCAPE",
        fixed_seed: Optional[int] = None,
        crop_coordinates: Optional[Dict[str, float]] = None,  # ✅ Thêm cropCoordinates
    ) -> Optional[List[Dict[str, Any]]]:
        """
        Generate videos from image.

        Payload được thiết kế để khớp với JSON mà GUI/user gửi trực tiếp tới API:
        - clientContext: projectId, tool, userPaygateTier (recaptchaToken, sessionId sẽ được inject riêng)
        - requests[]: aspectRatio, seed, textInput.prompt, videoModelKey, startImage.mediaId, metadata.sceneId
        - startImage: mediaId và cropCoordinates (tùy chọn)
        """
        self.last_error_detail = None
        url = "https://aisandbox-pa.googleapis.com/v1/video:batchAsyncGenerateVideoStartImage"
        
        # Generate scene IDs and seeds - use fixed seed if provided, otherwise random
        scene_ids = [str(uuid.uuid4()) for _ in range(num_videos)]
        if fixed_seed is not None:
            seeds = [fixed_seed] * num_videos
        else:
            seeds = [int(time.time() * 1000000 + i) % 100000 for i in range(num_videos)]
        
        # Map aspect ratio (vẫn dùng helper để đảm bảo hợp lệ) và chọn model hiệu quả
        mapped_aspect = self._map_video_aspect(aspect_ratio)
        effective_model = self._get_effective_model(model_key, mapped_aspect)
        
        # Bỏ check live status - chạy trực tiếp
        # Build requests body theo đúng format payload API yêu cầu
        # ✅ FIX: Dùng structuredPrompt thay vì double-encode prompt thành JSON string
        # Format đúng: "textInput": {"structuredPrompt": {"parts": [{"text": "..."}]}}
        requests_body: List[Dict[str, Any]] = []
        for i in range(num_videos):
            # ✅ Build startImage với cropCoordinates nếu có
            start_image = {"mediaId": media_id}
            if crop_coordinates:
                start_image["cropCoordinates"] = crop_coordinates
            
            requests_body.append(
                {
                    "aspectRatio": mapped_aspect,
                    "seed": seeds[i],
                    "textInput": {
                        "structuredPrompt": {
                            "parts": [{"text": prompt}]
                        }
                    },
                    "videoModelKey": effective_model,
                    "metadata": {},  # ✅ Metadata rỗng theo format WebUI
                    "startImage": start_image,
                }
            )
        
        # ✅ Payload khớp với curl example chuẩn:
        # - Có sessionId trong clientContext
        # - Dùng recaptchaContext thay vì recaptchaToken
        batch_id = str(uuid.uuid4())
        session_id = f";{int(time.time() * 1000)}"
        payload = {
            "mediaGenerationContext": {"batchId": batch_id},
            "clientContext": {
                "projectId": project_id,
                "tool": tool,
                "userPaygateTier": user_tier,
                "sessionId": session_id,  # ✅ Thêm sessionId theo format curl
            },
            "requests": requests_body,
            "useV2ModelConfig": True,
        }
        
        # Đảm bảo toàn bộ quá trình lấy token + gọi API được nối đuôi, không bị chen giữa
        with self._token_and_api_with_lock():
            max_retries = 5
            for attempt in range(max_retries):
                # 1) Lấy token reCAPTCHA mới cho mỗi attempt
                try:
                    self._maybe_inject_recaptcha(
                        payload["clientContext"],
                        raise_on_fail=True,
                        acquire_lock=False,
                        recaptcha_action="VIDEO_GENERATION",  # ✅ Dùng VIDEO_GENERATION cho I2V
                    )
                    time.sleep(0.1)  # delay nhỏ để Google validate token
                except RuntimeError as e:
                    self.last_error_detail = str(e)
                    self.last_error = str(e)
                    print(f"  ✗ Không thể lấy reCAPTCHA token: {e}")
                    return None

                # ✅ CRITICAL: Chuyển đổi recaptchaToken → recaptchaContext (theo format curl)
                # API I2V yêu cầu nested format, không phải flat format
                self._convert_to_recaptcha_context(payload["clientContext"])
                
                # ✅ FIX (flow2api): GIỮ sessionId trong clientContext - flow2api luôn gửi sessionId
                # sessionId giúp Google Labs tracking session và có thể ảnh hưởng đến reCAPTCHA trust score
                # Nếu sessionId bị xóa, tạo mới
                if "sessionId" not in payload.get("clientContext", {}):
                    payload["clientContext"]["sessionId"] = self._generate_session_id()

                try:
                    # 3) Rate limiting đơn giản dựa trên _min_api_call_interval
                    current_time = time.time()
                    elapsed = current_time - self._last_api_call_time
                    if elapsed < self._min_api_call_interval:
                        wait_time = self._min_api_call_interval - elapsed
                        if wait_time > 0:
                            print(f"  ⏳ Rate limit wait {wait_time:.2f}s trước khi gọi I2V...")
                            time.sleep(wait_time)
                    self._last_api_call_time = time.time()

                    print(f"→ Generating {num_videos} videos from image (attempt {attempt + 1}/{max_retries})")
                    
                    # ✅ FIX: Freshness check ngay trước khi gọi API
                    if not self._ensure_fresh_token(payload["clientContext"], recaptcha_action="VIDEO_GENERATION", acquire_lock=False):
                        self.last_error_detail = "Token expired và không thể lấy mới trước khi gọi I2V API"
                        self.last_error = self.last_error_detail
                        return None
                    self._convert_to_recaptcha_context(payload["clientContext"])
                    
                    # ✅ DEBUG: Log chi tiết payload trước khi gọi API - FULL VERSION
                    try:
                        debug_payload_str = json.dumps(payload, indent=2, ensure_ascii=False)
                        print(f"  📤 [DEBUG] === FULL PAYLOAD ===")
                        print(f"  📤 [DEBUG] Payload size: {len(debug_payload_str)} bytes")
                        print(f"  📤 [DEBUG] clientContext: {json.dumps(payload.get('clientContext', {}), indent=2)}")
                        print(f"  📤 [DEBUG] mediaGenerationContext: {json.dumps(payload.get('mediaGenerationContext', {}), indent=2)}")
                        print(f"  📤 [DEBUG] useV2ModelConfig: {payload.get('useV2ModelConfig')}")
                        print(f"  📤 [DEBUG] requests count: {len(payload.get('requests', []))}")
                        if payload.get('requests'):
                            print(f"  📤 [DEBUG] First request: {json.dumps(payload['requests'][0], indent=2)}")
                        print(f"  📤 [DEBUG] ===================")
                    except Exception as e:
                        print(f"  📤 [DEBUG] Error serializing payload: {e}")
                    
                    resp = self.session.post(
                        url,
                        headers=self._aisandbox_headers(),
                        data=json.dumps(payload),
                        timeout=120,
                    )

                    # 4) Xử lý nhanh các mã lỗi quan trọng với unified error handler
                    if resp.status_code == 429:
                        cookie_hash = self._cookie_hash
                        # ✅ FIX (flow2api): Thông báo cho captcha service tự phục hồi
                        self._notify_captcha_error_self_heal(429, "429 Rate Limit")
                        with LabsFlowClient._recaptcha_cookie_blocked_lock:
                            if not hasattr(LabsFlowClient, '_recaptcha_cookie_blocked_flags'):
                                LabsFlowClient._recaptcha_cookie_blocked_flags = {}
                            LabsFlowClient._recaptcha_cookie_blocked_flags[cookie_hash] = True
                        print(f"  ⚠️ [API] 429 Rate Limit, thử lại (attempt {attempt + 1})...")
                        
                        # ✅ Dùng unified error handler để xử lý 429
                        if self._handle_error_and_maybe_reset(429, "429 Rate Limit"):
                            print(f"  🔄 [I2V] Đã reset BrowserContext, retry với context mới...")
                            continue  # Retry với context mới
                        
                        if attempt < max_retries - 1:
                            time.sleep(5 * (attempt + 1))
                            continue
                        self.last_error_detail = "429 Rate Limit"
                        self.last_error = self.last_error_detail
                        return None

                    if resp.status_code == 403:
                        print("  ⚠️ 403 Forbidden từ I2V API, coi như cookie/token bị chặn.")
                        self._on_api_403()  # ✅ Track token source 403
                        
                        # ✅ FIX (flow2api): Thông báo cho captcha service tự phục hồi
                        self._notify_captcha_error_self_heal(403, resp.text[:200])
                        
                        # ✅ FIX (flow2api fallback): Sau 2 lần 403, thử format prompt đơn giản
                        if self._should_use_simple_prompt_format():
                            print(f"  🔄 [I2V Fallback] Chuyển sang format prompt đơn giản (flow2api style)...")
                            for req in payload.get("requests", []):
                                text_input = req.get("textInput", {})
                                sp = text_input.get("structuredPrompt", {})
                                parts = sp.get("parts", [])
                                if parts and isinstance(parts[0], dict):
                                    original_text = parts[0].get("text", prompt)
                                    req["textInput"] = {"prompt": original_text}
                        
                        # ✅ XỬ LÝ 403: Refresh cookie - xóa cookie cũ, reload để lấy cookie mới
                        # Retry tối đa 3 lần như yêu cầu
                        max_403_retries = 3
                        if not hasattr(self, '_403_refresh_retries'):
                            self._403_refresh_retries = {}
                        cookie_hash = self._cookie_hash
                        current_403_retries = self._403_refresh_retries.get(cookie_hash, 0)
                        
                        if current_403_retries < max_403_retries:
                            self._403_refresh_retries[cookie_hash] = current_403_retries + 1
                            print(f"  🔄 [I2V] 403 # {current_403_retries + 1}/{max_403_retries} - Refresh cookie & retry...")
                            
                            # Gọi hàm refresh cookie
                            self._refresh_cookie_on_403()
                            
                            # Đợi 1 giây trước khi retry
                            time.sleep(1)
                            continue  # Retry với cookie mới
                        else:
                            # Đã retry 3 lần mà vẫn 403 → báo fail
                            self._403_refresh_retries[cookie_hash] = 0  # Reset for next time
                            self.last_error_detail = f"403 Forbidden sau {max_403_retries} lần refresh cookie"
                            self.last_error = self.last_error_detail
                            print(f"  ❌ [I2V] 403 sau {max_403_retries} lần refresh cookie - Bỏ qua task này")
                            return None
                        
                        # (Code cũ - không cần nữa vì đã xử lý ở trên)
                        # ✅ Dùng unified error handler để xử lý 403
                        if self._handle_error_and_maybe_reset(403, f"403 Forbidden: {resp.text[:200]}"):
                            LabsFlowClient._zendriver_reset_page(self._cookie_hash)
                            print(f"  🔄 [I2V] Đã reset BrowserContext + Zendriver, retry...")
                            continue  # Retry với context mới
                        
                        self.last_error_detail = f"403 Forbidden: {resp.text[:200]}"
                        self.last_error = self.last_error_detail
                        return None

                    if resp.status_code >= 400:
                        error_msg = f"{resp.status_code} Client Error: {resp.text[:200]}"
                        print(f"  ⚠️ {error_msg}")
                        
                        # ✅ FIX: Re-fetch access token khi gặp 401 (token expired)
                        if resp.status_code == 401:
                            print(f"  🔄 [I2V] 401 - Re-fetch access token...")
                            if self.fetch_access_token():
                                print(f"  ✅ [I2V] Access token refreshed: {self.access_token[:20]}...")
                            else:
                                print(f"  ❌ [I2V] Không thể refresh access token")
                        
                        # ✅ DEBUG chi tiết cho lỗi 400
                        if resp.status_code == 400:
                            print(f"  ❌ [I2V DEBUG] Lỗi 400 - Invalid Argument")
                            print(f"  ❌ [I2V DEBUG] Cookie: {self._cookie_hash[:12]}...")
                            print(f"  ❌ [I2V DEBUG] URL: {url}")
                            print(f"  ❌ [I2V DEBUG] Media ID: {media_id[:60] if media_id else 'None'}...")
                            print(f"  ❌ [I2V DEBUG] Model: {model_key}")
                            print(f"  ❌ [I2V DEBUG] Aspect: {aspect_ratio} -> {mapped_aspect}")
                            print(f"  ❌ [I2V DEBUG] Effective Model: {effective_model}")
                            print(f"  ❌ [I2V DEBUG] Prompt: {prompt[:100]}...")
                            print(f"  ❌ [I2V DEBUG] Crop Coordinates: {crop_coordinates}")
                            try:
                                error_json = resp.json()
                                print(f"  ❌ [I2V DEBUG] Error JSON: {json.dumps(error_json, indent=2, ensure_ascii=False)[:1000]}")
                            except:
                                print(f"  ❌ [I2V DEBUG] Response text: {resp.text[:500]}")
                        
                        # ✅ Dùng unified error handler cho các lỗi 4xx khác
                        if resp.status_code in [400, 401]:
                            if self._handle_error_and_maybe_reset(resp.status_code, error_msg):
                                print(f"  🔄 [I2V] Đã reset BrowserContext, retry với context mới...")
                                continue
                        
                        if attempt < max_retries - 1:
                            time.sleep(5 * (attempt + 1))
                            continue
                        self.last_error_detail = error_msg
                        self.last_error = error_msg
                        return None

                    # 5) Thành công: parse kết quả
                    resp.raise_for_status()
                    result = resp.json()
                    print(f"  ✓ Image-to-video generation started")

                    # Reset counter 403 khi thành công
                    self._reset_403_counter_for_cookie()
                    self._on_api_success()  # ✅ Reset zendriver/playwright 403 counters

                    # Extract operations cho polling
                    operations: List[Dict[str, Any]] = []
                    if isinstance(result, dict) and "operations" in result:
                        for i, op in enumerate(result["operations"]):
                            operations.append({
                                "operation": {"name": op.get("operation", {}).get("name", "")},
                                "sceneId": scene_ids[i],
                                "status": "MEDIA_GENERATION_STATUS_PENDING",
                            })
                    else:
                        for scene_id in scene_ids:
                            operations.append({
                                "operation": {"name": str(uuid.uuid4()).replace('-', '')},
                                "sceneId": scene_id,
                                "status": "MEDIA_GENERATION_STATUS_PENDING",
                            })

                    return operations

                except Exception as e:
                    error_str = str(e)
                    print(f"  ⚠️ Exception I2V (attempt {attempt + 1}/{max_retries}): {error_str[:200]}")
                    if attempt < max_retries - 1:
                        time.sleep(5 * (attempt + 1))
                        continue
                    self.last_error_detail = error_str
                    self.last_error = error_str
                    return None

        # Hết retries mà vẫn fail
        return None

    def generate_videos_from_start_end(
        self,
        project_id: str,
        tool: str,
        user_tier: str,
        prompt: str,
        start_media_id: str,
        end_media_id: str,
        model_key: str = "veo_3_1_i2v_s_fast_ultra_fl",
        num_videos: int = 4,
        aspect_ratio: str = "VIDEO_ASPECT_RATIO_LANDSCAPE",
        fixed_seed: Optional[int] = None,
        start_crop_coordinates: Optional[Dict[str, float]] = None,  # ✅ Thêm cropCoordinates cho start
        end_crop_coordinates: Optional[Dict[str, float]] = None,    # ✅ Thêm cropCoordinates cho end
    ) -> Optional[List[Dict[str, Any]]]:
        """Generate videos from start and end images with a simpler, robust retry."""
        url = "https://aisandbox-pa.googleapis.com/v1/video:batchAsyncGenerateVideoStartAndEndImage"

        # Scene IDs và seeds
        scene_ids = [str(uuid.uuid4()) for _ in range(num_videos)]
        if fixed_seed is not None:
            seeds = [fixed_seed] * num_videos
        else:
            seeds = [int(time.time() * 1_000_000 + i) % 100_000 for i in range(num_videos)]

        # Bỏ check live status - chạy trực tiếp
        # Build payload cơ bản - ✅ FIX: Dùng structuredPrompt thay vì double-encode
        requests_body: List[Dict[str, Any]] = []
        
        for i in range(num_videos):
            # ✅ Build startImage với cropCoordinates nếu có
            start_image = {"mediaId": start_media_id}
            if start_crop_coordinates:
                start_image["cropCoordinates"] = start_crop_coordinates
            
            # ✅ Build endImage với cropCoordinates nếu có
            end_image = {"mediaId": end_media_id}
            if end_crop_coordinates:
                end_image["cropCoordinates"] = end_crop_coordinates
            
            requests_body.append(
                {
                    "aspectRatio": aspect_ratio,
                    "seed": seeds[i],
                    "textInput": {
                        "structuredPrompt": {
                            "parts": [{"text": prompt}]
                        }
                    },
                    "videoModelKey": model_key,
                    "metadata": {},  # ✅ Metadata rỗng theo format WebUI
                    "startImage": start_image,
                    "endImage": end_image,
                }
            )

        # ✅ Payload khớp WebUI: thêm mediaGenerationContext và useV2ModelConfig
        batch_id = str(uuid.uuid4())
        # ✅ Thêm sessionId theo format: ";timestamp"
        session_id = f";{int(time.time() * 1000)}"
        payload: Dict[str, Any] = {
            "mediaGenerationContext": {"batchId": batch_id},
            "clientContext": {
                "projectId": project_id,
                "tool": tool,
                "userPaygateTier": user_tier,
                "sessionId": session_id,  # ✅ Thêm sessionId theo format WebUI
            },
            "requests": requests_body,
            "useV2ModelConfig": True,
        }

        # Dùng chung lock token + API để nối đuôi an toàn
        with self._token_and_api_with_lock():
            max_retries = 5
            for attempt in range(max_retries):
                # 1) Lấy token mới
                try:
                    self._maybe_inject_recaptcha(
                        payload["clientContext"],
                        raise_on_fail=True,
                        acquire_lock=False,
                        recaptcha_action="VIDEO_GENERATION",  # ✅ Dùng VIDEO_GENERATION cho Start-End
                    )
                    time.sleep(0.1)
                except RuntimeError as e:
                    self.last_error_detail = str(e)
                    self.last_error = str(e)
                    print(f"  ✗ Không thể lấy reCAPTCHA token (start-end): {e}")
                    return None

                # 2) Verify token đã inject
                if not self._verify_token_before_api_call(payload):
                    return None

                # ✅ CRITICAL: Chuyển recaptchaToken → recaptchaContext cho Start-End API
                self._convert_to_recaptcha_context(payload["clientContext"])
                
                # ✅ FIX (flow2api): GIỮ sessionId trong clientContext - flow2api luôn gửi sessionId
                if "sessionId" not in payload.get("clientContext", {}):
                    payload["clientContext"]["sessionId"] = self._generate_session_id()

                try:
                    # 3) Rate limit đơn giản
                    current_time = time.time()
                    elapsed = current_time - self._last_api_call_time
                    if elapsed < self._min_api_call_interval:
                        wait_time = self._min_api_call_interval - elapsed
                        if wait_time > 0:
                            print(f"  ⏳ Rate limit wait {wait_time:.2f}s trước khi gọi start-end...")
                            time.sleep(wait_time)
                    self._last_api_call_time = time.time()

                    print(f"→ Generating {num_videos} videos from start-end (attempt {attempt + 1}/{max_retries})")
                    
                    # ✅ FIX: Freshness check ngay trước khi gọi API
                    if not self._ensure_fresh_token(payload["clientContext"], recaptcha_action="VIDEO_GENERATION", acquire_lock=False):
                        self.last_error_detail = "Token expired và không thể lấy mới trước khi gọi Start-End API"
                        self.last_error = self.last_error_detail
                        return None
                    self._convert_to_recaptcha_context(payload["clientContext"])
                    
                    # ✅ LOG FULL PAYLOAD để debug lỗi 400
                    if attempt == 0:  # Chỉ log lần đầu
                        print(f"  📤 [DEBUG] Start-End Payload:")
                        payload_str = json.dumps(payload, indent=2, ensure_ascii=False)
                        print(f"  📤 {payload_str[:1000]}...")  # Log 1000 chars đầu

                    resp = self.session.post(
                        url,
                        headers=self._aisandbox_headers(),
                        data=json.dumps(payload),
                        timeout=120,
                    )

                    # 4) Xử lý một số mã lỗi chính với unified error handler
                    if resp.status_code == 429:
                        cookie_hash = self._cookie_hash
                        # ✅ FIX (flow2api): Thông báo cho captcha service tự phục hồi
                        self._notify_captcha_error_self_heal(429, "429 Rate Limit (start-end)")
                        with LabsFlowClient._recaptcha_cookie_blocked_lock:
                            if not hasattr(LabsFlowClient, "_recaptcha_cookie_blocked_flags"):
                                LabsFlowClient._recaptcha_cookie_blocked_flags = {}
                            LabsFlowClient._recaptcha_cookie_blocked_flags[cookie_hash] = True
                        print(f"  ⚠️ [API] 429 Rate Limit (start-end), thử lại...")
                        
                        # ✅ Dùng unified error handler để xử lý 429
                        if self._handle_error_and_maybe_reset(429, "429 Rate Limit (start-end)"):
                            print(f"  🔄 [Start-End] Đã reset BrowserContext, retry với context mới...")
                            continue  # Retry với context mới
                        
                        if attempt < max_retries - 1:
                            time.sleep(5 * (attempt + 1))
                            continue
                        self.last_error_detail = "429 Rate Limit (start-end)"
                        self.last_error = self.last_error_detail
                        return None

                    if resp.status_code == 403:
                        print("  ⚠️ 403 Forbidden từ I2V start-end API, coi như token/cookie bị chặn.")
                        
                        # ✅ FIX (flow2api): Thông báo cho captcha service tự phục hồi
                        self._notify_captcha_error_self_heal(403, resp.text[:200])
                        
                        # ✅ FIX (flow2api fallback): Sau 2 lần 403, thử format prompt đơn giản
                        if self._should_use_simple_prompt_format():
                            print(f"  🔄 [Start-End Fallback] Chuyển sang format prompt đơn giản (flow2api style)...")
                            for req in payload.get("requests", []):
                                text_input = req.get("textInput", {})
                                sp = text_input.get("structuredPrompt", {})
                                parts = sp.get("parts", [])
                                if parts and isinstance(parts[0], dict):
                                    original_text = parts[0].get("text", prompt)
                                    req["textInput"] = {"prompt": original_text}
                        
                        # ✅ XỬ LÝ 403: Refresh cookie - xóa cookie cũ, reload để lấy cookie mới
                        # Retry tối đa 3 lần như yêu cầu
                        max_403_retries = 3
                        if not hasattr(self, '_403_refresh_retries'):
                            self._403_refresh_retries = {}
                        cookie_hash = self._cookie_hash
                        current_403_retries = self._403_refresh_retries.get(cookie_hash, 0)
                        
                        if current_403_retries < max_403_retries:
                            self._403_refresh_retries[cookie_hash] = current_403_retries + 1
                            print(f"  🔄 [Start-End] 403 # {current_403_retries + 1}/{max_403_retries} - Refresh cookie & retry...")
                            
                            # Gọi hàm refresh cookie
                            self._refresh_cookie_on_403()
                            
                            # Đợi 1 giây trước khi retry
                            time.sleep(1)
                            continue  # Retry với cookie mới
                        else:
                            # Đã retry 3 lần mà vẫn 403 → báo fail
                            self._403_refresh_retries[cookie_hash] = 0  # Reset for next time
                            self.last_error_detail = f"403 Forbidden sau {max_403_retries} lần refresh cookie"
                            self.last_error = self.last_error_detail
                            print(f"  ❌ [Start-End] 403 sau {max_403_retries} lần refresh cookie - Bỏ qua task này")
                            return None
                        
                        # (Code cũ - không cần nữa)
                        # ✅ Dùng unified error handler để xử lý 403
                        if self._handle_error_and_maybe_reset(403, f"403 Forbidden: {resp.text[:200]}"):
                            print(f"  🔄 [Start-End] Đã reset BrowserContext, retry với context mới...")
                            continue  # Retry với context mới
                        
                        self.last_error_detail = f"403 Forbidden: {resp.text[:200]}"
                        self.last_error = self.last_error_detail
                        return None

                    if resp.status_code >= 400:
                        err = f"{resp.status_code} Client Error: {resp.text[:200]}"
                        print(f"  ⚠️ {err}")
                        
                        # ✅ FIX: Re-fetch access token khi gặp 401 (token expired)
                        if resp.status_code == 401:
                            print(f"  🔄 [Start-End] 401 - Re-fetch access token...")
                            if self.fetch_access_token():
                                print(f"  ✅ [Start-End] Access token refreshed: {self.access_token[:20]}...")
                            else:
                                print(f"  ❌ [Start-End] Không thể refresh access token")
                        
                        # ✅ LOG CHI TIẾT CHO LỖI 400 INVALID_ARGUMENT
                        if resp.status_code == 400:
                            print(f"  ❌ [DEBUG] Lỗi 400 INVALID_ARGUMENT - Cookie: {self._cookie_hash[:8]}")
                            print(f"  ❌ [DEBUG] MediaIds: start={start_media_id[:50] if start_media_id else 'None'}..., end={end_media_id[:50] if end_media_id else 'None'}...")
                            print(f"  ❌ [DEBUG] Model key: {model_key}")
                            print(f"  ❌ [DEBUG] Prompt: {prompt[:100]}...")
                            try:
                                error_json = resp.json()
                                print(f"  ❌ [DEBUG] Full error: {json.dumps(error_json, indent=2, ensure_ascii=False)}")
                            except:
                                print(f"  ❌ [DEBUG] Full response text: {resp.text}")
                            
                            # ✅ Dùng unified error handler cho lỗi 400
                            if self._handle_error_and_maybe_reset(400, err):
                                print(f"  🔄 [Start-End] Đã reset BrowserContext, retry với context mới...")
                                continue
                        
                        if attempt < max_retries - 1:
                            time.sleep(5 * (attempt + 1))
                            continue
                        self.last_error_detail = err
                        self.last_error = err
                        return None

                    # 5) Thành công
                    resp.raise_for_status()
                    result = resp.json()
                    print("  ✓ Start-End video generation started")

                    self._reset_403_counter_for_cookie()

                    operations: List[Dict[str, Any]] = []
                    if isinstance(result, dict) and "operations" in result:
                        for i, op in enumerate(result["operations"]):
                            operations.append(
                                {
                                    "operation": {"name": op.get("operation", {}).get("name", "")},
                                    "sceneId": scene_ids[i],
                                    "status": "MEDIA_GENERATION_STATUS_PENDING",
                                }
                            )
                    else:
                        for scene_id in scene_ids:
                            operations.append(
                                {
                                    "operation": {"name": str(uuid.uuid4()).replace("-", "")},
                                    "sceneId": scene_id,
                                    "status": "MEDIA_GENERATION_STATUS_PENDING",
                                }
                            )

                    return operations

                except Exception as e:
                    err = str(e)
                    print(f"  ⚠️ Exception start-end (attempt {attempt + 1}/{max_retries}): {err[:200]}")
                    if attempt < max_retries - 1:
                        time.sleep(5 * (attempt + 1))
                        continue
                    self.last_error_detail = err
                    self.last_error = err
                    return None

        return None

    def generate_upscale_videos(
        self,
        media_ids: List[str],
        model_key: str = "veo_3_1_upsampler_1080p",
        aspect_ratio: str = "VIDEO_ASPECT_RATIO_LANDSCAPE",
        resolution: str = "VIDEO_RESOLUTION_1080P",
        fixed_seed: Optional[int] = None,
    ) -> Optional[List[Dict[str, Any]]]:
        """Start video upscaling jobs for given media IDs (batch) with HIGH_TRAFFIC retry.
        
        Args:
            media_ids: List of media IDs to upscale
            model_key: Model key - "veo_3_1_upsampler_1080p" or "veo_3_1_upsampler_4k"
            aspect_ratio: Aspect ratio - "VIDEO_ASPECT_RATIO_LANDSCAPE" or "VIDEO_ASPECT_RATIO_PORTRAIT"
            resolution: Resolution - "VIDEO_RESOLUTION_1080P" or "VIDEO_RESOLUTION_4K"
            fixed_seed: Optional fixed seed for reproducibility
        """
        # ✅ Cả 4K và 1080P đều dùng CÙNG endpoint batchAsyncGenerateVideoUpsampleVideo
        # Chỉ khác nhau ở resolution và videoModelKey trong payload
        url = "https://aisandbox-pa.googleapis.com/v1/video:batchAsyncGenerateVideoUpsampleVideo"
        
        if resolution == "VIDEO_RESOLUTION_4K" or model_key == "veo_3_1_upsampler_4k":
            resolution = "VIDEO_RESOLUTION_4K"
            model_key = "veo_3_1_upsampler_4k"
            print(f"  📺 Upscale 4K: model={model_key}, resolution={resolution}")
        else:
            resolution = "VIDEO_RESOLUTION_1080P"
            if model_key == "veo_2_1080p_upsampler_8s":
                model_key = "veo_3_1_upsampler_1080p"  # Upgrade to new model
            print(f"  📺 Upscale 1080P: model={model_key}, resolution={resolution}")
        
        print(f"  🔗 Endpoint: {url}")

        # seeds and scene IDs - use fixed seed if provided
        scene_ids = [str(uuid.uuid4()) for _ in media_ids]
        if fixed_seed is not None:
            seeds = [fixed_seed] * len(media_ids)
        else:
            seeds = [int(time.time() * 1000000 + i) % 100000 for i in range(len(media_ids))]

        # Map aspect ratio
        mapped_aspect = self._map_video_aspect(aspect_ratio)
        
        # Bỏ check live status - chạy trực tiếp
        requests_body = []
        for i, media_id in enumerate(media_ids):
            requests_body.append({
                "aspectRatio": mapped_aspect,
                "resolution": resolution,  # ✅ Thêm resolution vào payload
                "seed": seeds[i],
                "videoInput": {"mediaId": media_id},
                "videoModelKey": model_key,
                "metadata": {"sceneId": scene_ids[i]},
            })

        # ✅ Payload theo đúng thứ tự như Google Labs: requests trước, clientContext sau
        payload = {
            "requests": requests_body,
            "clientContext": {
                "sessionId": f";{int(time.time() * 1000)}"  # ✅ Thêm sessionId như Google Labs yêu cầu
            },
        }
        
        # Retry logic for HIGH_TRAFFIC error
        max_retries = 5
        for attempt in range(max_retries):
            # ✅ Mỗi attempt lấy token mới
            try:
                self._maybe_inject_recaptcha(payload["clientContext"], raise_on_fail=True, recaptcha_action="VIDEO_GENERATION")
                # ✅ Delay nhỏ sau khi có token để Google Labs validate token
                time.sleep(0.1)
            except RuntimeError as e:
                self.last_error_detail = str(e)
                self.last_error = str(e)
                print(f"  ✗ Không thể lấy reCAPTCHA token: {e}")
                return None

            # ✅ VERIFY: Đảm bảo token đã được inject vào payload trước khi gọi API
            if not self._verify_token_before_api_call(payload):
                return None
            
            # ✅ CRITICAL: Chuyển recaptchaToken → recaptchaContext cho Upscale API
            self._convert_to_recaptcha_context(payload["clientContext"])
            
            # ✅ FIX (flow2api): GIỮ sessionId trong clientContext - flow2api luôn gửi sessionId
            if "sessionId" not in payload.get("clientContext", {}):
                payload["clientContext"]["sessionId"] = self._generate_session_id()

            try:
                # ✅ Rate limiting - delay trước khi gọi API để tránh 403
                if attempt == 0:
                    self._rate_limit_api_call()
                
                print(f"→ Starting upscale for {len(media_ids)} media(s) (attempt {attempt + 1}/{max_retries})")
                print(f"  🔗 URL: {url}")
                print(f"  📦 Payload resolution: {resolution}, model: {model_key}")
                
                # ✅ FIX: Freshness check ngay trước khi gọi API
                if not self._ensure_fresh_token(payload["clientContext"], recaptcha_action="VIDEO_GENERATION"):
                    self.last_error_detail = "Token expired và không thể lấy mới trước khi gọi Upscale API"
                    self.last_error = self.last_error_detail
                    return None
                self._convert_to_recaptcha_context(payload["clientContext"])
                
                # ✅ Lấy headers và có thể override content-type cho 4K endpoint
                headers = self._aisandbox_headers()
                # Thử dùng application/json cho upscale endpoint (có thể Google yêu cầu)
                # headers["content-type"] = "application/json"
                
                resp = self.session.post(
                    url,
                    headers=headers,
                    data=json.dumps(payload),
                    timeout=120,
                )
                
                # Debug response
                print(f"  📡 Response status: {resp.status_code}")
                
                # Check for HIGH_TRAFFIC error
                if resp.status_code == 500:
                    try:
                        error_data = resp.json()
                        error_msg = json.dumps(error_data)
                        if "PUBLIC_ERROR_HIGH_TRAFFIC" in error_msg or "HIGH_TRAFFIC" in error_msg:
                            if attempt < max_retries - 1:
                                wait_time = (attempt + 1) * 5  # 5s, 10s, 15s
                                print(f"  ⚠️ VEO 3 quá tải (upscale), chờ {wait_time}s và thử lại...")
                                time.sleep(wait_time)
                                continue
                    except:
                        pass
                
                # ✅ Check 429 - Rate Limit với unified error handler
                if resp.status_code == 429:
                    print(f"  ⚠️ [Upscale] 429 Rate Limit, thử lại...")
                    # ✅ FIX (flow2api): Thông báo cho captcha service tự phục hồi
                    self._notify_captcha_error_self_heal(429, "429 Rate Limit (upscale)")
                    
                    if self._handle_error_and_maybe_reset(429, "429 Rate Limit (upscale)"):
                        print(f"  🔄 [Upscale] Đã reset BrowserContext, retry với context mới...")
                        continue
                    
                    if attempt < max_retries - 1:
                        time.sleep(5 * (attempt + 1))
                        continue
                    self.last_error_detail = "429 Rate Limit (upscale)"
                    return None
                
                # ✅ Check 403 - Forbidden với unified error handler
                if resp.status_code == 403:
                    print(f"  ⚠️ [Upscale] 403 Forbidden, thử lại...")
                    # ✅ FIX (flow2api): Thông báo cho captcha service tự phục hồi
                    self._notify_captcha_error_self_heal(403, resp.text[:200])
                    
                    # ✅ XỬ LÝ 403: Refresh cookie - xóa cookie cũ, reload để lấy cookie mới
                    # Retry tối đa 3 lần như yêu cầu
                    max_403_retries = 3
                    if not hasattr(self, '_403_refresh_retries'):
                        self._403_refresh_retries = {}
                    cookie_hash = self._cookie_hash
                    current_403_retries = self._403_refresh_retries.get(cookie_hash, 0)
                    
                    if current_403_retries < max_403_retries:
                        self._403_refresh_retries[cookie_hash] = current_403_retries + 1
                        print(f"  🔄 [Upscale] 403 # {current_403_retries + 1}/{max_403_retries} - Refresh cookie & retry...")
                        
                        # Gọi hàm refresh cookie
                        self._refresh_cookie_on_403()
                        
                        # Đợi 1 giây trước khi retry
                        time.sleep(1)
                        continue  # Retry với cookie mới
                    else:
                        # Đã retry 3 lần mà vẫn 403 → báo fail
                        self._403_refresh_retries[cookie_hash] = 0  # Reset for next time
                        self.last_error_detail = f"403 Forbidden sau {max_403_retries} lần refresh cookie"
                        self.last_error = self.last_error_detail
                        print(f"  ❌ [Upscale] 403 sau {max_403_retries} lần refresh cookie - Bỏ qua task này")
                        return None
                    
                    # (Code cũ - không cần nữa)
                    if self._handle_error_and_maybe_reset(403, f"403 Forbidden (upscale): {resp.text[:200]}"):
                        print(f"  🔄 [Upscale] Đã reset BrowserContext, retry với context mới...")
                        continue
                    
                    if attempt < max_retries - 1:
                        time.sleep(5 * (attempt + 1))
                        continue
                    self.last_error_detail = f"403 Forbidden (upscale): {resp.text[:200]}"
                    return None
                
                # ✅ Check 400/401 với unified error handler
                if resp.status_code in [400, 401]:
                    error_msg = f"{resp.status_code} Client Error (upscale): {resp.text[:200]}"
                    print(f"  ⚠️ {error_msg}")
                    
                    # ✅ FIX: Re-fetch access token khi gặp 401 (token expired)
                    if resp.status_code == 401:
                        print(f"  🔄 [Upscale] 401 - Re-fetch access token...")
                        if self.fetch_access_token():
                            print(f"  ✅ [Upscale] Access token refreshed: {self.access_token[:20]}...")
                        else:
                            print(f"  ❌ [Upscale] Không thể refresh access token")
                    
                    if self._handle_error_and_maybe_reset(resp.status_code, error_msg):
                        print(f"  🔄 [Upscale] Đã reset BrowserContext, retry với context mới...")
                        continue
                    
                    if attempt < max_retries - 1:
                        time.sleep(5 * (attempt + 1))
                        continue
                    self.last_error_detail = error_msg
                    return None
                
                # ✅ Check 404 - Not Found (endpoint không tồn tại)
                if resp.status_code == 404:
                    try:
                        error_body = resp.json()
                        error_msg = f"404 Not Found (upscale): {json.dumps(error_body, ensure_ascii=False)[:500]}"
                    except:
                        error_msg = f"404 Not Found (upscale): {resp.text[:500]}"
                    print(f"  ❌ {error_msg}")
                    print(f"  ❌ URL gọi: {url}")
                    print(f"  ❌ Model: {model_key}, Resolution: {resolution}")
                    
                    # 404 thường là endpoint không tồn tại
                    self.last_error_detail = error_msg
                    self.last_error = f"Endpoint upscale không tồn tại hoặc URL không đúng"
                    return None
                
                resp.raise_for_status()
                result = resp.json()
                print("  ✓ Upscale jobs started")

                operations = []
                if isinstance(result, dict) and "operations" in result:
                    for i, op in enumerate(result["operations"]):
                        operations.append({
                            "operation": {"name": op.get("operation", {}).get("name", "")},
                            "sceneId": scene_ids[i],
                            "status": "MEDIA_GENERATION_STATUS_PENDING",
                        })
                else:
                    for scene_id in scene_ids:
                        operations.append({
                            "operation": {"name": str(uuid.uuid4()).replace('-', '')},
                            "sceneId": scene_id,
                            "status": "MEDIA_GENERATION_STATUS_PENDING",
                        })

                return operations
                
            except Exception as e:
                if attempt < max_retries - 1:
                    wait_time = LabsFlowClient.calculate_retry_delay(attempt, 0, base_delay=5.0)
                    print(f"  ⚠️ Lỗi upscale (attempt {attempt + 1}): {str(e)[:100]}, retry sau {wait_time:.1f}s...")
                    time.sleep(wait_time)
                else:
                    print(f"  ✗ Failed upscale after {max_retries} attempts: {e}")
                    return None
        
        return None

    def create_whisk_workflow(self, workflow_name: str = None) -> Optional[str]:
        """Create Whisk workflow and return workflow ID."""
        try:
            print("→ Creating Whisk workflow...")
            url = "https://labs.google/fx/api/trpc/media.createOrUpdateWorkflow"
            
            # Generate workflow ID
            workflow_id = str(uuid.uuid4())
            workflow_name = workflow_name or f"Whisk Project: {time.strftime('%m/%d/%y')}"
            
            payload = {
                "json": {
                    "clientContext": {
                        "tool": "BACKBONE",
                    },
                    "mediaGenerationIdsToCopy": None,
                    "workflowMetadata": {
                        "workflowName": workflow_name
                    }
                },
                "meta": {
                    "values": {
                        "mediaGenerationIdsToCopy": ["undefined"]
                    }
                }
            }
            # ✅ Inject reCAPTCHA token for workflow creation
            try:
                self._maybe_inject_recaptcha(payload["json"]["clientContext"], raise_on_fail=True, recaptcha_action="IMAGE_GENERATION")
                # ✅ Delay nhỏ sau khi có token để Google Labs validate token (tránh 403 lần đầu)
                time.sleep(0.5)
            except RuntimeError as e:
                self.last_error_detail = str(e)
                print(f"  ✗ Không thể lấy reCAPTCHA token: {e}")
                return None
            
            # ✅ VERIFY: Đảm bảo token đã được inject vào payload trước khi gọi API
            if not self._verify_token_before_api_call(payload):
                return None
            
            # ✅ Rate limiting - delay trước khi gọi API để tránh 403
            self._rate_limit_api_call()
            
            resp = None
            try:
                resp = self.session.post(
                    url,
                    headers=self._labs_headers(),
                    cookies=self.cookies,
                    data=json.dumps(payload),
                    timeout=60,
                )
                resp.raise_for_status()
            except Exception as e:
                # Thu thập thông tin lỗi chi tiết từ server để debug (ví dụ: 401/403/5xx)
                detail = str(e)
                try:
                    if resp is not None:
                        try:
                            err_json = resp.json()
                            detail = json.dumps(err_json, ensure_ascii=False)
                        except Exception:
                            detail = f"status={resp.status_code} text={resp.text[:400]}"
                except Exception:
                    pass
                self.last_error_detail = detail
                self.last_error = detail
                print(f"  ✗ Failed to create workflow (HTTP): {detail}")
                return None
            
            try:
                result = resp.json()
            except Exception:
                result = {}
            print("  ✓ Whisk workflow created")
            print(f"  Response: {json.dumps(result, indent=2)}")
            
            # Extract workflow ID from response or use generated one
            actual_workflow_id = workflow_id
            if isinstance(result, dict):
                workflow_data = result.get("result", {}).get("data", {})
                if isinstance(workflow_data, dict):
                    # Try direct field first
                    wid = workflow_data.get("workflowId")
                    if not wid:
                        # Nested: data.json.result.workflowId
                        wid = (workflow_data.get("json", {}) or {}).get("result", {}).get("workflowId")
                    if wid:
                        actual_workflow_id = wid
            
            print(f"  ✓ Workflow ID: {actual_workflow_id}")
            return actual_workflow_id
            
        except Exception as e:
            detail = str(e)
            self.last_error_detail = detail
            self.last_error = detail
            print(f"  ✗ Failed to create workflow (exception): {detail}")
            return None

    def generate_image_from_text(
        self,
        workflow_id: str,
        prompt: str,
        image_model: str = "IMAGEN_3_5",
        aspect_ratio: str = "16:9",
        seed: int = None
    ) -> Optional[Dict[str, Any]]:
        """Generate image from text using Whisk API."""
        try:
            print(f"→ Generating image with prompt: '{prompt}'")
            
            # Bỏ check live status - chạy trực tiếp
            # Submit batch log first
            log_url = "https://labs.google/fx/api/trpc/general.submitBatchLog"
            app_events = [{
                "event": "BACKBONE_CREATE_PROJECT",
                "eventProperties": [
                    {"key": "TOOL_NAME", "stringValue": "BACKBONE"},
                    {"key": "BACKBONE_MODE", "stringValue": "CREATE"},
                    {"key": "BACKBONE_WORKFLOW", "stringValue": workflow_id},
                    {"key": "G1_MEMBERSHIP_STATUS", "stringValue": "AVAILABLE_CREDITS"},
                    {"key": "USER_AGENT", "stringValue": self.user_agent},
                    {"key": "IS_DESKTOP"},
                ],
                "activeExperiments": [],
                "eventMetadata": {"sessionId": f";{int(time.time()*1000)}"},
                "eventTime": time.strftime("%Y-%m-%dT%H:%M:%S.000Z", time.gmtime()),
            }]
            log_payload = {"json": {"appEvents": app_events}}
            
            resp = self.session.post(
                log_url,
                headers=self._labs_headers(),
                cookies=self.cookies,
                data=json.dumps(log_payload),
                timeout=60,
            )
            resp.raise_for_status()
            print("  ✓ Batch log submitted")
            
            
            gen_url = "https://aisandbox-pa.googleapis.com/v1/whisk:generateImage"
            # Use provided seed or generate random seed
            if seed is None:
                seed_value = int(time.time() * 1000) % 1000000
            else:
                seed_value = seed
            gen_payload = {
                "clientContext": {
                    "workflowId": workflow_id,
                    "tool": "BACKBONE",
                },
                "imageModelSettings": {
                    "imageModel": image_model,
                    "aspectRatio": self._map_image_aspect(aspect_ratio)
                },
                "seed": seed_value,
                "prompt": prompt,
                "mediaCategory": "MEDIA_CATEGORY_BOARD"
            }
            # ✅ Inject reCAPTCHA token for Whisk image generation
            try:
                self._maybe_inject_recaptcha(gen_payload["clientContext"], raise_on_fail=True, recaptcha_action="IMAGE_GENERATION")
                time.sleep(0.1)
            except RuntimeError as e:
                self.last_error_detail = str(e)
                print(f"  ✗ Không thể lấy reCAPTCHA token: {e}")
                return None
            
            print(f"  📤 Request URL: {gen_url}")
            print(f"  📤 Request Headers: {json.dumps(self._aisandbox_headers(), indent=4)}")
            print(f"  📤 Request Payload: {json.dumps(gen_payload, indent=4)}")

            # ✅ Retry logic với xử lý 403 reCAPTCHA error (tương tự generate_videos)
            max_retries = 3
            resp = None
            for attempt in range(max_retries):
                try:
                    # ✅ MỖI ATTEMPT LẤY TOKEN MỚI – tránh dùng lại token cũ dễ gây 403
                    try:
                        self._maybe_inject_recaptcha(gen_payload["clientContext"], raise_on_fail=True, recaptcha_action="IMAGE_GENERATION")
                        time.sleep(0.1)
                    except RuntimeError as e:
                        self.last_error_detail = str(e)
                        print(f"  ✗ Không thể lấy reCAPTCHA token: {e}")
                        return None
                    
                    # ✅ VERIFY: Đảm bảo token đã được inject vào payload trước khi gọi API
                    if not self._verify_token_before_api_call(gen_payload):
                        return None

                    # ✅ Rate limiting - delay trước khi gọi API để tránh 403
                    if attempt == 0:
                        self._rate_limit_api_call()

                    resp = self.session.post(
                        gen_url,
                        headers=self._aisandbox_headers(),
                        data=json.dumps(gen_payload),
                        timeout=120,
                    )
                    
                    # ✅ Check 403 - Token score thấp, cần lấy token mới
                    if resp.status_code == 403:
                        try:
                            error_data = resp.json()
                            error_msg = json.dumps(error_data)
                            
                            # ✅ Dùng unified error handler để xử lý 403
                            if self._handle_error_and_maybe_reset(403, error_msg):
                                print(f"  🔄 [Whisk] Đã reset BrowserContext, retry với context mới...")
                                continue  # Retry với context mới
                            
                            # Chưa đến ngưỡng reset, thử lấy token mới
                            if "reCAPTCHA" in error_msg or "recaptcha" in error_msg.lower():
                                if self._handle_403_recaptcha_error(gen_payload, attempt, max_retries):
                                    continue  # Retry với token mới
                                else:
                                    self.last_error_detail = "403 reCAPTCHA evaluation failed - không thể lấy token mới"
                                    return None
                        except Exception:
                            pass
                        
                        # Nếu không phải reCAPTCHA error, xử lý như lỗi thông thường
                        error_msg = f"403 Client Error: Forbidden for url: {gen_url}"
                        self.last_error_detail = error_msg
                        if attempt < max_retries - 1:
                            wait_time = LabsFlowClient.calculate_retry_delay(attempt, 403, base_delay=5.0)
                            print(f"  ⚠️ 403 Forbidden, retry sau {wait_time:.1f}s...")
                            time.sleep(wait_time)
                            continue
                        return None
                    
                    # ✅ Check 429 - Rate Limit với unified error handler
                    if resp.status_code == 429:
                        print(f"  ⚠️ [Whisk] 429 Rate Limit, thử lại...")
                        
                        if self._handle_error_and_maybe_reset(429, "429 Rate Limit (Whisk)"):
                            print(f"  🔄 [Whisk] Đã reset BrowserContext, retry với context mới...")
                            continue
                        
                        if attempt < max_retries - 1:
                            time.sleep(5 * (attempt + 1))
                            continue
                        self.last_error_detail = "429 Rate Limit (Whisk)"
                        return None
                    
                    # ✅ Check 400/401 với unified error handler
                    if resp.status_code in [400, 401]:
                        error_msg = f"{resp.status_code} Client Error (Whisk): {resp.text[:200]}"
                        print(f"  ⚠️ {error_msg}")
                        
                        # ✅ FIX: Re-fetch access token khi gặp 401 (token expired)
                        if resp.status_code == 401:
                            print(f"  🔄 [Whisk] 401 - Re-fetch access token...")
                            if self.fetch_access_token():
                                print(f"  ✅ [Whisk] Access token refreshed: {self.access_token[:20]}...")
                            else:
                                print(f"  ❌ [Whisk] Không thể refresh access token")
                        
                        if self._handle_error_and_maybe_reset(resp.status_code, error_msg):
                            print(f"  🔄 [Whisk] Đã reset BrowserContext, retry với context mới...")
                            continue
                        
                        if attempt < max_retries - 1:
                            time.sleep(5 * (attempt + 1))
                            continue
                        self.last_error_detail = error_msg
                        return None
                    
                    resp.raise_for_status()
                    
                    result = resp.json()
                    print("  ✓ Image generation started")
                    print(f"  Response: {json.dumps(result, indent=2)}")
                    
                    # ✅ Reset 403 counter khi thành công
                    self._reset_403_counter_for_cookie()
                    
                    return result
                    
                except Exception as e:
                    error_str = str(e)
                    # ✅ Check nếu exception chứa 403 - xử lý tương tự
                    if "403" in error_str or "Forbidden" in error_str:
                        if resp is not None and resp.status_code == 403:
                            try:
                                error_data = resp.json()
                                error_msg = json.dumps(error_data)
                                if "reCAPTCHA" in error_msg or "recaptcha" in error_msg.lower():
                                    if self._handle_403_recaptcha_error(gen_payload, attempt, max_retries):
                                        continue  # Retry với token mới
                            except Exception:
                                pass
                    
                    # ✅ Các lỗi khác: retry như bình thường
                    if attempt < max_retries - 1:
                        wait_time = LabsFlowClient.calculate_retry_delay(attempt, 0, base_delay=5.0)
                        print(f"  ⚠️ Lỗi Whisk generate image (attempt {attempt + 1}): {error_str[:100]}, retry sau {wait_time:.1f}s...")
                        time.sleep(wait_time)
                    else:
                        print(f"  ✗ Failed to generate image sau {max_retries} attempts: {e}")
                        # Try to get more error details
                        if resp is not None:
                            try:
                                print(f"  📥 Response Status: {resp.status_code}")
                                print(f"  📥 Response Headers: {dict(resp.headers)}")
                                print(f"  📥 Response Body: {resp.text[:500]}...")
                                detail = f"status={resp.status_code} text={resp.text[:400]}"
                            except:
                                detail = error_str
                        else:
                            detail = error_str
                        self.last_error_detail = detail
                        return None
            
            return None
        except Exception as e:
            detail = str(e)
            self.last_error_detail = detail
            self.last_error = detail
            print(f"  ✗ Failed to generate image: {detail}")
            return None

    def generate_flow_images(
        self,
        requests_payload: List[Dict[str, Any]],
        project_id: Optional[str] = None,
    ) -> Optional[Dict[str, Any]]:
        """Call flowMedia:batchGenerateImages with prepared requests list."""
        if not requests_payload:
            self.last_error_detail = "Empty Flow image payload"
            return None
        project = project_id or self.flow_project_id
        if not project:
            self.last_error_detail = "Missing FLOW_PROJECT_ID"
            return None
        url = f"https://aisandbox-pa.googleapis.com/v1/projects/{project}/flowMedia:batchGenerateImages"
        
        # Bỏ check live status - chạy trực tiếp
        # ✅ Thêm clientContext + recaptchaToken cho Flow image
        # ✅ FIX: Thêm projectId, tool, userPaygateTier vào top-level clientContext (giống generate_videos)
        client_context: Dict[str, Any] = {
            "projectId": project,
            "tool": "PINHOLE",
            "userPaygateTier": "PAYGATE_TIER_TWO",
        }
        try:
            self._maybe_inject_recaptcha(client_context, raise_on_fail=True, recaptcha_action="IMAGE_GENERATION")
            # ✅ Delay nhỏ sau khi có token để Google Labs validate token (giảm vì nối đuôi)
            time.sleep(0.1)
        except RuntimeError as e:
            self.last_error_detail = str(e)
            print(f"  ✗ Không thể lấy reCAPTCHA token: {e}")
            return None

        # ✅ Convert recaptchaToken → recaptchaContext (Flow image API yêu cầu nested format)
        self._convert_to_recaptcha_context(client_context)

        # ✅ Thêm sessionId vào top-level clientContext (theo format: ";timestamp")
        # ✅ Tạo sessionId chung cho cả top-level và tất cả requests (theo curl example)
        common_session_id = f";{int(time.time() * 1000)}"
        client_context["sessionId"] = common_session_id
        
        # ✅ Đảm bảo tất cả requests có cùng sessionId với top-level (theo curl example)
        # ✅ Đồng bộ sessionId + recaptchaContext vào mỗi request (theo curl thật)
        for req in requests_payload:
            if "clientContext" in req and isinstance(req["clientContext"], dict):
                req["clientContext"]["sessionId"] = common_session_id
                # ✅ FIX: Copy recaptchaContext từ top-level vào request con (curl thật có ở cả 2 nơi)
                rc = client_context.get("recaptchaContext")
                if rc:
                    req["clientContext"]["recaptchaContext"] = dict(rc)
                self._convert_to_recaptcha_context(req["clientContext"])

        # ✅ Thêm useNewMedia và mediaGenerationContext theo API format thực tế
        import uuid
        payload = {
            "clientContext": client_context,
            "mediaGenerationContext": {"batchId": str(uuid.uuid4())},
            "useNewMedia": True,
            "requests": requests_payload,
        }
        
        # ✅ VERIFY: Đảm bảo token đã được inject vào payload trước khi gọi API
        if not self._verify_token_before_api_call(payload):
            return None
        
        # ✅ Rate limiting - delay trước khi gọi API để tránh 403
        self._rate_limit_api_call()
        
        # ✅ Retry logic với xử lý 403 reCAPTCHA error và 500 Internal Server Error
        max_retries = 3  # ✅ Giảm từ 5 xuống 3 theo yêu cầu
        resp = None
        for attempt in range(max_retries):
            try:
                # ✅ MỖI ATTEMPT LẤY TOKEN MỚI – tránh dùng lại token cũ dễ gây 403
                try:
                    # ✅ Giữ lại sessionId khi update client_context
                    old_session_id = client_context.get("sessionId")
                    self._maybe_inject_recaptcha(client_context, raise_on_fail=True, recaptcha_action="IMAGE_GENERATION")
                    # ✅ Convert recaptchaToken → recaptchaContext (Flow image API yêu cầu nested format)
                    self._convert_to_recaptcha_context(client_context)
                    # ✅ Khôi phục sessionId nếu bị mất
                    if old_session_id and "sessionId" not in client_context:
                        client_context["sessionId"] = old_session_id
                    # ✅ Update lại payload với client_context mới
                    payload["clientContext"] = client_context
                    # ✅ Delay nhỏ sau khi có token để Google Labs validate token
                    time.sleep(0.1)
                except RuntimeError as e:
                    self.last_error_detail = str(e)
                    print(f"  ✗ Không thể lấy reCAPTCHA token: {e}")
                    return None
                
                # ✅ VERIFY: Đảm bảo token đã được inject vào payload trước khi gọi API
                if not self._verify_token_before_api_call(payload):
                    return None
                
                # ✅ CRITICAL: Chuyển recaptchaToken → recaptchaContext (theo format chuẩn cho tất cả video APIs)
                self._convert_to_recaptcha_context(payload["clientContext"])
                
                # ✅ Rate limiting - delay trước khi gọi API để tránh 403
                if attempt == 0:
                    self._rate_limit_api_call()
                
                # ✅ FIX: Freshness check ngay trước khi gọi API - nếu token expired thì lấy mới
                if not self._ensure_fresh_token(payload["clientContext"], recaptcha_action="IMAGE_GENERATION", acquire_lock=False):
                    self.last_error_detail = "Token expired và không thể lấy mới trước khi gọi Flow API"
                    return None
                # ✅ Convert lại format nếu token mới được inject
                self._convert_to_recaptcha_context(payload["clientContext"])
                
                # ✅ FIX: Sync recaptchaContext mới vào requests con (theo curl thật)
                rc = payload["clientContext"].get("recaptchaContext")
                if rc:
                    for req in payload.get("requests", []):
                        if "clientContext" in req and isinstance(req["clientContext"], dict):
                            req["clientContext"]["recaptchaContext"] = dict(rc)
                
                # ✅ LOG HEADERS VÀ PAYLOAD CHI TIẾT - FULL (KHÔNG TRUNCATE)
                request_headers = self._aisandbox_headers()
                payload_json = json.dumps(payload, indent=2, ensure_ascii=False)
                print(f"  📤 [Flow API] REQUEST (attempt {attempt + 1}/{max_retries}):")
                print(f"  📤 URL: {url}")
                print(f"  📤 Headers:")
                headers_json = json.dumps(request_headers, indent=2, ensure_ascii=False)
                print(headers_json)
                print(f"  📤 Payload size: {len(payload_json)} bytes")
                print(f"  📤 Payload FULL:")
                print(payload_json)
                
                resp = self.session.post(
                    url,
                    headers=request_headers,
                    data=json.dumps(payload),
                    timeout=120,
                )
                
                # ✅ LOG RESPONSE CHI TIẾT - FULL (KHÔNG TRUNCATE)
                print(f"  📥 [Flow API] RESPONSE (attempt {attempt + 1}/{max_retries}):")
                print(f"  📥 Status Code: {resp.status_code}")
                print(f"  📥 Headers: {dict(resp.headers)}")
                try:
                    response_text = resp.text
                    print(f"  📥 Response size: {len(response_text)} bytes")
                    if len(response_text) > 0:
                        try:
                            response_json = resp.json()
                            response_json_str = json.dumps(response_json, indent=2, ensure_ascii=False)
                            print(f"  📥 Response JSON FULL:")
                            print(response_json_str)
                        except:
                            print(f"  📥 Response text FULL:")
                            print(response_text)
                    else:
                        print(f"  📥 Response: (empty)")
                except Exception as log_err:
                    print(f"  ⚠️ Lỗi khi log response: {log_err}")
                    import traceback
                    print(traceback.format_exc())
                
                # ✅ Check 401 - Unauthorized (access token expired)
                if resp.status_code == 401:
                    print(f"  ⚠️ [Flow API] 401 Unauthorized - Re-fetch access token...")
                    if self.fetch_access_token():
                        print(f"  ✅ [Flow API] Access token refreshed: {self.access_token[:20]}...")
                    else:
                        print(f"  ❌ [Flow API] Không thể refresh access token")
                    if attempt < max_retries - 1:
                        time.sleep(2)
                        continue
                    self.last_error_detail = "401 Unauthorized - Access token expired"
                    return None
                
                # ✅ Check 400 - Bad Request (log chi tiết - FULL)
                if resp.status_code == 400:
                    try:
                        error_data = resp.json()
                        error_json_str = json.dumps(error_data, indent=2, ensure_ascii=False)
                        print(f"  ❌ [Flow API] 400 Bad Request Error FULL:")
                        print(error_json_str)
                        # ✅ Log thêm thông tin chi tiết về payload để debug
                        print(f"  🔍 [DEBUG] Payload structure:")
                        print(f"    - Top-level clientContext keys: {list(payload.get('clientContext', {}).keys())}")
                        print(f"    - Number of requests: {len(payload.get('requests', []))}")
                        if payload.get('requests'):
                            first_req = payload['requests'][0]
                            print(f"    - First request keys: {list(first_req.keys())}")
                            if 'clientContext' in first_req:
                                print(f"    - First request clientContext keys: {list(first_req['clientContext'].keys())}")
                        
                        # ✅ Kiểm tra nếu là lỗi "invalid argument"
                        error_message = error_data.get('error', {}).get('message', '')
                        error_status = error_data.get('error', {}).get('status', '')
                        error_details_list = error_data.get('error', {}).get('details', [])
                        # Trích xuất reason từ details nếu có
                        error_reason = ''
                        for d in error_details_list:
                            if isinstance(d, dict) and d.get('reason'):
                                error_reason = d['reason']
                                break
                        
                        if 'invalid argument' in error_message.lower() or 'INVALID_ARGUMENT' in error_status:
                            # ✅ Phân biệt: chỉ khi có PUBLIC_ERROR_UNSAFE_GENERATION mới là vi phạm nội dung
                            is_unsafe = ('public_error_unsafe_generation' in error_message.lower()
                                         or 'PUBLIC_ERROR_UNSAFE_GENERATION' in error_reason
                                         or 'unsafe' in error_message.lower())
                            if is_unsafe:
                                print(f"  ⚠️ [Flow API] Prompt vi phạm quy tắc nội dung của Google (attempt {attempt + 1}/{max_retries})")
                                self.last_error_detail = "Prompt vi phạm quy tắc nội dung của Google (400 INVALID_ARGUMENT - PUBLIC_ERROR_UNSAFE_GENERATION). Vui lòng chỉnh sửa nội dung prompt."
                            else:
                                # Lỗi INVALID_ARGUMENT nhưng KHÔNG phải unsafe → có thể do prompt sai format, quá dài, ký tự không hợp lệ...
                                short_msg = error_message[:200] if error_message else "Không rõ chi tiết"
                                print(f"  ⚠️ [Flow API] Prompt bị từ chối (400 INVALID_ARGUMENT): {short_msg} (attempt {attempt + 1}/{max_retries})")
                                self.last_error_detail = f"Prompt bị từ chối bởi Google (400 INVALID_ARGUMENT): {short_msg}. Vui lòng kiểm tra lại nội dung prompt (có thể quá dài, chứa ký tự đặc biệt, hoặc format không hợp lệ)."
                            # ✅ Không retry cho lỗi INVALID_ARGUMENT - prompt sai thì retry cũng sai
                            return None
                    except Exception as e:
                        print(f"  ❌ [Flow API] 400 Bad Request Error (text) FULL:")
                        print(resp.text)
                        print(f"  🔍 [DEBUG] Error parsing response: {e}")
                        # ✅ Nếu không parse được JSON, kiểm tra text
                        if 'invalid argument' in resp.text.lower() or 'INVALID_ARGUMENT' in resp.text:
                            if 'unsafe' in resp.text.lower() or 'PUBLIC_ERROR_UNSAFE_GENERATION' in resp.text:
                                self.last_error_detail = "Prompt vi phạm quy tắc nội dung của Google (400 INVALID_ARGUMENT - PUBLIC_ERROR_UNSAFE_GENERATION). Vui lòng chỉnh sửa nội dung prompt."
                            else:
                                self.last_error_detail = f"Prompt bị từ chối bởi Google (400 INVALID_ARGUMENT). Vui lòng kiểm tra lại nội dung prompt (có thể quá dài, chứa ký tự đặc biệt, hoặc format không hợp lệ)."
                            return None
                
                # ✅ Check 429 - Rate Limit (log chi tiết - FULL)
                if resp.status_code == 429:
                    # ✅ FIX (flow2api): Thông báo cho captcha service tự phục hồi
                    self._notify_captcha_error_self_heal(429, "429 Rate Limit (flow images)")
                    try:
                        error_data = resp.json()
                        error_json_str = json.dumps(error_data, indent=2, ensure_ascii=False)
                        print(f"  ⚠️ [Flow API] 429 Rate Limit Error FULL:")
                        print(error_json_str)
                    except Exception:
                        print(f"  ⚠️ [Flow API] 429 Rate Limit Error (text) FULL:")
                        print(resp.text)
                    
                    # ✅ Dùng unified error handler để xử lý 429
                    if self._handle_error_and_maybe_reset(429, "429 Rate Limit"):
                        print(f"  🔄 [Flow API] Đã reset BrowserContext, retry với context mới...")
                        continue  # Retry với context mới
                    
                    # Chưa đến ngưỡng reset, retry với exponential backoff
                    if attempt < max_retries - 1:
                        wait_time = LabsFlowClient.calculate_retry_delay(attempt, 429, base_delay=5.0)
                        print(f"  ⚠️ [Flow API] 429 Rate Limit, retry sau {wait_time:.1f}s...")
                        time.sleep(wait_time)
                        continue
                    
                    self.last_error_detail = "429 Rate Limit - Đã retry hết"
                    return None
                
                # ✅ Check 403 - Token score thấp, cần lấy token mới (log FULL)
                if resp.status_code == 403:
                    # ✅ FIX (flow2api): Thông báo cho captcha service tự phục hồi
                    self._notify_captcha_error_self_heal(403, resp.text[:200])
                    try:
                        error_data = resp.json()
                        error_msg = json.dumps(error_data, indent=2, ensure_ascii=False)
                        print(f"  ❌ [Flow API] 403 Forbidden Error FULL:")
                        print(error_msg)
                        
                        # ✅ Parse chi tiết error từ Google API
                        error_details = LabsFlowClient._parse_google_error_details(resp.text)
                        if error_details['reason']:
                            print(f"  📋 [Error Reason] {error_details['reason']}")
                        if error_details['is_recaptcha_error']:
                            print(f"  🔐 [Error Type] Đây là reCAPTCHA error!")
                        
                        # ✅ Dùng unified error handler để xử lý 403
                        if self._handle_error_and_maybe_reset(403, error_msg):
                            print(f"  🔄 [Flow API] Đã reset BrowserContext, retry với context mới...")
                            continue  # Retry với context mới
                        
                        # Chưa đến ngưỡng reset, thử lấy token mới
                        if error_details['is_recaptcha_error'] or "reCAPTCHA" in error_msg or "recaptcha" in error_msg.lower():
                            if self._handle_403_recaptcha_error(payload, attempt, max_retries):
                                # ✅ Convert lại recaptchaToken → recaptchaContext sau khi _handle_403 inject token mới
                                self._convert_to_recaptcha_context(payload["clientContext"])
                                continue  # Retry với token mới
                            else:
                                self.last_error_detail = "403 reCAPTCHA evaluation failed - không thể lấy token mới"
                                return None
                    except Exception:
                        print(f"  ❌ [Flow API] 403 Forbidden Error (text) FULL:")
                        print(resp.text)
                    
                    # Nếu không phải reCAPTCHA error, xử lý như lỗi thông thường
                    error_msg = f"403 Client Error: Forbidden for url: {url}"
                    self.last_error_detail = error_msg
                    if attempt < max_retries - 1:
                        wait_time = (attempt + 1) * 5
                        print(f"  ⚠️ 403 Forbidden, retry sau {wait_time}s...")
                        time.sleep(wait_time)
                        continue
                    return None
                
                resp.raise_for_status()
                result = resp.json()
                
                # ✅ LOG SUCCESS RESPONSE CHI TIẾT - FULL (KHÔNG TRUNCATE)
                result_json_str = json.dumps(result, indent=2, ensure_ascii=False)
                print(f"  ✅ [Flow API] SUCCESS RESPONSE:")
                print(f"  ✅ Response size: {len(result_json_str)} bytes")
                print(f"  ✅ Response JSON FULL:")
                print(result_json_str)
                
                print("  ✓ Flow images request accepted")
                
                # ✅ Reset 403 counter khi thành công
                self._reset_403_counter_for_cookie()
                
                return result
                
            except Exception as e:
                error_str = str(e)
                # ✅ LOG EXCEPTION CHI TIẾT - FULL
                print(f"  ❌ [Flow API] EXCEPTION (attempt {attempt + 1}/{max_retries}):")
                print(f"  ❌ Exception type: {type(e).__name__}")
                print(f"  ❌ Exception message: {error_str}")
                if resp is not None:
                    print(f"  ❌ Response status: {resp.status_code}")
                    try:
                        print(f"  ❌ Response text FULL:")
                        print(resp.text)
                    except:
                        pass
                import traceback
                print(f"  ❌ Traceback FULL:")
                print(traceback.format_exc())
                
                # ✅ Check nếu exception chứa 400 - invalid argument
                if resp is not None and resp.status_code == 400:
                    try:
                        error_data = resp.json()
                        error_message = error_data.get('error', {}).get('message', '')
                        error_status = error_data.get('error', {}).get('status', '')
                        error_details_list = error_data.get('error', {}).get('details', [])
                        error_reason = ''
                        for d in error_details_list:
                            if isinstance(d, dict) and d.get('reason'):
                                error_reason = d['reason']
                                break
                        
                        if 'invalid argument' in error_message.lower() or 'INVALID_ARGUMENT' in error_status:
                            is_unsafe = ('public_error_unsafe_generation' in error_message.lower()
                                         or 'PUBLIC_ERROR_UNSAFE_GENERATION' in error_reason
                                         or 'unsafe' in error_message.lower())
                            if is_unsafe:
                                self.last_error_detail = "Prompt vi phạm quy tắc nội dung của Google (400 INVALID_ARGUMENT - PUBLIC_ERROR_UNSAFE_GENERATION). Vui lòng chỉnh sửa nội dung prompt."
                            else:
                                short_msg = error_message[:200] if error_message else "Không rõ chi tiết"
                                self.last_error_detail = f"Prompt bị từ chối bởi Google (400 INVALID_ARGUMENT): {short_msg}. Vui lòng kiểm tra lại nội dung prompt (có thể quá dài, chứa ký tự đặc biệt, hoặc format không hợp lệ)."
                            # Không retry cho INVALID_ARGUMENT
                            return None
                    except Exception:
                        if 'invalid argument' in resp.text.lower() or 'INVALID_ARGUMENT' in resp.text:
                            if 'unsafe' in resp.text.lower() or 'PUBLIC_ERROR_UNSAFE_GENERATION' in resp.text:
                                self.last_error_detail = "Prompt vi phạm quy tắc nội dung của Google (400 INVALID_ARGUMENT - PUBLIC_ERROR_UNSAFE_GENERATION). Vui lòng chỉnh sửa nội dung prompt."
                            else:
                                self.last_error_detail = f"Prompt bị từ chối bởi Google (400 INVALID_ARGUMENT). Vui lòng kiểm tra lại nội dung prompt (có thể quá dài, chứa ký tự đặc biệt, hoặc format không hợp lệ)."
                            return None
                
                # ✅ Check nếu exception chứa 403 - xử lý tương tự
                if "403" in error_str or "Forbidden" in error_str:
                    if resp is not None and resp.status_code == 403:
                        try:
                            error_data = resp.json()
                            error_msg = json.dumps(error_data, indent=2, ensure_ascii=False)
                            print(f"  ❌ 403 Error details: {error_msg}")
                            if "reCAPTCHA" in error_msg or "recaptcha" in error_msg.lower():
                                if self._handle_403_recaptcha_error(payload, attempt, max_retries):
                                    # ✅ Convert lại recaptchaToken → recaptchaContext sau khi _handle_403 inject token mới
                                    self._convert_to_recaptcha_context(payload["clientContext"])
                                    continue  # Retry với token mới
                        except Exception:
                            pass
                
                # ✅ Xử lý đặc biệt cho lỗi 500 - Internal Server Error từ Google
                if resp is not None and resp.status_code == 500:
                    # ✅ Luôn set error detail để GUI hiển thị cho user
                    error_msg_500 = "500 Internal Server Error - Lỗi tạm thời từ phía Google Labs, vui lòng thử lại sau."
                    self.last_error_detail = error_msg_500
                    
                    if attempt < max_retries - 1:
                        wait_time = LabsFlowClient.calculate_retry_delay(attempt, 500, base_delay=10.0, max_delay=120.0)
                        print(f"  ⚠️ [Flow API] {error_msg_500}")
                        print(f"  ⚠️ Retry sau {wait_time:.1f}s... (attempt {attempt + 1}/{max_retries})")
                        time.sleep(wait_time)
                        
                        # ✅ Lấy token mới trước khi retry (có thể giúp)
                        try:
                            print(f"  🔑 [reCAPTCHA] Cookie {self._cookie_hash[:8]}... đang yêu cầu token...")
                            old_session_id = payload["clientContext"].get("sessionId")
                            self._maybe_inject_recaptcha(payload["clientContext"], raise_on_fail=False, recaptcha_action="IMAGE_GENERATION")
                            # ✅ Convert recaptchaToken → recaptchaContext (Flow image API yêu cầu nested format)
                            self._convert_to_recaptcha_context(payload["clientContext"])
                            # Khôi phục sessionId nếu bị mất
                            if old_session_id and "sessionId" not in payload["clientContext"]:
                                payload["clientContext"]["sessionId"] = old_session_id
                            new_token = payload["clientContext"].get("recaptchaContext", {}).get("token") or payload["clientContext"].get("recaptchaToken")
                            if new_token:
                                print(f"  ✅ Token đã được làm mới (length: {len(new_token)})")
                        except Exception as token_err:
                            print(f"  ⚠️ Không thể làm mới token: {token_err}")
                        
                        continue  # Retry
                
                # ✅ Các lỗi khác: retry với exponential backoff
                if attempt < max_retries - 1:
                    wait_time = LabsFlowClient.calculate_retry_delay(attempt, 0, base_delay=5.0)
                    print(f"  ⚠️ Lỗi Flow images (attempt {attempt + 1}): {error_str[:100]}, retry sau {wait_time:.1f}s...")
                    time.sleep(wait_time)
                else:
                    detail = error_str
                    if resp is not None:
                        try:
                            err_json = resp.json()
                            detail = json.dumps(err_json, indent=2, ensure_ascii=False)
                            print(f"  ✗ [Flow API] FINAL ERROR DETAILS (FULL):")
                            print(f"  ✗ Status: {resp.status_code}")
                            print(f"  ✗ Error JSON FULL:")
                            print(detail)
                        except Exception:
                            detail = f"status={resp.status_code} text={resp.text}"
                            print(f"  ✗ [Flow API] FINAL ERROR DETAILS (FULL):")
                            print(f"  ✗ Status: {resp.status_code}")
                            print(f"  ✗ Error text FULL:")
                            print(resp.text)
                    self.last_error_detail = detail
                    print(f"  ✗ Flow images request failed sau {max_retries} attempts")
                    return None
        
        return None

    def upsample_image(
        self,
        media_id: str,
        target_resolution: str = "UPSAMPLE_IMAGE_RESOLUTION_2K",
        session_id: Optional[str] = None,
        project_id: Optional[str] = None,
    ) -> Optional[Dict[str, Any]]:
        """Upsample an existing image (2K/4K)."""
        try:
            if not media_id:
                self.last_error_detail = "Missing media_id for upsampleImage"
                return None

            # Build clientContext with recaptchaToken
            client_context: Dict[str, Any] = {
                "sessionId": session_id or f";{int(time.time() * 1000)}",
                "projectId": project_id or self.flow_project_id or "",
                "tool": "PINHOLE",
                "userPaygateTier": "PAYGATE_TIER_TWO",
            }
            try:
                self._maybe_inject_recaptcha(client_context, raise_on_fail=True, recaptcha_action="IMAGE_GENERATION")
                self._convert_to_recaptcha_context(client_context)
                time.sleep(0.5)
            except RuntimeError as e:
                self.last_error_detail = str(e)
                print(f"  ✗ Không thể lấy reCAPTCHA token: {e}")
                return None

            payload = {
                "mediaId": media_id,
                "targetResolution": target_resolution,
                "clientContext": client_context,
            }

            url = "https://aisandbox-pa.googleapis.com/v1/flow/upsampleImage"
            # ✅ Upsample API yêu cầu Content-Type: text/plain;charset=UTF-8 (không phải application/json)
            headers = self._aisandbox_headers()
            headers["Content-Type"] = "text/plain;charset=UTF-8"
            
            # Debug log
            print(f"  → Upsample request: mediaId={media_id[:50]}..., resolution={target_resolution}")
            print(f"  → Payload: {json.dumps(payload, indent=2)[:500]}...")
            
            # ✅ VERIFY: Đảm bảo token đã được inject vào payload trước khi gọi API
            if not self._verify_token_before_api_call(payload):
                return None
            
            # ✅ Rate limiting - delay trước khi gọi API để tránh 403
            self._rate_limit_api_call()
            
            resp = self.session.post(
                url,
                headers=headers,
                data=json.dumps(payload),
                timeout=180,
            )
            resp.raise_for_status()
            result = resp.json()
            print(f"  ✅ Upsample response: {json.dumps(result, indent=2)[:1000]}")
            return result
        except Exception as e:
            detail = str(e)
            try:
                if "resp" in locals() and resp is not None:
                    err_json = resp.json()
                    detail = json.dumps(err_json, ensure_ascii=False)
            except Exception:
                pass
            self.last_error_detail = detail
            print(f"  ✗ Upsample image failed: {detail}")
            return None

    def extract_flow_media_id(self, response: Any) -> Optional[str]:
        """Extract mediaId from flow image response for upsample."""
        try:
            def _walk(node: Any) -> Optional[str]:
                if isinstance(node, dict):
                    # Check for mediaGenerationId
                    media_id = node.get("mediaGenerationId") or node.get("mediaId")
                    if isinstance(media_id, str) and media_id:
                        return media_id
                    # Check in nested structures
                    for key in ("image", "generatedImage", "media", "imageMedia"):
                        if key in node:
                            result = _walk(node[key])
                            if result:
                                return result
                    # Walk all values
                    for value in node.values():
                        result = _walk(value)
                        if result:
                            return result
                elif isinstance(node, list):
                    for item in node:
                        result = _walk(item)
                        if result:
                            return result
                return None
            
            return _walk(response)
        except Exception:
            return None

    def parse_flow_image_response(self, response: Any) -> List[Dict[str, Any]]:
        """Extract downloadable URLs or inline base64 blobs from Flow image response."""
        results: List[Dict[str, Any]] = []
        seen_inline_hashes: set = set()
        seen_urls: set = set()

        def _record_inline(data_str: str, mime_type: Optional[str]):
            if not data_str:
                return
            key = hash(data_str)
            if key in seen_inline_hashes:
                return
            seen_inline_hashes.add(key)
            results.append({
                "type": "inline",
                "data": data_str,
                "mime_type": mime_type,
            })

        def _record_url(url: str, mime_type: Optional[str] = None):
            if not url:
                return
            if url in seen_urls:
                return
            seen_urls.add(url)
            results.append({
                "type": "url",
                "url": url,
                "mime_type": mime_type,
            })

        def _record_data_url(data_url: str):
            if not data_url:
                return
            key = hash(data_url)
            if key in seen_inline_hashes:
                return
            seen_inline_hashes.add(key)
            results.append({
                "type": "data_url",
                "data": data_url,
            })

        def _ingest_media_entry(entry: Any):
            if not isinstance(entry, dict):
                return
            
            # Check for image.generatedImage.encodedImage structure
            image_obj = entry.get("image")
            if isinstance(image_obj, dict):
                gen_img = image_obj.get("generatedImage")
                if isinstance(gen_img, dict):
                    encoded = gen_img.get("encodedImage")
                    if isinstance(encoded, str) and encoded:
                        mime = "image/jpeg" if encoded.startswith("/9j/") else "image/png"
                        _record_inline(encoded, mime)
                        return  # Found encoded image, skip other checks
            
            inline_candidate = entry.get("inlineData") or entry.get("imageInlineData") or entry.get("base64Data")
            if isinstance(inline_candidate, dict):
                data_str = inline_candidate.get("data") or inline_candidate.get("bytes") or inline_candidate.get("base64")
                _record_inline(data_str, inline_candidate.get("mimeType"))
            if isinstance(entry.get("inlineData"), str):
                _record_inline(entry["inlineData"], entry.get("mimeType"))
            if isinstance(entry.get("base64Data"), str):
                _record_inline(entry["base64Data"], entry.get("mimeType"))
            
            # Check for encodedImage at top level
            encoded_img = entry.get("encodedImage")
            if isinstance(encoded_img, str) and encoded_img:
                mime = "image/jpeg" if encoded_img.startswith("/9j/") else "image/png"
                _record_inline(encoded_img, mime)

            for key in ("signedUri", "signedUrl", "contentUri", "imageUri", "downloadUri", "downloadUrl", "url", "publicUrl", "fifeUrl", "fife_url"):
                val = entry.get(key)
                if isinstance(val, str) and val.startswith("http"):
                    _record_url(val, entry.get("mimeType"))

            data_url = entry.get("dataUrl") or entry.get("dataURI")
            if isinstance(data_url, str) and data_url.startswith("data:image"):
                _record_data_url(data_url)

        def _walk(node: Any):
            if isinstance(node, dict):
                # Handle top-level "media" array (Flow API response format)
                if "media" in node and isinstance(node["media"], list):
                    print(f"  → Found 'media' array with {len(node['media'])} item(s)")
                    for idx, media_item in enumerate(node["media"]):
                        if not isinstance(media_item, dict):
                            continue
                        # Check for image.generatedImage.encodedImage structure
                        image_obj = media_item.get("image")
                        if isinstance(image_obj, dict):
                            gen_img = image_obj.get("generatedImage")
                            if isinstance(gen_img, dict):
                                encoded = gen_img.get("encodedImage")
                                if isinstance(encoded, str) and encoded:
                                    # This is base64 data (starts with /9j/ for JPEG or iVBOR for PNG)
                                    mime = "image/jpeg" if encoded.startswith("/9j/") else "image/png"
                                    print(f"  ✓ Found encodedImage in media[{idx}]: {len(encoded)} chars, type={mime}")
                                    _record_inline(encoded, mime)
                                    continue  # Found encoded image, skip other checks
                                
                                # ✅ Check for fifeUrl (Flow API returns image URL here)
                                fife_url = gen_img.get("fifeUrl")
                                if isinstance(fife_url, str) and fife_url.startswith("http"):
                                    print(f"  ✓ Found fifeUrl in media[{idx}]: {fife_url[:80]}...")
                                    _record_url(fife_url, "image/jpeg")  # fifeUrl is usually JPEG
                                    continue  # Found fifeUrl, skip other checks
                        # Also try standard media entry parsing
                        _ingest_media_entry(media_item)
                    # Continue walking to find other structures
                
                if "responses" in node and isinstance(node["responses"], list):
                    for resp in node["responses"]:
                        payload = resp.get("response") if isinstance(resp, dict) else None
                        if isinstance(payload, dict):
                            media_list: List[Any] = []
                            if isinstance(payload.get("imageMedia"), list):
                                media_list.extend(payload.get("imageMedia", []))
                            if isinstance(payload.get("images"), list):
                                media_list.extend(payload.get("images", []))
                            if isinstance(payload.get("media"), list):
                                media_list.extend(payload.get("media", []))
                            # Also check for direct imageMedia/image fields
                            if isinstance(payload.get("imageMedia"), dict):
                                media_list.append(payload.get("imageMedia"))
                            if isinstance(payload.get("image"), dict):
                                media_list.append(payload.get("image"))
                            for media in media_list:
                                _ingest_media_entry(media)
                            # Some responses embed inline data directly
                            _ingest_media_entry(payload)
                        elif isinstance(resp, dict):
                            # Response might be directly a media entry
                            _ingest_media_entry(resp)
                    return
                
                # Check for image.generatedImage.encodedImage in any dict
                image_obj = node.get("image")
                if isinstance(image_obj, dict):
                    gen_img = image_obj.get("generatedImage")
                    if isinstance(gen_img, dict):
                        encoded = gen_img.get("encodedImage")
                        if isinstance(encoded, str) and encoded:
                            _record_inline(encoded, "image/jpeg" if encoded.startswith("/9j/") else "image/png")
                
                # Check if this dict itself is a media entry
                if any(key in node for key in ("signedUri", "signedUrl", "contentUri", "imageUri", "inlineData", "imageInlineData")):
                    _ingest_media_entry(node)
                    return

                inline_candidate = None
                if isinstance(node.get("inlineData"), dict):
                    inline_candidate = node.get("inlineData")
                elif isinstance(node.get("imageInlineData"), dict):
                    inline_candidate = node.get("imageInlineData")
                elif isinstance(node.get("base64Data"), dict):
                    inline_candidate = node.get("base64Data")
                if inline_candidate:
                    data_str = inline_candidate.get("data") or inline_candidate.get("bytes")
                    _record_inline(data_str, inline_candidate.get("mimeType"))

                if isinstance(node.get("inlineData"), str):
                    _record_inline(node["inlineData"], node.get("mimeType"))
                if isinstance(node.get("base64Data"), str):
                    _record_inline(node["base64Data"], node.get("mimeType"))

                for key in ("signedUri", "contentUri", "imageUri", "downloadUri", "downloadUrl", "url"):
                    val = node.get(key)
                    if isinstance(val, str) and val.startswith("http"):
                        _record_url(val, node.get("mimeType"))

                for value in node.values():
                    if isinstance(value, str) and value.startswith("data:image"):
                        _record_data_url(value)
                    else:
                        _walk(value)
            elif isinstance(node, list):
                for item in node:
                    _walk(item)

        _walk(response)
        return results

    def extract_flow_media_payloads(self, response: Any) -> Tuple[List[Any], List[str]]:
        """Separate immediate payloads and pending operation names from flow response."""
        payloads: List[Any] = []
        operations: List[str] = []
        try:
            if isinstance(response, dict):
                responses = response.get("responses")
                if isinstance(responses, list):
                    for item in responses:
                        if not isinstance(item, dict):
                            continue
                        if isinstance(item.get("response"), dict):
                            payloads.append(item["response"])
                        op_dict = item.get("operation")
                        if isinstance(op_dict, dict):
                            name = op_dict.get("name") or op_dict.get("operation", {}).get("name")
                            if not name and isinstance(op_dict.get("operation"), dict):
                                name = op_dict["operation"].get("name")
                            if name:
                                operations.append(name)
                top_operations = response.get("operations")
                if isinstance(top_operations, list):
                    for op in top_operations:
                        if isinstance(op, dict):
                            name = op.get("name") or op.get("operation", {}).get("name")
                            if name:
                                operations.append(name)
        except Exception:
            pass
        return payloads, operations

    def poll_flow_operations(
        self,
        operation_names: List[str],
        poll_interval: float = 2.0,
        max_wait_seconds: int = 180,
        stop_event: Optional[Any] = None,
    ) -> List[Any]:
        """Poll long-running flow operations until completion.
        
        Args:
            operation_names: List of operation names to poll
            poll_interval: Interval between polls in seconds
            max_wait_seconds: Maximum time to wait
            stop_event: Optional threading.Event to check for stop signal
        """
        if not operation_names:
            return []
        pending = set(operation_names)
        collected: List[Any] = []
        deadline = time.time() + max_wait_seconds
        print(f"→ Polling Flow operations: {len(pending)} pending")
        while pending and time.time() < deadline:
            # ✅ Check stop_event trước mỗi lần poll
            if stop_event and stop_event.is_set():
                print(f"  ⏸️ Flow poll dừng do stop event (còn {len(pending)} operation(s) chưa xong)")
                return collected
            
            finished = []
            for name in list(pending):
                # ✅ Check stop_event trong vòng lặp poll từng operation
                if stop_event and stop_event.is_set():
                    print(f"  ⏸️ Flow poll dừng do stop event (còn {len(pending)} operation(s) chưa xong)")
                    return collected
                
                try:
                    op_name = name
                    if not op_name.startswith("projects/"):
                        op_name = op_name.lstrip("/")
                        op_url = f"https://aisandbox-pa.googleapis.com/v1/{op_name}"
                    else:
                        op_url = f"https://aisandbox-pa.googleapis.com/v1/{op_name}"
                    resp = self.session.get(
                        op_url,
                        headers=self._aisandbox_headers(),
                        timeout=60,
                    )
                    # ✅ FIX: Re-fetch access token khi gặp 401 và retry 1 lần
                    if resp.status_code == 401:
                        print(f"  ⚠️ [Flow Poll] 401 - Re-fetch access token...")
                        if self.fetch_access_token():
                            resp = self.session.get(
                                op_url,
                                headers=self._aisandbox_headers(),
                                timeout=60,
                            )
                    resp.raise_for_status()
                    data = resp.json()
                    if data.get("done") or "response" in data:
                        finished.append(name)
                        if "response" in data and isinstance(data["response"], dict):
                            collected.append(data["response"])
                        elif "result" in data:
                            collected.append(data["result"])
                        else:
                            collected.append(data)
                except Exception as e:
                    print(f"  ⚠️ Flow poll error for {name}: {e}")
            if finished:
                for name in finished:
                    pending.discard(name)
            else:
                # ✅ Check stop_event trước khi sleep
                if stop_event and stop_event.is_set():
                    print(f"  ⏸️ Flow poll dừng do stop event (còn {len(pending)} operation(s) chưa xong)")
                    return collected
                time.sleep(poll_interval)
        if pending:
            print(f"  ⚠️ Flow poll timeout for {len(pending)} operation(s)")
        else:
            print("  ✓ Flow polling completed")
        return collected
    
    def poll_until_complete(
        self,
        operations: List[Dict[str, Any]],
        max_wait_seconds: int = 300,
        poll_interval: int = 10
    ) -> Dict[str, Any]:
        """Poll video generation status until complete or timeout."""
        print(f"→ Polling for completion (max {max_wait_seconds}s, interval {poll_interval}s)...")
        
        deadline = time.time() + max_wait_seconds
        last_status = None
        
        while time.time() < deadline:
            status = self.check_video_status(operations)
            if status:
                last_status = status
                print(f"  Status: {json.dumps(status, indent=2)}")
                
                # Check if any videos are complete (basic heuristic)
                if isinstance(status, dict) and "operations" in status:
                    completed = 0
                    failed = 0
                    total = len(status["operations"])
                    
                    for op in status["operations"]:
                        op_status = op.get("status", "").upper()
                        if "COMPLETE" in op_status or "SUCCESS" in op_status:
                            completed += 1
                        elif "FAIL" in op_status or "ERROR" in op_status:
                            failed += 1
                    
                    print(f"  Progress: {completed}/{total} completed, {failed} failed")
                    
                    if completed + failed >= total:
                        print("  ✓ All operations finished")
                        break
            
            print(f"  Waiting {poll_interval}s...")
            time.sleep(poll_interval)
        
        if time.time() >= deadline:
            print("  ⚠ Polling timeout reached")
        
        return last_status or {}

    def run_image_recipe(
        self,
        workflow_id: str,
        prompt: str,
        image_model: str = "R2I",
        aspect_ratio: str = "16:9",
        subject_mgid: Optional[str] = None,
        scene_mgid: Optional[str] = None,
        style_mgid: Optional[str] = None,
        seed: Optional[int] = None,
    ) -> Optional[Dict[str, Any]]:
        """Call whisk:runImageRecipe with optional subject/scene/style mediaGenerationIds.
        Returns parsed JSON response or None on failure.
        """
        try:
            if seed is None:
                import time as _t
                seed = int(_t.time() * 1000) % 1000000
            url = "https://aisandbox-pa.googleapis.com/v1/whisk:runImageRecipe"
            payload: Dict[str, Any] = {
                "clientContext": {
                    "workflowId": workflow_id,
                    "tool": "BACKBONE",
                },
                "seed": seed,
                "imageModelSettings": {
                    "imageModel": image_model,
                    "aspectRatio": self._map_image_aspect(aspect_ratio),
                },
                "userInstruction": prompt,
            }
            # ✅ Inject reCAPTCHA token for Whisk image recipe run
            try:
                self._maybe_inject_recaptcha(payload["clientContext"], raise_on_fail=True, recaptcha_action="IMAGE_GENERATION")
                time.sleep(0.1)
            except RuntimeError as e:
                self.last_error_detail = str(e)
                print(f"  ✗ Không thể lấy reCAPTCHA token: {e}")
                return None
            recipe_inputs = []
            def push_item(mgid: Optional[str], category: str, caption: str) -> None:
                if isinstance(mgid, str) and mgid:
                    recipe_inputs.append({
                        "caption": caption,
                        "mediaInput": {
                            "mediaCategory": category,
                            "mediaGenerationId": mgid,
                        }
                    })
            push_item(subject_mgid, "MEDIA_CATEGORY_SUBJECT", "Subject image")
            push_item(scene_mgid, "MEDIA_CATEGORY_SCENE", "Scene image")
            push_item(style_mgid, "MEDIA_CATEGORY_STYLE", "Style image")
            if recipe_inputs:
                payload["recipeMediaInputs"] = recipe_inputs
            headers = self._aisandbox_headers()
            
            # ✅ Retry logic với xử lý 403 reCAPTCHA error (tương tự generate_videos)
            max_retries = 3
            resp = None
            for attempt in range(max_retries):
                try:
                    # ✅ MỖI ATTEMPT LẤY TOKEN MỚI – tránh dùng lại token cũ dễ gây 403
                    try:
                        self._maybe_inject_recaptcha(payload["clientContext"], raise_on_fail=True, recaptcha_action="IMAGE_GENERATION")
                        time.sleep(0.1)
                    except RuntimeError as e:
                        self.last_error_detail = str(e)
                        print(f"  ✗ Không thể lấy reCAPTCHA token: {e}")
                        return None
                    
                    # ✅ VERIFY: Đảm bảo token đã được inject vào payload trước khi gọi API
                    if not self._verify_token_before_api_call(payload):
                        return None
                    
                    # ✅ Rate limiting - delay trước khi gọi API để tránh 403
                    if attempt == 0:
                        self._rate_limit_api_call()
                    
                    resp = self.session.post(url, headers=headers, data=json.dumps(payload), timeout=120)
                    
                    # ✅ Check 403 - Token score thấp, cần lấy token mới
                    if resp.status_code == 403:
                        try:
                            error_data = resp.json()
                            error_msg = json.dumps(error_data)
                            
                            # ✅ Dùng unified error handler để xử lý 403
                            if self._handle_error_and_maybe_reset(403, error_msg):
                                print(f"  🔄 [Recipe] Đã reset BrowserContext, retry với context mới...")
                                continue  # Retry với context mới
                            
                            # Chưa đến ngưỡng reset, thử lấy token mới
                            if "reCAPTCHA" in error_msg or "recaptcha" in error_msg.lower():
                                if self._handle_403_recaptcha_error(payload, attempt, max_retries):
                                    continue  # Retry với token mới
                                else:
                                    self.last_error_detail = "403 reCAPTCHA evaluation failed - không thể lấy token mới"
                                    return None
                        except Exception:
                            pass
                        
                        # Nếu không phải reCAPTCHA error, xử lý như lỗi thông thường
                        error_msg = f"403 Client Error: Forbidden for url: {url}"
                        self.last_error_detail = error_msg
                        if attempt < max_retries - 1:
                            wait_time = LabsFlowClient.calculate_retry_delay(attempt, 403, base_delay=5.0)
                            print(f"  ⚠️ 403 Forbidden, retry sau {wait_time:.1f}s...")
                            time.sleep(wait_time)
                            continue
                        return None
                    
                    # ✅ Check 429 - Rate Limit với unified error handler
                    if resp.status_code == 429:
                        print(f"  ⚠️ [Recipe] 429 Rate Limit, thử lại...")
                        
                        if self._handle_error_and_maybe_reset(429, "429 Rate Limit (Recipe)"):
                            print(f"  🔄 [Recipe] Đã reset BrowserContext, retry với context mới...")
                            continue
                        
                        if attempt < max_retries - 1:
                            time.sleep(5 * (attempt + 1))
                            continue
                        self.last_error_detail = "429 Rate Limit (Recipe)"
                        return None
                    
                    # ✅ Check 400/401 với unified error handler
                    if resp.status_code in [400, 401]:
                        error_msg = f"{resp.status_code} Client Error (Recipe): {resp.text[:200]}"
                        print(f"  ⚠️ {error_msg}")
                        
                        if self._handle_error_and_maybe_reset(resp.status_code, error_msg):
                            print(f"  🔄 [Recipe] Đã reset BrowserContext, retry với context mới...")
                            continue
                        
                        if attempt < max_retries - 1:
                            time.sleep(5 * (attempt + 1))
                            continue
                        self.last_error_detail = error_msg
                        return None
                    
                    resp.raise_for_status()
                    result = resp.json()
                    print("  ✓ Image recipe started")
                    print(f"  Response: {json.dumps(result, indent=2)}")
                    
                    # ✅ Reset 403 counter khi thành công
                    self._reset_403_counter_for_cookie()
                    
                    return result
                    
                except Exception as e:
                    error_str = str(e)
                    # ✅ Check nếu exception chứa 403 - xử lý tương tự
                    if "403" in error_str or "Forbidden" in error_str:
                        if resp is not None and resp.status_code == 403:
                            try:
                                error_data = resp.json()
                                error_msg = json.dumps(error_data)
                                if "reCAPTCHA" in error_msg or "recaptcha" in error_msg.lower():
                                    if self._handle_403_recaptcha_error(payload, attempt, max_retries):
                                        continue  # Retry với token mới
                            except Exception:
                                pass
                    
                    # ✅ Các lỗi khác: retry như bình thường
                    if attempt < max_retries - 1:
                        wait_time = LabsFlowClient.calculate_retry_delay(attempt, 0, base_delay=5.0)
                        print(f"  ⚠️ Lỗi Whisk image recipe (attempt {attempt + 1}): {error_str[:100]}, retry sau {wait_time:.1f}s...")
                        time.sleep(wait_time)
                    else:
                        print(f"  ✗ Failed to run image recipe sau {max_retries} attempts: {e}")
                        self.last_error_detail = error_str
                        return None
            
            return None
        except Exception as e:
            detail = str(e)
            self.last_error_detail = detail
            self.last_error = detail
            print(f"  ✗ Failed to run image recipe: {detail}")
            return None

def main() -> int:
    parser = argparse.ArgumentParser(description="Complete Google Labs Flow video generation")
    parser.add_argument("--cookies", help="Cookie header string OR JSON array of cookies")
    parser.add_argument("--cookies-file", dest="cookies_file", help="Path to file containing cookies")
    parser.add_argument("--prompt", required=True, help="Text prompt for video generation")
    parser.add_argument("--model-key", dest="model_key", default="veo_3_1_t2v_fast_ultra", help="Video model key")
    parser.add_argument("--project-id", dest="project_id", help="Project ID (random UUID if not provided)")
    parser.add_argument("--tool", default="PINHOLE", help="Tool name")
    parser.add_argument("--user-tier", dest="user_tier", default="PAYGATE_TIER_TWO", help="User paygate tier")
    parser.add_argument("--num-videos", dest="num_videos", type=int, default=4, help="Number of videos to generate")
    parser.add_argument("--max-wait", dest="max_wait", type=int, default=300, help="Max wait time in seconds")
    parser.add_argument("--poll-interval", dest="poll_interval", type=int, default=10, help="Poll interval in seconds")
    
    args = parser.parse_args()
    
    # Get cookies
    cookies_source = None
    if args.cookies:
        cookies_source = args.cookies
    elif args.cookies_file:
        cookies_source = _read_file(args.cookies_file)
        if not cookies_source:
            print(f"Error: Cannot read cookies file: {args.cookies_file}")
            return 1
    else:
        print("Error: Must provide either --cookies or --cookies-file")
        return 1
    
    cookies = _parse_cookie_string(cookies_source)
    if not cookies:
        print("Error: No valid cookies found")
        return 1
    
    # Initialize client
    client = LabsFlowClient(cookies)
    
    # Step 1: Fetch access token
    if not client.fetch_access_token():
        print("Failed to get access token from session")
        return 2
    
    # Step 2: Set video model key
    if not client.set_video_model_key(args.model_key):
        print("Failed to set video model key")
        return 3
    
    # Step 3: Submit batch log
    if not client.submit_batch_log(args.tool):
        print("Failed to submit batch log")
        return 4
    
    # Step 4: Generate videos
    project_id = args.project_id or str(uuid.uuid4())
    operations = client.generate_videos(
        project_id=project_id,
        tool=args.tool,
        user_tier=args.user_tier,
        prompt=args.prompt,
        model_key=args.model_key,
        num_videos=args.num_videos
    )
    
    if not operations:
        print("Failed to start video generation")
        return 5
    
    # Step 5: Poll for completion
    final_status = client.poll_until_complete(
        operations=operations,
        max_wait_seconds=args.max_wait,
        poll_interval=args.poll_interval
    )
    
    print("=" * 60)
    print("FINAL STATUS:")
    print(json.dumps(final_status, indent=2))
    print("=" * 60)
    
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
