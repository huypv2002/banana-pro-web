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
    
    # ✅ Headless mode cho reCAPTCHA browser (mặc định False = hiện browser)
    _global_headless_mode: bool = False

    # ✅ Callback registry để lấy cookie mới khi bị chặn: {cookie_hash: callback_function}
    _recaptcha_renew_cookie_callbacks: Dict[str, Any] = {}  # {cookie_hash: callback(cookie_hash, old_cookies) -> new_cookies}
    # ✅ Flag để track cookie bị chặn từ API calls (403/429): {cookie_hash: True/False}
    _recaptcha_cookie_blocked_flags: Dict[str, bool] = {}  # {cookie_hash: is_blocked}
    _recaptcha_cookie_blocked_lock = threading.Lock()  # Lock để bảo vệ flags
    
    # ═══════════════════════════════════════════════════════════════════════
    # ✅ CHROME CDP - Primary token source (Chrome thật + CDP protocol)
    # Dùng Chrome thật để có trust score cao hơn
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
    _chrome_cdp_instances: Dict[str, Dict[str, Any]] = {}  # {profile_key: instance state}
    _chrome_cdp_cookie_profiles: Dict[str, str] = {}  # {cookie_hash: profile_key}
    _chrome_cdp_cookie_ports: Dict[str, int] = {}  # {cookie_hash: port}
    _chrome_cdp_cookie_locks: Dict[str, threading.Lock] = {}  # {cookie_hash: lock}
    _chrome_cdp_cookie_locks_guard = threading.Lock()

    # Token source tracking
    _last_token_source: Dict[str, str] = {}           # {cookie_hash: source}
    _chrome_cdp_consecutive_403: Dict[str, int] = {}   # {cookie_hash: count}
    _chrome_cdp_profile_403: Dict[str, int] = {}       # {profile_path: count}
    _cookie_profile_fallbacks: Dict[str, str] = {}     # {cookie_hash: fallback_profile_path}
    _profile_cooldown_until: Dict[str, float] = {}     # {profile_path: unix_ts}
    _profile_health_lock = threading.Lock()
    MAX_CHROME_CDP_403 = 3
    PROFILE_403_COOLDOWN_SECONDS = int(os.environ.get("PROFILE_403_COOLDOWN_SECONDS", "180"))
    
    # Compat state cho các nhánh self-heal/reset cũ còn sót lại
    _contexts_need_reset: Dict[str, bool] = {}
    _contexts_need_reset_lock = threading.Lock()
    _zendriver_pages: Dict[str, str] = {}
    _zendriver_cookies_injected: Dict[str, bool] = {}
    
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
                    cls._proxy_pool = [
                        p.to_browser_proxy() if hasattr(p, "to_browser_proxy")
                        else getattr(p, "to_" + "play" + "wright_proxy")()
                        for p in proxies
                    ]
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
                cls._proxy_pool = [
                    p.to_browser_proxy() if hasattr(p, "to_browser_proxy")
                    else getattr(p, "to_" + "play" + "wright_proxy")()
                    for p in proxies
                ]
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

    @classmethod
    def _list_available_profile_paths(cls) -> List[str]:
        """Lấy danh sách profile có cookie hợp lệ trên máy."""
        base_dir = os.environ.get("PROFILES_DIR", r"C:\BananaPro\chrome_profiles").strip()
        d = Path(base_dir)
        if not d.is_dir():
            fallback = os.environ.get("CHROME_PROFILE_PATH")
            return [fallback] if fallback else []
        profiles: List[str] = []
        for p in sorted(d.iterdir()):
            if not p.is_dir() or p.name.startswith("."):
                continue
            for cookies in (p / "Default" / "Network" / "Cookies", p / "Default" / "Cookies"):
                try:
                    if cookies.exists() and cookies.stat().st_size > 0:
                        profiles.append(str(p))
                        break
                except Exception:
                    continue
        return profiles

    @classmethod
    def _pick_alternate_profile(cls, current_profile: Optional[str]) -> Optional[str]:
        """Chọn profile khác khi profile hiện tại bị 403 liên tiếp.

        Ưu tiên profile không trong cooldown, ít 403 và ít cookie đang bám.
        """
        profiles = cls._list_available_profile_paths()
        if not profiles:
            return current_profile
        now = time.time()
        assignments: Dict[str, int] = {}
        for profile in getattr(cls, "_cookie_profile_paths", {}).values():
            if profile:
                assignments[profile] = assignments.get(profile, 0) + 1
        for profile in getattr(cls, "_cookie_profile_fallbacks", {}).values():
            if profile:
                assignments[profile] = assignments.get(profile, 0) + 1

        ranked = []
        for profile in profiles:
            if profile == current_profile:
                continue
            cooldown_until = cls._profile_cooldown_until.get(profile, 0.0)
            in_cooldown = cooldown_until > now
            score = cls._chrome_cdp_profile_403.get(profile, 0)
            load = assignments.get(profile, 0)
            ranked.append((1 if in_cooldown else 0, score, load, profile))
        if ranked:
            ranked.sort()
            return ranked[0][3]
        return current_profile

    @classmethod
    def _mark_profile_unhealthy(cls, profile_path: Optional[str], reason: str = "403") -> None:
        """Đưa profile vào cooldown để các cookie khác tạm thời tránh profile đang xấu."""
        if not profile_path:
            return
        with cls._profile_health_lock:
            cls._profile_cooldown_until[profile_path] = time.time() + cls.PROFILE_403_COOLDOWN_SECONDS
        print(f"  ⛔ [Profile Health] {Path(profile_path).name} cooldown {cls.PROFILE_403_COOLDOWN_SECONDS}s vì {reason}")
    
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

        # --- reCAPTCHA mode: Chrome CDP only ---
        # Enable by setting env AUTO_RECAPTCHA=1 (recommended for GUI)
        auto_flag = _env("AUTO_RECAPTCHA", "0") or "0"
        self.auto_recaptcha: bool = str(auto_flag) in ("1", "true", "True", "YES", "yes")
        self.chrome_cdp_only: bool = str(_env("CHROME_CDP_ONLY", "1") or "1") in ("1", "true", "True", "YES", "yes")
        self.use_chrome_cdp_recaptcha = True
        
        # ✅ Log mode đang dùng
        if self.auto_recaptcha:
            print("✓ reCAPTCHA mode: Chrome CDP off-screen")
        
        # ✅ Generate cookie hash để identify cookie và tạo file token riêng
        self._cookie_hash = self._get_cookie_hash(cookies)
        
        # ✅ Debug: Log cookie hash và số lượng cookies để kiểm tra
        cookie_count = len(cookies) if cookies else 0
        cookie_names = list(cookies.keys())[:5] if cookies else []  # Lấy 5 cookie đầu tiên để debug
        print(f"  🔍 Cookie hash: {self._cookie_hash[:8]}... (Tổng: {cookie_count} cookies, ví dụ: {', '.join(cookie_names[:3])})")
        
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
    def cleanup_browser_state(cls):
        """Đóng Chrome CDP process và dọn trạng thái browser dùng chung."""
        cls._cleanup_chrome_cdp()
    
    @classmethod
    def _get_chrome_cdp_profile_key(cls, profile_path: Optional[str] = None) -> str:
        if profile_path and os.path.exists(profile_path):
            return os.path.abspath(profile_path)
        return "__default__"

    @classmethod
    def _get_or_create_cookie_lock(cls, cookie_hash: str) -> threading.Lock:
        with cls._chrome_cdp_cookie_locks_guard:
            lock = cls._chrome_cdp_cookie_locks.get(cookie_hash)
            if lock is None:
                lock = threading.Lock()
                cls._chrome_cdp_cookie_locks[cookie_hash] = lock
            return lock

    @classmethod
    def _get_or_create_chrome_cdp_instance(cls, profile_path: Optional[str] = None) -> tuple[str, Dict[str, Any]]:
        profile_key = cls._get_chrome_cdp_profile_key(profile_path)
        instance = cls._chrome_cdp_instances.get(profile_key)
        if instance is None:
            instance = {
                "started": False,
                "process": None,
                "port": 9222,
                "lock": threading.Lock(),
                "user_data_dir": None,
                "profile_path": None if profile_key == "__default__" else profile_key,
            }
            cls._chrome_cdp_instances[profile_key] = instance
        return profile_key, instance

    @classmethod
    def _make_chrome_cdp_session_key(cls, cookie_hash: str, profile_key: Optional[str] = None) -> str:
        if profile_key:
            return f"{profile_key}::{cookie_hash}"
        return cookie_hash

    @classmethod
    def _iter_chrome_cdp_session_keys(cls, cookie_hash: str) -> list[str]:
        keys = [cookie_hash]
        suffix = f"::{cookie_hash}"
        for key in list(cls._chrome_cdp_pages.keys()):
            if key.endswith(suffix):
                keys.append(key)
        for key in list(cls._chrome_cdp_ws_conns.keys()):
            if key.endswith(suffix):
                keys.append(key)
        deduped = []
        seen = set()
        for key in keys:
            if key not in seen:
                seen.add(key)
                deduped.append(key)
        return deduped

    @classmethod
    def _cleanup_chrome_cdp(cls, profile_key: Optional[str] = None):
        """Đóng Chrome CDP process và cleanup resources.

        Nếu có `profile_key`, chỉ dọn instance của profile đó.
        """
        target_keys = [profile_key] if profile_key else list(cls._chrome_cdp_instances.keys())
        if not target_keys and profile_key is None:
            target_keys = ["__default__"]

        for key in target_keys:
            instance = cls._chrome_cdp_instances.get(key)
            if not instance:
                continue

            port = instance.get("port")

            for cookie_hash, ws_conn in list(cls._chrome_cdp_ws_conns.items()):
                if cls._chrome_cdp_cookie_profiles.get(cookie_hash) != key:
                    continue
                try:
                    ws_conn.close()
                except Exception:
                    pass
                cls._chrome_cdp_ws_conns.pop(cookie_hash, None)
                cls._chrome_cdp_ws_msg_ids.pop(cookie_hash, None)
                cls._chrome_cdp_page_ready.pop(cookie_hash, None)

            if instance.get("started"):
                for cookie_hash, tab_id in list(cls._chrome_cdp_tab_ids.items()):
                    if cls._chrome_cdp_cookie_profiles.get(cookie_hash) != key:
                        continue
                    try:
                        requests.get(
                            f"http://127.0.0.1:{port}/json/close/{tab_id}",
                            timeout=2,
                        )
                    except Exception:
                        pass

            for cookie_hash in list(cls._chrome_cdp_cookie_profiles.keys()):
                if cls._chrome_cdp_cookie_profiles.get(cookie_hash) != key:
                    continue
                cls._chrome_cdp_pages.pop(cookie_hash, None)
                cls._chrome_cdp_tab_ids.pop(cookie_hash, None)
                cls._chrome_cdp_cookies_injected.pop(cookie_hash, None)
                cls._chrome_cdp_cookie_profiles.pop(cookie_hash, None)
                cls._chrome_cdp_cookie_ports.pop(cookie_hash, None)
                cls._zendriver_pages.pop(cookie_hash, None)
                cls._zendriver_cookies_injected.pop(cookie_hash, None)

            proc = instance.get("process")
            if proc:
                try:
                    proc.terminate()
                    proc.wait(timeout=5)
                    print(f"  ✓ [Chrome CDP] Chrome process terminated ({key})")
                except Exception:
                    try:
                        proc.kill()
                    except Exception:
                        pass

            user_data_dir = instance.get("user_data_dir")
            if user_data_dir:
                import shutil
                try:
                    shutil.rmtree(user_data_dir, ignore_errors=True)
                except Exception:
                    pass

            cls._chrome_cdp_instances.pop(key, None)

    @classmethod
    def _get_global_browser(cls, headless: bool = False, browser_path: Optional[str] = None) -> Any:
        raise RuntimeError("Legacy browser path removed. Use Chrome CDP off-screen only.")

    @classmethod
    def reset_browser_state(cls, cookie_hash: Optional[str] = None):
        """Reset trạng thái Chrome CDP dùng chung cho một cookie hoặc toàn cục."""
        if cookie_hash:
            cls._zendriver_reset_page(cookie_hash)
            cls._chrome_cdp_consecutive_403[cookie_hash] = 0
            return
        cls._cleanup_chrome_cdp()

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
        - Nếu >= 6 lần liên tiếp: reset trạng thái Chrome CDP.
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
            print(f"  🚨 Đã đạt ngưỡng 6 lần lỗi liên tiếp ({status_code}). Reset Chrome CDP state...")
            self.reset_browser_state(cookie_hash)
            
            # Reset counter sau khi reset
            LabsFlowClient._shared_error_reset_counters[cookie_hash] = 0
            return True
            
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
        if hasattr(LabsFlowClient, '_cookie_profile_fallbacks'):
            fallback = LabsFlowClient._cookie_profile_fallbacks.get(self._cookie_hash)
            if fallback:
                return fallback
        if hasattr(LabsFlowClient, '_cookie_profile_paths'):
            return LabsFlowClient._cookie_profile_paths.get(self._cookie_hash)
        return self.profile_path
    
    def _refresh_cookies_from_profile(self) -> Optional[Dict[str, str]]:
        """
        Lấy cookie mới từ profile đã đăng nhập bằng Chrome CDP off-screen.
        
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
            from pathlib import Path
            from chrome_cdp_cookie import ChromeCDPSession

            profile_dir = Path(profile_path)
            if not profile_dir.exists():
                print(f"  ❌ Profile path không tồn tại: {profile_path}")
                return None

            account_info = LabsFlowClient._cookie_account_info.get(cookie_hash, {})
            session = ChromeCDPSession(
                profile_path=str(profile_dir),
                headless=True,
                window_pos=(-3000, -3000),
                window_size=(400, 300),
                log_fn=lambda msg: print(f"  {msg}"),
            )

            try:
                extracted = session.extract_cookies(
                    email=account_info.get("email", ""),
                    password=account_info.get("password", ""),
                    force_login=False,
                )
            finally:
                session.close()

            google_cookies: Dict[str, str] = {}
            for cookie in extracted or []:
                name = cookie.get("name")
                value = cookie.get("value")
                if name and value:
                    google_cookies[name] = value

            if google_cookies:
                print(f"  ✅ [Cookie Refresh] Đã lấy {len(google_cookies)} cookies mới từ profile qua Chrome CDP")
                return google_cookies

            print(f"  ⚠️ [Cookie Refresh] Không lấy được cookies Google từ profile qua Chrome CDP")
            return None
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
        
        # Reset Chrome CDP tab/page state
        LabsFlowClient._zendriver_reset_page(cookie_hash)
        LabsFlowClient._zendriver_cookies_injected.pop(cookie_hash, None)
        
        # 5. Reset error counters
        LabsFlowClient._token_timestamps.pop(cookie_hash, None)
        self._reset_all_error_counters()
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
        """Re-login profile bằng Chrome CDP off-screen để lấy cookie mới."""
        from pathlib import Path

        print(f"  🔐 [Headless Login] Bắt đầu re-login cho {email} qua Chrome CDP...")

        try:
            from chrome_cdp_cookie import ChromeCDPSession

            profile_dir = Path(profile_path)
            profile_dir.mkdir(parents=True, exist_ok=True)

            session = ChromeCDPSession(
                profile_path=str(profile_dir),
                headless=False,
                window_pos=(-3000, -3000),
                window_size=(400, 300),
                log_fn=lambda msg: print(f"  {msg}"),
            )

            try:
                extracted = session.extract_cookies(
                    email=email,
                    password=password,
                    force_login=True,
                )
            finally:
                session.close()

            google_cookies = {
                cookie["name"]: cookie["value"]
                for cookie in extracted or []
                if cookie.get("name") and cookie.get("value")
            }

            if google_cookies:
                print(f"  ✅ [Headless Login] Lấy được {len(google_cookies)} cookies mới qua Chrome CDP")
                return google_cookies

            print(f"  ❌ [Headless Login] Không lấy được cookies qua Chrome CDP")
            return None
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

    # region Legacy Browser Paths
    @classmethod
    def _ensure_recaptcha_worker(cls):
        return
    
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
        return None
                    
    @classmethod
    def _legacy_recaptcha_worker_disabled(
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
        return None

    def _legacy_recaptcha_client_disabled(
        self,
        timeout_s: int = 90,
        max_retries_on_403: int = 3,  # giữ tham số cho backward-compat
        acquire_lock: bool = True,
        recaptcha_action: str = "VIDEO_GENERATION",  # ✅ Thêm parameter action
    ) -> Optional[str]:
        return None

    # endregion Legacy Browser Paths

    def _legacy_context_restart_disabled(self) -> bool:
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
        profile_key, instance = cls._get_or_create_chrome_cdp_instance(profile_path)

        if instance.get("started"):
            proc = instance.get("process")
            if proc is None or proc.poll() is None:
                return instance.get("port"), profile_key
            instance["started"] = False
            instance["process"] = None

        with instance["lock"]:
            if instance.get("started"):
                proc = instance.get("process")
                if proc is None or proc.poll() is None:
                    return instance.get("port"), profile_key

            chrome_path = cls._find_chrome_binary()
            if not chrome_path:
                print("  ⚠️ [Chrome CDP] Không tìm thấy Chrome binary")
                return None, profile_key
            
            import subprocess
            import tempfile
            
            if not instance.get("user_data_dir"):
                if profile_path and os.path.exists(profile_path):
                    import shutil
                    temp_dir = tempfile.mkdtemp(prefix="chrome_cdp_profile_")
                    try:
                        result = subprocess.run(
                            ["robocopy", profile_path, temp_dir, "/E", "/B", "/NFL", "/NDL", "/NJH", "/NJS", "/NC", "/NS"],
                            capture_output=True, timeout=30
                        )
                        if result.returncode >= 8:
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

                    instance["user_data_dir"] = temp_dir
                    instance["profile_path"] = profile_path
                else:
                    instance["user_data_dir"] = tempfile.mkdtemp(prefix="chrome_cdp_recaptcha_")
                    instance["profile_path"] = None
                    if profile_path:
                        print(f"  ⚠️ [Chrome CDP] Profile path không tồn tại: {profile_path}, dùng temp dir trống")
            
            port = instance.get("port", 9222)
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
            instance["port"] = port
            
            chrome_args = [
                chrome_path,
                f"--remote-debugging-port={port}",
                f"--user-data-dir={instance['user_data_dir']}",
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
                # Off-screen nhưng vẫn là Chrome thật, không headless
                "--window-position=-3000,-3000",
                "--window-size=400,300",
            ]
            
            try:
                proc = subprocess.Popen(
                    chrome_args,
                    stdout=subprocess.DEVNULL,
                    stderr=subprocess.DEVNULL,
                )
                instance["process"] = proc
                instance["started"] = True
                print(f"  🚀 [Chrome CDP] Chrome launched (PID={proc.pid}, port={port})")
                
                wait_start = time.time()
                while time.time() - wait_start < 15:
                    try:
                        resp = requests.get(f"http://127.0.0.1:{port}/json/version", timeout=2)
                        if resp.status_code == 200:
                            version_info = resp.json()
                            print(f"  ✅ [Chrome CDP] Chrome sẵn sàng: {version_info.get('Browser', 'unknown')}")
                            return port, profile_key
                    except Exception:
                        pass
                    time.sleep(0.3)
                
                print("  ⚠️ [Chrome CDP] Timeout chờ Chrome khởi động")
                return port, profile_key
                
            except Exception as e:
                print(f"  ❌ [Chrome CDP] Lỗi launch Chrome: {e}")
                instance["started"] = False
                instance["process"] = None
                return None, profile_key
    
    @classmethod
    def _zendriver_reset_page(cls, cookie_hash: str):
        """Reset page/tab cho cookie (khi cần re-inject cookies)."""
        for session_key in cls._iter_chrome_cdp_session_keys(cookie_hash):
            old_ws = cls._chrome_cdp_ws_conns.pop(session_key, None)
            if old_ws:
                try:
                    old_ws.close()
                except Exception:
                    pass
            cls._chrome_cdp_ws_msg_ids.pop(session_key, None)
            cls._chrome_cdp_page_ready.pop(session_key, None)
            
            tab_id = cls._chrome_cdp_tab_ids.pop(session_key, None)
            port = cls._chrome_cdp_cookie_ports.get(session_key) or cls._chrome_cdp_cookie_ports.get(cookie_hash)
            if tab_id and port:
                try:
                    requests.get(
                        f"http://127.0.0.1:{port}/json/close/{tab_id}",
                        timeout=3,
                    )
                except Exception:
                    pass
            cls._chrome_cdp_pages.pop(session_key, None)
            cls._chrome_cdp_cookies_injected.pop(session_key, None)
            cls._chrome_cdp_cookie_profiles.pop(session_key, None)
            cls._chrome_cdp_cookie_ports.pop(session_key, None)
            cls._zendriver_pages.pop(session_key, None)
            cls._zendriver_cookies_injected.pop(session_key, None)

        cls._chrome_cdp_cookie_profiles.pop(cookie_hash, None)
        cls._chrome_cdp_cookie_ports.pop(cookie_hash, None)
        cls._zendriver_pages.pop(cookie_hash, None)
        cls._zendriver_cookies_injected.pop(cookie_hash, None)
    
    def _get_recaptcha_token_zendriver(
        self,
        timeout_s: int = 60,
        recaptcha_action: str = "VIDEO_GENERATION",
    ) -> Optional[str]:
        """
        Lấy reCAPTCHA token qua Chrome thật + CDP protocol.
        Chrome thật cho trust score cao hơn browser automation cũ.
        
        ✅ Improvements:
        - Persistent WebSocket connection (không open/close mỗi lần)
        - Không reload page nếu đã load sẵn → execute grecaptcha trực tiếp
        - Dùng GET thay PUT cho /json/new (đúng Chrome DevTools spec)
        - Robust cdp_send với per-command timeout
        - Auto-recovery khi WebSocket bị stale
        """
        cookie_hash = self._cookie_hash
        cookie_lock = LabsFlowClient._get_or_create_cookie_lock(cookie_hash)
        with cookie_lock:
            profile_path = self._get_profile_path_for_cookie()
            port, profile_key = LabsFlowClient._ensure_zendriver_worker(profile_path=profile_path)
            if not port:
                print("  ⚠️ [Chrome CDP] Chrome chưa sẵn sàng")
                return None
            session_key = LabsFlowClient._make_chrome_cdp_session_key(cookie_hash, profile_key)

            LabsFlowClient._chrome_cdp_cookie_profiles[session_key] = profile_key
            LabsFlowClient._chrome_cdp_cookie_ports[session_key] = port

            SITE_KEY = "6LdsFiUsAAAAAIjVDZcuLhaHiDn5nnHVXVRQGeMV"
            TARGET_URL = "https://labs.google/fx/tools/flow"

            def _create_new_tab() -> Optional[tuple]:
                """Tạo tab mới, trả về (ws_url, tab_id) hoặc None."""
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

                existing = LabsFlowClient._chrome_cdp_ws_conns.get(session_key)
                if existing is not None:
                    try:
                        existing.ping()
                        return existing
                    except Exception:
                        try:
                            existing.close()
                        except Exception:
                            pass
                        LabsFlowClient._chrome_cdp_ws_conns.pop(session_key, None)
                
                try:
                    conn = ws_sync.connect(ws_url, close_timeout=5, open_timeout=10)
                    LabsFlowClient._chrome_cdp_ws_conns[session_key] = conn
                    LabsFlowClient._chrome_cdp_ws_msg_ids[session_key] = 1
                    return conn
                except Exception as e:
                    print(f"  ⚠️ [Chrome CDP] WS connect failed: {e}")
                    return None

            def _cdp_send(ws, method: str, params: dict = None, cmd_timeout: float = 30) -> dict:
                """Gửi CDP command và nhận response. Per-command timeout."""
                msg_id = LabsFlowClient._chrome_cdp_ws_msg_ids.get(session_key, 1)
                payload = {"id": msg_id, "method": method}
                if params:
                    payload["params"] = params
                LabsFlowClient._chrome_cdp_ws_msg_ids[session_key] = msg_id + 1

                ws.send(json.dumps(payload))

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
                ws_url = LabsFlowClient._chrome_cdp_pages.get(session_key)
                need_navigate = False
                page_ready = LabsFlowClient._chrome_cdp_page_ready.get(session_key, False)

                if ws_url is None:
                    result = _create_new_tab()
                    if not result:
                        print("  ⚠️ [Chrome CDP] Không tạo được tab mới")
                        return None
                    ws_url, tab_id = result
                    LabsFlowClient._chrome_cdp_pages[session_key] = ws_url
                    LabsFlowClient._chrome_cdp_tab_ids[session_key] = tab_id
                    LabsFlowClient._chrome_cdp_cookies_injected[session_key] = False
                    LabsFlowClient._chrome_cdp_page_ready[session_key] = False
                    LabsFlowClient._zendriver_pages[session_key] = ws_url
                    LabsFlowClient._zendriver_cookies_injected[session_key] = False
                    need_navigate = True
                    page_ready = False
                    print(f"  📄 [Chrome CDP] Tạo tab mới cho cookie {cookie_hash[:8]}...")

                ws = _get_or_create_ws(ws_url)
                if ws is None:
                    print("  🔄 [Chrome CDP] WS stale, tạo tab mới...")
                    LabsFlowClient._zendriver_reset_page(cookie_hash)
                    LabsFlowClient._chrome_cdp_ws_conns.pop(session_key, None)

                    result = _create_new_tab()
                    if not result:
                        print("  ⚠️ [Chrome CDP] Không tạo được tab mới (retry)")
                        return None
                    ws_url, tab_id = result
                    LabsFlowClient._chrome_cdp_pages[session_key] = ws_url
                    LabsFlowClient._chrome_cdp_tab_ids[session_key] = tab_id
                    LabsFlowClient._chrome_cdp_cookies_injected[session_key] = False
                    LabsFlowClient._chrome_cdp_page_ready[session_key] = False
                    LabsFlowClient._zendriver_pages[session_key] = ws_url
                    LabsFlowClient._zendriver_cookies_injected[session_key] = False
                    need_navigate = True
                    page_ready = False

                    ws = _get_or_create_ws(ws_url)
                    if ws is None:
                        return None

                has_profile = profile_key != "__default__"
                profile_cookies_ok = False

                if has_profile and need_navigate and not LabsFlowClient._chrome_cdp_cookies_injected.get(session_key, False):
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

                    try:
                        loc_result = _cdp_send(ws, "Runtime.evaluate", {
                            "expression": "window.location.href",
                            "returnByValue": True,
                        }, cmd_timeout=5)
                        current_url = loc_result.get("result", {}).get("result", {}).get("value", "")
                        if "accounts.google" not in current_url and "signin" not in current_url.lower():
                            print(f"  ✅ [Chrome CDP] Profile cookies hợp lệ, không cần inject CDP cookies")
                            profile_cookies_ok = True
                            LabsFlowClient._chrome_cdp_cookies_injected[session_key] = True
                            LabsFlowClient._zendriver_cookies_injected[session_key] = True
                            need_navigate = False
                            page_ready = False
                        else:
                            print(f"  ⚠️ [Chrome CDP] Profile cookies expired, sẽ inject CDP cookies...")
                    except Exception:
                        pass

                if not profile_cookies_ok and not LabsFlowClient._chrome_cdp_cookies_injected.get(session_key, False):
                    _cdp_send(ws, "Network.enable", cmd_timeout=10)

                    inject_success = 0
                    inject_fail = 0
                    for name, value in self.cookies.items():
                        try:
                            if name.startswith("__Host-"):
                                result = _cdp_send(ws, "Network.setCookie", {
                                    "name": name,
                                    "value": value,
                                    "url": "https://labs.google/fx/tools/flow",
                                    "path": "/",
                                    "secure": True,
                                    "httpOnly": True,
                                }, cmd_timeout=5)
                            elif name.startswith("__Secure-"):
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
                                result = {}
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

                            success = result.get("result", {}).get("success", True) if result else False
                            if success and "error" not in result:
                                inject_success += 1
                            else:
                                inject_fail += 1
                        except Exception:
                            inject_fail += 1

                    try:
                        verify_result = _cdp_send(ws, "Network.getCookies", {
                            "urls": ["https://labs.google/fx/tools/flow"]
                        }, cmd_timeout=5)
                        actual_cookies = verify_result.get("result", {}).get("cookies", [])
                        session_found = any(c.get("name") == "__Secure-next-auth.session-token" for c in actual_cookies)
                        if not session_found:
                            print(f"  ⚠️ [Chrome CDP] Session token KHÔNG có trong browser sau inject! Thử lại...")
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

                    LabsFlowClient._chrome_cdp_cookies_injected[session_key] = True
                    LabsFlowClient._zendriver_cookies_injected[session_key] = True
                    need_navigate = True
                    page_ready = False
                    print(f"  🍪 [Chrome CDP] Đã inject {inject_success} cookies OK, {inject_fail} failed")

                if need_navigate:
                    print(f"  🌐 [Chrome CDP] Navigate đến {TARGET_URL}...")
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
                    page_ready = False
                elif page_ready:
                    print(f"  ⚡ [Chrome CDP] Page sẵn sàng, execute trực tiếp...")

                try:
                    loc_result = _cdp_send(ws, "Runtime.evaluate", {
                        "expression": "window.location.href",
                        "returnByValue": True,
                    }, cmd_timeout=5)
                    current_url = loc_result.get("result", {}).get("result", {}).get("value", "")
                    if "accounts.google" in current_url or "signin" in current_url.lower():
                        print(f"  ⚠️ [Chrome CDP] Redirected to login - cookie expired")
                        LabsFlowClient._zendriver_reset_page(cookie_hash)
                        LabsFlowClient._chrome_cdp_page_ready.pop(session_key, None)
                        LabsFlowClient._chrome_cdp_ws_conns.pop(session_key, None)
                        try:
                            ws.close()
                        except Exception:
                            pass
                        return None
                except Exception:
                    pass

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
                        LabsFlowClient._chrome_cdp_page_ready[session_key] = False
                        return None

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
                    LabsFlowClient._chrome_cdp_page_ready[session_key] = True
                    print(f"  ✅ [Chrome CDP] Token OK (len={len(val)})")
                    return val
                if isinstance(val, str) and val.startswith("ERROR:"):
                    print(f"  ⚠️ [Chrome CDP] reCAPTCHA error: {val[6:]}")
                    LabsFlowClient._chrome_cdp_page_ready[session_key] = False
                else:
                    print(f"  ⚠️ [Chrome CDP] Unexpected result: {val}")
                    LabsFlowClient._chrome_cdp_page_ready[session_key] = False
                return None

            except Exception as e:
                print(f"  ⚠️ [Chrome CDP] Error: {e}")
                import traceback
                traceback.print_exc()
                old_ws = LabsFlowClient._chrome_cdp_ws_conns.pop(session_key, None)
                if old_ws:
                    try:
                        old_ws.close()
                    except Exception:
                        pass
                LabsFlowClient._chrome_cdp_page_ready.pop(session_key, None)
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
        
        Hệ thống hiện chỉ dùng Chrome CDP off-screen.
        """
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
        
        # ✅ Reset all error counters cho cookie này
        self._reset_all_error_counters()
        LabsFlowClient._chrome_cdp_consecutive_403[cookie_hash] = 0
        
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
        LabsFlowClient._chrome_cdp_consecutive_403[cookie_hash] = 0
        promoted_profile = LabsFlowClient._cookie_profile_fallbacks.get(cookie_hash)
        if promoted_profile:
            LabsFlowClient._cookie_profile_paths[cookie_hash] = promoted_profile
            self.profile_path = promoted_profile
            LabsFlowClient._cookie_profile_fallbacks.pop(cookie_hash, None)
            print(f"  ✅ [Profile] Promote profile mới thành profile chính: {Path(promoted_profile).name}")
        profile_path = self._get_profile_path_for_cookie()
        if profile_path:
            LabsFlowClient._chrome_cdp_profile_403[profile_path] = 0
            with LabsFlowClient._profile_health_lock:
                LabsFlowClient._profile_cooldown_until.pop(profile_path, None)
        self._reset_all_error_counters()
        # ✅ Reset 403 refresh retries khi thành công
        if hasattr(self, '_403_refresh_retries'):
            self._403_refresh_retries[cookie_hash] = 0
    
    def _on_api_403(self):
        """Gọi khi API trả về 403 - tăng counter cho source đã dùng."""
        cookie_hash = self._cookie_hash
        source = LabsFlowClient._last_token_source.get(cookie_hash, "chrome_cdp")
        
        if source == "chrome_cdp":
            count = LabsFlowClient._chrome_cdp_consecutive_403.get(cookie_hash, 0) + 1
            LabsFlowClient._chrome_cdp_consecutive_403[cookie_hash] = count
            print(f"  📊 [Token Source] Chrome CDP 403 count: {count}/{self.MAX_CHROME_CDP_403}")
            profile_path = self._get_profile_path_for_cookie()
            if profile_path:
                pcount = LabsFlowClient._chrome_cdp_profile_403.get(profile_path, 0) + 1
                LabsFlowClient._chrome_cdp_profile_403[profile_path] = pcount
                print(f"  📊 [Profile] {Path(profile_path).name} 403 count: {pcount}/{self.MAX_CHROME_CDP_403}")
                if pcount >= self.MAX_CHROME_CDP_403:
                    LabsFlowClient._mark_profile_unhealthy(profile_path, reason="403")
                    alternate = LabsFlowClient._pick_alternate_profile(profile_path)
                    if alternate and alternate != profile_path:
                        LabsFlowClient._cookie_profile_fallbacks[cookie_hash] = alternate
                        self.profile_path = alternate
                        LabsFlowClient._zendriver_reset_page(cookie_hash)
                        print(f"  🔄 [Profile] {Path(profile_path).name} bị 403 liên tiếp, chuyển sang profile: {Path(alternate).name}")
        else:
            count = LabsFlowClient._chrome_cdp_consecutive_403.get(cookie_hash, 0) + 1
            LabsFlowClient._chrome_cdp_consecutive_403[cookie_hash] = count
            print(f"  📊 [Token Source] Chrome CDP 403 count: {count}/{self.MAX_CHROME_CDP_403}")
    
    def _should_use_zendriver(self) -> bool:
        """Quyết định có nên dùng Chrome CDP không (thay thế zendriver)."""
        # Kiểm tra Chrome có sẵn không (chỉ check 1 lần)
        if not LabsFlowClient._chrome_cdp_available:
            LabsFlowClient._check_zendriver_available()
        if not LabsFlowClient._chrome_cdp_available:
            return False
        cookie_hash = self._cookie_hash
        # Nếu Chrome CDP bị 403 quá nhiều → ngừng dùng tạm cho cookie hiện tại
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
        
        ✅ TOKEN SOURCE:
        1. Chrome CDP off-screen (duy nhất)
        
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
        
        # ✅ SOURCE 1: Chrome CDP off-screen (duy nhất)
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
                    print(f"  ⚠️ [Chrome CDP] Không lấy được token, thử reset tab/profile rồi retry 1 lần...")
            except Exception as e:
                print(f"  ⚠️ [Chrome CDP] Error: {e}, thử reset tab/profile rồi retry 1 lần...")

            try:
                LabsFlowClient._zendriver_reset_page(cookie_hash)
                time.sleep(1.0)
                token = self._get_recaptcha_token_zendriver(
                    timeout_s=75,
                    recaptcha_action=recaptcha_action,
                )
                if token and len(token.strip()) > 0:
                    token_generated_at = time.time()
                    self._record_token_source("chrome_cdp")
                    client_context["recaptchaToken"] = token
                    LabsFlowClient._token_timestamps[cookie_hash] = token_generated_at
                    print(f"  ✅ [Chrome CDP] Token injected sau retry (len={len(token)}, ts={token_generated_at:.0f})")
                    return True
                print(f"  ⚠️ [Chrome CDP] Retry vẫn không lấy được token.")
            except Exception as retry_err:
                print(f"  ⚠️ [Chrome CDP] Retry error: {retry_err}")
        
        # Chrome CDP fail
        if raise_on_fail:
            error_msg = f"Không thể lấy reCAPTCHA token bằng Chrome CDP off-screen. {self.last_error_detail or ''}"
            self.last_error_detail = error_msg
            print(f"  ✗ Chrome CDP không lấy được token, raise exception...")
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
                    self._on_api_success()  # ✅ Reset Chrome CDP 403 counters
                    
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
                    self._on_api_success()  # ✅ Reset Chrome CDP 403 counters

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
