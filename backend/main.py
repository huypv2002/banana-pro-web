"""
FastAPI backend for Banana Pro (Flow) image generation.
Runs on VPS Windows.
"""
import os, sys, uuid, time, asyncio, logging, json as _json
from pathlib import Path
from typing import Optional, List
from concurrent.futures import ThreadPoolExecutor

# ✅ Set env TRƯỚC khi import complete_flow (LabsFlowClient đọc env lúc __init__)
os.environ["AUTO_RECAPTCHA"] = "1"
os.environ["RECAPTCHA_MODE"] = "selenium"
os.environ["SELENIUM_HEADLESS"] = "0"

from fastapi import FastAPI, HTTPException, BackgroundTasks
from fastapi.middleware.cors import CORSMiddleware
from pydantic import BaseModel

from complete_flow import LabsFlowClient, _parse_cookie_string

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

app = FastAPI(title="Banana Pro API", version="1.0.0")

ALLOWED_ORIGINS = os.environ.get("ALLOWED_ORIGINS", "*").split(",")
app.add_middleware(CORSMiddleware, allow_origins=ALLOWED_ORIGINS,
                   allow_methods=["*"], allow_headers=["*"])

executor = ThreadPoolExecutor(max_workers=2)
jobs: dict = {}
upscaled_store: dict = {}  # {key: (base64_string, timestamp)}

def _cleanup_upscaled():
    """Remove entries older than 10 minutes."""
    cutoff = time.time() - 600
    expired = [k for k, v in upscaled_store.items() if v[1] < cutoff]
    for k in expired:
        del upscaled_store[k]

PROFILES_DIR = os.environ.get("PROFILES_DIR", r"C:\BananaPro\chrome_profiles").strip()

def get_active_profile():
    """Get first active profile from PROFILES_DIR, fallback to CHROME_PROFILE_PATH."""
    d = Path(PROFILES_DIR)
    if d.is_dir():
        for p in sorted(d.iterdir()):
            if p.is_dir() and not p.name.startswith("."):
                cookies = p / "Default" / "Network" / "Cookies"
                if cookies.exists() and cookies.stat().st_size > 0:
                    return str(p)
    return os.environ.get("CHROME_PROFILE_PATH")

@app.on_event("startup")
def startup_event():
    """Pre-warm Chrome CDP on startup."""
    profile = get_active_profile()
    if profile:
        logger.info(f"Pre-warming Chrome CDP with profile: {profile}")
        try:
            LabsFlowClient._ensure_zendriver_worker(profile_path=profile)
            logger.info("Chrome CDP ready.")
        except Exception as e:
            logger.warning(f"Chrome CDP pre-warm failed: {e}")

ASPECT_MAP = {
    "16:9": "IMAGE_ASPECT_RATIO_LANDSCAPE",
    "9:16": "IMAGE_ASPECT_RATIO_PORTRAIT",
    "1:1":  "IMAGE_ASPECT_RATIO_SQUARE",
    "4:3":  "IMAGE_ASPECT_RATIO_LANDSCAPE",
    "3:4":  "IMAGE_ASPECT_RATIO_PORTRAIT",
}


class GenerateRequest(BaseModel):
    cookie: str
    prompts: List[str]
    model: str = "NARWHAL"
    aspect_ratio: str = "16:9"
    variants: int = 1
    resolution: str = "1k"             # 1k | 2k | 4k
    reference_images: List[str] = []   # base64 images cho Image-to-Image
    folder_images: dict = {}           # {name: [base64...]} cho Folder Structure


class JobStatus(BaseModel):
    job_id: str
    status: str
    total: int
    completed: int
    images: List[dict]
    error: Optional[str] = None


def _parse_cookie_input(raw: str) -> dict:
    raw = raw.strip()
    if raw.startswith("["):
        try:
            items = _json.loads(raw)
            return {c["name"]: c["value"] for c in items if "name" in c and "value" in c}
        except Exception:
            pass
    return _parse_cookie_string(raw)


def _extract_image_url(result: dict) -> str:
    try:
        media = result.get("media") or []
        if media:
            img = media[0].get("image", {}).get("generatedImage", {})
            return img.get("fifeUrl") or img.get("imageUri") or img.get("uri") or ""
    except Exception:
        pass
    return ""


def _extract_media_id(result: dict) -> str:
    try:
        media = result.get("media") or []
        if media:
            return media[0].get("name") or media[0].get("image", {}).get("generatedImage", {}).get("mediaId") or ""
    except Exception:
        pass
    return ""


def _run_generation(job_id: str, cookie: str, prompts: List[str],
                    model: str, aspect_ratio: str, variants: int,
                    resolution: str = "1k",
                    reference_images: list = None, folder_images: dict = None):
    import threading, queue as _queue, random
    job = jobs[job_id]
    job["status"] = "running"
    logger.info(f"[{job_id}] prompts={len(prompts)}, variants={variants}, parallel={variants}, resolution={resolution}")
    try:
        cookies = _parse_cookie_input(cookie)
        if not cookies:
            raise ValueError("Cookie không hợp lệ.")

        client = LabsFlowClient(cookies, profile_path=get_active_profile())
        if not client.fetch_access_token():
            raise ValueError("Không thể lấy access token. Cookie có thể đã hết hạn.")

        project_id = client.flow_project_id
        aspect = ASPECT_MAP.get(aspect_ratio, "IMAGE_ASPECT_RATIO_LANDSCAPE")

        # Upload reference images
        global_image_inputs = []
        if reference_images:
            for b64 in reference_images:
                try:
                    import tempfile, base64 as _b64
                    header, data = b64.split(',', 1) if ',' in b64 else ('', b64)
                    ext = 'jpg'
                    if 'png' in header: ext = 'png'
                    elif 'webp' in header: ext = 'webp'
                    with tempfile.NamedTemporaryFile(suffix=f'.{ext}', delete=False) as f:
                        f.write(_b64.b64decode(data))
                        tmp_path = f.name
                    media_id = client.upload_image(tmp_path)
                    os.unlink(tmp_path)
                    if media_id:
                        global_image_inputs.append({"name": media_id.strip(), "imageInputType": "IMAGE_INPUT_TYPE_REFERENCE"})
                except Exception as e:
                    logger.warning(f"Upload ref image failed: {e}")

        # Build task queue: each task = (global_index, prompt_index, prompt, variant_num)
        task_queue = _queue.Queue()
        total = len(prompts) * variants
        # Results array pre-filled with None
        results = [None] * total
        idx = 0
        for pi, prompt in enumerate(prompts):
            for v in range(variants):
                task_queue.put((idx, pi, prompt, v))
                idx += 1

        lock = threading.Lock()

        def worker(worker_id):
            while not job.get("cancelled"):
                try:
                    task_idx, pi, prompt, v = task_queue.get_nowait()
                except _queue.Empty:
                    break
                try:
                    # Build image inputs for this prompt
                    image_inputs = list(global_image_inputs) if pi == 0 and global_image_inputs else []
                    per_prompt_ref = (folder_images or {}).get("__per_prompt_ref", {})
                    per_imgs = per_prompt_ref.get(str(pi), [])
                    if per_imgs:
                        for b64 in per_imgs[:3]:
                            try:
                                import tempfile, base64 as _b64
                                header, data = b64.split(',', 1) if ',' in b64 else ('', b64)
                                ext = 'jpg'
                                if 'png' in header: ext = 'png'
                                with tempfile.NamedTemporaryFile(suffix=f'.{ext}', delete=False) as f:
                                    f.write(_b64.b64decode(data))
                                    tmp_path = f.name
                                mid = client.upload_image(tmp_path)
                                os.unlink(tmp_path)
                                if mid:
                                    image_inputs.append({"name": mid.strip(), "imageInputType": "IMAGE_INPUT_TYPE_REFERENCE"})
                            except Exception:
                                pass

                    request_item = {
                        "clientContext": {
                            "sessionId": f";{int(time.time() * 1000)}",
                            "projectId": project_id,
                            "tool": "PINHOLE",
                            "userPaygateTier": "PAYGATE_TIER_TWO",
                        },
                    }
                    if image_inputs:
                        request_item["seed"] = random.randint(1, 999999)
                    request_item["imageModelName"] = model
                    request_item["imageAspectRatio"] = aspect
                    request_item["structuredPrompt"] = {"parts": [{"text": prompt}]}
                    if image_inputs:
                        request_item["imageInputs"] = image_inputs

                    result = client.generate_flow_images([request_item], project_id=project_id)
                    if result:
                        url = _extract_image_url(result)
                        # Upsample if 2k/4k
                        if url and resolution in ("2k", "4k"):
                            media_id = _extract_media_id(result)
                            if media_id:
                                target = "UPSAMPLE_IMAGE_RESOLUTION_2K" if resolution == "2k" else "UPSAMPLE_IMAGE_RESOLUTION_4K"
                                logger.info(f"[{job_id}] Upscale {resolution}: {media_id[:20]}...")
                                up_result = client.upsample_image(media_id, target_resolution=target, project_id=project_id)
                                if up_result and up_result.get("encodedImage"):
                                    # Store base64 separately, keep original URL for thumbnail
                                    up_key = f"{job_id}_{task_idx}"
                                    upscaled_store[up_key] = (up_result["encodedImage"], time.time())
                                    logger.info(f"[{job_id}] Upscale {resolution} OK ({len(up_result['encodedImage'])} chars)")
                        results[task_idx] = {"prompt": prompt, "url": url, "model": model,
                                             "upscaled": f"/upscaled/{job_id}_{task_idx}" if (resolution in ("2k","4k") and f"{job_id}_{task_idx}" in upscaled_store) else None}
                    else:
                        results[task_idx] = {"prompt": prompt, "url": None,
                                             "error": client.last_error_detail or "Tạo ảnh thất bại"}
                except Exception as e:
                    results[task_idx] = {"prompt": prompt, "url": None, "error": str(e)}
                finally:
                    with lock:
                        job["completed"] += 1
                        # Rebuild images list in order (fill completed ones)
                        job["images"] = [r if r else {"prompt": "", "url": None, "error": "pending"} for r in results[:job["completed"]]]
                        # Actually rebuild properly - show all completed in order
                        ordered = []
                        for r in results:
                            if r is not None:
                                ordered.append(r)
                        job["images"] = ordered

        # Launch workers with staggered start (3s delay between each)
        threads = []
        num_workers = min(variants, total)
        for w in range(num_workers):
            t = threading.Thread(target=worker, args=(w,), daemon=True)
            threads.append(t)
            t.start()
            if w < num_workers - 1:
                time.sleep(3)

        for t in threads:
            t.join()

        # Final: set all results in order
        job["images"] = [r if r else {"prompt": "?", "url": None, "error": "Cancelled"} for r in results]
        job["completed"] = total
        job["status"] = "done"
    except Exception as e:
        logger.error(f"[{job_id}] Fatal: {e}")
        job["status"] = "error"
        job["error"] = str(e)


class RecaptchaRequest(BaseModel):
    cookie: str
    action: str = "IMAGE_GENERATION"   # IMAGE_GENERATION | VIDEO_GENERATION

@app.post("/recaptcha-token")
def get_recaptcha_token(req: RecaptchaRequest):
    """Lấy reCAPTCHA token từ Chrome headless trên VPS. Thử nhiều profiles."""
    try:
        cookies = _parse_cookie_input(req.cookie)
        if not cookies:
            return {"ok": False, "error": "Cookie không hợp lệ"}

        # Lấy danh sách profiles có cookies
        profiles = []
        d = Path(PROFILES_DIR)
        if d.is_dir():
            for p in sorted(d.iterdir()):
                if p.is_dir() and not p.name.startswith("."):
                    c = p / "Default" / "Network" / "Cookies"
                    if c.exists() and c.stat().st_size > 0:
                        profiles.append(str(p))
        if not profiles:
            fallback = os.environ.get("CHROME_PROFILE_PATH")
            if fallback:
                profiles = [fallback]

        last_err = ""
        for profile in profiles:
            try:
                logger.info(f"[recaptcha-token] Trying profile: {profile}")
                client = LabsFlowClient(cookies, profile_path=profile)
                if not client.fetch_access_token():
                    last_err = client.last_error_detail or "Không lấy được access token"
                    continue
                ctx = {}
                got = client._maybe_inject_recaptcha(ctx, raise_on_fail=False, recaptcha_action=req.action)
                if got:
                    token = ctx.get("recaptchaToken")
                    if not token:
                        rc = ctx.get("recaptchaContext", {})
                        token = rc.get("token")
                    if token:
                        logger.info(f"[recaptcha-token] OK from profile: {profile}")
                        return {"ok": True, "token": token, "access_token": client.access_token}
                last_err = client.last_error_detail or "Không lấy được token"
                logger.warning(f"[recaptcha-token] Failed profile {profile}: {last_err}")
            except Exception as e:
                last_err = str(e)
                logger.warning(f"[recaptcha-token] Error profile {profile}: {e}")
                continue

        return {"ok": False, "error": last_err or "Tất cả profiles đều thất bại"}
    except Exception as e:
        return {"ok": False, "error": str(e)}


class TestCookieRequest(BaseModel):
    cookie: str

@app.post("/test-cookie")
def test_cookie(req: TestCookieRequest):
    try:
        cookies = _parse_cookie_input(req.cookie)
        if not cookies:
            return {"ok": False, "error": "Cookie không hợp lệ"}
        client = LabsFlowClient(cookies)
        ok = client.fetch_access_token()
        if ok:
            return {"ok": True, "email": getattr(client, '_last_email', ''), "expires": ""}
        return {"ok": False, "error": client.last_error_detail or "Không lấy được token"}
    except Exception as e:
        return {"ok": False, "error": str(e)}


@app.get("/health")
def health():
    return {"ok": True, "active_profile": get_active_profile()}

@app.get("/upscaled/{key}")
def get_upscaled(key: str):
    from fastapi.responses import Response
    import base64 as _b64
    _cleanup_upscaled()
    entry = upscaled_store.pop(key, None)
    if not entry:
        raise HTTPException(404, "Not found or expired")
    return Response(content=_b64.b64decode(entry[0]), media_type="image/jpeg",
                    headers={"Content-Disposition": f"attachment; filename={key}.jpg"})

@app.get("/profiles")
def list_profiles():
    d = Path(PROFILES_DIR)
    if not d.is_dir():
        return []
    profiles = []
    for p in sorted(d.iterdir()):
        if p.is_dir() and not p.name.startswith("."):
            cookies = p / "Default" / "Network" / "Cookies"
            profiles.append({"name": p.name, "has_cookies": cookies.exists() and cookies.stat().st_size > 0})
    return profiles


@app.post("/generate", response_model=JobStatus)
async def generate(req: GenerateRequest):
    if not req.prompts:
        raise HTTPException(400, "Cần ít nhất 1 prompt")
    if not req.cookie.strip():
        raise HTTPException(400, "Cookie không được để trống")
    req.variants = max(1, min(4, req.variants))

    job_id = str(uuid.uuid4())
    jobs[job_id] = {"status": "pending", "total": len(req.prompts) * req.variants,
                    "completed": 0, "images": [], "error": None, "cancelled": False}

    loop = asyncio.get_event_loop()
    loop.run_in_executor(executor, _run_generation,
                         job_id, req.cookie, req.prompts, req.model, req.aspect_ratio, req.variants,
                         req.resolution, req.reference_images or [], req.folder_images or {})

    return JobStatus(job_id=job_id, **{k: jobs[job_id][k] for k in ("status", "total", "completed", "images", "error")})


@app.get("/jobs/{job_id}", response_model=JobStatus)
def get_job(job_id: str):
    job = jobs.get(job_id)
    if not job:
        raise HTTPException(404, "Job không tồn tại")
    return JobStatus(job_id=job_id, **{k: job[k] for k in ("status", "total", "completed", "images", "error")})


@app.delete("/jobs/{job_id}")
def cancel_job(job_id: str):
    job = jobs.get(job_id)
    if not job:
        raise HTTPException(404, "Job không tồn tại")
    job["cancelled"] = True
    return {"ok": True}
