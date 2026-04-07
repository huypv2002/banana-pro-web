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
                                logger.info(f"[{job_id}][w{worker_id}] Upscale {resolution}: {media_id[:20]}...")
                                up_ok = False
                                for up_try in range(2):
                                    time.sleep(3)  # Delay to avoid rate limit after gen
                                    up_result = client.upsample_image(media_id, target_resolution=target, project_id=project_id)
                                    if up_result and up_result.get("encodedImage"):
                                        up_key = f"{job_id}_{task_idx}"
                                        upscaled_store[up_key] = (up_result["encodedImage"], time.time())
                                        logger.info(f"[{job_id}][w{worker_id}] Upscale OK ({len(up_result['encodedImage'])} chars)")
                                        up_ok = True
                                        break
                                    else:
                                        logger.warning(f"[{job_id}][w{worker_id}] Upscale attempt {up_try+1} failed: {client.last_error_detail}")
                                        time.sleep(2)
                                if not up_ok:
                                    logger.warning(f"[{job_id}][w{worker_id}] Upscale failed after retries, keeping 1K")
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


# ── Video Generation ──────────────────────────────────────────────────────────

VIDEO_MODEL_MAP = {
    "t2v_low_16_9":     "veo_3_1_t2v_fast_ultra_relaxed",
    "t2v_fast_16_9":    "veo_3_1_t2v_fast_ultra",
    "t2v_quality_16_9": "veo_3_1_t2v",
    "t2v_low_9_16":     "veo_3_1_t2v_fast_ultra_relaxed",
    "t2v_fast_9_16":    "veo_3_1_t2v_fast_ultra",
    "t2v_quality_9_16": "veo_3_1_t2v",
    "low_fast_16_9":    "veo_3_1_t2v_fast_ultra_relaxed",
    "fast_16_9":        "veo_3_1_t2v_fast_ultra",
    "quality_16_9":     "veo_3_1_t2v",
    "low_fast_9_16":    "veo_3_1_t2v_fast_ultra_relaxed",
    "fast_9_16":        "veo_3_1_t2v_fast_ultra",
    "quality_9_16":     "veo_3_1_t2v",
}

VIDEO_I2V_MODEL_MAP = {
    "i2v_low_16_9":     "veo_3_1_i2v_s_fast_ultra_relaxed",
    "i2v_fast_16_9":    "veo_3_1_i2v_s_fast_ultra",
    "i2v_quality_16_9": "veo_3_1_i2v_s",
    "i2v_low_9_16":     "veo_3_1_i2v_s_fast_ultra_relaxed",
    "i2v_fast_9_16":    "veo_3_1_i2v_s_fast_ultra",
    "i2v_quality_9_16": "veo_3_1_i2v_s",
    "fast_i2v":         "veo_3_1_i2v_s_fast_ultra",
    "quality_i2v":      "veo_3_1_i2v_s",
}

VIDEO_FL_MODEL_MAP = {
    # Start+End (first+last frame)
    "fl_low_16_9":     "veo_3_1_i2v_s_fast_ultra_relaxed",
    "fl_fast_16_9":    "veo_3_1_i2v_s_fast_ultra_fl",
    "fl_quality_16_9": "veo_3_1_i2v_s_landscape_fl",
    "fl_low_9_16":     "veo_3_1_i2v_s_fast_ultra_relaxed",
    "fl_fast_9_16":    "veo_3_1_i2v_s_fast_portrait_ultra_fl",
    "fl_quality_9_16": "veo_3_1_i2v_s_portrait_fl",
}

VIDEO_R2V_MODEL_MAP = {
    # Reference to video
    "r2v_low_16_9":  "veo_3_1_r2v_fast_landscape_ultra_relaxed",
    "r2v_fast_16_9": "veo_3_1_r2v_fast_landscape_ultra",
    "r2v_low_9_16":  "veo_3_1_r2v_fast_portrait_ultra_relaxed",
    "r2v_fast_9_16": "veo_3_1_r2v_fast_portrait_ultra",
}

class VideoGenerateRequest(BaseModel):
    cookie: str = ""
    prompts: List[str]
    mode: str = "t2v"
    model: str = "t2v_fast_16_9"
    num_videos: int = 1
    ref_images: dict = {}
    end_images: dict = {}
    delay: int = 3        # giây delay giữa các prompt (tuần tự)
    workers: int = 3      # số worker song song (chỉ T2V)
    # Legacy
    t2v_model: str = ""
    i2v_model: str = ""

class VideoFromImageRequest(BaseModel):
    cookie: str = ""
    prompts: List[str]
    image: str          # base64 data URL
    model: str = "fast_i2v"
    num_videos: int = 1

@app.post("/generate-video")
async def generate_video(req: VideoGenerateRequest):
    if not req.prompts:
        raise HTTPException(400, "Cần ít nhất 1 prompt")
    req.num_videos = max(1, min(4, req.num_videos))
    # Legacy compat
    if not req.model or req.model == "fast_16_9":
        if req.t2v_model: req.model = req.t2v_model
        elif req.i2v_model: req.model = req.i2v_model; req.mode = "i2v"
    job_id = str(uuid.uuid4())
    jobs[job_id] = {"status": "pending", "total": len(req.prompts),
                    "completed": 0, "videos": [], "error": None, "cancelled": False}
    loop = asyncio.get_event_loop()
    loop.run_in_executor(executor, _run_video_generation,
                         job_id, req.cookie, req.prompts, req.mode, req.model,
                         req.num_videos, req.ref_images or {}, req.end_images or {},
                         req.delay, req.workers)
    return {"job_id": job_id, "status": "pending", "total": len(req.prompts)}


@app.post("/generate-video-from-image")
async def generate_video_from_image(req: VideoFromImageRequest):
    if not req.prompts:
        raise HTTPException(400, "Cần ít nhất 1 prompt")
    if not req.image:
        raise HTTPException(400, "Cần ảnh đầu vào")
    req.num_videos = max(1, min(4, req.num_videos))
    job_id = str(uuid.uuid4())
    jobs[job_id] = {"status": "pending", "total": len(req.prompts),
                    "completed": 0, "videos": [], "error": None, "cancelled": False}
    loop = asyncio.get_event_loop()
    loop.run_in_executor(executor, _run_video_from_image,
                         job_id, req.cookie, req.prompts, req.image, req.model, req.num_videos)
    return {"job_id": job_id, "status": "pending", "total": len(req.prompts)}

@app.get("/video-jobs/{job_id}")
def get_video_job(job_id: str):
    job = jobs.get(job_id)
    if not job:
        raise HTTPException(404, "Job không tồn tại")
    return {"job_id": job_id, "status": job["status"], "total": job["total"],
            "completed": job["completed"], "videos": job.get("videos", []), "error": job.get("error")}


def _upload_b64(client, b64: str, cache: dict = None) -> str:
    """Upload base64 image, return media_id. Dùng cache để tránh upload lại."""
    import hashlib
    key = hashlib.md5(b64[:200].encode()).hexdigest()  # hash nhanh từ đầu chuỗi
    if cache is not None and key in cache:
        logger.info(f"[Upload Cache] HIT {key[:8]}... → {cache[key][:8]}...")
        return cache[key]
    import tempfile, base64 as _b64
    header, data = b64.split(',', 1) if ',' in b64 else ('', b64)
    ext = 'png' if 'png' in header else 'jpg'
    with tempfile.NamedTemporaryFile(suffix=f'.{ext}', delete=False) as f:
        f.write(_b64.b64decode(data)); tmp = f.name
    mid = client.upload_image(tmp)
    os.unlink(tmp)
    if not mid:
        raise ValueError("Upload ảnh thất bại: " + (client.last_error_detail or ""))
    if cache is not None:
        cache[key] = mid
    return mid


def _generate_r2v(client, project_id: str, prompt: str, media_ids: list,
                  model_key: str, num_videos: int, aspect: str):
    import uuid as _uuid, time as _time, json as _json
    url = "https://aisandbox-pa.googleapis.com/v1/video:batchAsyncGenerateVideoReferenceImages"
    seeds = [int(_time.time() * 1000000 + i) % 100000 for i in range(num_videos)]
    requests_body = [{
        "aspectRatio": aspect,
        "seed": seeds[i],
        "textInput": {"structuredPrompt": {"parts": [{"text": prompt.strip()}]}},
        "videoModelKey": model_key,
        "metadata": {},
        "referenceImages": [{"mediaId": mid, "imageUsageType": "IMAGE_USAGE_TYPE_ASSET"} for mid in media_ids],
    } for i in range(num_videos)]
    payload = {
        "mediaGenerationContext": {"batchId": str(_uuid.uuid4())},
        "clientContext": {
            "sessionId": f";{int(_time.time() * 1000)}",
            "projectId": str(_uuid.uuid4()),
            "tool": "PINHOLE", "userPaygateTier": "PAYGATE_TIER_TWO",
        },
        "requests": requests_body,
        "useV2ModelConfig": True,
    }
    # Inject recaptcha (dùng recaptchaContext format)
    client._maybe_inject_recaptcha(payload["clientContext"], raise_on_fail=False, recaptcha_action="VIDEO_GENERATION")
    client._convert_to_recaptcha_context(payload["clientContext"])
    resp = client.session.post(url, headers=client._aisandbox_headers(), data=_json.dumps(payload), timeout=120)
    if resp.status_code == 200:
        data = resp.json()
        return data.get("operations", [data])
    client.last_error_detail = f"R2V HTTP {resp.status_code}: {resp.text[:300]}"
    return None


def _run_video_generation(job_id: str, cookie: str, prompts: List[str],
                          mode: str, model: str, num_videos: int,
                          ref_images: dict = None, end_images: dict = None,
                          delay: int = 3, workers: int = 3):
    job = jobs[job_id]
    job["status"] = "running"
    ref_images = ref_images or {}
    end_images = end_images or {}
    is_portrait = "9_16" in model
    aspect = "VIDEO_ASPECT_RATIO_PORTRAIT" if is_portrait else "VIDEO_ASPECT_RATIO_LANDSCAPE"
    MAP = {"t2v": VIDEO_MODEL_MAP, "i2v": VIDEO_I2V_MODEL_MAP,
           "fl": VIDEO_FL_MODEL_MAP, "r2v": VIDEO_R2V_MODEL_MAP}
    model_key = MAP.get(mode, VIDEO_MODEL_MAP).get(model, "veo_3_1_t2v_fast_ultra")
    logger.info(f"[VIDEO {job_id}] mode={mode}, key={model_key}, prompts={len(prompts)}, workers={workers if mode=='t2v' else 1}, delay={delay}s")

    try:
        cookies = _parse_cookie_input(cookie)
        if not cookies:
            raise ValueError("Cookie không hợp lệ.")
        client = LabsFlowClient(cookies, profile_path=get_active_profile())
        if not client.fetch_access_token():
            raise ValueError("Không thể lấy access token.")
        project_id = client.flow_project_id
        upload_cache = {}

        def process_prompt(idx, prompt):
            """Xử lý 1 prompt, trả về result dict."""
            if job.get("cancelled"):
                return {"prompt": prompt, "urls": [], "error": "Cancelled"}
            try:
                # Re-fetch token trước mỗi prompt để tránh token hết hạn giữa chừng
                if not client.fetch_access_token():
                    return {"prompt": prompt, "urls": [], "error": "Token hết hạn, vui lòng lấy cookie mới"}
                ref_raw = ref_images.get(str(idx))
                ref_b64_list = ref_raw if isinstance(ref_raw, list) else ([ref_raw] if ref_raw else [])
                ref_b64 = ref_b64_list[0] if ref_b64_list else None
                end_b64 = end_images.get(str(idx))

                if mode == "t2v":
                    result = client.generate_videos(
                        project_id=project_id, tool="BACKBONE", user_tier="PAYGATE_TIER_TWO",
                        prompt=prompt, model_key=model_key, num_videos=num_videos, aspect_ratio=aspect)
                elif mode == "i2v":
                    if not ref_b64: raise ValueError("Chưa có ảnh Start")
                    result = client.generate_videos_from_image(
                        project_id=project_id, tool="BACKBONE", user_tier="PAYGATE_TIER_TWO",
                        prompt=prompt, media_id=_upload_b64(client, ref_b64, upload_cache),
                        model_key=model_key, num_videos=num_videos, aspect_ratio=aspect)
                elif mode == "fl":
                    if not ref_b64 or not end_b64: raise ValueError("Cần cả ảnh Start và End")
                    result = client.generate_videos_from_start_end(
                        project_id=project_id, tool="BACKBONE", user_tier="PAYGATE_TIER_TWO",
                        prompt=prompt,
                        start_media_id=_upload_b64(client, ref_b64, upload_cache),
                        end_media_id=_upload_b64(client, end_b64, upload_cache),
                        model_key=model_key, num_videos=num_videos, aspect_ratio=aspect)
                elif mode == "r2v":
                    if not ref_b64_list: raise ValueError("Chưa có ảnh tham chiếu")
                    media_ids = [_upload_b64(client, b64, upload_cache) for b64 in ref_b64_list[:15]]
                    result = _generate_r2v(client, project_id, prompt, media_ids, model_key, num_videos, aspect)
                else:
                    raise ValueError(f"Mode không hợp lệ: {mode}")

                if result:
                    urls, poll_err = _poll_video_status(client, result if isinstance(result, list) else [result], job_id, idx)
                    if urls:
                        return {"prompt": prompt, "urls": urls, "mode": mode}
                    return {"prompt": prompt, "urls": [], "error": poll_err or "Tạo video thất bại"}
                return {"prompt": prompt, "urls": [], "error": client.last_error_detail or "Thất bại"}
            except Exception as e:
                return {"prompt": prompt, "urls": [], "error": str(e)}

        if mode == "t2v":
            # Parallel workers với nối đuôi (queue-based)
            import threading, queue as _queue
            num_w = max(1, min(workers, len(prompts)))
            results = [None] * len(prompts)
            task_q = _queue.Queue()
            lock = threading.Lock()

            for i, p in enumerate(prompts):
                task_q.put((i, p))

            def worker():
                while not job.get("cancelled"):
                    try:
                        idx, prompt = task_q.get_nowait()
                    except _queue.Empty:
                        break
                    res = process_prompt(idx, prompt)
                    with lock:
                        results[idx] = res
                        job["completed"] += 1
                        job["videos"] = [r for r in results if r is not None]

            threads = []
            for w in range(num_w):
                t = threading.Thread(target=worker, daemon=True)
                threads.append(t)
                t.start()
                if w < num_w - 1:
                    time.sleep(delay)  # stagger start
            for t in threads:
                t.join()

            job["videos"] = [r if r else {"prompt": prompts[i], "urls": [], "error": "Cancelled"} for i, r in enumerate(results)]
        else:
            # Tuần tự với delay
            for idx, prompt in enumerate(prompts):
                if job.get("cancelled"):
                    break
                res = process_prompt(idx, prompt)
                job["videos"].append(res)
                job["completed"] += 1
                if idx < len(prompts) - 1 and not job.get("cancelled"):
                    time.sleep(delay)

        job["status"] = "done"
    except Exception as e:
        logger.error(f"[VIDEO {job_id}] Fatal: {e}")
        job["status"] = "error"
        job["error"] = str(e)


def _extract_video_urls(obj):
    """Extract video URLs from poll response (same logic as GUI app)."""
    def _walk(node, keys):
        results = []
        if isinstance(node, dict):
            for k, v in node.items():
                if k in keys and isinstance(v, str):
                    results.append(v)
                results.extend(_walk(v, keys))
        elif isinstance(node, (list, tuple)):
            for item in node:
                results.extend(_walk(item, keys))
        return results

    candidates = _walk(obj, ("fileUrl", "fifeUrl", "downloadUrl", "uri", "videoUrl", "url"))
    urls = []
    for u in candidates:
        if isinstance(u, str) and u.startswith("http"):
            lo = u.lower()
            if any(ext in lo for ext in (".mp4", ".mov", ".webm")):
                urls.append(u)
            elif "googleapis.com" in lo or "googleusercontent.com" in lo:
                urls.append(u)
            elif "/video/" in lo or "/media/" in lo:
                urls.append(u)
    return urls


def _poll_video_status(client, operations, job_id, prompt_idx, max_wait=600):
    """Poll video generation status. Returns (urls, error_msg)."""
    start = time.time()
    poll_interval = 5
    while time.time() - start < max_wait:
        try:
            status = client.check_video_status(operations)
            if not status or not isinstance(status, dict):
                time.sleep(poll_interval)
                continue
            ops = status.get("operations", [])
            if not ops:
                time.sleep(poll_interval)
                continue

            completed = failed = 0
            total = len(ops)
            fail_reasons = []
            for op in ops:
                op_status = (op.get("status") or "").upper()
                if "COMPLETE" in op_status or "SUCCESS" in op_status:
                    completed += 1
                elif "FAIL" in op_status or "ERROR" in op_status:
                    failed += 1
                    # Try extract error message
                    err = op.get("error") or op.get("errorMessage") or op.get("message") or ""
                    if err:
                        fail_reasons.append(str(err)[:100])

            if completed + failed >= total:
                urls = _extract_video_urls(status)
                logger.info(f"[VIDEO {job_id}][{prompt_idx}] Done: {len(urls)} videos ({completed} ok, {failed} fail)")
                if not urls and failed > 0:
                    err_msg = "; ".join(fail_reasons) if fail_reasons else f"Tạo video thất bại ({failed}/{total} fail)"
                    return [], err_msg
                return urls, None
        except Exception as e:
            logger.warning(f"[VIDEO {job_id}][{prompt_idx}] Poll error: {e}")
        time.sleep(poll_interval)
    logger.warning(f"[VIDEO {job_id}][{prompt_idx}] Timeout after {max_wait}s")
    return [], "Timeout: video chưa hoàn thành sau 10 phút"
