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

@app.on_event("startup")
def startup_event():
    """Pre-warm Chrome CDP on startup."""
    profile = os.environ.get("CHROME_PROFILE_PATH")
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


def _run_generation(job_id: str, cookie: str, prompts: List[str],
                    model: str, aspect_ratio: str, variants: int):
    job = jobs[job_id]
    job["status"] = "running"
    try:
        cookies = _parse_cookie_input(cookie)
        if not cookies:
            raise ValueError("Cookie không hợp lệ.")

        client = LabsFlowClient(cookies, profile_path=os.environ.get("CHROME_PROFILE_PATH"))
        if not client.fetch_access_token():
            raise ValueError("Không thể lấy access token. Cookie có thể đã hết hạn.")

        project_id = client.flow_project_id
        aspect = ASPECT_MAP.get(aspect_ratio, "IMAGE_ASPECT_RATIO_LANDSCAPE")

        for prompt in prompts:
            if job.get("cancelled"):
                break
            for _ in range(variants):
                if job.get("cancelled"):
                    break
                try:
                    request_item = {
                        "clientContext": {
                            "sessionId": f";{int(time.time() * 1000)}",
                            "projectId": project_id,
                            "tool": "PINHOLE",
                            "userPaygateTier": "PAYGATE_TIER_TWO",
                        },
                        "imageModelName": model,
                        "imageAspectRatio": aspect,
                        "structuredPrompt": {"parts": [{"text": prompt}]},
                    }
                    result = client.generate_flow_images([request_item], project_id=project_id)
                    if result:
                        url = _extract_image_url(result)
                        job["images"].append({"prompt": prompt, "url": url, "model": model})
                    else:
                        job["images"].append({"prompt": prompt, "url": None,
                                              "error": client.last_error_detail or "Tạo ảnh thất bại"})
                except Exception as e:
                    job["images"].append({"prompt": prompt, "url": None, "error": str(e)})
                finally:
                    job["completed"] += 1

        job["status"] = "done"
    except Exception as e:
        logger.error(f"[{job_id}] Fatal: {e}")
        job["status"] = "error"
        job["error"] = str(e)


@app.get("/health")
def health():
    return {"ok": True}


@app.post("/generate", response_model=JobStatus)
async def generate(req: GenerateRequest):
    if not req.prompts:
        raise HTTPException(400, "Cần ít nhất 1 prompt")
    if not req.cookie.strip():
        raise HTTPException(400, "Cookie không được để trống")

    job_id = str(uuid.uuid4())
    jobs[job_id] = {"status": "pending", "total": len(req.prompts) * req.variants,
                    "completed": 0, "images": [], "error": None, "cancelled": False}

    loop = asyncio.get_event_loop()
    loop.run_in_executor(executor, _run_generation,
                         job_id, req.cookie, req.prompts, req.model, req.aspect_ratio, req.variants)

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
