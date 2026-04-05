"""
FastAPI backend for Banana Pro image generation.
Runs on VPS Windows. Wraps LabsFlowClient from complete_flow.py.
"""
import os
import sys
import uuid
import asyncio
import logging
from pathlib import Path
from typing import Optional, List
from concurrent.futures import ThreadPoolExecutor

from fastapi import FastAPI, HTTPException, BackgroundTasks
from fastapi.middleware.cors import CORSMiddleware
from pydantic import BaseModel

# ── Add gui_app_clone to path (phải có sẵn trên VPS tại C:\BananaPro\gui_app_clone) ──
PARENT_DIR = Path(os.environ.get("GUI_APP_DIR", r"C:\BananaPro\gui_app_clone"))
sys.path.insert(0, str(PARENT_DIR))

from complete_flow import LabsFlowClient, _parse_cookie_string

# ── Env setup for reCAPTCHA (Chrome CDP headless on VPS) ──────────────────────
os.environ.setdefault("AUTO_RECAPTCHA", "1")
os.environ.setdefault("RECAPTCHA_MODE", "selenium")
os.environ.setdefault("SELENIUM_HEADLESS", "1")

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

app = FastAPI(title="Banana Pro API", version="1.0.0")

# ── CORS: allow Cloudflare Pages domain ───────────────────────────────────────
ALLOWED_ORIGINS = os.environ.get(
    "ALLOWED_ORIGINS",
    "*"  # Thay bằng domain Cloudflare Pages của bạn khi deploy
).split(",")

app.add_middleware(
    CORSMiddleware,
    allow_origins=ALLOWED_ORIGINS,
    allow_methods=["*"],
    allow_headers=["*"],
)

# ── Thread pool for blocking LabsFlowClient calls ─────────────────────────────
executor = ThreadPoolExecutor(max_workers=4)

# ── In-memory job store (dùng Redis nếu cần scale) ───────────────────────────
jobs: dict = {}  # job_id -> {"status": ..., "images": [...], "error": ...}


# ── Models ────────────────────────────────────────────────────────────────────
class GenerateRequest(BaseModel):
    cookie: str                          # Cookie string từ Cookie Editor
    prompts: List[str]                   # Danh sách prompts
    model: str = "NARWHAL"               # NARWHAL=Banana Pro 2, GEM_PIX_2=Banana Pro
    aspect_ratio: str = "16:9"
    variants: int = 1                    # Số ảnh mỗi prompt


class JobStatus(BaseModel):
    job_id: str
    status: str                          # pending | running | done | error
    total: int
    completed: int
    images: List[dict]
    error: Optional[str] = None


# ── Helper ────────────────────────────────────────────────────────────────────
def _run_generation(job_id: str, cookie: str, prompts: List[str],
                    model: str, aspect_ratio: str, variants: int):
    """Blocking function chạy trong thread pool."""
    job = jobs[job_id]
    job["status"] = "running"

    try:
        cookies = _parse_cookie_string(cookie)
        if not cookies:
            raise ValueError("Cookie không hợp lệ. Vui lòng kiểm tra lại.")

        client = LabsFlowClient(cookies)

        if not client.fetch_access_token():
            raise ValueError("Không thể lấy access token. Cookie có thể đã hết hạn.")

        # Lấy workflow_id (project)
        workflow_id = client.flow_project_id

        for prompt in prompts:
            if job.get("cancelled"):
                break
            for _ in range(variants):
                if job.get("cancelled"):
                    break
                try:
                    result = client.generate_image_from_text(
                        workflow_id=workflow_id,
                        prompt=prompt,
                        image_model=model,
                        aspect_ratio=aspect_ratio,
                    )
                    if result:
                        # result chứa image URL hoặc base64
                        image_url = (
                            result.get("imageUrl")
                            or result.get("url")
                            or result.get("image_url")
                            or ""
                        )
                        job["images"].append({
                            "prompt": prompt,
                            "url": image_url,
                            "model": model,
                        })
                    else:
                        job["images"].append({
                            "prompt": prompt,
                            "url": None,
                            "error": client.last_error_detail or "Tạo ảnh thất bại",
                        })
                except Exception as e:
                    logger.error(f"[{job_id}] Error generating image: {e}")
                    job["images"].append({
                        "prompt": prompt,
                        "url": None,
                        "error": str(e),
                    })
                finally:
                    job["completed"] += 1

        job["status"] = "done"

    except Exception as e:
        logger.error(f"[{job_id}] Fatal error: {e}")
        job["status"] = "error"
        job["error"] = str(e)


# ── Routes ────────────────────────────────────────────────────────────────────
@app.get("/health")
def health():
    return {"ok": True}


@app.post("/generate", response_model=JobStatus)
async def generate(req: GenerateRequest, background_tasks: BackgroundTasks):
    if not req.prompts:
        raise HTTPException(400, "Cần ít nhất 1 prompt")
    if not req.cookie.strip():
        raise HTTPException(400, "Cookie không được để trống")

    job_id = str(uuid.uuid4())
    total = len(req.prompts) * req.variants
    jobs[job_id] = {
        "status": "pending",
        "total": total,
        "completed": 0,
        "images": [],
        "error": None,
        "cancelled": False,
    }

    # Chạy trong thread pool để không block event loop
    loop = asyncio.get_event_loop()
    loop.run_in_executor(
        executor,
        _run_generation,
        job_id, req.cookie, req.prompts,
        req.model, req.aspect_ratio, req.variants,
    )

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
