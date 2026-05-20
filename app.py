import os
import re
import uuid
import asyncio
import logging
import secrets
import time
from contextlib import asynccontextmanager
from enum import Enum
from typing import Optional, Dict, AsyncIterator

import httpx
from fastapi import FastAPI, HTTPException, Header, Request
from fastapi.responses import StreamingResponse, FileResponse, Response
from pydantic import BaseModel, Field, field_validator

# ---------------------------------------------------------------------------
# Configuration & logging
# ---------------------------------------------------------------------------

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
)
logger = logging.getLogger("video-rotate")

UPLOAD_DIR = os.path.realpath(os.getenv("UPLOAD_DIR", "/tmp/videos"))
os.makedirs(UPLOAD_DIR, exist_ok=True)

KOOFR_USERNAME = os.getenv("KOOFR_USERNAME", "")
KOOFR_PASSWORD = os.getenv("KOOFR_PASSWORD", "")
KOOFR_BASE_URL = "https://app.koofr.net/dav/Google%20Drive"

# Shared secret to protect /serve and /download. Required in production.
ACCESS_TOKEN = os.getenv("ACCESS_TOKEN", "")

# Concurrency limits
MAX_CONCURRENT_FFMPEG = int(os.getenv("MAX_CONCURRENT_FFMPEG", "2"))
ffmpeg_semaphore = asyncio.Semaphore(MAX_CONCURRENT_FFMPEG)

# Job retention
JOB_TTL_SECONDS = int(os.getenv("JOB_TTL_SECONDS", str(60 * 60 * 6)))  # 6h
FILE_TTL_SECONDS = int(os.getenv("FILE_TTL_SECONDS", str(60 * 60 * 6)))  # 6h
MAX_JOBS = int(os.getenv("MAX_JOBS", "1000"))

# In-memory job store. For multi-worker deployments, swap for Redis.
jobs: Dict[str, dict] = {}

# Safe filename pattern (no slashes, no traversal)
SAFE_NAME_RE = re.compile(r"^[A-Za-z0-9._-]+$")


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def require_token(provided: Optional[str]) -> None:
    """Constant-time token check. If ACCESS_TOKEN is unset, refuse access."""
    if not ACCESS_TOKEN:
        raise HTTPException(
            status_code=503,
            detail="Server not configured: ACCESS_TOKEN missing",
        )
    if not provided or not secrets.compare_digest(provided, ACCESS_TOKEN):
        raise HTTPException(status_code=401, detail="Invalid or missing token")


def safe_join(base: str, *parts: str) -> str:
    """Join paths and ensure the result stays under `base`."""
    base_real = os.path.realpath(base)
    candidate = os.path.realpath(os.path.join(base_real, *parts))
    if not (candidate == base_real or candidate.startswith(base_real + os.sep)):
        raise HTTPException(status_code=400, detail="Invalid path")
    return candidate


def safe_filename(name: str) -> str:
    """Reject anything that isn't a flat, sane filename."""
    name = os.path.basename(name)
    if not name or not SAFE_NAME_RE.match(name):
        raise HTTPException(
            status_code=400,
            detail="Invalid filename: only [A-Za-z0-9._-] allowed",
        )
    return name


class Rotation(str, Enum):
    PORTRAIT = "portrait"
    R90 = "90"
    R180 = "180"
    R270 = "270"


ROTATION_FILTERS = {
    Rotation.PORTRAIT: "transpose=1",
    Rotation.R90: "transpose=1",
    Rotation.R270: "transpose=2",
    Rotation.R180: "hflip,vflip",  # cheaper than transpose=1,transpose=1
}


# ---------------------------------------------------------------------------
# Background cleanup
# ---------------------------------------------------------------------------

async def cleanup_loop():
    """Periodically prune old jobs and orphaned files."""
    while True:
        try:
            now = time.time()

            # Prune jobs
            expired = [
                jid for jid, j in jobs.items()
                if now - j.get("created_at", now) > JOB_TTL_SECONDS
            ]
            for jid in expired:
                jobs.pop(jid, None)

            # Cap job dict size (drop oldest)
            if len(jobs) > MAX_JOBS:
                ordered = sorted(jobs.items(), key=lambda kv: kv[1].get("created_at", 0))
                for jid, _ in ordered[: len(jobs) - MAX_JOBS]:
                    jobs.pop(jid, None)

            # Prune old files
            for fname in os.listdir(UPLOAD_DIR):
                fpath = os.path.join(UPLOAD_DIR, fname)
                try:
                    if now - os.path.getmtime(fpath) > FILE_TTL_SECONDS:
                        os.remove(fpath)
                        logger.info("Removed stale file %s", fname)
                except OSError as e:
                    logger.warning("Cleanup failed for %s: %s", fname, e)

        except Exception as e:
            logger.exception("Cleanup loop error: %s", e)

        await asyncio.sleep(300)  # every 5 min


@asynccontextmanager
async def lifespan(app: FastAPI):
    # Startup: wipe leftover files from previous runs
    for f in os.listdir(UPLOAD_DIR):
        try:
            os.remove(os.path.join(UPLOAD_DIR, f))
        except OSError as e:
            logger.warning("Startup cleanup failed for %s: %s", f, e)

    task = asyncio.create_task(cleanup_loop())
    try:
        yield
    finally:
        task.cancel()
        try:
            await task
        except asyncio.CancelledError:
            pass


app = FastAPI(title="Video Rotate & Serve", lifespan=lifespan)


# ---------------------------------------------------------------------------
# Models
# ---------------------------------------------------------------------------

class RotateRequest(BaseModel):
    url: str
    rotation: Rotation = Rotation.PORTRAIT
    output_filename: Optional[str] = Field(default=None, max_length=120)
    input_username: Optional[str] = None
    input_password: Optional[str] = None
    webdav_url: Optional[str] = None
    webdav_username: Optional[str] = None
    webdav_password: Optional[str] = None

    @field_validator("output_filename")
    @classmethod
    def _validate_output_filename(cls, v):
        if v is None:
            return v
        v = os.path.basename(v)
        if not SAFE_NAME_RE.match(v):
            raise ValueError("output_filename must match [A-Za-z0-9._-]+")
        return v

    @field_validator("url", "webdav_url")
    @classmethod
    def _validate_urls(cls, v):
        if v is None:
            return v
        if not (v.startswith("http://") or v.startswith("https://")):
            raise ValueError("URL must start with http:// or https://")
        return v


# ---------------------------------------------------------------------------
# /serve : stream a file from Koofr (with range support + auth)
# ---------------------------------------------------------------------------

@app.get("/serve/{file_path:path}")
async def serve_file(
    file_path: str,
    request: Request,
    x_access_token: Optional[str] = Header(default=None),
):
    require_token(x_access_token or request.query_params.get("token"))

    if not KOOFR_USERNAME or not KOOFR_PASSWORD:
        raise HTTPException(status_code=500, detail="Koofr credentials not configured")

    # Reject traversal attempts in the path portion
    if ".." in file_path.split("/"):
        raise HTTPException(status_code=400, detail="Invalid path")

    koofr_url = f"{KOOFR_BASE_URL}/{file_path}"
    range_header = request.headers.get("range")

    # Forward Range header to Koofr so we get a real 206 (or 200) with correct body
    upstream_headers = {}
    if range_header:
        upstream_headers["Range"] = range_header

    client = httpx.AsyncClient(
        follow_redirects=True,
        timeout=httpx.Timeout(30.0, read=600.0),
        auth=(KOOFR_USERNAME, KOOFR_PASSWORD),
    )

    try:
        req = client.build_request("GET", koofr_url, headers=upstream_headers)
        response = await client.send(req, stream=True)
    except httpx.HTTPError as e:
        await client.aclose()
        logger.exception("Koofr request failed")
        raise HTTPException(status_code=502, detail=f"Upstream error: {e}") from e

    if response.status_code not in (200, 206):
        status = response.status_code
        await response.aclose()
        await client.aclose()
        raise HTTPException(status_code=status, detail=f"Koofr returned {status}")

    # Build response headers from upstream
    headers = {"Accept-Ranges": "bytes"}
    for h in ("content-length", "content-range", "last-modified", "etag"):
        if h in response.headers:
            headers[h.title()] = response.headers[h]

    filename = file_path.rsplit("/", 1)[-1]
    headers["Content-Disposition"] = f'inline; filename="{filename}"'

    async def stream() -> AsyncIterator[bytes]:
        try:
            async for chunk in response.aiter_bytes(chunk_size=1024 * 1024):
                yield chunk
        finally:
            await response.aclose()
            await client.aclose()

    return StreamingResponse(
        stream(),
        status_code=response.status_code,
        media_type=response.headers.get("content-type", "video/mp4"),
        headers=headers,
    )


# ---------------------------------------------------------------------------
# /rotate : queue a rotation job
# ---------------------------------------------------------------------------

async def process_video(job_id: str, req: RotateRequest):
    output_filename = req.output_filename or f"{job_id}_output.mp4"
    input_path = safe_join(UPLOAD_DIR, f"{job_id}_input.mp4")
    output_path = safe_join(UPLOAD_DIR, output_filename)

    def set_step(step: str, message: Optional[str] = None):
        jobs[job_id]["step"] = step
        if message is not None:
            jobs[job_id]["message"] = message
        logger.info("[%s] %s%s", job_id, step, f" — {message}" if message else "")

    try:
        # ---- Step 1: Download ----
        set_step("downloading")
        auth = None
        if req.input_username and req.input_password:
            auth = (req.input_username, req.input_password)

        async with httpx.AsyncClient(
            follow_redirects=True,
            timeout=httpx.Timeout(30.0, read=600.0),
            auth=auth,
        ) as client:
            async with client.stream("GET", req.url) as response:
                response.raise_for_status()
                with open(input_path, "wb") as f:
                    async for chunk in response.aiter_bytes(chunk_size=1024 * 1024):
                        f.write(chunk)

        # ---- Step 2: Rotate with ffmpeg ----
        set_step("rotating")
        vf_filter = ROTATION_FILTERS[req.rotation]
        cmd = [
            "ffmpeg", "-y",
            "-i", input_path,
            "-vf", vf_filter,
            "-c:v", "libx264",
            "-preset", "veryfast",
            "-crf", "23",
            "-c:a", "copy",
            "-movflags", "+faststart",
            output_path,
        ]

        async with ffmpeg_semaphore:
            process = await asyncio.create_subprocess_exec(
                *cmd,
                stdout=asyncio.subprocess.DEVNULL,
                stderr=asyncio.subprocess.PIPE,
            )
            _, stderr = await process.communicate()

        if process.returncode != 0:
            tail = stderr.decode(errors="replace")[-500:]
            jobs[job_id]["status"] = "error"
            jobs[job_id]["error"] = f"FFmpeg failed: {tail}"
            logger.error("[%s] ffmpeg failed: %s", job_id, tail)
            return

        # ---- Step 3: Upload to WebDAV (optional) ----
        if req.webdav_url and req.webdav_username and req.webdav_password:
            file_size = os.path.getsize(output_path)
            webdav_dest = req.webdav_url.rstrip("/") + "/" + output_filename
            set_step("uploading", f"{file_size} bytes")

            try:
                # httpx.AsyncClient does NOT support sync file objects as `content`
                # (it raises RuntimeError / ReadError mid-upload). The clean fix
                # is to run a sync httpx.Client in a worker thread — the sync
                # client streams file objects natively, and to_thread keeps the
                # event loop free.
                def sync_upload() -> httpx.Response:
                    with httpx.Client(
                        timeout=httpx.Timeout(30.0, read=1200.0, write=1200.0),
                    ) as client:
                        with open(output_path, "rb") as f:
                            return client.put(
                                webdav_dest,
                                content=f,
                                auth=(req.webdav_username, req.webdav_password),
                                headers={
                                    "Content-Type": "video/mp4",
                                    "Content-Length": str(file_size),
                                },
                            )

                resp = await asyncio.to_thread(sync_upload)

                if resp.status_code not in (200, 201, 204):
                    jobs[job_id]["status"] = "error"
                    jobs[job_id]["error"] = (
                        f"WebDAV upload failed: HTTP {resp.status_code} - {resp.text[:300]}"
                    )
                    return

            except Exception as e:
                jobs[job_id]["status"] = "error"
                jobs[job_id]["error"] = f"Upload exception: {type(e).__name__}: {e}"
                logger.exception("[%s] upload failed", job_id)
                return

            jobs[job_id].update(
                status="done", step="complete", output=webdav_dest, error=None
            )
        else:
            jobs[job_id].update(
                status="done",
                step="complete",
                output=f"/download/{output_filename}",
                error=None,
            )

    except Exception as e:
        jobs[job_id]["status"] = "error"
        jobs[job_id]["error"] = f"General error: {type(e).__name__}: {e}"
        logger.exception("[%s] job failed", job_id)
    finally:
        # Always clean the input
        if os.path.exists(input_path):
            try:
                os.remove(input_path)
            except OSError:
                pass
        # Clean output only if it was uploaded elsewhere
        if req.webdav_url and os.path.exists(output_path):
            try:
                os.remove(output_path)
            except OSError:
                pass


@app.post("/rotate")
async def rotate_video(
    request: RotateRequest,
    x_access_token: Optional[str] = Header(default=None),
):
    require_token(x_access_token)

    job_id = uuid.uuid4().hex[:8]
    jobs[job_id] = {
        "status": "processing",
        "step": "queued",
        "error": None,
        "output": None,
        "message": None,
        "created_at": time.time(),
    }
    asyncio.create_task(process_video(job_id, request))
    return {"job_id": job_id, "status": "accepted"}


# ---------------------------------------------------------------------------
# /status, /download, /health
# ---------------------------------------------------------------------------

@app.get("/status/{job_id}")
async def get_status(
    job_id: str,
    x_access_token: Optional[str] = Header(default=None),
):
    require_token(x_access_token)
    if job_id not in jobs:
        raise HTTPException(status_code=404, detail="Job not found")
    return {"job_id": job_id, **jobs[job_id]}


@app.get("/download/{filename}")
async def download_file(
    filename: str,
    request: Request,
    x_access_token: Optional[str] = Header(default=None),
):
    require_token(x_access_token or request.query_params.get("token"))
    filename = safe_filename(filename)
    filepath = safe_join(UPLOAD_DIR, filename)
    if not os.path.exists(filepath):
        raise HTTPException(status_code=404, detail="File not found")
    return FileResponse(filepath, media_type="video/mp4", filename=filename)


@app.get("/health")
async def health():
    return {"status": "ok", "jobs_in_memory": len(jobs)}
