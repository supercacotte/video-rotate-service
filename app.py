import os
import uuid
import asyncio
from fastapi import FastAPI, HTTPException
from pydantic import BaseModel
from typing import Optional, Dict
import httpx

app = FastAPI(title="Video Rotate Service (Async)")

UPLOAD_DIR = "/tmp/videos"
os.makedirs(UPLOAD_DIR, exist_ok=True)

# In-memory job tracker
jobs: Dict[str, dict] = {}


class RotateRequest(BaseModel):
    url: str
    rotation: str = "portrait"
    output_filename: Optional[str] = None
    input_username: Optional[str] = None
    input_password: Optional[str] = None
    webdav_url: Optional[str] = None
    webdav_username: Optional[str] = None
    webdav_password: Optional[str] = None


def get_transpose_value(rotation: str) -> str:
    mapping = {
        "portrait": "transpose=1",
        "90": "transpose=1",
        "270": "transpose=2",
        "180": "transpose=1,transpose=1",
    }
    return mapping.get(rotation, "transpose=1")


async def process_video(job_id: str, request: RotateRequest):
    """Background task: download, rotate, upload."""
    input_path = os.path.join(UPLOAD_DIR, f"{job_id}_input.mp4")
    output_filename = request.output_filename or f"{job_id}_output.mp4"
    output_path = os.path.join(UPLOAD_DIR, output_filename)

    try:
        # Step 1: Download
        jobs[job_id]["step"] = "downloading"
        auth = None
        if request.input_username and request.input_password:
            auth = (request.input_username, request.input_password)

        async with httpx.AsyncClient(follow_redirects=True, timeout=600, auth=auth) as client:
            async with client.stream("GET", request.url) as response:
                response.raise_for_status()
                with open(input_path, "wb") as f:
                    async for chunk in response.aiter_bytes(chunk_size=1024 * 1024):
                        f.write(chunk)

        # Step 2: Rotate with ffmpeg (ultrafast preset for speed)
        jobs[job_id]["step"] = "rotating"
        vf_filter = get_transpose_value(request.rotation)
        cmd = [
            "ffmpeg", "-y",
            "-i", input_path,
            "-vf", vf_filter,
            "-c:a", "copy",
            "-preset", "ultrafast",
            "-movflags", "+faststart",
            output_path
        ]

        process = await asyncio.create_subprocess_exec(
            *cmd,
            stdout=asyncio.subprocess.PIPE,
            stderr=asyncio.subprocess.PIPE
        )
        stdout, stderr = await process.communicate()

        if process.returncode != 0:
            jobs[job_id]["status"] = "error"
            jobs[job_id]["error"] = f"FFmpeg failed: {stderr.decode()[-300:]}"
            return

        # Step 3: Upload to WebDAV if configured
        if request.webdav_url and request.webdav_username and request.webdav_password:
            jobs[job_id]["step"] = "uploading"
            webdav_dest = request.webdav_url.rstrip("/") + "/" + output_filename

            async with httpx.AsyncClient(timeout=600) as client:
                with open(output_path, "rb") as f:
                    file_data = f.read()
                resp = await client.put(
                    webdav_dest,
                    content=file_data,
                    auth=(request.webdav_username, request.webdav_password),
                    headers={"Content-Type": "video/mp4"}
                )
                if resp.status_code not in (200, 201, 204):
                    jobs[job_id]["status"] = "error"
                    jobs[job_id]["error"] = f"WebDAV upload failed: {resp.status_code}"
                    return

            jobs[job_id]["status"] = "done"
            jobs[job_id]["step"] = "complete"
            jobs[job_id]["output"] = webdav_dest
        else:
            jobs[job_id]["status"] = "done"
            jobs[job_id]["step"] = "complete"
            jobs[job_id]["output"] = f"/download/{output_filename}"

    except Exception as e:
        jobs[job_id]["status"] = "error"
        jobs[job_id]["error"] = str(e)
    finally:
        if os.path.exists(input_path):
            os.remove(input_path)
        if request.webdav_url and os.path.exists(output_path):
            os.remove(output_path)


@app.post("/rotate")
async def rotate_video(request: RotateRequest):
    job_id = str(uuid.uuid4())[:8]
    jobs[job_id] = {
        "status": "processing",
        "step": "queued",
        "error": None,
        "output": None
    }
    asyncio.create_task(process_video(job_id, request))
    return {"job_id": job_id, "status": "accepted"}


@app.get("/status/{job_id}")
async def get_status(job_id: str):
    if job_id not in jobs:
        raise HTTPException(status_code=404, detail="Job not found")
    return {"job_id": job_id, **jobs[job_id]}


@app.get("/download/{filename}")
async def download_file(filename: str):
    from fastapi.responses import FileResponse
    filepath = os.path.join(UPLOAD_DIR, filename)
    if not os.path.exists(filepath):
        raise HTTPException(status_code=404, detail="File not found")
    return FileResponse(filepath, media_type="video/mp4", filename=filename)


@app.get("/health")
async def health():
    return {"status": "ok"}


@app.on_event("startup")
async def cleanup_old_files():
    for f in os.listdir(UPLOAD_DIR):
        try:
            os.remove(os.path.join(UPLOAD_DIR, f))
        except:
            pass
