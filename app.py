import os
import json
import uuid
import asyncio
from fastapi import FastAPI, HTTPException, Query
from fastapi.responses import StreamingResponse
from pydantic import BaseModel
from typing import Optional, Dict, List
import httpx

app = FastAPI(title="Video Rotate & Serve")

UPLOAD_DIR = "/tmp/videos"
os.makedirs(UPLOAD_DIR, exist_ok=True)

# ===== CONFIG =====
# Set these as environment variables in Coolify, or hardcode for now
KOOFR_USERNAME = os.getenv("KOOFR_USERNAME", "")
KOOFR_PASSWORD = os.getenv("KOOFR_PASSWORD", "")
KOOFR_BASE_URL = "https://app.koofr.net/dav/Google%20Drive"

# In-memory job tracker
jobs: Dict[str, dict] = {}


# ===== SERVE ENDPOINT (proxy Koofr -> public URL) =====

@app.get("/serve/{file_path:path}")
async def serve_file(file_path: str):
    """
    Proxy a file from Koofr WebDAV as a public URL.
    Usage: /serve/Episodes%20%C3%A0%20publier/video.mp4
    This becomes: https://app.koofr.net/dav/Google%20Drive/Episodes%20%C3%A0%20publier/video.mp4
    """
    if not KOOFR_USERNAME or not KOOFR_PASSWORD:
        raise HTTPException(status_code=500, detail="Koofr credentials not configured")

    koofr_url = f"{KOOFR_BASE_URL}/{file_path}"

    async def stream_from_koofr():
        async with httpx.AsyncClient(
            follow_redirects=True,
            timeout=600,
            auth=(KOOFR_USERNAME, KOOFR_PASSWORD)
        ) as client:
            async with client.stream("GET", koofr_url) as response:
                if response.status_code != 200:
                    raise HTTPException(
                        status_code=response.status_code,
                        detail=f"Koofr returned {response.status_code}"
                    )
                async for chunk in response.aiter_bytes(chunk_size=1024 * 1024):
                    yield chunk

    return StreamingResponse(
        stream_from_koofr(),
        media_type="video/mp4",
        headers={
            "Content-Disposition": f"inline; filename={file_path.split('/')[-1]}",
            "Accept-Ranges": "bytes"
        }
    )


# ===== ROTATE ENDPOINTS =====

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

        # Step 2: Rotate with ffmpeg
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


# ===== CONCAT ENDPOINTS =====

class ConcatRequest(BaseModel):
    urls: List[str]
    # "auto"     = probe inputs, pick fast if compatible else reencode (default)
    # "fast"     = stream copy concat (only works if codec/res/fps/audio match)
    # "reencode" = force re-encode (slower, always works)
    mode: str = "auto"
    # Target spec for reencode mode (change to 1080:1920 for vertical sources)
    target_width: int = 1920
    target_height: int = 1080
    target_fps: int = 30
    output_filename: Optional[str] = None
    input_username: Optional[str] = None
    input_password: Optional[str] = None
    webdav_url: Optional[str] = None
    webdav_username: Optional[str] = None
    webdav_password: Optional[str] = None


async def _probe(path: str) -> dict:
    process = await asyncio.create_subprocess_exec(
        "ffprobe", "-v", "error",
        "-print_format", "json",
        "-show_streams",
        path,
        stdout=asyncio.subprocess.PIPE,
        stderr=asyncio.subprocess.PIPE,
    )
    stdout, stderr = await process.communicate()
    if process.returncode != 0:
        raise RuntimeError(f"ffprobe failed: {stderr.decode()[-300:]}")
    return json.loads(stdout.decode())


def _signature(probe: dict) -> tuple:
    """Tuple representing the file's shape — matching signatures can be concat-demuxed."""
    v = next((s for s in probe.get("streams", []) if s.get("codec_type") == "video"), {})
    a = next((s for s in probe.get("streams", []) if s.get("codec_type") == "audio"), {})
    return (
        v.get("codec_name"),
        v.get("width"),
        v.get("height"),
        v.get("r_frame_rate"),
        v.get("pix_fmt"),
        a.get("codec_name"),
        a.get("sample_rate"),
        a.get("channels"),
    )


async def _run_ffmpeg(cmd: List[str]) -> tuple[int, str]:
    process = await asyncio.create_subprocess_exec(
        *cmd,
        stdout=asyncio.subprocess.PIPE,
        stderr=asyncio.subprocess.PIPE,
    )
    _, stderr = await process.communicate()
    return process.returncode, stderr.decode()


async def process_concat(job_id: str, request: ConcatRequest):
    if len(request.urls) < 2:
        jobs[job_id]["status"] = "error"
        jobs[job_id]["error"] = "Provide at least 2 URLs to concatenate."
        return

    input_paths = [
        os.path.join(UPLOAD_DIR, f"{job_id}_input_{i:03d}.mp4")
        for i in range(len(request.urls))
    ]
    output_filename = request.output_filename or f"{job_id}_concat.mp4"
    output_path = os.path.join(UPLOAD_DIR, output_filename)
    list_file = os.path.join(UPLOAD_DIR, f"{job_id}_list.txt")

    try:
        # Step 1: Download all inputs in parallel
        jobs[job_id]["step"] = "downloading"
        auth = None
        if request.input_username and request.input_password:
            auth = (request.input_username, request.input_password)

        async def download_one(url: str, dest: str):
            async with httpx.AsyncClient(follow_redirects=True, timeout=600, auth=auth) as client:
                async with client.stream("GET", url) as response:
                    response.raise_for_status()
                    with open(dest, "wb") as f:
                        async for chunk in response.aiter_bytes(chunk_size=1024 * 1024):
                            f.write(chunk)

        await asyncio.gather(*[
            download_one(url, path) for url, path in zip(request.urls, input_paths)
        ])

        # Step 2: Decide strategy
        jobs[job_id]["step"] = "probing"
        mode_used = request.mode
        if request.mode == "auto":
            probes = await asyncio.gather(*[_probe(p) for p in input_paths])
            sigs = [_signature(p) for p in probes]
            mode_used = "fast" if all(s == sigs[0] for s in sigs) else "reencode"

        # Step 3: Run concat
        jobs[job_id]["step"] = f"concatenating ({mode_used})"

        if mode_used == "fast":
            # Write concat demuxer list file
            with open(list_file, "w") as f:
                for p in input_paths:
                    f.write(f"file '{p}'\n")

            cmd = [
                "ffmpeg", "-y",
                "-f", "concat", "-safe", "0",
                "-i", list_file,
                "-c", "copy",
                "-movflags", "+faststart",
                output_path
            ]
            returncode, stderr = await _run_ffmpeg(cmd)

            # If fast path fails despite matching probe, fall back to reencode
            if returncode != 0:
                mode_used = "reencode"
                jobs[job_id]["step"] = "concatenating (reencode-fallback)"

        if mode_used == "reencode":
            n = len(input_paths)
            w, h, fps = request.target_width, request.target_height, request.target_fps
            filter_parts = []
            for i in range(n):
                filter_parts.append(
                    f"[{i}:v]scale={w}:{h}:force_original_aspect_ratio=decrease,"
                    f"pad={w}:{h}:(ow-iw)/2:(oh-ih)/2:black,setsar=1,fps={fps}[v{i}];"
                    f"[{i}:a]aresample=48000,aformat=channel_layouts=stereo[a{i}];"
                )
            concat_in = "".join(f"[v{i}][a{i}]" for i in range(n))
            filter_complex = "".join(filter_parts) + f"{concat_in}concat=n={n}:v=1:a=1[outv][outa]"

            cmd = ["ffmpeg", "-y"]
            for p in input_paths:
                cmd.extend(["-i", p])
            cmd.extend([
                "-filter_complex", filter_complex,
                "-map", "[outv]", "-map", "[outa]",
                "-c:v", "libx264", "-preset", "veryfast", "-crf", "20",
                "-c:a", "aac", "-b:a", "192k",
                "-movflags", "+faststart",
                output_path
            ])
            returncode, stderr = await _run_ffmpeg(cmd)
            if returncode != 0:
                jobs[job_id]["status"] = "error"
                jobs[job_id]["error"] = f"FFmpeg failed: {stderr[-300:]}"
                return

        jobs[job_id]["mode_used"] = mode_used

        # Step 4: Upload to WebDAV if configured
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
        for p in input_paths:
            if os.path.exists(p):
                os.remove(p)
        if os.path.exists(list_file):
            os.remove(list_file)
        if request.webdav_url and os.path.exists(output_path):
            os.remove(output_path)


@app.post("/concat")
async def concat_videos(request: ConcatRequest):
    job_id = str(uuid.uuid4())[:8]
    jobs[job_id] = {
        "status": "processing",
        "step": "queued",
        "error": None,
        "output": None,
        "mode_used": None,
    }
    asyncio.create_task(process_concat(job_id, request))
    return {"job_id": job_id, "status": "accepted"}


# ===== SHARED ENDPOINTS =====

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
