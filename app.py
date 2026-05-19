import os
import uuid
import asyncio
from fastapi import FastAPI, HTTPException
from fastapi.responses import FileResponse
from pydantic import BaseModel
from typing import Optional
import httpx

app = FastAPI(title="Video Rotate Service")

UPLOAD_DIR = "/tmp/videos"
os.makedirs(UPLOAD_DIR, exist_ok=True)


class RotateRequest(BaseModel):
    url: str
    rotation: str = "portrait"
    output_filename: Optional[str] = None
    # Auth for downloading the input file (e.g. Koofr WebDAV)
    input_username: Optional[str] = None
    input_password: Optional[str] = None
    # WebDAV upload (optional)
    webdav_url: Optional[str] = None
    webdav_username: Optional[str] = None
    webdav_password: Optional[str] = None


class RotateResponse(BaseModel):
    download_url: Optional[str] = None
    webdav_path: Optional[str] = None
    filename: str
    status: str


def get_transpose_value(rotation: str) -> str:
    mapping = {
        "portrait": "transpose=1",
        "90": "transpose=1",
        "270": "transpose=2",
        "180": "transpose=1,transpose=1",
    }
    return mapping.get(rotation, "transpose=1")


@app.post("/rotate", response_model=RotateResponse)
async def rotate_video(request: RotateRequest):
    job_id = str(uuid.uuid4())[:8]
    input_path = os.path.join(UPLOAD_DIR, f"{job_id}_input.mp4")
    output_filename = request.output_filename or f"{job_id}_output.mp4"
    output_path = os.path.join(UPLOAD_DIR, output_filename)

    # Step 1: Download the video (with optional auth)
    auth = None
    if request.input_username and request.input_password:
        auth = (request.input_username, request.input_password)

    try:
        async with httpx.AsyncClient(follow_redirects=True, timeout=600, auth=auth) as client:
            async with client.stream("GET", request.url) as response:
                response.raise_for_status()
                with open(input_path, "wb") as f:
                    async for chunk in response.aiter_bytes(chunk_size=1024 * 1024):
                        f.write(chunk)
    except Exception as e:
        raise HTTPException(status_code=400, detail=f"Download failed: {str(e)}")

    # Step 2: Rotate with ffmpeg
    vf_filter = get_transpose_value(request.rotation)
    cmd = [
        "ffmpeg", "-y",
        "-i", input_path,
        "-vf", vf_filter,
        "-c:a", "copy",
        "-movflags", "+faststart",
        output_path
    ]

    try:
        process = await asyncio.create_subprocess_exec(
            *cmd,
            stdout=asyncio.subprocess.PIPE,
            stderr=asyncio.subprocess.PIPE
        )
        stdout, stderr = await process.communicate()

        if process.returncode != 0:
            raise HTTPException(
                status_code=500,
                detail=f"FFmpeg failed: {stderr.decode()[-500:]}"
            )
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"FFmpeg error: {str(e)}")
    finally:
        if os.path.exists(input_path):
            os.remove(input_path)

    # Step 3: Upload to WebDAV if credentials provided
    if request.webdav_url and request.webdav_username and request.webdav_password:
        webdav_dest = request.webdav_url.rstrip("/") + "/" + output_filename
        try:
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
                    raise HTTPException(
                        status_code=500,
                        detail=f"WebDAV upload failed: {resp.status_code} {resp.text[:200]}"
                    )
        except httpx.HTTPError as e:
            raise HTTPException(status_code=500, detail=f"WebDAV upload error: {str(e)}")
        finally:
            if os.path.exists(output_path):
                os.remove(output_path)

        return RotateResponse(
            webdav_path=webdav_dest,
            filename=output_filename,
            status="done"
        )

    # If no WebDAV, keep file for download
    return RotateResponse(
        download_url=f"/download/{output_filename}",
        filename=output_filename,
        status="done"
    )


@app.get("/download/{filename}")
async def download_file(filename: str):
    filepath = os.path.join(UPLOAD_DIR, filename)
    if not os.path.exists(filepath):
        raise HTTPException(status_code=404, detail="File not found or expired")
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
