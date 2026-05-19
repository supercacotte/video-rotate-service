import os
import uuid
import subprocess
import asyncio
from fastapi import FastAPI, HTTPException
from fastapi.responses import FileResponse
from pydantic import BaseModel
import httpx

app = FastAPI(title="Video Rotate Service")

UPLOAD_DIR = "/tmp/videos"
os.makedirs(UPLOAD_DIR, exist_ok=True)

class RotateRequest(BaseModel):
    url: str
    rotation: str = "portrait"  # "portrait" = 90° clockwise, "180", "270"

class RotateResponse(BaseModel):
    download_url: str
    filename: str
    status: str

def get_transpose_value(rotation: str) -> str:
    """Map rotation string to ffmpeg transpose value."""
    mapping = {
        "portrait": "transpose=1",        # 90° clockwise
        "90": "transpose=1",              # 90° clockwise
        "270": "transpose=2",             # 90° counter-clockwise
        "180": "transpose=1,transpose=1", # 180°
    }
    return mapping.get(rotation, "transpose=1")

@app.post("/rotate", response_model=RotateResponse)
async def rotate_video(request: RotateRequest):
    job_id = str(uuid.uuid4())[:8]
    input_path = os.path.join(UPLOAD_DIR, f"{job_id}_input.mp4")
    output_path = os.path.join(UPLOAD_DIR, f"{job_id}_output.mp4")

    # Step 1: Download the video from URL
    try:
        async with httpx.AsyncClient(follow_redirects=True, timeout=300) as client:
            response = await client.get(request.url)
            response.raise_for_status()
            with open(input_path, "wb") as f:
                f.write(response.content)
    except Exception as e:
        raise HTTPException(status_code=400, detail=f"Failed to download video: {str(e)}")

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
        # Clean up input file
        if os.path.exists(input_path):
            os.remove(input_path)

    filename = f"{job_id}_output.mp4"
    return RotateResponse(
        download_url=f"/download/{filename}",
        filename=filename,
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

# Cleanup old files on startup
@app.on_event("startup")
async def cleanup_old_files():
    for f in os.listdir(UPLOAD_DIR):
        try:
            os.remove(os.path.join(UPLOAD_DIR, f))
        except:
            pass
