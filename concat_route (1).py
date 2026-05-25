"""
/concat endpoint — merges 2+ videos into one.

Drop this file next to your existing FastAPI app (e.g. alongside main.py).
Then in main.py add:

    from concat_route import router as concat_router
    app.include_router(concat_router)

Adjust OUTPUT_DIR / TMP_DIR below if your service uses different paths.
Requires ffmpeg + ffprobe in the container (already there in your image).
"""

import asyncio
import json
import os
import uuid
from pathlib import Path
from typing import List, Literal, Optional

import httpx
from fastapi import APIRouter, HTTPException
from pydantic import BaseModel, HttpUrl

router = APIRouter()

# ───────── Adjust these to match your existing service ─────────
OUTPUT_DIR = Path(os.getenv("OUTPUT_DIR", "/app/output"))
TMP_DIR = Path(os.getenv("TMP_DIR", "/app/tmp"))
OUTPUT_DIR.mkdir(parents=True, exist_ok=True)
TMP_DIR.mkdir(parents=True, exist_ok=True)


class ConcatRequest(BaseModel):
    urls: List[HttpUrl]
    # "auto" probes inputs and picks the right strategy.
    # "fast"     = stream copy (only works if all inputs share codec/res/fps/audio)
    # "reencode" = forces re-encode (slower, always works)
    mode: Literal["auto", "fast", "reencode"] = "auto"
    output_format: str = "mp4"

    # Optional WebDAV push (same pattern as your /rotate endpoint)
    webdav_url: Optional[str] = None
    webdav_username: Optional[str] = None
    webdav_password: Optional[str] = None
    webdav_remote_path: Optional[str] = None


# ───────── Helpers ─────────

async def _download(url: str, dest: Path) -> None:
    async with httpx.AsyncClient(timeout=None, follow_redirects=True) as client:
        async with client.stream("GET", url) as r:
            r.raise_for_status()
            with dest.open("wb") as f:
                async for chunk in r.aiter_bytes(1024 * 1024):
                    f.write(chunk)


async def _run(cmd: List[str]) -> tuple[int, str, str]:
    proc = await asyncio.create_subprocess_exec(
        *cmd,
        stdout=asyncio.subprocess.PIPE,
        stderr=asyncio.subprocess.PIPE,
    )
    out, err = await proc.communicate()
    return proc.returncode, out.decode(errors="replace"), err.decode(errors="replace")


async def _probe(path: Path) -> dict:
    code, out, err = await _run([
        "ffprobe", "-v", "error",
        "-print_format", "json",
        "-show_streams",
        str(path),
    ])
    if code != 0:
        raise HTTPException(500, f"ffprobe failed on {path.name}: {err}")
    return json.loads(out)


def _signature(probe: dict) -> tuple:
    """A tuple representing the 'shape' of a file — videos with the same
    signature can be concat-demuxed without re-encoding."""
    v = next((s for s in probe["streams"] if s["codec_type"] == "video"), {})
    a = next((s for s in probe["streams"] if s["codec_type"] == "audio"), {})
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


async def _concat_fast(inputs: List[Path], output: Path) -> None:
    """Stream-copy via concat demuxer. Fast, lossless, no re-encoding."""
    list_file = TMP_DIR / f"{uuid.uuid4().hex}_list.txt"
    list_file.write_text("\n".join(f"file '{p.absolute()}'" for p in inputs))
    try:
        code, _, err = await _run([
            "ffmpeg", "-y",
            "-f", "concat", "-safe", "0",
            "-i", str(list_file),
            "-c", "copy",
            "-movflags", "+faststart",
            str(output),
        ])
        if code != 0:
            raise HTTPException(500, f"ffmpeg concat (fast) failed: {err[-2000:]}")
    finally:
        list_file.unlink(missing_ok=True)


async def _concat_reencode(inputs: List[Path], output: Path) -> None:
    """Concat filter with normalization. Handles mismatched inputs."""
    args = ["ffmpeg", "-y"]
    for p in inputs:
        args.extend(["-i", str(p)])
    n = len(inputs)

    # Normalize every input to 1920x1080 / 30 fps / stereo 48kHz before concat.
    # Change these targets if your sources are vertical/different.
    filter_parts = []
    for i in range(n):
        filter_parts.append(
            f"[{i}:v]scale=1920:1080:force_original_aspect_ratio=decrease,"
            f"pad=1920:1080:(ow-iw)/2:(oh-ih)/2:black,setsar=1,fps=30[v{i}];"
            f"[{i}:a]aresample=48000,aformat=channel_layouts=stereo[a{i}];"
        )
    concat_in = "".join(f"[v{i}][a{i}]" for i in range(n))
    filter_complex = "".join(filter_parts) + f"{concat_in}concat=n={n}:v=1:a=1[outv][outa]"

    args.extend([
        "-filter_complex", filter_complex,
        "-map", "[outv]", "-map", "[outa]",
        "-c:v", "libx264", "-preset", "veryfast", "-crf", "20",
        "-c:a", "aac", "-b:a", "192k",
        "-movflags", "+faststart",
        str(output),
    ])
    code, _, err = await _run(args)
    if code != 0:
        raise HTTPException(500, f"ffmpeg concat (reencode) failed: {err[-2000:]}")


async def _upload_webdav(local: Path, req: ConcatRequest) -> Optional[str]:
    if not (req.webdav_url and req.webdav_username
            and req.webdav_password and req.webdav_remote_path):
        return None
    target = f"{req.webdav_url.rstrip('/')}/{req.webdav_remote_path.lstrip('/')}"
    async with httpx.AsyncClient(
        auth=(req.webdav_username, req.webdav_password),
        timeout=None,
    ) as client:
        with local.open("rb") as f:
            r = await client.put(target, content=f)
            r.raise_for_status()
    return target


# ───────── Endpoint ─────────

@router.post("/concat")
async def concat(req: ConcatRequest):
    if len(req.urls) < 2:
        raise HTTPException(400, "Provide at least 2 URLs to concatenate.")

    job_id = uuid.uuid4().hex[:8]
    work_dir = TMP_DIR / job_id
    work_dir.mkdir(parents=True, exist_ok=True)

    local_paths: List[Path] = []
    try:
        # 1. Download inputs in parallel
        local_paths = [
            work_dir / f"input_{i:03d}{Path(str(u)).suffix or '.mp4'}"
            for i, u in enumerate(req.urls)
        ]
        await asyncio.gather(*[_download(str(u), p) for u, p in zip(req.urls, local_paths)])

        # 2. Pick strategy
        mode_used = req.mode
        if req.mode == "auto":
            probes = await asyncio.gather(*[_probe(p) for p in local_paths])
            sigs = [_signature(p) for p in probes]
            mode_used = "fast" if all(s == sigs[0] for s in sigs) else "reencode"

        # 3. Run
        output_name = f"{job_id}_concat.{req.output_format}"
        output_path = OUTPUT_DIR / output_name
        if mode_used == "fast":
            try:
                await _concat_fast(local_paths, output_path)
            except HTTPException:
                # Probe said they matched, but ffmpeg disagreed — fallback.
                await _concat_reencode(local_paths, output_path)
                mode_used = "reencode"
        else:
            await _concat_reencode(local_paths, output_path)

        # 4. Optional WebDAV push
        webdav_url = await _upload_webdav(output_path, req)

        return {
            "status": "done",
            "filename": output_name,
            "download_url": f"/download/{output_name}",
            "mode_used": mode_used,
            "webdav_url": webdav_url,
        }
    finally:
        for p in local_paths:
            p.unlink(missing_ok=True)
        try:
            work_dir.rmdir()
        except OSError:
            pass
