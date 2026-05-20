# Step 3: Upload to WebDAV if configured
        if request.webdav_url and request.webdav_username and request.webdav_password:
            jobs[job_id]["step"] = "uploading"
            webdav_dest = request.webdav_url.rstrip("/") + "/" + output_filename

            try:
                async with httpx.AsyncClient(timeout=600) as client:
                    file_size = os.path.getsize(output_path)
                    jobs[job_id]["error"] = f"Uploading {file_size} bytes to {webdav_dest}"
                    
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
                        jobs[job_id]["error"] = f"WebDAV upload failed: HTTP {resp.status_code} - {resp.text[:500]}"
                        return

            except Exception as e:
                jobs[job_id]["status"] = "error"
                jobs[job_id]["error"] = f"Upload exception: {type(e).__name__}: {str(e)}"
                return

            jobs[job_id]["status"] = "done"
            jobs[job_id]["step"] = "complete"
            jobs[job_id]["output"] = webdav_dest
