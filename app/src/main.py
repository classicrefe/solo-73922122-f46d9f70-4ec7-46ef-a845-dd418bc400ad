import logging
import os

from fastapi import FastAPI, HTTPException
from fastapi.responses import FileResponse, HTMLResponse
from pydantic import BaseModel

from .db import DB
from .recorder import Manager

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(name)s %(message)s")

DATA_DIR = os.environ.get("DATA_DIR", "./data")
os.makedirs(DATA_DIR, exist_ok=True)

db = DB(os.path.join(DATA_DIR, "recorder.db"))
mgr = Manager(db, DATA_DIR)
app = FastAPI(title="HLS 直播连续录音采集")


@app.on_event("startup")
async def _startup():
    mgr.recover()  # resume any recording/finalizing jobs after a restart


@app.on_event("shutdown")
async def _shutdown():
    await mgr.shutdown()


class StartBody(BaseModel):
    url: str


def _zip_available(job: dict) -> bool:
    return (job["state"] == "done" and bool(job["zip_path"])
            and os.path.isfile(job["zip_path"]))


def job_json(job_id: int) -> dict:
    job = db.get_job(job_id)
    if job is None:
        raise HTTPException(404, "job not found")
    counts = db.counts(job_id)
    out = {
        "id": job["id"],
        "url": job["url"],
        "state": job["state"],
        "created_at": job["created_at"],
        "first_seq": job["first_seq"],
        "last_seq": job["last_seq"],
        "stop_seq": job["stop_seq"],
        "expected": counts["total"],
        "saved": counts["done"],
        "missing": db.missing_seqs(job_id),
        "error": job["error"],
        "download_url": (f"/api/jobs/{job_id}/download"
                         if _zip_available(job) else None),
    }
    return out


@app.post("/api/jobs")
async def start_job(body: StartBody):
    url = body.url.strip()
    if not url.startswith(("http://", "https://")):
        raise HTTPException(400, "只支持 http(s) 清单地址")
    job_id = mgr.create_job(url)
    return job_json(job_id)


@app.post("/api/jobs/{job_id}/stop")
async def stop_job(job_id: int):
    job = db.get_job(job_id)
    if job is None:
        raise HTTPException(404, "job not found")
    if job["state"] != "recording":
        raise HTTPException(409, f"当前状态 {job['state']} 不可停止")
    mgr.accept_stop(job_id)
    return job_json(job_id)


@app.get("/api/jobs")
async def list_jobs():
    return [job_json(j["id"]) for j in db.list_jobs()]


@app.get("/api/jobs/{job_id}")
async def get_job(job_id: int):
    return job_json(job_id)


@app.get("/api/jobs/{job_id}/download")
async def download(job_id: int):
    job = db.get_job(job_id)
    if job is None:
        raise HTTPException(404, "job not found")
    if not _zip_available(job):
        raise HTTPException(409, "停止边界内全部应收片段保存后才可下载")
    return FileResponse(job["zip_path"], media_type="application/zip",
                        filename=f"recording_{job_id}.zip")


@app.get("/")
async def index():
    page = os.path.join(os.path.dirname(__file__), "static", "index.html")
    with open(page, encoding="utf-8") as fh:
        return HTMLResponse(fh.read())


# Test-only hooks for crash/recovery testing. Enabled exclusively via
# RECORDER_TEST_HOOKS=1; never set in the Compose deployment.
if os.environ.get("RECORDER_TEST_HOOKS") == "1":
    from .recorder import TEST_HOOKS

    @app.post("/api/_test/crash")
    async def _test_crash():
        os._exit(137)

    @app.post("/api/_test/arm_after_write/{job_id}/{seq}")
    async def _test_arm_after_write(job_id: int, seq: int):
        TEST_HOOKS["crash_after_write"] = (job_id, seq)
        return {"armed": [job_id, seq]}
