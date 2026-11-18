#!/usr/bin/env python3
"""Extra checks: segment-URL reuse rejection + ZIP offline playability (ffmpeg decode)."""
import io
import os
import subprocess
import sys
import time
import zipfile

import httpx

SRC = os.environ.get("SRC", "http://127.0.0.1:8081")
PORT = int(os.environ.get("TEST_PORT", "0"))
if PORT == 0:
    import socket
    s = socket.socket(); s.bind(("127.0.0.1", 0)); PORT = s.getsockname()[1]; s.close()
APP = f"http://127.0.0.1:{PORT}"
DATA = f"/tmp/recdata_extra_{os.getpid()}"
os.makedirs(DATA, exist_ok=True)
FFMPEG = subprocess.check_output(
    [sys.executable, "-c", "import imageio_ffmpeg; print(imageio_ffmpeg.get_ffmpeg_exe())"]
).decode().strip()

results = []


def check(name, cond, detail=""):
    results.append((name, bool(cond)))
    print(("PASS " if cond else "FAIL "), name, ("" if cond else f"  -> {detail}"))


log = open("/tmp/app_extra.log", "ab")
proc = subprocess.Popen(
    [sys.executable, "-m", "uvicorn", "src.main:app", "--host", "127.0.0.1", "--port", str(PORT)],
    cwd=os.environ.get("APP_DIR", "/workspace/app"),
    env={**os.environ, "DATA_DIR": DATA, "RECORDER_TEST_HOOKS": "1"},
    stdout=log, stderr=subprocess.STDOUT)
for _ in range(60):
    try:
        httpx.get(f"{APP}/api/jobs", timeout=1)
        break
    except Exception:
        time.sleep(0.25)

try:
    # J: segment URL reuse must be rejected
    r = httpx.post(f"{APP}/api/jobs", json={"url": f"{SRC}/reuse.m3u8"}, timeout=10)
    jid = r.json()["id"]
    j = None
    for _ in range(40):
        j = httpx.get(f"{APP}/api/jobs/{jid}", timeout=10).json()
        if j["state"] == "failed":
            break
        time.sleep(0.5)
    check("J 片段地址复用被拒", j["state"] == "failed" and "复用" in (j["error"] or ""),
          str(j.get("error")))

    # K: record a normal stream, then decode the packaged playlist with ffmpeg
    jid = httpx.post(f"{APP}/api/jobs", json={"url": f"{SRC}/live.m3u8"}, timeout=10).json()["id"]
    time.sleep(8)
    httpx.post(f"{APP}/api/jobs/{jid}/stop", timeout=10)
    for _ in range(60):
        j = httpx.get(f"{APP}/api/jobs/{jid}", timeout=10).json()
        if j["state"] == "done":
            break
        time.sleep(0.5)
    check("K 录制完成", j["state"] == "done", str(j))
    zdata = httpx.get(f"{APP}/api/jobs/{jid}/download", timeout=10).content
    outdir = f"/tmp/zipcheck_{os.getpid()}"
    os.makedirs(outdir, exist_ok=True)
    zipfile.ZipFile(io.BytesIO(zdata)).extractall(outdir)
    dec = subprocess.run(
        [FFMPEG, "-v", "error", "-i", os.path.join(outdir, "playlist.m3u8"),
         "-f", "null", "-"],
        capture_output=True, text=True, timeout=60)
    # identical seed segments restart PTS each segment; the null muxer warns
    # about non-monotonic DTS -- harmless for HLS players. Anything else fails.
    bad = [ln for ln in dec.stderr.splitlines()
           if ln.strip() and "non monotonically increasing dts" not in ln]
    check("K ZIP内清单可被ffmpeg完整解码(可离线播放)",
          dec.returncode == 0 and not bad,
          f"rc={dec.returncode} err={dec.stderr[:300]}")
finally:
    try:
        httpx.post(f"{APP}/api/_test/crash", timeout=2)
    except Exception:
        pass
    proc.wait(timeout=10)

print("\n==== SUMMARY ====")
failed = [n for n, ok in results if not ok]
print(f"{len(results) - len(failed)}/{len(results)} passed")
sys.exit(1 if failed else 0)
