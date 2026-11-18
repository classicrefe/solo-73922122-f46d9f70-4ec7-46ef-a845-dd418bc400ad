#!/usr/bin/env python3
"""End-to-end tests for the HLS live recorder against the local fake source."""
import io
import os
import signal
import sqlite3
import subprocess
import sys
import time
import zipfile

import httpx

PORT = int(os.environ.get("TEST_PORT", "0"))
if PORT == 0:
    import socket
    s = socket.socket(); s.bind(("127.0.0.1", 0)); PORT = s.getsockname()[1]; s.close()
APP = f"http://127.0.0.1:{PORT}"
SRC = os.environ.get("SRC", "http://127.0.0.1:8080")
DATA = f"/tmp/recdata_{os.getpid()}"
os.makedirs(DATA, exist_ok=True)
SEED = open(os.environ.get("SEED_TS", "/tmp/livesrc/seed.ts"), "rb").read()

results = []


def check(name, cond, detail=""):
    results.append((name, bool(cond)))
    print(("PASS " if cond else "FAIL "), name, ("" if cond else f"  -> {detail}"))


def api(method, path, **kw):
    return getattr(httpx, method)(f"{APP}{path}", timeout=10, **kw)


def get_job(jid):
    return api("get", f"/api/jobs/{jid}").json()


def wait_job(jid, states, timeout=60):
    t0 = time.time()
    j = get_job(jid)
    while time.time() - t0 < timeout:
        j = get_job(jid)
        if j["state"] in states:
            return j
        time.sleep(0.4)
    return j


def wait_saved(jid, n, timeout=60):
    t0 = time.time()
    while time.time() - t0 < timeout:
        j = get_job(jid)
        if j["saved"] >= n:
            return j
        time.sleep(0.4)
    return get_job(jid)


def start_job(url):
    r = api("post", "/api/jobs", json={"url": url})
    assert r.status_code == 200, r.text
    return r.json()["id"]


def stop_job(jid):
    return api("post", f"/api/jobs/{jid}/stop").json()


def fetch_zip(jid):
    r = api("get", f"/api/jobs/{jid}/download")
    assert r.status_code == 200, f"download failed: {r.status_code}"
    return zipfile.ZipFile(io.BytesIO(r.content))


app_proc = None


def port_in_use():
    try:
        api("get", "/api/jobs")
        return True
    except Exception:
        return False


def start_app():
    global app_proc
    assert not port_in_use(), f"port {PORT} still in use before start"
    log = open(os.environ.get("APP_LOG", "/tmp/app_test.log"), "ab")
    app_proc = subprocess.Popen(
        [sys.executable, "-m", "uvicorn", "src.main:app",
         "--host", "127.0.0.1", "--port", str(PORT)],
        cwd=os.environ.get("APP_DIR", "/workspace/app"),
        env={**os.environ, "DATA_DIR": DATA, "RECORDER_TEST_HOOKS": "1"},
        stdout=log, stderr=subprocess.STDOUT)
    for _ in range(60):
        if app_proc.poll() is not None:
            raise RuntimeError("app process exited during startup")
        try:
            api("get", "/api/jobs")
            return
        except Exception:
            time.sleep(0.25)
    raise RuntimeError("app did not start")


def crash_app():
    """Abruptly kill the app mid-work, like a power cut (no cleanup)."""
    global app_proc
    try:
        api("post", "/api/_test/crash")
    except Exception:
        pass
    if app_proc:
        app_proc.wait(timeout=10)
    app_proc = None
    for _ in range(40):
        if not port_in_use():
            return
        time.sleep(0.25)
    raise RuntimeError(f"port {PORT} still in use after crash")


def kill_app():
    crash_app()


def now_seq():
    return httpx.get(f"{SRC}/health", timeout=5).json()["now_seq"]


def verify_zip(jid, first, stop):
    zf = fetch_zip(jid)
    names = zf.namelist()
    pl = zf.read("playlist.m3u8").decode()
    segs = sorted(n for n in names if n.endswith(".ts"))
    expect = stop - first + 1
    ok = ("playlist.m3u8" in names and len(segs) == expect
          and "http" not in pl.lower() and "#EXT-X-ENDLIST" in pl
          and f"#EXT-X-MEDIA-SEQUENCE:{first}" in pl
          and all(n in pl for n in segs)
          and all(zf.read(n) == SEED for n in segs))
    return ok, f"segs={len(segs)} expect={expect} names={names[:3]}..."


try:
    start_app()

    # ---------- A: basic record -> stop -> package ----------
    jid = start_job(f"{SRC}/live.m3u8")
    r = api("get", f"/api/jobs/{jid}/download")
    check("A 录制中下载被拒(409)", r.status_code == 409, f"got {r.status_code}")
    time.sleep(9)
    j = stop_job(jid)
    check("A 停止被受理", j["state"] in ("finalizing", "done"), str(j))
    j = wait_job(jid, {"done"}, 40)
    check("A 全部应收片段保存后完成", j["state"] == "done", str(j))
    ok, detail = verify_zip(jid, j["first_seq"], j["stop_seq"])
    check("A ZIP可离线播放(本地m3u8+原片段,无远端引用)", ok, detail)

    # ---------- B: two concurrent recordings, slow one must not block ----------
    slow = start_job(f"{SRC}/live.m3u8?delay_ms=2500")
    fast = start_job(f"{SRC}/live.m3u8")
    time.sleep(6)
    sj, fj = get_job(slow), get_job(fast)
    check("B 慢下载不阻塞另一路", fj["saved"] >= 2 and fj["saved"] > sj["saved"],
          f"slow={sj['saved']} fast={fj['saved']}")
    t0 = time.time(); stop_job(slow); dt_stop = time.time() - t0
    t0 = time.time(); api("get", "/api/jobs"); dt_q = time.time() - t0
    check("B 停止受理不被慢下载阻塞(<1s)", dt_stop < 1.0, f"{dt_stop:.2f}s")
    check("B 查询不被慢下载阻塞(<1s)", dt_q < 1.0, f"{dt_q:.2f}s")
    stop_job(fast)
    js = wait_job(slow, {"done"}, 90)
    jf = wait_job(fast, {"done"}, 90)
    check("B 两路均完成", js["state"] == "done" and jf["state"] == "done",
          f"slow={js['state']} fast={jf['state']}")

    # ---------- C: expected segment returns 404 -> fail with missing list ----------
    miss = now_seq() + 4
    jid = start_job(f"{SRC}/live.m3u8?missing={miss}")
    j = wait_job(jid, {"failed"}, 60)
    check("C 应收片段404须失败", j["state"] == "failed", str(j))
    check("C 缺段被列出", miss in (j["missing"] or []), str(j["missing"]))
    check("C 错误说明404", "404" in (j["error"] or ""), str(j["error"]))
    r = api("get", f"/api/jobs/{jid}/download")
    check("C 失败任务不可下载", r.status_code == 409, f"got {r.status_code}")

    # ---------- D: window jumps over undiscovered seqs -> fail with missing ----------
    ja = now_seq() + 3
    jid = start_job(f"{SRC}/live.m3u8?jump_at={ja}&jump_by=8")
    j = wait_job(jid, {"failed"}, 60)
    check("D 窗口越过未发现序号须失败", j["state"] == "failed" and "越过" in (j["error"] or ""),
          str(j))
    check("D 缺段被列出", len(j["missing"] or []) > 0, str(j["missing"]))

    # ---------- H: invalid playlists rejected ----------
    for path, kw in [("master.m3u8", "单层"), ("enc.m3u8", "加密"), ("disc.m3u8", "不连续")]:
        jid = start_job(f"{SRC}/{path}")
        j = wait_job(jid, {"failed"}, 30)
        check(f"H 非法清单被拒({path})", j["state"] == "failed" and kw in (j["error"] or ""),
              str(j["error"]))

    # ---------- I: ENDLIST source auto-finalizes ----------
    jid = start_job(f"{SRC}/vod.m3u8")
    j = wait_job(jid, {"done"}, 30)
    check("I ENDLIST自动定界完成", j["state"] == "done" and j["saved"] == 3, str(j))

    # ---------- E: kill -9 mid-recording, restart resumes, nothing lost ----------
    jid = start_job(f"{SRC}/live.m3u8")
    j = wait_saved(jid, 8, 60)          # > window(5): early segs already slid out
    saved_before = j["saved"]
    kill_app()
    start_app()
    j = get_job(jid)
    check("E 重启后续录(状态/进度保留)", j["state"] == "recording" and j["saved"] >= saved_before,
          str(j))
    j = stop_job(jid)
    first, stop = j["first_seq"], j["stop_seq"]
    j = wait_job(jid, {"done"}, 60)
    ok, detail = verify_zip(jid, first, stop)
    check("E 重启后完成且片段完整(含滑出窗口的)", ok and stop - first + 1 >= saved_before,
          f"{detail} saved_before={saved_before} range={stop - first + 1}")

    # ---------- F: accepted stop boundary survives restart ----------
    jid = start_job(f"{SRC}/live.m3u8?delay_ms=800")
    wait_saved(jid, 2, 60)
    j = stop_job(jid)
    stop_seq = j["stop_seq"]
    kill_app()
    start_app()
    j = get_job(jid)
    check("F 已受理停止边界重启不变", j["stop_seq"] == stop_seq
          and j["state"] in ("finalizing", "done"), str(j))
    j = wait_job(jid, {"done"}, 60)
    ok, detail = verify_zip(jid, j["first_seq"], stop_seq)
    check("F 边界内片段全部入包", ok and j["stop_seq"] == stop_seq, detail)

    # ---------- G: crash between segment write and DB registration ----------
    jid = start_job(f"{SRC}/live.m3u8")
    j = wait_saved(jid, 2, 60)
    target = j["last_seq"] + 2          # a segment not downloaded yet
    api("post", f"/api/_test/arm_after_write/{jid}/{target}")
    # process dies right after writing `target` to disk, before registering it
    for _ in range(60):
        if not port_in_use():
            break
        time.sleep(0.5)
    assert not port_in_use(), "app did not crash as armed"
    if app_proc:
        app_proc.wait(timeout=10)
    app_proc = None
    fpath = f"{DATA}/segments/{jid}/seg_{target:08d}.ts"
    check("G 预备:片段已落盘但未登记", os.path.getsize(fpath) == len(SEED))
    conn = sqlite3.connect(f"{DATA}/recorder.db")
    st = conn.execute("SELECT status FROM segments WHERE job_id=? AND seq=?",
                      (jid, target)).fetchone()[0]
    conn.close()
    check("G 预备:DB中该段未登记为done", st != "done", f"status={st}")
    start_app()
    logtxt = open(os.environ.get("APP_LOG", "/tmp/app_test.log"), errors="ignore").read()
    check("G 重启按磁盘对账收养已落盘片段",
          f"job {jid} seq {target}: adopting fully written segment" in logtxt)
    j = stop_job(jid)
    j = wait_job(jid, {"done"}, 60)
    ok, detail = verify_zip(jid, j["first_seq"], j["stop_seq"])
    check("G 收养的片段进入最终包", ok, detail)

finally:
    kill_app()

print("\n==== SUMMARY ====")
failed = [n for n, ok in results if not ok]
print(f"{len(results) - len(failed)}/{len(results)} passed")
if failed:
    print("FAILED:", *failed, sep="\n  - ")
    sys.exit(1)
print("ALL PASS")
