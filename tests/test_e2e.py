#!/usr/bin/env python3
"""End-to-end tests for the HLS live recorder against the local fake source.

Covers the original behaviour (record/stop/package, concurrency, invalid
playlists, crash recovery) plus the long-segment hardening: bounded memory
during download, interrupted transfers never surfacing as complete segments,
restart reconciliation, and missing-segment reporting for unretrievable seqs.

Requires the fake source (livesource) running; see README.
"""
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


def _rss_kb(pid):
    """Resident set size of a process in KiB (Linux)."""
    try:
        with open(f"/proc/{pid}/status") as fh:
            for line in fh:
                if line.startswith("VmRSS:"):
                    return int(line.split()[1])
    except OSError:
        pass
    try:  # fall back to the whole process group (uvicorn may spawn workers)
        with open(f"/proc/{pid}/statm") as fh:
            return int(fh.read().split()[1]) * (os.sysconf("SC_PAGE_SIZE") // 1024)
    except OSError:
        return 0


def _seg_sizes(jid, first, n, size_bytes=32 * 1024 * 1024):
    if first is None:
        return False
    for seq in range(first, first + n):
        p = f"{DATA}/segments/{jid}/seg_{seq:08d}.ts"
        if not os.path.exists(p) or os.path.getsize(p) != size_bytes:
            return False
    return True


def _no_partial_done(jid):
    """No .part leftovers and every done segment matches its registered size."""
    d = f"{DATA}/segments/{jid}"
    if os.path.isdir(d) and any(n.endswith(".part") for n in os.listdir(d)):
        return False
    conn = sqlite3.connect(f"{DATA}/recorder.db")
    rows = conn.execute("SELECT seq, bytes FROM segments WHERE job_id=? AND status='done'",
                        (jid,)).fetchall()
    conn.close()
    for seq, nbytes in rows:
        p = f"{d}/seg_{seq:08d}.ts"
        if not os.path.exists(p) or os.path.getsize(p) != nbytes:
            return False
    return True


def verify_zip(jid, first, stop, expect_bytes=None):
    zf = fetch_zip(jid)
    names = zf.namelist()
    pl = zf.read("playlist.m3u8").decode()
    segs = sorted(n for n in names if n.endswith(".ts"))
    expect = stop - first + 1
    ok = ("playlist.m3u8" in names and len(segs) == expect
          and "http" not in pl.lower() and "#EXT-X-ENDLIST" in pl
          and f"#EXT-X-MEDIA-SEQUENCE:{first}" in pl
          and all(n in pl for n in segs))
    if expect_bytes is None:
        ok = ok and all(zf.read(n) == SEED for n in segs)
    else:
        ok = ok and all(len(zf.read(n)) == expect_bytes for n in segs)
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

    # ---------- L: long segments: bounded memory during download ----------
    # One 32 MiB body, throttled so RSS can be sampled mid-transfer. Peak RSS
    # must stay within a small delta of idle: the old implementation buffered
    # the entire body in memory (+32 MiB).
    baseline = _rss_kb(app_proc.pid)
    jid = start_job(f"{SRC}/vod.m3u8?count=1&size_mb=32&rate_kbps=4096")
    peak = 0
    deadline = time.time() + 120
    j = get_job(jid)
    while time.time() < deadline and j["state"] not in ("done", "failed"):
        peak = max(peak, _rss_kb(app_proc.pid))
        j = get_job(jid)
        time.sleep(0.15)
    seg_kb = 32 * 1024
    check("L 长片段下载期间内存不随片段增大(峰值仅小幅高于基线)",
          j["state"] == "done" and peak - baseline < 16 * 1024,
          f"baseline={baseline}KiB peak={peak}KiB delta={peak - baseline}KiB "
          f"seg={seg_kb}KiB state={j['state']}")
    check("L 长片段完整落盘", _seg_sizes(jid, j["first_seq"], 1),
          f"state={j['state']} saved={j['saved']}")
    ok, detail = verify_zip(jid, j["first_seq"], j["stop_seq"], expect_bytes=seg_kb * 1024)
    check("L 长片段包内字节数正确", j["state"] == "done" and ok, f"{detail} {j['error']}")

    # ---------- Q: stop/progress stay responsive during a long transfer ----------
    jid = start_job(f"{SRC}/live.m3u8?size_mb=8&rate_kbps=4096")
    deadline = time.time() + 60
    while time.time() < deadline:
        try:
            names = os.listdir(f"{DATA}/segments/{jid}")
        except OSError:
            names = []
        if any(n.endswith(".part") for n in names):
            break
        time.sleep(0.1)
    t0 = time.time(); jstop = stop_job(jid); dt_stop = time.time() - t0
    t0 = time.time(); api("get", "/api/jobs"); dt_q = time.time() - t0
    check("Q 长片段下载中停止/查询仍响应(<1s)",
          jstop["state"] in ("finalizing", "done") and dt_stop < 1.0 and dt_q < 1.0,
          f"stop={dt_stop:.2f}s query={dt_q:.2f}s state={jstop['state']}")
    j = wait_job(jid, {"done", "failed"}, 180)
    check("Q 长片段下载中停止后仍能完成", j["state"] == "done", f"{j['state']} {j['error']}")

    # ---------- P: long transfer interrupted by process exit ----------
    jid = start_job(f"{SRC}/vod.m3u8?count=2&size_mb=8&rate_kbps=2048")
    part_dir = f"{DATA}/segments/{jid}"
    deadline = time.time() + 60
    part_seen = False
    while time.time() < deadline:
        try:
            names = os.listdir(part_dir)
        except OSError:
            names = []
        if any(n.endswith(".part") and os.path.getsize(os.path.join(part_dir, n)) > 0
               for n in names):
            part_seen = True
            break
        time.sleep(0.05)
    j = get_job(jid)
    check("P 预备:存在未完成的.part", part_seen and j["saved"] == 0,
          f"part_seen={part_seen} saved={j['saved']}")
    kill_app()
    # after the crash the half-written .part may remain, but no half file may
    # have been published under the final segment name
    final_files = [n for n in os.listdir(part_dir) if n.endswith(".ts")]
    check("P 崩溃后半截文件未以完成段名落盘",
          all(os.path.getsize(os.path.join(part_dir, n)) == 8 * 1024 * 1024
              for n in final_files), f"files={final_files}")
    start_app()
    # wait for recovery to clean .part before the retry streams anew
    deadline = time.time() + 15
    while time.time() < deadline:
        if not any(n.endswith(".part") for n in os.listdir(part_dir)):
            break
        time.sleep(0.05)
    check("P 重启清理未完成的.part",
          not any(n.endswith(".part") for n in os.listdir(part_dir)),
          str(os.listdir(part_dir)))
    j = get_job(jid)
    check("P 重启后任务仍可续录", j["state"] in ("recording", "finalizing"), str(j))
    j = wait_job(jid, {"done", "failed"}, 180)
    ok, detail = verify_zip(jid, j["first_seq"], j["stop_seq"],
                            expect_bytes=8 * 1024 * 1024)
    check("P 中断后重下并完整打包", j["state"] == "done" and ok,
          f"{detail} {j['error']}")

    # ---------- M: interrupted transfer never becomes a "complete" segment ----------
    jid = start_job(f"{SRC}/live.m3u8?truncate_at=500000&size_mb=8")
    j = wait_job(jid, {"failed"}, 120)
    check("M 传输中断判失败(不会把半截当完成)", j["state"] == "failed", str(j))
    check("M 中断段未登记为done", _no_partial_done(jid), str(j))
    r = api("get", f"/api/jobs/{jid}/download")
    check("M 中断任务不可下载", r.status_code == 409, f"got {r.status_code}")

    # ---------- N: restart after failure keeps completed segments, retries rest ----------
    jid = start_job(f"{SRC}/live.m3u8?delay_ms=600")
    wait_saved(jid, 2, 60)
    kill_app()
    conn = sqlite3.connect(f"{DATA}/recorder.db")
    done = conn.execute("SELECT COUNT(*) FROM segments WHERE job_id=? AND status='done'",
                        (jid,)).fetchone()[0]
    conn.close()
    start_app()
    j = get_job(jid)
    check("N 重启保留完整落盘段", j["saved"] >= done, f"saved={j['saved']} db_done={done}")
    j = stop_job(jid)
    j = wait_job(jid, {"done"}, 60)
    ok, detail = verify_zip(jid, j["first_seq"], j["stop_seq"])
    check("N 重启后补收缺失段并完成", ok, f"{detail} {j['error']}")

    # ---------- R: a missing segment that is obtainable is backfilled on restart ----------
    jid = start_job(f"{SRC}/live.m3u8")
    j = wait_saved(jid, 3, 60)
    j = stop_job(jid)
    first, stop = j["first_seq"], j["stop_seq"]
    kill_app()
    # Mark an already-saved segment as missing AND remove its file, simulating a
    # segment lost during shutdown that the source can still serve.
    conn = sqlite3.connect(f"{DATA}/recorder.db")
    victim = first
    row = conn.execute("SELECT url FROM segments WHERE job_id=? AND seq=?",
                       (jid, victim)).fetchone()
    conn.execute("UPDATE segments SET status='missing', error='模拟丢失', bytes=NULL "
                 "WHERE job_id=? AND seq=?", (jid, victim))
    conn.commit()
    conn.close()
    try:
        os.remove(f"{DATA}/segments/{jid}/seg_{victim:08d}.ts")
    except OSError:
        pass
    start_app()
    j = wait_job(jid, {"done", "failed"}, 90)
    ok, detail = verify_zip(jid, first, stop)
    check("R 重启后补收可取得的缺失段", j["state"] == "done" and ok,
          f"{detail} {j['error']} {j['missing']}")

    # ---------- O: a seq inside the stop boundary that can never be retrieved ----------
    # Inject a sequence the recorder never received an address for (as after a
    # window jump), inside the boundary. It must be listed as missing and the
    # package must stay unavailable even if everything else is saved.
    jid = start_job(f"{SRC}/live.m3u8")
    j = wait_saved(jid, 3, 60)
    kill_app()
    conn = sqlite3.connect(f"{DATA}/recorder.db")
    badseq = now_seq() + 8
    conn.execute("INSERT OR REPLACE INTO segments(job_id,seq,url,status,error) "
                 "VALUES(?,?,NULL,'missing','窗口越过，未发现该序号')", (jid, badseq))
    conn.execute("UPDATE jobs SET last_seq=? WHERE id=?", (badseq, jid))
    conn.commit()
    conn.close()
    start_app()
    j = stop_job(jid)
    check("O 停止受理", j["state"] in ("finalizing", "done", "failed"), str(j))
    j = wait_job(jid, {"failed", "done"}, 60)
    check("O 不可取得缺段被列出", badseq in (j["missing"] or []), str(j))
    check("O 缺段时禁止下载", api("get", f"/api/jobs/{jid}/download").status_code == 409)
    check("O 缺段时不判完成", j["state"] == "failed", str(j))

finally:
    kill_app()

print("\n==== SUMMARY ====")
failed = [n for n, ok in results if not ok]
print(f"{len(results) - len(failed)}/{len(results)} passed")
if failed:
    print("FAILED:", *failed, sep="\n  - ")
    sys.exit(1)
print("ALL PASS")
