"""Recording engine.

Each job runs a playlist poller (follows the sliding window, keyed on media
sequence numbers) and a downloader (persists every expected segment). State
and progress live in SQLite; segment bytes live on the persistent volume, so a
restart can resume exactly where it stopped.

Segment bodies are streamed from the source to a ``.part`` file and only
published (atomic rename) once the whole body has been received and flushed.
An interrupted transfer or a killed process therefore never leaves a halved
file under the final name, and peak memory is independent of segment size.
"""

import asyncio
import logging
import os
import zipfile

import httpx

from .playlist import parse_media_playlist, PlaylistError

log = logging.getLogger("recorder")

MAX_POLL_ERRORS = 15          # consecutive playlist fetch failures before failing the job
MAX_DOWNLOAD_ATTEMPTS = 5     # per segment, transient errors only (404 fails immediately)
STREAM_CHUNK = 256 * 1024     # bounded buffer for segment downloads

# Test-only hook (active solely when RECORDER_TEST_HOOKS=1): crash the process
# right after a segment is fully written but before it is registered in the DB.
TEST_HOOKS = {"crash_after_write": None}  # (job_id, seq)


class SourceError(Exception):
    """The source violates the accepted subset or continuity rules."""


class SegmentNotFound(Exception):
    """The source explicitly answered 404 for an expected segment."""


def _fsync_file(path: str):
    """Flush a fully written file to stable storage (runs in a worker thread)."""
    try:
        fd = os.open(path, os.O_RDONLY)
    except OSError:
        return
    try:
        os.fsync(fd)
    except OSError:
        pass
    finally:
        os.close(fd)


def _fsync_dir(path: str):
    """Best-effort directory fsync so an atomic rename survives power loss."""
    try:
        fd = os.open(path, os.O_RDONLY)
    except OSError:
        return
    try:
        os.fsync(fd)
    except OSError:
        pass
    finally:
        os.close(fd)


class JobRunner:
    def __init__(self, mgr: "Manager", job_id: int):
        self.mgr = mgr
        self.job_id = job_id
        self.db = mgr.db
        self.wake = asyncio.Event()
        self.poller: asyncio.Task | None = None
        self.downloader: asyncio.Task | None = None
        self.poll_interval = 1.0

    # ---- paths ----

    @property
    def seg_dir(self) -> str:
        return self.mgr.seg_dir(self.job_id)

    def seg_path(self, seq: int) -> str:
        return os.path.join(self.seg_dir, f"seg_{seq:08d}.ts")

    # ---- lifecycle ----

    def start(self):
        os.makedirs(self.seg_dir, exist_ok=True)
        self._reconcile_disk()
        job = self.db.get_job(self.job_id)
        if job["state"] == "recording":
            self.poller = asyncio.create_task(self._poll_loop(), name=f"poll-{self.job_id}")
        self.downloader = asyncio.create_task(self._download_loop(), name=f"dl-{self.job_id}")
        log.info("job %s started/resumed in state %s", self.job_id, job["state"])

    def _reconcile_disk(self):
        """Crash recovery, done before any new downloads start.

        * a segment registered done whose file is gone/truncated -> download it
          again (if the source still offers it);
        * a segment still pending whose file exists -> it can only have been
          produced by an atomic publish, so it was fully received: adopt it;
        * ``.part`` files are incomplete by definition -> delete them.
        """
        for seg in self.db.list_segments(self.job_id):
            path = self.seg_path(seg["seq"])
            exists = os.path.exists(path)
            size = os.path.getsize(path) if exists else -1
            if seg["status"] == "done":
                expected = seg["bytes"]
                if size <= 0 or (expected is not None and size != expected):
                    log.warning(
                        "job %s seq %s: registered done but file missing/incomplete "
                        "(size=%s expected=%s), re-download",
                        self.job_id, seg["seq"], size, expected)
                    self.db.segment_pending(self.job_id, seg["seq"])
            elif seg["status"] == "pending":
                if size > 0:
                    log.info("job %s seq %s: adopting fully written segment from disk",
                             self.job_id, seg["seq"])
                    self.db.segment_done(self.job_id, seg["seq"], path, size)
                elif exists:
                    try:
                        os.remove(path)
                    except OSError:
                        pass
            elif seg["status"] == "missing" and seg["url"] is not None:
                if size > 0:
                    # A previously published full file survived; trust it.
                    log.info("job %s seq %s: adopting fully written segment from disk",
                             self.job_id, seg["seq"])
                    self.db.segment_done(self.job_id, seg["seq"], path, size)
                    continue
                # A failed fetch is not necessarily still failing; give a
                # previously missing but addressable segment another chance on
                # resume. Truly undiscoverable seqs keep url=NULL and stay
                # missing (reported, never downloaded).
                log.info("job %s seq %s: retrying previously missing segment",
                         self.job_id, seg["seq"])
                self.db.segment_pending(self.job_id, seg["seq"])
        try:
            names = os.listdir(self.seg_dir)
        except OSError:
            return
        for name in names:
            if name.endswith(".part"):
                try:
                    os.remove(os.path.join(self.seg_dir, name))
                except OSError:
                    pass

    # ---- playlist polling ----

    async def _poll_loop(self):
        errors = 0
        while True:
            job = self.db.get_job(self.job_id)
            if job["state"] != "recording":
                return
            try:
                await self._poll_once(job)
                errors = 0
            except (SourceError, PlaylistError) as exc:
                self.mgr.fail_job(self.job_id, str(exc))
                return
            except Exception as exc:  # transient fetch/parse trouble: retry
                errors += 1
                log.warning("job %s playlist fetch failed (%s/%s): %s",
                            self.job_id, errors, MAX_POLL_ERRORS, exc)
                if errors >= MAX_POLL_ERRORS:
                    self.mgr.fail_job(self.job_id, f"清单反复拉取失败: {exc}")
                    return
            await asyncio.sleep(self.poll_interval)

    async def _poll_once(self, job: dict):
        resp = await self.mgr.client.get(job["url"])
        resp.raise_for_status()
        # If stop was accepted while the fetch was in flight, this playlist
        # arrived after the boundary was pinned and must be discarded as a
        # whole. The state is re-read immediately before the (synchronous) DB
        # mutations below, so no segment can slip in past the boundary.
        fresh = self.db.get_job(self.job_id)
        if fresh["state"] != "recording":
            return
        pl = parse_media_playlist(resp.text, job["url"])
        last = pl.segments[-1].seq

        if fresh["first_seq"] is None:
            self.db.init_window(self.job_id, pl.media_sequence, last, pl.target_duration)
            self.poll_interval = min(max(pl.target_duration / 2.0, 0.25), 5.0)
            log.info("job %s window starts at seq %s", self.job_id, pl.media_sequence)
        else:
            if pl.media_sequence > fresh["last_seq"] + 1:
                missing = list(range(fresh["last_seq"] + 1, pl.media_sequence))
                self.db.insert_missing(self.job_id, missing)
                raise SourceError(f"窗口越过未发现的序号: {missing}")
            if last > fresh["last_seq"]:
                self.db.update_last_seq(self.job_id, last)

        for seg in pl.segments:
            if fresh["first_seq"] is not None and seg.seq < fresh["first_seq"]:
                # A source that rewinds below our start changes nothing about
                # the recording range; those seqs are not part of this job.
                continue
            row = self.db.get_segment(self.job_id, seg.seq)
            if row is not None:
                if row["url"] is not None and row["url"] != seg.url:
                    raise SourceError(f"序号 {seg.seq} 的片段地址发生变化（源不一致）")
                continue
            dup = self.db.get_segment_by_url(self.job_id, seg.url)
            if dup is not None:
                raise SourceError(
                    f"片段地址复用: {seg.url} 同时属于序号 {dup['seq']} 与 {seg.seq}")
            self.db.insert_segment(self.job_id, seg.seq, seg.url, seg.duration, seg.title)
        self.wake.set()

        if pl.endlist:
            log.info("job %s source signalled ENDLIST, pinning boundary", self.job_id)
            self.mgr.accept_stop(self.job_id)

    # ---- segment downloading ----

    async def _download_loop(self):
        try:
            while True:
                self.wake.clear()
                job = self.db.get_job(self.job_id)
                if job is None or job["state"] in ("failed", "done"):
                    return
                seg = self.db.next_pending(self.job_id)
                if seg is not None:
                    await self._download_one(seg)
                    continue
                if job["state"] == "finalizing":
                    if await self._finalize(job):
                        return
                    if self.db.next_pending(self.job_id) is None:
                        await asyncio.sleep(0.05)
                    continue
                await self.wake.wait()
        except asyncio.CancelledError:
            raise
        except Exception as exc:
            log.exception("job %s downloader crashed", self.job_id)
            self.mgr.fail_job(self.job_id, f"内部错误: {exc}")

    async def _download_one(self, seg: dict):
        seq, url = seg["seq"], seg["url"]
        if url is None:  # never discovered (window skipped it): not retrievable
            self.db.segment_missing(self.job_id, seq, "该序号未被任何清单提供")
            self.mgr.fail_job(self.job_id, f"停止边界内缺段: 序号 {seq} 无法取得")
            return
        path = self.seg_path(seq)
        tmp = path + ".part"
        delay = 0.5
        for attempt in range(1, MAX_DOWNLOAD_ATTEMPTS + 1):
            try:
                nbytes = await self._stream_segment(url, tmp)
                await asyncio.to_thread(self._commit_segment, tmp, path)
                if TEST_HOOKS["crash_after_write"] == (self.job_id, seq):
                    os._exit(137)  # test hook: die between flush and DB register
                self.db.segment_done(self.job_id, seq, path, nbytes)
                return
            except SegmentNotFound:
                self._discard_part(tmp)
                self.db.segment_missing(self.job_id, seq, "HTTP 404")
                self.mgr.fail_job(self.job_id, f"应收片段返回 404: 序号 {seq}")
                return
            except httpx.HTTPError as exc:
                self._discard_part(tmp)
                log.warning("job %s seq %s download attempt %s failed: %s",
                            self.job_id, seq, attempt, exc)
                await asyncio.sleep(delay)
                delay = min(delay * 2, 8.0)
            except OSError as exc:
                # local write/rename trouble (disk full, permissions...): retry
                self._discard_part(tmp)
                log.warning("job %s seq %s local write attempt %s failed: %s",
                            self.job_id, seq, attempt, exc)
                await asyncio.sleep(delay)
                delay = min(delay * 2, 8.0)
            except BaseException:
                self._discard_part(tmp)
                raise
        self.mgr.fail_job(self.job_id, f"片段序号 {seq} 多次重试后仍下载失败")

    async def _stream_segment(self, url: str, tmp: str) -> int:
        """Stream a segment to ``tmp`` with a bounded buffer.

        Returns the number of bytes written. Raises SegmentNotFound on 404 and
        httpx.HTTPError for any interrupted/truncated/failed transfer. The
        caller is responsible for removing ``tmp`` on failure.
        """
        timeout = httpx.Timeout(60.0, connect=10.0)
        async with self.mgr.client.stream("GET", url, timeout=timeout) as resp:
            if resp.status_code == 404:
                raise SegmentNotFound()
            if resp.status_code != 200:
                raise httpx.HTTPStatusError(
                    f"HTTP {resp.status_code}", request=resp.request, response=resp)
            expected = resp.headers.get("content-length")
            try:
                expected = int(expected) if expected is not None else None
            except ValueError:
                expected = None
            total = 0
            with open(tmp, "wb") as fh:
                try:
                    async for chunk in resp.aiter_bytes(STREAM_CHUNK):
                        if not chunk:
                            continue
                        fh.write(chunk)  # buffered local write; memory stays at chunk size
                        total += len(chunk)
                    fh.flush()
                except BaseException:
                    # covers cancellation / protocol errors: leave no final file
                    try:
                        fh.flush()
                    except OSError:
                        pass
                    raise
            # fsync may take a while on a big body; keep the event loop
            # (progress queries, stop acceptance, other jobs) free. The file is
            # closed already, so a cancelled wait cannot race the fsync.
            await asyncio.to_thread(_fsync_file, tmp)
        if expected is not None and total != expected:
            raise httpx.HTTPError(
                f"片段传输不完整: 收到 {total}/{expected} 字节")
        if total <= 0:
            raise httpx.HTTPError("片段为空")
        if expected is None and total % 188 != 0:
            # No Content-Length to compare against; a TS body that ends
            # mid-packet is the signature of an interrupted transfer.
            raise httpx.HTTPError(
                f"片段传输疑似不完整: {total} 字节不是 TS 包长的整数倍")
        return total

    @staticmethod
    def _commit_segment(tmp: str, path: str):
        # Atomic publish: readers/restarters either see no file or the full one.
        os.replace(tmp, path)
        _fsync_dir(os.path.dirname(path))

    @staticmethod
    def _discard_part(tmp: str):
        try:
            os.remove(tmp)
        except OSError:
            pass

    # ---- finalize ----

    async def _finalize(self, job: dict) -> bool:
        """Try to produce the offline package. Returns True when the job is
        terminal (done or failed), False when the downloader should keep
        working (segments still pending / requeued)."""
        job = self.db.get_job(self.job_id)
        if job is None or job["state"] != "finalizing":
            return True
        first = job["first_seq"]
        stop = job["stop_seq"]
        if stop is None:
            stop = job["last_seq"]

        if first is None and stop is None:
            # Stopped before any playlist was received: the boundary is empty,
            # so the package is empty too (nothing expected was skipped).
            log.info("job %s stopped before any playlist; packaging an empty range",
                     self.job_id)

        rows = self.db.list_segments(self.job_id)
        by_seq = {s["seq"]: s for s in rows}
        if first is not None and stop is not None:
            expected = list(range(first, stop + 1))
        elif stop is not None:
            expected = [s["seq"] for s in rows if s["seq"] <= stop]
        else:
            expected = [s["seq"] for s in rows]

        # Only sequences inside the pinned stop boundary may be packaged, and
        # every one of them must be present and actually saved.
        missing = []
        unknown = []
        for seq in expected:
            row = by_seq.get(seq)
            if row is None:
                unknown.append(seq)
            elif row["status"] == "missing":
                missing.append(seq)
            elif row["url"] is None:
                # A record without an address can never be fetched.
                self.db.segment_missing(self.job_id, seq, "该序号没有可用片段地址")
                missing.append(seq)
        if unknown:
            self.db.insert_missing(self.job_id, unknown)
            missing.extend(unknown)
        if missing:
            missing = sorted(set(missing))
            self.mgr.fail_job(
                self.job_id, f"停止边界内片段不齐全，缺段: {missing}")
            return True

        segs = [by_seq[seq] for seq in expected]
        if any(s["status"] != "done" for s in segs):
            return False

        # DB says done: verify the bytes are really on disk before publishing.
        bad = []
        for s in segs:
            path = self.seg_path(s["seq"])
            try:
                size = os.path.getsize(path)
            except OSError:
                size = -1
            if size <= 0 or (s["bytes"] is not None and size != s["bytes"]):
                bad.append(s["seq"])
                self.db.segment_pending(self.job_id, s["seq"])
        if bad:
            log.warning("job %s finalize: segments missing/incomplete on disk, "
                        "re-downloading: %s", self.job_id, bad)
            self.wake.set()
            return False

        target = job["target_duration"] or 2.0
        lines = [
            "#EXTM3U",
            "#EXT-X-VERSION:3",
            f"#EXT-X-TARGETDURATION:{max(1, round(target))}",
            "#EXT-X-PLAYLIST-TYPE:VOD",
            f"#EXT-X-MEDIA-SEQUENCE:{first if first is not None else 0}",
        ]
        for s in segs:
            lines.append(f"#EXTINF:{(s['duration'] or target):.3f},")
            lines.append(os.path.basename(self.seg_path(s["seq"])))
        lines.append("#EXT-X-ENDLIST")
        playlist_text = "\n".join(lines) + "\n"
        zip_path = self.mgr.zip_path(self.job_id)
        try:
            await asyncio.to_thread(self._write_package, playlist_text, segs, zip_path)
        except Exception as exc:
            log.exception("job %s package write failed", self.job_id)
            self.mgr.fail_job(self.job_id, f"打包失败: {exc}")
            return True
        if not self.db.job_done(self.job_id, zip_path):
            # State changed underneath us (e.g. another failure); never expose it.
            try:
                os.remove(zip_path)
            except OSError:
                pass
            return True
        log.info("job %s done: %s segments packaged", self.job_id, len(segs))
        return True

    def _write_package(self, playlist_text: str, segs: list, zip_path: str):
        os.makedirs(os.path.dirname(zip_path), exist_ok=True)
        playlist_file = os.path.join(self.seg_dir, "playlist.m3u8")
        with open(playlist_file, "w", encoding="utf-8") as fh:
            fh.write(playlist_text)
        tmp_zip = zip_path + ".part"
        try:
            with zipfile.ZipFile(tmp_zip, "w", zipfile.ZIP_STORED) as zf:
                zf.writestr("playlist.m3u8", playlist_text)
                for s in segs:
                    name = os.path.basename(self.seg_path(s["seq"]))
                    zf.write(self.seg_path(s["seq"]), name)
            with open(tmp_zip, "rb+") as fh:
                os.fsync(fh.fileno())
            os.replace(tmp_zip, zip_path)
            _fsync_dir(os.path.dirname(zip_path))
        except BaseException:
            try:
                os.remove(tmp_zip)
            except OSError:
                pass
            raise


class Manager:
    def __init__(self, db, data_dir: str):
        self.db = db
        self.data_dir = data_dir
        self.client = httpx.AsyncClient(
            follow_redirects=True,
            # Keep Content-Length comparable with the byte count we stream to
            # disk (no transparent decompression of segment bodies).
            headers={"User-Agent": "hls-live-recorder/1.0", "Accept-Encoding": "identity"},
            limits=httpx.Limits(max_connections=32, max_keepalive_connections=16),
        )
        self.runners: dict[int, JobRunner] = {}

    def seg_dir(self, job_id: int) -> str:
        return os.path.join(self.data_dir, "segments", str(job_id))

    def zip_path(self, job_id: int) -> str:
        return os.path.join(self.data_dir, "zips", f"job_{job_id}.zip")

    def create_job(self, url: str) -> int:
        job_id = self.db.insert_job(url)
        runner = JobRunner(self, job_id)
        self.runners[job_id] = runner
        runner.start()
        return job_id

    def recover(self):
        zdir = os.path.join(self.data_dir, "zips")
        os.makedirs(zdir, exist_ok=True)
        # Incomplete packages are never exposed; they are rebuilt from segments.
        for name in os.listdir(zdir):
            if name.endswith(".part"):
                try:
                    os.remove(os.path.join(zdir, name))
                except OSError:
                    pass
        for job in self.db.unfinished_jobs():
            runner = JobRunner(self, job["id"])
            self.runners[job["id"]] = runner
            runner.start()

    def accept_stop(self, job_id: int) -> bool:
        """Pin the boundary at the tail of the last received playlist and
        persist it immediately, so a restart cannot change it."""
        job = self.db.get_job(job_id)
        if job is None or job["state"] != "recording":
            return False
        self.db.set_stopping(job_id, job["last_seq"])
        runner = self.runners.get(job_id)
        if runner is not None:
            if runner.poller is not None and runner.poller is not asyncio.current_task():
                runner.poller.cancel()
            runner.wake.set()
        log.info("job %s stop accepted, boundary pinned at seq %s", job_id, job["last_seq"])
        return True

    def fail_job(self, job_id: int, message: str):
        self.db.job_failed(job_id, message)
        runner = self.runners.get(job_id)
        if runner is not None:
            if runner.poller is not None and runner.poller is not asyncio.current_task():
                runner.poller.cancel()
            runner.wake.set()
        log.error("job %s failed: %s", job_id, message)

    async def shutdown(self):
        tasks = []
        for runner in self.runners.values():
            for task in (runner.poller, runner.downloader):
                if task is not None and not task.done():
                    task.cancel()
                    tasks.append(task)
        if tasks:
            await asyncio.gather(*tasks, return_exceptions=True)
        await self.client.aclose()
