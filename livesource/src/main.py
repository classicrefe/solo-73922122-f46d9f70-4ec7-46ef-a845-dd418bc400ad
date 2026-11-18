"""Local fake live HLS source for testing the recorder.

Serves a sliding-window media playlist over a pre-generated MPEG-TS seed
segment. Query parameters on /live.m3u8 are propagated to segment URLs:

  delay_ms=N            sleep N ms before serving each segment (slow source)
  size_mb=F             serve a segment of at least F MiB (long segment)
  rate_kbps=K           throttle each segment body to K KiB/s (long download)
  missing=7,9           return 404 for those sequence numbers (missing segments)
  truncate_at=N         send only the first N bytes, then abort the body
                        (interrupted transfer; Content-Length stays full)
  jump_at=K&jump_by=J   once the clock reaches seq K, shift all sequence
                        numbers by J (window jumps over undiscovered numbers)

Also serves invalid playlists (/master.m3u8, /enc.m3u8, /disc.m3u8) and a
finite /vod.m3u8 for validation tests.
"""

import asyncio
import os
import time
from functools import lru_cache

from fastapi import FastAPI, HTTPException
from fastapi.responses import FileResponse, PlainTextResponse, StreamingResponse

WINDOW_SIZE = int(os.environ.get("WINDOW_SIZE", "5"))
SEGMENT_DURATION = float(os.environ.get("SEGMENT_DURATION", "2"))
SEED_TS = os.environ.get("SEED_TS", "/srv/seed.ts")
CHUNK = 256 * 1024

EPOCH = time.time()
app = FastAPI(title="fake live HLS source")


def _now_seq() -> int:
    return int((time.time() - EPOCH) // SEGMENT_DURATION) + 1


def _parse_missing(missing: str) -> set:
    out = set()
    for part in (missing or "").split(","):
        part = part.strip()
        if part:
            out.add(int(part))
    return out


def _segment_query(delay_ms: int, missing: str, size_mb: float,
                   rate_kbps: int, truncate_at: int) -> str:
    q = [f"delay_ms={delay_ms}", f"missing={missing}"]
    if size_mb:
        q.append(f"size_mb={size_mb}")
    if rate_kbps:
        q.append(f"rate_kbps={rate_kbps}")
    if truncate_at:
        q.append(f"truncate_at={truncate_at}")
    return "?" + "&".join(q)


def _playlist(seqs, extra_lines=None, endlist=False, query="") -> str:
    lines = [
        "#EXTM3U",
        "#EXT-X-VERSION:3",
        f"#EXT-X-TARGETDURATION:{max(1, round(SEGMENT_DURATION))}",
        f"#EXT-X-MEDIA-SEQUENCE:{seqs[0]}",
    ]
    if extra_lines:
        lines.extend(extra_lines)
    for s in seqs:
        lines.append(f"#EXTINF:{SEGMENT_DURATION:.3f},")
        lines.append(f"/seg/{s}.ts{query}")
    if endlist:
        lines.append("#EXT-X-ENDLIST")
    return "\n".join(lines) + "\n"


@lru_cache(maxsize=1)
def _seed() -> bytes:
    with open(SEED_TS, "rb") as fh:
        return fh.read()


def _long_length(size_mb: float) -> int:
    return int(size_mb * 1024 * 1024)


def _long_chunks(size_mb: float, limit: int):
    """Yield a long MPEG-TS body by cycling the seed segment in bounded
    chunks (the source itself never materialises the whole body)."""
    seed = _seed()
    pos = 0
    sent = 0
    while sent < limit:
        n = min(CHUNK, limit - sent)
        out = bytearray()
        while len(out) < n:
            take = min(n - len(out), len(seed) - pos)
            out += seed[pos:pos + take]
            pos += take
            if pos >= len(seed):
                pos = 0
        sent += n
        yield bytes(out)


@app.get("/live.m3u8", response_class=PlainTextResponse)
def live(delay_ms: int = 0, missing: str = "", size_mb: float = 0,
         rate_kbps: int = 0, truncate_at: int = 0,
         jump_at: int = 0, jump_by: int = 0):
    cur = _now_seq()
    eff = cur + (jump_by if jump_at and cur >= jump_at else 0)
    first = max(1, eff - WINDOW_SIZE + 1)
    q = _segment_query(delay_ms, missing, size_mb, rate_kbps, truncate_at)
    lines = [
        "#EXTM3U",
        "#EXT-X-VERSION:3",
        f"#EXT-X-TARGETDURATION:{max(1, round(SEGMENT_DURATION))}",
        f"#EXT-X-MEDIA-SEQUENCE:{first}",
    ]
    for s in range(first, eff + 1):
        lines.append(f"#EXTINF:{SEGMENT_DURATION:.3f},")
        lines.append(f"/seg/{s}.ts{q}")
    return "\n".join(lines) + "\n"


@app.get("/seg/{seq}.ts")
async def seg(seq: int, delay_ms: int = 0, missing: str = "",
              size_mb: float = 0, rate_kbps: int = 0, truncate_at: int = 0):
    if seq in _parse_missing(missing):
        raise HTTPException(404, f"segment {seq} intentionally missing")
    if delay_ms > 0:
        await asyncio.sleep(delay_ms / 1000.0)

    if size_mb > 0:
        total = _long_length(size_mb)
    else:
        total = len(_seed())
    headers = {"Content-Length": str(total), "Cache-Control": "no-store"}

    if not rate_kbps and not truncate_at and size_mb <= 0:
        return FileResponse(SEED_TS, media_type="video/mp2t", headers=headers)

    limit = total if not truncate_at else min(truncate_at, total)
    sleep_per_chunk = (CHUNK / 1024.0) / rate_kbps if rate_kbps else 0.0

    async def gen():
        if size_mb > 0:
            chunks = _long_chunks(size_mb, limit)
        else:
            seed = _seed()
            chunks = (seed[i:i + CHUNK] for i in range(0, limit, CHUNK))
        for piece in chunks:
            yield piece
            if sleep_per_chunk:
                await asyncio.sleep(sleep_per_chunk)
        # If truncating, report the full Content-Length but close the body
        # early so the client observes an interrupted transfer.

    return StreamingResponse(gen(), media_type="video/mp2t", headers=headers)


@app.get("/vod.m3u8", response_class=PlainTextResponse)
def vod(size_mb: float = 0, rate_kbps: int = 0, truncate_at: int = 0,
        count: int = 3, delay_ms: int = 0):
    count = max(1, min(count, 50))
    q = _segment_query(delay_ms, "", size_mb, rate_kbps, truncate_at)
    return _playlist(list(range(1, count + 1)), endlist=True, query=q)


@app.get("/reuse.m3u8", response_class=PlainTextResponse)
def reuse():
    # invalid: every sequence maps to the same segment URL (address reuse)
    lines = [
        "#EXTM3U",
        "#EXT-X-VERSION:3",
        f"#EXT-X-TARGETDURATION:{max(1, round(SEGMENT_DURATION))}",
        "#EXT-X-MEDIA-SEQUENCE:1",
    ]
    for _ in range(3):
        lines.append(f"#EXTINF:{SEGMENT_DURATION:.3f},")
        lines.append("/seg/1.ts")
    return "\n".join(lines) + "\n"


@app.get("/master.m3u8", response_class=PlainTextResponse)
def master():
    return ("#EXTM3U\n#EXT-X-STREAM-INF:BANDWIDTH=800000,RESOLUTION=320x240\n"
            "/live.m3u8\n")


@app.get("/enc.m3u8", response_class=PlainTextResponse)
def enc():
    return _playlist([1, 2, 3], extra_lines=[
        '#EXT-X-KEY:METHOD=AES-128,URI="/key.bin",IV=0x00000000000000000000000000000001'
    ])


@app.get("/disc.m3u8", response_class=PlainTextResponse)
def disc():
    seqs = [1, 2, 3]
    lines = [
        "#EXTM3U",
        "#EXT-X-VERSION:3",
        f"#EXT-X-TARGETDURATION:{max(1, round(SEGMENT_DURATION))}",
        "#EXT-X-MEDIA-SEQUENCE:1",
        "#EXTINF:2.000,",
        "/seg/1.ts",
        "#EXT-X-DISCONTINUITY",
        "#EXTINF:2.000,",
        "/seg/2.ts",
        "#EXTINF:2.000,",
        "/seg/3.ts",
    ]
    return "\n".join(lines) + "\n"


@app.get("/health")
def health():
    return {"ok": True, "now_seq": _now_seq()}


@app.get("/", response_class=PlainTextResponse)
def index():
    return (
        "fake live HLS source\n"
        "  /live.m3u8?delay_ms=0&missing=&size_mb=&rate_kbps=&truncate_at=&jump_at=&jump_by=\n"
        "  /vod.m3u8 /master.m3u8 /enc.m3u8 /disc.m3u8\n"
    )
