"""Local fake live HLS source for testing the recorder.

Serves a sliding-window media playlist over a pre-generated MPEG-TS seed
segment. Query parameters on /live.m3u8 are propagated to segment URLs:

  delay_ms=N            sleep N ms before serving each segment (slow source)
  missing=7,9           return 404 for those sequence numbers (missing segments)
  jump_at=K&jump_by=J   once the clock reaches seq K, shift all sequence
                        numbers by J (window jumps over undiscovered numbers)

Also serves invalid playlists (/master.m3u8, /enc.m3u8, /disc.m3u8) and a
finite /vod.m3u8 for validation tests.
"""

import asyncio
import os
import time

from fastapi import FastAPI, HTTPException, Query
from fastapi.responses import FileResponse, PlainTextResponse

WINDOW_SIZE = int(os.environ.get("WINDOW_SIZE", "5"))
SEGMENT_DURATION = float(os.environ.get("SEGMENT_DURATION", "2"))
SEED_TS = os.environ.get("SEED_TS", "/srv/seed.ts")

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


def _playlist(seqs, extra_lines=None, endlist=False) -> str:
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
        lines.append(f"/seg/{s}.ts")
    if endlist:
        lines.append("#EXT-X-ENDLIST")
    return "\n".join(lines) + "\n"


@app.get("/live.m3u8", response_class=PlainTextResponse)
def live(delay_ms: int = 0, missing: str = "", jump_at: int = 0, jump_by: int = 0):
    cur = _now_seq()
    eff = cur + (jump_by if jump_at and cur >= jump_at else 0)
    first = max(1, eff - WINDOW_SIZE + 1)
    lines = [
        "#EXTM3U",
        "#EXT-X-VERSION:3",
        f"#EXT-X-TARGETDURATION:{max(1, round(SEGMENT_DURATION))}",
        f"#EXT-X-MEDIA-SEQUENCE:{first}",
    ]
    q = f"?delay_ms={delay_ms}&missing={missing}"
    for s in range(first, eff + 1):
        lines.append(f"#EXTINF:{SEGMENT_DURATION:.3f},")
        lines.append(f"/seg/{s}.ts{q}")
    return "\n".join(lines) + "\n"


@app.get("/seg/{seq}.ts")
async def seg(seq: int, delay_ms: int = 0, missing: str = ""):
    if seq in _parse_missing(missing):
        raise HTTPException(404, f"segment {seq} intentionally missing")
    if delay_ms > 0:
        await asyncio.sleep(delay_ms / 1000.0)
    return FileResponse(SEED_TS, media_type="video/mp2t",
                        headers={"Cache-Control": "no-store"})


@app.get("/vod.m3u8", response_class=PlainTextResponse)
def vod():
    return _playlist([1, 2, 3], endlist=True)


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
        "  /live.m3u8?delay_ms=0&missing=&jump_at=&jump_by=\n"
        "  /vod.m3u8 /master.m3u8 /enc.m3u8 /disc.m3u8\n"
    )
