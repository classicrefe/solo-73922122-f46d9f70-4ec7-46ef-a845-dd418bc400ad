"""Strict HLS media-playlist parsing.

Only single-layer (media, not master), unencrypted, discontinuity-free,
MPEG-TS playlists are accepted. Anything else raises PlaylistError.
"""

from dataclasses import dataclass, field
from urllib.parse import urljoin, urlparse


class PlaylistError(Exception):
    """The playlist violates the accepted subset."""


@dataclass
class SegmentInfo:
    seq: int
    url: str
    duration: float
    title: str


@dataclass
class MediaPlaylist:
    media_sequence: int
    target_duration: float
    segments: list = field(default_factory=list)  # list[SegmentInfo]
    endlist: bool = False


def parse_media_playlist(text: str, base_url: str) -> MediaPlaylist:
    lines = [ln.strip() for ln in text.splitlines() if ln.strip()]
    if not lines or lines[0] != "#EXTM3U":
        raise PlaylistError("不是合法的 M3U8 清单")

    for ln in lines:
        if ln.startswith("#EXT-X-STREAM-INF") or ln.startswith("#EXT-X-I-FRAME-STREAM-INF"):
            raise PlaylistError("只接受单层媒体清单，不接受 master/多码率清单")

    media_sequence = None
    target_duration = None
    segments: list = []
    endlist = False
    pending_inf = None
    seen_urls = set()

    for ln in lines[1:]:
        if ln.startswith("#"):
            if ln.startswith("#EXT-X-MEDIA-SEQUENCE:"):
                try:
                    media_sequence = int(ln.split(":", 1)[1].strip())
                except ValueError:
                    raise PlaylistError("EXT-X-MEDIA-SEQUENCE 非法")
            elif ln.startswith("#EXT-X-TARGETDURATION:"):
                try:
                    target_duration = float(ln.split(":", 1)[1].strip())
                except ValueError:
                    raise PlaylistError("EXT-X-TARGETDURATION 非法")
            elif ln.startswith("#EXT-X-KEY:"):
                attrs = ln.split(":", 1)[1]
                if "METHOD=NONE" not in attrs:
                    raise PlaylistError("只接受未加密清单（发现 EXT-X-KEY）")
            elif ln.startswith("#EXT-X-DISCONTINUITY"):
                raise PlaylistError("不接受带不连续标记（EXT-X-DISCONTINUITY）的清单")
            elif ln.startswith("#EXT-X-MAP:"):
                raise PlaylistError("只接受 MPEG-TS 清单（发现 EXT-X-MAP/fMP4）")
            elif ln.startswith("#EXTINF:"):
                body = ln[len("#EXTINF:"):]
                dur, _, title = body.partition(",")
                try:
                    pending_inf = (float(dur.strip()), title.strip())
                except ValueError:
                    raise PlaylistError("EXTINF 时长非法")
            elif ln.startswith("#EXT-X-ENDLIST"):
                endlist = True
            # other tags are ignored
        else:
            if pending_inf is None:
                raise PlaylistError(f"片段缺少 EXTINF: {ln}")
            url = urljoin(base_url, ln)
            path = urlparse(url).path.lower()
            if not path.endswith(".ts"):
                raise PlaylistError(f"只接受 MPEG-TS 片段，非法片段地址: {ln}")
            if url in seen_urls:
                raise PlaylistError(f"清单内片段地址复用: {url}")
            seen_urls.add(url)
            dur, title = pending_inf
            segments.append(SegmentInfo(seq=0, url=url, duration=dur, title=title))
            pending_inf = None

    if media_sequence is None:
        raise PlaylistError("缺少 EXT-X-MEDIA-SEQUENCE，无法判定连续性")
    if not segments:
        raise PlaylistError("清单内没有片段")
    if target_duration is None or target_duration <= 0:
        target_duration = max(s.duration for s in segments)

    for i, s in enumerate(segments):
        s.seq = media_sequence + i
    return MediaPlaylist(
        media_sequence=media_sequence,
        target_duration=target_duration,
        segments=segments,
        endlist=endlist,
    )
