"""Content-addressed render store, parse spool, synthetic renders and the embedding call.

Renders are stored by sha256 of their bytes, fanned out to keep directories small
(renders/ab/cd/<sha256>). A synthetic render is a deterministic typographic drawing
of markdown used as embedding input when a chunk has no delivered render: the index
must stay modality-homogeneous (every embedding input is image + text), because mixing
modalities in one index degrades retrieval badly. Synthetic renders are never stored
and never enter an identity; RENDERER_VERSION is pinned so that changing the drawing
is a deliberate re-embedding event.
"""
import base64
import hashlib
import io
import json
import os
import textwrap
import urllib.request

import filetype
from PIL import Image, ImageDraw, ImageFont

STORE = os.environ.get("STORE", "/store")
EMBED_URL = os.environ["EMBED_URL"].rstrip("/") + "/v1/embeddings"  # OpenAI-compatible, multimodal
EMBED_API_KEY = os.environ.get("EMBED_API_KEY", "")                 # optional bearer for EMBED_URL
EMBED_MODEL = os.environ.get("EMBED_MODEL", "Qwen/Qwen3-VL-Embedding-8B")
EMBED_DIM = int(os.environ.get("EMBED_DIM", "1024"))                # must match vector(N) in the schema
RENDERER_VERSION = "1"  # PIL A4 1240x1754, DejaVu 24/34px, wrap 88/62


def sha(b: bytes) -> str:
    return hashlib.sha256(b).hexdigest()


def render_path(s: str) -> str:
    return os.path.join(STORE, "renders", s[:2], s[2:4], s)


def store_render(data: bytes) -> str:
    """Write-once by content: returns the sha256, writes only if missing."""
    s = sha(data)
    path = render_path(s)
    if not os.path.exists(path):
        os.makedirs(os.path.dirname(path), exist_ok=True)
        tmp = path + ".tmp"
        with open(tmp, "wb") as f:
            f.write(data)
        os.replace(tmp, path)
    return s


def spool_path(s: str) -> str:
    """Delivered payloads waiting for the parse worker, content-addressed by the file
    identity they will become. Held only until parsed, then deleted; /v1/gc sweeps
    leftovers. This is the one place raw bytes touch disk, and only transiently."""
    return os.path.join(STORE, "spool", s)


def spool_write(s: str, data: bytes):
    path = spool_path(s)
    if not os.path.exists(path):
        os.makedirs(os.path.dirname(path), exist_ok=True)
        tmp = path + ".tmp"
        with open(tmp, "wb") as f:
            f.write(data)
        os.replace(tmp, path)


def spool_read(s: str) -> bytes | None:
    path = spool_path(s)
    if not os.path.exists(path):
        return None
    with open(path, "rb") as f:
        return f.read()


def spool_delete(s: str):
    try:
        os.remove(spool_path(s))
    except FileNotFoundError:
        pass


def load_render(s: str) -> bytes | None:
    path = render_path(s)
    if not os.path.exists(path):
        return None
    with open(path, "rb") as f:
        return f.read()


def is_image(data: bytes) -> bool:
    try:
        Image.open(io.BytesIO(data)).verify()
        return True
    except Exception:
        return False


_F_B = ImageFont.truetype("/usr/share/fonts/truetype/dejavu/DejaVuSans.ttf", 24)
_F_H = ImageFont.truetype("/usr/share/fonts/truetype/dejavu/DejaVuSans-Bold.ttf", 34)


def synth_render(md: str) -> bytes:
    """Deterministic typographic render of markdown (embedding input only, never identity)."""
    img = Image.new("RGB", (1240, 1754), "white")
    d = ImageDraw.Draw(img)
    y = 60
    for raw in md.splitlines():
        if y > 1694:
            break
        if not raw.strip():
            y += 14
            continue
        head = raw.lstrip().startswith("#")
        font, lh, width = (_F_H, 44, 62) if head else (_F_B, 32, 88)
        for line in textwrap.wrap(raw.lstrip("# ").strip(), width) or [""]:
            if y > 1694:
                break
            d.text((70, y), line, font=font, fill="black")
            y += lh
    buf = io.BytesIO()
    img.save(buf, "PNG")
    return buf.getvalue()


def embed(md: str, image: bytes) -> list[float]:
    """One multimodal embedding of (image + text) - always both."""
    ft = filetype.guess(image)
    mime = ft.mime if ft and ft.mime.startswith("image/") else "image/png"
    body = {"model": EMBED_MODEL, "dimensions": EMBED_DIM, "messages": [{
        "role": "user", "content": [
            {"type": "image_url", "image_url": {"url": f"data:{mime};base64,"
                                                + base64.b64encode(image).decode()}},
            {"type": "text", "text": md[:24000]}]}]}
    headers = {"Content-Type": "application/json"}
    if EMBED_API_KEY:
        headers["Authorization"] = "Bearer " + EMBED_API_KEY
    req = urllib.request.Request(EMBED_URL, data=json.dumps(body).encode(), headers=headers)
    with urllib.request.urlopen(req, timeout=300) as r:
        return json.load(r)["data"][0]["embedding"]
