"""Per-file routing: bytes in, chunks out.

Strictly single-file - the loop over a delivery's members lives in the parse
worker, and sibling context arrives only as the read-only `members` dict.

Binary types are detected by magic (filetype); text formats have no magic, so they
go through a try-chain: tabular -> html sniff -> markdown fallback. Everything
visual converges on the PDF engine (offices via LibreOffice, html via Chromium),
so pagination, renders, link handling and VLM transcription exist in exactly one
place. Returns (chunks, meta) where chunks is a list of {"markdown", "render", "meta"}
(render None for text chunks - the embed worker synthesizes one; meta carries the
per-chunk verdict, see pdf_engine.transcribe), or (None, meta) for content that cannot
be parsed at all.

PARSER_VERSION names the parsing recipe in effect: this code (routing, the prompt, render
scale, link rules, text splitting, tabular layout), the VLM model behind VLM_URL, and the
Gotenberg image and fonts. It is set by the operator (env PARSER_VERSION, any string - a
memorable name beats a number), recorded on every parsed file, and enters the identity of
every chunk whose text was generated (VLM transcription of a render). Changing it makes
sha256 pings answer 404, feeds re-deliver, files get fresh chunk collections and the old
ones are garbage-collected. Too many variables affect the output for the version to be
derived automatically, so a re-index is always a deliberate, named event.

Two kinds of non-result. UnparseableError is the content's fault under this recipe -
corrupt, encrypted, unknown, or refused by a healthy converter - and is stored as complete
possession of nothing. It is raised only for what fails as a whole; a single page the VLM
refuses is a verdict on that page (a chunk with meta.status "unparseable"), never on the
document. RetryLater is the dependency's fault - Gotenberg or the VLM unreachable,
unhealthy, overloaded - and decides nothing about the document: the parse worker defers
the delivery and retries it with backoff. Gotenberg gets exactly one conversion at a time
(the `gate`) and a health check first, so that its answer to a document is the document's
own: the memory budget and the timeout clock are never shared. The `cache` (see
pdf_engine.transcribe_pages) is what lets a deferred delivery resume from the page it
reached.
"""
import contextlib
import io
import os
import re
import time

import filetype
import httpx
from PIL import Image

from pdf_engine import RetryLater, UnparseableError, parse_pdf, prompt_for, transcribe_pages  # noqa: F401 (re-exported)
from refs import rewrite_for_gotenberg
from tabular import tabular_to_chunks, SPREADSHEET_EXTENSIONS
from text_utils import linkify_urls, normalize_text, rewrite_md_refs, split_markdown

GOTENBERG_URL = os.environ.get("GOTENBERG_URL", "").rstrip("/")
GOTENBERG_TIMEOUT = float(os.environ.get("GOTENBERG_TIMEOUT", "300"))       # must exceed GOTENBERG_API_TIMEOUT
GOTENBERG_HEALTH_WAIT = float(os.environ.get("GOTENBERG_HEALTH_WAIT", "60"))  # covers a LibreOffice restart
PARSER_VERSION = os.environ.get("PARSER_VERSION", "1").strip() or "1"

OFFICE_TEXT_EXTENSIONS = ("doc", "docx", "ppt", "pptx", "odt", "odp")
_HTML_SNIFF_RE = re.compile(r"<(!doctype|html)\b", re.IGNORECASE)
_TITLE_RE = re.compile(rb"<title[^>]*>(.*?)</title>", re.IGNORECASE | re.DOTALL)
MAX_IMAGE_DIM = 1600

def image_item(png: bytes) -> dict:
    """A transcribe_pages item for a standalone image or a delivered render without text:
    the same prompt as a document page, with nothing extracted to support it."""
    return {"render": png, "prompt": prompt_for("")}


def _text_chunks(parts: list[str]) -> list[dict]:
    """Deterministic text is always a complete, embeddable chunk."""
    return [{"markdown": p, "render": None, "meta": {"status": "ok"}} for p in parts]


def _no_gate(name: str):
    return contextlib.nullcontext()


def parse_file(path: str, data: bytes, members: dict[str, bytes],
               gate=_no_gate, cache=None) -> tuple[list[dict] | None, dict]:
    """`gate(name)` is a context manager serializing access to an external module across
    all workers and replicas (the parse worker supplies a database advisory lock). `cache`
    is the transcription memo of pdf_engine.transcribe_pages (the parse worker supplies one
    backed by the chunks table); None transcribes everything afresh."""
    ft = filetype.guess(data)

    if ft and ft.mime.startswith("image/"):
        png = _normalize_image(data)
        if png is None:
            return None, {"kind": "unparseable", "content_type": ft.mime}
        return transcribe_pages([image_item(png)], cache), {"kind": "image", "content_type": ft.mime}

    if ft and ft.extension in SPREADSHEET_EXTENSIONS:
        chunks, meta = tabular_to_chunks(data, ft.extension)
        return _text_chunks(chunks), meta

    if ft and ft.extension in OFFICE_TEXT_EXTENSIONS:
        pdf = _gotenberg("/forms/libreoffice/convert",
                         [("files", (f"document.{ft.extension}", data, ft.mime))], gate)
        return parse_pdf(pdf, path, members, cache), {"kind": "office", "content_type": ft.mime}

    if ft and ft.extension == "pdf":
        return parse_pdf(data, path, members, cache), {"kind": "pdf", "content_type": "application/pdf"}

    if ft:  # known binary type with no route
        return None, {"kind": "unparseable", "content_type": ft.mime}

    # --- text try-chain (no magic bytes to go on) ---
    # The latin-1 fallback in normalize_text can decode ANY byte sequence, so guard
    # against binary data first: NUL bytes or a high control-character ratio means
    # this was never text.
    text = normalize_text(data)
    ctrl = sum(b < 9 or 13 < b < 32 for b in text[:4096])
    if b"\x00" in text or (text and ctrl / min(len(text), 4096) > 0.10):
        return None, {"kind": "unparseable"}

    try:
        chunks, meta = tabular_to_chunks(text)
        return _text_chunks(chunks), meta
    except Exception:
        pass

    head = re.sub(rb"\s+", b" ", text[:2000])
    if _HTML_SNIFF_RE.search(head.decode("utf-8", "replace")):
        g_html, assets = rewrite_for_gotenberg(path, text, members)
        pdf = _gotenberg("/forms/chromium/convert/html",
                         [("files", ("index.html", g_html, "text/html"))]
                         + [("files", (n, b, "application/octet-stream")) for n, b in assets], gate)
        meta = {"kind": "html", "content_type": "text/html"}
        if (m := _TITLE_RE.search(text)):
            meta["title"] = m.group(1).decode("utf-8", "replace").strip()[:300]
        return parse_pdf(pdf, path, members, cache), meta

    try:
        md = text.decode("utf-8")
    except UnicodeDecodeError:
        return None, {"kind": "unparseable"}
    md = linkify_urls(rewrite_md_refs(path, md, members))
    return _text_chunks(split_markdown(md)), {"kind": "text", "content_type": "text/markdown"}


def _gotenberg(route: str, files: list, gate) -> bytes:
    """One conversion at a time, health first. A healthy Gotenberg that then refuses the
    document (any non-2xx, timeouts included) has given its verdict on the document:
    UnparseableError under this recipe. Unreachable or unhealthy is RetryLater."""
    with gate("gotenberg"):
        _await_healthy()
        try:
            r = httpx.post(GOTENBERG_URL + route, files=files, timeout=GOTENBERG_TIMEOUT)
        except httpx.TransportError as e:
            raise RetryLater(f"gotenberg unreachable: {type(e).__name__}")
        if not r.is_success:
            raise UnparseableError(f"gotenberg {r.status_code}: {r.text[:120]}")
        return r.content


def _await_healthy():
    """Polls /health until every module is up. Gotenberg restarts LibreOffice and Chromium
    after every conversion (compose), so a brief 'down' is normal; anything longer than
    GOTENBERG_HEALTH_WAIT is an outage."""
    deadline = time.monotonic() + GOTENBERG_HEALTH_WAIT
    last = "no answer"
    while True:
        try:
            r = httpx.get(GOTENBERG_URL + "/health", timeout=5)
            if r.is_success:
                return
            last = f"{r.status_code}: {r.text[:120]}"
        except httpx.TransportError as e:
            last = type(e).__name__
        if time.monotonic() >= deadline:
            raise RetryLater(f"gotenberg unhealthy: {last}")
        time.sleep(1)


def _normalize_image(data: bytes) -> bytes | None:
    """Deterministic PNG render of an image: RGB, capped dimensions."""
    try:
        img = Image.open(io.BytesIO(data)).convert("RGB")
    except Exception:
        return None
    img.thumbnail((MAX_IMAGE_DIM, MAX_IMAGE_DIM))
    buf = io.BytesIO()
    img.save(buf, "PNG")
    return buf.getvalue()
