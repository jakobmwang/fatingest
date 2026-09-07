"""Page-level PDF engine on PDFium (pypdfium2), with LiteParse for markdown.

Two tools, each doing the one thing it does best:

  pypdfium2  opens the document, decides what a page provably contains (the union area of
             everything drawn that is not text - images, vector graphics, and the annotations
             a viewer draws: notes, stamps, ink, form fields), renders it at a pinned width,
             and reads its text layer character by character together with the links
  LiteParse  turns a page that has nothing drawn on it into markdown, headings, lists and
             links included

Two routes per page, decided by what the page provably contains. A page whose drawn share is
at most VLM_VISUAL_THRESHOLD (default 0: nothing drawn at all) has nothing but its text
layer, so the text layer IS the page: LiteParse reads it (everything kept: headers, footers,
small text, links) and the chunk's text is deterministic (meta.source "text_layer"); such a
page with no text either is blank without asking anyone. Every other page goes to the VLM
with its render and the text layer as spelling support (meta.source "vlm"). A guard compares
the text layer's tokens with LiteParse's output and hands the page to the VLM if anything is
missing - the cheap route may miss structure, never content. The render is kept and embedded
for every page either way.

Verdicts are per page, never per document: a page the VLM refuses, or one that cannot be
rendered, becomes a chunk with empty text and meta.status "unparseable"; a page with nothing
on it becomes one with meta.status "blank". Positions are therefore always page positions.
Only a document that cannot be opened at all is unparseable as a whole (UnparseableError
from extract_pages).

Transcription goes through one pool for the whole process, VLM_CONCURRENCY pages in flight
whatever file or delivery they belong to, and, given a cache, remembers every finished page
under its chunk identity before moving on - so a delivery deferred halfway through
(RetryLater) resumes with the pages it lacks, not from the start.

Links reach a chunk by two paths and mean the same thing on both: a reference to another
member of the delivery becomes its content address (sha256://<sha>), everything else stays
as written. On the text-layer route the targets are rewritten in LiteParse's markdown. On
the VLM route each anchor is handed over marked as [anchor](L1) and the mark is exchanged
for the real target after generation - an address is exact or it is worthless, so it never
passes through a generative model.

The prompts, the blank token, the visual threshold, the render width and the versions of
the two tools above are all covered by the pipeline's PARSER_VERSION (routing.py): changing
any of them is a deliberate re-index event.
"""
import base64
import collections
import ctypes
import hashlib
import io
import math
import json
import os
import re
import threading
from concurrent.futures import ThreadPoolExecutor, as_completed, wait

import httpx
import liteparse
import pypdfium2 as pdfium
from pypdfium2 import raw as R

RENDER_WIDTH = 1240          # pixels; the scale is per page, so every render is this wide
_VISUAL_GRID = 200           # cells across the width: the visual share is exact to the cell
# Everything that puts marks on a page and is not text. A form is a container: its own box
# is in page coordinates and encloses everything it draws, while the boxes of the objects
# INSIDE it are in the form's own coordinate system - so the container is counted and never
# descended into.
_DRAWN = (R.FPDF_PAGEOBJ_PATH, R.FPDF_PAGEOBJ_IMAGE, R.FPDF_PAGEOBJ_SHADING, R.FPDF_PAGEOBJ_FORM)
# Annotations the renderer never draws: a link is an invisible click area, a popup is the
# note window a viewer opens on demand. Everything else - notes, stamps, ink, free text,
# form fields - is drawn, unless flagged hidden or no-view.
_UNDRAWN_ANNOTS = (R.FPDF_ANNOT_LINK, R.FPDF_ANNOT_POPUP)
_UNDRAWN_FLAGS = R.FPDF_ANNOT_FLAG_HIDDEN | R.FPDF_ANNOT_FLAG_NOVIEW
# pdfium's marker for a hyphen it removed where a word was broken at a line break: the word
# is handed over joined, as a reader sees it. The character API reports it as 0x02, the
# text API as U+FFFE; both stand for "nothing here".
_SOFT_BREAK = frozenset("\x02\ufffe")


class UnparseableError(Exception):
    """The content cannot be parsed under this recipe: corrupt, encrypted, not what it claims
    to be - or refused by a healthy converter. Stored as complete possession of nothing; the
    verdict is per PARSER_VERSION, so a recipe change re-parses it."""


class RetryLater(Exception):
    """The dependency, not the content: unreachable, unhealthy, overloaded, refusing us.
    Nothing is decided about the document; the delivery is deferred and retried with
    backoff by the parse worker."""


def classify(service: str, r: httpx.Response) -> None:
    """Turns a non-2xx answer into the right exception. 401/403 (refusing US), 408/429 and
    5xx are the service's problem: RetryLater. Any other 4xx is the service's deterministic
    verdict on this request: UnparseableError."""
    if r.is_success:
        return
    detail = f"{service} {r.status_code}: {r.text[:120]}"
    if r.status_code in (401, 403, 408, 429) or r.status_code >= 500:
        raise RetryLater(detail)
    raise UnparseableError(detail)


def _visual_threshold() -> float:
    """VLM_VISUAL_THRESHOLD: the largest drawn share a page may have and still be read from
    its text layer alone. 0 (the default) means exactly nothing drawn; anything above it is
    the operator's explicit decision that marks up to that share of the page may go
    undescribed. Part of the recipe."""
    raw = os.environ.get("VLM_VISUAL_THRESHOLD", "0").strip() or "0"
    try:
        value = float(raw)
    except ValueError:
        raise ValueError(f"VLM_VISUAL_THRESHOLD must be a number between 0 and 1, not {raw!r}")
    if not 0.0 <= value <= 1.0:
        raise ValueError(f"VLM_VISUAL_THRESHOLD must be between 0 and 1, not {raw!r}")
    return value


VLM_URL = os.environ.get("VLM_URL", "").rstrip("/")
VLM_MODEL = os.environ.get("VLM_MODEL", "")
VLM_API_KEY = os.environ.get("VLM_API_KEY", "")   # optional; set when routing through a gateway
VLM_CONCURRENCY = max(1, int(os.environ.get("VLM_CONCURRENCY", "8")))   # pages in flight, process-wide
VLM_BLANK_TOKEN = os.environ.get("VLM_BLANK_TOKEN", "<blank>").strip() or "<blank>"
VLM_VISUAL_THRESHOLD = _visual_threshold()

# One pool for every VLM call this process makes, whichever delivery or file a page belongs
# to: VLM_CONCURRENCY is then exactly the load the process puts on the model, and a delivery
# of many small files fills the pool as well as one large file does.
_VLM_POOL = ThreadPoolExecutor(max_workers=VLM_CONCURRENCY, thread_name_prefix="vlm")

# One prompt for every image the VLM sees - a document page with its text layer as support,
# or a picture with none. It lives in a file so an operator can replace it (VLM_PROMPT_FILE)
# and must carry two placeholders: {support} for the extracted text and {blank} for the
# answer that means "nothing here", which is filled from VLM_BLANK_TOKEN so the instruction
# and the check can never disagree. The prompt is part of the recipe (PARSER_VERSION).
_PROMPT_DEFAULT = os.path.join(os.path.dirname(os.path.abspath(__file__)), "prompt.md")
VLM_PROMPT_FILE = os.environ.get("VLM_PROMPT_FILE", "").strip() or _PROMPT_DEFAULT


def _load_prompt(path: str) -> str:
    """The prompt template, refused at startup rather than at the first page if a placeholder
    is missing: a prompt without {blank} could never be checked, one without {support}
    would silently drop the text layer."""
    with open(path, encoding="utf-8") as f:
        text = f.read().strip()
    for placeholder in ("{support}", "{blank}"):
        if placeholder not in text:
            raise ValueError(f"VLM prompt {path} must contain the placeholder {placeholder}")
    return text


PROMPT = _load_prompt(VLM_PROMPT_FILE)


def prompt_for(support: str) -> str:
    """The prompt for one image: the template with the blank token and the (capped) support
    text filled in. Empty support for an image that has no text layer - a picture, a
    delivered render. The token goes in first, so a token-like string inside the extracted
    text is left alone."""
    return PROMPT.replace("{blank}", VLM_BLANK_TOKEN).replace("{support}", _clip(support))


_HTTP_RE = re.compile(r"^https?://", re.IGNORECASE)


def _sha(b: bytes) -> str:
    return hashlib.sha256(b).hexdigest()


def _resolve_uri(uri: str, member_path: str, members: dict[str, bytes] | None) -> str | None:
    """A link's target as an address the index can use: http(s) and sha256:// are already
    that; a relative or file: reference names another member of the same delivery and
    becomes its content address. None when it resolves to nothing. Both routes - the VLM's
    (L#) placeholders and the text layer's markdown links - rewrite through this one rule,
    so a link means the same thing whichever way the page was read."""
    if _HTTP_RE.match(uri) or uri.startswith("sha256://"):
        return uri
    if members is None:
        return None
    from refs import resolve_local_ref
    p = resolve_local_ref(member_path, re.sub(r"^file:(//)?", "", uri), members)
    return f"sha256://{_sha(members[p])}" if p else None


_MD_LINK_RE = re.compile(r"\]\(\s*<?([^)<>\s]+)>?\s*\)")
_MARK_RE = re.compile(r"\]\((L\d+)\)")


def _rewrite_md_links(md: str, member_path: str, members: dict[str, bytes] | None) -> str:
    """LiteParse writes every link with the target the document carries, so a reference to
    another member of the delivery arrives as the relative path it was written with. Each
    target goes through the same rule as everything else, which turns a reference to a
    member into its content address and leaves the rest - web addresses, mail addresses,
    targets that resolve to nothing - exactly as written."""
    def one(m):
        return f"]({_resolve_uri(m.group(1), member_path, members) or m.group(1)})"
    return _MD_LINK_RE.sub(one, md)


# PDFium is not thread-safe, and both tools sit on it, so the deterministic half of a
# document is done under one lock. It costs milliseconds per page against seconds per page
# of transcription, which happens outside it.
_PDFIUM_LOCK = threading.Lock()


def _log(evt: str, **fields):
    print(json.dumps({"evt": evt, **fields}), flush=True)


def _uri_links(doc, page) -> list[tuple[str, tuple[float, float, float, float]]]:
    """The page's link annotations that carry a URI action: (target, box) with the box as
    (left, bottom, right, top) in page points, corners ordered whichever way the file wrote
    them. Internal jumps and file launches carry no address and are not links to anyone
    outside the document; they stay plain text."""
    out = []
    pos = ctypes.c_int(0)
    link = R.FPDF_LINK()
    while R.FPDFLink_Enumerate(page.raw, ctypes.byref(pos), ctypes.byref(link)):
        action = R.FPDFLink_GetAction(link)
        if not action or R.FPDFAction_GetType(action) != R.PDFACTION_URI:
            continue
        n = R.FPDFAction_GetURIPath(doc.raw, action, None, 0)
        buf = ctypes.create_string_buffer(n)
        R.FPDFAction_GetURIPath(doc.raw, action, buf, n)
        raw = buf.raw.rstrip(b"\x00")
        try:
            uri = raw.decode("utf-8")
        except UnicodeDecodeError:
            uri = raw.decode("latin-1")
        rect = R.FS_RECTF()
        if not uri or not R.FPDFLink_GetAnnotRect(link, ctypes.byref(rect)):
            continue
        left, right = sorted((rect.left, rect.right))
        bottom, top = sorted((rect.bottom, rect.top))
        out.append((uri, (left, bottom, right, top)))
    return out


def _read_text(doc, page, member_path: str, members: dict[str, bytes] | None) -> tuple[str, str, dict[str, str]]:
    """The page's text layer, read once and returned three ways: the plain text (for the
    blank verdict, the guard and text_chars), the same text with every link's anchor marked
    as [anchor](L1) for the VLM, and the map from mark to target.

    PDFium hands out the characters in the order it reads them, with its own line breaks,
    and a box for each; a link is a rectangle with a target. A character whose centre lies
    in a link's rectangle belongs to that link's anchor - so a link across three lines is
    three anchors carrying the same mark, and no anchor ever has to be matched to a target
    by anything but its place on the page. Whitespace between two characters of the same
    anchor belongs to the anchor too; a line break never does.

    A target never passes through the VLM: an address is exact or it is worthless, and one
    mutated character in a 64-hex content address destroys it. Anchors sharing a target
    share a mark. Targets are resolved the same way as everywhere else, so a reference to
    another member of the delivery becomes its content address."""
    tp = page.get_textpage()
    try:
        n = tp.count_chars()
        chars = [chr(R.FPDFText_GetUnicode(tp.raw, i)) for i in range(n)]
        owner: list[str | None] = [None] * n
        links = _uri_links(doc, page)
        if links:
            for i, c in enumerate(chars):
                if c in "\r\n":
                    continue
                left, bottom, right, top = tp.get_charbox(i)
                cx, cy = (left + right) / 2, (bottom + top) / 2
                for uri, (l, b, r, t) in links:
                    if l <= cx <= r and b <= cy <= t:
                        owner[i] = uri
                        break
            # whitespace between two characters of the same anchor (a zero-width space has
            # no centre to fall inside the box) belongs to the anchor
            i = 0
            while i < n:
                if owner[i] is None and chars[i].isspace() and chars[i] not in "\r\n" and i > 0 and owner[i - 1]:
                    j = i
                    while j < n and owner[j] is None and chars[j].isspace() and chars[j] not in "\r\n":
                        j += 1
                    if j < n and owner[j] == owner[i - 1]:
                        for k in range(i, j):
                            owner[k] = owner[i - 1]
                    i = j
                else:
                    i += 1
    finally:
        tp.close()

    plain: list[str] = []
    support: list[str] = []
    labels: dict[str, str] = {}
    marks: dict[str, str] = {}
    i = 0
    while i < n:
        if owner[i] is None:
            if chars[i] not in _SOFT_BREAK:
                plain.append(chars[i])
                support.append(chars[i])
            i += 1
            continue
        j = i
        while j < n and owner[j] == owner[i]:
            j += 1
        anchor = "".join(c for c in chars[i:j] if c not in _SOFT_BREAK)
        plain.append(anchor)
        core = anchor.strip()
        if core:
            target = _resolve_uri(owner[i], member_path, members) or owner[i]
            mark = marks.get(target)
            if mark is None:
                mark = f"L{len(marks) + 1}"      # counted from 1: L0 is the example in the prompt
                                                 # and is never a real address
                marks[target] = mark
                labels[mark] = target
            lead = anchor[:len(anchor) - len(anchor.lstrip())]
            trail = anchor[len(anchor.rstrip()):]
            support.append(f"{lead}[{core.replace('[', chr(92) + '[').replace(']', chr(92) + ']')}]({mark}){trail}")
        else:
            support.append(anchor)
        i = j
    text = "".join(plain).replace("\r\n", "\n").replace("\r", "\n")
    marked = "".join(support).replace("\r\n", "\n").replace("\r", "\n")
    return text, marked, labels


_TRUNCATED_LINK_RE = re.compile(r"\[[^\[\]]*\](\(L?\d*)?$")


def _clip(support: str, limit: int = 20000) -> str:
    """The support text is capped, and a cut must not leave half a mark behind."""
    if len(support) <= limit:
        return support
    return _TRUNCATED_LINK_RE.sub("", support[:limit])


def _text_layer_markdown(pdf: bytes, n_pages: int) -> dict[int, str]:
    """LiteParse markdown per page (0-based) with everything kept - running headers and
    footers, very small text, links as [text](target). Empty when LiteParse fails on the
    document, in which case every page falls through to the VLM."""
    try:
        lp = liteparse.LiteParse(output_format="markdown", ocr_enabled=False, quiet=True,
                                 num_workers=1, preserve_very_small_text=True,
                                 keep_headers_footers=True, extract_links=True)
        try:
            res = lp.parse(pdf)
            return {i: (res.get_page(i + 1).markdown or "") for i in range(min(res.num_pages, n_pages))}
        finally:
            lp.close()
    except Exception as e:
        _log("text_layer_failed", tool="liteparse", err=f"{type(e).__name__}: {str(e)[:160]}")
        return {}


_WORD_RE = re.compile(r"\w+")     # Unicode by default: every alphabet counts as letters


def _fold(s: str) -> str:
    """Soft hyphens are invisible in the render and meaningless to a reader; case is not a
    difference either. Both halves of the guard fold the same way."""
    return s.replace("\xad", "").casefold()


def _tokens(s: str) -> collections.Counter:
    return collections.Counter(_WORD_RE.findall(_fold(s)))


def _complete(markdown: str, text: str) -> bool:
    """The guard behind the text-layer route: every token of the page's text layer must
    occur in the markdown, if not as a token then as a substring of its letters - a line
    break inside a hyphenated word, or a column joined into a paragraph, moves token
    boundaries but never letters. Anything still missing sends the page to the VLM
    instead: the cheap route may miss structure, never content."""
    missing = _tokens(text) - _tokens(markdown)
    if not missing:
        return True
    letters = "".join(_WORD_RE.findall(_fold(markdown)))
    return all(w in letters for w in missing)


def _render(page) -> bytes:
    """A PNG of the page at a pinned width, the same for every page of every document.
    Annotations are drawn, and form fields with them (the document has a form environment,
    see extract_pages): the render shows the page as a viewer shows it."""
    pil = page.render(scale=RENDER_WIDTH / page.get_width()).to_pil()
    buf = io.BytesIO()
    pil.save(buf, "PNG")
    return buf.getvalue()


def _annotation_boxes(page) -> list[tuple[float, float, float, float] | None]:
    """The boxes of the annotations the renderer draws - notes, stamps, ink, free text, form
    fields - as (left, bottom, right, top). Links and popups are never drawn, nor is anything
    flagged hidden or no-view, so they are not marks on the page. None stands for an
    annotation whose box cannot be read."""
    boxes: list[tuple[float, float, float, float] | None] = []
    for k in range(R.FPDFPage_GetAnnotCount(page.raw)):
        annot = R.FPDFPage_GetAnnot(page.raw, k)
        if not annot:
            boxes.append(None)
            continue
        try:
            if R.FPDFAnnot_GetSubtype(annot) in _UNDRAWN_ANNOTS or R.FPDFAnnot_GetFlags(annot) & _UNDRAWN_FLAGS:
                continue
            rect = R.FS_RECTF()
            if R.FPDFAnnot_GetRect(annot, ctypes.byref(rect)):
                boxes.append((min(rect.left, rect.right), min(rect.bottom, rect.top),
                              max(rect.left, rect.right), max(rect.bottom, rect.top)))
            else:
                boxes.append(None)
        finally:
            R.FPDFPage_CloseAnnot(annot)
    return boxes


def _visual_share(page) -> float:
    """Share of the page area covered by the union of everything drawn that is not text -
    images, vector graphics, and the annotations a viewer draws - so overlapping marks count
    once and nothing is excluded or guessed at. A hint to a reader whether the render adds
    anything to the text, and the one thing the routing turns on (VLM_VISUAL_THRESHOLD):
    exactly 0 means the page holds nothing but its text layer. Marks are counted on a grid,
    which is what makes the union cheap; the number is not rounded beyond the grid, so a
    single small mark is a small number, never zero.

    A mark that cannot be measured, or a page whose contents cannot be listed at all, is
    reported as a full page rather than as none: the number is a hint and may be wrong by a
    margin, but "nothing is drawn here" is a promise the text-layer route relies on, and it
    is never made on missing evidence."""
    w, h = page.get_width(), page.get_height()
    if w <= 0 or h <= 0:
        return 0.0
    boxes: list[tuple[float, float, float, float] | None] = []
    try:
        for obj in page.get_objects(max_depth=1):
            if obj.type not in _DRAWN:
                continue
            try:
                boxes.append(obj.get_bounds())            # PDF counts up from the bottom
            except Exception:
                boxes.append(None)
        boxes += _annotation_boxes(page)
    except Exception:
        return 1.0
    nx = _VISUAL_GRID
    ny = max(1, round(nx * h / w))
    covered = bytearray(nx * ny)
    unmeasured = False
    for box in boxes:
        if box is None:
            unmeasured = True
            continue
        x0, x1 = sorted((box[0], box[2]))
        y0, y1 = sorted((box[1], box[3]))
        a = max(0, min(nx, int(x0 / w * nx)))
        b = max(0, min(nx, math.ceil(x1 / w * nx)))
        c = max(0, min(ny, int((h - y1) / h * ny)))
        d = max(0, min(ny, math.ceil((h - y0) / h * ny)))
        for row in range(c, d):
            covered[row * nx + a:row * nx + b] = b"\x01" * max(0, b - a)
    share = sum(covered) / (nx * ny)
    if share == 0.0 and unmeasured:
        return 1.0
    return share


RENDER_HEIGHT = 1754         # a region is scaled to fit this box (the size of an A4 page render)
_REGION_GAP = 6.0            # points; drawn objects closer than this are one drawing (a chart is
                             # hundreds of bars, lines and ticks a few points apart)


def _cluster(boxes: list[tuple[float, float, float, float]]) -> list[list[float]]:
    """Boxes that overlap or lie within _REGION_GAP of each other merge, until nothing merges;
    top of the page first."""
    pending = [list(b) for b in boxes]
    merged = True
    while merged:
        merged = False
        out: list[list[float]] = []
        while pending:
            a = pending.pop()
            i = 0
            while i < len(out):
                b = out[i]
                if (a[0] <= b[2] + _REGION_GAP and b[0] <= a[2] + _REGION_GAP
                        and a[1] <= b[3] + _REGION_GAP and b[1] <= a[3] + _REGION_GAP):
                    a = [min(a[0], b[0]), min(a[1], b[1]), max(a[2], b[2]), max(a[3], b[3])]
                    out.pop(i)
                    merged = True
                else:
                    i += 1
            out.append(a)
        pending = out
    return sorted(pending, key=lambda b: (-b[3], b[0]))


def drawn_regions(pdf: bytes) -> list[list[tuple[bytes, list[float]]]]:
    """Per page, every drawing rendered alone: (png, [left, bottom, right, top] in points).
    A drawing is a cluster of drawn objects - images, vector graphics, forms, annotations -
    and text is not one; a chart's labels come along because they sit inside its box. The
    render is cut to the region with a small margin and scaled to fit RENDER_WIDTH x
    RENDER_HEIGHT, so a chart shrunk onto a giant single-sheet page is rendered as large as
    a chart on a small one. Nothing is filtered by size: a stray line is a region too, and
    the VLM's verdict on it is the verdict."""
    margin = 4.0
    pages: list[list[tuple[bytes, list[float]]]] = []
    with _PDFIUM_LOCK:
        doc = pdfium.PdfDocument(pdf)
        try:
            for pno in range(len(doc)):
                page = doc[pno]
                try:
                    w, h = page.get_width(), page.get_height()
                    boxes = []
                    for obj in page.get_objects(max_depth=1):
                        if obj.type in _DRAWN:
                            x0, y0, x1, y1 = obj.get_bounds()
                            boxes.append((min(x0, x1), min(y0, y1), max(x0, x1), max(y0, y1)))
                    boxes += [b for b in _annotation_boxes(page) if b is not None]
                    regions = []
                    for x0, y0, x1, y1 in _cluster(boxes):
                        x0, y0 = max(0.0, x0 - margin), max(0.0, y0 - margin)
                        x1, y1 = min(w, x1 + margin), min(h, y1 + margin)
                        if x1 <= x0 or y1 <= y0:
                            continue
                        scale = min(RENDER_WIDTH / (x1 - x0), RENDER_HEIGHT / (y1 - y0))
                        pil = page.render(scale=scale, crop=(x0, y0, w - x1, h - y1)).to_pil()
                        buf = io.BytesIO()
                        pil.save(buf, "PNG")
                        regions.append((buf.getvalue(), [round(x0, 1), round(y0, 1), round(x1, 1), round(y1, 1)]))
                    pages.append(regions)
                finally:
                    page.close()
        finally:
            doc.close()
    return pages


def extract_pages(pdf: bytes, member_path: str = "",
                  members: dict[str, bytes] | None = None) -> list[dict]:
    """Deterministic half: one item per page for transcribe_pages. A page with graphics is a
    VLM item {render, prompt, labels, meta}; a page without graphics is resolved here -
    {render, markdown, meta} from the text layer, or blank when it has no text either; a
    page that cannot be rendered is {error} (the verdict stays with that page; the document
    as a whole is unparseable only when it cannot be opened)."""
    with _PDFIUM_LOCK:
        try:
            doc = pdfium.PdfDocument(pdf)
            n_pages = len(doc)
        except Exception as e:                # corrupt, encrypted, or not a PDF at all
            detail = str(e)
            if "password" in detail.lower():
                raise UnparseableError("encrypted PDF")
            raise UnparseableError(f"cannot open PDF: {detail[:120]}")
        try:
            try:
                doc.init_forms()              # without a form environment PDFium draws no form fields
            except Exception as e:
                _log("forms_init_failed", err=f"{type(e).__name__}: {str(e)[:160]}")
            markdown: dict[int, str] | None = None
            pages: list[dict] = []
            for pno in range(n_pages):
                try:
                    page = doc[pno]
                except Exception as e:
                    pages.append({"error": f"cannot load page: {type(e).__name__}: {str(e)[:120]}"})
                    continue
                try:
                    visual = _visual_share(page)
                    render = _render(page)
                    meta: dict = {"visual": visual}
                    try:
                        text, support, labels = _read_text(doc, page, member_path, members)
                        meta["text_chars"] = len(text)
                    except Exception as e:    # unread is not empty: the VLM takes the page, unsupported
                        _log("text_layer_failed", tool="pdfium", page=pno + 1, err=f"{type(e).__name__}: {str(e)[:160]}")
                        text, support, labels = None, "", {}
                    if text is not None and visual <= VLM_VISUAL_THRESHOLD:
                        if not text.strip():
                            pages.append({"render": render, "markdown": "",
                                          "meta": {**meta, "status": "blank", "source": "text_layer"}})
                            continue
                        if markdown is None:
                            markdown = _text_layer_markdown(pdf, n_pages)
                        md = markdown.get(pno, "").strip()
                        if md and _complete(md, text):
                            pages.append({"render": render,
                                          "markdown": _rewrite_md_links(md, member_path, members),
                                          "meta": {**meta, "status": "ok", "source": "text_layer"}})
                            continue
                        _log("text_layer_incomplete", page=pno + 1)   # the VLM takes this page
                    pages.append({"render": render, "meta": meta, "labels": labels,
                                  "prompt": prompt_for(support)})
                except Exception as e:
                    pages.append({"error": f"cannot render page: {type(e).__name__}: {str(e)[:120]}"})
                finally:
                    page.close()
            return pages
        finally:
            doc.close()


def vlm(image_png: bytes, prompt: str) -> str:
    """One multimodal completion against the configured VLM (temperature 0)."""
    body = {"model": VLM_MODEL, "temperature": 0, "max_tokens": 4000,
            "messages": [{"role": "user", "content": [
                {"type": "image_url", "image_url": {"url": "data:image/png;base64,"
                                                    + base64.b64encode(image_png).decode()}},
                {"type": "text", "text": prompt}]}]}
    headers = {"Authorization": f"Bearer {VLM_API_KEY}"} if VLM_API_KEY else {}
    try:
        r = httpx.post(f"{VLM_URL}/v1/chat/completions", json=body, headers=headers, timeout=300)
    except httpx.TransportError as e:
        raise RetryLater(f"vlm unreachable: {type(e).__name__}")
    # Behind a gateway a 5xx may mean the model is down, not that this page is bad, so
    # 5xx stays retryable here (unlike Gotenberg, which is local and health-checked first).
    classify("vlm", r)
    try:
        md = r.json()["choices"][0]["message"]["content"].strip()
    except (ValueError, KeyError, IndexError, TypeError) as e:
        raise RetryLater(f"vlm malformed response: {type(e).__name__}")
    # strip the common "wrap everything in a code fence" tic
    if md.startswith("```"):
        md = re.sub(r"^```[a-zA-Z]*\n", "", md)
        md = re.sub(r"\n```$", "", md).strip()
    return md


def transcribe(render_png: bytes, prompt: str, labels: dict[str, str] | None = None) -> tuple[str, dict]:
    """One render -> (markdown, meta) with the verdict in meta.status: 'ok', 'blank' (the
    model answered VLM_BLANK_TOKEN, or nothing) or 'unparseable' (a healthy VLM refused this
    request - deterministic under this recipe, so it is a result, not an error). RetryLater
    propagates: the dependency's failure decides nothing. Link marks the model carried
    through are exchanged for their real targets here; a mark it invented is left alone."""
    try:
        md = vlm(render_png, prompt)
    except UnparseableError as e:
        return "", {"status": "unparseable", "error": str(e)[:200]}
    if md == VLM_BLANK_TOKEN or not md:
        return "", {"status": "blank"}
    if labels:
        md = _MARK_RE.sub(lambda m: f"]({labels.get(m.group(1), m.group(1))})", md)
    return md, {"status": "ok"}


def transcribe_pages(items: list[dict], cache=None) -> list[dict]:
    """Generative half for a list of renders, in order: [{markdown, render, meta}].
    An item is {render, prompt, labels?, meta?} for the VLM, {render, markdown, meta} when
    already resolved (text layer, blank by geometry), or {error} (an unrenderable page).

    `cache` remembers transcriptions under their chunk identity: get(render) -> (markdown,
    meta) | None and put(render, markdown, meta). Lookups happen first, one by one; misses
    go into the process-wide pool (identical renders once) and are stored the moment they
    finish. A RetryLater from any page lets this call's pages in flight complete into the
    cache, withdraws the ones not started and propagates - the retry then transcribes only
    what is still missing. The pool itself serves everyone else meanwhile."""
    out: list[dict | None] = [None] * len(items)
    todo: dict[str, list[int]] = {}
    for i, it in enumerate(items):
        if it.get("error"):
            out[i] = {"markdown": "", "render": None, "meta": {"status": "unparseable", "error": it["error"]}}
            continue
        if "markdown" in it:                     # resolved without the VLM: deterministic, not memoized
            out[i] = {"markdown": it["markdown"], "render": it["render"], "meta": it["meta"]}
            continue
        hit = cache.get(it["render"]) if cache else None
        if hit:
            out[i] = {"markdown": hit[0], "render": it["render"], "meta": hit[1]}
        else:
            todo.setdefault(_sha(it["render"]), []).append(i)

    def one(idxs: list[int]) -> tuple[list[int], str, dict]:
        it = items[idxs[0]]
        md, meta = transcribe(it["render"], it["prompt"], it.get("labels"))
        meta = {**meta, **(it.get("meta") or {}), "source": "vlm"}
        if cache:
            cache.put(it["render"], md, meta)
        return idxs, md, meta

    futures = [_VLM_POOL.submit(one, idxs) for idxs in todo.values()]
    try:
        for fut in as_completed(futures):
            idxs, md, meta = fut.result()          # RetryLater surfaces here
            for i in idxs:
                out[i] = {"markdown": md, "render": items[i]["render"], "meta": meta}
    except BaseException:
        for fut in futures:
            fut.cancel()                           # not started: withdrawn
        wait(futures)                              # in flight: finish, and reach the cache
        raise
    return out  # type: ignore[return-value]


def parse_pdf(pdf: bytes, member_path: str = "", members: dict[str, bytes] | None = None,
              cache=None) -> list[dict]:
    """Full engine: one chunk {markdown, render, meta} per page, in page order."""
    return transcribe_pages(extract_pages(pdf, member_path, members), cache)
