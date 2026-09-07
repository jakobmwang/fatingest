"""Text-side utilities: encoding normalization, markdown splitting and link hygiene."""
import hashlib
import re

from refs import resolve_local_ref

BARE_URL_RE = re.compile(r'(?<!\]\()(?<!\()(?<!")(https?://[^\s\)\]>"]+)', re.IGNORECASE)
MD_REF_RE = re.compile(r'(!?\[[^\]]*\]\()([^)\s]+)(\))')
SEPARATORS = [r'\n(?=#)', r'\n\n', r'\n', r'\. ', r' ']


def normalize_text(data: bytes) -> bytes:
    """Try common encodings, normalize EOL, return clean UTF-8 bytes."""
    for encoding in ("utf-8", "latin-1", "cp1252", "iso-8859-1"):
        try:
            text = data.decode(encoding)
            return text.replace("\r\n", "\n").replace("\r", "\n").encode("utf-8")
        except Exception:
            continue
    return data


def linkify_urls(markdown: str) -> str:
    """Convert bare URLs to markdown links so they survive as anchors."""
    return BARE_URL_RE.sub(r'[\1](\1)', markdown)


def rewrite_md_refs(member_path: str, markdown: str, members: dict[str, bytes]) -> str:
    """Rewrite relative markdown refs (links and images) that resolve inside the
    delivery to their content-addressed sha256:// form; leave everything else as-is."""
    def sub(m: re.Match) -> str:
        target = resolve_local_ref(member_path, m.group(2), members)
        if target is None:
            return m.group(0)
        return m.group(1) + "sha256://" + hashlib.sha256(members[target]).hexdigest() + m.group(3)
    return MD_REF_RE.sub(sub, markdown)


def split_markdown(text: str, target_size: int = 4000) -> list[str]:
    """Split on increasingly fine separators, then merge back up to target size."""
    if len(text) <= target_size:
        return [text] if text.strip() else []
    chunks = [text]
    for sep in SEPARATORS:
        result = []
        for chunk in chunks:
            if len(chunk) <= target_size:
                result.append(chunk)
            else:
                result.extend(re.split(f'({sep})', chunk))
        chunks = result
    merged, current = [], ""
    for chunk in chunks:
        if current and len(current) + len(chunk) > target_size:
            merged.append(current.strip())
            current = chunk
        else:
            current = current + chunk if current else chunk
    if current.strip():
        merged.append(current.strip())
    return merged
