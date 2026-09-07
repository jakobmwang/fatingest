"""Local reference resolution and rewriting for compound deliveries (archives).

A member of an archive may reference sibling members. Two kinds exist and are
distinguished by ELEMENT, not attribute name:

- embedding (img/src, srcset, script/src, source, link/href for CSS, media):
  becomes part of the member's rendered presentation
- navigation (a/href, area/href): points at another document

Resolution is pure path math against the member list (refs resolve relative to
the referencing member's own path; anything escaping the namespace, carrying a
scheme, or not present in the list resolves to None).

rewrite_for_gotenberg() prepares html for conversion: embedding refs become flat
"<sha256><ext>" filenames and the referenced members are returned as conversion
assets. Gotenberg stores assets by basename, so paths must be flattened; the
original extension is kept because Chromium infers MIME from it in file context
(stylesheets in particular). Navigation refs become "sha256://<sha256>" - the
content-addressed form in which all intra-delivery relationships are stored.
Chromium carries unknown schemes into PDF link annotations verbatim, where the
PDF engine picks them up as final targets. Unresolvable refs are left as-is and
degrade to plain anchor text downstream.

Member sha256s are content-derived and therefore known at unpack time, before any
parsing - rewriting has no ordering problem.
"""
import hashlib
import posixpath
import re

EMBED_TAGS = {"img", "script", "source", "link", "audio", "video", "track", "embed"}
NAV_TAGS = {"a", "area"}

_SCHEME_RE = re.compile(r"^[a-zA-Z][a-zA-Z0-9+.-]*:")
_ATTR_RE = re.compile(
    rb'(<\s*([a-zA-Z][a-zA-Z0-9]*)\b[^>]*?\b(src|href|srcset)\s*=\s*)(["\'])(.*?)\4',
    re.IGNORECASE | re.DOTALL)


def resolve_local_ref(member_path: str, ref: str, members: dict[str, bytes]) -> str | None:
    """Resolve a raw ref against the member namespace. Returns the member path or None."""
    if not ref or ref.startswith(("#", "//")) or _SCHEME_RE.match(ref):
        return None
    ref = ref.split("#", 1)[0].split("?", 1)[0]
    if not ref:
        return None
    target = posixpath.normpath(posixpath.join(posixpath.dirname(member_path), ref))
    if target.startswith("..") or target.startswith("/"):
        return None
    return target if target in members else None


def rewrite_for_gotenberg(member_path: str, html: bytes,
                          members: dict[str, bytes]) -> tuple[bytes, list[tuple[str, bytes]]]:
    """Flat asset names + only the members actually referenced by embedding elements."""
    assets: dict[str, bytes] = {}

    def map_ref(ref: str, embed: bool) -> str:
        target = resolve_local_ref(member_path, ref, members)
        if target is None:
            return ref
        sha = hashlib.sha256(members[target]).hexdigest()
        if not embed:
            return f"sha256://{sha}"
        flat = sha + posixpath.splitext(target)[1]
        assets[flat] = members[target]
        return flat

    def sub(m: re.Match) -> bytes:
        tag = m.group(2).decode().lower()
        attr = m.group(3).decode().lower()
        embed = tag in EMBED_TAGS
        if not embed and tag not in NAV_TAGS:
            return m.group(0)
        val = m.group(5).decode("utf-8", "replace")
        if attr == "srcset":
            out = []
            for cand in val.split(","):
                cand = cand.strip()
                if not cand:
                    continue
                bits = cand.split(None, 1)
                bits[0] = map_ref(bits[0], embed)
                out.append(" ".join(bits))
            new = ", ".join(out)
        else:
            new = map_ref(val, embed)
        return m.group(1) + m.group(4) + new.encode() + m.group(4)

    return _ATTR_RE.sub(sub, html), sorted(assets.items())
