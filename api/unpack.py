"""Everything is a list.

An archive unpacks into its members (recursively, flattened with path prefixes: a file
inside a/b.zip is a/b.zip/file); anything else becomes a singleton list. The per-file
parse flow stays strictly single-file - the loop over this list lives outside it.
"""
import filetype

from archive_to_files import archive_to_files

ARCHIVE_EXTENSIONS = ("zip", "tar", "gz")


def to_members(data: bytes, name: str = "") -> list[tuple[str, bytes]]:
    ft = filetype.guess(data)
    if ft and ft.extension in ARCHIVE_EXTENSIONS:
        files, _meta = archive_to_files(data)
        if files:
            # A bare gzip (no tar or zip inside) yields one member under the empty top-level
            # prefix; name it after the delivered filename, as gunzip would.
            return [(path or _gunzipped(name), member) for path, member in files]
    return [(name, data)]


def _gunzipped(name: str) -> str:
    return name[:-3] if name.lower().endswith(".gz") else name
