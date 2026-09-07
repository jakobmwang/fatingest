import gzip
import io
import json
import os
import posixpath
import tarfile
import zipfile

import filetype


def _log(evt: str, **fields):
    """Structured JSON log line, same shape as the rest of the service."""
    print(json.dumps({"evt": evt, **fields}), flush=True)

MAX_FILE_SIZE = int(os.getenv("MAX_FILE_SIZE", str(100 * 1024 * 1024)))
MAX_TOTAL_SIZE = MAX_FILE_SIZE * 10
MAX_DEPTH = 5


def archive_to_files(data: bytes, prefix: str = "", _depth: int = 0, _budget: list | None = None) -> tuple[list[tuple[str, bytes]], dict]:
    if _budget is None:
        _budget = [MAX_TOTAL_SIZE]

    if _depth > MAX_DEPTH:
        _log("archive_depth_exceeded", max_depth=MAX_DEPTH)
        return [], {}

    ft = filetype.guess(data)
    if not ft:
        return [], {}

    files = []

    # Decompress gz first, then let it fall through to tar or return as-is
    if ft.extension == "gz":
        try:
            with gzip.GzipFile(fileobj=io.BytesIO(data)) as f:
                data = safe_read(f, min(MAX_FILE_SIZE, _budget[0]))
            ft = filetype.guess(data)
            if not ft or ft.extension not in ("tar", "zip"):
                _budget[0] -= len(data)
                if _budget[0] < 0:
                    _log("archive_budget_exceeded", max_total_size=MAX_TOTAL_SIZE)
                    return [], {}
                return [(prefix.rstrip("/"), data)], {'num_files': 1}
        except Exception as e:
            _log("archive_error", format="gz", err=str(e)[:200])
            return [], {}

    if ft.extension == "zip":
        try:
            with zipfile.ZipFile(io.BytesIO(data)) as z:
                for name in z.namelist():
                    if z.getinfo(name).is_dir():
                        continue
                    clean = sanitize_path(name)
                    if not clean:
                        continue
                    try:
                        with z.open(name) as f:
                            file_bytes = safe_read(f, min(MAX_FILE_SIZE, _budget[0]))
                    except ValueError:
                        _log("archive_member_skipped", path=clean, reason="exceeds size limit")
                        continue
                    _budget[0] -= len(file_bytes)
                    if _budget[0] < 0:
                        _log("archive_budget_exceeded", max_total_size=MAX_TOTAL_SIZE)
                        break
                    files.append((f"{prefix}{clean}", file_bytes))
        except Exception as e:
            _log("archive_error", format="zip", err=str(e)[:200])

    elif ft.extension == "tar":
        try:
            with tarfile.open(fileobj=io.BytesIO(data)) as t:
                for member in t:
                    if not member.isfile():
                        continue
                    clean = sanitize_path(member.name)
                    if not clean:
                        continue
                    f = t.extractfile(member)
                    if not f:
                        continue
                    try:
                        file_bytes = safe_read(f, min(MAX_FILE_SIZE, _budget[0]))
                    except ValueError:
                        _log("archive_member_skipped", path=clean, reason="exceeds size limit")
                        continue
                    _budget[0] -= len(file_bytes)
                    if _budget[0] < 0:
                        _log("archive_budget_exceeded", max_total_size=MAX_TOTAL_SIZE)
                        break
                    files.append((f"{prefix}{clean}", file_bytes))
        except Exception as e:
            _log("archive_error", format="tar", err=str(e)[:200])

    # Recurse into nested archives
    result = []
    for path, file_bytes in files:
        child_ft = filetype.guess(file_bytes)
        if child_ft and child_ft.extension in ("zip", "gz", "tar"):
            result.extend(archive_to_files(file_bytes, prefix=f"{path}/", _depth=_depth + 1, _budget=_budget)[0])
        else:
            result.append((path, file_bytes))

    return result, {'num_files': len(result)}


def safe_read(f, max_size: int) -> bytes:
    """Read from file-like object with size limit."""
    chunks = []
    total = 0
    while True:
        chunk = f.read(65536)
        if not chunk:
            break
        total += len(chunk)
        if total > max_size:
            raise ValueError(f"Exceeds {max_size} byte limit")
        chunks.append(chunk)
    return b"".join(chunks)


def sanitize_path(name: str) -> str | None:
    """Strip path traversal. Returns None if path is invalid."""
    clean = posixpath.normpath(name).lstrip("/")
    if clean.startswith(".."):
        return None
    return clean
