"""SHA-256 checksum utility for Raw-ingestion idempotency (DEC-003).

Scope (Step 3.3.3): a single, narrow responsibility — compute the SHA-256
checksum of the *complete source file bytes*, exactly as the file sits on
disk. This is intentionally byte-level, not row-level or content-level:

- It does not parse CSV rows.
- It does not normalize whitespace, line endings, or encoding.
- Any change to the file (including a change that doesn't alter any row,
  e.g. trailing whitespace) produces a different checksum.

This is explicitly checksum-based idempotency, NOT change-data-capture
(CDC). It only answers "is this the same file I already loaded?" — it does
not identify which rows changed.
"""

from __future__ import annotations

import hashlib

_CHUNK_SIZE = 1024 * 1024  # 1 MB — stream large files without loading fully.


def compute_file_checksum(file_path: str) -> str:
    """Return the lowercase hex-encoded SHA-256 digest of `file_path`.

    Reads the file in binary mode and streams it in chunks, so behavior is
    identical for small and large (e.g. 80,000-row) files.
    """
    sha256 = hashlib.sha256()
    with open(file_path, "rb") as fh:
        for chunk in iter(lambda: fh.read(_CHUNK_SIZE), b""):
            sha256.update(chunk)
    return sha256.hexdigest()
