"""Landing: fetch one month's zip from BTS, exactly once, and record what was fetched.

The landing zone is immutable. A month is downloaded to a temporary name, checked to be a
readable zip, then renamed into place, so a half-finished download never looks like data.
A manifest next to each file records its URL, size and SHA-256 for lineage.
"""

from __future__ import annotations

import hashlib
import json
import logging
import urllib.request
import zipfile
from datetime import UTC, datetime
from pathlib import Path

from .config import BTS_URL, Config

log = logging.getLogger(__name__)


def month_url(month: str) -> str:
    year, mon = month.split("-")
    return BTS_URL.format(year=int(year), month=int(mon))


def landing_path(cfg: Config, month: str) -> Path:
    return cfg.landing / f"ontime_{month}.zip"


def sha256(path: Path) -> str:
    h = hashlib.sha256()
    with path.open("rb") as f:
        for chunk in iter(lambda: f.read(1 << 20), b""):
            h.update(chunk)
    return h.hexdigest()


def land(cfg: Config, month: str, force: bool = False) -> Path:
    """Return the local zip for a month, downloading it only if it isn't already landed."""
    path = landing_path(cfg, month)
    if path.exists() and not force:
        log.info("landing %s: already present, skipping download", month)
        return path
    url = month_url(month)
    tmp = path.with_suffix(".part")
    log.info("landing %s: downloading %s", month, url)
    req = urllib.request.Request(url, headers={"User-Agent": "open-lakehouse/0.1"})
    with urllib.request.urlopen(req, timeout=300) as resp, tmp.open("wb") as out:
        while chunk := resp.read(1 << 20):
            out.write(chunk)
    if not zipfile.is_zipfile(tmp):
        tmp.unlink()
        raise RuntimeError(f"{url} did not return a zip; is {month} published yet?")
    tmp.replace(path)
    write_manifest(path, url)
    return path


def write_manifest(path: Path, url: str) -> dict:
    manifest = {
        "file": path.name,
        "url": url,
        "bytes": path.stat().st_size,
        "sha256": sha256(path),
        "landed_at": datetime.now(UTC).isoformat(timespec="seconds"),
    }
    path.with_suffix(".json").write_text(json.dumps(manifest, indent=2))
    return manifest
