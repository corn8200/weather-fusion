from __future__ import annotations

from dataclasses import dataclass
from datetime import UTC, datetime, timedelta
from pathlib import Path
from typing import Callable, Protocol


class Downloader(Protocol):
    def __call__(self) -> bytes:  # pragma: no cover - structural contract
        ...


@dataclass
class CachedFile:
    path: Path
    fresh: bool


class CacheManager:
    """Simple timestamp-based cache for downloaded artifacts."""

    def __init__(self, root: Path, ttl_hours: int = 3) -> None:
        self.root = root
        self.ttl = timedelta(hours=max(ttl_hours, 0))
        self.root.mkdir(parents=True, exist_ok=True)

    def _is_fresh(self, path: Path) -> bool:
        if not path.exists():
            return False
        if self.ttl == timedelta(0):
            return False
        mtime = datetime.fromtimestamp(path.stat().st_mtime, tz=UTC)
        return datetime.now(UTC) - mtime <= self.ttl

    def _slot(self, namespace: str, name: str) -> Path:
        slot = self.root / namespace / name
        slot.parent.mkdir(parents=True, exist_ok=True)
        return slot

    def fetch(self, namespace: str, name: str, downloader: Downloader) -> CachedFile:
        target = self._slot(namespace, name)
        if self._is_fresh(target):
            return CachedFile(path=target, fresh=True)
        data = downloader()
        target.write_bytes(data)
        return CachedFile(path=target, fresh=False)

    def read_text(self, namespace: str, name: str, downloader: Downloader) -> str:
        cached = self.fetch(namespace, name, downloader)
        return cached.path.read_text()

    def read_bytes(self, namespace: str, name: str, downloader: Downloader) -> bytes:
        cached = self.fetch(namespace, name, downloader)
        return cached.path.read_bytes()
