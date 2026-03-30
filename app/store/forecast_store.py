"""In-memory forecast result cache with TTL-based expiry.

Results are keyed by ``"{service_name}:{metric_name}"`` and evicted
automatically after *ttl_seconds*.  The store is intentionally kept
simple (pure dict + lock) to avoid runtime dependencies; a Redis-backed
variant can be added later as a drop-in replacement.
"""

from __future__ import annotations

import asyncio
import logging
import time
from typing import Dict, List, Optional, Tuple

from ..models.forecast import ForecastResult

logger = logging.getLogger(__name__)

_DEFAULT_TTL = 3600   # 1 hour


class ForecastStore:
    """Thread-safe TTL cache for ForecastResult objects.

    Parameters
    ----------
    ttl_seconds:
        How long a result is considered fresh (default 3600 s).
    """

    def __init__(self, ttl_seconds: int = _DEFAULT_TTL) -> None:
        self.ttl_seconds = ttl_seconds
        # key → (stored_at_monotonic, ForecastResult)
        self._data: Dict[str, Tuple[float, ForecastResult]] = {}
        self._lock = asyncio.Lock()

    # ------------------------------------------------------------------
    # Write
    # ------------------------------------------------------------------

    async def set(self, result: ForecastResult) -> None:
        """Store *result*; overwrites any existing entry for the same key."""
        key = _make_key(result.service_name, result.metric_name)
        async with self._lock:
            self._data[key] = (time.monotonic(), result)
        logger.debug("forecast stored key=%s", key)

    async def delete(self, service_name: str, metric_name: str) -> bool:
        """Remove an entry; return True if it existed."""
        key = _make_key(service_name, metric_name)
        async with self._lock:
            existed = key in self._data
            self._data.pop(key, None)
        return existed

    # ------------------------------------------------------------------
    # Read
    # ------------------------------------------------------------------

    async def get(
        self, service_name: str, metric_name: str
    ) -> Optional[ForecastResult]:
        """Return the cached result or *None* if absent / expired."""
        key = _make_key(service_name, metric_name)
        async with self._lock:
            entry = self._data.get(key)
        if entry is None:
            return None
        stored_at, result = entry
        if (time.monotonic() - stored_at) > self.ttl_seconds:
            async with self._lock:
                self._data.pop(key, None)
            logger.debug("forecast expired key=%s", key)
            return None
        return result

    async def list_all(self) -> List[ForecastResult]:
        """Return all non-expired forecast results."""
        now = time.monotonic()
        async with self._lock:
            valid = [
                result
                for stored_at, result in self._data.values()
                if (now - stored_at) <= self.ttl_seconds
            ]
        return valid

    async def list_services(self) -> List[str]:
        """Return distinct service names that have at least one valid result."""
        results = await self.list_all()
        return list({r.service_name for r in results})

    # ------------------------------------------------------------------
    # Maintenance
    # ------------------------------------------------------------------

    async def evict_expired(self) -> int:
        """Remove all expired entries; return the number evicted."""
        now = time.monotonic()
        async with self._lock:
            expired_keys = [
                k
                for k, (stored_at, _) in self._data.items()
                if (now - stored_at) > self.ttl_seconds
            ]
            for k in expired_keys:
                del self._data[k]
        if expired_keys:
            logger.debug("evicted %d expired forecasts", len(expired_keys))
        return len(expired_keys)

    def __len__(self) -> int:
        return len(self._data)


def _make_key(service_name: str, metric_name: str) -> str:
    return f"{service_name}:{metric_name}"
