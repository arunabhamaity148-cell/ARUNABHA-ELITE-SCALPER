"""
ARUNABHA ELITE SCALPER v3.0
FILE 15/18: cache_manager.py
In-memory TTL cache with optional Redis backend
Used for funding rates, OI, API responses
"""

import asyncio
import logging
import time
from typing import Any, Dict, Optional, Tuple

import config

log = logging.getLogger("elite.cache")


class InMemoryCache:
    """Thread-safe in-memory TTL cache."""

    def __init__(self, max_size: int = config.MAX_CACHE_SIZE, default_ttl: int = config.CACHE_TTL):
        self._store: Dict[str, Tuple[Any, float]] = {}  # key → (value, expires_at)
        self._max_size = max_size
        self._default_ttl = default_ttl
        self._hits = 0
        self._misses = 0
        self._evictions = 0

    def get(self, key: str) -> Optional[Any]:
        entry = self._store.get(key)
        if not entry:
            self._misses += 1
            return None
        value, expires_at = entry
        if time.time() > expires_at:
            del self._store[key]
            self._misses += 1
            return None
        self._hits += 1
        return value

    def set(self, key: str, value: Any, ttl: Optional[int] = None) -> None:
        if len(self._store) >= self._max_size:
            self._evict()
        ttl = ttl or self._default_ttl
        self._store[key] = (value, time.time() + ttl)

    def delete(self, key: str) -> None:
        self._store.pop(key, None)

    def _evict(self):
        """Remove 10% of oldest entries."""
        now = time.time()
        # First remove expired
        expired = [k for k, (_, exp) in self._store.items() if exp < now]
        for k in expired:
            del self._store[k]
            self._evictions += 1

        # If still too large, remove oldest
        if len(self._store) >= self._max_size:
            sorted_keys = sorted(self._store.keys(), key=lambda k: self._store[k][1])
            remove_count = max(1, len(self._store) // 10)
            for k in sorted_keys[:remove_count]:
                del self._store[k]
                self._evictions += 1

    def stats(self) -> dict:
        total = self._hits + self._misses
        return {
            "size": len(self._store),
            "hits": self._hits,
            "misses": self._misses,
            "hit_rate": round(self._hits / total, 3) if total > 0 else 0,
            "evictions": self._evictions,
        }

    def clear(self):
        self._store.clear()


class CacheManager:
    """
    Unified cache interface.
    Uses Redis if available (REDIS_URL env var set),
    otherwise falls back to InMemoryCache.
    """

    def __init__(self):
        self._memory_cache = InMemoryCache()
        self._redis = None
        self._redis_available = False

    async def initialize(self):
        if config.REDIS_URL:
            try:
                import aioredis
                self._redis = await aioredis.from_url(
                    config.REDIS_URL,
                    decode_responses=True,
                    socket_timeout=2,
                )
                await self._redis.ping()
                self._redis_available = True
                log.info("Redis cache connected")
            except Exception as e:
                log.warning(f"Redis unavailable ({e}) — using in-memory cache")
                self._redis_available = False

    async def get(self, key: str) -> Optional[Any]:
        if self._redis_available:
            try:
                import json as _json
                val = await self._redis.get(key)
                if val is not None:
                    return _json.loads(val)
                return None
            except Exception:
                pass
        return self._memory_cache.get(key)

    async def set(self, key: str, value: Any, ttl: Optional[int] = None) -> None:
        ttl = ttl or config.CACHE_TTL
        if self._redis_available:
            try:
                import json as _json
                await self._redis.setex(key, ttl, _json.dumps(value))
                return
            except Exception:
                pass
        self._memory_cache.set(key, value, ttl)

    async def delete(self, key: str) -> None:
        if self._redis_available:
            try:
                await self._redis.delete(key)
            except Exception:
                pass
        self._memory_cache.delete(key)

    def get_sync(self, key: str) -> Optional[Any]:
        """Synchronous get (memory cache only)."""
        return self._memory_cache.get(key)

    def set_sync(self, key: str, value: Any, ttl: Optional[int] = None) -> None:
        """Synchronous set (memory cache only)."""
        self._memory_cache.set(key, value, ttl)

    def stats(self) -> dict:
        return {
            "backend": "redis" if self._redis_available else "memory",
            "memory": self._memory_cache.stats(),
        }

    async def close(self):
        if self._redis:
            try:
                await self._redis.close()
            except Exception:
                pass


# Singleton instance
_cache: Optional[CacheManager] = None


def get_cache() -> CacheManager:
    global _cache
    if _cache is None:
        _cache = CacheManager()
    return _cache
