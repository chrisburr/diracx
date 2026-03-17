from __future__ import annotations

__all__ = [
    "BaseLock",
    "BaseLimiter",
    "MutexLock",
    "ExclusiveRWLock",
    "SharedRWLock",
    "RateLimiter",
    "ConcurrencyLimiter",
]

import logging
import uuid
from abc import ABC, abstractmethod

from redis.asyncio import Redis

from .lock_registry import LockedObjectType

logger = logging.getLogger(__name__)

# Lua script for releasing a mutex lock only if the owner matches
_MUTEX_RELEASE_SCRIPT = """
if redis.call("get", KEYS[1]) == ARGV[1] then
    return redis.call("del", KEYS[1])
else
    return 0
end
"""

# Lua script for acquiring a shared read lock (if no writer holds exclusive)
_SHARED_ACQUIRE_SCRIPT = """
local writers = redis.call("hget", KEYS[1], "writer_owner")
if writers then
    return 0
end
redis.call("hincrby", KEYS[1], "readers", 1)
return 1
"""

# Lua script for releasing a shared read lock
_SHARED_RELEASE_SCRIPT = """
local readers = redis.call("hincrby", KEYS[1], "readers", -1)
if readers <= 0 then
    redis.call("hdel", KEYS[1], "readers")
end
return readers
"""

# Lua script for acquiring an exclusive write lock
_EXCLUSIVE_ACQUIRE_SCRIPT = """
local readers = tonumber(redis.call("hget", KEYS[1], "readers") or "0")
local writer = redis.call("hget", KEYS[1], "writer_owner")
if readers == 0 and not writer then
    redis.call("hset", KEYS[1], "writer_owner", ARGV[1])
    redis.call("pexpire", KEYS[1], ARGV[2])
    return 1
end
return 0
"""

# Lua script for releasing an exclusive write lock
_EXCLUSIVE_RELEASE_SCRIPT = """
if redis.call("hget", KEYS[1], "writer_owner") == ARGV[1] then
    redis.call("hdel", KEYS[1], "writer_owner")
    return 1
end
return 0
"""

# Lua script for concurrency limiter acquire
_CONCURRENCY_ACQUIRE_SCRIPT = """
local current = tonumber(redis.call("get", KEYS[1]) or "0")
if current < tonumber(ARGV[1]) then
    redis.call("incr", KEYS[1])
    return 1
end
return 0
"""

DEFAULT_LOCK_TTL_MS = 30000  # 30 seconds


class BaseLock(ABC):
    """Base class for locks."""

    def __init__(
        self,
        obj: LockedObjectType,
        key: int | str,
        *extra_keys: int | str,
    ):
        self.obj = obj
        self.key = key
        self.extra_keys = extra_keys
        self._owner_id = uuid.uuid4().hex

    @property
    def redis_key(self) -> str:
        """Compute the Redis key for this lock."""
        parts = [str(self.obj), str(self.key)]
        parts.extend(str(k) for k in self.extra_keys)
        return ":".join(parts)

    @abstractmethod
    async def acquire(self, redis: Redis) -> bool:
        """Attempt to acquire the lock.

        Returns True if acquired, False otherwise.
        """
        ...

    @abstractmethod
    async def release(self, redis: Redis) -> None:
        """Release the lock."""
        ...


class MutexLock(BaseLock):
    """A simple mutex lock backed by Redis SET NX."""

    def __init__(
        self,
        obj: LockedObjectType,
        key: int | str,
        *extra_keys: int | str,
        timeout: int | None = None,
        ttl_ms: int = DEFAULT_LOCK_TTL_MS,
    ):
        super().__init__(obj, key, *extra_keys)
        self.timeout = timeout
        self.ttl_ms = ttl_ms

    @property
    def redis_key(self) -> str:
        return f"lock:mutex:{super().redis_key}"

    async def acquire(self, redis: Redis) -> bool:
        result = await redis.set(
            self.redis_key,
            self._owner_id,
            nx=True,
            px=self.ttl_ms,
        )
        return result is not None

    async def release(self, redis: Redis) -> None:
        await redis.eval(  # type: ignore[arg-type]
            _MUTEX_RELEASE_SCRIPT, 1, self.redis_key, self._owner_id
        )

    async def extend(self, redis: Redis) -> bool:
        """Extend the TTL of the lock (watchdog pattern)."""
        result = await redis.pexpire(self.redis_key, self.ttl_ms)
        return bool(result)


class ExclusiveRWLock(BaseLock):
    """An exclusive (writer) lock in a reader-writer pattern."""

    def __init__(
        self,
        obj: LockedObjectType,
        key: int | str,
        *extra_keys: int | str,
        timeout: int | None = None,
        ttl_ms: int = DEFAULT_LOCK_TTL_MS,
    ):
        super().__init__(obj, key, *extra_keys)
        self.timeout = timeout
        self.ttl_ms = ttl_ms

    @property
    def redis_key(self) -> str:
        return f"lock:rw:{super().redis_key}"

    async def acquire(self, redis: Redis) -> bool:
        result = await redis.eval(  # type: ignore[arg-type]
            _EXCLUSIVE_ACQUIRE_SCRIPT,
            1,
            self.redis_key,
            self._owner_id,
            str(self.ttl_ms),
        )
        return bool(result)

    async def release(self, redis: Redis) -> None:
        await redis.eval(  # type: ignore[arg-type]
            _EXCLUSIVE_RELEASE_SCRIPT, 1, self.redis_key, self._owner_id
        )


class SharedRWLock(BaseLock):
    """A shared (reader) lock in a reader-writer pattern."""

    @property
    def redis_key(self) -> str:
        return f"lock:rw:{super().redis_key}"

    async def acquire(self, redis: Redis) -> bool:
        result = await redis.eval(  # type: ignore[arg-type]
            _SHARED_ACQUIRE_SCRIPT, 1, self.redis_key
        )
        return bool(result)

    async def release(self, redis: Redis) -> None:
        await redis.eval(  # type: ignore[arg-type]
            _SHARED_RELEASE_SCRIPT, 1, self.redis_key
        )


class BaseLimiter(BaseLock):
    """Base class for limiters.

    Limiters are lock primitives that are only enforced for non-interactive
    task execution (skipped in CLI/interactive mode).
    """


class RateLimiter(BaseLimiter):
    """A rate limiter using a sliding window counter."""

    limit: int | None = None
    window_seconds: int | None = None

    def __init__(
        self,
        obj: LockedObjectType,
        key: int | str,
        *extra_keys: int | str,
        n_items: int = 1,
    ):
        super().__init__(obj, key, *extra_keys)
        self.n_items = n_items

    @property
    def redis_key(self) -> str:
        return f"limiter:rate:{super().redis_key}"

    async def acquire(self, redis: Redis) -> bool:
        if self.limit is None or self.window_seconds is None:
            return True
        import time

        window_key = f"{self.redis_key}:{int(time.time()) // self.window_seconds}"
        current = int(await redis.get(window_key) or 0)
        if current + self.n_items > self.limit:
            return False
        pipe = redis.pipeline()
        pipe.incrby(window_key, self.n_items)
        pipe.expire(window_key, self.window_seconds * 2)
        await pipe.execute()
        return True

    async def release(self, redis: Redis) -> None:
        pass  # Rate limiters don't release


class ConcurrencyLimiter(BaseLimiter):
    """A concurrency limiter (semaphore-style)."""

    limit: int | None = None

    @property
    def redis_key(self) -> str:
        return f"limiter:conc:{super().redis_key}"

    async def acquire(self, redis: Redis) -> bool:
        if self.limit is None:
            return True
        result = await redis.eval(  # type: ignore[arg-type]
            _CONCURRENCY_ACQUIRE_SCRIPT, 1, self.redis_key, str(self.limit)
        )
        return bool(result)

    async def release(self, redis: Redis) -> None:
        if self.limit is None:
            return
        await redis.decr(self.redis_key)
