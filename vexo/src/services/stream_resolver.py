"""
Stream Resolver Worker - concurrency-limited stream URL resolution with caching.
"""
import asyncio
import time
from dataclasses import dataclass
from typing import Awaitable, Callable

from src.services.youtube import YouTubeService, StreamInfo
from src.utils.logging import get_logger, Category

log = get_logger(__name__)


@dataclass
class _Job:
    key: str
    coro_factory: Callable[[], Awaitable]
    future: asyncio.Future
    ttl_s: float


class StreamResolverWorker:
    def __init__(self, youtube: YouTubeService, *, concurrency: int = 2):
        self.youtube = youtube
        self.concurrency = max(1, int(concurrency))
        self._queue: asyncio.Queue[_Job] = asyncio.Queue()
        self._tasks: list[asyncio.Task] = []
        self._started = False

        self._cache: dict[str, tuple[float, StreamInfo | None]] = {}
        self._inflight: dict[str, asyncio.Future] = {}

    async def start(self) -> None:
        if self._started:
            return
        self._started = True
        for idx in range(self.concurrency):
            self._tasks.append(asyncio.create_task(self._worker_loop(idx)))
        log.info_cat(Category.SYSTEM, "stream_resolver_started", concurrency=self.concurrency)

    async def stop(self) -> None:
        if not self._started:
            return
        for task in self._tasks:
            task.cancel()
        for task in self._tasks:
            try:
                await task
            except asyncio.CancelledError:
                pass
            except Exception:
                pass
        self._tasks.clear()
        self._started = False

        for fut in self._inflight.values():
            if not fut.done():
                fut.cancel()
        self._inflight.clear()
        log.info_cat(Category.SYSTEM, "stream_resolver_stopped")

    def _get_cached(self, key: str) -> StreamInfo | None | object:
        hit = self._cache.get(key)
        if not hit:
            return None
        expires_at, value = hit
        if time.monotonic() >= expires_at:
            self._cache.pop(key, None)
            return None
        return value

    def _set_cache(self, key: str, value: StreamInfo | None, ttl_s: float) -> None:
        self._cache[key] = (time.monotonic() + ttl_s, value)

    async def get_stream_url(self, video_id: str, *, timeout_s: float = 15.0) -> StreamInfo | None:
        key = video_id.strip()
        cached = self._get_cached(key)
        if cached is not None:
            return cached if isinstance(cached, StreamInfo) or cached is None else None

        existing = self._inflight.get(key)
        if existing:
            try:
                return await asyncio.wait_for(asyncio.shield(existing), timeout=timeout_s)
            except asyncio.TimeoutError:
                log.info_cat(Category.PLAYBACK, "stream_resolver_timeout", video_id=video_id)
                return None

        loop = asyncio.get_running_loop()
        fut: asyncio.Future = loop.create_future()
        self._inflight[key] = fut

        if self._queue.qsize() > 0:
            log.debug_cat(Category.PLAYBACK, "stream_resolver_queue_depth", depth=self._queue.qsize())

        await self._queue.put(
            _Job(
                key=key,
                coro_factory=lambda: self.youtube.get_stream_url(video_id),
                future=fut,
                ttl_s=180.0,
            )
        )

        try:
            return await asyncio.wait_for(asyncio.shield(fut), timeout=timeout_s)
        except asyncio.TimeoutError:
            log.info_cat(Category.PLAYBACK, "stream_resolver_timeout", video_id=video_id)
            return None

    async def _worker_loop(self, worker_id: int) -> None:
        try:
            while True:
                job = await self._queue.get()
                try:
                    result = await job.coro_factory()
                    self._set_cache(job.key, result, job.ttl_s)
                    if not job.future.done():
                        job.future.set_result(result)
                except Exception as e:
                    if not job.future.done():
                        job.future.set_exception(e)
                    log.debug_cat(Category.PLAYBACK, "stream_resolver_failed", error=str(e))
                finally:
                    self._inflight.pop(job.key, None)
                    self._queue.task_done()
        except asyncio.CancelledError:
            return
