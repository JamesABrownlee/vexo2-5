"""
Enrichment Worker - offloads YouTube search and metadata lookups.
"""
import asyncio
import time
from dataclasses import dataclass
from typing import Awaitable, Callable

from src.services.youtube import YouTubeService, YTTrack
from src.services.metadata_enricher import MetadataEnricher
from src.utils.logging import get_logger, Category

log = get_logger(__name__)


@dataclass
class _Job:
    key: str
    op: str
    coro_factory: Callable[[], Awaitable]
    future: asyncio.Future
    ttl_s: float
    cache: dict[str, tuple[float, object]]
    inflight: dict[str, asyncio.Future]


class EnrichmentWorker:
    """Concurrency-limited worker for YouTube search/enrichment."""

    def __init__(self, youtube: YouTubeService, *, concurrency: int = 2, metadata_enricher: MetadataEnricher | None = None):
        self.youtube = youtube
        self.metadata_enricher = metadata_enricher
        self.concurrency = max(1, int(concurrency))
        self._queue: asyncio.Queue[_Job] = asyncio.Queue()
        self._tasks: list[asyncio.Task] = []
        self._started = False
        self._stopping = False

        # Cache: key -> (expires_at_monotonic, value)
        self._cache_search: dict[str, tuple[float, list[YTTrack]]] = {}
        self._cache_watch: dict[str, tuple[float, list[YTTrack]]] = {}
        self._cache_track: dict[str, tuple[float, YTTrack | None]] = {}
        self._cache_meta: dict[str, tuple[float, dict]] = {}

        # Inflight dedupe
        self._inflight_search: dict[str, asyncio.Future] = {}
        self._inflight_watch: dict[str, asyncio.Future] = {}
        self._inflight_track: dict[str, asyncio.Future] = {}
        self._inflight_meta: dict[str, asyncio.Future] = {}

    async def start(self) -> None:
        if self._started:
            return
        self._started = True
        self._stopping = False
        for idx in range(self.concurrency):
            self._tasks.append(asyncio.create_task(self._worker_loop(idx)))
        log.info_cat(Category.SYSTEM, "enrichment_worker_started", concurrency=self.concurrency)

    async def stop(self) -> None:
        if not self._started:
            return
        self._stopping = True
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

        # Cancel any inflight futures so callers don't hang.
        for inflight in (self._inflight_search, self._inflight_watch, self._inflight_track, self._inflight_meta):
            for fut in inflight.values():
                if not fut.done():
                    fut.cancel()
            inflight.clear()
        log.info_cat(Category.SYSTEM, "enrichment_worker_stopped")

    def _get_cached(self, cache: dict[str, tuple[float, object]], key: str):
        hit = cache.get(key)
        if not hit:
            return None
        expires_at, value = hit
        if time.monotonic() >= expires_at:
            cache.pop(key, None)
            return None
        return value

    def _set_cache(self, cache: dict[str, tuple[float, object]], key: str, value, ttl_s: float) -> None:
        cache[key] = (time.monotonic() + ttl_s, value)

    async def _submit(
        self,
        *,
        key: str,
        op: str,
        coro_factory: Callable[[], Awaitable],
        cache: dict[str, tuple[float, object]],
        inflight: dict[str, asyncio.Future],
        ttl_s: float,
        timeout_s: float,
    ):
        cached = self._get_cached(cache, key)
        if cached is not None:
            return cached

        existing = inflight.get(key)
        if existing:
            try:
                return await asyncio.wait_for(asyncio.shield(existing), timeout=timeout_s)
            except asyncio.TimeoutError:
                log.info_cat(Category.API, "enrichment_timeout", op=op, key=key)
                return None

        loop = asyncio.get_running_loop()
        fut: asyncio.Future = loop.create_future()
        inflight[key] = fut

        if self._queue.qsize() > 0:
            log.debug_cat(Category.API, "enrichment_queue_depth", op=op, depth=self._queue.qsize())

        await self._queue.put(
            _Job(
                key=key,
                op=op,
                coro_factory=coro_factory,
                future=fut,
                ttl_s=ttl_s,
                cache=cache,
                inflight=inflight,
            )
        )

        try:
            return await asyncio.wait_for(asyncio.shield(fut), timeout=timeout_s)
        except asyncio.TimeoutError:
            log.info_cat(Category.API, "enrichment_timeout", op=op, key=key)
            return None

    async def _worker_loop(self, worker_id: int) -> None:
        try:
            while True:
                job = await self._queue.get()
                try:
                    result = await job.coro_factory()
                    self._set_cache(job.cache, job.key, result, job.ttl_s)
                    if not job.future.done():
                        job.future.set_result(result)
                except Exception as e:
                    if not job.future.done():
                        job.future.set_exception(e)
                    log.debug_cat(Category.API, "enrichment_job_failed", op=job.op, error=str(e))
                finally:
                    job.inflight.pop(job.key, None)
                    self._queue.task_done()
        except asyncio.CancelledError:
            return

    async def search_tracks(
        self,
        query: str,
        *,
        filter_type: str = "songs",
        limit: int = 5,
        timeout_s: float = 8.0,
    ) -> list[YTTrack]:
        key = f"{filter_type}:{limit}:{query.strip().lower()}"
        result = await self._submit(
            key=key,
            op="search",
            coro_factory=lambda: self.youtube.search(query, filter_type=filter_type, limit=limit),
            cache=self._cache_search,
            inflight=self._inflight_search,
            ttl_s=120.0,
            timeout_s=timeout_s,
        )
        return result or []

    async def get_track_info(self, video_id: str, *, timeout_s: float = 6.0) -> YTTrack | None:
        key = video_id.strip()
        result = await self._submit(
            key=key,
            op="track_info",
            coro_factory=lambda: self.youtube.get_track_info(video_id),
            cache=self._cache_track,
            inflight=self._inflight_track,
            ttl_s=600.0,
            timeout_s=timeout_s,
        )
        return result

    async def get_watch_playlist(
        self,
        video_id: str,
        *,
        limit: int = 20,
        timeout_s: float = 8.0,
    ) -> list[YTTrack]:
        key = f"{video_id.strip()}:{limit}"
        result = await self._submit(
            key=key,
            op="watch_playlist",
            coro_factory=lambda: self.youtube.get_watch_playlist(video_id, limit=limit),
            cache=self._cache_watch,
            inflight=self._inflight_watch,
            ttl_s=120.0,
            timeout_s=timeout_s,
        )
        return result or []

    async def enrich_metadata(
        self,
        artist: str,
        title: str,
        *,
        timeout_s: float = 6.0,
    ) -> dict:
        if not self.metadata_enricher or not self.metadata_enricher.enabled:
            return {"genres": [], "year": None}

        key = f"{artist.strip().lower()}|{title.strip().lower()}"
        result = await self._submit(
            key=key,
            op="metadata",
            coro_factory=lambda: self.metadata_enricher.enrich(artist, title),
            cache=self._cache_meta,
            inflight=self._inflight_meta,
            ttl_s=600.0,
            timeout_s=timeout_s,
        )
        return result or {"genres": [], "year": None}
