import asyncio
import logging
import time
import collections
import random
import shlex
from dataclasses import dataclass, field
from datetime import datetime, timedelta, UTC
from typing import Optional
from urllib.parse import urlparse

import aiohttp
import discord
from discord.ext import commands

from src.services.youtube import YouTubeService, YTTrack, StreamInfo
from src.database.crud import SongCRUD, UserCRUD, PlaybackCRUD, ReactionCRUD, GuildCRUD
from src.utils.logging import get_logger, Category, Event

log = get_logger(__name__)


class MusicQueue:
    """A thread-safe-ish async queue that supports inserting at the front."""
    def __init__(self):
        self._items = collections.deque()
        self._event = asyncio.Event()

    def empty(self):
        return len(self._items) == 0

    def qsize(self):
        return len(self._items)

    def get_nowait(self):
        if not self._items:
            raise asyncio.QueueEmpty()
        item = self._items.popleft()
        if not self._items:
            self._event.clear()
        return item

    async def get(self):
        while not self._items:
            await self._event.wait()
        return self.get_nowait()

    def put_nowait(self, item):
        self._items.append(item)
        self._event.set()

    async def put(self, item):
        self.put_nowait(item)

    def put_at_front(self, item):
        self._items.appendleft(item)
        self._event.set()
    
    @property
    def _queue(self):
        """Compatibility with code that peeks at the internal deque."""
        return self._items


@dataclass
class QueueItem:
    """Item in the music queue."""
    video_id: str
    title: str
    artist: str
    url: str | None = None  # Stream URL, resolved when needed
    requester_id: int | None = None
    discovery_source: str = "user_request"
    discovery_reason: str | None = None
    for_user_id: int | None = None  # Democratic turn tracking
    song_db_id: int | None = None  # Database ID after insertion
    history_id: int | None = None  # Playback history ID
    duration_seconds: int | None = None
    genre: str | None = None
    year: int | None = None


@dataclass
class GuildPlayer:
    """Per-guild music player state."""
    guild_id: int
    voice_client: discord.VoiceClient | None = None
    queue: MusicQueue = field(default_factory=MusicQueue)
    current: QueueItem | None = None
    session_id: str | None = None
    is_playing: bool = False
    paused: bool = False
    volume: float = 1.0
    autoplay: bool = True
    pre_buffer: bool = True
    last_activity: datetime = field(default_factory=lambda: datetime.now(UTC))
    skip_votes: set = field(default_factory=set)
    _next_url: str | None = None  # Pre-buffered URL
    text_channel_id: int | None = None  # For Now Playing messages
    last_np_msg: discord.Message | None = None
    start_time: datetime | None = None  # When current song started
    _next_discovery: QueueItem | None = None  # Prefetched discovery song
    _prefetch_task: asyncio.Task | None = None  # Background prefetch task
    _maintenance_task: asyncio.Task | None = None  # Task to keep queue filled
    _consecutive_failures: int = 0  # Track consecutive failures for auto-recovery
    _last_health_check: datetime = field(default_factory=lambda: datetime.now(UTC))
    _np_lock: asyncio.Lock = field(default_factory=asyncio.Lock)
    _current_source: discord.AudioSource | None = None
    _current_stream_info: StreamInfo | None = None
    _play_task: asyncio.Task | None = None
    _play_start_lock: asyncio.Lock = field(default_factory=asyncio.Lock)


class MusicCog(commands.Cog):
    """Core music playback engine (queue + loop + voice streaming)."""
    
    FFMPEG_BEFORE_OPTIONS = (
        "-reconnect 1 -reconnect_streamed 1 "
        "-reconnect_on_network_error 1 -reconnect_on_http_error 403,429,500,502,503 "
        "-reconnect_delay_max 5"
    )
    FFMPEG_OPTIONS = "-vn -b:a 128k"
    
    IDLE_TIMEOUT = 300  # 5 minutes
    STREAM_FETCH_TIMEOUT = 30  # Max seconds to fetch stream URL
    PLAYBACK_TIMEOUT = 600  # Max seconds for a single song (10 min safety)
    DISCOVERY_TIMEOUT = 20  # Max seconds for discovery operation
    MAX_CONSECUTIVE_FAILURES = 3  # Auto-restart playback loop after this many failures
    SPOTIFY_ENRICH_TIMEOUT = 6  # Seconds; runs in background to avoid delaying playback

    # Radio presenter / DJ intro announcement policy:
    # - Always announce user-requested tracks
    # - Otherwise announce randomly 1 in N tracks
    RADIO_PRESENTER_RANDOM_ANNOUNCE_DENOMINATOR = 5

    @staticmethod
    def _is_user_requested(item: QueueItem) -> bool:
        # requester_id is set for /play and other explicit user queueing.
        # discovery/autoplay items typically have requester_id=None.
        return bool(item.requester_id) or item.discovery_source == "user_request"

    def _should_announce_radio_presenter(self, item: QueueItem) -> tuple[bool, str, int | None]:
        if self._is_user_requested(item):
            return True, "user_requested", None

        denom = max(1, int(self.RADIO_PRESENTER_RANDOM_ANNOUNCE_DENOMINATOR))
        roll = random.randrange(denom)
        return (roll == 0), f"random_1_in_{denom}", roll
    
    @staticmethod
    def _build_ffmpeg_options(stream_info: StreamInfo, bitrate: int = 128) -> dict:
        """Build FFmpeg options, injecting HTTP headers and bitrate."""
        before = MusicCog.FFMPEG_BEFORE_OPTIONS
        if stream_info.http_headers:
            ua = stream_info.http_headers.get("User-Agent")
            referer = stream_info.http_headers.get("Referer")
            if ua:
                before = f'-user_agent "{ua}" ' + before
            if referer:
                before = f'-referer "{referer}" ' + before
        
        return {
            "before_options": before, 
            "options": f"-vn -b:a {bitrate}k"
        }
    
    async def _get_next_item(self, player: GuildPlayer) -> QueueItem | None:
        """Get next item from queue or discovery."""
        if not player.queue.empty():
            return player.queue.get_nowait()
            
        if not player.autoplay:
            return None
            
        # Use prefetched discovery song if available
        if player._next_discovery:
            item = player._next_discovery
            player._next_discovery = None
            return item
            
        # Try to get discovery song
        guild_crud = GuildCRUD(self.bot.db) if hasattr(self.bot, "db") else None
        max_seconds = 0
        if guild_crud:
            try:
                max_dur = await guild_crud.get_setting(player.guild_id, "max_song_duration")
                if max_dur:
                    max_seconds = int(max_dur) * 60
            except: pass
            
        return await asyncio.wait_for(
            self._get_discovery_song_with_retry(player, max_seconds),
            timeout=self.DISCOVERY_TIMEOUT
        )

    async def _ensure_session(self, player: GuildPlayer):
        """Ensure guild and session exist in database."""
        if not hasattr(self.bot, "db") or not self.bot.db:
            return
            
        playback_crud = PlaybackCRUD(self.bot.db)
        guild_crud = GuildCRUD(self.bot.db)
        
        if not player.session_id:
            if player.voice_client and player.voice_client.guild:
                await guild_crud.get_or_create(
                    player.guild_id, 
                    player.voice_client.guild.name
                )
            
            player.session_id = await playback_crud.create_session(
                guild_id=player.guild_id,
                channel_id=player.voice_client.channel.id
            )
    
    def __init__(self, bot: commands.Bot):
        self.bot = bot
        self.players: dict[int, GuildPlayer] = {}
        self.youtube = YouTubeService()
        self._idle_check_task: asyncio.Task | None = None
        self._radio_presenter_task: asyncio.Task | None = None
        self._radio_presenter_enabled: bool | None = None  # unknown until checked
        self._radio_presenter_disabled_until: datetime | None = None
        self._radio_presenter_last_error: str | None = None
        self._background_tasks_started: bool = False
        self._obs_audio_subscribers: set[asyncio.Queue[bytes]] = set()
        self._obs_relay_lock = asyncio.Lock()
        self._obs_relay_task: asyncio.Task | None = None
        self._obs_relay_process: asyncio.subprocess.Process | None = None
        self._obs_relay_guild_id: int | None = None

    def _start_background_tasks(self, *, reason: str) -> None:
        if self._background_tasks_started:
            return
        self._background_tasks_started = True

        if not self._idle_check_task or self._idle_check_task.done():
            self._idle_check_task = asyncio.create_task(self._idle_check_loop())
            log.info_cat(Category.SYSTEM, "idle_check_loop_started", reason=reason)

        # Radio presenter health check loop (optional)
        try:
            from src.config import config

            url = getattr(config, "RADIO_PRESENTER_API_URL", None)
            if not url:
                log.info_cat(Category.API, "radio_presenter_disabled", reason="no_url", started_by=reason)
                return

            log.info_cat(Category.API, "radio_presenter_health_init", url=url, started_by=reason)
            if not self._radio_presenter_task or self._radio_presenter_task.done():
                self._radio_presenter_task = asyncio.create_task(self._radio_presenter_health_loop())
                log.info_cat(Category.API, "radio_presenter_health_loop_scheduled", url=url)
            # Kick an immediate check so logs show status right after startup/reload.
            asyncio.create_task(self._radio_presenter_check_once())
        except Exception as e:
            log.warning_cat(Category.API, "radio_presenter_init_failed", error=str(e), started_by=reason)

    async def cog_load(self):
        """Called when the cog is loaded."""
        self._start_background_tasks(reason="cog_load")

        log.event(Category.SYSTEM, Event.COG_LOADED, cog="music")

    @commands.Cog.listener()
    async def on_ready(self):
        # Some discord forks/versions do not reliably call cog_load on reload.
        self._start_background_tasks(reason="on_ready")
    
    async def cog_unload(self):
        """Called when the cog is unloaded."""
        if self._idle_check_task:
            self._idle_check_task.cancel()
        if self._radio_presenter_task:
            self._radio_presenter_task.cancel()
        await self._stop_obs_relay()
        self._background_tasks_started = False

        # Disconnect from all voice channels
        for player in self.players.values():
            if player.voice_client:
                await player.voice_client.disconnect(force=True)
        
        log.event(Category.SYSTEM, Event.COG_UNLOADED, cog="music")

    def get_player(self, guild_id: int) -> GuildPlayer:
        """Get or create a player for a guild."""
        if guild_id not in self.players:
            self.players[guild_id] = GuildPlayer(guild_id=guild_id)
        return self.players[guild_id]

    async def ensure_play_loop(self, player: GuildPlayer, *, reason: str) -> bool:
        """Start playback loop once per guild; no-op if one is already active."""
        async with player._play_start_lock:
            task = player._play_task
            if task and not task.done():
                return False
            player._play_task = asyncio.create_task(self._play_loop(player))
            log.info_cat(Category.PLAYBACK, "playback_loop_started", guild_id=player.guild_id, reason=reason)
            return True

    @staticmethod
    def _as_bool(value, default: bool = True) -> bool:
        if value is None:
            return default
        if isinstance(value, bool):
            return value
        if isinstance(value, str):
            return value.strip().lower() in {"1", "true", "yes", "on"}
        return bool(value)

    async def _guild_bool_setting(self, guild_id: int, key: str, default: bool = True) -> bool:
        if not hasattr(self.bot, "db") or not self.bot.db:
            return default
        try:
            guild_crud = GuildCRUD(self.bot.db)
            value = await guild_crud.get_setting(guild_id, key)
            return self._as_bool(value, default)
        except Exception:
            return default

    def _obs_enabled(self) -> bool:
        try:
            from src.config import config
            return bool(getattr(config, "OBS_AUDIO_ENABLED", False))
        except Exception:
            return False

    async def subscribe_obs_audio(self) -> asyncio.Queue[bytes] | None:
        """Register an OBS audio subscriber queue."""
        if not self._obs_enabled():
            return None

        queue: asyncio.Queue[bytes] = asyncio.Queue(maxsize=128)
        async with self._obs_relay_lock:
            self._obs_audio_subscribers.add(queue)
        await self._ensure_obs_relay_state()
        return queue

    async def unsubscribe_obs_audio(self, queue: asyncio.Queue[bytes]) -> None:
        """Remove an OBS audio subscriber queue."""
        async with self._obs_relay_lock:
            self._obs_audio_subscribers.discard(queue)
        await self._ensure_obs_relay_state()

    async def get_obs_audio_status(self) -> dict:
        """Expose OBS relay state for dashboard/API responses."""
        async with self._obs_relay_lock:
            listener_count = len(self._obs_audio_subscribers)
            relay_running = bool(self._obs_relay_task and not self._obs_relay_task.done())
            relay_guild_id = self._obs_relay_guild_id

        return {
            "enabled": self._obs_enabled(),
            "listeners": listener_count,
            "relay_running": relay_running,
            "relay_guild_id": relay_guild_id,
        }

    async def _stop_obs_relay(self) -> None:
        """Stop OBS relay task/process if running."""
        async with self._obs_relay_lock:
            task = self._obs_relay_task
            proc = self._obs_relay_process
            self._obs_relay_task = None
            self._obs_relay_process = None
            self._obs_relay_guild_id = None

        if task and not task.done():
            task.cancel()
            try:
                await task
            except asyncio.CancelledError:
                pass
            except Exception:
                pass

        if proc and proc.returncode is None:
            try:
                proc.terminate()
                await asyncio.wait_for(proc.wait(), timeout=3)
            except Exception:
                try:
                    proc.kill()
                    await proc.wait()
                except Exception:
                    pass

    async def _pick_obs_source_player(self) -> GuildPlayer | None:
        """Choose one active guild player as OBS source."""
        candidates = []
        for player in self.players.values():
            if not player.current or not player._current_stream_info:
                continue
            if not player.voice_client or not player.voice_client.is_connected():
                continue
            candidates.append(player)

        if not candidates:
            return None

        return max(
            candidates,
            key=lambda p: p.start_time or datetime.min.replace(tzinfo=UTC),
        )

    async def _ensure_obs_relay_state(self) -> None:
        """Start/stop relay based on listeners and active playback."""
        if not self._obs_enabled():
            await self._stop_obs_relay()
            return

        async with self._obs_relay_lock:
            has_listeners = bool(self._obs_audio_subscribers)
        if not has_listeners:
            await self._stop_obs_relay()
            return

        source_player = await self._pick_obs_source_player()
        if not source_player:
            await self._stop_obs_relay()
            return

        source_info = source_player._current_stream_info
        if not source_info:
            await self._stop_obs_relay()
            return

        async with self._obs_relay_lock:
            same_guild = self._obs_relay_guild_id == source_player.guild_id
            task_running = bool(self._obs_relay_task and not self._obs_relay_task.done())

        if same_guild and task_running:
            return

        await self._stop_obs_relay()
        relay_task = asyncio.create_task(self._run_obs_relay(source_player.guild_id, source_info))
        async with self._obs_relay_lock:
            self._obs_relay_task = relay_task
            self._obs_relay_guild_id = source_player.guild_id

    async def _drain_obs_stderr(self, proc: asyncio.subprocess.Process, guild_id: int) -> None:
        """Drain ffmpeg stderr so the pipe cannot block the relay process."""
        try:
            if not proc.stderr:
                return
            while True:
                line = await proc.stderr.readline()
                if not line:
                    break
                msg = line.decode(errors="ignore").strip()
                if msg:
                    log.debug_cat(Category.API, "obs_relay_ffmpeg", guild_id=guild_id, message=msg[:300])
        except Exception:
            pass

    async def _run_obs_relay(self, guild_id: int, stream_info: StreamInfo) -> None:
        """Run FFmpeg that transcodes current track to MP3 and fanouts to HTTP subscribers."""
        from src.config import config

        ffmpeg_opts = self._build_ffmpeg_options(stream_info, bitrate=max(32, int(config.OBS_AUDIO_BITRATE_KBPS)))
        cmd = [
            "ffmpeg",
            "-hide_banner",
            "-loglevel",
            "error",
            *shlex.split(ffmpeg_opts["before_options"]),
            "-i",
            stream_info.url,
            *shlex.split(ffmpeg_opts["options"]),
            "-f",
            "mp3",
            "pipe:1",
        ]

        proc: asyncio.subprocess.Process | None = None
        stderr_task: asyncio.Task | None = None
        try:
            proc = await asyncio.create_subprocess_exec(
                *cmd,
                stdout=asyncio.subprocess.PIPE,
                stderr=asyncio.subprocess.PIPE,
            )
            async with self._obs_relay_lock:
                self._obs_relay_process = proc

            stderr_task = asyncio.create_task(self._drain_obs_stderr(proc, guild_id))
            log.info_cat(Category.API, "obs_relay_started", guild_id=guild_id)

            while True:
                if not proc.stdout:
                    break
                chunk = await proc.stdout.read(4096)
                if not chunk:
                    break

                async with self._obs_relay_lock:
                    subscribers = list(self._obs_audio_subscribers)

                for queue in subscribers:
                    try:
                        if queue.full():
                            try:
                                queue.get_nowait()
                            except asyncio.QueueEmpty:
                                pass
                        queue.put_nowait(chunk)
                    except Exception:
                        pass
        except asyncio.CancelledError:
            raise
        except Exception as e:
            log.warning_cat(Category.API, "obs_relay_failed", guild_id=guild_id, error=str(e))
        finally:
            if proc and proc.returncode is None:
                try:
                    proc.terminate()
                    await asyncio.wait_for(proc.wait(), timeout=2)
                except Exception:
                    try:
                        proc.kill()
                        await proc.wait()
                    except Exception:
                        pass

            if stderr_task and not stderr_task.done():
                stderr_task.cancel()
                try:
                    await stderr_task
                except Exception:
                    pass

            async with self._obs_relay_lock:
                if self._obs_relay_process is proc:
                    self._obs_relay_process = None
                if self._obs_relay_task and self._obs_relay_task.done():
                    self._obs_relay_task = None
                    self._obs_relay_guild_id = None
            log.info_cat(Category.API, "obs_relay_stopped", guild_id=guild_id)

    async def _log_track_start(self, player: GuildPlayer, item: QueueItem) -> int | None:
        """Log track start to database and update library."""
        if not hasattr(self.bot, "db") or not self.bot.db:
            return None
            
        try:
            playback_crud = PlaybackCRUD(self.bot.db)
            song_crud = SongCRUD(self.bot.db)
            user_crud = UserCRUD(self.bot.db)

            # Check Song Existence and Persistence Policy
            if not item.song_db_id:
                is_ephemeral = (item.discovery_source != "user_request")
                song = await song_crud.get_or_create_by_yt_id(
                    canonical_yt_id=item.video_id,
                    title=item.title,
                    artist_name=item.artist,
                    is_ephemeral=is_ephemeral,
                    duration_seconds=item.duration_seconds,
                    release_year=item.year
                )
                item.song_db_id = song["id"]
                if not is_ephemeral and song.get("is_ephemeral"):
                    await song_crud.make_permanent(song["id"])

            # Ensure user exists
            target_user_id = item.for_user_id or item.requester_id
            if target_user_id:
                member = player.voice_client.guild.get_member(target_user_id)
                username = member.name if member else "Unknown User"
                await user_crud.get_or_create(target_user_id, username)
            
            # Log play
            history_id = await playback_crud.log_track(
                session_id=player.session_id,
                song_id=item.song_db_id,
                discovery_source=item.discovery_source,
                discovery_reason=item.discovery_reason,
                for_user_id=target_user_id
            )

            # Update Library
            if item.discovery_source == "user_request" and target_user_id:
                from src.database.crud import LibraryCRUD
                lib_crud = LibraryCRUD(self.bot.db)
                await lib_crud.add_to_library(target_user_id, item.song_db_id, "request")
            
            return history_id
        except Exception as e:
            log.error_cat(Category.DATABASE, "Failed to log playback start", error=str(e))
            return None

    async def _resolve_stream(self, item: QueueItem) -> StreamInfo | None:
        """Resolve stream URL for an item with timeout."""
        try:
            return await asyncio.wait_for(
                self.youtube.get_stream_url(item.video_id),
                timeout=self.STREAM_FETCH_TIMEOUT
            )
        except asyncio.TimeoutError:
            log.warning_cat(Category.PLAYBACK, "Stream URL fetch timed out", video_id=item.video_id)
            return None
        except Exception as e:
            log.error_cat(Category.PLAYBACK, "Stream resolution failed", error=str(e))
            return None

    async def _notify_radio_presenter(self, player: GuildPlayer, item: QueueItem) -> None:
        """Notify external radio-presenter/TTS service that a song is starting."""
        try:
            if not await self._guild_bool_setting(player.guild_id, "radio_presenter_enabled", True):
                log.debug_cat(Category.API, "radio_presenter_notify_skipped", reason="guild_disabled", guild_id=player.guild_id)
                return

            from src.config import config

            url = getattr(config, "RADIO_PRESENTER_API_URL", None)
            if not url:
                log.debug_cat(Category.API, "radio_presenter_notify_skipped", reason="no_url")
                return

            now = datetime.now(UTC)
            if self._radio_presenter_disabled_until and now < self._radio_presenter_disabled_until:
                log.debug_cat(
                    Category.API,
                    "radio_presenter_notify_skipped",
                    reason="disabled_until",
                    disabled_until=self._radio_presenter_disabled_until.isoformat(),
                    url=url,
                )
                return

            # If we haven't checked connectivity yet, do a quick TCP probe once.
            if self._radio_presenter_enabled is None:
                ok = await self._radio_presenter_can_connect(url)
                self._radio_presenter_enabled = ok
                if not ok:
                    self._radio_presenter_disabled_until = now + timedelta(seconds=300)
                    self._radio_presenter_last_error = "initial_connect_failed"
                    log.warning_cat(
                        Category.API,
                        "radio_presenter_disabled",
                        guild_id=player.guild_id,
                        reason="initial_connect_failed",
                        disabled_for_s=300,
                        url=url,
                    )
                    return
            elif self._radio_presenter_enabled is False:
                log.debug_cat(Category.API, "radio_presenter_notify_skipped", reason="unreachable", url=url)
                return

            voice_channel_id = None
            if player.voice_client and getattr(player.voice_client, "channel", None):
                voice_channel_id = player.voice_client.channel.id

            requested_by = None
            if item.requester_id and player.voice_client and player.voice_client.guild:
                member = player.voice_client.guild.get_member(item.requester_id)
                requested_by = member.display_name if member else None
            if requested_by is None and item.requester_id:
                u = self.bot.get_user(item.requester_id)
                requested_by = u.display_name if u else None

            song_for = None
            if item.for_user_id and player.voice_client and player.voice_client.guild:
                member = player.voice_client.guild.get_member(item.for_user_id)
                song_for = member.display_name if member else None
            if song_for is None and item.for_user_id:
                u = self.bot.get_user(item.for_user_id)
                song_for = u.display_name if u else None

            voice_id = getattr(config, "RADIO_PRESENTER_VOICE", None) or None
            if hasattr(self.bot, "db") and self.bot.db:
                try:
                    guild_crud = GuildCRUD(self.bot.db)
                    setting = await guild_crud.get_setting(player.guild_id, "radio_presenter_voice")
                    if setting:
                        voice_id = str(setting).strip() or voice_id
                except Exception:
                    pass

            payload = {
                "song_name": item.title,
                "artist": item.artist,
                "guild_id": str(player.guild_id),
                "channel_id": voice_channel_id,
                "voice": voice_id,
                "requested_by": requested_by,
                "song_for": song_for,
            }

            log.debug_cat(
                Category.API,
                "radio_presenter_notify_start",
                guild_id=player.guild_id,
                url=url,
                song=item.title,
                artist=item.artist,
            )
            t0 = time.perf_counter()
            timeout = aiohttp.ClientTimeout(total=3)
            async with aiohttp.ClientSession(timeout=timeout) as session:
                async with session.post(url, json=payload) as resp:
                    body = None
                    try:
                        body = await resp.text()
                    except Exception:
                        await resp.read()
                    ms = int((time.perf_counter() - t0) * 1000)
                    if 200 <= resp.status < 300:
                        self._radio_presenter_enabled = True
                        self._radio_presenter_last_error = None
                        log.info_cat(
                            Category.API,
                            "radio_presenter_notified",
                            guild_id=player.guild_id,
                            status=resp.status,
                            ms=ms,
                            song=item.title,
                            artist=item.artist,
                        )
                    else:
                        self._radio_presenter_enabled = False
                        self._radio_presenter_disabled_until = datetime.now(UTC) + timedelta(seconds=300)
                        self._radio_presenter_last_error = f"http_{resp.status}"
                        log.warning_cat(
                            Category.API,
                            "radio_presenter_disabled",
                            guild_id=player.guild_id,
                            reason=f"http_{resp.status}",
                            disabled_for_s=300,
                            ms=ms,
                            url=url,
                            response=(body[:500] if isinstance(body, str) else None),
                            song=item.title,
                            artist=item.artist,
                        )
        except Exception as e:
            self._radio_presenter_enabled = False
            self._radio_presenter_disabled_until = datetime.now(UTC) + timedelta(seconds=300)
            self._radio_presenter_last_error = str(e)
            log.warning_cat(
                Category.API,
                "radio_presenter_notify_failed",
                guild_id=player.guild_id,
                error=str(e),
                song=getattr(item, "title", None),
            )

    async def _radio_presenter_can_connect(self, url: str) -> bool:
        """Check if the radio presenter host/port is reachable via TCP."""
        try:
            parsed = urlparse(url)
            host = parsed.hostname
            port = parsed.port or (443 if parsed.scheme == "https" else 80)
            if not host:
                return False

            async def _probe():
                reader, writer = await asyncio.open_connection(host, port)
                writer.close()
                try:
                    await writer.wait_closed()
                except Exception:
                    pass

            await asyncio.wait_for(_probe(), timeout=1.5)
            return True
        except Exception:
            return False

    async def _radio_presenter_health_loop(self) -> None:
        """Periodically check connectivity and re-enable the integration when it comes back."""
        log.info_cat(Category.API, "radio_presenter_health_loop_started")
        await asyncio.sleep(2)
        first = True
        while True:
            try:
                from src.config import config

                url = getattr(config, "RADIO_PRESENTER_API_URL", None)
                if not url:
                    self._radio_presenter_enabled = None
                    await asyncio.sleep(30)
                    continue

                ok = await self._radio_presenter_can_connect(url)
                if ok:
                    if self._radio_presenter_enabled is False:
                        log.info_cat(Category.API, "radio_presenter_reenabled", url=url)
                    self._radio_presenter_enabled = True
                    self._radio_presenter_disabled_until = None
                    self._radio_presenter_last_error = None
                else:
                    if self._radio_presenter_enabled is True:
                        log.warning_cat(Category.API, "radio_presenter_unreachable", url=url)
                    self._radio_presenter_enabled = False
                if first:
                    first = False
                    log.info_cat(Category.API, "radio_presenter_health_status", url=url, ok=ok)

            except asyncio.CancelledError:
                raise
            except Exception as e:
                log.debug_cat(Category.API, "radio_presenter_health_check_failed", error=str(e))

            await asyncio.sleep(60)

    async def _radio_presenter_check_once(self) -> None:
        """One-shot health check for visibility on startup/reload."""
        try:
            from src.config import config

            url = getattr(config, "RADIO_PRESENTER_API_URL", None)
            if not url:
                log.info_cat(Category.API, "radio_presenter_check_skipped", reason="no_url")
                return

            ok = await self._radio_presenter_can_connect(url)
            self._radio_presenter_enabled = ok if self._radio_presenter_enabled is None else self._radio_presenter_enabled
            log.info_cat(Category.API, "radio_presenter_check", url=url, ok=ok)
        except Exception as e:
            log.debug_cat(Category.API, "radio_presenter_check_failed", error=str(e))
    

    # ==================== PLAYBACK LOOP ====================
    
    async def _play_loop(self, player: GuildPlayer):
        """Main playback loop for a guild with self-healing capabilities."""
        player.is_playing = True
        player._consecutive_failures = 0
        
        # Start queue maintenance
        if not player._maintenance_task or player._maintenance_task.done():
            player._maintenance_task = asyncio.create_task(self._maintain_queue(player))

        try:
            while player.voice_client and player.voice_client.is_connected():
                player.skip_votes.clear()
                player._last_health_check = datetime.now(UTC)
                
                # 1. Get next item from queue
                try:
                    if player.queue.empty():
                        if not player.autoplay:
                            break
                        await self._fill_queue_if_needed(player)
                    
                    item = await asyncio.wait_for(player.queue.get(), timeout=10.0)
                except (asyncio.TimeoutError, asyncio.QueueEmpty):
                    continue
                except Exception as e:
                    log.error_cat(Category.PLAYBACK, "Error getting next item", error=str(e))
                    break

                player.current = item
                player.last_activity = datetime.now(UTC)
                
                # 2. Database: Ensure session and log playback
                await self._ensure_session(player)
                item.history_id = await self._log_track_start(player, item)

                # 3. Get stream URL (if not already prefetched)
                if not item.url:
                    stream_info = await self._resolve_stream(item)
                    if not stream_info:
                        player._consecutive_failures += 1
                        continue
                    item.url = stream_info.url
                else:
                    stream_info = StreamInfo(url=item.url)

                player._current_stream_info = stream_info

                player._consecutive_failures = 0

                # 4. Prefetch next song's URL if it's already in queue
                if player.pre_buffer and not player.queue.empty():
                    asyncio.create_task(self._pre_buffer_next(player))

                # 5. Play the audio
                try:
                    if player.voice_client.is_playing() or player.voice_client.is_paused():
                        # Clear any stale source before starting the next track.
                        player.voice_client.stop()
                        await asyncio.sleep(0.15)

                    bitrate = 128
                    if player.voice_client.channel:
                        bitrate = min(512, player.voice_client.channel.bitrate // 1000)
                    
                    ffmpeg_opts = self._build_ffmpeg_options(stream_info, bitrate=bitrate)
                    source = await discord.FFmpegOpusAudio.from_probe(item.url, **ffmpeg_opts)
                    player._current_source = source
                    
                    play_complete = asyncio.Event()
                    player.voice_client.play(source, after=lambda _: self.bot.loop.call_soon_threadsafe(play_complete.set))
                    player.start_time = datetime.now(UTC)
                    await self._ensure_obs_relay_state()

                    asyncio.create_task(self._spotify_enrich_and_refresh_now_playing(player, item))

                    should_announce, announce_reason, roll = self._should_announce_radio_presenter(item)
                    if should_announce:
                        asyncio.create_task(self._notify_radio_presenter(player, item))
                    else:
                        log.debug_cat(
                            Category.API,
                            "radio_presenter_announce_skipped",
                            guild_id=player.guild_id,
                            reason=announce_reason,
                            roll=roll,
                            song=item.title,
                            artist=item.artist,
                        )
                    await self._notify_now_playing(player)
                    
                    max_wait = (item.duration_seconds or 600) + 60
                    try:
                        await asyncio.wait_for(play_complete.wait(), timeout=max_wait)
                    except asyncio.TimeoutError:
                        log.warning_cat(Category.PLAYBACK, "Playback timeout - auto-healing", title=item.title)
                        if player.voice_client.is_playing():
                            player.voice_client.stop()
                    
                    if hasattr(self.bot, "db") and self.bot.db and item.history_id:
                        playback_crud = PlaybackCRUD(self.bot.db)
                        completed = not (player.skip_votes and len(player.skip_votes) > 0)
                        await playback_crud.mark_completed(item.history_id, completed)

                except Exception as e:
                    log.event(Category.PLAYBACK, Event.PLAYBACK_ERROR, level=logging.ERROR, title=item.title, error=str(e))
                    continue
                finally:
                    player.current = None
                    player._current_source = None
                    player._current_stream_info = None
                    await self._ensure_obs_relay_state()
                    # Trigger maintenance after song ends
                    asyncio.create_task(self._fill_queue_if_needed(player))
        
        finally:
            player.is_playing = False
            player.current = None
            player._current_source = None
            player._current_stream_info = None
            if player._play_task is asyncio.current_task():
                player._play_task = None
            await self._ensure_obs_relay_state()
            if player._maintenance_task:
                player._maintenance_task.cancel()

    async def _fill_queue_if_needed(self, player: GuildPlayer):
        """Fill the queue with discovery songs if it drops below 4 items."""
        if not player.autoplay:
            return

        # Get max duration setting
        max_seconds = 0
        if hasattr(self.bot, "db") and self.bot.db:
            try:
                guild_crud = GuildCRUD(self.bot.db)
                max_dur = await guild_crud.get_setting(player.guild_id, "max_song_duration")
                if max_dur:
                    max_seconds = int(max_dur) * 60
            except: pass

        TARGET_SIZE = 4
        while player.queue.qsize() < TARGET_SIZE:
            log.debug_cat(Category.DISCOVERY, "Queue low, fetching discovery song", 
                         current_size=player.queue.qsize(), target=TARGET_SIZE)
            item = await self._get_discovery_song_with_retry(player, max_seconds=max_seconds)
            if item:
                await player.queue.put(item)
                # Prefetch stream URL for the first item in queue if it doesn't have one
                if player.queue.qsize() == 1:
                    asyncio.create_task(self._pre_buffer_next(player))
            else:
                break

    async def _maintain_queue(self, player: GuildPlayer):
        """Background task to keep the queue filled while the player is active."""
        try:
            while player.voice_client and player.voice_client.is_connected():
                if player.autoplay:
                    await self._fill_queue_if_needed(player)
                await asyncio.sleep(30)  # Check every 30 seconds
        except asyncio.CancelledError:
            pass
            player.current = None
    
    async def _get_discovery_song(self, player: GuildPlayer) -> QueueItem | None:
        """Get next song from discovery engine."""
        # Get voice channel members
        if not player.voice_client or not player.voice_client.channel:
            return None
        
        voice_members = [m.id for m in player.voice_client.channel.members if not m.bot]
        if not voice_members:
            return None
        
        # Try discovery engine first
        if hasattr(self.bot, "discovery") and self.bot.discovery:
            try:
                # Get Cooldown Setting
                cooldown = 7200 # Default 2 hours
                if hasattr(self.bot, "db"):
                    guild_crud = GuildCRUD(self.bot.db)
                    setting = await guild_crud.get_setting(player.guild_id, "replay_cooldown")
                    if setting:
                        try:
                            cooldown = int(setting)
                        except ValueError:
                            pass

                discovered = await self.bot.discovery.get_next_song(
                    player.guild_id,
                    voice_members,
                    cooldown_seconds=cooldown
                )
                if discovered:
                    # Normalize discovery source names for DB compatibility across schema versions.
                    # Some older DBs used 'artist' and some used 'same_artist'.
                    source_map = {"same_artist": "artist"}
                    db_source = source_map.get(discovered.strategy, discovered.strategy)
                    return QueueItem(
                        video_id=discovered.video_id,
                        title=discovered.title,
                        artist=discovered.artist,
                        discovery_source=db_source,
                        discovery_reason=discovered.reason,
                        for_user_id=discovered.for_user_id,
                        duration_seconds=discovered.duration_seconds,
                        genre=discovered.genre,
                        year=discovered.year,
                    )
            except Exception as e:
                log.event(Category.DISCOVERY, Event.DISCOVERY_FAILED, level=logging.ERROR, error=str(e))
        else:
            log.warning_cat(Category.DISCOVERY, "Discovery engine not initialized")
        
        # Fallback: Get random track from charts
        log.event(Category.DISCOVERY, "fallback_to_charts", guild_id=player.guild_id)
        return await self._get_chart_fallback()
    
    async def _get_discovery_song_with_retry(self, player: GuildPlayer, max_seconds: int = 0) -> QueueItem | None:
        """Get discovery song with retry logic for duration limits."""
        for attempt in range(3):
            item = await self._get_discovery_song(player)
            if not item:
                return None
            
            if max_seconds > 0 and item.duration_seconds and item.duration_seconds > max_seconds:
                log.event(Category.DISCOVERY, "song_skipped_duration", 
                         title=item.title, duration=item.duration_seconds, 
                         max_duration=max_seconds, attempt=attempt + 1)
                continue
            return item
        return None
    
    async def _prefetch_discovery_song(self, player: GuildPlayer):
        """Prefetch the next discovery song in background to reduce delay."""
        if player._next_discovery:
            return  # Already have one prefetched
        
        try:
            # Get max duration setting
            guild_crud = GuildCRUD(self.bot.db) if hasattr(self.bot, "db") else None
            max_seconds = 0
            if guild_crud:
                try:
                    max_dur = await guild_crud.get_setting(player.guild_id, "max_song_duration")
                    if max_dur:
                        max_seconds = int(max_dur) * 60
                except: pass
            
            # Get discovery song with timeout
            item = await asyncio.wait_for(
                self._get_discovery_song_with_retry(player, max_seconds),
                timeout=self.DISCOVERY_TIMEOUT
            )
            
            if item:
                # Also prefetch stream URL for zero-delay playback
                try:
                    stream_info = await asyncio.wait_for(
                        self.youtube.get_stream_url(item.video_id),
                        timeout=self.STREAM_FETCH_TIMEOUT
                    )
                    if stream_info:
                        item.url = stream_info.url
                        log.debug_cat(Category.DISCOVERY, "Prefetched discovery song with URL", title=item.title)
                except asyncio.TimeoutError:
                    log.debug_cat(Category.DISCOVERY, "Prefetch stream URL timed out", title=item.title)
                
                player._next_discovery = item
                
        except asyncio.TimeoutError:
            log.debug_cat(Category.DISCOVERY, "Discovery prefetch timed out")
        except Exception as e:
            log.debug_cat(Category.DISCOVERY, "Discovery prefetch failed", error=str(e))
    
    async def _get_chart_fallback(self) -> QueueItem | None:
        """Get a random track from Top 100 US/UK charts as fallback."""
        import random
        
        region = random.choice(["US", "UK"])
        query = f"Top 100 Songs {region} 2024"
        
        log.event(Category.DISCOVERY, Event.SEARCH_STARTED, query=query, type="chart_playlist")
        
        # Try to find a chart playlist
        playlists = await self.youtube.search_playlists(query, limit=3)
        
        if playlists:
            playlist = random.choice(playlists)
            log.event(Category.DISCOVERY, Event.SEARCH_COMPLETED, playlist=playlist.get('title', 'Unknown'))
            
            # Get tracks from playlist
            tracks = await self.youtube.get_playlist_tracks(playlist["browse_id"], limit=50)
            if tracks:
                track = random.choice(tracks)
                return QueueItem(
                    video_id=track.video_id,
                    title=track.title,
                    artist=track.artist,
                    discovery_source="wildcard",
                    discovery_reason=f"🎲 Random from {region} Top 100",
                    duration_seconds=track.duration_seconds,
                    year=track.year
                )
        
        # Direct search fallback - search for popular songs
        log.event(Category.DISCOVERY, "fallback_direct_search")
        results = await self.youtube.search("top hits 2024 popular", filter_type="songs", limit=20)
        
        if results:
            track = random.choice(results)
            log.event(Category.DISCOVERY, Event.SEARCH_COMPLETED, title=track.title, type="direct_search")
            return QueueItem(
                video_id=track.video_id,
                title=track.title,
                artist=track.artist,
                discovery_source="wildcard",
                discovery_reason="🎲 Popular track from charts",
                duration_seconds=track.duration_seconds,
                year=track.year
            )
        
        log.warning_cat(Category.DISCOVERY, "No chart tracks found via any method")
        return None

    async def _notify_now_playing(self, player: GuildPlayer):
        """Ask the NowPlaying cog to render/update the Now Playing message."""
        log.debug_cat(Category.SYSTEM, "_notify_now_playing called", guild_id=player.guild_id)
        np = self.bot.get_cog("NowPlayingCog")
        if not np:
            log.debug_cat(Category.SYSTEM, "NowPlayingCog not found", guild_id=player.guild_id)
            return
        send_fn = getattr(np, "send_now_playing_for_player", None)
        if not send_fn:
            log.debug_cat(Category.SYSTEM, "send_now_playing_for_player method not found", guild_id=player.guild_id)
            return
        try:
            await send_fn(player)
        except Exception as e:
            log.debug_cat(Category.SYSTEM, "NowPlaying update failed", error=str(e), guild_id=player.guild_id)

    async def _spotify_enrich_and_refresh_now_playing(self, player: GuildPlayer, item: QueueItem):
        """Enrich current track metadata via Spotify without delaying playback, then refresh Now Playing only if data changed."""
        spotify = getattr(self.bot, "spotify", None)
        if not spotify:
            return

        if item.year and item.genre:
            return

        # Track what changed to decide if we should refresh
        metadata_changed = False

        try:
            query = f"{item.artist} {item.title}"
            sp_track = await asyncio.wait_for(
                spotify.search_track(query),
                timeout=self.SPOTIFY_ENRICH_TIMEOUT,
            )
            if not sp_track:
                return

            if not item.year:
                item.year = sp_track.release_year
                metadata_changed = True

            artist = await asyncio.wait_for(
                spotify.get_artist(sp_track.artist_id),
                timeout=self.SPOTIFY_ENRICH_TIMEOUT,
            )
            if artist and artist.genres and not item.genre:
                item.genre = artist.genres[0].title()
                metadata_changed = True

            if hasattr(self.bot, "db") and self.bot.db and item.song_db_id:
                try:
                    song_crud = SongCRUD(self.bot.db)

                    if item.genre:
                        await song_crud.clear_genres(item.song_db_id)
                        await song_crud.add_genre(item.song_db_id, item.genre)

                    await song_crud.get_or_create_by_yt_id(
                        canonical_yt_id=item.video_id,
                        title=item.title,
                        artist_name=item.artist,
                        release_year=item.year,
                        duration_seconds=item.duration_seconds,
                    )
                except Exception as e:
                    log.debug_cat(Category.DATABASE, "Failed to persist Spotify enrichment", error=str(e))

        except asyncio.TimeoutError:
            log.debug_cat(Category.API, "Spotify enrichment timed out", title=item.title, artist=item.artist)
            return
        except Exception as e:
            log.debug_cat(Category.API, "Spotify enrichment failed", error=str(e))
            return

        # Only refresh Now Playing if metadata actually changed and this is still the current song
        if metadata_changed:
            try:
                # Give the initial Now Playing send a chance to complete to avoid racing two sends.
                await asyncio.sleep(3)
                if not player.current or player.current.video_id != item.video_id:
                    return
                await self._notify_now_playing(player)
            except Exception as e:
                log.debug_cat(Category.SYSTEM, "Failed to refresh Now Playing after Spotify enrichment", error=str(e))
    

    async def _pre_buffer_next(self, player: GuildPlayer):
        """Pre-buffer the next song's URL."""
        try:
            # Peek at next item without removing
            if player.queue.empty():
                return

            next_item = list(player.queue._queue)[0]
            if not next_item.url:
                stream_info = await self.youtube.get_stream_url(next_item.video_id)
                if stream_info:
                    next_item.url = stream_info.url
                    player._next_url = stream_info.url
                    log.debug_cat(Category.QUEUE, "Pre-buffered URL", title=next_item.title)
        except Exception as e:
            log.debug_cat(Category.QUEUE, "Pre-buffer failed", error=str(e))
    
    async def _idle_check_loop(self):
        """Check for idle players, stuck players, and disconnect when needed."""
        STUCK_THRESHOLD = 300  # 5 minutes without health check update = stuck
        
        while True:
            await asyncio.sleep(60)  # Check every minute
            
            now = datetime.now(UTC)
            for guild_id, player in list(self.players.items()):
                if not player.voice_client or not player.voice_client.is_connected():
                    continue

                # Check if idle for too long (only when truly inactive).
                idle_seconds = (now - player.last_activity).total_seconds()
                vc = player.voice_client
                vc_active = False
                try:
                    vc_active = bool(vc and (vc.is_playing() or vc.is_paused()))
                except Exception:
                    vc_active = False

                has_active_work = bool(
                    player.is_playing
                    or vc_active
                    or player.current is not None
                    or not player.queue.empty()
                    or player.autoplay
                )
                if idle_seconds > self.IDLE_TIMEOUT and not has_active_work:
                    log.event(Category.VOICE, Event.VOICE_DISCONNECTED, guild_id=guild_id, reason="idle_timeout")
                    await player.voice_client.disconnect()
                    player.voice_client = None
                    continue
                
                # Check if player is stuck
                if player.is_playing:
                    time_since_health = (now - player._last_health_check).total_seconds()
                    if time_since_health > STUCK_THRESHOLD:
                        log.warning_cat(Category.PLAYBACK, "Stuck player detected - auto-restarting", 
                                      guild_id=guild_id, stuck_seconds=time_since_health)
                        
                        try:
                            if player.voice_client.is_playing():
                                player.voice_client.stop()
                        except Exception:
                            pass
                        
                        player.is_playing = False
                        player._consecutive_failures = 0
                        # Restart loop happens in next iteration if autoplay is on or queue not empty

    async def reconcile_voice_state(self) -> None:
        """Reconcile internal player voice state with discord.py's guild voice clients."""
        for guild_id, player in list(self.players.items()):
            guild = self.bot.get_guild(guild_id)
            guild_vc = guild.voice_client if guild else None

            # Prefer discord.py's active guild voice client as source of truth.
            if guild_vc and guild_vc.is_connected():
                if player.voice_client is not guild_vc:
                    player.voice_client = guild_vc
                    log.debug_cat(Category.VOICE, "voice_state_reconciled", guild_id=guild_id, action="attach_guild_vc")

                # Auto-heal playback loop if queue exists and we are connected but loop isn't running.
                if (not player.is_playing) and (player.current or not player.queue.empty()):
                    await self.ensure_play_loop(player, reason="watchdog_reconcile")
                continue

            # No connected guild VC -> normalize stale state.
            if player.voice_client and not player.voice_client.is_connected():
                player.voice_client = None
            if not guild_vc and player.is_playing:
                player.is_playing = False

    async def _confirm_voice_disconnect(self, guild_id: int, guild_name: str) -> None:
        """Confirm a bot voice disconnect with retry window to tolerate transient WS drops."""
        # Voice reconnects can take a few seconds; only declare disconnect after repeated checks.
        for delay_s in (1.0, 1.5, 2.0, 2.5):
            await asyncio.sleep(delay_s)

            player = self.players.get(guild_id)
            if not player:
                return

            vc = player.voice_client
            if vc and vc.is_connected():
                return

            guild = self.bot.get_guild(guild_id)
            guild_vc = guild.voice_client if guild else None
            if guild_vc:
                # Keep attached to guild VC while reconnect is in-flight.
                player.voice_client = guild_vc
                if guild_vc.is_connected():
                    if player.current or not player.queue.empty():
                        try:
                            await self.ensure_play_loop(player, reason="voice_reconnect_confirm")
                        except Exception:
                            pass
                return

            # If member voice state still shows us in a channel, reconnect is not settled yet.
            try:
                me = guild.me if guild else None
                if me and me.voice and me.voice.channel:
                    continue
            except Exception:
                pass

        player = self.players.get(guild_id)
        if not player:
            return
        player.voice_client = None
        player.is_playing = False
        log.event(Category.VOICE, Event.VOICE_DISCONNECTED, guild=guild_name, reason="bot_disconnected")

    async def _resume_playback_after_reconnect(self, guild_id: int, *, delay_s: float = 2.5) -> None:
        """Best-effort delayed resume after voice reconnect completes."""
        await asyncio.sleep(delay_s)
        player = self.players.get(guild_id)
        if not player:
            return

        guild = self.bot.get_guild(guild_id)
        guild_vc = guild.voice_client if guild else None
        if not guild_vc or not guild_vc.is_connected():
            return

        player.voice_client = guild_vc
        if player.current or not player.queue.empty():
            try:
                await self.ensure_play_loop(player, reason="voice_reconnect_delayed")
            except Exception as e:
                log.debug_cat(
                    Category.PLAYBACK,
                    "playback_loop_delayed_resume_failed",
                    guild_id=guild_id,
                    error=str(e),
                )
    
    # ==================== EVENTS ====================
    
    @commands.Cog.listener()
    async def on_voice_state_update(
        self,
        member: discord.Member,
        before: discord.VoiceState,
        after: discord.VoiceState
    ):
        """Handle voice state changes."""
        # Handle bot being disconnected or moved
        if member.id == self.bot.user.id:
            if after.channel:
                player = self.players.get(member.guild.id)
                if player and member.guild.voice_client:
                    player.voice_client = member.guild.voice_client
                    # Voice WS can reconnect without a fresh /play command.
                    # If there is pending work, ensure playback loop is running again.
                    if (
                        player.voice_client.is_connected()
                        and (player.current or not player.queue.empty())
                    ):
                        try:
                            await self.ensure_play_loop(player, reason="voice_reconnect")
                        except Exception as e:
                            log.debug_cat(
                                Category.PLAYBACK,
                                "playback_loop_resume_failed",
                                guild_id=member.guild.id,
                                error=str(e),
                            )
                    # Also schedule a delayed check to handle out-of-order reconnect events.
                    asyncio.create_task(self._resume_playback_after_reconnect(member.guild.id))
            elif before.channel and not after.channel:
                asyncio.create_task(self._confirm_voice_disconnect(member.guild.id, member.guild.name))
            return

        if member.bot:
            return
        
        player = self.players.get(member.guild.id)
        if not player or not player.voice_client or not player.voice_client.channel:
            return
        
        # Check if bot is alone in its current voice channel
        if before.channel == player.voice_client.channel and not after.channel == player.voice_client.channel:
            members = [m for m in player.voice_client.channel.members if not m.bot]
            if not members:
                # Everyone left, stop and disconnect
                if player.voice_client.is_playing():
                    player.voice_client.stop()
                await player.voice_client.disconnect()
                player.voice_client = None
                log.event(Category.VOICE, Event.VOICE_DISCONNECTED, guild=member.guild.name, reason="everyone_left")


async def setup(bot: commands.Bot):
    """Load the music cog."""
    await bot.add_cog(MusicCog(bot))
