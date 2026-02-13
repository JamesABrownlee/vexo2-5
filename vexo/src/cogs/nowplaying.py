"""
Now Playing cog.

Owns:
- `/nowplaying` command
- Now Playing message rendering/sending
- persistence/cleanup of the last Now Playing message per guild (across restarts)
- interactive view buttons
"""
import aiohttp
import asyncio
import io
import json
import time
from datetime import datetime, UTC
from urllib.parse import quote_plus

import discord
from discord import app_commands
from discord.ext import commands

from src.database.crud import SongCRUD, ReactionCRUD, LibraryCRUD, NowPlayingMessageCRUD
from src.utils.logging import get_logger, Category

log = get_logger(__name__)


class NowPlayingView(discord.ui.View):
    """Interactive Now Playing controls with dynamic queue select."""

    # Persistent view: timeout must be None and every component needs a custom_id.
    def __init__(self, bot: commands.Bot, queue_items: list = None):
        super().__init__(timeout=None)
        self.bot = bot
        self._busy_lock = asyncio.Lock()
        self._interaction_cooldown_s: float = 3.0
        self._cooldown_until_monotonic: float = 0.0
        self._cooldown_task: asyncio.Task | None = None

        # Add select menu with queue items if provided.
        # Note: discord.py adds decorator-defined children during View.__init__.
        # If we add the select after that, it appears at the end; we want it first.
        if queue_items:
            existing_items = list(self.children)

            select_options = [
                discord.SelectOption(
                    label=f"{i+1}. {qi.title[:50]}",
                    description=qi.artist[:100],
                    value=str(i),
                )
                for i, qi in enumerate(queue_items[:10])  # Limit to 10 items
            ]

            if select_options:
                select = discord.ui.Select(
                    placeholder="⏭️ Choose next song...",
                    custom_id="np:skip_to",
                    options=select_options,
                    min_values=1,
                    max_values=1,
                    row=0,
                )
                select.callback = self.skip_to_callback

                self.clear_items()
                self.add_item(select)
                for item in existing_items:
                    self.add_item(item)

    @property
    def music(self):
        return self.bot.get_cog("MusicCog")

    async def _set_all_disabled(self, disabled: bool, interaction: discord.Interaction | None = None) -> None:
        for item in self.children:
            try:
                item.disabled = disabled
            except Exception:
                pass

        if interaction is not None:
            try:
                msg = getattr(interaction, "message", None)
                if msg is not None:
                    await msg.edit(view=self)
            except Exception:
                pass

    async def _begin_interaction_cooldown(self, interaction: discord.Interaction) -> bool:
        now = time.monotonic()
        if now < self._cooldown_until_monotonic:
            remaining = max(0.0, self._cooldown_until_monotonic - now)
            await self._safe_send(interaction, f"⏳ Controls cooling down. Try again in {remaining:.1f}s.", ephemeral=True)
            return False

        self._cooldown_until_monotonic = now + self._interaction_cooldown_s
        await self._set_all_disabled(True, interaction)
        return True

    async def _end_interaction_cooldown(self, interaction: discord.Interaction, *, keep_disabled: bool = False) -> None:
        if self._cooldown_task and not self._cooldown_task.done():
            self._cooldown_task.cancel()

        if keep_disabled:
            self._cooldown_until_monotonic = 0.0
            await self._set_all_disabled(True, interaction)
            return

        async def _release() -> None:
            try:
                await asyncio.sleep(self._interaction_cooldown_s)
            except asyncio.CancelledError:
                return
            self._cooldown_until_monotonic = 0.0
            await self._set_all_disabled(False, interaction)

        self._cooldown_task = asyncio.create_task(_release())

    def _guild_id_from_interaction(self, interaction: discord.Interaction) -> int | None:
        try:
            return int(interaction.guild_id) if interaction.guild_id else None
        except Exception:
            return None

    async def _safe_defer(self, interaction: discord.Interaction, *, ephemeral: bool = True) -> bool:
        try:
            t0 = time.perf_counter()
            try:
                # Prefer "thinking" ACK for components, but support older discord libs.
                try:
                    await interaction.response.defer(thinking=True, ephemeral=ephemeral)
                except TypeError:
                    try:
                        await interaction.response.defer(ephemeral=ephemeral)
                    except TypeError:
                        await interaction.response.defer()
            except discord.InteractionResponded:
                log.debug_cat(
                    Category.USER,
                    "interaction_defer_already_responded",
                    module=__name__,
                    interaction_id=getattr(interaction, "id", None),
                    custom_id=(interaction.data or {}).get("custom_id") if isinstance(interaction.data, dict) else None,
                    guild_id=getattr(interaction, "guild_id", None),
                    channel_id=getattr(getattr(interaction, "channel", None), "id", None),
                    message_id=getattr(getattr(interaction, "message", None), "id", None),
                    user_id=getattr(getattr(interaction, "user", None), "id", None),
                )
                return True
            ms = int((time.perf_counter() - t0) * 1000)
            # If defer itself took a long time, it's a strong signal of event-loop lag / network delay.
            log_fn = log.warning_cat if ms >= 2500 else log.info_cat
            log_fn(
                Category.USER,
                "interaction_defer_ok",
                module=__name__,
                interaction_id=getattr(interaction, "id", None),
                interaction_type=str(getattr(getattr(interaction, "type", None), "name", getattr(interaction, "type", None))),
                custom_id=(interaction.data or {}).get("custom_id") if isinstance(interaction.data, dict) else None,
                guild_id=getattr(interaction, "guild_id", None),
                channel_id=getattr(getattr(interaction, "channel", None), "id", None),
                message_id=getattr(getattr(interaction, "message", None), "id", None),
                user_id=getattr(getattr(interaction, "user", None), "id", None),
                ms=ms,
            )
            return True
        except discord.NotFound:
            log.warning_cat(
                Category.USER,
                "interaction_defer_not_found",
                module=__name__,
                interaction_id=getattr(interaction, "id", None),
                custom_id=(interaction.data or {}).get("custom_id") if isinstance(interaction.data, dict) else None,
                guild_id=getattr(interaction, "guild_id", None),
                channel_id=getattr(getattr(interaction, "channel", None), "id", None),
                message_id=getattr(getattr(interaction, "message", None), "id", None),
                user_id=getattr(getattr(interaction, "user", None), "id", None),
            )
            return False
        except Exception as e:
            log.exception_cat(Category.SYSTEM, "Failed to defer NowPlayingView interaction", error=str(e))
            return False

    async def _safe_send(self, interaction: discord.Interaction, content: str, *, ephemeral: bool = True) -> None:
        try:
            if interaction.response.is_done():
                await interaction.followup.send(content, ephemeral=ephemeral)
            else:
                await interaction.response.send_message(content, ephemeral=ephemeral)
        except discord.NotFound:
            return
        except Exception as e:
            log.exception_cat(Category.SYSTEM, "Failed to send NowPlayingView response", error=str(e))
            return

    async def on_error(self, interaction: discord.Interaction, error: Exception, item: discord.ui.Item):
        try:
            log.exception_cat(
                Category.SYSTEM,
                "NowPlayingView item callback error",
                error=str(error),
                item_type=type(item).__name__,
                guild_id=self._guild_id_from_interaction(interaction),
            )
        except Exception:
            pass

        try:
            await self._safe_send(interaction, "❌ That button failed. Try again.", ephemeral=True)
        except Exception:
            return

    @discord.ui.button(emoji="⏸", style=discord.ButtonStyle.secondary, custom_id="np:pause_resume")
    async def pause_resume(self, interaction: discord.Interaction, button: discord.ui.Button):
        with log.span(
            Category.USER,
            "np_button_pause_resume",
            module=__name__,
            view="NowPlayingView",
            custom_id=getattr(button, "custom_id", None),
            interaction_id=getattr(interaction, "id", None),
            guild_id=interaction.guild_id,
            channel_id=getattr(interaction.channel, "id", None),
            message_id=getattr(getattr(interaction, "message", None), "id", None),
            user_id=getattr(interaction.user, "id", None),
        ):
            if not await self._safe_defer(interaction, ephemeral=True):
                return

        music = self.music
        if not music:
            return

        guild_id = self._guild_id_from_interaction(interaction)
        if not guild_id:
            await self._safe_send(interaction, "❌ This button can only be used in a server.", ephemeral=True)
            return

        if self._busy_lock.locked():
            await self._safe_send(interaction, "⏳ Bot is busy — try again in a moment.", ephemeral=True)
            return

        if not await self._begin_interaction_cooldown(interaction):
            return

        async with self._busy_lock:
            try:
                player = music.get_player(guild_id)
                if player.voice_client:
                    if player.voice_client.is_playing():
                        player.voice_client.pause()
                        await self._safe_send(interaction, "⏸ Paused", ephemeral=True)
                    elif player.voice_client.is_paused():
                        player.voice_client.resume()
                        await self._safe_send(interaction, "▶ Resumed", ephemeral=True)
            except Exception as e:
                log.exception_cat(Category.SYSTEM, "NowPlayingView pause/resume failed", error=str(e))
                return
            finally:
                await self._end_interaction_cooldown(interaction)

    @discord.ui.button(emoji="⏹", style=discord.ButtonStyle.danger, custom_id="np:stop")
    async def stop(self, interaction: discord.Interaction, button: discord.ui.Button):
        with log.span(
            Category.USER,
            "np_button_stop",
            module=__name__,
            view="NowPlayingView",
            custom_id=getattr(button, "custom_id", None),
            interaction_id=getattr(interaction, "id", None),
            guild_id=interaction.guild_id,
            channel_id=getattr(interaction.channel, "id", None),
            message_id=getattr(getattr(interaction, "message", None), "id", None),
            user_id=getattr(interaction.user, "id", None),
        ):
            if not await self._safe_defer(interaction, ephemeral=True):
                return

        music = self.music
        if not music:
            return

        guild_id = self._guild_id_from_interaction(interaction)
        if not guild_id:
            await self._safe_send(interaction, "❌ This button can only be used in a server.", ephemeral=True)
            return

        if self._busy_lock.locked():
            await self._safe_send(interaction, "⏳ Bot is busy — try again in a moment.", ephemeral=True)
            return

        if not await self._begin_interaction_cooldown(interaction):
            return

        async with self._busy_lock:
            try:
                player = music.get_player(guild_id)
                if not player.voice_client:
                    return

                while not player.queue.empty():
                    try:
                        player.queue.get_nowait()
                    except asyncio.QueueEmpty:
                        break

                if player.is_playing or player.voice_client.is_playing():
                    player.voice_client.stop()

                await player.voice_client.disconnect()
                player.voice_client = None

                await self._safe_send(interaction, "⏹ Stopped and cleared queue!", ephemeral=True)
                # Keep disabled until a new Now Playing message is posted.
            except Exception as e:
                log.exception_cat(Category.SYSTEM, "NowPlayingView stop failed", error=str(e))
                return
            finally:
                await self._end_interaction_cooldown(interaction, keep_disabled=True)

    @discord.ui.button(emoji="⏭", style=discord.ButtonStyle.secondary, custom_id="np:skip")
    async def skip(self, interaction: discord.Interaction, button: discord.ui.Button):
        with log.span(
            Category.USER,
            "np_button_skip",
            module=__name__,
            view="NowPlayingView",
            custom_id=getattr(button, "custom_id", None),
            interaction_id=getattr(interaction, "id", None),
            guild_id=interaction.guild_id,
            channel_id=getattr(interaction.channel, "id", None),
            message_id=getattr(getattr(interaction, "message", None), "id", None),
            user_id=getattr(interaction.user, "id", None),
        ):
            if not await self._safe_defer(interaction, ephemeral=True):
                return

        music = self.music
        if not music:
            return

        guild_id = self._guild_id_from_interaction(interaction)
        if not guild_id:
            await self._safe_send(interaction, "❌ This button can only be used in a server.", ephemeral=True)
            return

        if self._busy_lock.locked():
            await self._safe_send(interaction, "⏳ Bot is busy — try again in a moment.", ephemeral=True)
            return

        if not await self._begin_interaction_cooldown(interaction):
            return

        async with self._busy_lock:
            try:
                player = music.get_player(guild_id)
                vc = player.voice_client
                # Recover stale player VC references after transient reconnects.
                if (not vc or not vc.is_connected()) and interaction.guild:
                    guild_vc = interaction.guild.voice_client
                    if guild_vc and guild_vc.is_connected():
                        vc = guild_vc
                        player.voice_client = guild_vc

                if not vc:
                    await self._safe_send(interaction, "ℹ️ Bot is not connected to voice.", ephemeral=True)
                    return

                # If there is a current track, treat this as skippable even if is_playing flags are stale.
                if player.current is not None or vc.is_playing() or vc.is_paused():
                    try:
                        vc.stop()
                    except Exception:
                        pass
                    await self._safe_send(interaction, "⏭️ Skipped!", ephemeral=True)
                else:
                    await self._safe_send(interaction, "ℹ️ Nothing is currently playing to skip.", ephemeral=True)
            except Exception as e:
                log.exception_cat(Category.SYSTEM, "NowPlayingView skip failed", error=str(e))
                return
            finally:
                await self._end_interaction_cooldown(interaction)

    @discord.ui.button(emoji="❤️", style=discord.ButtonStyle.secondary, custom_id="np:like")
    async def like(self, interaction: discord.Interaction, button: discord.ui.Button):
        with log.span(
            Category.USER,
            "np_button_like",
            module=__name__,
            view="NowPlayingView",
            custom_id=getattr(button, "custom_id", None),
            interaction_id=getattr(interaction, "id", None),
            guild_id=interaction.guild_id,
            channel_id=getattr(interaction.channel, "id", None),
            message_id=getattr(getattr(interaction, "message", None), "id", None),
            user_id=getattr(interaction.user, "id", None),
        ):
            if not await self._safe_defer(interaction, ephemeral=True):
                return

        music = self.music
        if not music:
            return

        guild_id = self._guild_id_from_interaction(interaction)
        if not guild_id:
            await self._safe_send(interaction, "❌ This button can only be used in a server.", ephemeral=True)
            return

        if self._busy_lock.locked():
            await self._safe_send(interaction, "⏳ Bot is busy — try again in a moment.", ephemeral=True)
            return

        if not await self._begin_interaction_cooldown(interaction):
            return

        try:
            async with self._busy_lock:
                player = music.get_player(guild_id)
                current = player.current
                if not current:
                    return
    
                title = current.title
                song_db_id = current.song_db_id
    
                if hasattr(music.bot, "db") and music.bot.db and song_db_id:
                    try:
                        song_crud = SongCRUD(music.bot.db)
                        reaction_crud = ReactionCRUD(music.bot.db)
    
                        await song_crud.make_permanent(song_db_id)
                        await reaction_crud.add_reaction(interaction.user.id, song_db_id, "like")
    
                        lib_crud = LibraryCRUD(music.bot.db)
                        await lib_crud.add_to_library(interaction.user.id, song_db_id, "like")
                    except Exception as e:
                        log.error_cat(Category.USER, "Failed to log like", error=str(e))
    
                await self._safe_send(interaction, f"❤️ Liked **{title}**!", ephemeral=True)
        except Exception as e:
            log.exception_cat(Category.SYSTEM, "NowPlayingView like failed", error=str(e))
            return
        finally:
            await self._end_interaction_cooldown(interaction)

    @discord.ui.button(emoji="👎", style=discord.ButtonStyle.secondary, custom_id="np:dislike")
    async def dislike(self, interaction: discord.Interaction, button: discord.ui.Button):
        with log.span(
            Category.USER,
            "np_button_dislike",
            module=__name__,
            view="NowPlayingView",
            custom_id=getattr(button, "custom_id", None),
            interaction_id=getattr(interaction, "id", None),
            guild_id=interaction.guild_id,
            channel_id=getattr(interaction.channel, "id", None),
            message_id=getattr(getattr(interaction, "message", None), "id", None),
            user_id=getattr(interaction.user, "id", None),
        ):
            if not await self._safe_defer(interaction, ephemeral=True):
                return

        music = self.music
        if not music:
            return

        guild_id = self._guild_id_from_interaction(interaction)
        if not guild_id:
            await self._safe_send(interaction, "❌ This button can only be used in a server.", ephemeral=True)
            return

        if self._busy_lock.locked():
            await self._safe_send(interaction, "⏳ Bot is busy — try again in a moment.", ephemeral=True)
            return

        if not await self._begin_interaction_cooldown(interaction):
            return

        try:
            async with self._busy_lock:
                player = music.get_player(guild_id)
                current = player.current
                if not current:
                    return
    
                title = current.title
                song_db_id = current.song_db_id
    
                if hasattr(music.bot, "db") and music.bot.db and song_db_id:
                    try:
                        song_crud = SongCRUD(music.bot.db)
                        reaction_crud = ReactionCRUD(music.bot.db)
    
                        await song_crud.make_permanent(song_db_id)
                        await reaction_crud.add_reaction(interaction.user.id, song_db_id, "dislike")
                    except Exception as e:
                        log.error_cat(Category.USER, "Failed to log dislike", error=str(e))
    
            await self._safe_send(interaction, f"👎 Disliked **{title}**", ephemeral=True)
        except Exception as e:
            log.exception_cat(Category.SYSTEM, "NowPlayingView dislike failed", error=str(e))
            return
        finally:
            await self._end_interaction_cooldown(interaction)

    async def skip_to_callback(self, interaction: discord.Interaction):
        """Skip to a specific song in the queue (callback for dynamic select)."""
        with log.span(
            Category.USER,
            "np_select_skip_to",
            module=__name__,
            view="NowPlayingView",
            custom_id="np:skip_to",
            interaction_id=getattr(interaction, "id", None),
            guild_id=interaction.guild_id,
            user_id=getattr(interaction.user, "id", None),
        ):
            if not await self._safe_defer(interaction, ephemeral=True):
                return

        music = self.music
        if not music:
            return

        guild_id = self._guild_id_from_interaction(interaction)
        if not guild_id:
            await self._safe_send(interaction, "❌ This can only be used in a server.", ephemeral=True)
            return

        if self._busy_lock.locked():
            await self._safe_send(interaction, "⏳ Bot is busy — try again in a moment.", ephemeral=True)
            return

        if not await self._begin_interaction_cooldown(interaction):
            return

        try:
            async with self._busy_lock:
                player = music.get_player(guild_id)
                # Selected value is present in interaction.data['values']
                values = interaction.data.get("values") if isinstance(interaction.data, dict) else None
                selected_index = int(values[0]) if values and len(values) > 0 else 0
                queue_items = list(player.queue._queue)
    
                if selected_index < 0 or selected_index >= len(queue_items):
                    await self._safe_send(interaction, "❌ Invalid queue position.", ephemeral=True)
                    return
    
                # Remove all items before the selected index
                for _ in range(selected_index):
                    try:
                        player.queue.get_nowait()
                    except asyncio.QueueEmpty:
                        break
    
                selected_song = queue_items[selected_index]
            await self._safe_send(interaction, f"⏭️ Skipped to **{selected_song.title}**", ephemeral=True)
        except Exception as e:
            log.exception_cat(Category.SYSTEM, "NowPlayingView skip_to failed", error=str(e))
            await self._safe_send(interaction, "❌ Error skipping to song.", ephemeral=True)
        finally:
            await self._end_interaction_cooldown(interaction)


class NowPlayingCog(commands.Cog):
    def __init__(self, bot: commands.Bot):
        self.bot = bot
        self._persistent_view: NowPlayingView | None = None
        self._sticky_bump_cooldown_s: int = 20
        self._last_sticky_bump_at: dict[int, datetime] = {}
        self._np_worker_spacing_s: float = 1.1
        self._np_worker_tasks: dict[int, asyncio.Task] = {}
        self._np_worker_events: dict[int, asyncio.Event] = {}
        self._np_pending_updates: dict[int, dict[str, bool]] = {}
        self._np_pending_swaps: dict[int, dict[str, int | str]] = {}
        self._np_last_video_sent: dict[int, str] = {}
        self._np_artwork_swap_attempted_video: dict[int, str] = {}

    @property
    def music(self):
        return self.bot.get_cog("MusicCog")

    async def cog_load(self):
        # Register a persistent view so buttons keep working after restarts.
        # (We still send a fresh view per message; this is just for dispatching interactions.)
        if not self._persistent_view:
            self._persistent_view = NowPlayingView(self.bot)
            self.bot.add_view(self._persistent_view)

        if hasattr(self.bot, "db") and self.bot.db:
            await self._cleanup_persisted_now_playing_messages()

    async def cog_unload(self):
        for task in list(self._np_worker_tasks.values()):
            if task and not task.done():
                task.cancel()
        self._np_worker_tasks.clear()
        self._np_worker_events.clear()
        self._np_pending_updates.clear()
        self._np_pending_swaps.clear()
        self._np_last_video_sent.clear()
        self._np_artwork_swap_attempted_video.clear()

        if self._persistent_view:
            try:
                self.bot.remove_view(self._persistent_view)
            except Exception:
                pass
            self._persistent_view = None

    async def _cleanup_persisted_now_playing_messages(self) -> None:
        """Delete any persisted Now Playing message(s) so we don't spam channels after restarts."""
        crud = NowPlayingMessageCRUD(self.bot.db)
        rows = await crud.list_all()
        for row in rows:
            guild_id = row.get("guild_id")
            channel_id = row.get("channel_id")
            message_id = row.get("message_id")

            try:
                channel = self.bot.get_channel(channel_id) or await self.bot.fetch_channel(channel_id)
                if hasattr(channel, "fetch_message"):
                    try:
                        msg = await channel.fetch_message(message_id)
                        await msg.delete()
                        log.info_cat(
                            Category.SYSTEM,
                            "startup_deleted_now_playing_message",
                            guild_id=guild_id,
                            channel_id=channel_id,
                            message_id=message_id,
                        )
                    except discord.NotFound:
                        pass
                    except discord.Forbidden:
                        log.warning_cat(
                            Category.SYSTEM,
                            "Missing permissions to delete startup Now Playing message",
                            guild_id=guild_id,
                            channel_id=channel_id,
                            message_id=message_id,
                        )
            except Exception as e:
                log.debug_cat(Category.SYSTEM, "Startup Now Playing cleanup failed", error=str(e))
            finally:
                try:
                    if guild_id is not None:
                        await crud.delete(int(guild_id))
                except Exception:
                    pass

    @staticmethod
    def _retry_after_from_http_error(exc: discord.HTTPException, default: float = 1.5) -> float:
        retry_after = None

        try:
            response = getattr(exc, "response", None)
            headers = getattr(response, "headers", None) if response else None
            if headers:
                raw = headers.get("Retry-After") or headers.get("retry-after")
                if raw is not None:
                    retry_after = float(raw)
        except Exception:
            retry_after = None

        if retry_after is None:
            try:
                if getattr(exc, "retry_after", None) is not None:
                    retry_after = float(exc.retry_after)
            except Exception:
                retry_after = None

        if retry_after is None:
            try:
                body = json.loads(getattr(exc, "text", "") or "{}")
                retry_after = float(body.get("retry_after"))
            except Exception:
                retry_after = None

        if retry_after is None:
            retry_after = default
        return max(0.2, min(30.0, retry_after))

    async def _discord_call_with_backoff(self, guild_id: int, op: str, call, *, attempts: int = 4):
        for attempt in range(1, attempts + 1):
            try:
                return await call()
            except discord.HTTPException as e:
                if e.status != 429:
                    raise
                delay_s = self._retry_after_from_http_error(e)
                log.warning_cat(
                    Category.SYSTEM,
                    "now_playing_discord_rate_limited",
                    guild_id=guild_id,
                    operation=op,
                    attempt=attempt,
                    retry_after_s=delay_s,
                )
                await asyncio.sleep(delay_s)
        raise RuntimeError(f"Now Playing operation '{op}' exhausted retries after rate limits")

    async def _ensure_np_worker(self, guild_id: int) -> None:
        if guild_id in self._np_worker_tasks and not self._np_worker_tasks[guild_id].done():
            return

        event = asyncio.Event()
        self._np_worker_events[guild_id] = event
        self._np_worker_tasks[guild_id] = asyncio.create_task(self._np_worker_loop(guild_id))

    async def _enqueue_now_playing_update(self, guild_id: int, player, *, repost: bool, force: bool) -> None:
        pending = self._np_pending_updates.get(guild_id)
        if pending is None:
            pending = {"player": player, "repost": repost, "force": force}
        else:
            pending["player"] = player
            pending["repost"] = pending["repost"] or repost
            pending["force"] = pending["force"] or force
        self._np_pending_updates[guild_id] = pending

        await self._ensure_np_worker(guild_id)
        event = self._np_worker_events.get(guild_id)
        if event:
            event.set()

    async def _enqueue_now_playing_swap(self, *, guild_id: int, channel_id: int, message_id: int, video_id: str) -> None:
        self._np_pending_swaps[guild_id] = {
            "guild_id": guild_id,
            "channel_id": channel_id,
            "message_id": message_id,
            "video_id": video_id,
        }
        await self._ensure_np_worker(guild_id)
        event = self._np_worker_events.get(guild_id)
        if event:
            event.set()

    async def _np_worker_loop(self, guild_id: int) -> None:
        last_dispatch_at = 0.0
        event = self._np_worker_events.get(guild_id)
        if not event:
            return

        try:
            while True:
                await event.wait()
                event.clear()

                pending = self._np_pending_updates.pop(guild_id, None)
                if pending:
                    elapsed = time.monotonic() - last_dispatch_at
                    if elapsed < self._np_worker_spacing_s:
                        await asyncio.sleep(self._np_worker_spacing_s - elapsed)

                    player = pending.get("player")
                    if not player:
                        log.debug_cat(Category.SYSTEM, "now_playing_worker_missing_player", guild_id=guild_id)
                        continue

                    try:
                        await self._send_now_playing_for_player_impl(
                            player,
                            repost=bool(pending.get("repost")),
                            force=bool(pending.get("force")),
                        )
                    except Exception as e:
                        log.debug_cat(
                            Category.SYSTEM,
                            "now_playing_worker_dispatch_failed",
                            guild_id=guild_id,
                            error=str(e),
                        )
                    finally:
                        last_dispatch_at = time.monotonic()
                    continue

                swap = self._np_pending_swaps.pop(guild_id, None)
                if swap:
                    elapsed = time.monotonic() - last_dispatch_at
                    if elapsed < self._np_worker_spacing_s:
                        await asyncio.sleep(self._np_worker_spacing_s - elapsed)
                    try:
                        await self._swap_loading_to_image(
                            guild_id=int(swap["guild_id"]),
                            channel_id=int(swap["channel_id"]),
                            message_id=int(swap["message_id"]),
                            video_id=str(swap["video_id"]),
                        )
                    except Exception as e:
                        log.debug_cat(
                            Category.SYSTEM,
                            "now_playing_worker_swap_failed",
                            guild_id=guild_id,
                            error=str(e),
                        )
                    finally:
                        last_dispatch_at = time.monotonic()
        except asyncio.CancelledError:
            return
        finally:
            self._np_pending_updates.pop(guild_id, None)
            self._np_pending_swaps.pop(guild_id, None)
            self._np_worker_events.pop(guild_id, None)
            self._np_worker_tasks.pop(guild_id, None)

    async def send_now_playing_for_player(self, player, *, repost: bool = False, force: bool = False) -> None:
        if not player:
            return
        await self._enqueue_now_playing_update(player.guild_id, player, repost=repost, force=force)

    async def _send_now_playing_for_player_impl(self, player, *, repost: bool = False, force: bool = False) -> None:
        """Post a Now Playing view immediately with a loading embed, then swap to the image when ready.

        If `repost=True`, tries to delete the existing Now Playing message and send a new one (to "bump" it).
        """
        if not player.current or not player.text_channel_id:
            return

        channel = self.bot.get_channel(player.text_channel_id)
        if not channel:
            try:
                channel = await self.bot.fetch_channel(player.text_channel_id)
            except Exception:
                log.debug_cat(
                    Category.SYSTEM,
                    "now_playing_channel_not_found",
                    guild_id=player.guild_id,
                    channel_id=player.text_channel_id,
                )
                return

        item = player.current
        video_id = item.video_id
        last_video_id = self._np_last_video_sent.get(player.guild_id)

        # New song -> allow one fresh artwork attempt.
        if last_video_id != video_id:
            self._np_artwork_swap_attempted_video.pop(player.guild_id, None)

        # Drop duplicate refreshes for the same song.
        if not repost and not force and last_video_id == video_id:
            return

        # Create view with dynamic queue select options (top 10)
        queue_items = list(player.queue._queue)[:10]
        view = NowPlayingView(self.bot, queue_items=queue_items)

        loading_embed = discord.Embed(
            title="🎵 Now Playing",
            description=f"Track: **{item.title}**\nArtist: {item.artist}",
            color=0x7c3aed,
        )
        loading_embed.set_thumbnail(url=item.thumbnail_url)
        if item.discovery_reason:
            loading_embed.add_field(name="Discovery", value=item.discovery_reason, inline=False)

        msg = None
        existing_art: tuple[bytes, str] | None = None

        try:
            async with player._np_lock:
                # Try reuse the persisted message (edit instead of delete/send).
                if hasattr(self.bot, "db") and self.bot.db:
                    try:
                        np_crud = NowPlayingMessageCRUD(self.bot.db)
                        old = await np_crud.get(player.guild_id)
                        if old:
                            old_channel_id = old.get("channel_id")
                            old_message_id = old.get("message_id")

                            # If channel changed, do not reuse old message.
                            if int(old_channel_id) == int(player.text_channel_id):
                                try:
                                    old_channel = self.bot.get_channel(old_channel_id) or await self.bot.fetch_channel(old_channel_id)
                                    if hasattr(old_channel, "fetch_message"):
                                        msg = await self._discord_call_with_backoff(
                                            player.guild_id,
                                            "fetch_persisted_message",
                                            lambda: old_channel.fetch_message(old_message_id),
                                        )
                                except Exception:
                                    msg = None
                            else:
                                try:
                                    await np_crud.delete(player.guild_id)
                                except Exception:
                                    pass
                    except Exception as e:
                        log.debug_cat(Category.SYSTEM, "Now Playing persistence lookup failed", error=str(e), guild_id=player.guild_id)

                # Fallback: if we still have the in-memory reference, try reuse it.
                if msg is None and player.last_np_msg is not None:
                    msg = player.last_np_msg

                # If we want to bump the message to the bottom, delete it and re-send.
                if repost and msg is not None:
                    # Best effort: reuse existing artwork from the current message so we don't show "loading" again.
                    try:
                        if getattr(msg, "attachments", None):
                            att = msg.attachments[0]
                            filename = getattr(att, "filename", None) or "nowplaying.png"
                            data = await att.read()
                            if data:
                                existing_art = (data, filename)
                    except Exception:
                        existing_art = None

                    try:
                        await self._discord_call_with_backoff(
                            player.guild_id,
                            "delete_now_playing",
                            lambda: msg.delete(),
                        )
                    except discord.Forbidden:
                        # Can't delete; skip repost mode to avoid churn.
                        return
                    except Exception:
                        return
                    else:
                        msg = None

                # Show loading state immediately.
                if msg is not None:
                    try:
                        try:
                            await self._discord_call_with_backoff(
                                player.guild_id,
                                "edit_loading_embed_with_attachments",
                                lambda: msg.edit(embed=loading_embed, view=view, attachments=[]),
                            )
                        except TypeError:
                            # Older libs may not support attachments= in edit.
                            await self._discord_call_with_backoff(
                                player.guild_id,
                                "edit_loading_embed",
                                lambda: msg.edit(embed=loading_embed, view=view),
                            )
                    except Exception:
                        msg = None

                if msg is None:
                    if existing_art is not None:
                        data, filename = existing_art
                        file = discord.File(io.BytesIO(data), filename=filename)
                        msg = await self._discord_call_with_backoff(
                            player.guild_id,
                            "send_now_playing_with_file",
                            lambda: channel.send(file=file, view=view),
                        )
                    else:
                        msg = await self._discord_call_with_backoff(
                            player.guild_id,
                            "send_now_playing_loading_embed",
                            lambda: channel.send(embed=loading_embed, view=view),
                        )

                player.last_np_msg = msg
                self._np_last_video_sent[player.guild_id] = video_id

                if hasattr(self.bot, "db") and self.bot.db:
                    try:
                        np_crud = NowPlayingMessageCRUD(self.bot.db)
                        await np_crud.upsert(player.guild_id, player.text_channel_id, msg.id)
                    except Exception as e:
                        log.debug_cat(Category.SYSTEM, "Failed to persist Now Playing message", error=str(e), guild_id=player.guild_id)

            # Fetch image and swap the message after releasing the lock.
            if (
                existing_art is None
                and self._np_artwork_swap_attempted_video.get(player.guild_id) != video_id
            ):
                self._np_artwork_swap_attempted_video[player.guild_id] = video_id
                await self._enqueue_now_playing_swap(
                    guild_id=player.guild_id,
                    channel_id=player.text_channel_id,
                    message_id=msg.id,
                    video_id=video_id,
                )
        except Exception as e:
            log.exception_cat(
                Category.SYSTEM,
                "send_now_playing_for_player failed",
                error=str(e),
                guild_id=player.guild_id,
            )

    @commands.Cog.listener()
    async def on_message(self, message: discord.Message):
        """Optional 'sticky' behaviour: bump Now Playing to the bottom by re-posting after users chat."""
        try:
            if not message.guild or message.author.bot:
                return

            music = self.music
            if not music:
                return

            player = music.get_player(message.guild.id)
            if not player.current or not player.text_channel_id or not player.is_playing:
                return

            if message.channel.id != player.text_channel_id:
                return

            if not player.last_np_msg:
                return

            # If Now Playing is already the last message, nothing to do.
            last_message_id = getattr(message.channel, "last_message_id", None)
            if last_message_id and int(last_message_id) == int(player.last_np_msg.id):
                return

            now = datetime.now(UTC)
            last = self._last_sticky_bump_at.get(message.guild.id)
            if last and (now - last).total_seconds() < self._sticky_bump_cooldown_s:
                return

            self._last_sticky_bump_at[message.guild.id] = now
            await self.send_now_playing_for_player(player, repost=True, force=True)
        except Exception as e:
            log.debug_cat(Category.SYSTEM, "sticky_now_playing_bump_failed", error=str(e))

    async def _swap_loading_to_image(self, *, guild_id: int, channel_id: int, message_id: int, video_id: str) -> None:
        """Fetch the dashboard-rendered image and edit the message to show it."""
        music = self.music
        if not music:
            return

        player = music.get_player(guild_id)
        if not player.current or player.current.video_id != video_id:
            return

        channel = self.bot.get_channel(channel_id)
        if not channel:
            try:
                channel = await self.bot.fetch_channel(channel_id)
            except Exception:
                return

        try:
            msg = await self._discord_call_with_backoff(
                guild_id,
                "fetch_now_playing_message_for_swap",
                lambda: channel.fetch_message(message_id),
            )
        except Exception:
            return

        item = player.current

        # Fetch additional stats for rendering, if available.
        requested_by_str = ""
        liked_by_str = ""
        disliked_by_str = ""

        if hasattr(self.bot, "db") and self.bot.db and item.song_db_id:
            try:
                stats = await self.bot.db.fetch_one(
                    """
                    SELECT 
                        (SELECT GROUP_CONCAT(DISTINCT u.username) FROM playback_history ph JOIN users u ON ph.for_user_id = u.id WHERE ph.song_id = ? AND ph.discovery_source = "user_request") as requested_by,
                        (SELECT GROUP_CONCAT(DISTINCT u.username) FROM song_reactions sr JOIN users u ON sr.song_id = ? AND sr.user_id = u.id AND sr.reaction = 'like') as liked_by,
                        (SELECT GROUP_CONCAT(DISTINCT u.username) FROM song_reactions sr JOIN users u ON sr.song_id = ? AND sr.user_id = u.id AND sr.reaction = 'dislike') as disliked_by
                    """,
                    (item.song_db_id, item.song_db_id, item.song_db_id),
                )
                if stats:
                    requested_by_str = stats.get("requested_by") or ""
                    liked_by_str = stats.get("liked_by") or ""
                    disliked_by_str = stats.get("disliked_by") or ""
            except Exception:
                pass

        current_time_str = "0:00"
        progress_percent = 0
        if player.start_time:
            elapsed = (datetime.now(UTC) - player.start_time).total_seconds()
            minutes, seconds = divmod(int(elapsed), 60)
            current_time_str = f"{minutes}:{seconds:02d}"
            if item.duration_seconds:
                progress_percent = min(100, int((elapsed / item.duration_seconds) * 100))

        total_time_str = "0:00"
        if item.duration_seconds:
            minutes, seconds = divmod(item.duration_seconds, 60)
            total_time_str = f"{minutes}:{seconds:02d}"

        for_user_str = ""
        target_user_id = item.for_user_id or item.requester_id
        if target_user_id and player.voice_client and player.voice_client.guild:
            member = player.voice_client.guild.get_member(target_user_id)
            if member:
                for_user_str = member.display_name
            else:
                user = self.bot.get_user(target_user_id)
                if user:
                    for_user_str = user.display_name

        params = {
            "title": item.title,
            "artist": item.artist,
            "thumbnail": f"https://img.youtube.com/vi/{item.video_id}/hqdefault.jpg",
            "genre": item.genre or "",
            "year": str(item.year) if item.year else "",
            "progress": str(progress_percent),
            "duration": total_time_str,
            "current": current_time_str,
            "requestedBy": requested_by_str,
            "likedBy": liked_by_str,
            "dislikedBy": disliked_by_str,
            "queueSize": str(player.queue.qsize()),
            "discoveryReason": item.discovery_reason or "",
            "forUser": for_user_str,
            "videoUrl": f"https://youtube.com/watch?v={item.video_id}",
        }

        query_str = "&".join([f"{k}={quote_plus(str(v))}" for k, v in params.items()])
        image_url = f"http://dashboard:3000/api/now-playing/image?{query_str}"

        # Updated view (queue may have changed)
        queue_items = list(player.queue._queue)[:10]
        view = NowPlayingView(self.bot, queue_items=queue_items)

        try:
            async with aiohttp.ClientSession() as session:
                async with session.get(image_url, timeout=5) as resp:
                    if resp.status != 200:
                        raise RuntimeError(f"dashboard image http {resp.status}")
                    image_data = await resp.read()

            file = discord.File(io.BytesIO(image_data), filename="nowplaying.png")

            # Replace the loading embed with the image.
            try:
                await self._discord_call_with_backoff(
                    guild_id,
                    "swap_loading_to_image_edit",
                    lambda: msg.edit(embed=None, view=view, attachments=[file]),
                )
            except TypeError:
                # Some libs require deleting and re-sending to attach a file.
                try:
                    await self._discord_call_with_backoff(
                        guild_id,
                        "swap_loading_to_image_delete",
                        lambda: msg.delete(),
                    )
                except Exception:
                    return
                new_msg = await self._discord_call_with_backoff(
                    guild_id,
                    "swap_loading_to_image_resend",
                    lambda: channel.send(file=file, view=view),
                )
                player.last_np_msg = new_msg
                if hasattr(self.bot, "db") and self.bot.db:
                    try:
                        np_crud = NowPlayingMessageCRUD(self.bot.db)
                        await np_crud.upsert(guild_id, channel_id, new_msg.id)
                    except Exception:
                        pass
        except Exception as e:
            # Keep the initial embed unchanged when artwork generation fails.
            log.debug_cat(
                Category.SYSTEM,
                "now_playing_image_swap_failed",
                guild_id=guild_id,
                channel_id=channel_id,
                message_id=message_id,
                video_id=video_id,
                error=str(e),
            )
            return

    @app_commands.command(name="nowplaying", description="Show the current song")
    async def nowplaying(self, interaction: discord.Interaction):
        with log.span(
            Category.SYSTEM,
            "command_nowplaying",
            module=__name__,
            cog=type(self).__name__,
            command="/nowplaying",
            guild_id=interaction.guild_id,
            channel_id=getattr(interaction.channel, "id", None),
            user_id=getattr(interaction.user, "id", None),
        ):
            music = self.music
            if not music:
                await interaction.response.send_message("❌ Music system is not loaded.", ephemeral=True)
                return

            player = music.get_player(interaction.guild_id)
            if not player.current:
                await interaction.response.send_message("❌ Nothing is playing", ephemeral=True)
                return

            embed = discord.Embed(
                title="🎵 Now Playing",
                description=f"**{player.current.title}**\nby {player.current.artist}",
                color=discord.Color.green(),
            )

            if player.current.discovery_reason:
                embed.add_field(name="Discovery", value=player.current.discovery_reason, inline=False)

            if player.current.for_user_id:
                user = self.bot.get_user(player.current.for_user_id)
                if user:
                    embed.set_footer(text=f"🎲 Playing for {user.display_name}")
            elif player.current.requester_id:
                user = self.bot.get_user(player.current.requester_id)
                if user:
                    embed.set_footer(text=f"Requested by {user.display_name}")

            await interaction.response.send_message(embed=embed, ephemeral=True)


async def setup(bot: commands.Bot):
    await bot.add_cog(NowPlayingCog(bot))
