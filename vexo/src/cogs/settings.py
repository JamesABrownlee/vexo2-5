"""
Settings Cog - Server settings commands
"""
import logging
import os
import socket

import aiohttp
import discord
from discord import app_commands
from discord.ext import commands

logger = logging.getLogger(__name__)


class SettingsCog(commands.Cog):
    """Server settings commands."""

    def __init__(self, bot: commands.Bot):
        self.bot = bot

    @staticmethod
    def _as_bool(value, default: bool = True) -> bool:
        if value is None:
            return default
        if isinstance(value, bool):
            return value
        if isinstance(value, str):
            return value.strip().lower() in {"1", "true", "yes", "on"}
        return bool(value)

    settings_group = app_commands.Group(
        name="settings",
        description="Server settings",
        default_permissions=discord.Permissions(manage_guild=True),
    )

    @settings_group.command(name="prebuffer", description="Toggle pre-buffering for next song")
    @app_commands.describe(enabled="Enable or disable pre-buffering")
    async def prebuffer(self, interaction: discord.Interaction, enabled: bool):
        """Toggle pre-buffering for next song URL."""
        music = self.bot.get_cog("MusicCog")
        if music:
            player = music.get_player(interaction.guild_id)
            player.pre_buffer = enabled

        if hasattr(self.bot, "db") and self.bot.db:
            from src.database.crud import GuildCRUD

            guild_crud = GuildCRUD(self.bot.db)
            await guild_crud.set_setting(interaction.guild_id, "prebuffer", enabled)

        status = "enabled" if enabled else "disabled"
        await interaction.response.send_message(
            f"Pre-buffering {status}\n"
            f"{'May use more CPU/memory but reduces gaps between songs' if enabled else 'Lower resource usage but may have brief gaps'}",
            ephemeral=True,
        )

    @settings_group.command(name="discovery_weights", description="Set discovery strategy weights")
    @app_commands.describe(
        similar="Weight for similar songs (0-100)",
        artist="Weight for same artist (0-100)",
        wildcard="Weight for wildcard/charts (0-100)",
    )
    async def discovery_weights(
        self,
        interaction: discord.Interaction,
        similar: int,
        artist: int,
        wildcard: int,
    ):
        """Set discovery strategy weights for this server."""
        if not all(0 <= w <= 100 for w in [similar, artist, wildcard]):
            await interaction.response.send_message("All weights must be between 0 and 100", ephemeral=True)
            return

        total = similar + artist + wildcard
        if total == 0:
            await interaction.response.send_message("At least one weight must be greater than 0", ephemeral=True)
            return

        weights = {"similar": similar, "artist": artist, "wildcard": wildcard}

        if hasattr(self.bot, "db") and self.bot.db:
            from src.database.crud import GuildCRUD

            guild_crud = GuildCRUD(self.bot.db)
            await guild_crud.set_setting(interaction.guild_id, "discovery_weights", weights)

        pct_similar = (similar / total) * 100
        pct_artist = (artist / total) * 100
        pct_wildcard = (wildcard / total) * 100

        await interaction.response.send_message(
            "Discovery weights updated:\n"
            f"- Similar songs: {pct_similar:.0f}%\n"
            f"- Same artist: {pct_artist:.0f}%\n"
            f"- Wildcard (charts): {pct_wildcard:.0f}%",
            ephemeral=True,
        )

    @settings_group.command(name="sticky_nowplaying", description="Toggle sticky now-playing repost behavior")
    @app_commands.describe(enabled="Enable or disable sticky now-playing repost")
    async def sticky_nowplaying(self, interaction: discord.Interaction, enabled: bool):
        if hasattr(self.bot, "db") and self.bot.db:
            from src.database.crud import GuildCRUD

            guild_crud = GuildCRUD(self.bot.db)
            await guild_crud.set_setting(interaction.guild_id, "sticky_now_playing_enabled", enabled)

        await interaction.response.send_message(
            f"Sticky now-playing {'enabled' if enabled else 'disabled'}",
            ephemeral=True,
        )

    @settings_group.command(name="nowplaying_artwork", description="Toggle artwork generation for now-playing message")
    @app_commands.describe(enabled="Enable or disable now-playing artwork generation")
    async def nowplaying_artwork(self, interaction: discord.Interaction, enabled: bool):
        if hasattr(self.bot, "db") and self.bot.db:
            from src.database.crud import GuildCRUD

            guild_crud = GuildCRUD(self.bot.db)
            await guild_crud.set_setting(interaction.guild_id, "now_playing_artwork_enabled", enabled)

        await interaction.response.send_message(
            f"Now-playing artwork {'enabled' if enabled else 'disabled'}",
            ephemeral=True,
        )

    @settings_group.command(name="radio_presenter", description="Toggle radio-presenter/TTS announcements")
    @app_commands.describe(enabled="Enable or disable radio-presenter announcements")
    async def radio_presenter(self, interaction: discord.Interaction, enabled: bool):
        if hasattr(self.bot, "db") and self.bot.db:
            from src.database.crud import GuildCRUD

            guild_crud = GuildCRUD(self.bot.db)
            await guild_crud.set_setting(interaction.guild_id, "radio_presenter_enabled", enabled)

        await interaction.response.send_message(
            f"Radio presenter {'enabled' if enabled else 'disabled'}",
            ephemeral=True,
        )

    @settings_group.command(name="show", description="Show current server settings")
    async def show_settings(self, interaction: discord.Interaction):
        """Show current settings for this server."""
        embed = discord.Embed(title="Server Settings", color=discord.Color.blue())

        if hasattr(self.bot, "db") and self.bot.db:
            from src.database.crud import GuildCRUD

            guild_crud = GuildCRUD(self.bot.db)
            all_settings = await guild_crud.get_all_settings(interaction.guild_id)

            prebuffer = self._as_bool(all_settings.get("prebuffer"), True)
            embed.add_field(name="Pre-buffering", value="Enabled" if prebuffer else "Disabled", inline=True)

            weights = all_settings.get("discovery_weights", {"similar": 60, "artist": 10, "wildcard": 30})
            total = sum(weights.values()) if isinstance(weights, dict) else 0
            if total > 0:
                weights_text = (
                    f"Similar: {(weights.get('similar', 0) / total) * 100:.0f}%\n"
                    f"Artist: {(weights.get('artist', 0) / total) * 100:.0f}%\n"
                    f"Wildcard: {(weights.get('wildcard', 0) / total) * 100:.0f}%"
                )
            else:
                weights_text = "Default (60/10/30)"
            embed.add_field(name="Discovery Weights", value=weights_text, inline=True)

            autoplay = self._as_bool(all_settings.get("autoplay"), True)
            embed.add_field(name="Autoplay", value="Enabled" if autoplay else "Disabled", inline=True)

            sticky_np = self._as_bool(all_settings.get("sticky_now_playing_enabled"), True)
            embed.add_field(name="Sticky Now Playing", value="Enabled" if sticky_np else "Disabled", inline=True)

            artwork_enabled = self._as_bool(all_settings.get("now_playing_artwork_enabled"), True)
            embed.add_field(name="Now Playing Artwork", value="Enabled" if artwork_enabled else "Disabled", inline=True)

            radio_presenter_enabled = self._as_bool(all_settings.get("radio_presenter_enabled"), True)
            embed.add_field(name="Radio Presenter", value="Enabled" if radio_presenter_enabled else "Disabled", inline=True)
        else:
            embed.description = "Settings stored in memory only (database not available)"

        await interaction.response.send_message(embed=embed, ephemeral=True)

    @app_commands.command(name="dj", description="Set the DJ role")
    @app_commands.describe(role="The role that can use DJ commands")
    @app_commands.default_permissions(administrator=True)
    async def set_dj_role(self, interaction: discord.Interaction, role: discord.Role):
        """Set the DJ role for this server."""
        if hasattr(self.bot, "db") and self.bot.db:
            from src.database.crud import GuildCRUD

            guild_crud = GuildCRUD(self.bot.db)
            await guild_crud.set_setting(interaction.guild_id, "dj_role_id", role.id)

        await interaction.response.send_message(f"DJ role set to {role.mention}", ephemeral=True)

    @app_commands.command(name="restart", description="Restart the bot (Admin only)")
    @app_commands.default_permissions(administrator=True)
    async def restart(self, interaction: discord.Interaction):
        """Restart the bot process."""
        if not interaction.user.guild_permissions.administrator:
            await interaction.response.send_message("You need Administrator permission to use this command.", ephemeral=True)
            return

        await interaction.response.send_message("Restarting bot...", ephemeral=True)
        logger.warning(f"Bot restart requested by {interaction.user} ({interaction.user.id})")

        if os.path.exists("/var/run/docker.sock"):
            try:
                hostname = socket.gethostname()
                connector = aiohttp.UnixConnector(path="/var/run/docker.sock")
                async with aiohttp.ClientSession(connector=connector) as session:
                    url = f"http://localhost/containers/{hostname}/restart"
                    async with session.post(url) as resp:
                        if resp.status == 204:
                            logger.info("Docker restart command sent successfully")
                            return
                        text = await resp.text()
                        logger.error(f"Docker restart failed: {resp.status} - {text}")
            except Exception as e:
                logger.error(f"Failed to restart via Docker socket: {e}")

        logger.info("Falling back to process exit")
        try:
            await self.bot.close()
        except Exception:
            pass

        os._exit(0)


async def setup(bot: commands.Bot):
    """Load the settings cog."""
    await bot.add_cog(SettingsCog(bot))
