"""
Manual verification: recent_history_window returns rows after a track is played.

Run:
  python -m scripts.verify_recent_history_window
"""
import asyncio
import tempfile
from pathlib import Path

from src.database.connection import DatabaseManager
from src.database.crud import PlaybackCRUD, SongCRUD, GuildCRUD


async def main() -> None:
    with tempfile.TemporaryDirectory() as tmpdir:
        db_path = Path(tmpdir) / "test.db"
        db = await DatabaseManager.create(db_path)

        guild_crud = GuildCRUD(db)
        playback_crud = PlaybackCRUD(db)
        song_crud = SongCRUD(db)

        guild_id = 123
        await guild_crud.get_or_create(guild_id, "Test Guild")
        session_id = await playback_crud.create_session(guild_id=guild_id, channel_id=1)

        song = await song_crud.get_or_create_by_yt_id(
            canonical_yt_id="dQw4w9WgXcQ",
            title="Test Song",
            artist_name="Test Artist",
            duration_seconds=123,
        )

        await playback_crud.log_track(session_id=session_id, song_id=song["id"])

        rows = await playback_crud.get_recent_history_window(guild_id, seconds=3600)
        assert rows, "Expected recent_history_window to return at least one row"

        await db.close()

    print("OK: recent_history_window returned rows for a just-played track.")


if __name__ == "__main__":
    asyncio.run(main())
