"""
Preference Manager - Learn and apply user music preferences
"""
import logging
from collections import Counter
from dataclasses import dataclass
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from src.database.crud import PreferenceCRUD, SongCRUD, UserCRUD
    from src.services.spotify import SpotifyTrack

logger = logging.getLogger(__name__)


@dataclass
class SongInfo:
    """Song info for preference learning."""
    song_id: int
    title: str
    artist: str
    genres: list[str]
    year: int | None = None


class PreferenceManager:
    """Manages user music preferences."""
    
    def __init__(
        self,
        preference_crud: "PreferenceCRUD",
        song_crud: "SongCRUD",
        user_crud: "UserCRUD",
    ):
        self.preferences = preference_crud
        self.songs = song_crud
        self.users = user_crud
    
    async def learn_from_playlist(
        self,
        user_id: int,
        tracks: list["SpotifyTrack"],
    ) -> dict[str, int]:
        """
        Learn preferences from imported playlist tracks.
        
        Returns counts of learned preferences.
        """
        # Check if user opted out
        if await self.users.is_opted_out(user_id):
            logger.info(f"User {user_id} opted out, skipping preference learning")
            return {"genres": 0, "artists": 0, "decades": 0}
        
        genre_counts: Counter = Counter()
        artist_counts: Counter = Counter()
        decade_counts: Counter = Counter()
        
        for track in tracks:
            # Count genres
            if track.genres:
                for genre in track.genres:
                    genre_counts[genre.lower()] += 1
            
            # Count artists
            artist_counts[track.artist.lower()] += 1
            
            # Count decades
            if track.release_year:
                decade = f"{(track.release_year // 10) * 10}s"
                decade_counts[decade] += 1
        
        total = len(tracks) if tracks else 1
        
        # Convert counts to affinity scores (0.0 to 1.0)
        for genre, count in genre_counts.items():
            # Score based on frequency, capped at 1.0
            score = min(count / (total * 0.1), 1.0)
            current = await self.preferences.get_preference(user_id, "genre", genre)
            # Average with existing preference if any
            new_score = (current + score) / 2 if current else score
            await self.preferences.update_preference(user_id, "genre", genre, new_score)
        
        for artist, count in artist_counts.items():
            score = min(count / (total * 0.05), 1.0)
            current = await self.preferences.get_preference(user_id, "artist", artist)
            new_score = (current + score) / 2 if current else score
            await self.preferences.update_preference(user_id, "artist", artist, new_score)
        
        for decade, count in decade_counts.items():
            score = min(count / (total * 0.1), 1.0)
            current = await self.preferences.get_preference(user_id, "decade", decade)
            new_score = (current + score) / 2 if current else score
            await self.preferences.update_preference(user_id, "decade", decade, new_score)
        
        logger.info(
            f"Learned preferences for user {user_id}: "
            f"{len(genre_counts)} genres, {len(artist_counts)} artists, {len(decade_counts)} decades"
        )
        
        return {
            "genres": len(genre_counts),
            "artists": len(artist_counts),
            "decades": len(decade_counts),
        }
    
    async def record_like(self, user_id: int, song: SongInfo) -> None:
        """
        Record a like - positive feedback.
        Boosts related preferences.
        """
        if await self.users.is_opted_out(user_id):
            return
        
        # Boost genre preferences
        for genre in song.genres:
            current = await self.preferences.get_preference(user_id, "genre", genre)
            new_score = min(current + 0.1, 1.0)
            await self.preferences.update_preference(user_id, "genre", genre, new_score)
        
        # Boost artist preference
        current = await self.preferences.get_preference(user_id, "artist", song.artist.lower())
        new_score = min(current + 0.2, 1.0)
        await self.preferences.update_preference(user_id, "artist", song.artist.lower(), new_score)
        
        # Boost decade preference
        if song.year:
            decade = f"{(song.year // 10) * 10}s"
            current = await self.preferences.get_preference(user_id, "decade", decade)
            new_score = min(current + 0.05, 1.0)
            await self.preferences.update_preference(user_id, "decade", decade, new_score)
        
        logger.debug(f"Recorded like for user {user_id}: {song.title}")
    
    async def record_dislike(self, user_id: int, song: SongInfo) -> None:
        """
        Record a dislike - negative feedback.
        Reduces preferences (but genres floor at 0).
        
        NOTE: This should only be called for explicit dislikes,
        NOT for skips!
        """
        if await self.users.is_opted_out(user_id):
            return
        
        # Slightly reduce genre preferences (floor at 0)
        for genre in song.genres:
            current = await self.preferences.get_preference(user_id, "genre", genre)
            new_score = max(current - 0.05, 0.0)
            await self.preferences.update_preference(user_id, "genre", genre, new_score)
        
        # Reduce artist preference (can go negative)
        current = await self.preferences.get_preference(user_id, "artist", song.artist.lower())
        new_score = max(current - 0.3, -1.0)
        await self.preferences.update_preference(user_id, "artist", song.artist.lower(), new_score)
        
        logger.debug(f"Recorded dislike for user {user_id}: {song.title}")

    async def boost_artist(self, user_id: int, artist_name: str, amount: float = 0.2) -> None:
        """Explicitly boost an artist preference."""
        if await self.users.is_opted_out(user_id):
            return
        
        current = await self.preferences.get_preference(user_id, "artist", artist_name.lower())
        new_score = min(current + amount, 1.0)
        await self.preferences.update_preference(user_id, "artist", artist_name.lower(), new_score)
        logger.info(f"Boosted artist {artist_name} for user {user_id} by {amount}")
    
    async def get_user_preferences_summary(self, user_id: int) -> dict:
        """Get a summary of user preferences for display."""
        all_prefs = await self.preferences.get_all_preferences(user_id)
        
        top_genres = await self.preferences.get_top_preferences(user_id, "genre", limit=5)
        top_artists = await self.preferences.get_top_preferences(user_id, "artist", limit=5)
        top_decades = await self.preferences.get_top_preferences(user_id, "decade", limit=3)
        
        return {
            "top_genres": top_genres,
            "top_artists": top_artists,
            "top_decades": top_decades,
            "total_preferences": sum(len(v) for v in all_prefs.values()),
        }
