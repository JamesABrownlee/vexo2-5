"""
Optional metadata enrichment (Discogs/MusicBrainz) for genres/year.
"""
import asyncio
import logging
import os
from functools import partial
from typing import Optional

from src.utils.logging import get_logger, Category

log = get_logger(__name__)

try:
    import discogs_client  # type: ignore
except Exception:
    discogs_client = None

try:
    import musicbrainzngs  # type: ignore
except Exception:
    musicbrainzngs = None


class DiscogsService:
    def __init__(self):
        self.client: Optional["discogs_client.Client"] = None
        self.enabled = False
        self._cache: dict[str, dict] = {}
        self._initialize()

    def _initialize(self) -> None:
        if not discogs_client:
            self.enabled = False
            return

        user_agent = "VexoBot/1.0"
        token = os.getenv("DISCOGS_TOKEN")
        key = os.getenv("DISCOGS_KEY") or os.getenv("DISCOGS_CONSUMER_KEY")
        secret = os.getenv("DISCOGS_SECRET") or os.getenv("DISCOGS_CONSUMER_SECRET")

        try:
            if token:
                self.client = discogs_client.Client(user_agent, user_token=token)
                self.enabled = True
            elif key and secret:
                self.client = discogs_client.Client(user_agent, consumer_key=key, consumer_secret=secret)
                self.enabled = True
            else:
                self.enabled = False
        except Exception as e:
            log.warning_cat(Category.API, "discogs_init_failed", error=str(e))
            self.enabled = False

    async def get_metadata(self, artist: str, title: str) -> dict:
        if not self.enabled or not self.client:
            return {"genres": [], "year": None}

        cache_key = f"{artist.lower()} - {title.lower()}"
        if cache_key in self._cache:
            return self._cache[cache_key]

        loop = asyncio.get_event_loop()
        try:
            result = await loop.run_in_executor(None, partial(self._search_sync, artist, title))
            if result:
                self._cache[cache_key] = result
                return result
        except Exception as e:
            log.debug_cat(Category.API, "discogs_search_failed", error=str(e))

        return {"genres": [], "year": None}

    def _search_sync(self, artist: str, title: str) -> dict:
        try:
            query = f"{artist} - {title}"
            results = self.client.search(query, type="release")
            if not results:
                return {"genres": [], "year": None}
            release = results[0]
            genres = getattr(release, "genres", []) or []
            styles = getattr(release, "styles", []) or []
            combined = list(set(genres + styles))
            return {"genres": combined, "year": getattr(release, "year", None)}
        except Exception:
            return {"genres": [], "year": None}


class MusicBrainzService:
    def __init__(self):
        enabled = os.getenv("MUSICBRAINZ_ENABLED", "").strip().lower() in {"1", "true", "yes", "on"}
        if not enabled or not musicbrainzngs:
            self.enabled = False
            return

        try:
            musicbrainzngs.set_useragent("VexoBot", "1.0", "https://example.com")
            self.enabled = True
        except Exception:
            self.enabled = False

        self._cache: dict[str, dict] = {}

    def get_metadata_sync(self, artist: str, title: str) -> dict:
        if not self.enabled:
            return {"genres": [], "year": None}

        cache_key = f"{artist.lower()} - {title.lower()}"
        if cache_key in self._cache:
            return self._cache[cache_key]

        try:
            artist_tags = []
            artists = musicbrainzngs.search_artists(artist=artist, limit=1).get("artist-list", [])
            if artists:
                artist_obj = artists[0]
                if "tag-list" in artist_obj:
                    artist_tags = [t["name"].title() for t in artist_obj["tag-list"]]

            recording_tags = []
            year = None
            recordings = musicbrainzngs.search_recordings(artist=artist, recording=title, limit=1).get("recording-list", [])
            if recordings:
                rec_obj = recordings[0]
                if "tag-list" in rec_obj:
                    recording_tags = [t["name"].title() for t in rec_obj["tag-list"]]
                if "date" in rec_obj:
                    try:
                        year = int(rec_obj["date"][:4])
                    except Exception:
                        year = None
                elif rec_obj.get("release-list"):
                    try:
                        date_str = rec_obj["release-list"][0].get("date", "")
                        if date_str:
                            year = int(date_str[:4])
                    except Exception:
                        year = None

            combined = list(set(recording_tags + artist_tags))
            result = {"genres": combined, "year": year}
            self._cache[cache_key] = result
            return result
        except Exception:
            return {"genres": [], "year": None}


class MetadataEnricher:
    def __init__(self):
        self.discogs = DiscogsService()
        self.musicbrainz = MusicBrainzService()
        self.enabled = bool(self.discogs.enabled or self.musicbrainz.enabled)
        if self.enabled:
            log.info_cat(
                Category.API,
                "metadata_enricher_enabled",
                discogs=self.discogs.enabled,
                musicbrainz=self.musicbrainz.enabled,
            )

    async def enrich(self, artist: str, title: str) -> dict:
        if not self.enabled:
            return {"genres": [], "year": None}

        result = {"genres": [], "year": None}

        if self.discogs.enabled:
            discogs_data = await self.discogs.get_metadata(artist, title)
            if discogs_data:
                result["genres"] = discogs_data.get("genres", []) or []
                result["year"] = discogs_data.get("year")
                if result["genres"] or result["year"]:
                    return result

        if self.musicbrainz.enabled:
            loop = asyncio.get_event_loop()
            mb = await loop.run_in_executor(None, partial(self.musicbrainz.get_metadata_sync, artist, title))
            if mb:
                if mb.get("genres"):
                    result["genres"] = mb["genres"]
                if mb.get("year"):
                    result["year"] = mb["year"]

        return result
