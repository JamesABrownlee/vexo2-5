"""
YouTube Music API Wrapper
"""
import asyncio
import logging
import re
from dataclasses import dataclass
from functools import partial
from typing import Any

import yt_dlp
from ytmusicapi import YTMusic

import random
import time
from functools import wraps

from src.utils.logging import get_logger, Category, Event

log = get_logger(__name__)


def retry_with_backoff(retries=3, backoff_in_seconds=1):
    def decorator(func):
        @wraps(func)
        async def wrapper(*args, **kwargs):
            x = 0
            while True:
                try:
                    return await func(*args, **kwargs)
                except Exception as e:
                    if x == retries:
                        log.event(Category.API, Event.API_ERROR, level=logging.ERROR, retries=retries, error=str(e))
                        raise
                    else:
                        sleep = (backoff_in_seconds * 2 ** x + random.uniform(0, 1))
                        log.warning_cat(Category.API, f"Retry {x + 1}/{retries} for {func.__name__}", sleep=f"{sleep:.2f}s", error=str(e))
                        await asyncio.sleep(sleep)
                        x += 1
        return wrapper
    return decorator


@dataclass
class YTTrack:
    """YouTube track info."""
    video_id: str
    title: str
    artist: str
    duration_seconds: int | None = None
    album: str | None = None
    year: int | None = None
    thumbnail_url: str | None = None


@dataclass
class StreamInfo:
    """Stream URL and HTTP headers extracted by yt-dlp."""
    url: str
    http_headers: dict[str, str] | None = None


class YouTubeService:
    """YouTube Music API wrapper."""
    
    def __init__(self, cookies_path: str | None = None, po_token: str | None = None):
        self.yt = YTMusic()
        # Set a longer timeout on the underlying requests session (default can be too low)
        if hasattr(self.yt, '_session'):
            self.yt._session.timeout = 20
        self.cookies_path = cookies_path
        self.po_token = po_token
        self._ydl_opts = {
            "format": "bestaudio/best",
            "source_address": "0.0.0.0",
            "quiet": True,
            "no_warnings": True,
            "extract_flat": False,
            "socket_timeout": 20,
        }
        if cookies_path:
            self._ydl_opts["cookiefile"] = cookies_path
        if po_token:
            self._ydl_opts["extractor_args"] = {"youtube": {"po_token": [po_token]}}
    
    @retry_with_backoff()
    async def search(self, query: str, filter_type: str = "songs", limit: int = 5) -> list[YTTrack]:
        """Search YouTube Music for tracks."""
        loop = asyncio.get_event_loop()
        try:
            results = await loop.run_in_executor(
                None,
                partial(self.yt.search, query, filter=filter_type, limit=limit)
            )
            
            tracks = []
            for r in results:
                if not r.get("videoId"):
                    continue
                
                duration = None
                if r.get("duration_seconds"):
                    duration = r["duration_seconds"]
                elif r.get("duration"):
                    # Parse duration string like "3:45"
                    duration = self._parse_duration(r["duration"])
                
                artist = "Unknown"
                if r.get("artists") and len(r["artists"]) > 0:
                    artist = r["artists"][0].get("name", "Unknown")
                
                tracks.append(YTTrack(
                    video_id=r["videoId"],
                    title=r.get("title", "Unknown"),
                    artist=artist,
                    duration_seconds=duration,
                    album=r.get("album", {}).get("name") if r.get("album") else None,
                    year=r.get("year"),
                    thumbnail_url=r.get("thumbnails", [{}])[-1].get("url"),
                ))
            
            return tracks
        except Exception as e:
            log.event(Category.API, Event.SEARCH_FAILED, level=logging.ERROR, service="youtube", error=str(e))
            return []
    
    @retry_with_backoff()
    async def get_watch_playlist(self, video_id: str, limit: int = 20) -> list[YTTrack]:
        """Get related tracks from a video's watch playlist."""
        loop = asyncio.get_event_loop()
        try:
            results = await loop.run_in_executor(
                None,
                partial(self.yt.get_watch_playlist, videoId=video_id, limit=limit)
            )
            
            tracks = []
            for t in results.get("tracks", []):
                if not t.get("videoId"):
                    continue
                
                artist = "Unknown"
                if t.get("artists") and len(t["artists"]) > 0:
                    artist = t["artists"][0].get("name", "Unknown")
                
                tracks.append(YTTrack(
                    video_id=t["videoId"],
                    title=t.get("title", "Unknown"),
                    artist=artist,
                    duration_seconds=t.get("length_seconds") or t.get("duration_seconds"),
                    year=t.get("year"),
                ))
            
            return tracks
        except Exception as e:
            log.error_cat(Category.API, "Error getting watch playlist", error=str(e))
            return []
    
    @retry_with_backoff()
    async def get_playlist_tracks(self, playlist_id: str, limit: int = 100) -> list[YTTrack]:
        """Get tracks from a YouTube Music playlist."""
        loop = asyncio.get_event_loop()
        try:
            results = await loop.run_in_executor(
                None,
                partial(self.yt.get_playlist, playlist_id, limit=limit)
            )
            
            tracks = []
            for t in results.get("tracks", []):
                if not t.get("videoId"):
                    continue
                
                artist = "Unknown"
                if t.get("artists") and len(t["artists"]) > 0:
                    artist = t["artists"][0].get("name", "Unknown")
                
                tracks.append(YTTrack(
                    video_id=t["videoId"],
                    title=t.get("title", "Unknown"),
                    artist=artist,
                    duration_seconds=t.get("duration_seconds"),
                ))
            
            return tracks
        except Exception as e:
            log.error_cat(Category.API, "Error getting playlist", error=str(e))
            return []

    @retry_with_backoff()
    async def get_track_info(self, video_id: str) -> YTTrack | None:
        """Get detailed track info for a single video (duration, title, artist, year).

        Used by discovery to fill missing duration/year fields.
        """
        loop = asyncio.get_event_loop()
        url = f"https://www.youtube.com/watch?v={video_id}"

        try:
            # Prefer ytmusicapi's metadata first (non-blocking via executor)
            try:
                def fetch_via_ytmusic():
                    return self.yt.get_song(video_id)

                song_info = await loop.run_in_executor(None, fetch_via_ytmusic)
                if song_info:
                    # ytmusicapi returns nested structures; be defensive when reading
                    video_details = song_info.get("videoDetails") if isinstance(song_info, dict) else None
                    if video_details:
                        title = video_details.get("title") or song_info.get("title") or "Unknown"
                        artist = video_details.get("author") or song_info.get("uploader") or "Unknown"
                        duration = None
                        try:
                            ld = video_details.get("lengthSeconds")
                            if ld:
                                duration = int(ld)
                        except Exception:
                            duration = None

                        year = None
                        publish = video_details.get("publishDate") or video_details.get("uploadDate") or song_info.get("upload_date")
                        if publish:
                            try:
                                year = int(str(publish)[0:4])
                            except Exception:
                                year = None

                        thumbnail = None
                        thumbs = song_info.get("thumbnails") or video_details.get("thumbnail")
                        if thumbs and isinstance(thumbs, list):
                            thumbnail = thumbs[-1].get("url") if thumbs[-1] else None

                        return YTTrack(
                            video_id=video_id,
                            title=title,
                            artist=artist,
                            duration_seconds=duration,
                            album=None,
                            year=year,
                            thumbnail_url=thumbnail,
                        )
            except Exception:
                # Fall back to yt-dlp extraction below
                pass

            # Fallback: use yt-dlp if ytmusicapi didn't return usable info
            def extract():
                opts = {**self._ydl_opts, "skip_download": True, "quiet": True, "no_warnings": True}
                with yt_dlp.YoutubeDL(opts) as ydl:
                    info = ydl.extract_info(url, download=False)
                    return info

            info = await loop.run_in_executor(None, extract)
            if not info:
                return None

            duration = info.get("duration")
            title = info.get("title") or "Unknown"

            # Best-effort artist detection: prefer explicit metadata, fall back to uploader
            artist = info.get("artist") or info.get("uploader") or "Unknown"

            year = None
            if info.get("release_date"):
                try:
                    year = int(info["release_date"][0:4])
                except Exception:
                    pass
            elif info.get("upload_date"):
                try:
                    year = int(info["upload_date"][0:4])
                except Exception:
                    pass

            thumbnail = None
            thumbs = info.get("thumbnails")
            if thumbs and isinstance(thumbs, list):
                thumbnail = thumbs[-1].get("url") if thumbs[-1] else None

            return YTTrack(
                video_id=video_id,
                title=title,
                artist=artist,
                duration_seconds=duration,
                album=None,
                year=year,
                thumbnail_url=thumbnail,
            )
        except Exception as e:
            log.event(Category.API, Event.API_ERROR, level=logging.ERROR, service="youtube", video_id=video_id, error=str(e))
            return None
    
    @retry_with_backoff(retries=2, backoff_in_seconds=1)
    async def get_stream_url(self, video_id: str) -> StreamInfo | None:
        """Get the audio stream URL and HTTP headers for a video using yt-dlp.
        
        Has retry logic and is designed to not block the event loop indefinitely.
        """
        loop = asyncio.get_event_loop()
        url = f"https://www.youtube.com/watch?v={video_id}"

        try:
            def extract():
                # Use isolated options to prevent interference
                opts = {
                    **self._ydl_opts,
                    "retries": 2,  # Reduce retry count
                    "fragment_retries": 2,
                    "socket_timeout": 15,  # Shorter timeout
                    "http_chunk_size": 10485760,  # 10MB chunks
                }
                with yt_dlp.YoutubeDL(opts) as ydl:
                    info = ydl.extract_info(url, download=False)
                    stream_url = info.get("url")
                    if not stream_url:
                        return None
                    return StreamInfo(
                        url=stream_url,
                        http_headers=info.get("http_headers"),
                    )

            return await loop.run_in_executor(None, extract)
        except Exception as e:
            log.event(Category.API, Event.API_ERROR, level=logging.ERROR, service="youtube", video_id=video_id, error=str(e))
            raise  # Re-raise to trigger retry
    
    @retry_with_backoff()
    async def search_playlists(self, query: str, limit: int = 5) -> list[dict]:
        """Search for playlists."""
        loop = asyncio.get_event_loop()
        try:
            results = await loop.run_in_executor(
                None,
                partial(self.yt.search, query, filter="playlists", limit=limit)
            )
            return [
                {
                    "browse_id": r.get("browseId"),
                    "title": r.get("title"),
                    "author": r.get("author"),
                }
                for r in results if r.get("browseId")
            ]
        except Exception as e:
            log.error_cat(Category.API, "Error searching playlists", error=str(e))
            return []
    
    def _parse_duration(self, duration_str: str) -> int | None:
        """Parse duration string like '3:45' to seconds."""
        if not duration_str:
            return None
        try:
            parts = duration_str.split(":")
            if len(parts) == 2:
                return int(parts[0]) * 60 + int(parts[1])
            elif len(parts) == 3:
                return int(parts[0]) * 3600 + int(parts[1]) * 60 + int(parts[2])
        except ValueError:
            pass
        return None
