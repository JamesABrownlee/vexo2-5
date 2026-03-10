"""
Ollama AI client for music discovery and recommendations.

This service provides:
- Health check with caching
- Seed-based suggestions (for /play ai)
- User preference-based suggestions (for join-triggered recommendations)
- Strict JSON output parsing with graceful fallbacks
"""
import asyncio
import json
import logging
import time
from dataclasses import dataclass
from typing import Optional

import aiohttp

from src.utils.logging import get_logger, Category

log = get_logger(__name__)


@dataclass
class AISuggestion:
    """A single AI-suggested track."""
    title: str
    artist: str
    reason: str


@dataclass
class AIPlayModeResult:
    """Result from AI play mode with autoplay next + alternatives."""
    autoplay_next: AISuggestion
    alternatives: list[AISuggestion]


class OllamaClient:
    provider_name = "ollama"
    """Async Ollama client for AI-powered music discovery."""
    
    def __init__(
        self,
        base_url: str = "https://ollama.plingindigo.org",
        model: str = "llama3.1:8b",
        bearer_token: str = None,
        health_cache_ttl: int = 45,
        request_timeout: int = 25,
    ):
        """
        Initialize Ollama client.
        
        Args:
            base_url: Base URL for the Ollama API
            model: Model name to use for completions
            bearer_token: Optional Bearer token for authentication
            health_cache_ttl: Seconds to cache health status
            request_timeout: Timeout for API requests in seconds
        """
        self.base_url = base_url.rstrip("/")
        self.model = model
        self.bearer_token = bearer_token
        self.health_cache_ttl = health_cache_ttl
        self.request_timeout = request_timeout
        
        self._last_health_check: float = 0
        self._last_health_status: bool = False
        self._health_lock = asyncio.Lock()
        
    async def health_check(self) -> bool:
        """
        Check if Ollama service is available.
        
        Returns cached result if within TTL, otherwise performs fresh check.
        
        Returns:
            True if service is healthy, False otherwise
        """
        now = time.monotonic()
        
        # Return cached status if still valid
        if now - self._last_health_check < self.health_cache_ttl:
            return self._last_health_status
        
        async with self._health_lock:
            # Double-check after acquiring lock
            if now - self._last_health_check < self.health_cache_ttl:
                return self._last_health_status
            
            try:
                headers = {}
                if self.bearer_token:
                    headers["Authorization"] = f"Bearer {self.bearer_token}"
                
                async with aiohttp.ClientSession() as session:
                    async with session.get(
                        f"{self.base_url}/api/tags",
                        headers=headers,
                        timeout=aiohttp.ClientTimeout(total=5)
                    ) as resp:
                        if resp.status == 200:
                            self._last_health_status = True
                            self._last_health_check = now
                            log.debug_cat(
                                Category.API,
                                "Ollama health check passed",
                                url=self.base_url
                            )
                            return True
                        else:
                            log.warning_cat(
                                Category.API,
                                "Ollama health check failed",
                                status=resp.status,
                                url=self.base_url
                            )
                            self._last_health_status = False
                            self._last_health_check = now
                            return False
            except asyncio.TimeoutError:
                log.warning_cat(
                    Category.API,
                    "Ollama health check timeout",
                    url=self.base_url
                )
                self._last_health_status = False
                self._last_health_check = now
                return False
            except Exception as e:
                log.warning_cat(
                    Category.API,
                    "Ollama health check error",
                    error=str(e),
                    url=self.base_url
                )
                self._last_health_status = False
                self._last_health_check = now
                return False
    
    async def _generate(self, prompt: str, system_prompt: str = "") -> Optional[dict]:
        """
        Send a generation request to Ollama.
        
        Args:
            prompt: User prompt
            system_prompt: System prompt for context
            
        Returns:
            Parsed JSON response or None on failure
        """
        try:
            payload = {
                "model": self.model,
                "prompt": prompt,
                "stream": False,
                "format": "json",  # Request JSON output
            }
            
            if system_prompt:
                payload["system"] = system_prompt
            
            headers = {}
            if self.bearer_token:
                headers["Authorization"] = f"Bearer {self.bearer_token}"
            
            async with aiohttp.ClientSession() as session:
                async with session.post(
                    f"{self.base_url}/api/generate",
                    json=payload,
                    headers=headers,
                    timeout=aiohttp.ClientTimeout(total=self.request_timeout)
                ) as resp:
                    if resp.status != 200:
                        log.warning_cat(
                            Category.API,
                            "Ollama generation failed",
                            status=resp.status,
                            url=self.base_url
                        )
                        return None
                    
                    data = await resp.json()
                    response_text = data.get("response", "")
                    
                    # Parse JSON from response
                    try:
                        return json.loads(response_text)
                    except json.JSONDecodeError as e:
                        log.warning_cat(
                            Category.API,
                            "Ollama returned invalid JSON",
                            error=str(e),
                            response_preview=response_text[:200]
                        )
                        return None
                        
        except asyncio.TimeoutError:
            log.warning_cat(
                Category.API,
                "Ollama generation timeout",
                timeout=self.request_timeout
            )
            return None
        except Exception as e:
            log.exception_cat(
                Category.API,
                "Ollama generation error",
                error=str(e)
            )
            return None
    
    async def suggest_from_seed(
        self,
        seed_track: dict,
        exclude_list: list[dict],
        n_candidates: int = 20
    ) -> list[AISuggestion]:
        """
        Get AI suggestions based on a seed track.
        
        Args:
            seed_track: Dict with 'title', 'artist', optionally 'genre', 'year'
            exclude_list: List of dicts with 'title', 'artist' to exclude
            n_candidates: Number of suggestions to request
            
        Returns:
            List of AISuggestion objects
        """
        # Build exclude string
        exclude_str = ""
        if exclude_list:
            exclude_items = [f"- {t.get('title', '')} by {t.get('artist', '')}" for t in exclude_list[:50]]
            exclude_str = "\n\nDo NOT suggest any of these tracks:\n" + "\n".join(exclude_items)
        
        seed_title = seed_track.get("title", "Unknown")
        seed_artist = seed_track.get("artist", "Unknown")
        seed_genre = seed_track.get("genre", "")
        seed_year = seed_track.get("year", "")
        
        genre_info = f"\nGenre: {seed_genre}" if seed_genre else ""
        year_info = f"\nYear: {seed_year}" if seed_year else ""
        
        prompt = f"""Based on the seed track "{seed_title}" by {seed_artist}{genre_info}{year_info}, suggest {n_candidates} similar songs that a listener would enjoy.

Return ONLY valid JSON in this exact format (no other text):
{{
  "suggestions": [
    {{"title": "Song Name", "artist": "Artist Name", "reason": "Brief reason"}},
    ...
  ]
}}

Each suggestion must have:
- title: The song title
- artist: The artist name
- reason: A brief (10-20 word) explanation of why it's similar{exclude_str}

Return strictly valid JSON with no markdown, no extra text."""
        
        system_prompt = "You are a music recommendation AI. You respond ONLY with valid JSON. No markdown, no explanations, just pure JSON."
        
        result = await self._generate(prompt, system_prompt)
        if not result or "suggestions" not in result:
            return []
        
        suggestions = []
        for item in result["suggestions"][:n_candidates]:
            if isinstance(item, dict) and "title" in item and "artist" in item:
                suggestions.append(AISuggestion(
                    title=item["title"],
                    artist=item["artist"],
                    reason=item.get("reason", "Similar style and vibe")
                ))
        
        log.info_cat(
            Category.API,
            "Ollama seed suggestions generated",
            seed=f"{seed_title} by {seed_artist}",
            count=len(suggestions)
        )
        
        return suggestions
    
    async def suggest_for_user(
        self,
        liked_tracks: list[dict],
        disliked_tracks: list[dict],
        group_disliked_tracks: list[dict],
        exclude_list: list[dict],
        n_candidates: int = 20
    ) -> list[AISuggestion]:
        """
        Get personalized AI suggestions based on user preferences.
        
        Args:
            liked_tracks: List of dicts with 'title', 'artist' the user likes
            disliked_tracks: List of dicts with 'title', 'artist' the user dislikes
            group_disliked_tracks: List of dicts disliked by other VC members
            exclude_list: Additional tracks to exclude (recently played)
            n_candidates: Number of suggestions to request
            
        Returns:
            List of AISuggestion objects
        """
        # Build likes string
        likes_str = "none provided"
        if liked_tracks:
            like_items = [f"- {t.get('title', '')} by {t.get('artist', '')}" for t in liked_tracks[:20]]
            likes_str = "\n".join(like_items)
        
        # Build dislikes/exclusions
        all_excludes = (disliked_tracks or []) + (group_disliked_tracks or []) + (exclude_list or [])
        exclude_str = ""
        if all_excludes:
            exclude_items = [f"- {t.get('title', '')} by {t.get('artist', '')}" for t in all_excludes[:80]]
            exclude_str = "\n\nDo NOT suggest any of these tracks (user/group dislikes or recently played):\n" + "\n".join(exclude_items)
        
        prompt = f"""Based on a user's music preferences, suggest {n_candidates} songs they would enjoy.

USER LIKES:
{likes_str}

Return ONLY valid JSON in this exact format (no other text):
{{
  "suggestions": [
    {{"title": "Song Name", "artist": "Artist Name", "reason": "Brief reason"}},
    ...
  ]
}}

Each suggestion must have:
- title: The song title
- artist: The artist name
- reason: A brief (10-20 word) explanation of why the user would like it{exclude_str}

Return strictly valid JSON with no markdown, no extra text."""
        
        system_prompt = "You are a personalized music recommendation AI. You respond ONLY with valid JSON. No markdown, no explanations, just pure JSON."
        
        result = await self._generate(prompt, system_prompt)
        if not result or "suggestions" not in result:
            return []
        
        suggestions = []
        for item in result["suggestions"][:n_candidates]:
            if isinstance(item, dict) and "title" in item and "artist" in item:
                suggestions.append(AISuggestion(
                    title=item["title"],
                    artist=item["artist"],
                    reason=item.get("reason", "Matches your taste")
                ))
        
        log.info_cat(
            Category.API,
            "Ollama user suggestions generated",
            liked_count=len(liked_tracks),
            count=len(suggestions)
        )
        
        return suggestions
    
    async def suggest_for_play_mode(
        self,
        seed_track: dict,
        exclude_list: list[dict],
        n_alternatives: int = 5
    ) -> Optional[AIPlayModeResult]:
        """
        Get AI suggestions for play mode: 1 autoplay next + N alternatives.
        
        Args:
            seed_track: Dict with 'title', 'artist', optionally 'genre', 'year'
            exclude_list: List of dicts with 'title', 'artist' to exclude
            n_alternatives: Number of alternative suggestions to request
            
        Returns:
            AIPlayModeResult with autoplay_next and alternatives, or None on failure
        """
        # Build exclude string
        exclude_str = ""
        if exclude_list:
            exclude_items = [f"- {t.get('title', '')} by {t.get('artist', '')}" for t in exclude_list[:50]]
            exclude_str = "\n\nDo NOT suggest any of these tracks:\n" + "\n".join(exclude_items)
        
        seed_title = seed_track.get("title", "Unknown")
        seed_artist = seed_track.get("artist", "Unknown")
        seed_genre = seed_track.get("genre", "")
        seed_year = seed_track.get("year", "")
        
        genre_info = f"\nGenre: {seed_genre}" if seed_genre else ""
        year_info = f"\nYear: {seed_year}" if seed_year else ""
        
        prompt = f"""Based on the currently playing track "{seed_title}" by {seed_artist}{genre_info}{year_info}, suggest:
1. ONE best default next track (autoplay_next)
2. {n_alternatives} alternative tracks for the user to choose from

Return ONLY valid JSON in this EXACT format (no other text):
{{
  "autoplay_next": {{"title": "Song Name", "artist": "Artist Name", "reason": "Brief reason"}},
  "alternatives": [
    {{"title": "Song Name", "artist": "Artist Name", "reason": "Brief reason"}},
    {{"title": "Song Name", "artist": "Artist Name", "reason": "Brief reason"}},
    {{"title": "Song Name", "artist": "Artist Name", "reason": "Brief reason"}},
    {{"title": "Song Name", "artist": "Artist Name", "reason": "Brief reason"}},
    {{"title": "Song Name", "artist": "Artist Name", "reason": "Brief reason"}}
  ]
}}

Each suggestion must have:
- title: The song title
- artist: The artist name
- reason: A brief (10-15 word) explanation of why it fits{exclude_str}

Return strictly valid JSON with no markdown, no extra text."""
        
        system_prompt = "You are a music recommendation AI for an autoplay system. You respond ONLY with valid JSON. No markdown, no explanations, just pure JSON."
        
        result = await self._generate(prompt, system_prompt)
        if not result:
            return None
        
        # Validate structure
        if "autoplay_next" not in result or "alternatives" not in result:
            log.warning_cat(
                Category.API,
                "AI play mode response missing required fields",
                has_autoplay=("autoplay_next" in result),
                has_alternatives=("alternatives" in result)
            )
            return None
        
        # Parse autoplay_next
        autoplay_data = result["autoplay_next"]
        if not isinstance(autoplay_data, dict) or "title" not in autoplay_data or "artist" not in autoplay_data:
            log.warning_cat(Category.API, "AI play mode autoplay_next invalid format")
            return None
        
        autoplay_next = AISuggestion(
            title=autoplay_data["title"],
            artist=autoplay_data["artist"],
            reason=autoplay_data.get("reason", "AI recommended")
        )
        
        # Parse alternatives
        alternatives = []
        for item in result["alternatives"][:n_alternatives]:
            if isinstance(item, dict) and "title" in item and "artist" in item:
                alternatives.append(AISuggestion(
                    title=item["title"],
                    artist=item["artist"],
                    reason=item.get("reason", "Alternative choice")
                ))
        
        if not alternatives:
            log.warning_cat(Category.API, "AI play mode returned no valid alternatives")
            return None
        
        log.info_cat(
            Category.API,
            "AI play mode suggestions generated",
            seed=f"{seed_title} by {seed_artist}",
            alternatives_count=len(alternatives)
        )
        
        return AIPlayModeResult(autoplay_next=autoplay_next, alternatives=alternatives)
