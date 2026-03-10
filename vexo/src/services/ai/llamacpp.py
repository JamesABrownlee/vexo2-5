"""
Minimal Llama.cpp client that conforms to the BaseAIClient interface.

This implementation prefers OpenAI-compatible endpoints exposed by some
llama.cpp HTTP wrappers (e.g. /v1/chat/completions or /v1/completions). If
those are not available, the health check will fail and the factory may
fall back to Ollama.

Responses are normalized to the same types used by the existing Ollama client.
"""
import asyncio
import json
import logging
import time
from dataclasses import dataclass
from typing import Optional

import aiohttp

from src.utils.logging import get_logger, Category
from src.services.ai.base import BaseAIClient, AISuggestion, AIPlayModeResult

log = get_logger(__name__)


class LlamaCppClient(BaseAIClient):
    provider_name = "llamacpp"

    def __init__(self, base_url: str = "http://localhost:8080", model: str = "", bearer_token: Optional[str] = None, health_cache_ttl: int = 45, request_timeout: int = 25):
        self.base_url = base_url.rstrip("/")
        self.model = model
        self.bearer_token = bearer_token
        self.health_cache_ttl = health_cache_ttl
        self.request_timeout = request_timeout

        self._last_health_check = 0.0
        self._last_health_status = False
        self._health_lock = asyncio.Lock()

    async def health_check(self) -> bool:
        now = time.monotonic()
        if now - self._last_health_check < self.health_cache_ttl:
            return self._last_health_status

        async with self._health_lock:
            if now - self._last_health_check < self.health_cache_ttl:
                return self._last_health_status
            try:
                # Try OpenAI-compatible models endpoint first
                url = f"{self.base_url}/v1/models"
                async with aiohttp.ClientSession() as session:
                    async with session.get(url, timeout=aiohttp.ClientTimeout(total=3)) as resp:
                        ok = resp.status == 200
                        self._last_health_status = ok
                        self._last_health_check = now
                        if ok:
                            log.debug_cat(Category.API, "LlamaCPP health check passed", url=self.base_url)
                        else:
                            log.warning_cat(Category.API, "LlamaCPP health check failed", status=resp.status, url=self.base_url)
                        return ok
            except Exception as e:
                # Try a lightweight ping endpoint as a fallback
                try:
                    ping_url = f"{self.base_url}/ping"
                    async with aiohttp.ClientSession() as session:
                        async with session.get(ping_url, timeout=aiohttp.ClientTimeout(total=2)) as resp:
                            ok = resp.status == 200
                            self._last_health_status = ok
                            self._last_health_check = now
                            if ok:
                                log.debug_cat(Category.API, "LlamaCPP ping passed", url=ping_url)
                            else:
                                log.warning_cat(Category.API, "LlamaCPP ping failed", status=resp.status, url=ping_url)
                            return ok
                except Exception:
                    log.warning_cat(Category.API, "LlamaCPP health check error", error=str(e), url=self.base_url)
                    self._last_health_status = False
                    self._last_health_check = now
                    return False

    async def _post_openai(self, payload: dict) -> Optional[dict]:
        """Helper to call OpenAI-compatible endpoints on the llama.cpp server."""
        try:
            url = f"{self.base_url}/v1/chat/completions"
            headers = {"Content-Type": "application/json"}
            if self.bearer_token:
                headers["Authorization"] = f"Bearer {self.bearer_token}"

            async with aiohttp.ClientSession() as session:
                async with session.post(url, json=payload, headers=headers, timeout=aiohttp.ClientTimeout(total=self.request_timeout)) as resp:
                    if resp.status != 200:
                        # Try /v1/completions as a fallback
                        alt = f"{self.base_url}/v1/completions"
                        async with session.post(alt, json=payload, headers=headers, timeout=aiohttp.ClientTimeout(total=self.request_timeout)) as resp2:
                            if resp2.status != 200:
                                log.warning_cat(Category.API, "LlamaCPP generation failed", status=resp2.status, url=alt)
                                return None
                            data2 = await resp2.json()
                            return data2
                    data = await resp.json()
                    return data
        except Exception as e:
            log.warning_cat(Category.API, "LlamaCPP generation error", error=str(e))
            return None

    async def suggest_from_seed(self, seed_track: dict, exclude_list: list[dict], n_candidates: int = 20) -> list[AISuggestion]:
        # Use similar prompt as Ollama but send via OpenAI-compatible call
        seed_title = seed_track.get("title", "Unknown")
        seed_artist = seed_track.get("artist", "Unknown")
        prompt = f"Based on the seed track \"{seed_title}\" by {seed_artist}, suggest {n_candidates} similar songs. Return JSON with a top-level 'suggestions' array of {{title, artist, reason}} objects."

        payload = {
            "model": self.model or "gpt",
            "messages": [{"role": "user", "content": prompt}],
            "max_tokens": 512,
            "temperature": 0.7,
        }

        data = await self._post_openai(payload)
        if not data:
            return []

        # Attempt to extract text
        text = None
        if "choices" in data and data["choices"]:
            c = data["choices"][0]
            text = (c.get("message") or {}).get("content") if isinstance(c.get("message"), dict) else c.get("text")
        if not text and isinstance(data.get("text"), str):
            text = data.get("text")

        if not text:
            return []

        # Try to parse JSON
        try:
            parsed = json.loads(text)
        except Exception:
            # Try to find JSON substring
            try:
                start = text.find('{')
                if start != -1:
                    parsed = json.loads(text[start:])
                else:
                    return []
            except Exception:
                return []

        suggestions = []
        for item in parsed.get("suggestions", [])[:n_candidates]:
            if isinstance(item, dict) and "title" in item and "artist" in item:
                suggestions.append(AISuggestion(title=item["title"], artist=item["artist"], reason=item.get("reason", "AI suggested")))

        return suggestions

    async def suggest_for_user(self, liked_tracks: list[dict], disliked_tracks: list[dict], group_disliked_tracks: list[dict], exclude_list: list[dict], n_candidates: int = 20) -> list[AISuggestion]:
        likes = "\n".join([f"- {t.get('title','')} by {t.get('artist','')}" for t in (liked_tracks or [])[:20]]) or "none provided"
        prompt = f"Based on the user's likes:\n{likes}\nSuggest {n_candidates} songs the user would enjoy. Return JSON with suggestions array of {{title,artist,reason}}."
        payload = {
            "model": self.model or "gpt",
            "messages": [{"role": "user", "content": prompt}],
            "max_tokens": 512,
            "temperature": 0.7,
        }
        data = await self._post_openai(payload)
        if not data:
            return []

        text = None
        if "choices" in data and data["choices"]:
            c = data["choices"][0]
            text = (c.get("message") or {}).get("content") if isinstance(c.get("message"), dict) else c.get("text")
        if not text and isinstance(data.get("text"), str):
            text = data.get("text")
        if not text:
            return []

        try:
            parsed = json.loads(text)
        except Exception:
            try:
                start = text.find('{')
                if start != -1:
                    parsed = json.loads(text[start:])
                else:
                    return []
            except Exception:
                return []

        suggestions = []
        for item in parsed.get("suggestions", [])[:n_candidates]:
            if isinstance(item, dict) and "title" in item and "artist" in item:
                suggestions.append(AISuggestion(title=item["title"], artist=item["artist"], reason=item.get("reason", "AI suggested")))
        return suggestions

    async def suggest_for_play_mode(self, seed_track: dict, exclude_list: list[dict], n_alternatives: int = 5) -> Optional[AIPlayModeResult]:
        seed_title = seed_track.get("title", "Unknown")
        seed_artist = seed_track.get("artist", "Unknown")
        prompt = f"Based on {seed_title} by {seed_artist}, return JSON with 'autoplay_next' and 'alternatives' array (each item title,artist,reason)."
        payload = {
            "model": self.model or "gpt",
            "messages": [{"role": "user", "content": prompt}],
            "max_tokens": 512,
            "temperature": 0.7,
        }
        data = await self._post_openai(payload)
        if not data:
            return None

        text = None
        if "choices" in data and data["choices"]:
            c = data["choices"][0]
            text = (c.get("message") or {}).get("content") if isinstance(c.get("message"), dict) else c.get("text")
        if not text and isinstance(data.get("text"), str):
            text = data.get("text")
        if not text:
            return None

        try:
            parsed = json.loads(text)
        except Exception:
            try:
                start = text.find('{')
                if start != -1:
                    parsed = json.loads(text[start:])
                else:
                    return None
            except Exception:
                return None

        ap = parsed.get("autoplay_next")
        alts = parsed.get("alternatives", [])
        if not ap or not isinstance(ap, dict) or not alts:
            return None

        autoplay = AISuggestion(title=ap.get("title"), artist=ap.get("artist"), reason=ap.get("reason", "AI recommended"))
        alternatives = []
        for item in alts[:n_alternatives]:
            if isinstance(item, dict) and "title" in item and "artist" in item:
                alternatives.append(AISuggestion(title=item["title"], artist=item["artist"], reason=item.get("reason", "Alternative")))

        if not alternatives:
            return None

        return AIPlayModeResult(autoplay_next=autoplay, alternatives=alternatives)
