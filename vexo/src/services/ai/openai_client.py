"""OpenAI client that conforms to BaseAIClient.

Uses OpenAI's Chat Completions API and enforces strict JSON-only output.

Config via env:
- OPENAI_API_KEY (required)
- OPENAI_MODEL (default: gpt-4o-mini)
- OPENAI_BASE_URL (default: https://api.openai.com)

Note: This is treated as an AI provider alongside ollama/llamacpp.
"""

from __future__ import annotations

import asyncio
import json
import time
from typing import Optional

import aiohttp

from src.services.ai.base import BaseAIClient, AISuggestion, AIPlayModeResult
from src.services.ai.language_policy import language_policy_prompt
from src.utils.logging import get_logger, Category

log = get_logger(__name__)


class OpenAIClient(BaseAIClient):
    provider_name = "openai"

    SYSTEM_PROMPT = (
        "You are a music recommendation service."
        " STRICTLY return only a single valid JSON object as the entire response — nothing else."
        " Do NOT include any explanation, commentary, markdown, code fences, or analysis."
        " Do NOT output any text before or after the JSON."
        " If you cannot produce valid JSON exactly as requested, output exactly this JSON: {\"error\":\"unable_to_comply\"}"
    )

    PLAY_MODE_SYSTEM_PROMPT = (
        "You are a music recommendation service."
        " STRICTLY return only a single valid JSON object with exactly two top-level keys: \"autoplay_next\" and \"alternatives\" — nothing else."
        " The \"autoplay_next\" value must be an object with keys: \"title\", \"artist\", \"reason\"."
        " The \"alternatives\" value must be an array of objects, each with keys: \"title\", \"artist\", \"reason\"."
        " Do NOT include any explanation, commentary, markdown, code fences, or analysis."
        " Do NOT output any text before or after the JSON."
        " If you cannot produce valid JSON exactly as requested, output exactly this JSON: {\"error\":\"unable_to_comply\"}"
    )

    def __init__(
        self,
        api_key: str | None,
        model: str = "gpt-4o-mini",
        base_url: str = "https://api.openai.com",
        health_cache_ttl: int = 120,
        request_timeout: int = 30,
    ):
        self.api_key = api_key or ""
        self.model = model or "gpt-4o-mini"
        self.base_url = (base_url or "https://api.openai.com").rstrip("/")
        self.health_cache_ttl = health_cache_ttl
        self.request_timeout = request_timeout

        self._last_health_check = 0.0
        self._last_health_status = False
        self._health_lock = asyncio.Lock()

    def _headers(self) -> dict:
        return {
            "Content-Type": "application/json",
            "Authorization": f"Bearer {self.api_key}",
        }

    async def health_check(self) -> bool:
        if not self.api_key:
            return False

        now = time.monotonic()
        if now - self._last_health_check < self.health_cache_ttl:
            return self._last_health_status

        async with self._health_lock:
            if now - self._last_health_check < self.health_cache_ttl:
                return self._last_health_status

            try:
                url = f"{self.base_url}/v1/models"
                async with aiohttp.ClientSession() as session:
                    async with session.get(url, headers=self._headers(), timeout=aiohttp.ClientTimeout(total=5)) as resp:
                        ok = resp.status == 200
                        self._last_health_status = ok
                        self._last_health_check = now
                        if not ok:
                            text = await resp.text()
                            log.warning_cat(Category.API, "OpenAI health check failed", status=resp.status, body=text[:200])
                        return ok
            except Exception as e:
                self._last_health_status = False
                self._last_health_check = now
                log.warning_cat(Category.API, "OpenAI health check error", error=str(e))
                return False

    @staticmethod
    def _truncate(text: str, limit: int = 500) -> str:
        if not text:
            return ""
        return text if len(text) <= limit else f"{text[:limit]}..."

    @staticmethod
    def _extract_text(data: dict) -> Optional[str]:
        if not isinstance(data, dict):
            return None
        choices = data.get("choices")
        if isinstance(choices, list) and choices:
            c0 = choices[0]
            if isinstance(c0, dict):
                msg = c0.get("message")
                if isinstance(msg, dict) and isinstance(msg.get("content"), str):
                    return msg.get("content")
                if isinstance(c0.get("text"), str):
                    return c0.get("text")
        if isinstance(data.get("text"), str):
            return data.get("text")
        return None

    @staticmethod
    def _clean_to_json(text: str) -> Optional[object]:
        if not text:
            return None
        cleaned = text.strip()
        start = cleaned.find("{")
        end = cleaned.rfind("}")
        if start != -1 and end != -1 and end > start:
            cleaned = cleaned[start : end + 1]
        try:
            return json.loads(cleaned)
        except Exception:
            return None

    @staticmethod
    def _normalize_suggestions(parsed: object, n_candidates: int) -> list[AISuggestion]:
        items: list[dict] = []
        if isinstance(parsed, dict) and isinstance(parsed.get("suggestions"), list):
            items = [i for i in parsed.get("suggestions", []) if isinstance(i, dict)]
        elif isinstance(parsed, list):
            items = [i for i in parsed if isinstance(i, dict)]

        out: list[AISuggestion] = []
        for item in items[:n_candidates]:
            title = item.get("title")
            artist = item.get("artist")
            if title and artist:
                out.append(AISuggestion(title=title, artist=artist, reason=item.get("reason", "AI suggested")))
        return out

    async def _chat(self, system_prompt: str, user_prompt: str, max_tokens: int = 1024) -> Optional[dict]:
        if not self.api_key:
            return None

        url = f"{self.base_url}/v1/chat/completions"
        payload = {
            "model": self.model,
            "messages": [
                {"role": "system", "content": system_prompt},
                {"role": "user", "content": user_prompt},
            ],
            "temperature": 0.0,
            "max_tokens": max_tokens,
        }

        try:
            async with aiohttp.ClientSession() as session:
                async with session.post(url, json=payload, headers=self._headers(), timeout=aiohttp.ClientTimeout(total=self.request_timeout)) as resp:
                    text = await resp.text()
                    if resp.status != 200:
                        log.warning_cat(Category.API, "OpenAI chat failed", status=resp.status, body=self._truncate(text))
                        return None
                    try:
                        return json.loads(text)
                    except Exception:
                        log.warning_cat(Category.API, "OpenAI returned non-JSON", body=self._truncate(text))
                        return None
        except Exception as e:
            log.warning_cat(Category.API, "OpenAI chat error", error=str(e))
            return None

    async def suggest_from_seed(self, seed_track: dict, exclude_list: list[dict], n_candidates: int = 20) -> list[AISuggestion]:
        seed_title = seed_track.get("title", "Unknown")
        seed_artist = seed_track.get("artist", "Unknown")
        language_policy = language_policy_prompt(seed_track)

        exclude_str = ""
        if exclude_list:
            exclude_items = [f"- {t.get('title','')} by {t.get('artist','')}" for t in exclude_list[:50]]
            exclude_str = "\n\nDo NOT suggest any of these tracks:\n" + "\n".join(exclude_items)

        prompt = (
            f"Based on the seed track \"{seed_title}\" by {seed_artist}, suggest {n_candidates} similar songs.\n\n"
            f"{language_policy}\n\n"
            "Return ONLY valid JSON in this exact format (no other text):\n"
            "{\n  \"suggestions\": [\n    {\"title\": \"Song Name\", \"artist\": \"Artist Name\", \"reason\": \"Brief reason\"}\n  ]\n}\n\n"
            "Each suggestion must have title, artist, reason." + exclude_str + "\n\n"
            "Return strictly valid JSON with no markdown, no extra text."
        )

        data = await self._chat(self.SYSTEM_PROMPT, prompt, max_tokens=1200)
        if not data:
            return []

        content = self._extract_text(data) or ""
        parsed = self._clean_to_json(content)
        if parsed is None:
            log.warning_cat(Category.API, "OpenAI suggestion parse failed", text=self._truncate(content))
            return []

        return self._normalize_suggestions(parsed, n_candidates)

    async def suggest_for_user(self, liked_tracks: list[dict], disliked_tracks: list[dict], group_disliked_tracks: list[dict], exclude_list: list[dict], n_candidates: int = 20) -> list[AISuggestion]:
        likes = "\n".join([f"- {t.get('title','')} by {t.get('artist','')}" for t in (liked_tracks or [])[:20]]) or "none provided"
        language_policy = language_policy_prompt()

        all_excludes = (disliked_tracks or []) + (group_disliked_tracks or []) + (exclude_list or [])
        exclude_str = ""
        if all_excludes:
            exclude_items = [f"- {t.get('title','')} by {t.get('artist','')}" for t in all_excludes[:80]]
            exclude_str = "\n\nDo NOT suggest any of these tracks:\n" + "\n".join(exclude_items)

        prompt = (
            f"Based on a user's music preferences, suggest {n_candidates} songs they would enjoy.\n\n"
            f"{language_policy}\n\n"
            f"USER LIKES:\n{likes}\n\n"
            "Return ONLY valid JSON in this exact format (no other text):\n"
            "{\n  \"suggestions\": [\n    {\"title\": \"Song Name\", \"artist\": \"Artist Name\", \"reason\": \"Brief reason\"}\n  ]\n}\n\n"
            "Each suggestion must have title, artist, reason." + exclude_str + "\n\n"
            "Return strictly valid JSON with no markdown, no extra text."
        )

        data = await self._chat(self.SYSTEM_PROMPT, prompt, max_tokens=1200)
        if not data:
            return []

        content = self._extract_text(data) or ""
        parsed = self._clean_to_json(content)
        if parsed is None:
            log.warning_cat(Category.API, "OpenAI suggestion parse failed", text=self._truncate(content))
            return []

        return self._normalize_suggestions(parsed, n_candidates)

    async def suggest_for_play_mode(self, seed_track: dict, exclude_list: list[dict], n_alternatives: int = 5) -> Optional[AIPlayModeResult]:
        seed_title = seed_track.get("title", "Unknown")
        seed_artist = seed_track.get("artist", "Unknown")
        language_policy = language_policy_prompt(seed_track)

        exclude_str = ""
        if exclude_list:
            exclude_items = [f"- {t.get('title','')} by {t.get('artist','')}" for t in exclude_list[:50]]
            exclude_str = "\n\nDo NOT suggest any of these tracks:\n" + "\n".join(exclude_items)

        prompt = (
            f"Based on the currently playing track \"{seed_title}\" by {seed_artist}, suggest:\n"
            "1) ONE best default next track (autoplay_next)\n"
            f"2) {n_alternatives} alternative tracks (alternatives)\n\n"
            f"{language_policy}\n\n"
            "Return ONLY valid JSON in this EXACT format (no other text):\n"
            "{\n"
            "  \"autoplay_next\": {\"title\": \"Song Name\", \"artist\": \"Artist Name\", \"reason\": \"Brief reason\"},\n"
            "  \"alternatives\": [\n"
            "    {\"title\": \"Song Name\", \"artist\": \"Artist Name\", \"reason\": \"Brief reason\"}\n"
            "  ]\n"
            "}\n\n"
            "Return strictly valid JSON with no markdown, no extra text." + exclude_str
        )

        data = await self._chat(self.PLAY_MODE_SYSTEM_PROMPT, prompt, max_tokens=1500)
        if not data:
            return None

        content = self._extract_text(data) or ""
        parsed = self._clean_to_json(content)
        if not isinstance(parsed, dict):
            log.warning_cat(Category.API, "OpenAI playmode parse failed", text=self._truncate(content))
            return None

        ap = parsed.get("autoplay_next")
        alts = parsed.get("alternatives")
        if not isinstance(ap, dict) or not isinstance(alts, list):
            return None
        if not ap.get("title") or not ap.get("artist"):
            return None

        autoplay = AISuggestion(title=ap.get("title"), artist=ap.get("artist"), reason=ap.get("reason", "AI suggested"))
        alternatives: list[AISuggestion] = []
        for item in alts[:n_alternatives]:
            if isinstance(item, dict) and item.get("title") and item.get("artist"):
                alternatives.append(AISuggestion(title=item["title"], artist=item["artist"], reason=item.get("reason", "Alternative")))

        if not alternatives:
            return None

        return AIPlayModeResult(autoplay_next=autoplay, alternatives=alternatives)
