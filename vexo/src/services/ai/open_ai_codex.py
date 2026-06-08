"""OpenAI Codex client via internal proxy transport.

This provider is designed for environments where direct outbound OpenAI calls
are not desired, and all traffic must go through an internal proxy endpoint.

Expected proxy API (HTTP POST):
- Authorization: Bearer <PROXY_TOKEN>
- JSON body:
  {
    "label": "<string>",
    "prompt": "<full prompt text>"
  }

Config via env:
- CODEX_PROXY_URL   (required) e.g. https://your-host/api/musicbots/suggest
- CODEX_PROXY_TOKEN (required)
- CODEX_PROXY_LABEL (optional, default: hal)
- CODEX_MODEL       (default: gpt-5.1-codex-mini)

Note: This conforms to BaseAIClient and is selectable as LOCAL_AI_PROVIDER=openai_codex.
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


class OpenAICodexClient(BaseAIClient):
    provider_name = "openai_codex"

    SYSTEM_PROMPT = (
        "You are a music recommendation service."
        " STRICTLY return only a single valid JSON object as the entire response — nothing else."
        " Do NOT include any explanation, commentary, markdown, code fences, or analysis."
        " Do NOT output any text before or after the JSON."
        ' If you cannot produce valid JSON exactly as requested, output exactly this JSON: {"error":"unable_to_comply"}'
    )

    PLAY_MODE_SYSTEM_PROMPT = (
        "You are a music recommendation service."
        " STRICTLY return only a single valid JSON object with exactly two top-level keys: \"autoplay_next\" and \"alternatives\" — nothing else."
        " The \"autoplay_next\" value must be an object with keys: \"title\", \"artist\", \"reason\"."
        " The \"alternatives\" value must be an array of objects, each with keys: \"title\", \"artist\", \"reason\"."
        " Do NOT include any explanation, commentary, markdown, code fences, or analysis."
        " Do NOT output any text before or after the JSON."
        ' If you cannot produce valid JSON exactly as requested, output exactly this JSON: {"error":"unable_to_comply"}'
    )

    def __init__(
        self,
        proxy_url: str | None,
        proxy_token: str | None,
        proxy_label: str | None = None,
        model: str = "gpt-5.1-codex-mini",
        target_url: str = "https://chatgpt.com/backend-api/codex/responses",
        health_cache_ttl: int = 120,
        request_timeout: int = 45,
    ):
        self.proxy_url = (proxy_url or "").rstrip("/")
        self.proxy_token = proxy_token or ""
        self.proxy_label = (proxy_label or "").strip() or "hal"
        self.model = model or "gpt-5.1-codex-mini"
        self.target_url = target_url
        self.health_cache_ttl = health_cache_ttl
        self.request_timeout = request_timeout

        self._last_health_check = 0.0
        self._last_health_status = False
        self._health_lock = asyncio.Lock()

    def _proxy_headers(self) -> dict:
        return {
            "Content-Type": "application/json",
            "Authorization": f"Bearer {self.proxy_token}",
        }

    async def health_check(self) -> bool:
        if not self.proxy_url or not self.proxy_token:
            return False

        now = time.monotonic()
        if now - self._last_health_check < self.health_cache_ttl:
            return self._last_health_status

        async with self._health_lock:
            if now - self._last_health_check < self.health_cache_ttl:
                return self._last_health_status

            try:
                # Cheapest sanity check: ask for a short deterministic response.
                resp = await self._responses_call(
                    instructions="Reply with exactly OK.",
                    user_prompt="Reply with exactly OK.",
                    max_output_tokens=8,
                )
                text = (self._extract_text(resp) or "").strip()
                ok = text == "OK"
                self._last_health_status = ok
                self._last_health_check = now
                if not ok:
                    log.warning_cat(Category.API, "Codex proxy health check failed", text=text[:50])
                return ok
            except Exception as e:
                self._last_health_status = False
                self._last_health_check = now
                log.warning_cat(Category.API, "Codex proxy health check error", error=str(e))
                return False

    @staticmethod
    def _truncate(text: str, limit: int = 500) -> str:
        if not text:
            return ""
        return text if len(text) <= limit else f"{text[:limit]}..."

    @staticmethod
    def _extract_text(data: dict) -> Optional[str]:
        """Extract output text from common proxy response shapes."""
        if not isinstance(data, dict):
            return None

        for key in ("suggestion", "text", "response", "content", "message", "result", "output_text", "completion"):
            value = data.get(key)
            if isinstance(value, str) and value.strip():
                return value

        # Typical Responses shape:
        # {"output": [{"content": [{"type":"output_text","text":"..."}]}]}
        out = data.get("output")
        if isinstance(out, list):
            chunks: list[str] = []
            for item in out:
                if not isinstance(item, dict):
                    continue
                content = item.get("content")
                if not isinstance(content, list):
                    continue
                for c in content:
                    if not isinstance(c, dict):
                        continue
                    if c.get("type") in ("output_text", "text") and isinstance(c.get("text"), str):
                        chunks.append(c["text"])
            if chunks:
                return "".join(chunks)

        if isinstance(data.get("output"), str):
            return data.get("output")

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

    async def _responses_call(
        self,
        instructions: str,
        user_prompt: str,
        max_output_tokens: int = 1200,
    ) -> Optional[dict]:
        if not self.proxy_url or not self.proxy_token:
            return None

        proxy_payload = {
            "label": self.proxy_label,
            "model": self.model,
            "prompt": f"{instructions.strip()}\n\n{user_prompt.strip()}",
        }

        try:
            async with aiohttp.ClientSession() as session:
                async with session.post(
                    self.proxy_url,
                    json=proxy_payload,
                    headers=self._proxy_headers(),
                    timeout=aiohttp.ClientTimeout(total=self.request_timeout),
                ) as resp:
                    if resp.status != 200:
                        body = await resp.text()
                        log.warning_cat(Category.API, "Codex proxy call failed", status=resp.status, body=self._truncate(body))
                        return None
                    body = await resp.text()
                    try:
                        return json.loads(body)
                    except Exception:
                        return {"text": body}
        except Exception as e:
            log.warning_cat(Category.API, "Codex proxy call error", error=str(e))
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

        data = await self._responses_call(self.SYSTEM_PROMPT, prompt, max_output_tokens=1500)
        if not data:
            return []

        if isinstance(data, dict) and isinstance(data.get("suggestions"), list):
            parsed = data
        else:
            content = self._extract_text(data) or ""
            parsed = self._clean_to_json(content)
        if parsed is None:
            log.warning_cat(Category.API, "Codex suggestion parse failed", text=self._truncate(self._extract_text(data) or ""))
            return []

        return self._normalize_suggestions(parsed, n_candidates)

    async def suggest_for_user(
        self,
        liked_tracks: list[dict],
        disliked_tracks: list[dict],
        group_disliked_tracks: list[dict],
        exclude_list: list[dict],
        n_candidates: int = 20,
    ) -> list[AISuggestion]:
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

        data = await self._responses_call(self.SYSTEM_PROMPT, prompt, max_output_tokens=1500)
        if not data:
            return []

        if isinstance(data, dict) and isinstance(data.get("suggestions"), list):
            parsed = data
        else:
            content = self._extract_text(data) or ""
            parsed = self._clean_to_json(content)
        if parsed is None:
            log.warning_cat(Category.API, "Codex suggestion parse failed", text=self._truncate(self._extract_text(data) or ""))
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

        data = await self._responses_call(self.PLAY_MODE_SYSTEM_PROMPT, prompt, max_output_tokens=1800)
        if not data:
            return None

        if isinstance(data, dict) and (
            isinstance(data.get("autoplay_next"), dict) or isinstance(data.get("alternatives"), list)
        ):
            parsed = data
        else:
            content = self._extract_text(data) or ""
            parsed = self._clean_to_json(content)
        if not isinstance(parsed, dict):
            log.warning_cat(Category.API, "Codex playmode parse failed", text=self._truncate(self._extract_text(data) or ""))
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
