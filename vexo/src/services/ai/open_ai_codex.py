"""OpenAI Codex (Responses API) client via internal proxy transport.

This provider is designed for environments where direct outbound OpenAI calls
are not desired, and all traffic must go through an internal proxy endpoint.

Expected proxy API (HTTP POST):
- Authorization: Bearer <PROXY_TOKEN>
- JSON body:
  {
    "label": "<string>",
    "url": "https://chatgpt.com/backend-api/codex/responses",
    "method": "POST",
    "stream": false,
    "json": { ... OpenAI Responses payload ... }
  }

Config via env:
- CODEX_PROXY_URL   (required) e.g. https://your-host/internal/openai-proxy
- CODEX_PROXY_TOKEN (required)
- CODEX_MODEL       (default: gpt-5.1-codex-mini)
- CODEX_TARGET_URL  (default: https://chatgpt.com/backend-api/codex/responses)

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
        # If no label is provided, omit it entirely and let the proxy pick the best available backend.
        # Do not default to any static label.
        self.proxy_label = (proxy_label or "").strip() or None
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
        """Extract output text from an OpenAI Responses-style payload."""
        if not isinstance(data, dict):
            return None

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

        # Fallbacks sometimes used by proxies
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

    async def _responses_call(
        self,
        instructions: str,
        user_prompt: str,
        max_output_tokens: int = 1200,
    ) -> Optional[dict]:
        if not self.proxy_url or not self.proxy_token:
            return None

        # NOTE: This proxy enforces stream=true.
        proxy_payload = {
            "url": self.target_url,
            "method": "POST",
            "stream": True,
            "json": {
                "model": self.model,
                "instructions": instructions,
                "store": False,
                "stream": True,
                "input": [
                    {
                        "role": "user",
                        "content": [{"type": "input_text", "text": user_prompt}],
                    }
                ],
                # NOTE: This Codex endpoint (via proxy) may reject some common OpenAI params
                # like temperature/max_output_tokens. Keep the payload minimal and rely on
                # strong prompting for deterministic JSON.
            },
        }
        if self.proxy_label:
            proxy_payload["label"] = self.proxy_label

        def _mk_text_response(text: str) -> dict:
            # Wrap extracted final text into a minimal Responses-like dict
            return {
                "output": [
                    {
                        "type": "message",
                        "role": "assistant",
                        "content": [{"type": "output_text", "text": text}],
                    }
                ]
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

                    # Parse SSE stream. We collect deltas from response.output_text.delta.
                    buf = ""
                    current_event = None
                    data_lines: list[str] = []

                    async for raw in resp.content:
                        line = raw.decode("utf-8", errors="ignore")
                        for part in line.splitlines():
                            if part.startswith("event:"):
                                current_event = part.split(":", 1)[1].strip()
                            elif part.startswith("data:"):
                                data_lines.append(part.split(":", 1)[1].strip())
                            elif part.strip() == "":
                                # Dispatch event
                                if current_event and data_lines:
                                    data_str = "\n".join(data_lines)
                                    try:
                                        payload = json.loads(data_str)
                                        if current_event == "response.output_text.delta":
                                            delta = payload.get("delta")
                                            if isinstance(delta, str):
                                                buf += delta
                                        elif current_event == "response.output_text.done":
                                            text = payload.get("text")
                                            if isinstance(text, str) and text:
                                                return _mk_text_response(text)
                                        elif current_event == "response.completed":
                                            # If we didn't catch output_text.done for some reason, fall back to collected deltas
                                            if buf:
                                                return _mk_text_response(buf)
                                    except Exception:
                                        pass
                                current_event = None
                                data_lines = []

                    # Stream ended; return whatever we collected
                    if buf:
                        return _mk_text_response(buf)
                    return None
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

        content = self._extract_text(data) or ""
        parsed = self._clean_to_json(content)
        if parsed is None:
            log.warning_cat(Category.API, "Codex suggestion parse failed", text=self._truncate(content))
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

        content = self._extract_text(data) or ""
        parsed = self._clean_to_json(content)
        if parsed is None:
            log.warning_cat(Category.API, "Codex suggestion parse failed", text=self._truncate(content))
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

        content = self._extract_text(data) or ""
        parsed = self._clean_to_json(content)
        if not isinstance(parsed, dict):
            log.warning_cat(Category.API, "Codex playmode parse failed", text=self._truncate(content))
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
