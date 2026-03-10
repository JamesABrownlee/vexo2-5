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
import re
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

    @staticmethod
    def _truncate(text: str, limit: int = 500) -> str:
        if not text:
            return ""
        if len(text) <= limit:
            return text
        return f"{text[:limit]}..."

    @staticmethod
    def _strip_code_fence(text: str) -> str:
        if "```" not in text:
            return text
        parts = text.split("```")
        if len(parts) >= 3:
            return parts[1].strip()
        return text

    @staticmethod
    def _strip_json_label(text: str) -> str:
        trimmed = text.lstrip()
        if trimmed.startswith("json"):
            lines = trimmed.splitlines()
            if len(lines) > 1:
                return "\n".join(lines[1:]).strip()
        return text

    @staticmethod
    def _payload_for_completions(payload: dict) -> dict:
        if "prompt" in payload:
            payload.setdefault("stream", False)
            return payload
        if "messages" in payload:
            prompt = "\n".join(
                [m.get("content", "") for m in payload.get("messages", []) if isinstance(m, dict)]
            )
            updated = dict(payload)
            updated.pop("messages", None)
            updated["prompt"] = prompt
            updated.setdefault("stream", False)
            return updated
        payload.setdefault("stream", False)
        return payload

    @staticmethod
    def _decode_response(text: str) -> Optional[dict]:
        if not text:
            return None
        try:
            return json.loads(text)
        except Exception:
            if "data:" in text:
                lines = [line.strip() for line in text.splitlines() if line.strip().startswith("data:")]
                for line in reversed(lines):
                    payload = line.replace("data:", "", 1).strip()
                    if not payload or payload == "[DONE]":
                        continue
                    try:
                        return json.loads(payload)
                    except Exception:
                        continue
            return {"_raw_text": text}

    @staticmethod
    def _extract_text(data: dict) -> Optional[str]:
        if not data:
            return None
        if "choices" in data and data["choices"]:
            choice = data["choices"][0]
            if isinstance(choice, dict):
                message = choice.get("message")
                if isinstance(message, dict) and isinstance(message.get("content"), str):
                    return message.get("content")
                delta = choice.get("delta")
                if isinstance(delta, dict) and isinstance(delta.get("content"), str):
                    return delta.get("content")
                if isinstance(choice.get("text"), str):
                    return choice.get("text")
        if isinstance(data.get("text"), str):
            return data.get("text")
        if isinstance(data.get("content"), str):
            return data.get("content")
        if isinstance(data.get("response"), str):
            return data.get("response")
        if isinstance(data.get("_raw_text"), str):
            return data.get("_raw_text")
        return None

    @staticmethod
    def _normalize_suggestions(parsed: object, n_candidates: int) -> list[AISuggestion]:
        items: list[dict] = []
        if isinstance(parsed, list):
            items = [i for i in parsed if isinstance(i, dict)]
        elif isinstance(parsed, dict):
            if isinstance(parsed.get("suggestions"), list):
                items = [i for i in parsed.get("suggestions", []) if isinstance(i, dict)]
            elif isinstance(parsed.get("alternatives"), list):
                items = [i for i in parsed.get("alternatives", []) if isinstance(i, dict)]

        suggestions: list[AISuggestion] = []
        for item in items[:n_candidates]:
            title = item.get("title") if isinstance(item, dict) else None
            artist = item.get("artist") if isinstance(item, dict) else None
            if title and artist:
                suggestions.append(AISuggestion(title=title, artist=artist, reason=item.get("reason", "AI suggested")))
        return suggestions

    @staticmethod
    def _extract_pairs_from_text(text: str, n_candidates: int) -> list[AISuggestion]:
        if not text:
            return []
        titles = re.findall(r'["\']title["\']\s*:\s*["\']([^"\']+)["\']', text)
        artists = re.findall(r'["\']artist["\']\s*:\s*["\']([^"\']+)["\']', text)
        suggestions: list[AISuggestion] = []
        for title, artist in zip(titles, artists):
            suggestions.append(AISuggestion(title=title, artist=artist, reason="AI suggested"))
            if len(suggestions) >= n_candidates:
                break
        return suggestions

    @staticmethod
    def _extract_autoplay_from_text(text: str) -> Optional[dict]:
        if not text:
            return None
        match = re.search(r'["\']autoplay_next["\']\s*:\s*["\']([^"\']+)["\']', text)
        if match:
            return LlamaCppClient._coerce_autoplay_next(match.group(1))
        return None

    @staticmethod
    def _coerce_autoplay_next(value: object) -> Optional[dict]:
        if isinstance(value, dict):
            return value
        if isinstance(value, str):
            if " - " in value:
                artist, title = value.split(" - ", 1)
                return {"title": title.strip(), "artist": artist.strip()}
            if " by " in value:
                title, artist = value.split(" by ", 1)
                return {"title": title.strip(), "artist": artist.strip()}
        return None

    async def _post_openai(self, payload: dict) -> Optional[dict]:
        """Helper to call OpenAI-compatible endpoints on the llama.cpp server."""
        try:
            url = f"{self.base_url}/v1/chat/completions"
            headers = {"Content-Type": "application/json"}
            if self.bearer_token:
                headers["Authorization"] = f"Bearer {self.bearer_token}"

            async with aiohttp.ClientSession() as session:
                async with session.post(url, json=payload, headers=headers, timeout=aiohttp.ClientTimeout(total=self.request_timeout)) as resp:
                    text = await resp.text()
                    if resp.status != 200:
                        log.warning_cat(
                            Category.API,
                            "LlamaCPP generation failed",
                            status=resp.status,
                            url=url,
                            body=self._truncate(text),
                        )
                        # Try /v1/completions as a fallback
                        alt = f"{self.base_url}/v1/completions"
                        alt_payload = self._payload_for_completions(payload)
                        async with session.post(alt, json=alt_payload, headers=headers, timeout=aiohttp.ClientTimeout(total=self.request_timeout)) as resp2:
                            alt_text = await resp2.text()
                            if resp2.status != 200:
                                log.warning_cat(
                                    Category.API,
                                    "LlamaCPP generation failed",
                                    status=resp2.status,
                                    url=alt,
                                    body=self._truncate(alt_text),
                                )
                                return None
                            return self._decode_response(alt_text)
                    return self._decode_response(text)
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
            "stream": False,
        }

        data = await self._post_openai(payload)
        if not data:
            return []

        # Attempt to extract text
        text = self._extract_text(data)

        if not text:
            choice_preview = None
            if isinstance(data, dict) and data.get("choices"):
                try:
                    choice_preview = self._truncate(json.dumps(data.get("choices")[0], default=str))
                except Exception:
                    choice_preview = "<unserializable>"
            log.warning_cat(
                Category.API,
                "LlamaCPP response missing text",
                keys=list(data.keys()) if isinstance(data, dict) else None,
                choice=choice_preview,
            )
            return []

        text = self._strip_json_label(self._strip_code_fence(text))

        # Try to parse JSON
        try:
            parsed = json.loads(text)
        except Exception:
            # Try to find JSON substring
            try:
                start = text.find('{')
                end = text.rfind('}')
                if start != -1 and end != -1 and end > start:
                    parsed = json.loads(text[start:end + 1])
                else:
                    suggestions = self._extract_pairs_from_text(text, n_candidates)
                    if suggestions:
                        return suggestions
                    log.warning_cat(Category.API, "LlamaCPP suggestion parse failed", text=self._truncate(text))
                    return []
            except Exception:
                suggestions = self._extract_pairs_from_text(text, n_candidates)
                if suggestions:
                    return suggestions
                log.warning_cat(Category.API, "LlamaCPP suggestion parse failed", text=self._truncate(text))
                return []

        return self._normalize_suggestions(parsed, n_candidates)

    async def suggest_for_user(self, liked_tracks: list[dict], disliked_tracks: list[dict], group_disliked_tracks: list[dict], exclude_list: list[dict], n_candidates: int = 20) -> list[AISuggestion]:
        likes = "\n".join([f"- {t.get('title','')} by {t.get('artist','')}" for t in (liked_tracks or [])[:20]]) or "none provided"
        prompt = f"Based on the user's likes:\n{likes}\nSuggest {n_candidates} songs the user would enjoy. Return JSON with suggestions array of {{title,artist,reason}}."
        payload = {
            "model": self.model or "gpt",
            "messages": [{"role": "user", "content": prompt}],
            "max_tokens": 512,
            "temperature": 0.7,
            "stream": False,
        }
        data = await self._post_openai(payload)
        if not data:
            return []

        text = self._extract_text(data)
        if not text:
            choice_preview = None
            if isinstance(data, dict) and data.get("choices"):
                try:
                    choice_preview = self._truncate(json.dumps(data.get("choices")[0], default=str))
                except Exception:
                    choice_preview = "<unserializable>"
            log.warning_cat(
                Category.API,
                "LlamaCPP response missing text",
                keys=list(data.keys()) if isinstance(data, dict) else None,
                choice=choice_preview,
            )
            return []

        text = self._strip_json_label(self._strip_code_fence(text))

        try:
            parsed = json.loads(text)
        except Exception:
            try:
                start = text.find('{')
                end = text.rfind('}')
                if start != -1 and end != -1 and end > start:
                    parsed = json.loads(text[start:end + 1])
                else:
                    suggestions = self._extract_pairs_from_text(text, n_candidates)
                    if suggestions:
                        return suggestions
                    log.warning_cat(Category.API, "LlamaCPP suggestion parse failed", text=self._truncate(text))
                    return []
            except Exception:
                suggestions = self._extract_pairs_from_text(text, n_candidates)
                if suggestions:
                    return suggestions
                log.warning_cat(Category.API, "LlamaCPP suggestion parse failed", text=self._truncate(text))
                return []

        return self._normalize_suggestions(parsed, n_candidates)

    async def suggest_for_play_mode(self, seed_track: dict, exclude_list: list[dict], n_alternatives: int = 5) -> Optional[AIPlayModeResult]:
        seed_title = seed_track.get("title", "Unknown")
        seed_artist = seed_track.get("artist", "Unknown")
        prompt = f"Based on {seed_title} by {seed_artist}, return JSON with 'autoplay_next' and 'alternatives' array (each item title,artist,reason)."
        payload = {
            "model": self.model or "gpt",
            "messages": [{"role": "user", "content": prompt}],
            "max_tokens": 512,
            "temperature": 0.7,
            "stream": False,
        }
        data = await self._post_openai(payload)
        if not data:
            return None

        text = self._extract_text(data)
        if not text:
            choice_preview = None
            if isinstance(data, dict) and data.get("choices"):
                try:
                    choice_preview = self._truncate(json.dumps(data.get("choices")[0], default=str))
                except Exception:
                    choice_preview = "<unserializable>"
            log.warning_cat(
                Category.API,
                "LlamaCPP response missing text",
                keys=list(data.keys()) if isinstance(data, dict) else None,
                choice=choice_preview,
            )
            return None

        text = self._strip_json_label(self._strip_code_fence(text))

        try:
            parsed = json.loads(text)
        except Exception:
            try:
                start = text.find('{')
                end = text.rfind('}')
                if start != -1 and end != -1 and end > start:
                    parsed = json.loads(text[start:end + 1])
                else:
                    parsed = None
            except Exception:
                parsed = None

        if parsed is None:
            autoplay = self._extract_autoplay_from_text(text)
            alternatives = self._extract_pairs_from_text(text, n_alternatives)
            if not autoplay and alternatives:
                first = alternatives[0]
                autoplay = {"title": first.title, "artist": first.artist, "reason": "AI suggested"}
                alternatives = alternatives[1:]
            if autoplay and alternatives:
                return AIPlayModeResult(
                    autoplay_next=AISuggestion(title=autoplay.get("title"), artist=autoplay.get("artist"), reason=autoplay.get("reason", "AI suggested")),
                    alternatives=alternatives,
                )
            log.warning_cat(Category.API, "LlamaCPP suggestion parse failed", text=self._truncate(text))
            return None

        ap = self._coerce_autoplay_next(parsed.get("autoplay_next")) if isinstance(parsed, dict) else None
        alts = parsed.get("alternatives", []) if isinstance(parsed, dict) else []
        if not ap or not isinstance(ap, dict) or not alts:
            log.warning_cat(Category.API, "LlamaCPP play mode response missing fields")
            return None

        autoplay = AISuggestion(title=ap.get("title"), artist=ap.get("artist"), reason=ap.get("reason", "AI recommended"))
        alternatives = []
        for item in alts[:n_alternatives]:
            if isinstance(item, dict) and "title" in item and "artist" in item:
                alternatives.append(AISuggestion(title=item["title"], artist=item["artist"], reason=item.get("reason", "Alternative")))

        if not alternatives:
            return None

        return AIPlayModeResult(autoplay_next=autoplay, alternatives=alternatives)
