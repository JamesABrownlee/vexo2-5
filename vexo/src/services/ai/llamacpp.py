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

    def __init__(self, base_url: str = "http://localhost:8080", model: str = "", bearer_token: Optional[str] = None, health_cache_ttl: int = 45, request_timeout: int = 60):
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
    def _extract_text_details(data: dict) -> tuple[Optional[str], bool, Optional[str], bool, bool]:
        if not data:
            return None, False, None, False, False

        used_reasoning = False
        finish_reason = None
        reasoning_available = False
        content_empty_reasoning = False

        if "choices" in data and data["choices"]:
            choice = data["choices"][0]
            if isinstance(choice, dict):
                finish_reason = choice.get("finish_reason") if isinstance(choice.get("finish_reason"), str) else None
                message = choice.get("message")
                if isinstance(message, dict):
                    content = message.get("content") if isinstance(message.get("content"), str) else None
                    reasoning = message.get("reasoning_content") if isinstance(message.get("reasoning_content"), str) else None
                    if reasoning:
                        reasoning_available = True
                    if (not content or not content.strip()) and reasoning_available:
                        content_empty_reasoning = True
                    if content and content.strip():
                        return content, False, finish_reason, reasoning_available, content_empty_reasoning
                    if isinstance(choice.get("text"), str) and choice.get("text"):
                        return choice.get("text"), False, finish_reason, reasoning_available, content_empty_reasoning
                    if reasoning and reasoning.strip():
                        return reasoning, True, finish_reason, reasoning_available, content_empty_reasoning

        if isinstance(data.get("text"), str) and data.get("text"):
            return data.get("text"), False, finish_reason, reasoning_available, content_empty_reasoning
        if isinstance(data.get("content"), str) and data.get("content"):
            return data.get("content"), False, finish_reason, reasoning_available, content_empty_reasoning
        if isinstance(data.get("response"), str) and data.get("response"):
            return data.get("response"), False, finish_reason, reasoning_available, content_empty_reasoning
        if isinstance(data.get("_raw_text"), str) and data.get("_raw_text"):
            return data.get("_raw_text"), False, finish_reason, reasoning_available, content_empty_reasoning

        return None, used_reasoning, finish_reason, reasoning_available, content_empty_reasoning

    @staticmethod
    def _clean_text(text: str) -> tuple[str, Optional[str]]:
        if not text:
            return "", None
        cleaned = text.strip()
        cleaned = LlamaCppClient._strip_json_label(LlamaCppClient._strip_code_fence(cleaned))
        cleaned = re.sub(r"<think>.*?</think>", "", cleaned, flags=re.DOTALL).strip()
        start = cleaned.find("{")
        end = cleaned.rfind("}")
        if start != -1 and end != -1 and end > start:
            return cleaned, cleaned[start:end + 1]
        return cleaned, None

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
            "messages": [
                {"role": "system", "content": self.SYSTEM_PROMPT},
                {"role": "user", "content": prompt},
            ],
            "max_tokens": 1024,
            "temperature": 0.0,
            "stream": False,
        }

        data = await self._post_openai(payload)
        if not data:
            return []

        # Attempt to extract text
        text, used_reasoning, finish_reason, reasoning_available, content_empty_reasoning = self._extract_text_details(data)

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

        if content_empty_reasoning:
            log.warning_cat(Category.API, "LlamaCPP content empty with reasoning_content present", finish_reason=finish_reason)
        if reasoning_available and not used_reasoning:
            log.warning_cat(Category.API, "LlamaCPP reasoning_content present but content used", finish_reason=finish_reason)
        if used_reasoning:
            log.warning_cat(Category.API, "LlamaCPP used reasoning_content fallback", finish_reason=finish_reason)
        if finish_reason == "length":
            log.warning_cat(Category.API, "LlamaCPP response truncated", finish_reason=finish_reason)

        cleaned_text, json_block = self._clean_text(text)

        # Try to parse JSON
        try:
            parsed = json.loads(json_block or cleaned_text)
        except Exception:
            # Try to find JSON substring
            try:
                if json_block:
                    parsed = json.loads(json_block)
                else:
                    suggestions = self._extract_pairs_from_text(cleaned_text, n_candidates)
                    if suggestions:
                        return suggestions
                    log.warning_cat(Category.API, "LlamaCPP suggestion parse failed", text=self._truncate(cleaned_text))
                    return []
            except Exception:
                suggestions = self._extract_pairs_from_text(cleaned_text, n_candidates)
                if suggestions:
                    return suggestions
                log.warning_cat(Category.API, "LlamaCPP suggestion parse failed", text=self._truncate(cleaned_text))
                return []

        return self._normalize_suggestions(parsed, n_candidates)

    async def suggest_for_user(self, liked_tracks: list[dict], disliked_tracks: list[dict], group_disliked_tracks: list[dict], exclude_list: list[dict], n_candidates: int = 20) -> list[AISuggestion]:
        likes = "\n".join([f"- {t.get('title','')} by {t.get('artist','')}" for t in (liked_tracks or [])[:20]]) or "none provided"
        prompt = f"Based on the user's likes:\n{likes}\nSuggest {n_candidates} songs the user would enjoy. Return JSON with suggestions array of {{title,artist,reason}}."
        payload = {
            "model": self.model or "gpt",
            "messages": [
                {"role": "system", "content": self.SYSTEM_PROMPT},
                {"role": "user", "content": prompt},
            ],
            "max_tokens": 1024,
            "temperature": 0.0,
            "stream": False,
        }
        data = await self._post_openai(payload)
        if not data:
            return []

        text, used_reasoning, finish_reason, reasoning_available, content_empty_reasoning = self._extract_text_details(data)
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

        if content_empty_reasoning:
            log.warning_cat(Category.API, "LlamaCPP content empty with reasoning_content present", finish_reason=finish_reason)
        if reasoning_available and not used_reasoning:
            log.warning_cat(Category.API, "LlamaCPP reasoning_content present but content used", finish_reason=finish_reason)
        if used_reasoning:
            log.warning_cat(Category.API, "LlamaCPP used reasoning_content fallback", finish_reason=finish_reason)
        if finish_reason == "length":
            log.warning_cat(Category.API, "LlamaCPP response truncated", finish_reason=finish_reason)

        cleaned_text, json_block = self._clean_text(text)

        try:
            parsed = json.loads(json_block or cleaned_text)
        except Exception:
            try:
                if json_block:
                    parsed = json.loads(json_block)
                else:
                    suggestions = self._extract_pairs_from_text(cleaned_text, n_candidates)
                    if suggestions:
                        return suggestions
                    log.warning_cat(Category.API, "LlamaCPP suggestion parse failed", text=self._truncate(cleaned_text))
                    return []
            except Exception:
                suggestions = self._extract_pairs_from_text(cleaned_text, n_candidates)
                if suggestions:
                    return suggestions
                log.warning_cat(Category.API, "LlamaCPP suggestion parse failed", text=self._truncate(cleaned_text))
                return []

        return self._normalize_suggestions(parsed, n_candidates)

    async def suggest_for_play_mode(self, seed_track: dict, exclude_list: list[dict], n_alternatives: int = 9) -> Optional[AIPlayModeResult]:
        seed_title = seed_track.get("title", "Unknown")
        seed_artist = seed_track.get("artist", "Unknown")
        prompt = f"Based on {seed_title} by {seed_artist}, suggest up to {n_alternatives} alternatives and return JSON with 'autoplay_next' and 'alternatives' array (each item title,artist,reason)."
        payload = {
            "model": self.model or "gpt",
            "messages": [
                {"role": "system", "content": self.PLAY_MODE_SYSTEM_PROMPT},
                {"role": "user", "content": prompt},
            ],
            "max_tokens": 1536,
            "temperature": 0.0,
            "stream": False,
        }
        data = await self._post_openai(payload)
        if not data:
            return None

        text, used_reasoning, finish_reason, reasoning_available, content_empty_reasoning = self._extract_text_details(data)
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

        if content_empty_reasoning:
            log.warning_cat(Category.API, "LlamaCPP content empty with reasoning_content present", finish_reason=finish_reason)
        if reasoning_available and not used_reasoning:
            log.warning_cat(Category.API, "LlamaCPP reasoning_content present but content used", finish_reason=finish_reason)
        if used_reasoning:
            log.warning_cat(Category.API, "LlamaCPP used reasoning_content fallback", finish_reason=finish_reason)
        if finish_reason == "length":
            log.warning_cat(Category.API, "LlamaCPP response truncated", finish_reason=finish_reason)

        cleaned_text, json_block = self._clean_text(text)

        try:
            parsed = json.loads(json_block or cleaned_text)
        except Exception:
            try:
                if json_block:
                    parsed = json.loads(json_block)
                else:
                    parsed = None
            except Exception:
                parsed = None

        if parsed is None:
            autoplay = self._extract_autoplay_from_text(cleaned_text)
            alternatives = self._extract_pairs_from_text(cleaned_text, n_alternatives)
            if not autoplay and alternatives:
                first = alternatives[0]
                autoplay = {"title": first.title, "artist": first.artist, "reason": "AI suggested"}
                alternatives = alternatives[1:]
            if autoplay and alternatives:
                return AIPlayModeResult(
                    autoplay_next=AISuggestion(title=autoplay.get("title"), artist=autoplay.get("artist"), reason=autoplay.get("reason", "AI suggested")),
                    alternatives=alternatives,
                )
            log.warning_cat(Category.API, "LlamaCPP suggestion parse failed", text=self._truncate(cleaned_text))
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
