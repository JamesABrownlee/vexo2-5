"""Language policy helpers for music recommendation prompts.

The bot should allow one-off non-English requests while keeping autoplay and
discovery mostly suitable for an English-speaking room.
"""
from __future__ import annotations

import re


FOREIGN_LANGUAGE_NAMES = {
    "arabic",
    "bengali",
    "dutch",
    "farsi",
    "french",
    "german",
    "greek",
    "hindi",
    "italian",
    "japanese",
    "korean",
    "kurdish",
    "mandarin",
    "persian",
    "polish",
    "portuguese",
    "punjabi",
    "russian",
    "spanish",
    "turkish",
    "urdu",
}

EXPLICIT_LANGUAGE_SESSION_TERMS = {
    "all",
    "continue",
    "keep",
    "more",
    "night",
    "only",
    "playlist",
    "queue",
    "radio",
    "session",
    "set",
}

NON_ENGLISH_ONE_SHOT = "non_english_one_shot"
EXPLICIT_LANGUAGE_SESSION = "explicit_language_session"
ENGLISH_DEFAULT = "english_default"


def _normalize(value: object) -> str:
    return re.sub(r"\s+", " ", str(value or "").lower()).strip()


def _find_language(text: str) -> str | None:
    for language in sorted(FOREIGN_LANGUAGE_NAMES):
        if re.search(rf"\b{re.escape(language)}\b", text):
            return language
    return None


def _looks_non_english_text(text: str) -> bool:
    """Best-effort signal for titles/artists with non-ASCII text.

    This intentionally does not try to classify every song. It is just a nudge
    for cases where the request/result clearly carries non-English characters.
    The AI prompt still handles ambiguous cases.
    """
    compact = "".join(ch for ch in text if ch.isalpha())
    if not compact:
        return False
    non_ascii = sum(1 for ch in compact if ord(ch) > 127)
    return non_ascii >= 2 or (non_ascii >= 1 and non_ascii / max(1, len(compact)) >= 0.08)


def infer_language_policy(*, query: str | None = None, title: str | None = None, artist: str | None = None) -> dict[str, str | None]:
    text = _normalize(" ".join([query or "", title or "", artist or ""]))
    language = _find_language(text)

    if language:
        has_session_term = any(re.search(rf"\b{re.escape(term)}\b", text) for term in EXPLICIT_LANGUAGE_SESSION_TERMS)
        policy = EXPLICIT_LANGUAGE_SESSION if has_session_term else NON_ENGLISH_ONE_SHOT
        return {"policy": policy, "language_hint": language}

    if _looks_non_english_text(_normalize(" ".join([title or "", artist or ""]))):
        return {"policy": NON_ENGLISH_ONE_SHOT, "language_hint": None}

    return {"policy": ENGLISH_DEFAULT, "language_hint": None}


def language_policy_prompt(seed_track: dict | None = None) -> str:
    seed_track = seed_track or {}
    policy = seed_track.get("language_policy") or ENGLISH_DEFAULT
    language_hint = seed_track.get("language_hint")
    language_text = f" {language_hint}" if language_hint else " non-English"

    if policy == NON_ENGLISH_ONE_SHOT:
        return (
            "Language policy: The listening group is mostly English-speaking. "
            f"Treat this as a one-time{language_text} request. "
            "Match the seed track's mood, tempo, genre, and era, but choose an English-language autoplay_next. "
            "Alternatives should be mostly English-language; include at most one non-English alternative only if it is exceptionally relevant. "
            "Do not continue a run of non-English tracks unless the user explicitly asked for that."
        )

    if policy == EXPLICIT_LANGUAGE_SESSION:
        return (
            "Language policy: The user explicitly asked for a language-specific session. "
            "It is okay to recommend tracks in that language, while still avoiding duplicates and keeping the room-friendly mood."
        )

    return (
        "Language policy: The listening group is mostly English-speaking. "
        "Prefer English-language tracks about 95% of the time. "
        "If the seed appears to be non-English but no language-specific session was requested, treat it as a one-time non-English request: "
        "match mood, tempo, genre, and era, but return to English-language autoplay. "
        "Do not let a single non-English request change the room's default language."
    )


def is_likely_non_english_suggestion(suggestion: object, language_hint: str | None = None) -> bool:
    title = _normalize(getattr(suggestion, "title", ""))
    artist = _normalize(getattr(suggestion, "artist", ""))
    reason = _normalize(getattr(suggestion, "reason", ""))
    text = " ".join([title, artist, reason])

    if language_hint and re.search(rf"\b{re.escape(language_hint.lower())}\b", text):
        return True

    if _find_language(text):
        return True

    return _looks_non_english_text(" ".join([title, artist]))
