"""
Base abstractions for Local AI providers.

Defines the BaseAIClient interface and small helper types used by callers.
"""
from __future__ import annotations
import abc
from dataclasses import dataclass
from typing import List, Optional


@dataclass
class AISuggestion:
    title: str
    artist: str
    reason: str


@dataclass
class AIPlayModeResult:
    autoplay_next: AISuggestion
    alternatives: List[AISuggestion]


class BaseAIClient(abc.ABC):
    """Abstract base for local AI provider clients.

    Concrete implementations must normalize responses to the simple types above.
    """

    provider_name: str

    @abc.abstractmethod
    async def health_check(self) -> bool:
        raise NotImplementedError()

    @abc.abstractmethod
    async def suggest_from_seed(self, seed_track: dict, exclude_list: list[dict], n_candidates: int = 20) -> List[AISuggestion]:
        raise NotImplementedError()

    @abc.abstractmethod
    async def suggest_for_user(self, liked_tracks: list[dict], disliked_tracks: list[dict], group_disliked_tracks: list[dict], exclude_list: list[dict], n_candidates: int = 20) -> List[AISuggestion]:
        raise NotImplementedError()

    @abc.abstractmethod
    async def suggest_for_play_mode(self, seed_track: dict, exclude_list: list[dict], n_alternatives: int = 5) -> Optional[AIPlayModeResult]:
        raise NotImplementedError()
