"""Factory and status helpers for Local AI providers.

Responsible for instantiating provider clients, probing availability and
selecting a provider based on configuration + runtime health.
"""
from typing import Optional, Dict
from src.config import config
from src.utils.logging import get_logger, Category

from src.services.ai.base import BaseAIClient
from src.services.ollama_client import OllamaClient
from src.services.ai.llamacpp import LlamaCppClient

log = get_logger(__name__)


def _make_ollama() -> OllamaClient:
    return OllamaClient(
        base_url=getattr(config, "OLLAMA_BASE_URL", None) or "",
        model=getattr(config, "OLLAMA_MODEL", None) or "",
        bearer_token=getattr(config, "OLLAMA_TOKEN", None),
    )


def _make_llamacpp() -> LlamaCppClient:
    return LlamaCppClient(
        base_url=getattr(config, "LLAMACPP_BASE_URL", None) or "",
        model=getattr(config, "LLAMACPP_MODEL", None) or getattr(config, "LOCAL_AI_MODEL", None) or "",
        bearer_token=getattr(config, "LLAMACPP_TOKEN", None),
    )


class AIProviderStatus:
    def __init__(self, key: str, label: str, available: bool, selected: bool = False, auto_selected: bool = False, message: str | None = None):
        self.key = key
        self.label = label
        self.available = available
        self.selected = selected
        self.auto_selected = auto_selected
        self.message = message


class AIClientFactory:
    """Create and select AI provider clients.

    Selection rules implemented here match the repo requirements:
    - If config specifies a provider and it is healthy, use it
    - If specified provider is unhealthy but the other is healthy, auto-fallback
    - If none healthy, return None
    """

    def __init__(self):
        self._ollama = _make_ollama()
        self._llamacpp = _make_llamacpp()

    async def get_for_config(self) -> Optional[BaseAIClient]:
        # Respect explicit provider if set
        preferred = (getattr(config, "LOCAL_AI_PROVIDER", None) or "ollama").lower()

        try:
            if preferred == "ollama":
                if await self._ollama.health_check():
                    return self._ollama
                if await self._llamacpp.health_check():
                    log.info_cat(Category.API, "ai_provider_fallback", from_provider="ollama", to_provider="llamacpp")
                    return self._llamacpp
            elif preferred == "llamacpp":
                if await self._llamacpp.health_check():
                    return self._llamacpp
                if await self._ollama.health_check():
                    log.info_cat(Category.API, "ai_provider_fallback", from_provider="llamacpp", to_provider="ollama")
                    return self._ollama
        except Exception as e:
            log.warning_cat(Category.API, "ai_provider_selection_error", error=str(e))

        # Last attempt: if any provider is healthy, return it
        try:
            if await self._ollama.health_check():
                return self._ollama
            if await self._llamacpp.health_check():
                return self._llamacpp
        except Exception:
            return None

        return None

    async def status(self) -> Dict:
        """Return status summary for both providers and selected provider per rules."""
        ollama_ok = await self._ollama.health_check()
        llamacpp_ok = await self._llamacpp.health_check()

        preferred = (getattr(config, "LOCAL_AI_PROVIDER", None) or "ollama").lower()
        selected = None
        message = None

        # Selection logic
        if getattr(config, "LOCAL_AI_ENABLED", False):
            if ollama_ok and not llamacpp_ok:
                selected = "ollama"
                if preferred != selected:
                    message = "ollama auto-selected because it's the only available provider."
            elif llamacpp_ok and not ollama_ok:
                selected = "llamacpp"
                if preferred != selected:
                    message = "llama.cpp auto-selected because it's the only available provider."
            elif ollama_ok and llamacpp_ok:
                # both available -> respect user preference
                selected = preferred if preferred in {"ollama", "llamacpp"} else "ollama"
            else:
                selected = None
                message = "No Local AI backends responded."

        return {
            "ai_enabled": bool(getattr(config, "LOCAL_AI_ENABLED", False)),
            "ai_available": bool(ollama_ok or llamacpp_ok),
            "selected_provider": selected,
            "providers": {
                "ollama": {"available": bool(ollama_ok), "label": "Ollama"},
                "llamacpp": {"available": bool(llamacpp_ok), "label": "llama.cpp"},
            },
            "message": message,
        }
