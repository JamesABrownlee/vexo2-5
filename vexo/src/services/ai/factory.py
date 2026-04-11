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
from src.services.ai.openai_client import OpenAIClient
from src.services.ai.open_ai_codex import OpenAICodexClient

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


def _make_gemma() -> LlamaCppClient:
    client = LlamaCppClient(
        base_url=getattr(config, "GEMMA_BASE_URL", None) or "https://GEMMA.systemsfinance.co.uk",
        model=getattr(config, "GEMMA_MODEL", None) or "gemma-4-e4b-it-Q4_K_M.gguf",
        bearer_token=getattr(config, "GEMMA_TOKEN", None),
    )
    client.provider_name = "gemma"
    return client


def _make_openai() -> OpenAIClient:
    return OpenAIClient(
        api_key=getattr(config, "OPENAI_API_KEY", None),
        model=getattr(config, "OPENAI_MODEL", None) or "gpt-4o-mini",
        base_url=getattr(config, "OPENAI_BASE_URL", None) or "https://api.openai.com",
    )


def _make_openai_codex() -> OpenAICodexClient:
    return OpenAICodexClient(
        proxy_url=getattr(config, "CODEX_PROXY_URL", None),
        proxy_token=getattr(config, "CODEX_PROXY_TOKEN", None),
        proxy_label=getattr(config, "CODEX_PROXY_LABEL", None),
        model=getattr(config, "CODEX_MODEL", None) or "gpt-5.1-codex-mini",
        target_url=getattr(config, "CODEX_TARGET_URL", None) or "https://chatgpt.com/backend-api/codex/responses",
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
        self._gemma = _make_gemma()
        self._openai = _make_openai()
        self._openai_codex = _make_openai_codex()

    async def get_for_config(self) -> Optional[BaseAIClient]:
        # Respect explicit provider if set
        preferred = (getattr(config, "LOCAL_AI_PROVIDER", None) or "ollama").lower()

        providers = {
            "ollama": self._ollama,
            "llamacpp": self._llamacpp,
            "gemma": self._gemma,
            "openai": self._openai,
            "openai_codex": self._openai_codex,
        }

        # Selection: preferred first, then fall back to any healthy provider.
        order = [preferred] + [k for k in providers.keys() if k != preferred]

        try:
            for key in order:
                client = providers.get(key)
                if not client:
                    continue
                if await client.health_check():
                    if key != preferred:
                        log.info_cat(Category.API, "ai_provider_fallback", from_provider=preferred, to_provider=key)
                    return client
        except Exception as e:
            log.warning_cat(Category.API, "ai_provider_selection_error", error=str(e))

        return None

        return None

    async def status(self) -> Dict:
        """Return status summary for both providers and selected provider per rules."""
        ollama_ok = await self._ollama.health_check()
        llamacpp_ok = await self._llamacpp.health_check()
        gemma_ok = await self._gemma.health_check()
        openai_ok = await self._openai.health_check()
        openai_codex_ok = await self._openai_codex.health_check()

        preferred = (getattr(config, "LOCAL_AI_PROVIDER", None) or "ollama").lower()
        selected = None
        message = None

        # Selection logic
        if getattr(config, "LOCAL_AI_ENABLED", False):
            available = {
                "ollama": bool(ollama_ok),
                "llamacpp": bool(llamacpp_ok),
                "gemma": bool(gemma_ok),
                "openai": bool(openai_ok),
                "openai_codex": bool(openai_codex_ok),
            }

            if any(available.values()):
                if preferred in available and available[preferred]:
                    selected = preferred
                else:
                    # Auto-select first available in a sensible order
                    for key in ("ollama", "llamacpp", "gemma", "openai", "openai_codex"):
                        if available.get(key):
                            selected = key
                            break

                if preferred and selected and preferred != selected:
                    message = f"{selected} auto-selected because preferred provider is unavailable."
            else:
                selected = None
                message = "No AI backends responded."

        return {
            "ai_enabled": bool(getattr(config, "LOCAL_AI_ENABLED", False)),
            "ai_available": bool(ollama_ok or llamacpp_ok or gemma_ok or openai_ok or openai_codex_ok),
            "selected_provider": selected,
            "providers": {
                "ollama": {"available": bool(ollama_ok), "label": "Ollama"},
                "llamacpp": {"available": bool(llamacpp_ok), "label": "llama.cpp"},
                "gemma": {"available": bool(gemma_ok), "label": "Gemma"},
                "openai": {"available": bool(openai_ok), "label": "OpenAI"},
                "openai_codex": {"available": bool(openai_codex_ok), "label": "OpenAI Codex (proxy)"},
            },
            "message": message,
        }
