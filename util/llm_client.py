import os
from typing import Optional, Tuple
from langchain_openai import ChatOpenAI
from openai import OpenAI


DEFAULT_BASE_URL = "https://api.openai.com/v1"
DEFAULT_API_KEY = "dream-flow"
DEFAULT_MODEL = "gpt-4.1"
DEFAULT_MAX_TOKENS = 32768


def get_llm(model: str | None = None,
            temperature: float = 0.0,
            max_tokens: int | None = None,
            api_key: str | None = None,
            base_url: str | None = None) -> ChatOpenAI:
    """Create a centralized ChatOpenAI client targeting an OpenAI-compatible endpoint.

    Environment overrides (used if arguments are not provided):
      - LLM_API_BASE: endpoint base URL
      - LLM_API_KEY: API key
      - LLM_MODEL: default model name
    """

    resolved_base_url = base_url or os.getenv("LLM_API_BASE", DEFAULT_BASE_URL)
    resolved_api_key = api_key or os.getenv("LLM_API_KEY", DEFAULT_API_KEY)
    resolved_model = model or os.getenv("LLM_MODEL", DEFAULT_MODEL)
    resolved_max_tokens = max_tokens or os.getenv("LLM_MAX_TOKENS", DEFAULT_MAX_TOKENS)

    kwargs = {}
    if max_tokens is not None:
        kwargs["max_tokens"] = max_tokens

    return ChatOpenAI(
        model=resolved_model,
        openai_api_key=resolved_api_key,
        openai_api_base=resolved_base_url,
        temperature=temperature,
        max_tokens=resolved_max_tokens,
        **kwargs,
    )


def resolve_defaults(model: Optional[str] = None,
                     api_key: Optional[str] = None,
                     base_url: Optional[str] = None) -> Tuple[str, str, str]:
    resolved_base_url = base_url or os.getenv("LLM_API_BASE", DEFAULT_BASE_URL)
    resolved_api_key = api_key or os.getenv("LLM_API_KEY", DEFAULT_API_KEY)
    resolved_model = model or os.getenv("LLM_MODEL", DEFAULT_MODEL)
    return resolved_model, resolved_api_key, resolved_base_url


def get_openai_client(api_key: Optional[str] = None,
                      base_url: Optional[str] = None) -> OpenAI:
    _, resolved_api_key, resolved_base_url = resolve_defaults(None, api_key, base_url)
    return OpenAI(api_key=resolved_api_key, base_url=resolved_base_url)



