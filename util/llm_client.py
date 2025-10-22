import os
from typing import Optional, Tuple
from langchain_openai import ChatOpenAI
from openai import OpenAI

# 상수 정의
DEFAULT_BASE_URL = "https://api.openai.com/v1"
DEFAULT_API_KEY = ""
DEFAULT_MODEL = "gpt-4.1"
DEFAULT_MAX_TOKENS = 32768

def get_llm(model: str | None = None,
            temperature: float = 0.1,
            max_tokens: int | None = None,
            api_key: str | None = None,
            base_url: str | None = None) -> ChatOpenAI:
    """ChatOpenAI 클라이언트 생성 (최적화: 중복 제거)"""
    resolved_base_url = base_url or os.getenv("LLM_API_BASE", DEFAULT_BASE_URL)
    resolved_api_key = api_key or os.getenv("LLM_API_KEY", DEFAULT_API_KEY)
    resolved_model = model or os.getenv("LLM_MODEL", DEFAULT_MODEL)
    resolved_max_tokens = max_tokens or int(os.getenv("LLM_MAX_TOKENS", DEFAULT_MAX_TOKENS))

    return ChatOpenAI(
        model=resolved_model,
        openai_api_key=resolved_api_key,
        openai_api_base=resolved_base_url,
        max_tokens=resolved_max_tokens,
        temperature=temperature
    )


def resolve_defaults(model: Optional[str] = None,
                     api_key: Optional[str] = None,
                     base_url: Optional[str] = None) -> Tuple[str, str, str]:
    """환경변수 기본값 해결 (최적화: 단순화)"""
    return (
        model or os.getenv("LLM_MODEL", DEFAULT_MODEL),
        api_key or os.getenv("LLM_API_KEY", DEFAULT_API_KEY),
        base_url or os.getenv("LLM_API_BASE", DEFAULT_BASE_URL)
    )


def get_openai_client(api_key: Optional[str] = None,
                      base_url: Optional[str] = None) -> OpenAI:
    """OpenAI 클라이언트 생성 (최적화: 직접 호출)"""
    return OpenAI(
        api_key=api_key or os.getenv("LLM_API_KEY", DEFAULT_API_KEY),
        base_url=base_url or os.getenv("LLM_API_BASE", DEFAULT_BASE_URL)
    )



