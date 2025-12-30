"""LLM 클라이언트 생성 모듈"""

import os
from typing import Any
from langchain_openai import ChatOpenAI
from util.custom_llm_client import CustomLLMClient

# =========================
# 상수 정의
# =========================
DEFAULT_BASE_URL = "https://api.openai.com/v1"
DEFAULT_API_KEY = ""
DEFAULT_MODEL = "gpt-4.1"
DEFAULT_MAX_TOKENS = 32768

# 커스텀 LLM 클래스 딕셔너리
CUSTOM_LLM_CLASSES = {
    "custom": CustomLLMClient,
}

# OpenAI 추론(reasoning) 모델 리스트
REASONING_MODELS: set[str] = {
    "gpt-5",
    "gpt-5.1",
    "gpt-5.1-thinking",
    "gpt-5.1-instant",
    "gpt-5-pro",
    "o1",
    "o1-mini",
    "o3-mini",
    "o4-mini",
}


def _is_reasoning_model(model_name: str) -> bool:
    """추론 모델 여부 판별"""
    if model_name in REASONING_MODELS:
        return True
    reasoning_prefixes = ("gpt-5", "o1", "o3", "o4")
    return any(model_name.startswith(p) for p in reasoning_prefixes)


def get_llm(
    model: str | None = None,
    temperature: float = 0.1,
    max_tokens: int | None = None,
    api_key: str | None = None,
    base_url: str | None = None,
    is_custom_llm: bool = False,
    company_name: str | None = None,
) -> Any:
    """
    LLM 클라이언트 생성
    
    - 일반 모델(gpt-4.1, gpt-4o 등): temperature 사용
    - 추론 모델(gpt-5.*, o3-mini 등): reasoning_effort 사용
    """
    base_url = base_url or os.getenv("LLM_API_BASE", DEFAULT_BASE_URL)
    api_key = api_key or os.getenv("LLM_API_KEY", DEFAULT_API_KEY)
    model = model or os.getenv("LLM_MODEL", DEFAULT_MODEL)
    max_tokens = max_tokens or int(os.getenv("LLM_MAX_TOKENS", DEFAULT_MAX_TOKENS))
    is_custom_llm = is_custom_llm or bool(os.getenv("IS_CUSTOM_LLM", None))
    company_name = company_name or os.getenv("COMPANY_NAME", None)

    # 커스텀 LLM 분기
    if is_custom_llm:
        cls = CUSTOM_LLM_CLASSES.get(company_name or "custom", CustomLLMClient)
        return cls(
            model=model,
            temperature=temperature,
            max_tokens=max_tokens,
            api_key=api_key,
            base_url=base_url,
        )

    # OpenAI LLM (ChatOpenAI)
    kwargs: dict[str, Any] = dict(
        model=model,
        openai_api_key=api_key,
        openai_api_base=base_url,
        max_tokens=max_tokens,
    )

    if _is_reasoning_model(model):
        default_effort = os.getenv("LLM_REASONING_EFFORT", "medium")
        kwargs["reasoning_effort"] = default_effort
    else:
        kwargs["temperature"] = temperature

    return ChatOpenAI(**kwargs)
