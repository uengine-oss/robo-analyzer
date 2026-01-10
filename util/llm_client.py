"""LLM 클라이언트 팩토리

OpenAI 호환 API 또는 커스텀 LLM 클라이언트를 생성합니다.

LLM 캐싱:
    LLM_CACHE_ENABLED=true 환경변수로 SQLite 기반 캐싱 활성화
    동일한 프롬프트 호출 시 캐싱된 결과 반환 (테스트 시 유용)

사용법:
    llm = get_llm(api_key="...")
    response = llm.invoke("안녕하세요")
"""

import logging
import os
from typing import Optional

from langchain_openai import ChatOpenAI
from langchain_community.cache import SQLiteCache
from langchain_core.globals import set_llm_cache

from config.settings import settings
from util.custom_llm_client import CustomLLMClient


# LLM 캐싱 초기화 (한 번만 실행)
_cache_initialized = False


def _init_llm_cache():
    """LLM 캐싱 초기화"""
    global _cache_initialized
    
    if _cache_initialized:
        return
    
    config = settings.llm
    
    if config.cache_enabled:
        cache_path = config.cache_db_path
        # 상대 경로면 프로젝트 루트 기준으로 변환
        if not os.path.isabs(cache_path):
            cache_path = os.path.join(settings.path.base_dir, cache_path)
        
        try:
            set_llm_cache(SQLiteCache(database_path=cache_path))
            logging.info("LLM 캐싱 활성화: %s", cache_path)
        except Exception as e:
            logging.warning("LLM 캐싱 초기화 실패: %s", e)
    else:
        logging.debug("LLM 캐싱 비활성화됨")
    
    _cache_initialized = True


# 추론 모델 (thinking 기반)
REASONING_MODELS = frozenset({
    "gpt-5", "o1", "o1-pro", "o1-mini", "o1-preview", "o3", "o3-mini", "o4-mini"
})


def _is_reasoning_model(model: str) -> bool:
    """추론 모델 여부 확인"""
    model_lower = model.lower() if model else ""
    return any(rm in model_lower for rm in REASONING_MODELS)


def get_llm(
    *,
    api_key: Optional[str] = None,
    base_url: Optional[str] = None,
    model: Optional[str] = None,
    max_tokens: Optional[int] = None,
    is_custom_llm: Optional[bool] = None,
    company_name: Optional[str] = None,
):
    """LLM 클라이언트 생성
    
    Args:
        api_key: API 키 (기본: 환경변수)
        base_url: API 기본 URL (기본: 환경변수)
        model: 모델명 (기본: 환경변수)
        max_tokens: 최대 토큰 (기본: 환경변수)
        is_custom_llm: 커스텀 LLM 사용 여부 (기본: 환경변수)
        company_name: 커스텀 LLM 회사명 (기본: 환경변수)
    
    Returns:
        ChatOpenAI 또는 CustomLLMClient 인스턴스
    """
    # LLM 캐싱 초기화
    _init_llm_cache()
    
    config = settings.llm
    
    # 매개변수 기본값 설정
    base_url = base_url or config.api_base
    api_key = api_key or config.api_key
    model = model or config.model
    max_tokens = max_tokens or config.max_tokens
    is_custom_llm = is_custom_llm if is_custom_llm is not None else config.is_custom
    company_name = company_name if company_name is not None else config.company_name

    if is_custom_llm:
        logging.debug("CustomLLM 사용: model=%s, company=%s", model, company_name)
        return CustomLLMClient(
            api_key=api_key,
            model=model,
            base_url=base_url,
            max_tokens=max_tokens,
            company_name=company_name,
        )

    # OpenAI 호환 클라이언트
    kwargs = {
        "model": model,
        "base_url": base_url,
        "api_key": api_key,
        "max_tokens": max_tokens,
        "temperature": 0.2,
    }

    # 추론 모델 특수 처리
    if _is_reasoning_model(model):
        kwargs["reasoning_effort"] = config.reasoning_effort
        logging.debug("추론 모델 사용: model=%s, effort=%s", model, kwargs["reasoning_effort"])

    return ChatOpenAI(**kwargs)
