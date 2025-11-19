import os
from typing import Optional, Any
from langchain_openai import ChatOpenAI
from openai import OpenAI
from util.custom_llm_client import PoscoLLMClient

# =========================
# 상수 정의
# =========================
DEFAULT_BASE_URL = "https://api.openai.com/v1"
DEFAULT_API_KEY = ""
DEFAULT_MODEL = "gpt-4.1"
DEFAULT_MAX_TOKENS = 32768

# 커스텀 LLM 클래스 딕셔너리 (단순: 포스코만 지원)
custom_llm_class = {
    "posco": PoscoLLMClient,
}

# OpenAI 추론(reasoning) 모델 리스트
# 필요 시 여기다 사용하는 모델명을 추가하면 됨.
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
    """
    간단한 판별 로직:
    - REASONING_MODELS에 정확히 있으면 True
    - 아니면 False
    """
    # model_name이 'gpt-5.1-mini' 같이 올 수도 있으니, 앞부분 매칭도 함께 고려
    if model_name in REASONING_MODELS:
        return True

    # prefix 기반 추가 방어 로직 (옵션)
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
    - 추론 모델(gpt-5.*, o3-mini, o4-mini 등): temperature 미전달, reasoning_effort 사용
    """

    # 환경변수와 기본값 처리
    base_url = base_url or os.getenv("LLM_API_BASE", DEFAULT_BASE_URL)
    api_key = api_key or os.getenv("LLM_API_KEY", DEFAULT_API_KEY)
    model = model or os.getenv("LLM_MODEL", DEFAULT_MODEL)
    max_tokens = max_tokens or int(os.getenv("LLM_MAX_TOKENS", DEFAULT_MAX_TOKENS))
    # env 값이 세팅되어 있으면 truthy로 취급
    is_custom_llm = is_custom_llm or bool(os.getenv("IS_CUSTOM_LLM", None))
    company_name = company_name or os.getenv("COMPANY_NAME", None)

    # =========================
    # 1) 커스텀 LLM 분기
    # =========================
    if is_custom_llm:
        cls = custom_llm_class.get(company_name)
        if cls is None:
            raise ValueError(f"지원하지 않는 커스텀 LLM 클래스: {company_name}")
        return cls(
            model=model,
            temperature=temperature,   # 커스텀 쪽은 내부 구현에 맡김
            max_tokens=max_tokens,
            api_key=api_key,
            base_url=base_url,
        )

    # =========================
    # 2) OpenAI 기본 LLM (ChatOpenAI)
    # =========================
    # 공통 파라미터
    kwargs: dict[str, Any] = dict(
        model=model,
        openai_api_key=api_key,   # 기존 코드와 호환성 유지
        openai_api_base=base_url,
        max_tokens=max_tokens,
    )

    if _is_reasoning_model(model):
        # 추론 모델:
        # temperature를 보내면 일부 모델에서 에러/무시될 수 있으므로 아예 안 보냄.
        #
        # 기본 reasoning_effort는 medium으로 두고,
        # 필요하면 환경변수 LLM_REASONING_EFFORT 로 조정 가능
        # (예: "minimal", "low", "medium", "high", 일부 gpt-5.1 계열은 "none"도 지원)
        default_effort = os.getenv("LLM_REASONING_EFFORT", "medium")
        kwargs["reasoning_effort"] = default_effort
    else:
        # 일반 모델: 기존처럼 temperature 사용
        kwargs["temperature"] = temperature

    return ChatOpenAI(**kwargs)


def get_openai_client(
    api_key: Optional[str] = None,
    base_url: Optional[str] = None,
) -> OpenAI:
    """
    OpenAI 클라이언트 생성

    - ChatCompletions / Responses API 직접 호출할 때 사용
    - reasoning_effort, reasoning 등은 실제 호출부에서 자유롭게 세팅
    """
    return OpenAI(
        api_key=api_key or os.getenv("LLM_API_KEY", DEFAULT_API_KEY),
        base_url=base_url or os.getenv("LLM_API_BASE", DEFAULT_BASE_URL),
    )
