import os
from typing import Optional, Tuple, Any, Callable
from langchain_openai import ChatOpenAI
from openai import OpenAI
from util.custom_llm_client import PoscoLLMClass

# 상수 정의
DEFAULT_BASE_URL = "https://api.openai.com/v1"
DEFAULT_API_KEY = ""
DEFAULT_MODEL = "gpt-4.1"
DEFAULT_MAX_TOKENS = 32768

# 커스텀 LLM 클래스 딕셔너리 (단순: 포스코만 지원)
custom_llm_class: dict[str, Callable[..., Any]] = {
    "posco": PoscoLLMClass,
}

def get_llm(model: str | None = None,
            temperature: float = 0.1,
            max_tokens: int | None = None,
            api_key: str | None = None,
            base_url: str | None = None,
            is_custom_llm: bool = False,
            custom_class_name: str | None = None
            ) -> Any:

    """LLM 클라이언트 생성 """
    base_url = base_url or os.getenv("LLM_API_BASE", DEFAULT_BASE_URL)
    api_key = api_key or os.getenv("LLM_API_KEY", DEFAULT_API_KEY)
    model = model or os.getenv("LLM_MODEL", DEFAULT_MODEL)
    max_tokens = max_tokens or int(os.getenv("LLM_MAX_TOKENS", DEFAULT_MAX_TOKENS))
    is_custom_llm = is_custom_llm or (os.getenv("IS_CUSTOM_LLM", "").strip().lower() == "true")
    custom_class_name = (custom_class_name or "posco").strip().lower()

    # 커스텀 LLM 클래스 사용 여부 확인
    if is_custom_llm:
        cls = custom_llm_class.get(custom_class_name)
        if cls is None:
            raise ValueError(f"지원하지 않는 커스텀 LLM 클래스: {custom_class_name}")
        return cls(
            model=model,
            temperature=temperature,
            max_tokens=max_tokens,
            api_key=api_key,
            base_url=base_url,
        )

    return ChatOpenAI(
        model=model,
        openai_api_key=api_key,
        openai_api_base=base_url,
        max_tokens=max_tokens,
        temperature=temperature
    )


def get_openai_client(api_key: Optional[str] = None,
                      base_url: Optional[str] = None) -> OpenAI:
    """OpenAI 클라이언트 생성"""
    return OpenAI(
        api_key=api_key or os.getenv("LLM_API_KEY", DEFAULT_API_KEY),
        base_url=base_url or os.getenv("LLM_API_BASE", DEFAULT_BASE_URL)
    )



