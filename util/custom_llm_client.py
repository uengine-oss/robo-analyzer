# custom_llm_client.py : 포스코 llm class

from typing import Any, Optional


class PoscoLLMClass:
    """포스코 전용 LLM 래퍼 (간단 스텁).

    - invoke(input) 메서드만 제공하며 현재는 빈 문자열을 반환합니다.
    - 실제 HTTP 호출 로직은 추후 구현 예정입니다.
    """

    def __init__(
        self,
        model: str,
        temperature: float = 0.1,
        max_tokens: Optional[int] = None,
        api_key: Optional[str] = None,
        base_url: Optional[str] = None,
    ) -> None:
        self.model = model
        self.temperature = temperature
        self.max_tokens = max_tokens
        self.api_key = api_key
        self.base_url = base_url

    def invoke(self, input: Any) -> str:
        """ """
        return