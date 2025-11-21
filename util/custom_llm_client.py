import requests
from typing import Any, Dict, List, Optional, Union
 
from langchain_core.language_models import BaseChatModel
from langchain_core.messages import BaseMessage, HumanMessage, SystemMessage, AIMessage
from langchain_core.outputs import ChatResult, ChatGeneration
 
 
class PoscoLLMClient(BaseChatModel):
    api_key: str
    model: str
    base_url: str
    temperature: float = 0.1
    max_tokens: Optional[int] = None
    timeout: int = 500
    verify_ssl: bool = False
 
    def __init__(
        self,
        api_key: str,
        model: str,
        base_url: str,
        temperature: float = 0.1,
        max_tokens: Optional[int] = None,
        timeout: int = 500,
        verify_ssl: bool = False,
    ):
        super().__init__(
            api_key=api_key.strip(),
            model=model,
            base_url=base_url.rstrip("/"),
            temperature=temperature,
            max_tokens=max_tokens,
            timeout=timeout,
            verify_ssl=verify_ssl,
        )
 
    def _llm_type(self) -> str:
        return "posco-chat-model"
 
    def _convert_messages(self, messages: List[BaseMessage]) -> List[Dict[str, str]]:
        out = []
        for m in messages:
            if isinstance(m, SystemMessage):
                role = "system" if self.model.startswith("gpt-5") else "developer"
            elif isinstance(m, HumanMessage):
                role = "user"
            elif isinstance(m, AIMessage):
                role = "assistant"
            else:
                role = "user"
            out.append({"role": role, "content": m.content})
        return out
 
    def _generate(
        self,
        messages: List[BaseMessage],
        stop: Optional[Union[str, List[str]]] = None
    ) -> ChatResult:
        headers = {
            "accept": "*/*",
            "Authorization": f"Bearer {self.api_key}",
            "Content-Type": "application/json",
        }
 
        payload: Dict[str, Any] = {
            "model": self.model,
            "messages": self._convert_messages(messages),
        }
 
        # gpt-5 계열과 일반 모델 분기
        if self.model.startswith("gpt-5"):
            # gpt-5는 temperature 없이 진행
            if self.max_tokens is not None:
                payload["max_completion_tokens"] = self.max_tokens
        else:
            payload["temperature"] = self.temperature
            if self.max_tokens is not None:
                payload["max_tokens"] = self.max_tokens
 
        if stop:
            payload["stop"] = stop
 
        resp = requests.post(
            self.base_url,
            headers=headers,
            json=payload,
            timeout=self.timeout,
            verify=self.verify_ssl,
        )
 
        if not resp.ok:
            try:
                error_data = resp.json()
            except ValueError:
                error_data = resp.text
            print(f"[ERROR] API 호출 실패: status={resp.status_code}, body={error_data}")
            resp.raise_for_status()
 
        content_type = resp.headers.get("Content-Type", "").lower()
        if "application/json" in content_type:
            data = resp.json()
            content = (
                data.get("choices", [{}])[0]
                    .get("message", {})
                    .get("content", "")
                    .strip()
            )
        else:
            content = resp.text.strip()
 
        ai_msg = AIMessage(content=content)
        return ChatResult(generations=[ChatGeneration(message=ai_msg)])
 
    def invoke(
        self,
        prompt_value: Any,
        config: Optional[Dict[str, Any]] = None
    ) -> str:
        text = getattr(prompt_value, "to_string", lambda: str(prompt_value))()
 
        if config:
            self.temperature = config.get("temperature", self.temperature)
            self.max_tokens = config.get("max_tokens", self.max_tokens)
 
        messages: List[BaseMessage] = [
            SystemMessage(content="당신은 PostgreSQL에서 Oracle PL/SQL로 코드를 변환하는 전문가입니다. 사용자가 PostgreSQL 코드를 제공하면, Oracle PL/SQL 문법에 맞게 정확하게 변환해주세요."),
            HumanMessage(content=text)
        ]
 
        result = self._generate(messages, stop=config.get("stop") if config else None)
        return result.generations[0].message.content
 
    def __call__(self, prompt_value: Any, **config: Any) -> str:
        return self.invoke(prompt_value, config)