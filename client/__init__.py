"""클라이언트 모듈

외부 API 클라이언트를 제공합니다.
- llm_client: LLM API 클라이언트 (OpenAI, Custom)
- embedding_client: 임베딩 API 클라이언트
"""

from client.llm_client import get_llm
from client.embedding_client import EmbeddingClient

__all__ = ["get_llm", "EmbeddingClient"]

