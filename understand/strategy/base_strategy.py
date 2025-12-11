from abc import ABC, abstractmethod
from typing import AsyncGenerator, Any


class UnderstandStrategy(ABC):
    """Understanding 단계 실행을 위한 전략 인터페이스."""

    @abstractmethod
    async def understand(self, file_names: list, orchestrator: Any, **kwargs) -> AsyncGenerator[bytes, None]:
        """파일 단위 Understanding을 수행합니다."""
        raise NotImplementedError

