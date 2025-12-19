from abc import ABC, abstractmethod
from typing import AsyncGenerator, Any


class ConversionStrategy(ABC):
    """변환 전략의 기본 인터페이스"""
    
    @abstractmethod
    async def convert(self, file_names: list, orchestrator: Any, **kwargs) -> AsyncGenerator[bytes, None]:
        """
        파일 변환을 수행합니다.
        
        Args:
            file_names: 변환할 파일 목록 [(directory, file_name), ...]
            orchestrator: ServiceOrchestrator 인스턴스
            **kwargs: 추가 매개변수
            
        Yields:
            bytes: 스트리밍 응답 데이터
        """
        pass
