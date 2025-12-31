"""분석 전략 기본 인터페이스

모든 분석 전략(Framework, DBMS)의 기본 인터페이스를 정의합니다.

주요 구성:
- AnalyzerStrategy: 추상 기본 클래스
- 공통 유틸리티 함수들
"""

from abc import ABC, abstractmethod
from typing import Any, AsyncGenerator


class AnalyzerStrategy(ABC):
    """분석 전략 기본 인터페이스
    
    Framework(Java/Kotlin)와 DBMS(PL/SQL) 분석을 위한 전략 패턴.
    
    사용법:
        strategy = AnalyzerFactory.create("framework")
        async for chunk in strategy.analyze(file_names, orchestrator):
            yield chunk
    """

    @abstractmethod
    async def analyze(
        self,
        file_names: list[tuple[str, str]],
        orchestrator: Any,
        **kwargs,
    ) -> AsyncGenerator[bytes, None]:
        """파일 목록을 분석하여 결과를 스트리밍합니다.
        
        Args:
            file_names: [(directory, file_name), ...] 튜플 리스트
            orchestrator: ServiceOrchestrator 인스턴스
            **kwargs: 추가 옵션
            
        Yields:
            NDJSON 형식의 바이트 스트림
        """
        raise NotImplementedError

    @staticmethod
    def calc_progress(current_line: int, total_lines: int) -> int:
        """현재 진행률 계산 (0-99%)
        
        Args:
            current_line: 현재 처리 중인 라인
            total_lines: 전체 라인 수
            
        Returns:
            진행률 (0-99)
        """
        if total_lines <= 0:
            return 0
        return min(int((current_line / total_lines) * 100), 99)

