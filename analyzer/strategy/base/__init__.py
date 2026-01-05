"""공통 AST 처리 베이스 모듈

전략(DBMS/Framework) 간 공통 로직을 제공합니다.

주요 구성:
- StatementNode: 평탄화된 AST 노드 (unit_key/unit_name/unit_kind로 통합)
- AnalysisBatch, BatchPlanner: 배치 계획 및 실행
- BaseAstProcessor: 공통 파이프라인 (템플릿 메서드 패턴)
- FileStatus, FileAnalysisContext: 파일 분석 상태 추적
"""

from analyzer.strategy.base.statement_node import StatementNode
from analyzer.strategy.base.batch import AnalysisBatch, BatchPlanner
from analyzer.strategy.base.processor import BaseAstProcessor
from analyzer.strategy.base.file_context import FileStatus, FileAnalysisContext

__all__ = [
    "StatementNode",
    "AnalysisBatch",
    "BatchPlanner",
    "BaseAstProcessor",
    "FileStatus",
    "FileAnalysisContext",
]

