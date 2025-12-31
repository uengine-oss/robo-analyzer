"""Framework(Java/Kotlin) 분석 모듈

주요 클래스:
- FrameworkAnalyzer: 전체 분석 전략 (진입점)
- FrameworkAstProcessor: AST 처리 및 LLM 분석
"""

from .framework_analyzer import FrameworkAnalyzer
from .ast_processor import FrameworkAstProcessor

__all__ = ["FrameworkAnalyzer", "FrameworkAstProcessor"]

