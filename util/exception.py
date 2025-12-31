"""ROBO Analyzer 예외 클래스 정의

예외 계층 구조:
- RoboAnalyzerError (기본)
  ├── AnalysisError (분석 관련)
  │   ├── AstParseError (AST 파싱)
  │   ├── LLMCallError (LLM 호출)
  │   └── CodeProcessError (코드 처리)
  ├── Neo4jError (데이터베이스)
  │   ├── QueryExecutionError (쿼리 실행)
  │   └── ConnectionError (연결)
  ├── FileProcessError (파일 처리)
  └── ConfigError (설정 오류)

모든 예외는:
- 명확한 에러 메시지
- 컨텍스트 정보 (파일명, 라인 등)
- 원본 예외 체이닝
"""

import logging
import traceback
from typing import Any, Optional


class RoboAnalyzerError(Exception):
    """ROBO Analyzer 기본 예외 클래스
    
    모든 예외의 부모 클래스로, 공통 기능을 제공합니다.
    - 구조화된 에러 메시지
    - 컨텍스트 정보 저장
    - 로깅 자동화
    """
    
    def __init__(
        self,
        message: str = "ROBO Analyzer 오류가 발생했습니다",
        *,
        context: Optional[dict[str, Any]] = None,
        cause: Optional[Exception] = None,
        log_level: int = logging.ERROR,
    ):
        self.message = message
        self.context = context or {}
        self.cause = cause
        
        # 자동 로깅
        logger = logging.getLogger(self.__class__.__module__)
        log_msg = self._format_log_message()
        logger.log(log_level, log_msg, exc_info=cause is not None)
        
        super().__init__(message)
    
    def _format_log_message(self) -> str:
        """로그 메시지 포맷팅"""
        parts = [f"[{self.__class__.__name__}] {self.message}"]
        
        if self.context:
            ctx_str = ", ".join(f"{k}={v}" for k, v in self.context.items())
            parts.append(f"컨텍스트: {ctx_str}")
        
        if self.cause:
            parts.append(f"원인: {type(self.cause).__name__}: {self.cause}")
        
        return " | ".join(parts)
    
    def to_dict(self) -> dict[str, Any]:
        """API 응답용 딕셔너리 변환"""
        result = {
            "error_type": self.__class__.__name__,
            "message": self.message,
        }
        if self.context:
            result["context"] = self.context
        if self.cause:
            result["cause"] = str(self.cause)
        return result


# =============================================================================
# 분석 관련 예외
# =============================================================================

class AnalysisError(RoboAnalyzerError):
    """분석 과정에서 발생하는 예외 (기본)"""
    
    def __init__(
        self,
        message: str = "분석 과정에서 오류가 발생했습니다",
        *,
        file_name: Optional[str] = None,
        line_range: Optional[tuple[int, int]] = None,
        **kwargs,
    ):
        context = kwargs.pop("context", {}) or {}
        if file_name:
            context["file"] = file_name
        if line_range:
            context["lines"] = f"{line_range[0]}-{line_range[1]}"
        
        super().__init__(message, context=context, **kwargs)


class AstParseError(AnalysisError):
    """AST 파싱 중 발생하는 예외"""
    
    def __init__(
        self,
        message: str = "AST 파싱 중 오류가 발생했습니다",
        *,
        node_type: Optional[str] = None,
        **kwargs,
    ):
        context = kwargs.pop("context", {}) or {}
        if node_type:
            context["node_type"] = node_type
        
        super().__init__(message, context=context, **kwargs)


class LLMCallError(AnalysisError):
    """LLM 호출 중 발생하는 예외"""
    
    def __init__(
        self,
        message: str = "LLM 호출 중 오류가 발생했습니다",
        *,
        prompt_name: Optional[str] = None,
        model: Optional[str] = None,
        **kwargs,
    ):
        context = kwargs.pop("context", {}) or {}
        if prompt_name:
            context["prompt"] = prompt_name
        if model:
            context["model"] = model
        
        super().__init__(message, context=context, **kwargs)


class CodeProcessError(AnalysisError):
    """코드 처리 중 발생하는 예외"""
    
    def __init__(
        self,
        message: str = "코드 처리 중 오류가 발생했습니다",
        *,
        stage: Optional[str] = None,
        **kwargs,
    ):
        context = kwargs.pop("context", {}) or {}
        if stage:
            context["stage"] = stage
        
        super().__init__(message, context=context, **kwargs)


# =============================================================================
# Neo4j 관련 예외
# =============================================================================

class Neo4jError(RoboAnalyzerError):
    """Neo4j 작업 중 발생하는 예외 (기본)"""
    
    def __init__(
        self,
        message: str = "Neo4j 작업 중 오류가 발생했습니다",
        *,
        query: Optional[str] = None,
        **kwargs,
    ):
        context = kwargs.pop("context", {}) or {}
        if query:
            # 쿼리가 너무 길면 잘라서 저장
            context["query"] = query[:200] + "..." if len(query) > 200 else query
        
        super().__init__(message, context=context, **kwargs)


class QueryExecutionError(Neo4jError):
    """Cypher 쿼리 실행 중 발생하는 예외"""
    
    def __init__(
        self,
        message: str = "Cypher 쿼리 실행 중 오류가 발생했습니다",
        *,
        query_count: Optional[int] = None,
        **kwargs,
    ):
        context = kwargs.pop("context", {}) or {}
        if query_count:
            context["query_count"] = query_count
        
        super().__init__(message, context=context, **kwargs)


class Neo4jConnectionError(Neo4jError):
    """Neo4j 연결 관련 예외"""
    
    def __init__(
        self,
        message: str = "Neo4j 연결에 실패했습니다",
        *,
        uri: Optional[str] = None,
        **kwargs,
    ):
        context = kwargs.pop("context", {}) or {}
        if uri:
            context["uri"] = uri
        
        super().__init__(message, context=context, **kwargs)


# =============================================================================
# 파일 처리 예외
# =============================================================================

class FileProcessError(RoboAnalyzerError):
    """파일 처리 중 발생하는 예외"""
    
    def __init__(
        self,
        message: str = "파일 처리 중 오류가 발생했습니다",
        *,
        file_path: Optional[str] = None,
        operation: Optional[str] = None,
        **kwargs,
    ):
        context = kwargs.pop("context", {}) or {}
        if file_path:
            context["file_path"] = file_path
        if operation:
            context["operation"] = operation
        
        super().__init__(message, context=context, **kwargs)


# =============================================================================
# 설정 관련 예외
# =============================================================================

class ConfigError(RoboAnalyzerError):
    """설정 오류 예외"""
    
    def __init__(
        self,
        message: str = "설정 오류가 발생했습니다",
        *,
        config_key: Optional[str] = None,
        **kwargs,
    ):
        context = kwargs.pop("context", {}) or {}
        if config_key:
            context["config_key"] = config_key
        
        super().__init__(message, context=context, **kwargs)


class MissingApiKeyError(ConfigError):
    """API 키 누락 예외"""
    
    def __init__(
        self,
        message: str = "API 키가 설정되지 않았습니다",
        **kwargs,
    ):
        super().__init__(message, config_key="LLM_API_KEY", **kwargs)


