"""Robo Analyzer 예외 클래스 정의"""


class CustomBaseException(Exception):
    """기본 예외 클래스"""
    def __init__(self, message="오류가 발생했습니다"):
        self.message = message
        super().__init__(self.message)


class AnalysisError(CustomBaseException):
    """분석 과정에서 발생하는 예외"""
    def __init__(self, message="분석 과정에서 오류가 발생했습니다."):
        super().__init__(message)


class ProcessAnalyzeCodeError(AnalysisError):
    """코드 분석 처리 중 발생하는 예외"""
    def __init__(self, message="코드 분석 처리 도중에 오류가 발생했습니다."):
        super().__init__(message)


class LLMCallError(AnalysisError):
    """LLM 호출 중 발생하는 예외"""
    def __init__(self, message="LLM을 호출하고 응답을 받는 과정에서 오류가 발생했습니다."):
        super().__init__(message)


class UtilProcessingError(AnalysisError):
    """유틸리티 처리 중 발생하는 예외"""
    def __init__(self, message="유틸리티 처리 중 오류가 발생했습니다."):
        super().__init__(message)


class Neo4jError(AnalysisError):
    """Neo4j 그래프 DB 작업 중 발생하는 예외"""
    def __init__(self, message="Neo4j에서 그래프 DB 작업 도중 오류가 발생했습니다."):
        super().__init__(message)


class FileProcessingError(AnalysisError):
    """파일 처리 중 발생하는 예외"""
    def __init__(self, message="파일 처리 도중 오류가 발생했습니다."):
        super().__init__(message)
