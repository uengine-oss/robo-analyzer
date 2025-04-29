class CustomBaseException(Exception):
    def __init__(self, message="기본 오류가 발생했습니다"):
        self.message = message
        super().__init__(self.message)

"""----------------------------------------------------------Converting---------------------------------------------------------"""
class ConvertingError(CustomBaseException):
    def __init__(self, message="Converting 과정에서 오류가 발생했습니다."):
        super().__init__(message)

class GenerateTargetError(ConvertingError):
    def __init__(self, message="Target 파일을 생성하는 도중 오류가 발생했습니다."):
        super().__init__(message)
"""----------------------------------------------------------Understanding---------------------------------------------------------"""
class UnderstandingError(CustomBaseException):
    def __init__(self, message="Understanding 과정에서 오류가 발생했습니다."):
        super().__init__(message)

class ProcessAnalyzeCodeError(UnderstandingError):
    def __init__(self, message="코드 분석 처리 도중에 오류가 발생했습니다."):
        super().__init__(message)
"""----------------------------------------------------------공통---------------------------------------------------------"""

class LLMCallError(UnderstandingError, ConvertingError):
    def __init__(self, message="LLM을 호출하고 응답을 받는 과정에서 오류가 발생했습니다."):
        super().__init__(message)

class UtilProcessingError(UnderstandingError, ConvertingError):
    def __init__(self, message="유틸리티 처리 중 오류가 발생했습니다."):
        super().__init__(message)

class Neo4jError(UnderstandingError, ConvertingError):
    def __init__(self, message="Neo4j에서 그래프 DB에 읽기 쓰기 작업을 하는 도중 오류가 발생했습니다."):
        super().__init__(message)

class FileProcessingError(UnderstandingError, ConvertingError):
    def __init__(self, message="파일 처리 도중 오류가 발생했습니다."):
        super().__init__(message)