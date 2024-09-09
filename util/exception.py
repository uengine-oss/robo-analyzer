class CustomBaseException(Exception):
    def __init__(self, message="기본 오류가 발생했습니다"):
        self.message = message
        super().__init__(self.message)

# class RepositoryCreationError(CustomBaseException):
#     def __init__(self, message="데이터베이스 연결에 실패했습니다"):
#         super().__init__(message)

# class CommandCreationError(CustomBaseException):
#     def __init__(self, message="잘못된 입력값이 제공되었습니다"):
#         super().__init__(message)

# class SkeletonCreationError(CustomBaseException):
#     def __init__(self, message="인증에 실패했습니다"):
#         super().__init__(message)

# class ServiceCreationError(CustomBaseException):
#     def __init__(self, message="인증에 실패했습니다"):
#         super().__init__(message)

# class ParentSkeletonCreationError(CustomBaseException):
#     def __init__(self, message="인증에 실패했습니다"):
#         super().__init__(message)

# class PomXmlCreationError(CustomBaseException):
#     def __init__(self, message="인증에 실패했습니다"):
#         super().__init__(message)

# class AplPropertiesCreationError(CustomBaseException):
#     def __init__(self, message="인증에 실패했습니다"):
#         super().__init__(message)

# class MainCreationError(CustomBaseException):
#     def __init__(self, message="인증에 실패했습니다"):
#         super().__init__(message)

"""-------------------------------------------------------------------------------------------------------------------"""
class UnderstandingError(CustomBaseException):
    def __init__(self, message="Understanding 과정에서 오류가 발생했습니다."):
        super().__init__(message)

class TokenCountError(UnderstandingError):
    def __init__(self, message="토큰 계산 중 오류가 발생했습니다."):
        super().__init__(message)

class ExtractCodeError(UnderstandingError):
    def __init__(self, message="범위내에 코드 추출 도중에 오류가 발생했습니다."):
        super().__init__(message)

class SummarizeCodeError(UnderstandingError):
    def __init__(self, message="코드를 요약하는 도중 오류가 발생했습니다."):
        super().__init__(message)

class FocusedCodeError(UnderstandingError):
    def __init__(self, message="분석할 코드 생성 도중에 오류가 발생했습니다."):
        super().__init__(message)

class RemoveInfoCodeError(UnderstandingError):
    def __init__(self, message="불필요한 정보를 제거하는 도중 오류가 발생했습니다."):
        super().__init__(message)

class ProcessResultError(UnderstandingError):
    def __init__(self, message="LLM의 결과 처리를 준비 및 시작하는 도중 오류가 발생했습니다."):
        super().__init__(message)

class HandleResultError(UnderstandingError):
    def __init__(self, message="LLM의 결과를 이용해서 추가적인 처리를 하는 도중 오류가 발생했습니다."):
        super().__init__(message)

class LLMCallError(UnderstandingError):
    def __init__(self, message="LLM을 호출하고 응답을 받는 과정에서 오류가 발생했습니다."):
        super().__init__(message)

class EventRsRqError(UnderstandingError):
    def __init__(self, message="이벤트를 송신하고 수신하는 도중 오류가 발생했습니다"):
        super().__init__(message)

class CreateNodeError(UnderstandingError):
    def __init__(self, message="노드를 생성을 위한 사이퍼쿼리 생성 및 실행 도중 오류가 발생했습니다."):
        super().__init__(message)

class TraverseCodeError(UnderstandingError):
    def __init__(self, message="노드를 순회하는 도중 오류가 발생했습니다."):
        super().__init__(message)
"""-------------------------------------------------------------------------------------------------------------------"""
class Neo4jError(CustomBaseException):
    def __init__(self, message="Neo4j에서 그래프 DB에 읽기 쓰기 작업을 하는 도중 오류가 발생했습니다."):
        super().__init__(message)


class Java2dethsError(CustomBaseException):
    def __init__(self, message="2단계 깊이 기준 노드로 자바로 전환하는 도중 오류가 발생했습니다."):
        super().__init__(message)
