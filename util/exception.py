class CustomBaseException(Exception):
    def __init__(self, message="기본 오류가 발생했습니다"):
        self.message = message
        super().__init__(self.message)

"""----------------------------------------------------------Converting---------------------------------------------------------"""
class ConvertingError(CustomBaseException):
    def __init__(self, message="Converting 과정에서 오류가 발생했습니다."):
        super().__init__(message)

class EntityCreationError(ConvertingError):
    def __init__(self, message="엔티티 클래스를 생성하는 도중 오류가 발생했습니다."):
        super().__init__(message)

class RepositoryCreationError(ConvertingError):
    def __init__(self, message="리포지토리 인터페이스를 생성하는 도중 오류가 발생했습니다."):
        super().__init__(message)

class CommandCreationError(ConvertingError):
    def __init__(self, message="잘못된 입력값이 제공되었습니다"):
        super().__init__(message)

class SkeletonCreationError(ConvertingError):
    def __init__(self, message="인증에 실패했습니다"):
        super().__init__(message)

class ServiceCreationError(ConvertingError):
    def __init__(self, message="인증에 실패했습니다"):
        super().__init__(message)

class ParentSkeletonCreationError(ConvertingError):
    def __init__(self, message="인증에 실패했습니다"):
        super().__init__(message)

class PomXmlCreationError(ConvertingError):
    def __init__(self, message="스프링부트의 Pom.xml 파일을 생성하는 도중 오류가 발생했습니다."):
        super().__init__(message)

class AplPropertiesCreationError(ConvertingError):
    def __init__(self, message="스프링부트의 application.properties 파일을 생성하는 도중 오류가 발생했습니다."):
        super().__init__(message)

class MainCreationError(ConvertingError):
    def __init__(self, message="스프링부트의 메인 클래스를 생성하는 도중 오류가 발생했습니다."):
        super().__init__(message)

class VariableNodeError(ConvertingError):
    def __init__(self, message="변수 노드를 처리하는 도중 오류가 발생했습니다."):
        super().__init__(message)

"""----------------------------------------------------------Understanding---------------------------------------------------------"""
class UnderstandingError(CustomBaseException):
    def __init__(self, message="Understanding 과정에서 오류가 발생했습니다."):
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

class EventRsRqError(UnderstandingError):
    def __init__(self, message="이벤트를 송신하고 수신하는 도중 오류가 발생했습니다"):
        super().__init__(message)

class CreateNodeError(UnderstandingError):
    def __init__(self, message="노드를 생성을 위한 사이퍼쿼리 생성 및 실행 도중 오류가 발생했습니다."):
        super().__init__(message)

"""----------------------------------------------------Service---------------------------------------------------------------"""
class Neo4jError(CustomBaseException):
    def __init__(self, message="Neo4j에서 그래프 DB에 읽기 쓰기 작업을 하는 도중 오류가 발생했습니다."):
        super().__init__(message)


class Java2dethsError(CustomBaseException):
    def __init__(self, message="2단계 깊이 기준 노드로 자바로 전환하는 도중 오류가 발생했습니다."):
        super().__init__(message)


class AddLineNumError(CustomBaseException):
    def __init__(self, message="스토어드 프로시저 코드에 라인 번호를 추가하는 도중 오류가 발생했습니다."):
        super().__init__(message)

"""----------------------------------------------------------공통---------------------------------------------------------"""
class TokenCountError(UnderstandingError, ConvertingError):
    def __init__(self, message="토큰 계산 중 오류가 발생했습니다."):
        super().__init__(message)

class LLMCallError(UnderstandingError, ConvertingError):
    def __init__(self, message="LLM을 호출하고 응답을 받는 과정에서 오류가 발생했습니다."):
        super().__init__(message)

class ProcessResultError(UnderstandingError, ConvertingError):
    def __init__(self, message="LLM의 결과 처리를 준비 및 시작하는 도중 오류가 발생했습니다."):
        super().__init__(message)

class TraverseCodeError(UnderstandingError, ConvertingError):
    def __init__(self, message="노드를 순회하는 도중 오류가 발생했습니다."):
        super().__init__(message)

class ExtractCodeError(UnderstandingError, ConvertingError):
    def __init__(self, message="범위내에 코드 추출 도중에 오류가 발생했습니다."):
        super().__init__(message)

class HandleResultError(UnderstandingError, ConvertingError):
    def __init__(self, message="LLM의 결과를 이용해서 추가적인 처리를 하는 도중 오류가 발생했습니다."):
        super().__init__(message)

class SaveFileError(UnderstandingError, ConvertingError):
    def __init__(self, message="파일을 읽고 저장하는 도중 오류가 발생했습니다."):
        super().__init__(message)


"""----------------------------------------------------------결과비교---------------------------------------------------------"""
class CompareResultError(CustomBaseException):
    def __init__(self, message="결과 검증 및 비교하는 도중 오류가 발생했습니다."):
        super().__init__(message)