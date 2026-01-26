"""API 요청 헬퍼 함수

API 요청에서 헤더 정보를 추출하는 유틸리티 함수들입니다.
"""

from fastapi import HTTPException, Request

from config.settings import settings


def extract_api_key(request: Request, missing_status: int = 401) -> str:
    """API 키 추출 (헤더 → 환경 변수 순서로 폴백)
    
    Args:
        request: FastAPI Request 객체
        missing_status: API 키가 없을 때 반환할 HTTP 상태 코드
        
    Returns:
        API 키 문자열
        
    Raises:
        HTTPException: API 키가 없을 때
    """
    # 1. 헤더에서 API 키 추출
    api_key = (
        request.headers.get("OpenAI-Api-Key") or
        request.headers.get("Anthropic-Api-Key")
    )
    
    # 2. 헤더에 없으면 환경 변수에서 폴백
    if not api_key:
        api_key = settings.llm.api_key
    
    if not api_key:
        raise HTTPException(
            missing_status, 
            "요청 헤더 누락: OpenAI-Api-Key 또는 Anthropic-Api-Key (환경변수 LLM_API_KEY도 설정되지 않음)"
        )
    return api_key


def extract_locale(request: Request) -> str:
    """Accept-Language 헤더에서 로케일 추출
    
    Args:
        request: FastAPI Request 객체
        
    Returns:
        로케일 문자열 (기본값: "ko")
    """
    return request.headers.get("Accept-Language", "ko")

