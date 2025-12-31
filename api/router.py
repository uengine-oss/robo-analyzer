"""ROBO Analyzer API 라우터

엔드포인트:
- POST /robo/analyze/   : 소스 파일 분석 → Neo4j 그래프 생성
- DELETE /robo/data/    : 사용자 데이터 전체 삭제
"""

import logging

from fastapi import APIRouter, HTTPException, Request
from fastapi.responses import StreamingResponse

from api.orchestrator import (
    AnalysisOrchestrator,
    create_orchestrator,
    extract_user_id,
)
from config.settings import settings
from util.stream_utils import build_error_body, stream_with_error_boundary


router = APIRouter(prefix=settings.api_prefix)
logger = logging.getLogger(__name__)


@router.post("/analyze/")
async def analyze_source_code(request: Request):
    """소스 파일을 분석하여 Neo4j 그래프 데이터 생성
    
    Request Headers:
        Session-UUID: 사용자 세션 ID (필수)
        OpenAI-Api-Key: LLM API 키 (필수)
        Accept-Language: 출력 언어 (기본: ko)
    
    Request Body:
        projectName: 프로젝트명 (필수)
        strategy: "framework" | "dbms" (기본: framework)
        target: "java" | "oracle" | ... (기본: java)
    
    Response: NDJSON 스트림
    """
    body = await request.json()
    orchestrator = await create_orchestrator(request, body)
    
    file_names = orchestrator.discover_source_files()
    if not file_names:
        raise HTTPException(400, "분석할 소스 파일이 없습니다.")

    logger.info(
        "[API] 분석 시작 | project=%s | strategy=%s | files=%d",
        orchestrator.project_name,
        orchestrator.strategy,
        len(file_names),
    )

    return StreamingResponse(
        stream_with_error_boundary(orchestrator.run_analysis(file_names)),
        media_type="application/x-ndjson",
    )


@router.delete("/data/")
async def delete_user_data(request: Request):
    """사용자 데이터 전체 삭제 (임시 파일 + Neo4j 그래프)
    
    Request Headers:
        Session-UUID: 세션 UUID (필수)
    
    Response: JSON
    """
    user_id = extract_user_id(request)
    logger.info("[API] 데이터 삭제 요청 | user=%s", user_id)

    try:
        await AnalysisOrchestrator(
            user_id=user_id,
            api_key="",
            locale="",
            project_name="",
        ).cleanup_all_data()
        logger.info("[API] 데이터 삭제 완료 | user=%s", user_id)
    except Exception as e:
        logger.error("[API] 데이터 삭제 실패 | user=%s | error=%s", user_id, e)
        raise HTTPException(500, build_error_body(e))

    return {"message": "모든 데이터가 삭제되었습니다."}
