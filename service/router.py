"""Robo Analyzer API 라우터

엔드포인트:
- POST /backend/understanding/ : 소스 파일 분석 → Neo4j 그래프 생성
- DELETE /backend/deleteAll/   : 사용자 데이터 전체 삭제
"""

import logging

from fastapi import APIRouter, HTTPException, Request
from fastapi.responses import StreamingResponse

from service.service import (
    ServiceOrchestrator,
    create_orchestrator,
    get_user_id,
)
from util.utility_tool import build_error_body, stream_with_error_boundary


router = APIRouter(prefix="/backend")
logger = logging.getLogger(__name__)


# =============================================================================
# 엔드포인트
# =============================================================================

@router.post("/understanding/")
async def understand_data(request: Request):
    """소스 파일을 분석하여 Neo4j 그래프 데이터 생성
    
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

    logger.info("Understanding: project=%s, strategy=%s, files=%d",
                orchestrator.project_name, orchestrator.strategy, len(file_names))

    return StreamingResponse(
        stream_with_error_boundary(orchestrator.understand_project(file_names))
    )


@router.delete("/deleteAll/")
async def delete_all_data(request: Request):
    """사용자 데이터 전체 삭제 (임시 파일 + Neo4j 그래프)
    
    Request Headers:
        Session-UUID: 세션 UUID (필수)
    
    Response: JSON
    """
    user_id = get_user_id(request)

    try:
        await ServiceOrchestrator(user_id=user_id, api_key='', locale='', project_name='').cleanup_all_data()
    except Exception as e:
        raise HTTPException(500, build_error_body(e))

    return {"message": "모든 임시 파일이 삭제되었습니다."}
