"""Legacy Modernizer API 라우터

엔드포인트:
- POST /backend/understanding/ : 소스 파일 분석 → Neo4j 그래프 생성
- POST /backend/converting/    : 코드 변환 (DBMS → Java, Architecture 다이어그램 등)
- POST /backend/download/      : 변환된 프로젝트 ZIP 다운로드
- DELETE /backend/deleteAll/   : 사용자 데이터 전체 삭제
"""

import logging
import os

from fastapi import APIRouter, HTTPException, Request
from fastapi.responses import FileResponse, StreamingResponse

from service.service import (
    BASE_DIR,
    ServiceOrchestrator,
    create_orchestrator,
    get_user_id,
    parse_class_names,
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


@router.post("/converting/")
async def convert_project(request: Request):
    """코드 변환 프로세스 실행
    
    Request Body:
        projectName: 프로젝트명 (필수)
        strategy: "framework" | "dbms" | "architecture" (기본: framework)
        target: "java" | "oracle" | ... (기본: java)
        classNames: ["dir/ClassName", ...] (architecture 전략 필수)
    
    Response: NDJSON 스트림
    """
    body = await request.json()
    orchestrator = await create_orchestrator(request, body, api_key_missing_status=400)
    
    # architecture 전략: classNames 필수
    if orchestrator.strategy == 'architecture':
        raw_class_names = body.get('classNames') or []
        if not raw_class_names:
            raise HTTPException(400, "architecture 전략은 classNames가 필요합니다.")
        file_names, class_names = [], parse_class_names(raw_class_names)
    else:
        file_names = orchestrator.discover_source_files()
        class_names = []
        if not file_names:
            raise HTTPException(400, "변환할 소스 파일이 없습니다.")

    logger.info("Converting: project=%s, strategy=%s, files=%d, classes=%d",
                orchestrator.project_name, orchestrator.strategy, len(file_names), len(class_names))

    return StreamingResponse(
        stream_with_error_boundary(orchestrator.convert_project(file_names, class_names=class_names)),
        media_type="text/plain"
    )


@router.post("/download/")
async def download_project(request: Request):
    """변환된 프로젝트를 ZIP 파일로 다운로드
    
    Request Body:
        projectName: 프로젝트명 (필수)
    
    Response: application/zip
    """
    user_id = get_user_id(request)
    body = await request.json()
    project_name = body.get('projectName', 'project')

    source_dir = os.path.join(BASE_DIR, 'target', 'java', user_id, project_name)
    output_path = os.path.join(BASE_DIR, 'data', user_id, 'zipfile', f'{project_name}.zip')
    os.makedirs(os.path.dirname(output_path), exist_ok=True)

    try:
        await ServiceOrchestrator(user_id=user_id, api_key='', locale='', project_name=project_name).zip_project(source_dir, output_path)
    except Exception as e:
        raise HTTPException(500, build_error_body(e))

    return FileResponse(output_path, filename=f"{project_name}.zip", media_type='application/octet-stream')


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
