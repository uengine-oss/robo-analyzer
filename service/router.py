import logging
import os
from fastapi import APIRouter, HTTPException, Request
from fastapi.responses import FileResponse, StreamingResponse
from service.service import ServiceOrchestrator, BASE_DIR
from conversion.strategies.strategy_factory import StrategyFactory
from dotenv import load_dotenv
from util.utility_tool import build_error_body


load_dotenv()
router = APIRouter()
logger = logging.getLogger(__name__)
TEST_SESSIONS = ("EN_TestSession", "KO_TestSession")


#-------------------------------------------------------------------------#
# Helpers
#-------------------------------------------------------------------------#
async def _resolve_user_and_api_key(request: Request, missing_env_status: int) -> tuple[str, str]:
    """사용자 ID와 API 키를 추출합니다.

    매개변수:
    - request: FastAPI Request
    - missing_env_status: 테스트 세션에서 환경 변수 키가 없을 때 사용할 상태 코드

    반환값:
    - (user_id, api_key)
    """
    user_id = request.headers.get('Session-UUID')
    if not user_id:
        raise HTTPException(status_code=400, detail="요청 헤더 누락: Session-UUID")

    if user_id in TEST_SESSIONS:
        api_key = os.getenv("LLM_API_KEY") or os.getenv("API_KEY")
        if not api_key:
            raise HTTPException(status_code=missing_env_status, detail="환경 변수에 API 키가 설정되어 있지 않습니다.")
        return user_id, api_key

    api_key = request.headers.get('OpenAI-Api-Key') or request.headers.get('Anthropic-Api-Key')
    if not api_key:
        raise HTTPException(status_code=401, detail="요청 헤더 누락: OpenAI-Api-Key 또는 Anthropic-Api-Key")
    return user_id, api_key


def _locale(request: Request) -> str:
    """요청 헤더에서 로케일을 추출합니다."""
    return request.headers.get('Accept-Language', 'ko')


def _extract_payload(file_data: dict) -> tuple[str, str, list[tuple[str, str]], str]:
    """요청 JSON에서 projectName, dbms, (systemName, fileName) 리스트, targetLang을 추출"""
    project_name = file_data.get('projectName')
    dbms = (file_data.get('dbms') or 'postgres').strip().lower()
    target_lang = (file_data.get('targetLang') or 'java').strip().lower()
    
    if not project_name:
        raise HTTPException(status_code=400, detail="projectName이 없습니다.")
    
    files = [
        (system.get('name'), sp_name)
        for system in (file_data.get('systems') or [])
        for sp_name in (system.get('sp') or [])
        if system.get('name') and sp_name
    ]
    
    if not files:
        raise HTTPException(status_code=400, detail="시스템 또는 파일 정보가 없습니다.")
    
    return project_name, dbms, files, target_lang


#-------------------------------------------------------------------------#
# Endpoints
#-------------------------------------------------------------------------#
@router.post("/cypherQuery/")
async def understand_data(request: Request):
    """Neo4j 사이퍼 쿼리 생성 및 실행 - PL/SQL 파일을 분석하여 그래프 데이터를 생성"""
    from util.utility_tool import stream_with_error_boundary
    file_data = await request.json()
    user_id, api_key = await _resolve_user_and_api_key(request, missing_env_status=401)
    project_name, dbms, file_names, target_lang = _extract_payload(file_data)

    logging.info("User ID: %s, Project: %s, DBMS: %s, Target: %s, Files: %d", user_id, project_name, dbms, target_lang, len(file_names))

    orchestrator = ServiceOrchestrator(user_id, api_key, _locale(request), project_name, dbms, target_lang)
    await orchestrator.validate_api_key()

    return StreamingResponse(stream_with_error_boundary(orchestrator.understand_project(file_names)))


@router.post("/downloadJava/")
async def download_spring_project(request: Request):
    """생성된 Spring Boot 프로젝트를 ZIP으로 압축하여 다운로드"""
    user_id = request.headers.get('Session-UUID')
    if not user_id:
        raise HTTPException(status_code=400, detail="요청 헤더 누락: Session-UUID")

    body = await request.json()
    project_name = body.get('projectName', 'project')
    user_java_dir = os.path.join(BASE_DIR, 'target', 'java', user_id)
    output_zip_path = os.path.join(BASE_DIR, 'data', user_id, 'zipfile', f'{project_name}.zip')

    os.makedirs(os.path.dirname(output_zip_path), exist_ok=True)

    try:
        await ServiceOrchestrator(user_id, '', '', project_name, '').zip_project(
            os.path.join(user_java_dir, project_name), 
            output_zip_path
        )
    except Exception as e:
        raise HTTPException(status_code=500, detail=build_error_body(e))

    return FileResponse(path=output_zip_path, filename=f"{project_name}.zip", media_type='application/octet-stream')
    



@router.delete("/deleteAll/")
async def delete_all_data(request: Request):
    """사용자 데이터 전체 삭제 - 임시 파일 및 Neo4j 그래프 데이터"""
    user_id = request.headers.get('Session-UUID')
    if not user_id:
        raise HTTPException(status_code=400, detail="요청 헤더 누락: Session-UUID")

    try:
        await ServiceOrchestrator(user_id, '', '', '', '').cleanup_all_data()
    except Exception as e:
        raise HTTPException(status_code=500, detail=build_error_body(e))

    return {"message": "모든 임시 파일이 삭제되었습니다."}


@router.post("/convert/")
async def convert_project(request: Request):
    """전략패턴을 사용한 다양한 변환 타입 처리"""
    from util.utility_tool import stream_with_error_boundary
    file_data = await request.json()
    user_id, api_key = await _resolve_user_and_api_key(request, missing_env_status=400)
    project_name, dbms, file_names, target_lang = _extract_payload(file_data)

    # 새로운 필드들
    conversion_type = file_data.get('conversionType', 'framework')
    target_framework = file_data.get('targetFramework', 'springboot')
    target_dbms = file_data.get('targetDbms', 'oracle')

    logging.info("Convert: type=%s, project=%s, files=%d, target=%s",
                conversion_type, project_name, len(file_names),
                target_framework if conversion_type == 'framework' else f"{dbms}→{target_dbms}")

    # 전략 생성
    strategy = StrategyFactory.create_strategy(
        conversion_type,
        target_dbms=target_dbms,
        target_framework=target_framework
    )

    orchestrator = ServiceOrchestrator(user_id, api_key, _locale(request), project_name, dbms, target_lang)
    await orchestrator.validate_api_key()

    # 전략 실행 (에러 경계 적용)
    return StreamingResponse(stream_with_error_boundary(strategy.convert(file_names, orchestrator=orchestrator)), media_type="text/plain")