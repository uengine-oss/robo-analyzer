import logging
import os
from fastapi import APIRouter, HTTPException, Request
from fastapi.responses import FileResponse, StreamingResponse
from service.service import ServiceOrchestrator, BASE_DIR
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


def _extract_payload(file_data: dict) -> tuple[str, str, str, list[tuple[str, str]], list[tuple[str, str]]]:
    """요청 JSON에서 projectName, strategy, target, files, classNames를 추출
    
    Returns:
        tuple: (projectName, strategy, target, files, classNames)
            - projectName: 프로젝트 이름
            - strategy: 전략 (dbms | framework | architecture)
            - target: 타겟 (oracle | postgresql | java | python | mermaid)
            - files: [(systemName, fileName), ...] 리스트
            - classNames: [(systemName, className), ...] 리스트 (architecture 전략용)
    """
    project_name = file_data.get('projectName')
    strategy = (file_data.get('strategy') or 'dbms').strip().lower()
    target = (file_data.get('target') or 'oracle').strip().lower()
    raw_class_names = file_data.get('classNames') or []
    
    if not project_name:
        raise HTTPException(status_code=400, detail="projectName이 없습니다.")
    
    # architecture 전략은 classNames 필수 (형식: "systemName/className")
    if strategy == 'architecture':
        if not raw_class_names:
            raise HTTPException(status_code=400, detail="architecture 전략은 classNames가 필요합니다.")
        
        class_names = []
        for item in raw_class_names:
            if '/' not in item:
                raise HTTPException(status_code=400, detail=f"잘못된 classNames 형식: '{item}'. 'systemName/className' 형식이어야 합니다.")
            system_name, class_name = item.split('/', 1)
            if not system_name or not class_name:
                raise HTTPException(status_code=400, detail=f"잘못된 classNames 형식: '{item}'. systemName과 className이 모두 필요합니다.")
            class_names.append((system_name.strip(), class_name.strip()))
        
        return project_name, strategy, target, [], class_names
    
    # 다른 전략은 files 필수
    files = [
        (system.get('name'), sp_name)
        for system in (file_data.get('systems') or [])
        for sp_name in (system.get('sp') or [])
        if system.get('name') and sp_name
    ]
    
    if not files:
        raise HTTPException(status_code=400, detail="시스템 또는 파일 정보가 없습니다.")
    
    return project_name, strategy, target, files, []


#-------------------------------------------------------------------------#
# Endpoints
#-------------------------------------------------------------------------#
@router.post("/cypherQuery/")
async def understand_data(request: Request):
    """소스 파일을 분석하여 Neo4j 그래프 데이터를 생성"""
    from util.utility_tool import stream_with_error_boundary
    file_data = await request.json()
    user_id, api_key = await _resolve_user_and_api_key(request, missing_env_status=401)
    project_name, strategy, target, file_names, _ = _extract_payload(file_data)

    logging.info("Understand: user=%s, project=%s, strategy=%s, target=%s, files=%d",
                 user_id, project_name, strategy, target, len(file_names))

    orchestrator = ServiceOrchestrator(
        user_id=user_id,
        api_key=api_key,
        locale=_locale(request),
        project_name=project_name,
        strategy=strategy,
        target=target,
    )
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
        await ServiceOrchestrator(
            user_id=user_id,
            api_key='',
            locale='',
            project_name=project_name
        ).zip_project(
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
        await ServiceOrchestrator(
            user_id=user_id,
            api_key='',
            locale='',
            project_name=''
        ).cleanup_all_data()
    except Exception as e:
        raise HTTPException(status_code=500, detail=build_error_body(e))

    return {"message": "모든 임시 파일이 삭제되었습니다."}


@router.post("/convert/")
async def convert_project(request: Request):
    """변환 프로세스 실행"""
    from util.utility_tool import stream_with_error_boundary
    file_data = await request.json()
    user_id, api_key = await _resolve_user_and_api_key(request, missing_env_status=400)
    project_name, strategy, target, file_names, class_names = _extract_payload(file_data)

    logging.info("Convert: user=%s, project=%s, strategy=%s, target=%s, files=%d, classes=%d",
                 user_id, project_name, strategy, target, len(file_names), len(class_names))

    orchestrator = ServiceOrchestrator(
        user_id=user_id,
        api_key=api_key,
        locale=_locale(request),
        project_name=project_name,
        strategy=strategy,
        target=target,
    )
    await orchestrator.validate_api_key()

    return StreamingResponse(
        stream_with_error_boundary(orchestrator.convert_project(file_names, class_names=class_names)),
        media_type="text/plain"
    )