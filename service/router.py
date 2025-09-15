###########################################################################
# Imports
###########################################################################
import logging
import os
from fastapi import APIRouter, HTTPException, Request
from fastapi.responses import FileResponse, StreamingResponse
from service.service import delete_all_temp_data, process_project_zipping
from service.service import generate_and_execute_cypherQuery
from service.service import generate_spring_boot_project
from service.service import validate_anthropic_api_key
from dotenv import load_dotenv


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
        raise HTTPException(status_code=400, detail="사용자 ID가 없습니다.")

    if user_id in TEST_SESSIONS:
        api_key = os.getenv("LLM_API_KEY") or os.getenv("API_KEY")
        if not api_key:
            raise HTTPException(status_code=missing_env_status, detail="환경 변수에 API 키가 설정되어 있지 않습니다.")
        return user_id, api_key

    api_key = request.headers.get('OpenAI-Api-Key') or request.headers.get('Anthropic-Api-Key')
    if not api_key:
        raise HTTPException(status_code=401, detail="Anthropic API 키가 없습니다.")
    return user_id, api_key


async def _ensure_valid_key(user_id: str, api_key: str) -> None:
    """API 키를 검증합니다. 테스트 세션은 검증을 생략합니다."""
    if user_id in TEST_SESSIONS:
        return
    if not await validate_anthropic_api_key(api_key):
        raise HTTPException(status_code=401, detail="유효하지 않은 API 키입니다.")


def _locale(request: Request) -> str:
    """요청 헤더에서 로케일을 추출합니다."""
    return request.headers.get('Accept-Language', 'ko')


def _extract_file_names(file_data: dict) -> list[tuple[str, str]]:
    """요청 JSON에서 (folderName, fileName) 튜플 리스트를 추출합니다."""
    files = [(item['folderName'], item['fileName']) for item in file_data.get('fileInfos', [])]
    if not files:
        raise HTTPException(status_code=400, detail="파일 정보가 없습니다.")
    return files


#-------------------------------------------------------------------------#
# Endpoints
#-------------------------------------------------------------------------#
@router.post("/cypherQuery/")
async def understand_data(request: Request):
    """전달받은 파일로 Neo4j 사이퍼 쿼리를 생성/실행하고 그래프 데이터를 스트리밍합니다.

    매개변수:
    - request: fileInfos를 포함한 요청 객체

    반환값:
    - StreamingResponse
    """
    try:
        user_id, api_key = await _resolve_user_and_api_key(request, missing_env_status=401)
        await _ensure_valid_key(user_id, api_key)
        locale = _locale(request)
        file_data = await request.json()
        file_names = _extract_file_names(file_data)
        logging.info("User ID: %s, File Infos: %s", user_id, file_names)
        return StreamingResponse(generate_and_execute_cypherQuery(file_names, user_id, api_key, locale))
    
    except Exception as e:
        error_message = f"Understanding 처리 중 오류 발생: {str(e)}"
        logger.exception(error_message)
        raise HTTPException(status_code=500, detail=error_message)


@router.post("/springBoot/")
async def covnert_spring_project(request: Request):
    """PL/SQL 파일을 스프링 부트 프로젝트로 변환하고 결과를 스트리밍합니다.

    매개변수:
    - request: fileInfos를 포함한 요청 객체

    반환값:
    - StreamingResponse
    """
    try:
        user_id, api_key = await _resolve_user_and_api_key(request, missing_env_status=400)
        await _ensure_valid_key(user_id, api_key)
        locale = _locale(request)
        file_data = await request.json()
        logging.info("Received File Info for Convert Spring Boot: %s", file_data)
        file_names = _extract_file_names(file_data)
        return StreamingResponse(generate_spring_boot_project(file_names, user_id, api_key, locale), media_type="text/plain")
    
    except Exception as e:
        error_message = f"스프링 부트 프로젝트 생성 도중 오류 발생: {str(e)}"
        logger.exception(error_message)
        raise HTTPException(status_code=500, detail=error_message)




@router.post("/downloadJava/")
async def download_spring_project(request: Request):
    """생성된 스프링 부트 프로젝트를 ZIP으로 압축해 반환합니다.

    매개변수:
    - request: projectName을 포함한 요청 객체

    반환값:
    - FileResponse
    """
    try:
        user_id = request.headers.get('Session-UUID')
        if not user_id:
            raise HTTPException(status_code=400, detail="사용자 ID가 없습니다.")
        body = await request.json()
        project_name = body.get('projectName', 'project')

        base_dir = os.getenv('DOCKER_COMPOSE_CONTEXT') or os.path.dirname(os.getcwd())
        target_path = os.path.join(base_dir, 'target', 'java', user_id, project_name)
        zipfile_dir = os.path.join(base_dir, user_id, 'zipfile') if base_dir.endswith('/data') or base_dir.endswith('\\data') else os.path.join(base_dir, 'data', user_id, 'zipfile')
        os.makedirs(zipfile_dir, exist_ok=True)
        output_zip_path = os.path.join(zipfile_dir, f'{project_name}.zip')

        await process_project_zipping(target_path, output_zip_path)

        return FileResponse(path=output_zip_path, filename=f"{project_name}.zip", media_type='application/octet-stream')
    
    except Exception as e:
        error_message = f"스프링 부트 프로젝트를 Zip 파일로 압축하는데 실패했습니다: {str(e)}"
        logger.exception(error_message)
        raise HTTPException(status_code=500, detail=error_message)
    



@router.delete("/deleteAll/")
async def delete_all_data(request: Request):
    """사용자의 임시 파일과 그래프 데이터를 삭제합니다.

    매개변수:
    - request: 세션 헤더를 포함한 요청 객체

    반환값:
    - dict: 삭제 결과 메시지
    """
    try:
        user_id = request.headers.get('Session-UUID')
        if not user_id:
            raise HTTPException(status_code=400, detail="사용자 ID가 없습니다.")
        logging.info("User ID: %s", user_id)
        await delete_all_temp_data(user_id)
        return {"message": "모든 임시 파일이 삭제되었습니다."}
        
    except Exception as e:
        error_message = f"임시 파일 삭제 중 오류 발생: {str(e)}"
        logger.exception(error_message)
        raise HTTPException(status_code=500, detail=error_message)