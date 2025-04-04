import asyncio
import logging
import os
from fastapi import APIRouter, HTTPException, Request, logger
from fastapi.responses import FileResponse, StreamingResponse
from service.service import delete_all_temp_data, process_project_zipping
from service.service import generate_and_execute_cypherQuery
from service.service import generate_spring_boot_project


router = APIRouter()
logger = logging.getLogger(__name__)  


# 역할: 전달받은 파일들을 분석하여 Neo4j 사이퍼 쿼리를 생성하고 실행합니다
#
# 매개변수:
#   - request: 분석할 파일 정보가 담긴 요청 객체 (fileInfos: [{fileName, objectName}, ...])
#
# 반환값: 
#   - StreamingResponse: Neo4j 그래프 데이터 스트림
@router.post("/cypherQuery/")
async def understand_data(request: Request):    
    try:
        # * 사용자 ID 추출
        user_id = request.headers.get('Session-UUID')
        if not user_id:
            raise HTTPException(status_code=400, detail="사용자 ID가 없습니다.")

        # * OpenAI API 키 추출
        api_key = request.headers.get('Anthropic-Api-Key')
        if not api_key:
            raise HTTPException(status_code=400, detail="Anthropic API 키가 없습니다.")

        # * 파일 정보 추출  
        file_data = await request.json()
        if not file_data:
            raise HTTPException(status_code=400, detail="파일 정보가 없습니다.")


        # * 파일 이름 추출
        file_names = [(item['fileName'], item['objectName']) for item in file_data['fileInfos']]
        logging.info("User ID: %s, File Infos: %s", user_id, file_names)
        

        # * Cypher 쿼리 생성 및 실행(Understanding)
        return StreamingResponse(generate_and_execute_cypherQuery(file_names, user_id, api_key))
    
    except Exception as e:
        error_message = f"Understanding 처리 중 오류 발생: {str(e)}"
        logger.exception(error_message)
        raise HTTPException(status_code=500, detail=error_message)


# 역할: 스토어드 프로시저를 스프링 부트 프로젝트로 변환합니다
#
# 매개변수: 
#   - request: 변환할 파일 이름 정보가 담긴 요청 객체 (fileInfos: [{fileName, objectName}, ...])
#
# 반환값: 
#   - StreamingResponse: 변환 진행 상태 메시지 스트림
@router.post("/springBoot/")
async def covnert_spring_project(request: Request):

    try:
        # * 사용자 ID 추출
        user_id = request.headers.get('Session-UUID')
        if not user_id:
            raise HTTPException(status_code=400, detail="사용자 ID가 없습니다.")
    
        # * OpenAI API 키 추출
        api_key = request.headers.get('Anthropic-Api-Key')
        if not api_key:
            raise HTTPException(status_code=400, detail="Anthropic API 키가 없습니다.")

        # * 요청 객체에서 파일 이름 정보 추출 (filename, objectName)
        file_data = await request.json()
        logging.info("Received File Info for Convert Spring Boot: %s", file_data)
        

        # * 파일 이름과 패키지 이름을 튜플로 추출
        file_names = [(item['fileName'], item['objectName']) for item in file_data['fileInfos']]
        if not file_names:
            raise HTTPException(status_code=400, detail="파일 정보가 없습니다.")


        # * 스프링 부트 프로젝트 생성 시작
        return StreamingResponse(generate_spring_boot_project(file_names, user_id, api_key), media_type="text/plain")
    
    except Exception as e:
        error_message = f"스프링 부트 프로젝트 생성 도중 오류 발생: {str(e)}"
        logger.exception(error_message)
        raise HTTPException(status_code=500, detail=error_message)



 
# 역할: 생성된 스프링 부트 프로젝트를 ZIP 파일로 압축하여 다운로드를 제공합니다
# 매개변수: 없음
# 반환값: 
#   - FileResponse: 압축된 프로젝트 파일
@router.post("/downloadJava/")
async def download_spring_project(request: Request):
    try:
        # * 사용자 ID 추출
        user_id = request.headers.get('Session-UUID')
        if not user_id:
            raise HTTPException(status_code=400, detail="사용자 ID가 없습니다.")
    
    
        # * 환경에 따라 저장 경로 설정
        if os.getenv('DOCKER_COMPOSE_CONTEXT'):
            base_dir = os.getenv('DOCKER_COMPOSE_CONTEXT')
        else:
            base_dir = os.path.dirname(os.getcwd())
        target_path = os.path.join(base_dir, 'target', 'java', user_id)
        zipfile_dir = os.path.join(base_dir, 'data', user_id, 'zipfile')
        

        # * 디렉토리 존재 여부 확인 및 생성
        if not os.path.exists(zipfile_dir):
            os.makedirs(zipfile_dir)


        # * 압축 파일 경로
        output_zip_path = os.path.join(zipfile_dir, 'project.zip')
        

        # * 프로젝트 압축
        await process_project_zipping(target_path, output_zip_path)


        return FileResponse(
            path=output_zip_path, 
            filename="project.zip", 
            media_type='application/octet-stream'
        )
    
    except Exception as e:
        error_message = f"스프링 부트 프로젝트를 Zip 파일로 압축하는데 실패했습니다: {str(e)}"
        logger.exception(error_message)
        raise HTTPException(status_code=500, detail=error_message)
    


# 역할: 생성된 모든 임시 파일과 디렉토리를 정리합니다
# 매개변수: 없음
# 반환값: 
#   - dict: 삭제 완료 메시지가 포함된 딕셔너리
@router.delete("/deleteAll/")
async def delete_all_data(request: Request):
    try:
        # * 사용자 ID 추출
        user_id = request.headers.get('Session-UUID')
        if not user_id:
            raise HTTPException(status_code=400, detail="사용자 ID가 없습니다.")
        

        # * 임시 파일 삭제
        logging.info("User ID: %s", user_id)
        await delete_all_temp_data(user_id)
        return {"message": "모든 임시 파일이 삭제되었습니다."}
        
    except Exception as e:
        error_message = f"임시 파일 삭제 중 오류 발생: {str(e)}"
        logger.exception(error_message)
        raise HTTPException(status_code=500, detail=error_message)