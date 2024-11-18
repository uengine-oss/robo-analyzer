import logging
import os
import shutil
from fastapi import APIRouter, HTTPException, Request
from fastapi.responses import FileResponse, StreamingResponse
from service.service import delete_all_temp_files, process_zip_file, transform_fileName
from service.service import generate_and_execute_cypherQuery
from service.service import generate_two_depth_match
from service.service import generate_simple_java
from service.service import generate_spring_boot_project


router = APIRouter()


# 역할: 전달된 파일 이름으로, 분석할 파일을 찾아서 사이퍼 쿼리를 생성 및 실행
# 매개변수:
#   request: 전달된 파일이름
# 반환값: 
#   - 스트림 : 그래프를 그리기 위한 데이터 모음
@router.post("/cypherQuery/")
async def understand_data(request: Request):    
    try:
        file_data = await request.json()
        logging.info("Received Files Info: %s", file_data)

        file_names = [(item['fileName'], item['objectName']) for item in file_data['fileInfos']]
        
        if not file_names:
            raise HTTPException(status_code=400, detail="파일 정보가 없습니다.")
        
    except Exception:
        raise HTTPException(status_code=500, detail="Understanding에 실패했습니다.")

    return StreamingResponse(generate_and_execute_cypherQuery(file_names))



# 역할: 선택된 테이블과 2단계 깊이 기준의 노드들을 가져와서 자바 코드로 변환한 뒤, 스트림하는 함수
# 매개변수: 
#   - node_info : 선택된 테이블 노드의 정보
# 반환값: 
#   - 스트림 : 자바 코드
# TODO FRONT에서 값이 전달이 제대로 안되고 있습니다.
@router.post("/java/")
async def convert_java(request: Request):

    try:
        node_info = await request.json()
        logging.info("Received Node Info for Java: %s", node_info)  
        cypher_query_for_java = await generate_two_depth_match(node_info)

    except Exception:
        raise HTTPException(status_code=500, detail="테이블 노드 기준으로 자바 코드 생성에 실패했습니다.")

    return StreamingResponse(generate_simple_java(cypher_query_for_java))
    


# 역할: 채팅(요구사항)과 이전 히스토리를 기반으로 자바 코드를 다시 생성하여, 스트리밍하는 함수
# 매개변수: 
#   - userInput : 채팅(요구사항)
#   - prevHistory : 이전 히스토리
# 반환값: 생성된 자바 코드를 스트리밍하는 응답 객체
# TODO FRONT에서 값이 전달이 안되고 있습니다.
@router.post("/chat/")
async def receive_chat(request: Request):

    try:
        chat_info = await request.json()
        logging.info("Received chat Info for Java:", chat_info)

        userInput = chat_info['userInput']
        prevHistory = chat_info['prevHistory']

    except Exception:
        raise HTTPException(status_code=500, detail="자바 코드 생성을 위해 전달된 데이터가 잘못되었습니다.")

    return StreamingResponse(generate_simple_java(None, prevHistory, userInput))



# 역할: 스토어드 프로시저를 스프링부트 기반의 자바 프로젝트로 전환하여, 각 단계의 완료를 스트리밍하는 함수
# 매개변수: 
#   - fileInfos : converting할 스토어드 프로시저 파일 정보들
# 반환값: 
#   - 스트림 : 각 단계의 완료 메시지
@router.post("/springBoot/")
async def covnert_spring_project(request: Request):

    try:

        file_data = await request.json()
        logging.info("Received File Info for Convert Spring Boot: %s", file_data)
        file_names = [(item['fileName'], item['objectName']) for item in file_data['fileInfos']]
        
        if not file_names:
            raise HTTPException(status_code=400, detail="파일 정보가 없습니다.")

    except Exception:
        raise HTTPException(status_code=500, detail="스프링 부트 프로젝트 생성을 위해 전달된 데이터가 잘못되었습니다.")

    return StreamingResponse(generate_spring_boot_project(file_names), media_type="text/plain")


 
# 역할: 생성된 스프링부트 프로젝트를 Zip 파일로 압축하여, 사용자가 다운로드 받을 수 있게하는 함수
# 매개변수: 없음
# 반환값: 
#   - 스프링부트 기반의 자바 프로젝트(Zip)
@router.post("/downloadJava/")
async def download_spring_project():
    try:
        parent_dir = os.path.dirname(os.getcwd())
        
        target_path = os.path.join(parent_dir, 'target')
        zipfile_dir = os.path.join(parent_dir, 'zipfile')
        
        if not os.path.exists(zipfile_dir):
            os.makedirs(zipfile_dir)
            
        output_zip_path = os.path.join(zipfile_dir, 'project.zip')
        
        await process_zip_file(target_path, output_zip_path)
        
        return FileResponse(
            path=output_zip_path, 
            filename="project.zip", 
            media_type='application/octet-stream'
        )
    except Exception:
        raise HTTPException(status_code=500, detail="스프링 부트 프로젝트를 Zip 파일로 압축하는데 실패했습니다.")
    


# 역할: 임시 저장된 모든 파일을 삭제하는 함수
# 매개변수: 없음
# 반환값: 삭제 결과 메시지
@router.delete("/deleteAll/")
async def delete_all_files():
    try:
        docker_context = os.getenv('DOCKER_COMPOSE_CONTEXT')
        
        if docker_context:
            paths = {
                'target_dir': os.path.join(docker_context, 'target'),
                'zip_dir': os.path.join(docker_context, 'zipfile')
            }
        else:
            parent_dir = os.path.dirname(os.getcwd())
            paths = {
                'target_dir': os.path.join(parent_dir, 'target'),
                'zip_dir': os.path.join(parent_dir, 'zipfile')
            }

        await delete_all_temp_files(paths)
        return {"message": "모든 임시 파일이 삭제되었습니다."}
        
    except Exception as e:
        logging.error(f"파일 삭제 중 오류 발생: {str(e)}")
        raise HTTPException(status_code=500, detail="임시 파일 삭제 중 오류가 발생했습니다.")