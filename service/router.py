import logging
import os
import shutil
from fastapi import APIRouter, HTTPException, Request
from fastapi.responses import FileResponse, StreamingResponse
from service.service import process_zip_file, transform_fileName
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

        file_names = [(item['fileName'], item['objectName']) for item in file_data['fileNames']]
        
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
#   - fileName : 스토어드 프로시저 파일 이름
# 반환값: 
#   - 스트림 : 각 단계의 완료 메시지
@router.post("/springBoot/")
async def covnert_spring_project(request: Request):

    try:
        fileInfo = await request.json()
        logging.info("Received File Name for Convert Spring Boot:", fileInfo)

        fileName = fileInfo['fileName']
        original_name, _ = os.path.splitext(fileName)

    except Exception:
        raise HTTPException(status_code=500, detail="스프링 부트 프로젝트 생성을 위해 전달된 데이터가 잘못되었습니다.")

    return StreamingResponse(generate_spring_boot_project(original_name), media_type="text/plain")


 
# 역할: 생성된 스프링부트 프로젝트를 Zip 파일로 압축하여, 사용자가 다운로드 받을 수 있게하는 함수
# 매개변수: 
#   - fileName : 스토어드 프로시저 파일 이름
# 반환값: 
#   - 스프링부트 기반의 자바 프로젝트(Zip)
@router.post("/downloadJava/")
async def download_spring_project(request: Request):
    
    try:
        fileInfo = await request.json()
        logging.info("Received File Name for Download Spring Boot:", fileInfo)

        project_folder_name = fileInfo['fileName']
        original_name, _ = os.path.splitext(project_folder_name)
        _, lower_file_name = await transform_fileName(original_name)
        
        output_zip_path = os.path.join('data', 'zipfile', f'{original_name}.zip')
        input_zip_path = os.path.join('data', 'java', f'{lower_file_name}')
        await process_zip_file(input_zip_path, output_zip_path)
        return FileResponse(path=output_zip_path, filename=f"{original_name}.zip", media_type='application/octet-stream')

    except Exception:
        raise HTTPException(status_code=500, detail="스프링 부트 프로젝트를 Zip 파일로 압축하는데 실패했습니다.")
    
    
@router.post("/showJavaResult/")
async def show_java_result(request: Request):
    try:

        target_folder = os.path.join(os.getcwd(), 'target')
        if not os.path.exists(target_folder):
            os.makedirs(target_folder)
            logging.info("Created /target folder")
        # 요청으로부터 스토어드 프로시저 파일 정보를 JSON 형태로 추출합니다.
        fileInfo = await request.json()
        logging.info("Received File Name for Show Java Result: %s", fileInfo)

        # 스토어드 프로시저 파일 정보에서 파일 이름을 추출하고 확장자를 제거합니다.
        project_folder_name = fileInfo['fileName']
        original_name, _ = os.path.splitext(project_folder_name)
        _, lower_file_name = await transform_fileName(original_name)
        
        # 자바 프로젝트 파일들이 있는 경로
        source_path = os.path.join('data', 'java', f'{lower_file_name}')
        
        # /target 폴더 하위에 추가할 경로
        target_path = os.path.join('target', f'{lower_file_name}')
        
        # 소스 경로가 디렉토리인지 확인
        if not os.path.isdir(source_path):
            raise HTTPException(status_code=400, detail=f"{source_path} is not a directory")

        # /target 폴더로 복사
        shutil.copytree(source_path, target_path, dirs_exist_ok=True)

        return {"message": "Java project added to /target successfully"}

    except Exception as e:
        logging.error("Error showing Java result: %s", e)
        raise HTTPException(status_code=500, detail="스프링 부트 프로젝트 결과를 표시하는데 실패했습니다.")


# @router.get("/getFiles/")
# async def get_files():
#     try:
#         # Check if src folder exists, if not create it
#         src_directory = os.path.join(os.getcwd(), 'src')
#         if not os.path.exists(src_directory):
#             os.makedirs(src_directory)
#             logging.info("Created /src folder")
#         # * src 디렉토리 경로 설정
#         src_directory = os.path.join(os.getcwd(), 'src')
        
#         # * 파일 목록과 정보를 저장할 리스트 초기화
#         files_info = []
        
#         # * src 디렉토리 내의 파일 목록을 가져옴
#         for file_name in os.listdir(src_directory):
#             file_path = os.path.join(src_directory, file_name)
            
#             # * 파일인지 확인하고 정보를 읽음
#             if os.path.isfile(file_path):
#                 file_info = {
#                     "name": file_name,
#                     "content": open(file_path, 'r', encoding='utf-8').read()
#                 }
#                 files_info.append(file_info)
        
#         # * 파일 목록과 정보를 반환
#         return {"files": files_info}
    
#     except Exception as e:
#         raise HTTPException(status_code=500, detail=f"Failed to retrieve files: {str(e)}")