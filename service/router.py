import asyncio
import logging
import os
from fastapi import File, UploadFile, Form
from fastapi import APIRouter, HTTPException, Request
from fastapi.responses import FileResponse, StreamingResponse
from service.service import save_file_to_disk, process_zip_file, transform_fileName
from service.service import generate_and_execute_cypherQuery
from service.service import generate_two_depth_match
from service.service import generate_simple_java
from service.service import generate_spring_boot_project


router = APIRouter()


# 역할: Antlr 서버에서 전달된 분석 결과를 받아 파일로 저장하고, 저장된 파일을 사용하여 사이퍼 쿼리를 생성 및 실행하는 함수
# 매개변수:
#   antlr_file: Antlr 서버에서 전달된 구문 분석 결과 파일
#   plsql_File: Antlr 서버에서 전달된 PL/SQL 파일
#   sp_fileName: 스토어드 프로시저 파일의 이름
# 반환값: 
#   - 스트림 : 그래프를 그리기 위한 데이터 모음
@router.post("/cypherQuery/")
async def understand_data(antlr_file: UploadFile = File(...), plsql_file: UploadFile = File(...), sp_fileName: str = Form(...)):
    
    try:
        # * Antlr 서버에서 전달된 두 파일(스토어드 프로시저, ANTLR 구문 분석 결과)을 비동기적으로 읽어들임
        antlr_data, sql_data = await asyncio.gather(antlr_file.read(), plsql_file.read())
        logging.info(f"Received File: {sp_fileName}")


        # * 읽어들인 데이터를 디스크에 저장하는 함수를 호출하고, 저장된 파일 이름과 마지막 라인 번호를 반환받음
        saved_filename, last_line = await save_file_to_disk(antlr_data, sql_data, sp_fileName)
        
    except Exception:
        raise HTTPException(status_code=500, detail="Understanding에 실패했습니다.")


    # * 사이퍼쿼리를 생성하고 실행하는 함수를 호출하여, 결과를 스트림으로 전달
    return StreamingResponse(generate_and_execute_cypherQuery(saved_filename, last_line))



# 역할: 선택된 테이블과 2단계 깊이 기준의 노드들을 가져와서 자바 코드로 변환한 뒤, 스트림하는 함수
# 매개변수: 
#   - node_info : 선택된 테이블 노드의 정보
# 반환값: 
#   - 스트림 : 자바 코드
@router.post("/java/")
async def convert_java(request: Request):

    try:
        # * 요청으로부터 테이블 노드 정보를 JSON 형태로 추출합니다
        node_info = await request.json()
        logging.info("Received Node Info for Java: %s", node_info)  


        # * 테이블 노드 정보를 기반으로 2단계 깊이 조회를 위한 사이퍼 쿼리를 생성하고 실행합니다
        cypher_query_for_java = await generate_two_depth_match(node_info)


    except Exception:
        raise HTTPException(status_code=500, detail="테이블 노드 기준으로 자바 코드 생성에 실패했습니다.")


    # * 사이퍼쿼리의 결과를 바탕으로 자바 코드로 변환하고, 그 결과를 스트리밍 응답으로 반환합니다
    return StreamingResponse(generate_simple_java(cypher_query_for_java))
    


# 역할: 채팅(요구사항)과 이전 히스토리를 기반으로 자바 코드를 다시 생성하여, 스트리밍하는 함수
# 매개변수: 
#   - userInput : 채팅(요구사항)
#   - prevHistory : 이전 히스토리
# 반환값: 생성된 자바 코드를 스트리밍하는 응답 객체
@router.post("/chat/")
async def receive_chat(request: Request):

    try:
        # * 요청으로부터 채팅 정보를 JSON 형태로 추출합니다
        chat_info = await request.json()
        logging.info("Received chat Info for Java:", chat_info)


        # * 채팅 정보에서 사용자의 입력과 이전 히스토리를 추출합니다
        userInput = chat_info['userInput']
        prevHistory = chat_info['prevHistory']

    except Exception:
        raise HTTPException(status_code=500, detail="자바 코드 생성을 위해 전달된 데이터가 잘못되었습니다.")


    # * 전달된 정보를 바탕으로 자바 코드로 변환하여, 스트리밍으로 응답합니다
    return StreamingResponse(generate_simple_java(None, prevHistory, userInput))



# 역할: 스토어드 프로시저를 스프링부트 기반의 자바 프로젝트로 전환하여, 각 단계의 완료를 스트리밍하는 함수
# 매개변수: 
#   - fileName : 스토어드 프로시저 파일 이름
# 반환값: 
#   - 스트림 : 각 단계의 완료 메시지
@router.post("/springBoot/")
async def covnert_spring_project(request: Request):

    try:
        # * 요청으로부터 스토어드 프로시저 파일 정보를 JSON 형태로 추출합니다.
        fileInfo = await request.json()
        logging.info("Received File Name for Convert Spring Boot:", fileInfo)


        # * 스토어드 프로시저 파일 정보에서 파일 이름을 추출하고 확장자를 제거합니다.
        fileName = fileInfo['fileName']
        original_name, _ = os.path.splitext(fileName)

    except Exception:
        raise HTTPException(status_code=500, detail="스프링 부트 프로젝트 생성을 위해 전달된 데이터가 잘못되었습니다.")


    # * 스프링부트 전환 과정이 시작되며, 각 단계마다 결과를 스트리밍으로 응답합니다
    return StreamingResponse(generate_spring_boot_project(original_name), media_type="text/plain")


 
# 역할: 생성된 스프링부트 프로젝트를 Zip 파일로 압축하여, 사용자가 다운로드 받을 수 있게하는 함수
# 매개변수: 
#   - fileName : 스토어드 프로시저 파일 이름
# 반환값: 
#   - 스프링부트 기반의 자바 프로젝트(Zip)
@router.post("/downloadJava/")
async def download_spring_project(request: Request):
    
    try:
        # * 요청으로부터 스토어드 프로시저 파일 정보를 JSON 형태로 추출합니다.
        fileInfo = await request.json()
        logging.info("Received File Name for Download Spring Boot:", fileInfo)


        # * 스토어드 프로시저 파일 정보에서 파일 이름을 추출하고 확장자를 제거합니다.
        project_folder_name = fileInfo['fileName']
        original_name, _ = os.path.splitext(project_folder_name)
        _, lower_file_name = await transform_fileName(original_name)
        
        # * ZIP 파일로 만들기 위한 자바 프로젝트 파일들을 읽을 경로와, ZIP 파일로 저장할 경로 설정후 압축
        output_zip_path = os.path.join('data', 'zipfile', f'{original_name}.zip')
        input_zip_path = os.path.join('data', 'java', f'{lower_file_name}')
        await process_zip_file(input_zip_path, output_zip_path)
        return FileResponse(path=output_zip_path, filename=f"{original_name}.zip", media_type='application/octet-stream')

    except Exception:
        raise HTTPException(status_code=500, detail="스프링 부트 프로젝트를 Zip 파일로 압축하는데 실패했습니다.")
