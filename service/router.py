import asyncio
import logging
import os
from fastapi import File, UploadFile, Form
from fastapi import APIRouter, HTTPException, Request
from fastapi.responses import FileResponse, JSONResponse, StreamingResponse
from service.service import save_file_to_disk, zip_directory
from service.service import generate_and_execute_cypherQuery
from service.service import generate_two_depth_match
from service.service import generate_java_from_content
from service.service import create_spring_boot_project


router = APIRouter()

# TODO Pydantic을 써서 전달된 데이터 검증 추가

# 역할: Antlr 서버에서 전달된 분석 결과를 받아 파일로 저장하고, 저장된 파일을 사용하여 사이퍼 쿼리를 생성 및 실행합니다.
# 매개변수:
#   analysis_file: Antlr 서버에서 전달된 구문 분석 완료된 파일
#   plsql_File: Antlr 서버에서 전달된 PL/SQL 파일
#   fileName: 저장할 파일의 이름
# 반환값: 사이퍼 쿼리 실행 결과를 그래프 객체로 스트리밍하는 응답 객체
@router.post("/cypherQuery/")
async def upload_data(analysis_file: UploadFile = File(...), plsql_file: UploadFile = File(...), fileName: str = Form(...)):
    

    # * Antlr 서버에서 전달된 두 파일(스토어드 프로시저, ANTLR 구문 분석 결과)을 비동기적으로 읽어들임
    antlr_data, sql_data = await asyncio.gather(analysis_file.read(), plsql_file.read())
    logging.info(f"Received File: {fileName}")


    # * 읽어들인 데이터를 디스크에 저장하는 함수를 호출하고, 저장된 파일 이름과 마지막 라인 번호를 반환받음
    saved_filename, last_line = await save_file_to_disk(antlr_data, sql_data, fileName)
    

    # * 파일 저장에 실패한 경우, HTTP 500 에러 반환
    if saved_filename is None:
        raise HTTPException(status_code=500, detail="Failed to save the file.")
    

    # * 사이퍼쿼리를 생성하고 실행하는 함수를 호출하여, 결과를 스트림으로 전달
    return StreamingResponse(generate_and_execute_cypherQuery(saved_filename, last_line))



# 역할: 선택된 테이블과 2단계 깊이로 연결된 노드들을 가져와서 자바 코드로 변환한 뒤, 스트리밍 응답으로 반환합니다.
# 매개변수: 선택된 테이블 노드 정보
# 반환값: 생성된 자바 코드를 스트리밍하는 응답 객체
@router.post("/Java/")
async def convert_to_java(request: Request):

    try:
        # * 요청으로부터 테이블 노드 정보를 JSON 형태로 추출합니다
        node_info = await request.json()
        logging.info("Received Node Info for Java: %s", node_info)  

        # * 테이블 노드 정보를 기반으로 2단계 깊이 조회를 위한 사이퍼 쿼리를 생성하고 실행합니다
        cypher_query_for_java = await generate_two_depth_match(node_info)


        # * 사이퍼 쿼리 생성 실패 시, HTTP 500 에러 반환
        if cypher_query_for_java is None:
            raise HTTPException(status_code=500, detail="Failed to generate cypher query from node information.")
        
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


    # * 사이퍼쿼리의 결과를 바탕으로 자바 코드로 변환하고, 그 결과를 스트리밍 응답으로 반환합니다
    return StreamingResponse(generate_java_from_content(cypher_query_for_java))
    


# 역할: 클라이언트로부터 받은 채팅과 이전 히스토리를 기반으로 자바 코드로 변환하여 스트리밍 응답으로 반환합니다.
# 매개변수: 채팅과 이전 히스토리를 담은 객체
# 반환값: 생성된 자바 코드를 스트리밍하는 응답 객체
@router.post("/Chat/")
async def receive_to_chat(request: Request):


    # * 요청으로부터 채팅 정보를 JSON 형태로 추출합니다
    chat_info = await request.json()
    logging.info("Received chat Info for Java:", chat_info)


    # * 채팅 정보에서 사용자의 입력과 이전 히스토리를 추출합니다
    userInput = chat_info['userInput']
    prevHistory = chat_info['prevHistory']
    logging.info(userInput)
    logging.info(prevHistory)

    # * 사용자 입력 또는 이전 히스토리가 누락된 경우, HTTP 500 에러 반환
    if userInput is None or prevHistory is None:
        raise HTTPException(status_code=500, detail="User input or previous history is missing.")


    # * 전달된 정보를 바탕으로 자바 코드로 변환하여, 스트리밍으로 응답합니다
    return StreamingResponse(generate_java_from_content(None, prevHistory, userInput))



# 역할: 스토어드 프로시저를 스프링부트 기반의 자바 프로젝트로 전환
# 매개변수: 스토어드 프로시저 파일 이름
# 반환값: 생성된 스프링부트 파일
@router.post("/springBoot/")
async def covnert_to_spring_project(request: Request):


    # * 요청으로부터 스토어드 프로시저 파일 정보를 JSON 형태로 추출합니다
    fileInfo = await request.json()
    logging.info("Received File Name for Convert Spring Boot:", fileInfo)


    # * 스토어드 프로시저 파일 정보에서 파일 이름을 추출합니다
    fileName = fileInfo['fileName']


    # * 스토어드 프로시저 파일 이름이 누락된 경우, HTTP 500 에러 반환
    if fileName is None:
        raise HTTPException(status_code=500, detail="fileName is missing.")


    # * 스토어드 프로시저 파일 이름에서 확장자를 제거합니다.
    original_name, _ = os.path.splitext(fileName)


    # * 스프링부트 전환 과정이 시작되며, 각 단계마다 결과를 스트리밍으로 응답합니다
    return StreamingResponse(create_spring_boot_project(original_name), media_type="text/plain")


 
# 역할: 스토어드 프로시저에서 자바로 변환된 프로젝트를 다운로드 받을 수 있게 합니다
# 매개변수: 스토어드 프로시저 파일 이름
# 반환값: 스프링부트 기반의 자바 프로젝트
@router.post("/downloadJava/")
async def download_spring_project(request: Request):
    
    
    # * 요청으로부터 스토어드 프로시저 파일 정보를 JSON 형태로 추출합니다
    fileInfo = await request.json()
    logging.info("Received File Name for Download Spring Boot:", fileInfo)


    # * 스토어드 프로시저 파일 정보에서 파일 이름을 추출합니다
    project_folder_name = fileInfo['fileName']


    # * 스토어드 프로시저 파일 이름이 누락된 경우, HTTP 500 에러 반환
    if project_folder_name is None:
        raise HTTPException(status_code=500, detail="projectFolderName is missing.")
    

    # * 스토어드 프로시저 파일 이름에서 확장자를 제거합니다.
    original_name, _ = os.path.splitext(project_folder_name)
    
    
    # * ZIP 파일로 만들기 위한 자바 프로젝트 파일들을 읽을 경로와, ZIP 파일로 저장할 경로 설정
    output_zip_path = os.path.join('convert', 'zipfile', f'{original_name}.zip')
    input_zip_path = os.path.join('convert', 'converting_result', f'{original_name}')


    # * 지정된 경로들로 프로젝트를 ZIP 파일로 압축
    await zip_directory(input_zip_path, output_zip_path)

    
    return FileResponse(path=output_zip_path, filename=f"{original_name}.zip", media_type='application/octet-stream')