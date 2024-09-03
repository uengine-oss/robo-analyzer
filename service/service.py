import asyncio
import json
import logging
import zipfile
import aiofiles
import os
from convert.create_main import start_main_processing
from convert.create_pomxml import start_pomxml_processing
from convert.create_properties import start_APLproperties_processing
from convert.create_repository import start_repository_processing
from convert.create_entity import start_entity_processing
from convert.create_service_preprocessing import start_service_processing
from convert.create_service_postprocessing import merge_service_code 
from convert.create_service_skeleton import start_service_skeleton_processing
from cypher.neo4j_connection import Neo4jConnection
from cypher.analysis import analysis
from cypher.cypher_prompt.convert_java_prompt import process_convert_to_java

BASE_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__))) 
SAVE_SQL_DIR = os.path.join(BASE_DIR, "cypher", "sql")
SAVE_ANALYSIS_DIR = os.path.join(BASE_DIR, "cypher", "analysis")
ANALYSIS_RESULT_DIR = os.path.join(BASE_DIR, "cyper")
os.makedirs(SAVE_SQL_DIR, exist_ok=True)
os.makedirs(SAVE_ANALYSIS_DIR, exist_ok=True)


# 역할: Antlr 서버에서 분석된 결과를 파일 형태로 저장하고, 파일 이름을 반환합니다.
# 매개변수: 
#   antlr_data: Antlr 서버에서 분석된 결과 내용, 
#   sql_data: 원본 스토어드 프로시저 파일 내용, 
#   file_name: 저장할 파일의 기본 이름
# 반환값: 저장된 파일의 이름, 파일 내용의 마지막 라인 번호(파일 길이)
async def save_file_to_disk(antlr_data, sql_data, file_name):
    try: 
        # * 전달된 PLSQL 및 ANTLR 분석 내용을 파일로 저장하기 위한 준비
        fileName = os.path.splitext(file_name)[0]
        sql_file_path = os.path.join(SAVE_SQL_DIR, f"{fileName}.txt")
        analysis_file_path = os.path.join(SAVE_ANALYSIS_DIR, f"{fileName}.json")

        # * 전달된 PLSQL에 줄 번호 추가
        contents, last_line = add_line_numbers(sql_data.decode('utf-8'))
        
        # * PLSQL 파일과 분석 결과 파일을 비동기적으로 저장
        async with aiofiles.open(sql_file_path, "wb") as sql_file, aiofiles.open(analysis_file_path, "w") as analysis_file:
            await asyncio.gather(sql_file.write(contents.encode()), analysis_file.write(antlr_data.decode('utf-8')))

        logging.info("File saved")
        return fileName, last_line

    except Exception as e:
        logging.exception(f"Unexpected error with file {fileName}: {str(e)}")


# 역할: 원본 파일 내용(스토어드 프로시저)의 각 라인에 줄 번호를 추가합니다.
# 매개변수: 
#      - contents : 원본  스토어드 프로시저 파일 내용
# 반환값: 줄 번호가 추가된 원본 파일 내용(스토어드 프로시저), 파일 내용의 마지막 라인번호
def add_line_numbers(contents):
    lines = contents.splitlines()
    numbered_lines = [f"{index + 1}: {line}" for index, line in enumerate(lines)]
    last_line = len(lines) 
    return "\n".join(numbered_lines), last_line


# 역할: 저장된 스토어드 프로시저 파일과 분석 파일을 사용하여 사이퍼 쿼리를 생성 및 실행하고, 그 결과를 스트림 형식으로 반환합니다.
# 매개변수:
#    - file_name: 처리할 스토어드 프로시저 파일의 기본 이름
#    - last_line: 스토어드 프로시저 파일의 마지막 라인 번호
# 반환값: 사이퍼 쿼리 실행 결과를 스트림 형식으로 반환
async def generate_and_execute_cypher(file_name, last_line):
    connection = Neo4jConnection()
    sql_file_path = os.path.join(SAVE_SQL_DIR, f"{file_name}.txt")
    analysis_file_path = os.path.join(SAVE_ANALYSIS_DIR, f"{file_name}.json")
    output_file_path = os.path.join(SAVE_SQL_DIR, 'cypher_queries.txt')
    receive_queue = asyncio.Queue()
    send_queue = asyncio.Queue()
    
    try:
        # * 스토어드 프로시저 파일과, ANTLR 구문 분석 파일 읽기 작업을 병렬로 처리
        async with aiofiles.open(analysis_file_path, 'r', encoding='utf-8') as analysis_file, aiofiles.open(sql_file_path, 'r', encoding='utf-8') as sql_file:
            analysis_data, sql_content = await asyncio.gather(analysis_file.read(), sql_file.readlines())
            analysis_data = json.loads(analysis_data)
        

        # * 사이퍼 쿼리를 생성 및 실행을 처리하는 메서드
        async def create_cypher_and_execute():
            while True:
                analysis_result = await receive_queue.get()
                logging.info("Event Received!")
                if analysis_result.get('type') == 'end_analysis':
                    logging.info("Analysis Done")
                    break


                # * 사이퍼쿼리 분석 과정에서 전달된 이벤트에서 각종 정보를 추출합니다 
                cypher_queries = analysis_result['query_data']
                next_analysis_line = analysis_result['line_number']
                analysis_progress = int((next_analysis_line / last_line) * 100)


                # * 사이퍼쿼리 내용을 파일에 쓰기
                async with aiofiles.open(output_file_path, 'a', encoding='utf-8') as file:
                    await file.write("\n".join(cypher_queries))
                    await file.write("\n분석완료\n")
                logging.info("Cypher queries have been saved to cypher_queries.txt")
                

                # * 생성된(전달된) 사이퍼쿼리를 실행하여, 노드와 관계를 생성하고, 그래프 객체형태로 가져옵니다
                await connection.execute_queries(cypher_queries)
                graph_result = await connection.execute_query_and_return_graph()


                # * 그래프 객체, 다음 분석될 라인 번호, 분석 진행 상태를 묶어서 스트림 형태로 전달할 수 있게 처리합니다
                final_data = {"graph": graph_result, "line_number": next_analysis_line, "analysis_progress": analysis_progress}
                encoded_data = json.dumps(final_data).encode('utf-8') + b"send_stream"
                await send_queue.put({'type': 'process_completed'})
                logging.info("Send Response")
                yield encoded_data
                # await asyncio.sleep(1)  # 1초 지연 추가 (db에 캐쉬된 상태면, 그래프가 너무 빨리 생성되어서 추가한 것) 


        # * 스토어드 프로시저을 분석(사이퍼쿼리 생성) 작업을 비동기 태스크로 실행하고, 데이터 스트림 생성합니다
        analysis_task = asyncio.create_task(analysis(analysis_data, sql_content, receive_queue, send_queue, last_line))
        async for graph_data in create_cypher_and_execute(): 
            yield graph_data
        await asyncio.gather(analysis_task)
        yield "end_of_stream" # * 스트림 종료 신호

    except Exception:
        logging.exception("During prepare to generate cypher queries unexpected error occurred")
    finally:
        await connection.close()


# 역할: 주어진 노드 ID를 기반으로 두 단계 깊이의 관계를 가진 노드와 관계를 조회하여 그래프 객체로 반환합니다.
# 매개변수: 
#      - node_info : 노드 정보를 담고 있는 객체, 반드시 'id' 키를 포함해야 합니다.
# 반환값: 두 단계 깊이의 사이퍼 쿼리 실행 결과로 얻은 그래프 객체
async def generate_two_depth_match(node_info):
    try:
        connection = Neo4jConnection()
        

        # * 주어진 노드 ID로부터 두 단계 깊이의 노드와 관계를 조회하는 사이퍼쿼리를 준비합니다
        query = f"""
        MATCH path = (n {{name: '{node_info}'}})-[*1..2]-(related)
        RETURN n, relationships(path), related
        """

        # * 사이퍼 쿼리 실행하여, 결과를 가져옵니다
        graph_object_result = await connection.execute_query_and_return_graph(query)
        return graph_object_result
    
    except Exception:
        logging.exception("Error prepare execute 2 depth cypher queries")
    finally:
        await connection.close()


# 역할: 사이퍼 쿼리 결과를 바탕으로 자바 코드를 생성하고, 이를 스트림 형태로 반환합니다.
# 매개변수:
#   cypher_query: 사이퍼 쿼리 문자열
#   previous_history: 이전에 처리된 자바 코드의 히스토리
#   requirements_chat: 요구사항을 담은 채팅 데이터
# 반환값: 생성된 자바 코드를 스트림 형태로 반환
async def generate_java_from_content(cypher_query=None, previous_history=None, requirements_chat=None):
    try:
        # * 사이퍼 쿼리와 관련 데이터를 바탕으로 자바 코드 생성 프로세스를 실행합니다
        async for java_code in process_convert_to_java(cypher_query, previous_history, requirements_chat):
            yield java_code
        yield "END_OF_STREAM"
    except Exception as e:
        logging.exception("Error transferring Java code to stream")
        yield {"error": str(e)}


# 역할 : 전달받은 이름을 각각의 표기법에 맞게 변환하는 함수
# 매개변수 : 
#   - sp_fileName : 스토어드 프로시저 파일의 이름
# 반환값 : 표기법에 따른 이름들
async def convert_fileName_for_java(sp_fileName):
    components = sp_fileName.split('_')
    pascal_case = ''.join(x.title() for x in components)
    lower_case = sp_fileName.replace('_', '').lower()
    return pascal_case, lower_case


# 역할: 스프링 부트 프로젝트 생성을 위한 단계별 변환 과정을 수행하는 비동기 제너레이터 함수입니다.
# 매개변수: 
#      - fileName : 스프링 부트 프로젝트로 전환될 스토어드 프로시저의 파일 이름.
# 반환값: 각 변환 단계의 완료 메시지를 스트리밍 형태로 반환합니다.
async def create_spring_boot_project(fileName):
    
    try:
        # * 프로젝트 이름을 각 표기법에 맞게 변환합니다.
        pascal_case, lower_case = await convert_fileName_for_java(fileName)

        # * 1 단계 : 엔티티 클래스 생성
        table_node_data, entity_name_list = await start_entity_processing(lower_case) 
        yield f"Step1 completed"
        
        # * 2 단계 : 리포지토리 인터페이스 생성
        jpa_method_list = await start_repository_processing(table_node_data, lower_case) 
        yield f"Step2 completed\n"
        
        # * 3 단계 : 서비스 스켈레톤 생성
        service_skeleton_code, procedure_variable_list = await start_service_skeleton_processing(lower_case, entity_name_list)
        yield f"Step3 completed\n"
        
        # * 4 단계 : 서비스 생성
        await start_service_processing(service_skeleton_code, jpa_method_list, procedure_variable_list)
        await merge_service_code(lower_case, service_skeleton_code, pascal_case)
        yield f"Step4 completed\n"
        
        # * 5 단계 : pom.xml 생성
        await start_pomxml_processing(lower_case)
        yield f"Step5 completed\n"
        
        # * 6 단계 : application.properties 생성
        await start_APLproperties_processing(lower_case)
        yield f"Step6 completed\n"

        # * 7 단계 : StartApplication.java 생성
        await start_main_processing(lower_case, pascal_case)
        yield f"Step7 completed\n"
        yield "Completed Converting.\n"

    except Exception as e:
        logging.exception("Error During Create Spring Boot Project")
        yield {"error": str(e)}


# 역할: 전환된 스프링 기반의 자바 프로젝트를 zip으로 압축합니다.
# 매개변수: 
#      - source_dir : zip으로 압축할 파일 경로
#      - output_zip : 결과인 zip 파일이 저장되는 경로
# 반환값: 없음
async def zip_directory(source_dir, output_zip):
    try:
        # * zipfile 모듈을 사용하여 ZIP 파일 생성
        os.makedirs(os.path.dirname(output_zip), exist_ok=True)
        logging.info(f"Zipping contents of {source_dir} to {output_zip}")
        with zipfile.ZipFile(output_zip, 'w', zipfile.ZIP_DEFLATED) as zipf:
            for root, _, files in os.walk(source_dir):
                for file in files:
                    file_path = os.path.join(root, file)
                    arcname = os.path.relpath(file_path, start=source_dir)
                    zipf.write(file_path, arcname)
        logging.info("Zipping completed successfully.")
    except Exception as e:
        logging.error(f"Failed to zip directory {source_dir} to {output_zip}: {str(e)}")
        raise