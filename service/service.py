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
from convert.create_service_preprocessing import start_service_Preprocessing
from convert.create_service_postprocessing import start_service_Postprocessing 
from convert.create_service_skeleton import start_service_skeleton_processing
from understand.neo4j_connection import Neo4jConnection
from understand.analysis import analysis
from prompt.java2deths_prompt import convert_2deths_java
from util.exception import AddLineNumError, ConvertingError, Java2dethsError, LLMCallError, Neo4jError


BASE_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__))) 
SAVE_PLSQL_DIR = os.path.join(BASE_DIR, "data", "plsql")
SAVE_ANTLR_DIR = os.path.join(BASE_DIR, "data", "analysis")
os.makedirs(SAVE_PLSQL_DIR, exist_ok=True)
os.makedirs(SAVE_ANTLR_DIR, exist_ok=True)


# 역할: Antlr 서버에서 분석된 결과를 파일 형태로 저장하고, 파일 이름을 반환합니다.
# 매개변수: 
#   - antlr_data: Antlr 서버에서 분석된 결과 내용, 
#   - sql_data: 원본 스토어드 프로시저 파일 내용, 
#   - file_name: 저장할 파일의 기본 이름
# 반환값: 
#   - base_sp_file_name: 저장된 스토어드 프로시저  파일이름(확장자 제거)
#   - last_line: 스토어드 프로시저 파일 내용의 마지막 라인 번호
async def save_file_to_disk(antlr_data, plsql_data, sp_file_name):
    try: 
        # * 전달된 PLSQL 및 ANTLR 분석 내용을 파일로 저장하기 위한 준비
        base_sp_file_name = os.path.splitext(sp_file_name)[0]
        plplsql_file_path = os.path.join(SAVE_PLSQL_DIR, f"{base_sp_file_name}.txt")
        antlr_file_path = os.path.join(SAVE_ANTLR_DIR, f"{base_sp_file_name}.json")


        # * 전달된 PLSQL에 줄 번호 추가
        contents, last_line = add_line_numbers(plsql_data.decode('utf-8'))


        # * PLSQL 파일과 분석 결과 파일을 비동기적으로 저장
        async with aiofiles.open(plplsql_file_path, "wb") as plsql_file, aiofiles.open(antlr_file_path, "w") as antlr_file:
            await asyncio.gather(plsql_file.write(contents.encode()), antlr_file.write(antlr_data.decode('utf-8')))

        logging.info("\nSuccessed File Saved\n")
        return base_sp_file_name, last_line
    
    except AddLineNumError:
        raise
    except Exception:
        err_msg = "Antlr에서 전달된 스토어드 프로시저 파일과 분석 결과 파일을 저장하는 도중 문제가 발생"
        logging.exception(err_msg)
        raise OSError(err_msg)


# 역할: 스토어드 프로시저의 각 라인에 줄 번호를 추가합니다.
# 매개변수: 
#   - contents : 스토어드 프로시저 파일 내용
# 반환값: 
#   - numbered_contents : 라인 번호가 추가된 스토어드 프로시저 파일 내용 
#   - last_line: 스토어드 프로시저 파일 내용의 마지막 라인번호
def add_line_numbers(contents):
    try: 
        # * 각 라인에 번호를 추가합니다.
        lines = contents.splitlines()
        numbered_lines = [f"{index + 1}: {line}" for index, line in enumerate(lines)]
        last_line = len(lines) 
        numbered_contents = "\n".join(numbered_lines)
        return numbered_contents, last_line
    except Exception:
        err_msg = "전달된 스토어드 프로시저 코드에 라인번호를 추가하는 도중 문제가 발생했습니다."
        logging.exception(err_msg)
        raise AddLineNumError(err_msg)


# 역할: 사이퍼 쿼리를 생성 및 실행하고, 그 결과를 스트림 형식으로 반환
# 매개변수:
#    - sp_file_name: 스토어드 프로시저 파일 이름(확장자 제거)
#    - last_line: 스토어드 프로시저 파일의 내용의 마지막 라인 번호
# 반환값: 
#   - 스트림 : 그래프를 그리기 위한 데이터들
async def generate_and_execute_cypherQuery(base_sp_file_name, last_line):
    connection = Neo4jConnection()
    plsql_file_path = os.path.join(SAVE_PLSQL_DIR, f"{base_sp_file_name}.txt")
    antlr_file_path = os.path.join(SAVE_ANTLR_DIR, f"{base_sp_file_name}.json")
    receive_queue = asyncio.Queue()
    send_queue = asyncio.Queue()

    try:
        # * 스토어드 프로시저 파일과, ANTLR 구문 분석 파일 읽기 작업을 병렬로 처리
        async with aiofiles.open(antlr_file_path, 'r', encoding='utf-8') as antlr_file, aiofiles.open(plsql_file_path, 'r', encoding='utf-8') as plsql_file:
            antlr_data, plsql_content = await asyncio.gather(antlr_file.read(), plsql_file.readlines())
            antlr_data = json.loads(antlr_data)
        

        # * 사이퍼쿼리를 생성 및 실행을 처리하는 메서드
        async def process_analysis_code():
            while True:
                analysis_result = await receive_queue.get()
                logging.info("Analysis Event Received!")
                if analysis_result.get('type') == 'end_analysis':
                    logging.info("Understanding Completed")
                    break
                
                elif analysis_result.get('type') == 'error':
                    logging.info("Understanding Failed")
                    break


                # * 사이퍼쿼리 분석 과정에서 전달된 이벤트에서 각종 정보를 추출합니다 
                cypher_queries = analysis_result.get('query_data', [])
                next_analysis_line = analysis_result['line_number']
                analysis_progress = int((next_analysis_line / last_line) * 100)


                # * 전달된 사이퍼쿼리를 실행하여, 노드와 관계를 생성하고, 그래프 객체형태로 가져옵니다
                await connection.execute_queries(cypher_queries)
                graph_result = await connection.execute_query_and_return_graph()


                # * 그래프 객체, 다음 분석될 라인 번호, 분석 진행 상태를 묶어서 스트림 형태로 전달할 수 있게 처리합니다
                stream_data = {"graph": graph_result, "line_number": next_analysis_line, "analysis_progress": analysis_progress}
                encoded_stream_data = json.dumps(stream_data).encode('utf-8') + b"send_stream"
                await send_queue.put({'type': 'process_completed'})
                logging.info("Send Response")
                yield encoded_stream_data

        # * understanding 과정을 비동기 태스크로 실행하고, 데이터 스트림 생성하여 전달
        analysis_task = asyncio.create_task(analysis(antlr_data, plsql_content, receive_queue, send_queue, last_line))
        async for stream_data_chunk in process_analysis_code():
            yield stream_data_chunk
        await analysis_task
        yield "end_of_stream" # * 스트림 종료 신호


    except Exception:
        error_msg = "사이퍼쿼리를 생성 및 실행하고 스트림으로 반환하는 과정에서 오류가 발생했습니다"
        logging.exception(error_msg)
        yield json.dumps({"error": error_msg}).encode('utf-8') + b"send_stream"
    finally:
        await connection.close()


# 역할: 노드 ID를 기반으로 두 단계 깊이의 노드를 조회하여 그래프 객체로 반환합니다.
# 매개변수: 
#   - node_info : 노드 정보
# 반환값: 
#   - graph_object_result : 그래프 객체
async def generate_two_depth_match(node_info):
    try:
        connection = Neo4jConnection()
        

        # * 주어진 노드 ID로부터 두 단계 깊이의 노드와 관계를 조회하는 사이퍼쿼리를 준비합니다
        query = f"""
        MATCH path = (n {{name: '{node_info}'}})-[*1..2]-(related)
        RETURN n, relationships(path), related
        """

        # * 사이퍼 쿼리 실행하여, 결과를 그래프 객체로 가져옵니다
        graph_object_result = await connection.execute_query_and_return_graph(query)
        return graph_object_result
    
    except Neo4jError:
        raise
    except Exception:
        err_msg = "2단계 깊이 기준 노드를 조회하는 사이퍼쿼리를 준비 도중 오류가 발생했습니다."
        logging.exception(err_msg)
        raise Java2dethsError(err_msg)
    finally:
        await connection.close()


# 역할: 사이퍼 쿼리, 요구사항, 이전 히스토리를 바탕으로 자바 코드를 생성하여, 스트림 형태로 반환
# 매개변수:
#   - cypher_query: 사이퍼쿼리
#   - previous_history: 이전 히스토리
#   - requirements_chat: 요구사항
# 반환값: 
#   - 스트림 : 자바 코드
async def generate_simple_java(cypher_query=None, previous_history=None, requirements_chat=None):
    try:
        # * 사이퍼 쿼리, 요구사항, 이전 히스토리를 바탕으로 자바 코드 생성 프로세스를 실행합니다
        async for java_code in convert_2deths_java(cypher_query, previous_history, requirements_chat):
            yield java_code
        yield "END_OF_STREAM"
        
    except LLMCallError:
        yield "stream-error"
    except Exception:
        err_msg = "2단계 깊이 기준으로 전환된 자바 코드를 스트림으로 전달하는 도중 오류가 발생했습니다."
        logging.exception(err_msg)
        yield "stream-error" 


# 역할 : 전달받은 스토어드 프로시저 파일 이름을 각각의 표기법에 맞게 변환하는 함수
# 매개변수 : 
#   - sp_fileName : 스토어드 프로시저 파일의 이름
# 반환값 : 
#   - pascal_file_name : 파스칼 표기법으로 변환된 파일 이름
#   - lower_file_name : 소문자로 변환된 파일
async def transform_fileName(sp_fileName):
    try:
        # * 파일 이름에서 _를 제거하고, 각각의 표기법으로 전환합니다.
        words = sp_fileName.split('_')
        pascal_file_name = ''.join(x.title() for x in words)
        lower_file_name = sp_fileName.replace('_', '').lower()
        return pascal_file_name, lower_file_name
    except Exception:
        err_msg = "스토어드 프로시저 파일 이름을 파스칼, 소문자로 구성된 이름으로 변환 도중 오류가 발생했습니다."
        logging.exception(err_msg)
        raise OSError(err_msg)


# 역할: 스프링 부트 프로젝트 생성을 위한 단계별 변환 과정을 수행하는 비동기 제너레이터 함수
# 매개변수: 
#   - fileName : 스프링 부트 프로젝트로 전환될 스토어드 프로시저의 파일 이름.
# 반환값: 
#   - 스트림 : 각 변환 단계의 완료 메시지
async def generate_spring_boot_project(fileName):
    
    try:
        # * 프로젝트 이름을 각 표기법에 맞게 변환합니다.
        pascal_file_name, lower_file_name = await transform_fileName(fileName)

        # * 1 단계 : 엔티티 클래스 생성
        table_node_data, entity_name_list = await start_entity_processing(lower_file_name) 
        yield f"Step1 completed"
        
        # * 2 단계 : 리포지토리 인터페이스 생성
        jpa_method_list, repository_interface_names = await start_repository_processing(table_node_data, lower_file_name) 
        yield f"Step2 completed\n"
        
        # * 3 단계 : 서비스 스켈레톤 생성
        service_skeleton_code, procedure_variable_list, service_class_name, summarzied_service_skeleton = await start_service_skeleton_processing(lower_file_name, entity_name_list, repository_interface_names)
        yield f"Step3 completed\n"
        
        # * 4 단계 : 서비스 생성
        await start_service_Preprocessing(summarzied_service_skeleton, jpa_method_list, procedure_variable_list)
        await start_service_Postprocessing(lower_file_name, service_skeleton_code, service_class_name)
        yield f"Step4 completed\n"
        
        # * 5 단계 : pom.xml 생성
        await start_pomxml_processing(lower_file_name)
        yield f"Step5 completed\n"
        
        # * 6 단계 : application.properties 생성
        await start_APLproperties_processing(lower_file_name)
        yield f"Step6 completed\n"

        # * 7 단계 : StartApplication.java 생성
        await start_main_processing(lower_file_name, pascal_file_name)
        yield f"Step7 completed\n"
        yield "Completed Converting.\n"

    except (ConvertingError, Neo4jError):
        yield "convert-error"
    except Exception:
        err_msg = "스프링 부트 프로젝트로 전환하는 도중 오류가 발생했습니다."
        logging.exception(err_msg)
        yield "convert-error" 


# 역할: 전환된 스프링 기반의 자바 프로젝트를 zip으로 압축하는 함수
# 매개변수: 
#   - source_directory : zip으로 압축할 대상이 있는 파일 경로
#   - output_zip_path : 압축된 zip 파일이 저장되는 경로
# 반환값: 없음
async def process_zip_file(source_directory, output_zip_path):
    try:
        # * zipfile 모듈을 사용하여 ZIP 파일 생성
        os.makedirs(os.path.dirname(output_zip_path), exist_ok=True)
        logging.info(f"Zipping contents of {source_directory} to {output_zip_path}")
        with zipfile.ZipFile(output_zip_path, 'w', zipfile.ZIP_DEFLATED) as zipf:
            for current_path, _, files in os.walk(source_directory):
                for file in files:
                    file_path = os.path.join(current_path, file)
                    arcname = os.path.relpath(file_path, start=source_directory)
                    zipf.write(file_path, arcname)
        logging.info("Zipping completed successfully.")

    except Exception:
        err_msg = "스프링부트 프로젝트를 Zip으로 압축하는 도중 문제가 발생했습니다."
        logging.exception(err_msg)
        raise OSError(err_msg)