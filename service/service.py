import asyncio
import json
import logging
import shutil
import zipfile
import aiofiles
import os
from convert.create_main import start_main_processing
from convert.create_pomxml import start_pomxml_processing
from convert.create_properties import start_APLproperties_processing
from convert.create_repository import start_repository_processing
from convert.create_entity import start_entity_processing
from convert.create_service_preprocessing import start_service_preprocessing
from convert.create_service_postprocessing import create_service_class_file, start_service_postprocessing 
from convert.create_service_skeleton import start_service_skeleton_processing
from prompt.understand_ddl import understand_ddl
from understand.neo4j_connection import Neo4jConnection
from understand.analysis import analysis
from prompt.java2deths_prompt import convert_2deths_java
from util.exception import AddLineNumError, ConvertingError, Java2dethsError, LLMCallError, Neo4jError, ProcessResultError


BASE_DIR = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
PLSQL_DIR = os.path.join(BASE_DIR, "src")
ANALYSIS_DIR = os.path.join(BASE_DIR, "analysis")
DDL_DIR = os.path.join(BASE_DIR, "ddl")
os.makedirs(PLSQL_DIR, exist_ok=True)
os.makedirs(ANALYSIS_DIR, exist_ok=True)
os.makedirs(DDL_DIR, exist_ok=True)


# 역할: PL/SQL 코드의 각 라인에 번호를 추가하여 코드 추적과 디버깅을 용이하게 합니다.
# 매개변수: 
#   - plsql : 원본 PL/SQL 코드 (라인 단위 리스트)
# 반환값: 
#   - numbered_plsql : 각 라인 앞에 번호가 추가된 PL/SQL 코드
def add_line_numbers(plsql):
    try: 
        # * 각 라인에 번호를 추가합니다.
        numbered_lines = [f"{index + 1}: {line}" for index, line in enumerate(plsql)]
        numbered_plsql = "".join(numbered_lines)
        return numbered_plsql
    except Exception:
        err_msg = "전달된 스토어드 프로시저 코드에 라인번호를 추가하는 도중 문제가 발생했습니다."
        logging.error(err_msg, exc_info=False)
        raise AddLineNumError(err_msg)



# 역할: Neo4j에서 노드와 관계를 조회하여 그래프 데이터를 스트림 형태로 반환합니다.
#      코드 분석 결과를 실시간으로 시각화하기 위한 데이터를 제공합니다.
# 매개변수:
#   - file_names : 분석할 파일 이름과 객체 이름 튜플의 리스트
# 반환값: 
#   - 스트림 : 그래프 데이터 (노드, 관계, 분석 진행률 등)
# TODO 어떤 파일을 understanding 중인지 표시가 필요, ddl 처리 또한 표시 필요 
async def generate_and_execute_cypherQuery(file_names):
    connection = Neo4jConnection()
    receive_queue = asyncio.Queue()
    send_queue = asyncio.Queue()

    try:
        # * 각 패키지 및 프로시저에 대한 understanding 시작
        for file_name, object_name in file_names:
            plsql_file_path = os.path.join(PLSQL_DIR, file_name)
            base_name = os.path.splitext(file_name)[0]
            antlr_file_path = os.path.join(ANALYSIS_DIR, f"{base_name}.json")
            ddl_file_name = file_name.replace('TPX_', 'TPJ_')
            ddl_file_path = os.path.join(DDL_DIR, ddl_file_name)
            has_ddl_info = False

            # * DDL 파일 존재 확인 및 처리
            if os.path.exists(ddl_file_path):
                ddl_start = {"type": "DDL", "MESSAGE" : "START DDL PROCESSING", "file": ddl_file_name}
                yield json.dumps(ddl_start).encode('utf-8') + b"send_stream"
                logging.info(f"DDL 파일 처리 시작: {ddl_file_name}")
                ddl_results = await process_ddl_and_table_nodes(ddl_file_path, connection, object_name)  # DDL 파일 처리
                has_ddl_info = True

            # * 스토어드 프로시저 파일과, ANTLR 구문 분석 파일 읽기 작업을 병렬로 처리
            async with aiofiles.open(antlr_file_path, 'r', encoding='utf-8') as antlr_file, aiofiles.open(plsql_file_path, 'r', encoding='utf-8') as plsql_file:
                antlr_data, plsql_content = await asyncio.gather(antlr_file.read(), plsql_file.readlines())

                # * PLSQL, Antlr 데이터 전처리
                antlr_data = json.loads(antlr_data)
                last_line = len(plsql_content)
                plsql_content = add_line_numbers(plsql_content)

            # * 사이퍼쿼리를 생성 및 실행을 처리하는 메서드
            async def process_analysis_code():
                while True:
                    analysis_result = await receive_queue.get()
                    logging.info(f"Analysis Event Received for file: {file_name}")
                    if analysis_result.get('type') == 'end_analysis':
                        logging.info(f"Understanding Completed for {file_name}")
                        break
                    
                    elif analysis_result.get('type') == 'error':
                        logging.info(f"Understanding Failed for {file_name}")
                        break


                    # * 사이퍼쿼리 분석 과정에서 전달된 이벤트에서 각종 정보를 추출합니다 
                    cypher_queries = analysis_result.get('query_data', [])
                    next_analysis_line = analysis_result['line_number']
                    analysis_progress = int((next_analysis_line / last_line) * 100)


                    # * 전달된 사이퍼쿼리를 실행하여, 노드와 관계를 생성하고, 그래프 객체형태로 가져옵니다
                    await connection.execute_queries(cypher_queries)
                    graph_result = await connection.execute_query_and_return_graph()


                    # * 그래프 객체, 다음 분석될 라인 번호, 분석 진행 상태를 묶어서 스트림 형태로 전달할 수 있게 처리합니다
                    stream_data = {"graph": graph_result, "line_number": next_analysis_line, "analysis_progress": analysis_progress, "current_file": file_name}
                    encoded_stream_data = json.dumps(stream_data).encode('utf-8') + b"send_stream"
                    await send_queue.put({'type': 'process_completed'})
                    logging.info(f"Send Response for {file_name}")
                    yield encoded_stream_data

            # * understanding 과정을 비동기 태스크로 실행하고, 데이터 스트림 생성하여 전달
            analysis_task = asyncio.create_task(analysis(antlr_data, plsql_content, receive_queue, send_queue, last_line, object_name, ddl_results, has_ddl_info))
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


# 역할: DDL 파일을 읽어서 테이블 구조를 분석하고 Neo4j 그래프 데이터베이스에 저장합니다
# 매개변수:
#   - ddl_file_path: DDL 파일의 경로
#   - connection: Neo4j 데이터베이스 연결 객체
#   - object_name: 프로시저 이름
# 반환값:
#   - ddl_result: 분석된 테이블 구조 정보 (테이블명, 컬럼, 키 정보 등)
async def process_ddl_and_table_nodes(ddl_file_path, connection: Neo4jConnection, object_name):
    
    try:
        async with aiofiles.open(ddl_file_path, 'r', encoding='utf-8') as ddl_file:
            ddl_content = await ddl_file.read()
            ddl_result = understand_ddl(ddl_content)
            cypher_queries = []
            
            for table in ddl_result['analysis']:

                # * 테이블의 기본 정보 추출 (이름, 컬럼, 키 정보)
                table_info = table['table']
                columns = table['columns']
                keys = table['keys']
                

                # * 컬럼 정보를 Neo4j 속성으로 변환 (컬럼명: 데이터타입:설명)
                column_props = {
                    col['name']: {
                        'type': col['type'],
                        'nullable': col['nullable']
                    }
                    for col in columns
                }

                
                # * 테이블의 메타 정보를 Neo4j 노드 속성으로 구성
                props = {
                    'name': table_info['name'],
                    'primary_keys': ','.join(key for key in keys['primary']),
                    'foreign_keys': ','.join(fk['column'] for fk in keys['foreign']),
                    'reference_tables': ','.join(
                        f"{fk['references']['table']}.{fk['references']['column']}"
                        for fk in keys['foreign']
                    ),
                    'object_name': object_name,
                    'columns': json.dumps(column_props)  # column_props를 JSON 문자열로 변환
                }
                            
                                
                # * Neo4j 테이블 노드 생성 쿼리 구성
                props_str = ', '.join(f"`{k}`: '{v}'" for k, v in props.items())
                query = f"CREATE (t:Table {{{props_str}}})"
                cypher_queries.append(query)
            
            # * 생성된 모든 테이블 노드 쿼리를 실행
            await connection.execute_queries(cypher_queries)
            return ddl_result
            
    except Exception:
        err_msg = f"DDL 파일 처리 중 오류가 발생했습니다"
        logging.error(err_msg, exc_info=False)
        raise ProcessResultError(err_msg)
    

# 역할: 특정 노드를 중심으로 2단계 깊이까지의 연관 노드와 관계를 조회합니다.
#      선택된 노드의 주변 컨텍스트를 파악하기 위한 서브그래프를 제공합니다.
# 매개변수: 
#   - node_info : 중심이 되는 노드의 식별 정보
# 반환값: 
#   - graph_object_result : 2단계 깊이까지의 서브그래프 데이터
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
        logging.error(err_msg, exc_info=False)
        raise Java2dethsError(err_msg)
    finally:
        await connection.close()


# 역할: 사이퍼 쿼리와 사용자 요구사항을 기반으로 Java 코드를 생성합니다.
#      생성된 코드는 실시간으로 스트리밍됩니다.
# 매개변수:
#   - cypher_query : Neo4j 사이퍼 쿼리
#   - previous_history : 이전 코드 생성 히스토리
#   - requirements_chat : 사용자의 추가 요구사항
# 반환값: 
#   - 스트림 : 생성된 Java 코드
async def generate_simple_java_code(cypher_query=None, previous_history=None, requirements_chat=None):
    try:
        # * 사이퍼 쿼리, 요구사항, 이전 히스토리를 바탕으로 간단한 자바 코드 생성 프로세스를 실행합니다
        async for java_code in convert_2deths_java(cypher_query, previous_history, requirements_chat):
            yield java_code
        yield "END_OF_STREAM"
        
    except LLMCallError:
        yield "stream-error"
    except Exception:
        err_msg = "2단계 깊이 기준으로 전환된 자바 코드를 스트림으로 전달하는 도중 오류가 발생했습니다."
        logging.error(err_msg, exc_info=False)
        yield "stream-error" 


# 역할: PL/SQL 파일 이름을 Java 네이밍 컨벤션에 맞게 변환합니다.
#      파스칼 케이스와 소문자 형식으로 변환된 이름을 제공합니다.
# 매개변수: 
#   - sp_fileName : 원본 PL/SQL 파일 이름
# 반환값: 
#   - pascal_file_name : 파스칼 케이스로 변환된 이름
#   - lower_file_name : 소문자로 변환된 이름
async def transform_file_name(sp_fileName):
    try:
        # * 파일 이름에서 _를 제거하고, 각각의 표기법으로 전환합니다.
        words = sp_fileName.split('_')
        pascal_file_name = ''.join(x.title() for x in words)
        lower_file_name = sp_fileName.replace('_', '').lower()
        return pascal_file_name, lower_file_name
    except Exception:
        err_msg = "스토어드 프로시저 파일 이름을 파스칼, 소문자로 구성된 이름으로 변환 도중 오류가 발생했습니다."
        logging.error(err_msg, exc_info=False)
        raise OSError(err_msg)



# 역할: PL/SQL 프로시저를 스프링 부트 프로젝트로 변환하는 전체 프로세스를 관리합니다.
#      엔티티, 리포지토리, 서비스 등 각 계층의 변환을 순차적으로 진행합니다.
# 매개변수: 
#   - file_names : 변환할 파일 이름과 객체 이름 튜플의 리스트
# 반환값: 
#   - 스트림 : 각 변환 단계의 진행 상태 메시지
async def generate_spring_boot_project(file_names):
    try:
        for file_name, object_name in file_names:
            merge_method_code = ""

            yield f"Start converting {object_name}\n"

            # * 1 단계 : 엔티티 클래스 생성
            entity_name_list = await start_entity_processing(object_name) 
            yield f"{file_name}-Step1 completed\n"
            
            # * 2 단계 : 리포지토리 인터페이스 생성
            jpa_method_list, global_variables = await start_repository_processing(object_name) 
            yield f"{file_name}-Step2 completed\n"
            
            # * 3 단계 : 서비스 스켈레톤 생성
            service_creation_info, service_skeleton, service_class_name = await start_service_skeleton_processing(entity_name_list, object_name, global_variables)
            yield f"{file_name}-Step3 completed\n"

            # * 4 단계 : 각 프로시저별 서비스 생성
            for service_data in service_creation_info:
                await start_service_preprocessing(
                    service_data['service_method_skeleton'],
                    service_data['command_class_variable'],
                    service_data['procedure_name'],
                    jpa_method_list, 
                    object_name
                )
                merge_method_code = await start_service_postprocessing(
                    service_data['method_skeleton_code'],
                    service_data['procedure_name'],
                    object_name,
                    merge_method_code
                )
            await create_service_class_file(service_skeleton, service_class_name, merge_method_code)            
            yield f"{file_name}-Step4 completed\n"

        # * 5 단계 : pom.xml 생성
        await start_pomxml_processing()
        yield f"{file_name}-Step5 completed\n"
        
        # * 6 단계 : application.properties 생성
        await start_APLproperties_processing()
        yield f"{file_name}-Step6 completed\n"

        # * 7 단계 : StartApplication.java 생성
        await start_main_processing()
        yield f"{file_name}-Step7 completed\n"
        yield f"Completed Converting {file_name}.\n\n"

        yield "All files have been converted successfully.\n"

    except (ConvertingError, Neo4jError):
        yield "convert-error"
    except Exception:
        err_msg = "스프링 부트 프로젝트로 전환하는 도중 오류가 발생했습니다."
        logging.error(err_msg, exc_info=False)
        yield "convert-error"


# 역할: 생성된 스프링 부트 프로젝트를 ZIP 파일로 압축합니다.
#      프로젝트의 전체 구조를 유지하면서 압축 파일을 생성합니다.
# 매개변수: 
#   - source_directory : 압축할 프로젝트 디렉토리 경로
#   - output_zip_path : 생성될 ZIP 파일 경로
# 반환값: 없음
async def process_project_zipping(source_directory, output_zip_path):
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
        logging.error(err_msg, exc_info=False)
        raise OSError(err_msg)
    

# 역할: 임시 생성된 모든 파일과 Neo4j 데이터를 정리합니다.
#      다음 변환 작업을 위해 작업 환경을 초기화합니다.
# 매개변수: 
#   - delete_paths : 삭제할 디렉토리 경로들이 담긴 딕셔너리
#       - docker 환경: {'java_dir': 경로, 'zip_dir': 경로}
#       - 로컬 환경: {'target_dir': 경로, 'zip_dir': 경로}
async def delete_all_temp_data(delete_paths: dict):
    try:
        neo4j = Neo4jConnection()
        for dir_path in delete_paths.values():
            if os.path.exists(dir_path):
                shutil.rmtree(dir_path)
                os.makedirs(dir_path)
                logging.info(f"디렉토리 삭제 및 재생성 완료: {dir_path}")
        
        delete_query = ["MATCH (n) DETACH DELETE n"] 
        await neo4j.execute_queries(delete_query)
        logging.info(f"Neo4J 데이터 초기화 완료")
        
    except Exception as e:
        logging.exception(f"파일 삭제 및 그래프 데이터 삭제 중 오류 발생: {str(e)}")
        raise OSError("임시 파일 삭제 및 그래프 데이터 삭제 중 오류가 발생했습니다.")