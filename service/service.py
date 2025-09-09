import asyncio
import json
import logging
import shutil
from typing import Any, AsyncGenerator
import zipfile
import aiofiles
import os

import httpx
from convert.create_controller import generate_controller_class, start_controller_processing
from convert.create_controller_skeleton import start_controller_skeleton_processing
from convert.create_pomxml import start_pomxml_processing
from convert.create_properties import start_APLproperties_processing
from convert.create_repository import start_repository_processing
from convert.create_entity import start_entity_processing
from convert.create_service_preprocessing import start_service_preprocessing
from convert.create_service_postprocessing import generate_service_class, start_service_postprocessing
from convert.create_service_skeleton import start_service_skeleton_processing
from convert.create_main import start_main_processing
from convert.validate_service_preprocessing import start_validate_service_preprocessing
from prompt.convert_project_name_prompt import generate_project_name_prompt
from prompt.understand_ddl import understand_ddl
from understand.neo4j_connection import Neo4jConnection
from understand.analysis import analysis
from util.exception import ConvertingError, Neo4jError, UnderstandingError, FileProcessingError
from util.utility_tool import add_line_numbers
from util.llm_client import get_llm


# 환경에 따라 저장 경로 설정
if os.getenv('DOCKER_COMPOSE_CONTEXT'):
    BASE_DIR = os.getenv('DOCKER_COMPOSE_CONTEXT')
else:
    BASE_DIR = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))


# 역할: 사용자 ID를 기반으로 디렉토리 경로를 반환합니다.
#
# 매개변수:
#   - user_id: 사용자 ID
#
# 반환값:
#   - 디렉토리 경로 딕셔너리
def get_user_directories(user_id: str):
    user_base = os.path.join(BASE_DIR, 'data', user_id)
    return {
        'plsql': os.path.join(user_base, "src"),
        'analysis': os.path.join(user_base, "analysis"),
        'ddl': os.path.join(user_base, "ddl"),
    }


# 역할: Understanding 과정에서 생성된 사이퍼쿼리를 실행한 결과를 그래프 데이터를 스트림 형태로 반환합니다.
#
# 매개변수:
#   - file_names : 분석할 파일 이름과 객체 이름
#   - user_id : 사용자 ID
#   - api_key : OpenAI API 키
#
# 반환값: 
#   - 스트림 : 그래프 데이터 (노드, 관계, 분석 진행률 등)
async def generate_and_execute_cypherQuery(file_names: list, user_id: str, api_key: str, locale: str) -> AsyncGenerator[Any, None]:
    connection = Neo4jConnection()
    receive_queue = asyncio.Queue()
    send_queue = asyncio.Queue()
    
    dirs = get_user_directories(user_id)

    try:
        # 패키지 별 노드를 가져오기 위한 정보를 추출
        object_names = [name[1] for name in file_names]
        prepare_msg = {"type": "ALARM", "MESSAGE": "Preparing Analysis Data"}
        yield json.dumps(prepare_msg).encode('utf-8') + b"send_stream"

        # 이전에 사용자가 생성한 노드 존재 여부를 확인
        node_exists = await connection.node_exists(user_id, object_names)
        if node_exists:
            already_analyzed = {"type": "ALARM", "MESSAGE": "ALREADY ANALYZED"}
            yield json.dumps(already_analyzed).encode('utf-8') + b"send_stream"
            graph_data = await connection.execute_query_and_return_graph(user_id, object_names)
            stream_data = {"type": "DATA", "graph": graph_data, "analysis_progress": 100}
            yield json.dumps(stream_data).encode('utf-8') + b"send_stream"
            return
        
        # DDL 처리: 여러 파일이 있을 경우 순차 실행 후 결과를 합산, 한 번만 수행
        has_ddl_info = False
        ddl_results = None
        try:
            ddl_dir = dirs['ddl']
            ddl_candidates = [f for f in os.listdir(ddl_dir) if 'ddl' in f.lower()]
        except Exception:
            ddl_candidates = []

        if ddl_candidates:
            aggregated = {"analysis": []}
            for ddl_file_name in ddl_candidates:
                ddl_file_path = os.path.join(dirs['ddl'], ddl_file_name)
                ddl_start = {"type": "ALARM", "MESSAGE": "START DDL PROCESSING", "file": ddl_file_name}
                yield json.dumps(ddl_start).encode('utf-8') + b"send_stream"
                logging.info(f"DDL 파일 처리 시작: {ddl_file_name}")
                try:
                    base_object_name = os.path.splitext(ddl_file_name)[0]
                    one_result = await process_ddl_and_table_nodes(ddl_file_path, connection, base_object_name, user_id, api_key, locale)
                    if one_result and isinstance(one_result, dict):
                        aggregated["analysis"].extend(one_result.get("analysis", []))
                        has_ddl_info = True
                except Exception as _e:
                    logging.error(f"DDL 파일 처리 실패: {ddl_file_name} - {_e}")
                    continue
            ddl_results = aggregated if has_ddl_info else None

        # 각 패키지 및 프로시저에 대한 understanding 시작
        for file_name, object_name in file_names:
            plsql_file_path = os.path.join(dirs['plsql'], file_name)
            base_name = os.path.splitext(file_name)[0]
            analysis_file_path = os.path.join(dirs['analysis'], f"{base_name}.json")
            

            # PLSQL/ANTLR 파일 읽기 및 전처리
            async with aiofiles.open(analysis_file_path, 'r', encoding='utf-8') as antlr_file, \
                     aiofiles.open(plsql_file_path, 'r', encoding='utf-8') as plsql_file:
                antlr_data, plsql_content = await asyncio.gather(antlr_file.read(), plsql_file.readlines())
                antlr_data = json.loads(antlr_data)
                last_line = len(plsql_content)
                plsql_content, _ = add_line_numbers(plsql_content)

            # 분석 처리 함수 정의
            async def process_analysis_code():
                while True:
                    analysis_result = await receive_queue.get()
                    logging.info(f"Analysis Event Received for file: {file_name}")
                    
                    if analysis_result.get('type') == 'end_analysis':
                        logging.info(f"Understanding Completed for {file_name}\n")
                        # 파일 분석이 종료되었으므로 마지막 결과를 포함하여 마지막 라인 번호와 함께 전달
                        graph_result = await connection.execute_query_and_return_graph(user_id, object_names)
                        stream_data = {
                            "type": "DATA", 
                            "graph": graph_result, 
                            "line_number": last_line, 
                            "analysis_progress": 100, 
                            "current_file": object_name
                        }
                        yield json.dumps(stream_data).encode('utf-8') + b"send_stream"
                        break
                    
                    elif analysis_result.get('type') == 'error':
                        logging.info(f"Understanding Failed for {file_name}")
                        break

                    # 이벤트에서 정보 추출
                    cypher_queries = analysis_result.get('query_data', [])
                    next_analysis_line = analysis_result['line_number']
                    analysis_progress = int((next_analysis_line / last_line) * 100)

                    # 사이퍼 쿼리 실행 - 여기서 발생하는 예외는 상위로 전파됨
                    await connection.execute_queries(cypher_queries)
                    graph_result = await connection.execute_query_and_return_graph(user_id, object_names)
                    
                    # 스트림 데이터 생성 및 반환
                    stream_data = {
                        "type": "DATA", 
                        "graph": graph_result, 
                        "line_number": next_analysis_line, 
                        "analysis_progress": analysis_progress, 
                        "current_file": object_name
                    }
                    await send_queue.put({'type': 'process_completed'})
                    logging.info(f"Send Response for {file_name}")
                    yield json.dumps(stream_data).encode('utf-8') + b"send_stream"

            # 태스크 생성 및 실행
            analysis_task = asyncio.create_task(
                analysis(antlr_data, plsql_content, receive_queue, send_queue, 
                         last_line, object_name, ddl_results, has_ddl_info, user_id, api_key, locale)
            )
            
            # 단순화된 예외 처리 - 기본 로직 흐름 유지하면서 예외는 상위로 전파
            async for stream_data_chunk in process_analysis_code():
                yield stream_data_chunk
            
            # 태스크 완료 대기
            await analysis_task

        # 모든 파일 처리가 완료된 후 최종 메시지 전송
        completion_message = {"type": "ALARM", "MESSAGE": "ALL_ANALYSIS_COMPLETED"}
        yield json.dumps(completion_message).encode('utf-8') + b"send_stream"
    
    # 예외 처리부 - 모든 예외는 여기서 처리
    except UnderstandingError as e:
        yield json.dumps({"error": str(e)}).encode('utf-8') + b"send_stream"
    except Exception as e:
        logging.exception(f"사이퍼쿼리 생성/실행 중 오류: {str(e)}")
        yield json.dumps({"error": str(e)}).encode('utf-8') + b"send_stream"
    finally:
        await connection.close()



# 역할: 처리할 DDL 파일을 읽어서 테이블 구조를 분석하고 Neo4j 그래프 데이터베이스에 저장합니다
#
# 매개변수:
#   - ddl_file_path: DDL 파일의 경로
#   - connection: Neo4j 데이터베이스 연결 객체
#   - object_name: 패키지 이름
#   - user_id: 사용자 ID
#
# 반환값:
#   - ddl_result: 분석된 테이블 구조 정보 (테이블명, 컬럼, 키 정보 등)
async def process_ddl_and_table_nodes(ddl_file_path: str, connection: Neo4jConnection, object_name: str, user_id: str, api_key: str, locale: str):
    
    try:
        # * 처리할 DDL 파일 읽기
        async with aiofiles.open(ddl_file_path, 'r', encoding='utf-8') as ddl_file:
            ddl_content = await ddl_file.read()
            ddl_result = understand_ddl(ddl_content, api_key, locale)
            cypher_queries = []
            
            
            # * 테이블 구조 분석 결과를 반복하여 각 테이블에 대한 사이퍼쿼리 생성
            for table in ddl_result['analysis']:

                # * 테이블의 기본 정보 추출 (이름, 컬럼, 키 정보)
                table_info = table['table']
                columns = table['columns']
                keys = table.get('keys', {'primary': [], 'foreign': []})
                primary_list = keys.get('primary', [])
                foreign_list = keys.get('foreign', [])
            
                # * 테이블의 메타 정보를 Neo4j 노드 속성으로 구성
                schema_val = (table_info.get('schema') or '').upper()
                table_name_val = table_info['name'].upper()
                props = {
                    'schema': schema_val,
                    'name': table_name_val,
                    'user_id': user_id,
                    'primary_keys': ','.join(primary_list),
                    'foreign_keys': ','.join([fk.get('column', '') for fk in foreign_list if fk.get('column')]),
                    'reference_tables': ','.join([
                        (
                            (
                                (fk.get('references', {}).get('schema') or '').upper() + '.'
                            ) if fk.get('references', {}).get('schema') else ''
                        )
                        + (fk.get('references', {}).get('table', '') or '').upper()
                        + ('.' + fk.get('references', {}).get('column', '') if fk.get('references', {}).get('column') else '')
                        for fk in foreign_list
                        if fk.get('references', {}).get('table')
                    ]),
                    'object_name': object_name,
                }

                # * 각 컬럼을 "타입:nullable여부" 형태로 저장 (누락 키 안전 처리)
                for col in columns:
                    # 컬럼 이름/타입 정제 및 빈 이름은 무시
                    col_name = (col.get('name') or '').strip()
                    if not col_name:
                        continue
                    col_type = (col.get('type') or '').strip()
                    # Oracle은 명시하지 않으면 NULL 허용이 기본 → 기본값 true
                    nullable_raw = col.get('nullable', True)
                    if isinstance(nullable_raw, bool):
                        nullable_str = 'true' if nullable_raw else 'false'
                    else:
                        text = str(nullable_raw).strip().lower()
                        if text in ('true', 't', '1', 'yes', 'y'):
                            nullable_str = 'true'
                        elif text in ('false', 'f', '0', 'no', 'n'):
                            nullable_str = 'false'
                        else:
                            # 해석 불가 시 기본 true
                            nullable_str = 'true'
                    props[col_name] = f"{col_name}§{col_type}§nullable:{nullable_str}"
        
                # * Neo4j 테이블 노드 생성 쿼리 구성
                props_str = ', '.join(f"`{k}`: '{v}'" for k, v in props.items())
                query = f"CREATE (t:Table {{{props_str}}})"
                cypher_queries.append(query)
            

            # * 생성된 모든 테이블 노드 쿼리를 실행
            await connection.execute_queries(cypher_queries)
            logging.info(f"DDL 파일 처리 완료: {object_name}")
            return ddl_result
    
    except UnderstandingError as e:
        raise
    except Exception as e:
        err_msg = f"DDL 파일 처리 중 오류가 발생했습니다: {str(e)}"
        logging.error(err_msg)
        raise UnderstandingError(err_msg)
    


# 역할: PL/SQL 프로시저를 스프링 부트 프로젝트로 변환하는 전체 프로세스를 관리합니다.
#
# 매개변수: 
#   - file_names : 변환할 파일 이름과 객체 이름 튜플의 리스트
#   - user_id : 사용자 ID
#   - api_key : OpenAI API 키
#
# 반환값: 
#   - 스트림 : 각 변환 단계의 진행 상태 메시지
async def generate_spring_boot_project(file_names: list, user_id: str, api_key: str, locale: str) -> AsyncGenerator[Any, None]:
    try:
        #==================================================================
        # 유틸리티 함수 정의
        #==================================================================
        def create_message(data_type, **kwargs):
            """스트림 메시지를 생성하는 유틸리티 함수"""
            message = {"data_type": data_type, **kwargs}
            return json.dumps(message).encode('utf-8') + b"send_stream"
        
        #==================================================================
        # 1. 프로젝트 이름 생성
        #==================================================================
        project_name = await generate_project_name_prompt(file_names, api_key)
        logging.info(f"프로젝트 이름 생성 완료: {project_name}")
        
        yield create_message("data", file_type="project_name", project_name=project_name)
        
        #==================================================================
        # 2. 엔티티 클래스 생성
        #==================================================================
        yield create_message("message", step=1, content="Generating Entity Class")
        
        entity_result_list = await start_entity_processing(file_names, user_id, api_key, project_name, locale)
        
        for entity in entity_result_list:
            yield create_message(
                "data", 
                file_type="entity_class",
                file_name=f"{entity['entityName']}.java",
                code=entity['entityCode']
            )
            logging.info(f"[디버그]entity: {entity['entityName']} 전달 완료")
        
        yield create_message("Done", step=1)
        
        #==================================================================
        # 3. 리포지토리 인터페이스 생성
        #==================================================================
        yield create_message("message", step=2, content="Generating Repository Interface")
        
        used_query_methods, global_variables, sequence_methods, repository_list = await start_repository_processing(
            file_names, user_id, api_key, project_name, locale
        )
        
        for repo in repository_list:
            yield create_message(
                "data",
                file_type="repository_class",
                file_name=f"{repo['repositoryName']}.java",
                code=repo['code']
            )
            logging.info(f"[디버그]repository: {repo['repositoryName']} 전달 완료")

        
        yield create_message("Done", step=2)
        
        #==================================================================
        # 4. 서비스, 컨트롤러 생성
        #==================================================================
        file_count = len(file_names)
        current_file_index = 0
        logging.info(f"변환할 파일 개수: {file_count}")
        for file_name, object_name in file_names:
            merge_method_code = ""
            merge_controller_method_code = ""
            current_file_index += 1

            yield create_message("message", step=3, 
                content=f"Business Logic Processing")
            logging.info(f"Start converting {object_name}\n")

            #--------------------------------------------------------------
            # 4.1 서비스, 컨트롤러 스켈레톤 생성
            #--------------------------------------------------------------
            yield create_message("message", step=3, content=f"{object_name} - Service Skeleton")
            
            service_creation_info, service_skeleton, service_class_name, exist_command_class, command_class_list = (
                await start_service_skeleton_processing(
                    entity_result_list, object_name, global_variables, user_id, api_key, project_name, locale
                )
            )
            
            controller_skeleton, controller_class_name = await start_controller_skeleton_processing(
                object_name, exist_command_class, project_name
            )

            #--------------------------------------------------------------
            # 4.2 커맨드 클래스 생성 및 전송
            #--------------------------------------------------------------
            yield create_message("message", step=3, content=f"{object_name} - Command Class")
            
            for command_class in command_class_list:
                yield create_message(
                    "data",
                    file_type="command_class",
                    file_name=f"{command_class['commandName']}.java",
                    code=command_class['commandCode']
                )
            
            yield create_message("Done", step=3, file_count=file_count, current_count=current_file_index)

            #--------------------------------------------------------------
            # 4.3 프로시저별 서비스 및 컨트롤러 메서드 생성
            #--------------------------------------------------------------
            yield create_message("message", step=4, content=f"{object_name} - Service Controller Processing")
            
            for service_data in service_creation_info:
                # 서비스 전처리
                variable_nodes, merged_java_code = await start_service_preprocessing(
                    service_data['service_method_skeleton'],
                    service_data['command_class_variable'],
                    service_data['procedure_name'],
                    used_query_methods, 
                    object_name,
                    sequence_methods,
                    user_id,
                    api_key,
                    locale
                )

                # 서비스 검증
                await start_validate_service_preprocessing(
                    variable_nodes,
                    service_data['service_method_skeleton'],
                    service_data['command_class_variable'],
                    service_data['procedure_name'],
                    used_query_methods, 
                    object_name,
                    sequence_methods,
                    user_id,
                    api_key,
                    locale
                )

                # 서비스 후처리
                # 병합된 자바 코드가 이미 준비된 경우, 후처리 스켈레톤 치환만 수행
                if merged_java_code:
                    indented = textwrap.indent(merged_java_code.strip(), '        ')
                    completed_service_code = service_data['method_skeleton_code'].replace("        CodePlaceHolder", "CodePlaceHolder").replace("CodePlaceHolder", indented)
                    merge_method_code = f"{merge_method_code}\n\n{completed_service_code}"
                else:
                    merge_method_code = await start_service_postprocessing(
                        service_data['method_skeleton_code'],
                        service_data['procedure_name'],
                        object_name,
                        merge_method_code,
                        user_id,
                    )

                # 컨트롤러 처리
                merge_controller_method_code = await start_controller_processing(
                    service_data['method_signature'],
                    service_data['procedure_name'],
                    service_data['command_class_variable'],
                    service_data['command_class_name'],
                    service_data['node_type'],
                    merge_controller_method_code,
                    controller_skeleton,
                    object_name,
                    user_id,
                    api_key,
                    project_name,
                    locale
                )

            #--------------------------------------------------------------
            # 4.4 최종 서비스 및 컨트롤러 클래스 생성
            #--------------------------------------------------------------
            service_code = await generate_service_class(
                service_skeleton, service_class_name, merge_method_code, user_id, project_name
            )
            
            controller_code = await generate_controller_class(
                controller_skeleton, controller_class_name, merge_controller_method_code, user_id, project_name
            )

            # 결과 전송
            yield create_message(
                "data",
                file_type="service_class",
                file_name=f"{service_class_name}.java",
                code=service_code
            )

            yield create_message(
                "data",
                file_type="controller_class",
                file_name=f"{controller_class_name}.java",
                code=controller_code
            )
            
        yield create_message("Done", step=4)

        #==================================================================
        # 5. pom.xml 생성
        #==================================================================
        yield create_message("message", step=5, content="Generating pom.xml")
        
        pom_xml_code = await start_pomxml_processing(user_id, project_name)
        
        yield create_message(
            "data",
            file_type="pom",
            file_name="pom.xml",
            code=pom_xml_code
        )
        
        yield create_message("Done", step=5)

        #==================================================================
        # 6. application.properties 생성
        #==================================================================
        yield create_message("message", step=6, content="Generating application.properties")
        
        properties_code = await start_APLproperties_processing(user_id, project_name)
        
        yield create_message(
            "data",
            file_type="properties",
            file_name="application.properties",
            code=properties_code
        )
        
        yield create_message("Done", step=6)

        #==================================================================
        # 7. 메인 어플리케이션 클래스 생성
        #==================================================================
        yield create_message("message", step=7, content="Generating Main Application")
        
        main_code = await start_main_processing(user_id, project_name)
        
        yield create_message(
            "data",
            file_type="main",
            file_name=f"{project_name.capitalize()}Application.java",
            code=main_code
        )
        
        yield create_message("Done", step=7)

    except ConvertingError as e:
        raise
    except Exception as e:
        err_msg = f"스프링 부트 프로젝트로 전환하는 도중 오류가 발생했습니다: {str(e)}"
        logging.error(err_msg)
        raise ConvertingError(err_msg)


# 역할: 생성된 스프링 부트 프로젝트를 ZIP 파일로 압축합니다.
#
# 매개변수: 
#   - source_directory : 압축할 프로젝트 디렉토리 경로
#   - output_zip_path : 생성될 ZIP 파일 경로
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

    except Exception as e:
        err_msg = f"스프링부트 프로젝트를 Zip으로 압축하는 도중 문제가 발생했습니다: {str(e)}"
        logging.error(err_msg)
        raise FileProcessingError(err_msg)
    

# 역할: 임시 생성된 모든 파일과 Neo4j 데이터를 정리합니다.
#
# 매개변수: 
#   - user_id : 사용자 ID   
async def delete_all_temp_data(user_id:str):
    neo4j = Neo4jConnection()
    
    try:
        # * 삭제할 사용자 기본 디렉토리 경로 설정
        user_base_dir = os.path.join(BASE_DIR, 'data', user_id)
        user_target_dir = os.path.join(BASE_DIR, 'target', 'java', user_id)
        
        
        # * 삭제할 디렉토리 목록
        dirs_to_delete = [user_base_dir, user_target_dir]


        # * 디렉토리 삭제 및 재생성
        for dir_path in dirs_to_delete:
            if os.path.exists(dir_path):
                shutil.rmtree(dir_path)
                os.makedirs(dir_path)
                logging.info(f"디렉토리 삭제 및 재생성 완료: {dir_path}")
        
        
        # * Neo4j 데이터 삭제 (해당 사용자의 데이터만 삭제)
        delete_query = [f"MATCH (n {{user_id: '{user_id}'}}) DETACH DELETE n"]
        await neo4j.execute_queries(delete_query)
        logging.info(f"Neo4J 데이터 초기화 완료 - User ID: {user_id}")
    
    except Neo4jError:
        raise
    except Exception as e:
        err_msg = f"파일 삭제 및 그래프 데이터 삭제 중 오류 발생: {str(e)}"
        logging.exception(err_msg)
        raise FileProcessingError(err_msg)


"""
역할: OpenAI 호환 엔드포인트에 대해 API 키 유효성을 간단 호출로 검증합니다.
반환: bool
"""
async def validate_anthropic_api_key(api_key: str) -> bool:
    try:
        # get_llm은 base_url, api_key를 환경변수 또는 인자값으로 사용
        llm = get_llm(max_tokens=8, api_key=api_key)
        # 간단 호출 (LangChain Runnable 인터페이스 사용)
        result = (llm).invoke("ping")
        # 호출이 예외 없이 성공하면 유효한 것으로 간주
        return True if result else True
    except Exception as e:
        logging.error(f"OpenAI 호환 키 검증 실패: {str(e)}")
        return False