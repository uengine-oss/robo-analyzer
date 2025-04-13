import asyncio
import json
import logging
import shutil
from typing import Any, AsyncGenerator, Dict, List, Tuple
import aiofiles
import os
from internal.neo4j_connection import Neo4jConnection
from understand.analysis import analysis
from util.exception import Neo4jError, FileProcessingError, UnderstandingError
from util.string_utils import add_line_numbers


if os.getenv('DOCKER_COMPOSE_CONTEXT'):
    BASE_DIR = os.getenv('DOCKER_COMPOSE_CONTEXT')
else:
    BASE_DIR = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))


def get_user_directories(user_id: str) -> Dict[str, str]:
    """
    역할:
      사용자 별 작업에 필요한 디렉토리 경로를 생성하여 반환합니다.
      - 'plsql': PL/SQL 소스 파일 저장 경로
      - 'analysis': 분석 결과 저장 경로
      - 'ddl': DDL 파일 저장 경로
      - 'target': Converting 대상 파일 저장 경로

    매개변수:
      user_id (str): 사용자 식별 ID 문자열. 해당 값은 사용자별 폴더를 생성하는 기준입니다.

    반환값:
      Dict[str, str]: 각 디렉토리 유형에 대한 경로를 담은 딕셔너리.
    """
    
    user_base = os.path.join(BASE_DIR, 'data', user_id)
    return {
        'plsql': os.path.join(user_base, "src"),        
        'analysis': os.path.join(user_base, "analysis"),
        'ddl': os.path.join(user_base, "ddl"), 
        'target': os.path.join(BASE_DIR, 'target', 'java', user_id, 'demo', 'src', 'main', 'java', 'com', 'example', 'demo', 'command')
    }


async def process_ddl_and_sequence_files(file_infos: List[Dict[str, Any]], user_id: str, understanding_strategy, api_key: str) -> None:
    """
    역할:
      전달된 DDL 및 시퀀스 파일들을 처리하여 테이블 구조와 시퀀스 메타데이터를 분석하고,
      이를 기반으로 Neo4j 그래프 데이터베이스에 저장하는 작업을 수행합니다.

    매개변수:
      file_infos (List[Dict[str, Any]]):
          DDL 파일 및 시퀀스 파일에 대한 정보를 담은 리스트.
          각 딕셔너리는 최소한 'fileName'과 'packageName' 키를 포함해야 하며,
          선택적으로 'relatedTables'와 'relatedSequences' 키를 포함할 수 있습니다.
      user_id (str):
          사용자 식별 ID. 파일 저장 및 Neo4j 데이터 삽입에 사용됩니다.
      understanding_strategy:
          DDL 분석 및 시퀀스 분석에 사용되는 전략 패턴 객체.
      api_key (str):
          Anthropic API 키. 키 검증 및 사용에 사용됩니다.

    반환값:
      없음 (None)
    """
    neo4j_connection = Neo4jConnection()
    dirs = get_user_directories(user_id)
    ddl_dir = dirs['ddl']
    
    try:
        # DDL 디렉토리 확인 및 생성
        if not os.path.exists(ddl_dir):
            os.makedirs(ddl_dir)
            logging.info(f"DDL 디렉토리 생성 완료: {ddl_dir}")
        
        # 전달받은 각 파일별로 DDL 및 시퀀스 처리 수행
        for file_info in file_infos:
            # 'packageName' 대신 'packageName' 사용 (이후 처리에서 사용됨)
            package_name = file_info['packageName']
            related_tables = file_info.get('relatedTables', [])
            related_sequences = file_info.get('relatedSequences', [])
            
            # 관련 테이블 파일 처리
            for related_name in related_tables:
                table_file_path = os.path.join(ddl_dir, f"{related_name}.sql")
                
                # 관련 파일이 존재하는 경우에만 처리 진행
                logging.info(f"관련 테이블 파일 처리 시작: {related_name}.sql")
                
                # 파일 내용 읽기 (올바른 변수명 table_file_path 사용)
                async with aiofiles.open(table_file_path, 'r', encoding='utf-8') as related_file:
                    content = await related_file.read()
                
                # 전략 패턴을 사용하여 내용 분석
                result = await understanding_strategy.analyze_ddl(content, api_key)
                
                # Neo4j 사이퍼 쿼리 목록 준비
                cypher_queries = []
                
                # 분석된 각 테이블에 대한 Neo4j 노드 생성 쿼리 구성
                for table in result['analysis']:
                    # 테이블 기본 정보 추출
                    table_info = table['table']
                    columns = table['columns']
                    keys = table['keys']
                    
                    # 테이블의 Neo4j 노드 속성 구성
                    table_props = {
                        'name': table_info['name'],
                        'user_id': user_id,
                        'primary_keys': ','.join(key for key in keys['primary']),
                        'foreign_keys': ','.join(fk['column'] for fk in keys['foreign']),
                        'reference_tables': ','.join(
                            f"{fk['references']['table']}.{fk['references']['column']}"
                            for fk in keys['foreign']
                        ),
                        'package_name': package_name,
                    }
                    
                    # 각 컬럼 정보를 "컬럼명§타입§nullable여부" 형식으로 저장
                    for col in columns:
                        col_name = col.get('name', '')
                        col_type = col.get('type', '')
                        col_nullable = str(col.get('nullable', False)).lower()
                        table_props[col_name] = f"{col_name}§{col_type}§nullable:{col_nullable}"
                    
                    # Neo4j 테이블 노드 생성 사이퍼 쿼리 구성
                    props_str = ', '.join(f"`{k}`: '{v}'" for k, v in table_props.items())
                    table_query = f"CREATE (t:Table {{{props_str}}})"
                    cypher_queries.append(table_query)
                
                # 생성된 모든 테이블 노드 쿼리를 Neo4j에서 실행
                await neo4j_connection.execute_cypher_queries(cypher_queries)
                logging.info(f"테이블 파일 처리 완료: {related_name}.sql (테이블 {len(result['analysis'])}개)")
            
            # 관련 시퀀스 파일 처리
            for related_name in related_sequences:
                related_file_path = os.path.join(ddl_dir, f"{related_name}.sql")
                
                logging.info(f"관련 시퀀스 파일 처리 시작: {related_name}.sql")
                
                # 파일 내용 읽기
                async with aiofiles.open(related_file_path, 'r', encoding='utf-8') as related_file:
                    content = await related_file.read()
                
                # 시퀀스 노드 생성
                sequence_props = {
                    'name': related_name,
                    'user_id': user_id,
                    'procedure_name': package_name,
                    'node_code': content
                }
                
                # Neo4j 시퀀스 노드 생성 사이퍼 쿼리 구성
                props_str = ', '.join(f"`{k}`: '{v}'" for k, v in sequence_props.items())
                sequence_query = f"CREATE (s:Sequence {{{props_str}}})"
                await neo4j_connection.execute_cypher_queries([sequence_query])
                logging.info(f"시퀀스 파일 처리 완료: {related_name}.sql")
    
    except UnderstandingError:
        raise
    except Exception as e:
        err_msg = f"DDL 및 시퀀스 파일 처리 중 오류가 발생했습니다: {str(e)}"
        logging.error(err_msg)
        raise FileProcessingError(err_msg)
    finally:
        await neo4j_connection.close_connection()
        

async def generate_cypher_query_and_stream_results(file_names: List[Tuple[str, str]], user_id: str, understanding_strategy, api_key: str) -> AsyncGenerator[bytes, None]:
    """
    역할:
      여러 PL/SQL 파일을 분석하여 Neo4j 그래프 데이터베이스에 노드와 관계를 생성하고,
      그 결과를 실시간 스트림 형태로 클라이언트에게 반환합니다.
    
    매개변수:
      - file_names: 분석할 파일 이름과 객체 이름의 튜플 목록 [(파일명, 객체명), ...]
      - user_id: 사용자 식별 ID
      - understanding_strategy: PL/SQL 분석을 위한 전략 패턴 객체
      - api_key: Anthropic API 키
    
    반환값:
      - 비동기 제너레이터: 분석된 그래프 데이터, 진행률, 현재 분석 대상 등 정보를 포함한 JSON 스트림(바이트 형태)
    """

    # Neo4j 연결 객체 생성 및 분석 결과 전달/완료 알림을 위한 큐 생성
    neo4j_connection = Neo4jConnection()
    receive_queue = asyncio.Queue()  # 분석 결과를 받아올 큐
    send_queue = asyncio.Queue()     # 처리 완료 알림용 큐

    # 사용자 관련 디렉터리 경로 정보 조회
    dirs = get_user_directories(user_id)

    # 현재 코드에서는 DDL 관련 변수들이 구체적으로 정의되지 않았으므로 임시 초기화
    ddl_results = {}
    has_ddl_info = False

    try:
        # --- 1. 이전에 분석한 데이터 존재 여부 확인 ---
        # 파일 목록에서 패키지 혹은 객체 이름 추출
        package_names = [obj_name for _, obj_name in file_names]
        node_exists = await neo4j_connection.check_node_existence(user_id, package_names)
        if node_exists:
            # 이미 분석된 데이터 존재 시, 기존 데이터를 즉시 클라이언트에 반환
            already_analyzed_msg = {"type": "ALARM", "MESSAGE": "ALREADY_ANALYZED"}
            yield json.dumps(already_analyzed_msg).encode('utf-8') + b"send_stream"

            # 기존 그래프 데이터 조회 후 반환 (분석 진행률은 100%로 전송)
            graph_data = await neo4j_connection.fetch_graph_data(user_id, package_names)
            stream_data = {"type": "DATA", "graph": graph_data, "analysis_progress": 100}
            yield json.dumps(stream_data).encode('utf-8') + b"send_stream"
            return

        # --- 2. PL/SQL 파일별 분석 처리 ---
        for file_name, package_name in file_names:
            # 파일 경로 구성: PL/SQL 소스 파일과 분석(ANTLR) 결과 파일
            plsql_file_path = os.path.join(dirs['plsql'], file_name)
            base_name = os.path.splitext(file_name)[0]
            analysis_file_path = os.path.join(dirs['analysis'], f"{base_name}.json")

            # 파일 내용을 비동기로 읽어 동시에 처리 (분석 정보와 소스 코드)
            async with aiofiles.open(analysis_file_path, 'r', encoding='utf-8') as antlr_file, \
                       aiofiles.open(plsql_file_path, 'r', encoding='utf-8') as plsql_file:
                antlr_raw, plsql_lines = await asyncio.gather(
                    antlr_file.read(),
                    plsql_file.readlines()
                )

            # JSON 파싱을 통해 ANTLR 분석 데이터 로드
            antlr_data = json.loads(antlr_raw)
            # 소스 파일 총 라인 수 계산
            last_line = len(plsql_lines)
            # 소스 코드에 라인 번호를 추가하는 전처리 함수 호출
            plsql_content, _ = add_line_numbers(plsql_lines)

            # --- 3. 내부 함수: 분석 결과 처리 및 스트림 데이터 생성 ---
            async def process_analysis_results() -> AsyncGenerator[bytes, None]:
                """
                역할:
                  분석 결과 큐로부터 이벤트를 수신하고, 해당 이벤트에 따라 Neo4j에 쿼리를 실행한 후,
                  클라이언트에 전송할 스트림 데이터를 생성합니다.
                  
                매개변수:
                  - 외부 변수를 클로저를 통해 사용 (file_name, package_name, receive_queue, send_queue, last_line, neo4j_connection 등)
                
                반환값:
                  - 분석 진행률, 현재 그래프 데이터 등 정보를 포함한 JSON 스트림 데이터를 바이트형태로 yield
                """
                while True:
                    # 분석 프로세스로부터 결과 이벤트 수신 대기
                    analysis_result = await receive_queue.get()
                    logging.info(f"분석 이벤트 수신 - 파일: {file_name}")

                    # 종료 이벤트: 분석 작업 완료 시 break
                    if analysis_result.get('type') == 'end_analysis':
                        logging.info(f"파일 분석 완료: {file_name}")
                        break

                    # 에러 이벤트: 분석 중 오류 발생 시 로그 남기고 종료
                    elif analysis_result.get('type') == 'error':
                        error_msg = analysis_result.get('message', '원인 불명')
                        logging.error(f"파일 분석 실패: {file_name} - {error_msg}")
                        break

                    # 정상적인 분석 결과 처리
                    cypher_queries = analysis_result.get('query_data', [])
                    next_analysis_line = analysis_result.get('line_number', 0)
                    # 진행률 계산 (소스 코드의 총 라인 수 대비 현재 진행중인 라인)
                    analysis_progress = int((next_analysis_line / last_line) * 100)

                    # Neo4j에 쿼리 실행: 분석 결과에 따른 데이터 삽입 및 갱신
                    await neo4j_connection.execute_cypher_queries(cypher_queries)
                    # 최신 그래프 데이터를 조회
                    graph_result = await neo4j_connection.fetch_graph_data(user_id, package_names)

                    # 스트림으로 전송할 데이터 구성
                    stream_data = {
                        "type": "DATA",
                        "graph": graph_result,
                        "line_number": next_analysis_line,
                        "analysis_progress": analysis_progress,
                        "current_file": package_name
                    }
                    encoded_stream_data = json.dumps(stream_data).encode('utf-8') + b"send_stream"

                    # 처리 완료 알림 전달 (다음 분석 이벤트 처리를 위한 동기화)
                    await send_queue.put({'type': 'process_completed'})
                    logging.info(f"분석 결과 응답 전송 - 파일: {file_name}, 진행률: {analysis_progress}%")
                    
                    # 생성된 스트림 데이터를 클라이언트에 yield
                    yield encoded_stream_data

            # --- 4. 분석 작업 실행 ---
            # 분석 함수(외부 정의)를 비동기 태스크로 실행하여 결과를 receive_queue/send_queue로 주고받음
            analysis_task = asyncio.create_task(
                analysis(
                    antlr_data,            # ANTLR 파싱 결과 데이터
                    plsql_content,         # 전처리된 PL/SQL 소스 코드 (라인 번호 포함)
                    receive_queue,         # 분석 결과 수신용 큐
                    send_queue,            # 처리 완료 알림용 큐
                    last_line,             # 소스 파일의 총 라인 수
                    package_name,          # 현재 분석 대상 객체 이름
                    ddl_results,           # DDL 분석 결과 (현재는 빈 딕셔너리)
                    has_ddl_info,          # DDL 정보 유무 플래그
                    user_id,               # 사용자 식별 ID
                    understanding_strategy, # PL/SQL 분석 전략 객체
                    api_key                # Anthropic API 키
                )
            )

            # --- 5. 분석 결과 스트림 전송 ---
            # 내부 비동기 제너레이터를 통해 분석 진행 중 생성되는 스트림 데이터를 yield
            async for stream_data_chunk in process_analysis_results():
                yield stream_data_chunk

            # 분석 태스크가 끝날 때까지 대기 (예외 발생 시 처리)
            await analysis_task

    except UnderstandingError as e:
        error_info = {"type": "ERROR", "error": str(e)}
        yield json.dumps(error_info).encode('utf-8') + b"send_stream"
    except Exception as e:
        error_msg = f"스토어드 프로시저 분석 및 이해 준비 및 Cypher 쿼리 실행 중 오류 발생: {str(e)}"
        logging.exception(error_msg)
        error_info = {"type": "ERROR", "error": error_msg}
        yield json.dumps(error_info).encode('utf-8') + b"send_stream"
    finally:
        await neo4j_connection.close_connection()


async def delete_all_temp_data(user_id: str):
    """
    역할:
      사용자의 임시 생성된 파일 및 디렉토리와 Neo4j 그래프 데이터를 삭제하여 초기 상태로 복구합니다.
    
    매개변수:
      - user_id (str): 삭제 대상 데이터의 사용자 식별자.
    
    반환:
      - 반환값 없음 (비동기 함수로서 작업 완료 시 정상 종료됩니다).
    """
    # Neo4j 연결 객체 생성 (그래프 데이터 삭제 용도)
    neo4j_connection = Neo4jConnection()
    
    try:
        # 1. 사용자 관련 디렉토리 경로 구성
        #    사용자별 임시 데이터가 저장되는 기본 경로와 타겟 경로를 설정합니다.
        user_base_dir = os.path.join(BASE_DIR, 'data', user_id)
        user_target_dir = os.path.join(BASE_DIR, 'target', 'java', user_id)
        dirs_to_delete = [user_base_dir, user_target_dir]

        # 2. 각 디렉토리에 대해 삭제 및 빈 디렉토리 재생성 수행
        for dir_path in dirs_to_delete:
            if os.path.exists(dir_path):
                # 디렉토리와 해당 하위의 모든 파일 및 폴더 삭제
                shutil.rmtree(dir_path)
                # 동일 경로에 빈 디렉토리 재생성 (향후 작업을 위해)
                os.makedirs(dir_path)
                logging.info(f"디렉토리 삭제 및 재생성 완료: {dir_path}")
        
        # 3. Neo4j 데이터베이스에서 사용자 관련 데이터 삭제
        #    사용자 식별자(user_id)를 조건으로 매칭되는 노드를 삭제 (DETACH DELETE: 관계도 함께 삭제)
        delete_query = [f"MATCH (n {{user_id: '{user_id}'}}) DETACH DELETE n"]
        await neo4j_connection.execute_cypher_queries(delete_query)
        logging.info(f"Neo4J 데이터베이스 사용자 데이터 초기화 완료 - User ID: {user_id}")

    except Neo4jError:
        raise
    except Exception as e:
        err_msg = f"파일 삭제 및 그래프 데이터 삭제 중 오류 발생: {str(e)}"
        logging.exception(err_msg)
        raise OSError(err_msg)
    finally:
        await neo4j_connection.close_connection()