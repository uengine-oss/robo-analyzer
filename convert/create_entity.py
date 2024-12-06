import json
import os
import logging
import aiofiles
import tiktoken
from prompt.convert_entity_prompt import convert_entity_code
from understand.neo4j_connection import Neo4jConnection
from util.exception import EntityCreationError, LLMCallError, Neo4jError, TokenCountError

MAX_TOKENS = 1000
ENTITY_PATH = 'java/demo/src/main/java/com/example/demo/entity'
encoder = tiktoken.get_encoding("cl100k_base")


# 역할: Neo4j 데이터베이스에서 가져온 테이블 정보를 LLM이 처리할 수 있는 크기로 나누어 Java 엔티티 클래스를 생성합니다.
# 
# 매개변수: 
#   table_data_list (list): 
#     - Neo4j 데이터베이스에서 가져온 테이블 정보 목록
#     - 각 테이블은 테이블명과 컬럼 정보를 포함합니다
#     - 예시: [{'name': 'user_table', 'fields': [('id', 'int'), ('name', 'varchar')]}]
#
# 반환값:
#   entity_name_list (list): 
#     - 생성된 Java 엔티티 클래스의 이름 목록
#     - 예시: ['UserEntity', 'ProductEntity']
async def process_table_by_token_limit(table_data_list):

    current_tokens = 0        
    table_data_chunk = []   
    entity_name_list = []
    
    try:
        # * 테이블 노드 정보를 순회하면서, 토큰 수를 계산합니다.
        for table in table_data_list:
            table_str = json.dumps(table, ensure_ascii=False) 
            table_tokens = len(encoder.encode(table_str))


            # * 토큰 수가 초과되었다면, LLM을 이용하여 분석을 진행합니다
            if table_data_chunk and current_tokens + table_tokens >= MAX_TOKENS:
                entity_names = await create_entity_class(table_data_chunk)
                entity_name_list.extend(entity_names)
                table_data_chunk = []     
                current_tokens = 0         
            table_data_chunk.append(table) 
            current_tokens += table_tokens   
        

        # * 처리되지 않은 테이블 데이터가 남아있다면 처리합니다
        if table_data_chunk: 
            result = await create_entity_class(table_data_chunk)
            entity_name_list.extend(result)
        
        return entity_name_list
    
    except (OSError, LLMCallError):
        raise
    except Exception:
        err_msg = "엔티티 생성 과정에서 테이블 노드 토큰 계산 도중 문제가 발생"
        logging.error(err_msg, exc_info=False)
        raise TokenCountError(err_msg)


# 역할: LLM을 사용하여 테이블 정보를 분석하고, 이를 바탕으로 Java 엔티티 클래스 파일을 생성합니다.
#
# 매개변수:
#   table_data_group (list):
#     - 처리할 테이블 정보 그룹
#     - 각 테이블은 이름과 필드 정보를 포함
#     - 예시: [{'name': 'user_table','fields': [('user_id', 'number'), ('user_name', 'varchar')]}]
#
# 반환값:
#   entity_name_list (list):
#     - 생성된 엔티티 클래스의 이름 목록
#     - 예시: ['UserEntity']
async def create_entity_class(table_data_group):

    try:
        # * 테이블 데이터를 LLM에게 전달하여, Entity 클래스 생성을 위한 정보를 받습니다
        analysis_data = convert_entity_code(table_data_group)
        entity_name_list = []


        # * Entity 클래스을 저장할 경로를 설정합니다
        base_directory = os.getenv('DOCKER_COMPOSE_CONTEXT')
        if base_directory:
            entity_directory = os.path.join(base_directory, ENTITY_PATH)
        else:
            parent_workspace_dir = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))  # 현재 프로젝트의 상위 디렉토리
            entity_directory = os.path.join(parent_workspace_dir, 'target', ENTITY_PATH)
        os.makedirs(entity_directory, exist_ok=True)


        # * Entity Class를 파일로 생성합니다
        for entity in analysis_data['analysis']:
            entity_name_list.append(entity['entityName'])
            file_path = os.path.join(entity_directory, f"{entity['entityName']}.java")
            async with aiofiles.open(file_path, 'w', encoding='utf-8') as file:
                await file.write(entity['code'])
        return entity_name_list
    
    except LLMCallError: 
        raise
    except Exception:
        err_msg = "엔티티 클래스 파일 쓰기 및 생성 도중 오류가 발생"
        logging.error(err_msg, exc_info=False)
        raise OSError(err_msg)


# 역할: 전체 엔티티 생성 프로세스를 관리하는 메인 함수입니다.
#      Neo4j 데이터베이스 연결부터 엔티티 파일 생성까지 모든 과정을 조율합니다.
#
# 매개변수:
#   object_name (str):
#     - 처리할 데이터베이스 객체(패키지/프로시저)의 이름
#     - Neo4j에서 이 이름으로 관련 테이블들을 검색
#     - 예시: 'HR_PACKAGE'
#
# 반환값:
#   entity_name_list (list):
#     - 생성된 모든 Java 엔티티 클래스의 이름 목록
#     - 예시: ['EmployeeEntity', 'DepartmentEntity']
async def start_entity_processing(object_name):
    connection = Neo4jConnection()
    logging.info(f"[{object_name}] 엔티티 생성을 시작합니다.")
    try:
        # * 테이블 노드를 가져오기 위한 사이퍼쿼리 생성 및 실행합니다
        query = [f"MATCH (n:Table {{object_name: '{object_name}'}}) RETURN n"]
        table_nodes = await connection.execute_queries(query)

        # * 테이블 데이터에서 필요한 필드만 추출합니다
        METADATA_FIELDS = {'name', 'object_name', 'id', 'primary_keys', 'foreign_keys', 'description', 'reference_tables'}
        table_data_list = []


        # * 테이블 데이터의 구조를 사용하기 쉽게 구조를 변경합니다
        for item in table_nodes[0]:
            node_data = item['n']
            table_info = {'name': node_data['name']}
            
            # * 일반 필드 추출
            fields = [(key, value) for key, value in node_data.items() 
                     if key not in METADATA_FIELDS and value]
            if fields:
                table_info['fields'] = fields
            
            # * 메타데이터 추가 (값이 있는 경우만)
            # TODO 일단 외래키 처리는 하지 않음 (추후에 검토)
            for meta_key in ['primary_keys']:
                if node_data.get(meta_key):
                    table_info[meta_key] = node_data[meta_key]
            
            table_data_list.append(table_info)
        

        # * 엔티티 클래스 생성을 시작합니다.
        entity_name_list = await process_table_by_token_limit(table_data_list)
        entity_count = len(entity_name_list)
        logging.info(f"[{object_name}] {entity_count}개의 엔티티가 생성되었습니다.\n")
        return entity_name_list
    
    except (TokenCountError, Neo4jError, OSError, LLMCallError):
        raise
    except Exception:
        err_msg = f"[{object_name}] 엔티티 클래스를 생성하는 도중 오류가 발생했습니다."
        logging.error(err_msg, exc_info=False)
        raise EntityCreationError(err_msg)
    finally:
        await connection.close()