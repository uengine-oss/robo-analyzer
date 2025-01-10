import os
import logging
import tiktoken
from prompt.convert_entity_prompt import convert_entity_code
from understand.neo4j_connection import Neo4jConnection
from util.exception import ConvertingError, EntityCreationError, FilePathError, ProcessResultError, SaveFileError, TokenCountError
from util.file_utils import save_file
from util.token_utils import calculate_code_token

MAX_TOKENS = 1000
ENTITY_PATH = 'demo/src/main/java/com/example/demo/entity'
encoder = tiktoken.get_encoding("cl100k_base")


# 역할: Neo4j 데이터베이스에서 가져온 테이블 정보를 LLM이 처리할 수 있는 크기로 나누어 Java 엔티티 클래스를 생성합니다.
# 
# 매개변수: 
#   table_data_list (list):  Neo4j 데이터베이스에서 가져온 테이블 정보 목록
#   orm_type (str): 사용할 ORM 유형 (JPA, MyBatis 등)   
#   user_id (str): 사용자 ID
#
# 반환값:
#   entity_name_list (list): 생성된 Java 엔티티 클래스의 이름 목록
async def process_table_by_token_limit(table_data_list: list, orm_type: str, user_id: str) -> list[str]:
 
    try:
        current_tokens = 0
        table_data_chunk = []
        entity_name_list = []
        entity_code_dict = {}

        # 역할: 테이블 데이터를 LLM에게 전달하여 Entity 클래스 생성 정보를 받습니다.
        async def process_entity_class_code() -> None:
            nonlocal entity_name_list, entity_code_dict, current_tokens, table_data_chunk

            try:
                # * 테이블 데이터를 LLM에게 전달하여 Entity 클래스 생성 정보를 받음
                analysis_data = convert_entity_code(table_data_chunk, orm_type)
                
                
                # * 각 엔티티별로 파일 생성
                for entity in analysis_data['analysis']:
                    entity_name = entity['entityName']
                    entity_code = entity['code']
                    
                    await generate_entity_class(entity_name, entity_code, user_id)
                    entity_name_list.append(entity_name)
                    entity_code_dict[entity_name] = entity_code
                

                # * 다음 사이클을 위한 상태 초기화
                table_data_chunk = []
                current_tokens = 0
                
            
            except (FilePathError, SaveFileError):
                raise
            except Exception as e:
                err_msg = f"LLM을 통한 엔티티 분석 중 오류가 발생: {str(e)}"
                logging.error(err_msg)
                raise ProcessResultError(err_msg)
    


        # * 테이블 데이터 처리
        for table in table_data_list:
            table_tokens = calculate_code_token(table)
            total_tokens = current_tokens + table_tokens

            # * 토큰 제한 초과시 처리
            if table_data_chunk and total_tokens >= MAX_TOKENS:
                await process_entity_class_code()
            
            # * 현재 테이블 추가
            table_data_chunk.append(table)
            current_tokens += table_tokens


        # * 남은 데이터 처리
        if table_data_chunk:
            await process_entity_class_code()


        return entity_name_list, entity_code_dict

    except (FilePathError, SaveFileError, ProcessResultError):
        raise
    except Exception as e:
        err_msg = f"테이블 데이터 처리 중 토큰 계산 오류가 발생했습니다: {str(e)}"
        logging.error(err_msg)
        raise TokenCountError(err_msg)


# 역할: LLM을 사용하여 테이블 정보를 분석하고, 이를 바탕으로 Java 엔티티 클래스 파일을 생성합니다.
#
# 매개변수:
#   entity_name (str): 생성할 엔티티 클래스의 이름
#   entity_code (str): 생성할 엔티티 클래스의 코드
#   user_id (str): 사용자 ID
async def generate_entity_class(entity_name: str, entity_code: str, user_id: str) -> None:
    try:
        # * 저장 경로 설정
        if os.getenv('DOCKER_COMPOSE_CONTEXT'):
            save_path = os.path.join(os.getenv('DOCKER_COMPOSE_CONTEXT'), 'target', 'java', user_id, ENTITY_PATH)
        else:
            parent_workspace_dir = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
            save_path = os.path.join(parent_workspace_dir, 'target', 'java', user_id, ENTITY_PATH)


        # * Entity Class 파일 생성
        await save_file(
            content=entity_code, 
            filename=f"{entity_name}.java", 
            base_path=save_path
        )
    
    except SaveFileError:
        raise
    except Exception as e:
        err_msg = f"엔티티 클래스 파일 생성 중 오류가 발생: {str(e)}"
        logging.error(err_msg)
        raise FilePathError(err_msg)


# 역할: 전체 엔티티 생성 프로세스를 관리하는 메인 함수입니다.
#
# 매개변수:
#   object_name (str): 처리할 객체(패키지/프로시저)의 이름
#   orm_type (str): 사용할 ORM 유형 (JPA, MyBatis 등)
#   - user_id : 사용자 ID
#
# 반환값:
#   entity_name_list (list): 생성된 모든 Java 엔티티 클래스의 이름 목록
async def start_entity_processing(object_name: str, orm_type: str, user_id: str) -> list[str]:

    connection = Neo4jConnection()
    logging.info(f"[{object_name}] 엔티티 생성을 시작합니다.")

    try:
        # * 테이블 노드를 가져오기 위한 사이퍼쿼리 생성 및 실행합니다
        query = [f"MATCH (n:Table {{object_name: '{object_name}', user_id: '{user_id}'}}) RETURN n"]
        table_nodes = (await connection.execute_queries(query))[0]


        # * 테이블 데이터 구조화
        METADATA_FIELDS = {'name', 'object_name', 'id', 'primary_keys', 
                          'foreign_keys', 'description', 'reference_tables'}
        table_data_list = []


        # * 테이블 데이터의 구조를 사용하기 쉽게 구조를 변경합니다
        for node in table_nodes:
            node_data = node['n']
            table_info = {
                'name': node_data['name'],
                'fields': [
                    (key, value) for key, value in node_data.items() 
                    if key not in METADATA_FIELDS and value
                ]
            }
            
            # * 기본키 정보 추가
            if primary_keys := node_data.get('primary_keys'):
                table_info['primary_keys'] = primary_keys
            
            table_data_list.append(table_info)
        

        # * 엔티티 클래스 생성을 시작합니다.
        entity_name_list, entity_code_dict = await process_table_by_token_limit(table_data_list, orm_type, user_id)

        logging.info(f"[{object_name}] 엔티티가 생성되었습니다.\n")
        return entity_name_list, entity_code_dict
    
    except ConvertingError:
        raise
    except Exception as e:
        err_msg = f"[{object_name}] 엔티티 클래스를 생성하는 도중 오류가 발생했습니다: {str(e)}"
        logging.error(err_msg)
        raise EntityCreationError(err_msg)
    finally:
        await connection.close()