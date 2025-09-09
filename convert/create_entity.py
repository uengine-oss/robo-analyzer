import os
import logging
import tiktoken
from prompt.convert_entity_prompt import convert_entity_code
from understand.neo4j_connection import Neo4jConnection
from util.exception import ConvertingError, GenerateTargetError
from util.utility_tool import calculate_code_token, save_file


MAX_TOKENS = 3000
# project_name은 함수 매개변수로 받음
encoder = tiktoken.get_encoding("cl100k_base")


# 역할: Neo4j 데이터베이스에서 가져온 테이블 정보를 LLM이 처리할 수 있는 크기로 나누어 Java 엔티티 클래스를 생성합니다.
# 
# 매개변수: 
#   table_data_list (list):  Neo4j 데이터베이스에서 가져온 테이블 정보 목록
#   user_id (str): 사용자 ID
#   api_key (str): OpenAI API 키
#   project_name (str): 프로젝트 이름
#
# 반환값:
#   entity_result_list (list): 생성된 Java 엔티티 클래스의 이름과 코드를 포함한 딕셔너리 목록
async def process_table_by_token_limit(table_data_list: list, user_id: str, api_key: str, project_name: str, locale: str) -> list[dict]:
 
    try:
        current_tokens = 0
        table_data_chunk = []
        entity_result_list = []


        # 역할: 테이블 데이터를 LLM에게 전달하여 Entity 클래스 생성 정보를 받습니다.
        async def process_entity_class_code() -> None:
            nonlocal entity_result_list, current_tokens, table_data_chunk

            try:
                # * 테이블 데이터를 LLM에게 전달하여 Entity 클래스 생성 정보를 받음
                analysis_data = convert_entity_code(table_data_chunk, api_key, project_name, locale)


                # * 각 엔티티별로 파일 생성
                for entity in analysis_data['analysis']:
                    entity_name = entity['entityName']
                    entity_code = entity['code']
                    
                    # * 엔티티 클래스 파일 생성
                    await generate_entity_class(entity_name, entity_code, user_id, project_name)
                                        
                    # * 엔티티 클래스 정보 저장
                    entity_result_list.append({
                        'entityName': entity_name,
                        'entityCode': entity_code
                    })


                # * 다음 사이클을 위한 상태 초기화
                table_data_chunk = []
                current_tokens = 0
                
            
            except ConvertingError:
                raise
            except Exception as e:
                err_msg = f"LLM을 통한 엔티티 분석 중 오류가 발생: {str(e)}"
                logging.error(err_msg)
                raise ConvertingError(err_msg)
    


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


        return entity_result_list

    except ConvertingError:
        raise
    except Exception as e:
        err_msg = f"테이블 데이터 처리 중 토큰 계산 오류가 발생했습니다: {str(e)}"
        logging.error(err_msg)
        raise ConvertingError(err_msg)


# 역할: LLM을 사용하여 테이블 정보를 분석하고, 이를 바탕으로 Java 엔티티 클래스 파일을 생성합니다.
#
# 매개변수:
#   entity_name (str): 생성할 엔티티 클래스의 이름
#   entity_code (str): 생성할 엔티티 클래스의 코드
#   user_id (str): 사용자 ID
#   project_name (str): 프로젝트 이름
async def generate_entity_class(entity_name: str, entity_code: str, user_id: str, project_name: str) -> None:
    try:
        # 엔티티 경로 생성
        entity_path = f'{project_name}/src/main/java/com/example/{project_name}/entity'
        
        # * 저장 경로 설정
        if os.getenv('DOCKER_COMPOSE_CONTEXT'):
            save_path = os.path.join(os.getenv('DOCKER_COMPOSE_CONTEXT'), 'target', 'java', user_id, entity_path)
        else:
            parent_workspace_dir = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
            save_path = os.path.join(parent_workspace_dir, 'target', 'java', user_id, entity_path)


        # * Entity Class 파일 생성
        await save_file(
            content=entity_code, 
            filename=f"{entity_name}.java", 
            base_path=save_path
        )
    
    except ConvertingError:
        raise
    except Exception as e:
        err_msg = f"엔티티 클래스 파일 생성 중 오류가 발생: {str(e)}"
        logging.error(err_msg)
        raise GenerateTargetError(err_msg)


# 역할: 전체 엔티티 생성 프로세스를 관리하는 메인 함수입니다.
#
# 매개변수:
#   file_names (list): 파일 이름과 객체 이름의 튜플 리스트 [(file_name, object_name), ...]
#   user_id (str): 사용자 ID
#   api_key (str): OpenAI API 키
#   project_name (str): 프로젝트 이름(선택적)
#
# 반환값:
#   entity_result_list (list): 생성된 모든 Java 엔티티 클래스의 이름과 코드를 포함한 딕셔너리 목록
async def start_entity_processing(file_names: list, user_id: str, api_key: str, project_name: str = None, locale: str = 'ko') -> list[dict]:

    connection = Neo4jConnection()
    logging.info(f"엔티티 생성을 시작합니다.")
    
    try:
        # 사용자 ID 기준으로 해당 사용자의 모든 테이블 조회 (object_name 필터 제거)
        query = [f"MATCH (n:Table) WHERE n.user_id = '{user_id}' RETURN n"]
        table_nodes = (await connection.execute_queries(query))[0]
        
        # 테이블 데이터 구조화
        METADATA_FIELDS = {'name', 'object_name', 'id', 'primary_keys', 'user_id',
                          'foreign_keys', 'reference_tables'}
        table_data_list = []
        
        # 테이블 데이터의 구조를 사용하기 쉽게 변경
        for node in table_nodes:
            node_data = node['n']
            table_name = node_data['name']
            
            table_info = {
                'name': table_name,
                'fields': [
                    (key, value) for key, value in node_data.items() 
                    if key not in METADATA_FIELDS and value
                ]
            }
            
            # 기본키 정보 추가
            if primary_keys := node_data.get('primary_keys'):
                table_info['primary_keys'] = primary_keys
            
            table_data_list.append(table_info)
        
        # 엔티티 클래스 생성
        if table_data_list:
            entity_result_list = await process_table_by_token_limit(table_data_list, user_id, api_key, project_name, locale)
            logging.info(f"총 {len(entity_result_list)}개의 엔티티가 생성되었습니다.")
            return entity_result_list
        else:
            logging.info("테이블이 발견되지 않았습니다.")
            return []
        
    except ConvertingError:
        raise
    except Exception as e:
        err_msg = f"엔티티 클래스를 생성하는 도중 오류가 발생했습니다: {str(e)}"
        logging.error(err_msg)
        raise ConvertingError(err_msg)
    finally:
        await connection.close()