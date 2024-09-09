import json
import os
import logging
import aiofiles
from prompt.entity_prompt import convert_entity_class
from understand.neo4j_connection import Neo4jConnection
from langchain_openai import ChatOpenAI

# * 인코더 설정 및 파일 이름 초기화
llm = ChatOpenAI(model_name="gpt-4o")


# 역할: 테이블 데이터의 토큰 수에 따라, LLM으로 분석을 결정하는 함수
# 매개변수: 
#   - table_data_list : 테이블 노드 정보
#   - lower_file_name : 소문자로 구성된 스토어드 프로시저 파일 이름
# 반환값:
#   - entity_name_list : 생성된 엔티티 클래스들의 이름 목록 
async def calculate_tokens_and_process(table_data_list, lower_file_name):

    current_tokens = 0        
    table_data_chunk = []   
    entity_name_list = []
    
    try:
        # * 테이블 노드 정보를 순회하면서, 토큰 수를 계산합니다.
        for table in table_data_list:
            table_str = json.dumps(table)
            table_tokens = llm.get_num_tokens_from_messages([{"content": table_str}])
            # table_tokens = llm.get_num_tokens_from_messages([{"role": "user", "content": table_str}])


            # * 토큰 수가 초과되었다면, LLM을 이용하여 분석을 진행합니다
            if current_tokens + table_tokens >= 1000:
                entity_names = await create_entity_class(table_data_chunk, lower_file_name)
                if isinstance(entity_names, Exception): return entity_names
                entity_name_list.extend(entity_names)
                table_data_chunk = []     
                current_tokens = 0         
            table_data_chunk.append(table) 
            current_tokens += table_tokens   
        
        # * 처리되지 않은 테이블 데이터가 남아있다면 처리합니다
        if table_data_chunk: 
            result = await create_entity_class(table_data_chunk, lower_file_name)
            if isinstance(result, Exception): return result
            entity_name_list.extend(result)
        return entity_name_list
    

    except Exception:
        error_msg = "테이블 노드의 토큰 계산중 오류가 발생"
        logging.exception(error_msg)
        return Exception(error_msg)


# 역할: 전달된 테이블 데이터를 LLM으로 분석하여, Entity 클래스를 생성
# 매개변수: 
#   - table_data_group : 테이블 노드 데이터 그룹
#   - lower_file_name : 소문자로 구성된 스토어드 프로시저 파일 이름
# 반환값:
#   - entity_name_list : 생성된 엔티티 클래스들의 이름 목록 
async def create_entity_class(table_data_group, lower_file_name):

    try:
        # * 테이블 데이터를 LLM에게 전달하여, Entity 클래스 생성을 위한 정보를 받습니다
        analysis_data = convert_entity_class(table_data_group, lower_file_name)
        entity_name_list = []


        # * Entity 클래스을 저장할 경로를 설정합니다
        base_directory = os.getenv('DOCKER_COMPOSE_CONTEXT', 'data')
        entity_directory = os.path.join(base_directory, 'java', f'{lower_file_name}', 'src', 'main', 'java', 'com', 'example', f'{lower_file_name}', 'entity')
        os.makedirs(entity_directory, exist_ok=True)


        # * Entity Class를 파일로 생성합니다
        for item in analysis_data['analysis']:
            entity_name_list.append(item['entityName'])
            file_path = os.path.join(entity_directory, f"{item['entityName']}.java")
            async with aiofiles.open(file_path, 'w', encoding='utf-8') as file:
                await file.write(item['code'])
        return entity_name_list
     
    except Exception:
        error_msg = "엔티티 클래스 파일 생성중 오류가 발생"
        logging.exception(error_msg)
        return Exception(error_msg)


# 역할: Neo4J에서 모든 테이블 노드를 가져오고, 테이블 노드 데이터를 재구성
# 매개변수: 
#    - lower_file_name : 소문자로 구성된 스토어드 프로시저 파일 이름
# 반환값:
#   - transformed_table_data : 재구성된 테이블 노드 데이터
#   - entity_name_list : 생성된 엔티티 클래스들의 이름 목록 
async def start_entity_processing(lower_file_name):
    connection = Neo4jConnection()
    
    try:
        # * 테이블 노드를 가져오기 위한 사이퍼쿼리 생성 및 실행합니다
        query = ['MATCH (n:Table) RETURN n']
        table_nodes = await connection.execute_queries(query)
        if isinstance(table_nodes, Exception): return table_nodes

        table_data_list = []


        # * 테이블 데이터의 구조를 사용하기 쉽게 구조를 변경합니다
        for item in table_nodes[0]:
            transformed_table_info = {
                'name': item['n']['name'],
                'fields': [(key, value) for key, value in item['n'].items() if key != 'name'],
                'keyType': 'Long',  # TODO 실제 기본키 타입으로 변경 필요
            }
            table_data_list.append(transformed_table_info)
        

        # * 엔티티 클래스 생성을 시작합니다.
        entity_name_list = await calculate_tokens_and_process(table_data_list, lower_file_name)
        if isinstance(entity_name_list, Exception): return entity_name_list
        logging.info("\nSuccess Create Entity Class\n")
        return table_data_list, entity_name_list
    
    except Exception:
        error_msg = "Neo4J로 부터 테이블 노드를 가져와서 처리하는 도중 문제가 발생"
        logging.exception(error_msg)
        return Exception(error_msg)
    finally:
        await connection.close()