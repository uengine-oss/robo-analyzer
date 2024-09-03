import json
import os
import logging
import aiofiles
import tiktoken
from convert.converting_prompt.entity_prompt import convert_entity_code
from cypher.neo4j_connection import Neo4jConnection

# * 인코더 설정 및 파일 이름 초기화
encoder = tiktoken.get_encoding("cl100k_base")


# 역할: 테이블 데이터를 토큰화하고, 토큰의 개수가 1000을 넘으면 LLM으로 분석을 시작합니다
# 매개변수: 
#    - table_data : 테이블 노드 정보
#    - lower_file_name : 소문자 프로젝트 이름
# 반환값: 없음
async def calculate_tokens_and_process(table_data, lower_file_name):

    total_tokens = 0        
    table_data_chunk = []   
    entity_name_list = []
    
    try:
        # * 테이블 노드 정보를 순회하면서, 토큰 수를 계산합니다.
        for item in table_data:
            item_json = json.dumps(item, ensure_ascii=False)  
            item_tokens = len(encoder.encode(item_json))  
            
            # * 토큰 수가 초과되었다면, LLM을 이용하여 분석을 진행합니다
            if total_tokens + item_tokens >= 1000:  
                entity_name_list.extend(await create_entity_class(table_data_chunk, lower_file_name))  
                table_data_chunk = []     
                total_tokens = 0         
            table_data_chunk.append(item) 
            total_tokens += item_tokens   
        
        # * 남은 데이터 덩어리가 있으면 처리합니다
        if table_data_chunk: 
            entity_name_list.extend(await create_entity_class(table_data_chunk, lower_file_name))  

        return entity_name_list
    
    except Exception:
        logging.exception(f"Error occurred while tokenizing table data")
        raise


# 역할: 테이블 데이터를 LLM으로 분석하여, Entity 클래스를 생성합니다
# 매개변수: 
#    - table_data_group : 테이블 노드 정보 그룹
#    - lower_file_name : 소문자 프로젝트 이름
# 반환값: 없음
async def create_entity_class(table_data_group, lower_file_name):

    try:
        # * 테이블 데이터를 LLM에게 전달하여, Entity 클래스 생성을 위한 정보를 받습니다
        analysis_data = convert_entity_code(table_data_group, lower_file_name)
        logging.info("\nSuccess RqRs LLM\n")
        analysis = analysis_data['analysis']
        entity_name_list = []

        # * Entity 클래스을 저장할 경로를 설정합니다
        base_directory = os.getenv('DOCKER_COMPOSE_CONTEXT', 'convert')
        entity_directory = os.path.join(base_directory, 'converting_result', f'{lower_file_name}', 'src', 'main', 'java', 'com', 'example', f'{lower_file_name}', 'entity')
        os.makedirs(entity_directory, exist_ok=True)


        # * Entity Class를 파일로 생성합니다
        for item in analysis:
            entity_name_list.append(item['entityName'])
            file_path = os.path.join(entity_directory, f"{item['entityName']}.java")
            async with aiofiles.open(file_path, 'w', encoding='utf-8') as file:
                await file.write(item['code'])
                logging.info("\nSuccess Create Java Entity Class\n")

        return entity_name_list
     
    except Exception:
        logging.exception(f"Error occurred while create entity class")
        raise


# 역할: Neo4J에서 테이블 노드를 가지고와서 데이터 구조를 변경하고, entity class 생성을 준비합니다. 
# 매개변수: 
#    - lower_file_name : 소문자 스토어드 프로시저 파일 이름
# 반환값: 변경된 테이블 데이터 구조
async def start_entity_processing(lower_file_name):
    try:
        # * 테이블 노드 정보를 가져오기 위한 사이퍼쿼리 준비 및 실행합니다
        connection = Neo4jConnection()
        query = ['MATCH (n:Table) RETURN n']
        table_nodes = await connection.execute_queries(query)
        logging.info("\nSuccess received Table Nodes from Neo4J\n")
        transformed_table_data = []


        # * 테이블 데이터의 구조를 사용하기 쉽게 구조를 변경합니다
        for item in table_nodes[0]:
            table = item['n']
            transformed_node = {
                'name': table['name'],
                'fields': [(key, value) for key, value in table.items() if key not in ['name']],
                'keyType': 'Long', # TODO 실제 기본키 타입으로 변경 필요
            }
            transformed_table_data.append(transformed_node)
        logging.info("\nSuccess Transformed Table Nodes Data\n")
        entity_name_list = await calculate_tokens_and_process(transformed_table_data, lower_file_name)
        return transformed_table_data, entity_name_list
    
    except Exception:
        logging.exception(f"Error occurred while bring table node from neo4j")
        raise 
    finally:
        await connection.close()