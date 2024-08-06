import json
import unittest
import sys
import os
import logging
import aiofiles
import tiktoken
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..', '..')))
from test_converting.converting_prompt.entity_prompt import convert_entity_code
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..', '..', '..')))
from cypher.neo4j_connection import Neo4jConnection

# * 로깅 설정
logging.basicConfig(level=logging.INFO)
logging.getLogger('asyncio').setLevel(logging.ERROR)

# * 인코더 설정 및 파일 이름 초기화
encoder = tiktoken.get_encoding("cl100k_base")
fileName = None


# 역할 : 전달받은 이름을 전부 소문자로 전환하는 함수입니다,
# 매개변수 : 
#   - sp_fileName : 스토어드 프로시저 파일의 이름
# 반환값 : 전부 소문자로 전환된 프로젝트 이름
def convert_to_lower_case_no_underscores(sp_fileName):
    return sp_fileName.replace('_', '').lower()


# 역할: 테이블 데이터를 토큰화하고, 토큰의 개수가 1000을 넘으면 LLM으로 분석을 시작합니다
# 매개변수: 
#    - table_data : 테이블 노드 정보
# 반환값: 없음
async def calculate_tokens_and_process(table_data):

    total_tokens = 0        
    table_data_chunk = []   

    try:
        # * 테이블 노드 정보를 순회하면서, 토큰 수를 계산합니다.
        for item in table_data:
            item_json = json.dumps(item, ensure_ascii=False)  
            item_tokens = len(encoder.encode(item_json))  
            
            # * 토큰 수가 초과되었다면, LLM을 이용하여 분석을 진행합니다
            if total_tokens + item_tokens >= 1000:  
                await create_entity_class(table_data_chunk)  
                table_data_chunk = []     
                total_tokens = 0         
            table_data_chunk.append(item) 
            total_tokens += item_tokens   
        
        # * 남은 데이터 덩어리가 있으면 처리합니다
        if table_data_chunk: 
            await create_entity_class(table_data_chunk)

    except Exception:
        logging.exception(f"Error occurred while tokenizing table data")
        raise


# 역할: 테이블 데이터를 LLM으로 분석하여, Entity 클래스를 생성합니다
# 매개변수: 
#    - table_data_group : 테이블 노드 정보 그룹
# 반환값: 없음
async def create_entity_class(table_data_group):

    try:
        # * 테이블 데이터를 LLM에게 전달하여, Entity 클래스 생성을 위한 정보를 받습니다
        analysis_data = convert_entity_code(table_data_group, fileName)
        logging.info("\nSuccess RqRs LLM\n")
        analysis = analysis_data['analysis']

        entity_directory = os.path.join('test', 'test_converting', 'converting_result', 'entity')
        os.makedirs(entity_directory, exist_ok=True)

        # * Entity Class를 파일로 생성합니다
        for item in analysis:
            file_path = os.path.join(entity_directory, f"{item['entityName']}.java")
            async with aiofiles.open(file_path, 'w', encoding='utf-8') as file:
                await file.write(item['code'])
                logging.info("\nSuccess Create Java Entity Class\n")

    except Exception:
        logging.exception(f"Error occurred while create entity class")
        raise


# 역할: Neo4J에서 테이블 노드를 가지고와서 데이터 구조를 변경하고, entity class 생성을 준비합니다. 
# 매개변수: 
#    - sp_fileName : 스토어드 프로시저 파일 이름
# 반환값: 변경된 테이블 데이터 구조
async def start_entity_processing(sp_fileName):
    try:
        # * 파일 이름을 초기화합니다
        global fileName 
        fileName = convert_to_lower_case_no_underscores(sp_fileName)

        
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
                'fields': [value for key, value in table.items() if key not in ['name', 'id']],
                'keyType': 'Long', # TODO 실제 기본키 타입으로 변경 필요
            }
            transformed_table_data.append(transformed_node)
        logging.info("\nSuccess Transformed Table Nodes Data\n")
        await calculate_tokens_and_process(transformed_table_data)
        return transformed_table_data
    
    except Exception:
        logging.exception(f"Error occurred while bring table node from neo4j")
        raise 
    finally:
        await connection.close()


# 엔티티 클래스를 생성하는 테스트 모듈
class AsyncTestCase(unittest.IsolatedAsyncioTestCase):
    async def test_create_entity_class(self):
        await start_entity_processing("P_B_CAC120_CALC_SUIP_STD")

if __name__ == '__main__':
    unittest.main()