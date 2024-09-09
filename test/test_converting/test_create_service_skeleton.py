import json
import unittest
import sys
import os
import logging
import aiofiles
import tiktoken
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..', '..')))
from test_converting.converting_prompt.service_skeleton_prompt import convert_service_skeleton_code
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..', '..', '..')))
from understand.neo4j_connection import Neo4jConnection

# 로깅 설정
logging.basicConfig(level=logging.INFO)
logging.getLogger('asyncio').setLevel(logging.ERROR)


# * 인코더 설정 및 파일 이름 초기화
encoder = tiktoken.get_encoding("cl100k_base")
fileName = None


# 역할 : 전달받은 이름을 전부 소문자로 전환하는 함수입니다,
# 매개변수 : 
#   - fileName : 스토어드 프로시저 파일의 이름
# 반환값 : 전부 소문자로 전환된 프로젝트 이름
def convert_to_lower_case_no_underscores(fileName):
    return fileName.replace('_', '').lower()


# 역할: 프로시저 노드 코드에서 입력 매개변수 부분만 추출하는 메서드입니다.
# 매개변수: 
#      - procedure_code : 프로시저 노드 부분의 스토어드 프로시저 코드.
# 반환값: 프로시저 노드 코드에서 변수 선언 부분만 필터링된 코드.
def extract_procedure_variable_code(procedure_code):
    try:

        # * AS 키워드를 찾아서 AS 이후 모든 라인을 제거합니다.
        as_index = procedure_code.find(' AS')
        if as_index != -1:
            newline_after_as = procedure_code.find('\n', as_index)
            procedure_code = procedure_code[:newline_after_as]

        return procedure_code

    except Exception:
        logging.exception("Error during code placeholder removal(understanding)")
        raise


# 역할: 프로시저 노드와 Declare 노드를 처리하는 함수입니다.
# 매개변수: 
#      - procedure_data : 프로시저 노드 데이터
# 반환값: 
#      - service_skeleton_code : 서비스 스켈레톤 클래스 코드 
#      - command_class_variable : 프로시저의 입력 매개변수(Command 클래스에 선언된 변수 목록)
async def calculate_tokens_and_process(procedure_data):

    service_skeleton_code = None  # 서비스 스켈레톤 코드 초기화
    
    try:
        # * 데이터 처리를 진행합니다.
        service_skeleton_code, command_class_variable = await create_service_skeleton(procedure_data)
        return service_skeleton_code, command_class_variable

    except Exception:
        logging.exception(f"Error occurred while procedure node token check")
        raise


# 역할: LLM을 사용하여 주어진 프로시저 데이터 그룹을 분석하고, 결과를 서비스 및 커맨드 파일로 저장합니다.
# 매개변수: 
#      - procedure_data : 분석할 프로시저 데이터 그룹
# 반환값: 
#      - service_class_code : 서비스 스켈레톤 클래스 코드
#      - command_class_variable : 프로시저의 입력 매개변수(Command 클래스에 선언된 변수 목록)
async def create_service_skeleton(procedure_data_group):
    
    try:
        # * LLM을 사용하여 주어진 데이터를 분석하고 받은 결과에서 정보를 추출합니다
        analysis_data = convert_service_skeleton_code(procedure_data_group, fileName)  
        command_class_name = analysis_data['commandName']
        command_class_code = analysis_data['command']
        command_class_variable = analysis_data['command_class_variable']
        service_class_code = analysis_data['service']
        logging.info("\nSuccess RqRs LLM\n")  
        command_class_directory = os.path.join('test', 'test_converting', 'converting_result', 'command')  
        os.makedirs(command_class_directory, exist_ok=True) 


        # * 커맨드 코드를 파일로 저장합니다.
        command_class_path = os.path.join(command_class_directory, f"{command_class_name}.java")  
        async with aiofiles.open(command_class_path, 'w', encoding='utf-8') as file:  
            await file.write(command_class_code)  
            logging.info(f"\nSuccess Create {command_class_name} Java File\n") 

        return service_class_code, command_class_variable

    except Exception:
        logging.exception(f"Error occurred while create service skeleton and command")
        raise


# 역할: Neo4j 데이터베이스에서 프로시저, Declare 노드를 가져와서 JSON의 구조를 단순하게 변경하는 함수입니다.
# 매개변수: 
#      - sp_fileName : 스토어드 프로시저 파일 이름
# 반환값: 
#      - service : 서비스 스켈레톤 클래스 코드
#      - command_class_variable : 프로시저의 입력 매개변수(Command 클래스에 선언된 변수 목록)
async def start_service_skeleton_processing(sp_fileName):
    
    try:
        global fileName
        fileName = convert_to_lower_case_no_underscores(sp_fileName)
        connection = Neo4jConnection()  

        # * Neo4j 데이터베이스에서 프로시저, Declare 노드를 검색하는 쿼리를 실행합니다.
        query = ['MATCH (n:CREATE_PROCEDURE_BODY) RETURN n', 'MATCH (n:DECLARE) RETURN n']  
        procedure_declare_nodes = await connection.execute_queries(query)  
        logging.info("\nSuccess Received Procedure, Declare Nodes from Neo4J\n")  
        transformed_node_data = [] 
        

        # * Neo4j로 부터 전달받은 프로시저 노드의 데이터의 구조를 사용하기 쉽게 변경합니다.
        for item in procedure_declare_nodes[0]:
            transformed_node = {
                'type': 'procedure',
                'code': extract_procedure_variable_code(item['n']['summarized_code'])
            }
            transformed_node_data.append(transformed_node)  


        # * Neo4j로 부터 전달받은 Declare 노드의 데이터의 구조를 사용하기 쉽게 변경합니다.
        for item in procedure_declare_nodes[1]:
            transformed_node = {
                'type': 'declare',
                'code': item['n']['summarized_code']
            }
            transformed_node_data.append(transformed_node)   
                

        # * 변환된 데이터를 사용하여 토큰 계산 및 서비스 스켈레톤 생성을 수행합니다.
        logging.info("\nSuccess Transformed Procedure, Declare Nodes Data\n")  
        service_skeleton, command_class_variable = await calculate_tokens_and_process(transformed_node_data)  
        return service_skeleton, command_class_variable
    
    except Exception:
        logging.exception(f"Error occurred while bring procedure node from neo4j")
        raise
    finally:
        await connection.close()  


# 서비스 스켈레톤 생성하기 위한 테스트 모듈
class AsyncTestCase(unittest.IsolatedAsyncioTestCase):
    async def test_create_command_service_skeleton(self):
        await start_service_skeleton_processing("P_B_CAC120_CALC_SUIP_STD")


if __name__ == '__main__':
    unittest.main()