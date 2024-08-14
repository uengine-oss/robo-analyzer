import logging
import os
import re
import sys
import unittest
import aiofiles
import tiktoken

logging.basicConfig(level=logging.INFO)
logging.getLogger('asyncio').setLevel(logging.ERROR)
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..', '..')))
from test_converting.converting_prompt.parent_skeleton_prompt import convert_parent_skeleton
from test_converting.converting_prompt.service_prompt import convert_code
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..', '..', '..')))
from cypher.neo4j_connection import Neo4jConnection


# * 인코더 설정 및 파일 이름 및 변수 초기화 
encoder = tiktoken.get_encoding("cl100k_base")
fileName = None
procedure_variables = []
service_skeleton = None
count = 0

# 역할 : 전달받은 이름을 전부 소문자로 전환하는 함수입니다,
# 매개변수 : 
#   - fileName : 스토어드 프로시저 파일의 이름
# 반환값 : 전부 소문자로 전환된 프로젝트 이름
def convert_to_lower_case_no_underscores(fileName):
    return fileName.replace('_', '').lower()


# TODO 레이블 대문자 변경 필요!!
# 역할: 해당 노드에서 사용된 변수노드를 가져오는 메서드입니다.
# 매개변수: 
#      - node_id : 노드의 고유 식별자.
# 반환값: 노드에서 사용된 변수 노드의 목록.
async def fetch_variable_nodes(node_id):
    try:
        query = [f"MATCH (v:Variable) WHERE v.role_{node_id} IS NOT NULL RETURN v.id, v.name, v"]
        connection = Neo4jConnection()
        variable_nodes = await connection.execute_queries(query)
        
        variable_node_list = []
        seen_roles = {}

        
        # * 전달된 변수 정보 데이터를 이용해서 사용하게 쉽게 재구성 
        for node in variable_nodes[0]:
            var_name = node['v']['name']
            role_start = node['v'][f'role_{node_id}']


            # * 역할 설명 정규화 (예: "저장합니다", "저장함", "저장됨" -> "저장")
            normalized_role = re.sub(r'합니다$|함$|됨$', '', role_start).strip()


            # * 중복된 역할 제거 후 리스트에 추가
            if var_name not in seen_roles:
                seen_roles[var_name] = {normalized_role}
            else:
                seen_roles[var_name].add(normalized_role)


        # * 최종 결과를 리스트에 사전 형태로 추가
        for name, roles in seen_roles.items():
            variable_node_list.append({name: list(roles)})


        logging.info("\nSuccessfully received variable nodes from Neo4J\n")
        await connection.close()
        return variable_node_list

    except Exception:
        logging.exception("Error during fetching variable nodes from Neo4J (converting)")
        raise


# 역할 : Service 클래스 파일 생성하는 함수
# 매개변수 :
#   - service_code : 서비스 코드
#   - startLine : 시작 라인
#   - endLine : 종료 라인
# 반환값 : 없음
async def create_service_file(service_code, startLine = None, endLine = None):

    # * 서비스 파일을 저장할 디렉토리 경로를 설정합니다.
    service_directory = os.path.join('test', 'test_converting', 'converting_result', 'service')
    os.makedirs(service_directory, exist_ok=True)


    # * 제공된 정보를 사용하여 파일 이름을 구성합니다.
    service_file_name = f"{endLine}_parent_of_{startLine}_service.txt" if startLine and endLine else f"service{count}.txt"
    service_file_path = os.path.join(service_directory, service_file_name)


    # * 비동기 파일 쓰기를 사용하여 서비스 코드를 파일에 작성합니다.
    async with aiofiles.open(service_file_path, 'w', encoding='utf-8') as file:
        await file.write(service_code)
    logging.info(f"\nSuccess Create service Java File\n")


# 역할 : 부모 서비스 틀을 만드는 함수입니다.
# 매개변수 :
#   - start_line : 노드의 시작 라인
#   - summarized_code : 요약 처리된 코드
async def create_parent_skeleton(start_line, end_line, summarized_code, parent_schedule_stack):
    analysis_result = convert_parent_skeleton(summarized_code)
    service_code = analysis_result['code']
    parent_schedule_stack.append(service_code)
    await create_service_file(service_code, start_line, end_line)
    return parent_schedule_stack


# 역할: llm에게 분석할 스토어드 프로시저 코드를 전달한 뒤, 해당 결과를 바탕으로 Service를 생성합니다.
# 반환값 : Service 클래스 파일
async def process_converting(convert_sp_code, schedule_stack, total_tokens, variable_list):
    
    try:
        # * 정리된 코드를 분석합니다(llm에게 전달)
        analysis_result = convert_code(convert_sp_code, variable_list, procedure_variables, fileName)
        global count
        count += 1

        # * 분석 결과 각각의 데이터를 추출하고, 필요한 변수를 초기화합니다 
        service_code = analysis_result['code']


        # * 서비스 클래스를 파일로 생성       
        await create_service_file(service_code)


        # * 다음 분석 주기를 위해 필요한 변수를 초기화합니다
        service_code = service_skeleton
        convert_sp_code = ""
        total_tokens = 0
        variable_list.clear()

        return (convert_sp_code, schedule_stack, total_tokens, variable_list)


    except Exception:
        logging.exception("An error occurred during analysis results processing(converting)")
        raise



# 역할: 실제 서비스를 생성하는 함수입니다.
# 매개변수: 
#   node_data_list - 노드와 관계에 대한 정보가 담긴 리스트
# 반환값: 없음 
async def process_service_class(node_data_list):

    parent_schedule_stack = []
    variable_list = []
    total_tokens = 0
    start_node_tokens = 0
    parent_startLine = 0
    summarized_parent_startLine = 0
    child_done_flag = 0
    start_summarize_flag = 0
    statement_flag = 0
    convert_sp_code = ""


    # * 노드 순회
    for result in node_data_list:
        node_start = result['n']
        relationship = result['r']
        node_end = result['m']
        relationship_name = relationship[1]
        start_node_tokens = node_start['token']
        print(f"시작 노드: {node_start['startLine']}, ({node_start['name']}), {node_start['endLine']}, tokens: {start_node_tokens}")
        print(f"관계: {relationship_name}")
        print(f"종료 노드: {node_end['startLine']}, ({node_end['name']}), {node_end['endLine']}, tokens: {node_end['token']}")
        print("-" * 40) 


        # * 조건문 가독성을 위해 변수 선언
        is_parent_done = parent_startLine == node_start['startLine']
        is_summarized_parent_done = summarized_parent_startLine == node_start['startLine']
        

        # * 각 부모 노드의 자식 노드를 모두 순회한 경우 플래그 초기화
        if is_parent_done or is_summarized_parent_done or child_done_flag:
            if is_parent_done:
                child_done_flag = 0
            if is_summarized_parent_done:
                start_summarize_flag = 0
            continue


        # * 토큰이 넘었다면 converting 진행
        if (start_node_tokens + total_tokens >= 1200 and convert_sp_code):
            (convert_sp_code, schedule_stack, total_tokens, variable_list) = await process_converting(convert_sp_code, schedule_stack, total_tokens, variable_list)
        elif not child_done_flag:
            total_tokens += start_node_tokens


        # * 각 노드의 타입에 따라서 플래그를 설정
        if relationship_name == "PARENT_OF":
            if start_node_tokens <= 1000 and not child_done_flag:
                parent_startLine = node_start['startLine']
                child_done_flag = 1
                statement_flag = 1
            elif start_node_tokens > 1000:
                start_summarize_flag = 1
                summarized_parent_startLine = node_start['startLine']
                statement_flag = 2
        else:
            statement_flag = 3


        # * 플래그 값에 따라서 converting할 코드를 생성 및 처리
        if statement_flag == 3 and not start_summarize_flag:
            convert_sp_code += "\n" + node_start['node_code']
        elif statement_flag == 2 and start_summarize_flag:
            parent_schedule_stack = await create_parent_skeleton(node_start['startLine'], node_end['startLine'], node_start['summarized_code'], parent_schedule_stack)
        elif statement_flag == 1 and not start_summarize_flag:
            convert_sp_code += "\n" + node_start['node_code']


        # * 변수 노드를 가져옵니다
        # TODO 수정 필요 한번에 다 들고와서 정리하는게 더 빠름
        start_line = node_end['startLine'] if relationship_name == "PARENT_OF" else node_start['startLine']
        variable_list = await fetch_variable_nodes(start_line)


# 역할: 서비스를 생성하기 위한 데이터를 준비하는 함수입니다.
# 매개변수: 
#   sp_fileName - 스토어드 프로시저 파일 이름
# 반환값: 없음 
async def start_service_processing(sp_fileName):
    
    # * 서비스 스켈레톤
    service_skeleton_code = """
package com.example.pbcac120calcsuipstd.service;

import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RestController;
import com.example.pbcac120calcsuipstd.command.OgadwCommand;
import org.springframework.beans.factory.annotation.Autowired;
import java.util.List;
import org.springframework.transaction.annotation.Transactional;

@RestController
@Transactional
public class OgadwService {

    @PostMapping(path="/calculate")
    public void calculate(@RequestBody OgadwCommand command) {
    }
}
    """

    # * Command 클래스의 변수
    procedure_variable = {
            "procedureParameters": [
                "p_TL_APL_ID IN VARCHAR2",
                "p_TL_ACC_ID IN VARCHAR2",
                "p_APPYYMM IN VARCHAR2",
                "p_WORK_GBN IN VARCHAR2",
                "p_WORK_DTL_GBN IN VARCHAR2",
                "p_INSR_CMPN_CD IN VARCHAR2",
                "p_WORK_EMP_NO IN VARCHAR2",
                "p_RESULT OUT VARCHAR2",
            ]
    }
    
    # * 전역 변수 초기화
    global fileName, procedure_variables, service_skeleton
    fileName = convert_to_lower_case_no_underscores(sp_fileName)
    procedure_variables = list(procedure_variable["procedureParameters"])
    service_skeleton = service_skeleton_code

    # * Neo4j 연결 생성
    connection = Neo4jConnection() 
    
    try:
        # * 노드와 관계를 가져오는 쿼리 
        # TODO 대문자로 변경 필요 
        query = [
            """
            MATCH (n)-[r]->(m)
            WHERE NOT (
                n:ROOT OR n:Variable OR n:DECLARE OR n:Table OR n:CREATE_PROCEDURE_BODY OR n:OPERATION OR
                m:ROOT OR m:Variable OR m:DECLARE OR m:Table OR m:CREATE_PROCEDURE_BODY OR m:OPERATION
            )
            RETURN n, r, m
            """
        ]
        
        
        # * 쿼리 실행
        results = await connection.execute_queries(query)
        

        # * 결과를 함수로 전달
        await process_service_class(results[0])


    except Exception as e:
        print(f"An error occurred from neo4j for service creation: {e}")
    finally:
        await connection.close() 


# 서비스 생성을 위한 테스트 모듈입니다.
class TestAnalysisMethod(unittest.IsolatedAsyncioTestCase):
    async def test_create_service(self):
        await start_service_processing("P_B_CAC120_CALC_SUIP_STD")


if __name__ == "__main__":
    unittest.main()