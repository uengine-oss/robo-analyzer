import logging
import os
import re
import sys
import unittest
import tiktoken

logging.basicConfig(level=logging.INFO)
logging.getLogger('asyncio').setLevel(logging.ERROR)
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..', '..')))
from convert.converting_prompt.parent_skeleton_prompt import convert_parent_skeleton
from convert.converting_prompt.service_prompt import convert_code
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..', '..', '..')))
from understand.neo4j_connection import Neo4jConnection


# * 인코더 설정 및 파일 이름 및 변수 초기화 
encoder = tiktoken.get_encoding("cl100k_base")
fileName = None
procedure_variables = []
service_skeleton = None


# 역할: 주어진 범위에서 startLine과 endLine을 추출해서 스토어드 프로시저 코드를 잘라내는 함수입니다.
# 매개변수: 
#     - code : 스토어드 프로시저 코드
#     - context_range : 잘라낼 범위를 나타내는 딕셔너리의 리스트
# 반환값: 범위에 맞게 추출된 스토어드 프로시저 코드.
def extract_code_within_range(code, context_range):
    try:
        if not (code and context_range):
            return ""

        # * context_range에서 가장 작은 시작 라인과 가장 큰 끝 라인을 찾습니다.
        start_line = min(range_item['startLine'] for range_item in context_range)
        end_line = max(range_item['endLine'] for range_item in context_range)


        # * 코드를 라인별로 분리합니다.
        code_lines = code.split('\n')
        

        # * 지정된 라인 번호를 기준으로 코드를 추출합니다.
        extracted_lines = [
            line for line in code_lines 
            if ':' in line and start_line <= int(line.split(':')[0].split('~')[0].strip()) <= end_line
        ]
        
        return '\n'.join(extracted_lines)
    except Exception:
        logging.exception("Error occurred while extracting code within range(understanding)")
        raise


# 역할: 주어진 스토어드 프로시저 코드의 토큰의 개수를 계산하는 함수입니다.
# 매개변수: 
#      - code - 토큰을 계산할 스토어드 프로시저
# 반환값: 계산된 토큰의 수
def count_tokens_in_text(code):
    
    if not code: return 0

    try:
        # * 코드를 토큰화하고 토큰의 개수를 반환합니다.
        tokens = encoder.encode(code)
        return len(tokens)
    except Exception:
        logging.exception("Unexpected error occurred during token counting(understanding)")
        raise


# 역할 : 전달받은 이름을 전부 소문자로 전환하는 함수입니다,
# 매개변수 : 
#   - fileName : 스토어드 프로시저 파일의 이름
# 반환값 : 전부 소문자로 전환된 프로젝트 이름
def convert_to_lower_case_no_underscores(fileName):
    return fileName.replace('_', '').lower()


# 역할: 변수 노드에서 실제로 사용된 변수 노드만 추려서 제공하는 함수
# 매개변수: 
#      - node_id : 노드의 고유 식별자.
#      - variable_dict : 변수 딕녀서리
#      - variable_node : 모든 변수 노드 리스트
# 반환값: 노드에서 사용된 변수 노드의 목록.
async def fetch_variable_nodes(node_id, variable_dict, variable_node):
    try:

        # * 전달된 변수 정보 데이터를 이용해서 사용하게 쉽게 재구성 
        for node in variable_node:
            for key, value in node['v'].items():
                if '_' in key:
                    start, end = map(int, key.split('_'))
                    if start <= node_id <= end:
                        var_name = node['v']['name']
                        var_type = node['v'].get('type', 'Unknown')
                        var_role = value
                        var_info = f"{var_type} : {var_name}, {var_role}"
                        variable_dict[key] = var_info

        return variable_dict

    except Exception:
        logging.exception("Error during fetching variable nodes from Neo4J (converting)")
        raise


# 역할 : 노드를 업데이트할 쿼리를 실행하여 노드에 Java 속성을 추가하는 함수입니다.
# 매개변수 :
#   - node_update_query : 노드를 업데이트할 쿼리 리스트
async def process_node_update_java_properties(node_update_query, connection):

    # * Neo4j 연결 생성 및 쿼리 실행
    await connection.execute_queries(node_update_query)
    logging.info(f"Successfully updated node properties")


# 역할 : 사이즈가 매우 큰 부모 노드를 처리하는 함수입니다.
# 매개변수 :
#   - start_line : 노드의 시작 라인
#   - summarized_code : 자식 노드의 요약 처리된 코드
async def process_over_size_node(start_line, summarized_code, connection):

    # * 노드 업데이트 쿼리를 저장할 리스트
    node_update_query = []


    # * 요약된 코드를 분석하여 결과를 가져옵니다
    analysis_result = convert_parent_skeleton(summarized_code)
    logging.info(f"Successfully Converted Parent Node")
    service_code = analysis_result['code']


    # * Neo4j 데이터베이스에서 해당 노드의 Java 코드를 업데이트하는 쿼리 생성
    query = f"MATCH (n) WHERE n.startLine = {start_line} SET n.java_code = '{service_code.replace('\n', '\\n').replace("'", "\\'")}'"
    node_update_query.append(query)     


    # * 생성된 쿼리를 실행하는 함수를 호출
    await process_node_update_java_properties(node_update_query, connection)


# 역할: llm에게 분석할 스토어드 프로시저 코드를 전달한 뒤, 해당 결과를 바탕으로 Service를 생성합니다.
# 매개변수:
#   - convert_sp_code : 스토어드 프로시저 코드
#   - total_tokens : 총 토큰 수
#   - variable_dict : 변수 딕셔너리
#   - context_range : 컨텍스트 범위
# 반환값: 
#   - convert_sp_code : 초기화된 스토어드 프로시저 코드
#   - total_tokens : 초기화된 총 토큰 수
#   - variable_dict : 초기화된 변수 딕셔너리
#   - context_range : 컨텍스트 범위
async def process_converting(convert_sp_code, total_tokens, variable_dict, context_range, connection):
    
    try:
        # * 노드 업데이트 쿼리를 저장할 리스트 및 범위 개수
        parent_context_range = []
        range_count = len(context_range)


        # * 프로시저 코드를 분석합니다(llm에게 전달)
        analysis_result, prompt_template = convert_code(convert_sp_code, service_skeleton, variable_dict, procedure_variables, context_range, range_count)
        combined_context = f"{prompt_template}\n{context_range}\n{service_skeleton}\n{variable_dict}\n{procedure_variables}\n{analysis_result}"
        combined_context_tokens = count_tokens_in_text(combined_context)
        logging.info(f"토큰 수 : {combined_context_tokens + total_tokens}")
        
        if combined_context_tokens + total_tokens > 4096 and range_count > 1:
            largest_range_index = max(range(len(context_range)), key=lambda i: context_range[i]['endLine'] - context_range[i]['startLine'])
            parent_context_range.append(context_range.pop(largest_range_index))
            half_point = len(context_range) // 2

            for sub_range in [context_range[:half_point], context_range[half_point:]]:
                if sub_range:
                    extract_range_code = extract_code_within_range(convert_sp_code, sub_range)
                    sub_analysis_result, _ = convert_code(convert_sp_code, service_skeleton, variable_dict, procedure_variables, sub_range, len(sub_range))
                    await handle_analysis_result(sub_analysis_result, connection)
            
            if parent_context_range:
                    parent_analysis_result, _ = convert_code(convert_sp_code, service_skeleton, variable_dict, procedure_variables, parent_context_range, len(parent_context_range))
                    await handle_analysis_result(parent_analysis_result, connection)
        else:
            extract_range_code = extract_code_within_range(convert_sp_code, context_range)
            await handle_analysis_result(analysis_result, connection, extract_range_code)


        logging.info(f"\nsuccessfully converted code\n")
        convert_sp_code = ""
        total_tokens = 0
        context_range.clear()
        variable_dict.clear()

        return (convert_sp_code, total_tokens, variable_dict, context_range)

    except Exception:
        logging.exception("An error occurred during analysis results processing(converting)")
        raise


# 역할: 
# 매개변수 : 
#   - analysis_result :
# 반환값 : 
async def handle_analysis_result(analysis_result, connection):
    
    node_update_query = []

    # * 분석 결과 각각의 데이터를 추출하고, 필요한 변수를 초기화합니다 
    for result in analysis_result['analysis']:
        start_line = result['range']['startLine']
        service_code = result['code']
        query = f"MATCH (n) WHERE n.startLine = {start_line} SET n.java_code = '{service_code.replace('\n', '\\n').replace("'", "\\'")}'"
        node_update_query.append(query)        
    

    # * 노드 업데이트 쿼리를 실행하는 메서드를 호출 
    await process_node_update_java_properties(node_update_query, connection)



# 역할: 각 노드에 자바 속성을 추가하기 위한 로직이 담긴 함수입니다.
# 매개변수: 
#   node_data_list - 노드와 관계에 대한 정보가 담긴 리스트
# 반환값: 없음 
async def process_service_class(node_list, connection):

    variable_dict = {}                    # 변수 정보를 저장하는 딕셔너리
    context_range = []                    # 분석할 컨텍스트 범위를 저장하는 리스트
    total_tokens = 0                      # 총 토큰 수
    convert_sp_code = ""                  # converting할 프로시저 코드 문자열
    traverse_node = node_list[0]          # 순회할 노드 리스트
    variable_node = node_list[1]          # 변수 노드 리스트
    small_parent_info = {}
    big_parent_info = {}
    another_big_parent_startLine = 0

    # * Converting 하기 위한 노드의 순회 시작
    for node in traverse_node:
        start_node = node['n']
        relationship = node['r'][1] if node['r'] else "NEXT"
        end_node = node['m']
        node_tokens = 0
        print("\n"+"-" * 40) 
        print(f"시작 노드 : [ 시작 라인 : {start_node['startLine']}, 이름 : ({start_node['name']}), 끝라인: {start_node['endLine']}, 토큰 : {start_node['token']}")
        print(f"관계: {relationship}")
        if end_node: print(f"종료 노드 : [ 시작 라인 : {end_node['startLine']}, 이름 : ({end_node['name']}), 끝라인: {end_node['endLine']}, 토큰 : {end_node['token']}")
        
        
        # * 가독성을 위해 복잡한 조건을 변수로 
        is_small_parent_traverse_1deth = start_node['startLine'] == small_parent_info.get("startLine", 0) and relationship == "NEXT"
        is_big_parent_traverse_1deth = start_node['startLine'] == big_parent_info.get("startLine", 0) and relationship == "NEXT"


        # * context_range에서 시작라인이 가능 작은 순서로 정렬 (llm이 혼동하지 않게)
        context_range = sorted(context_range, key=lambda x: x['startLine'])
        
        
        # * 현재 노드의 시작라인이 최상위 부모 노드와 같다면, 1단계 깊이 자식들 순회완료로 다음 레벨의 시작라인을 저장
        if is_small_parent_traverse_1deth:
            print(f"작은 부모 노드({start_node['startLine']})의 1단계 깊이 자식들 순회 완료")
            small_parent_info["nextLine"] = end_node['startLine']
            continue
        elif is_big_parent_traverse_1deth:
            print(f"큰 부모 노드({start_node['startLine']})의 1단계 깊이 자식들 순회 완료")
            big_parent_info["nextLine"] = end_node['startLine']
            (convert_sp_code, total_tokens, variable_dict, context_range) = await process_converting(convert_sp_code, total_tokens, variable_dict, context_range, connection)
            continue


        # * 가독성을 위해 복잡한 조건을 변수로 분리
        is_big_parent_processed = big_parent_info.get('nextLine', 0) == start_node['startLine']
        is_small_parent_processed = small_parent_info.get('nextLine', 0) == start_node['startLine']
        is_last_child_processed = small_parent_info.get('endLine') and small_parent_info['endLine'] < start_node['startLine']


        # * 최상위 부모와 같은 레벨인 다음 노드로 넘어갔을 경우, 최상위 부모 정보를 초기화합니다. (큰)
        if is_big_parent_processed:
            print(f"큰 부모 노드({big_parent_info['startLine']})의 모든 자식들 순회 완료")
            big_parent_info.clear()


        # * 최상위 부모와 같은 레벨인 다음 노드로 넘어갔을 경우, 최상위 부모 정보를 초기화합니다. (작은)
        if is_small_parent_processed:
            print(f"작은 부모 노드({small_parent_info['startLine']})의 모든 자식들 순회 완료1")
            small_parent_info.clear()
        elif is_last_child_processed:
            print(f"작은 부모 노드({small_parent_info['startLine']})의 모든 자식들 순회 완료2")
            small_parent_info.clear()


        # * 가독성을 위해 조건을 변수로 분리
        is_big_parent_and_small_child = relationship == "PARENT_OF" and start_node['token'] > 1700 and end_node['token'] < 1700
        is_small_parent = relationship == "PARENT_OF" and start_node['token'] < 1700 and not small_parent_info
        is_single_node = relationship == "NEXT" and not small_parent_info and not big_parent_info


        # * 각 노드의 타입에 따라서 어떤 노드의 토큰을 더 할지를 결정합니다. 
        if is_big_parent_and_small_child:
            node_tokens += end_node['token']
        elif is_small_parent or is_single_node:
            node_tokens += start_node['token']


        # * 가독성을 위해 조건을 변수로 분리
        is_token_limit_exceeded = (total_tokens + node_tokens >= 1400 or len(context_range) >= 10) and context_range 
    

        # * 총 토큰 수 검사를 진행합니다.
        if is_token_limit_exceeded:
            print(f"토큰 및 결과 범위 초과로 converting 진행합니다.")
            (convert_sp_code, total_tokens, variable_dict, context_range) = await process_converting(convert_sp_code, total_tokens, variable_dict, context_range, connection)
        print(f"토큰 합계 : {total_tokens + node_tokens}, 결과 개수 : {len(context_range)}")
        total_tokens += node_tokens


        # * 가독성을 위해 조건을 변수로 분리
        is_small_parent_reassignment = small_parent_info and not convert_sp_code and relationship == "PARENT_OF"


        # * 크기가 작은 부모에 대한 자식들 처리 도중에 context range 개수 초과로 converting이 되었을 때를 위한 할당   
        if is_small_parent_reassignment:
            print(f"다시 부모 정보를 할당")
            convert_sp_code = small_parent_info['code']
            total_tokens = small_parent_info['token']


        # * 관계 타입에 따라 노드의 토큰 수를 파악하여 각 변수값을 할당합니다.  
        # TODO 최적화 필요 HOW ? 중복 로직을 함수로 빼난다? 조건도 너무 복잡한데.. 
        if relationship == "PARENT_OF":
            
            # * 부모 노드의 크기가 매우 큰 경우 처리 
            if start_node['token'] >= 1700:
                if not big_parent_info: 
                    await process_over_size_node(start_node['startLine'], start_node['summarized_code'], connection)
                    big_parent_info = {"startLine": start_node['startLine'], "nextLine": 0}
                if end_node['token'] >= 1700:
                    await process_over_size_node(end_node['startLine'], end_node['summarized_code'], connection)
                else:
                    another_big_parent_startLine = start_node['startLine']
                    convert_sp_code += f"\n{end_node['node_code']}"
                    context_range.append({"startLine": end_node['startLine'], "endLine": end_node['endLine']})
                    variable_dict = await fetch_variable_nodes(end_node['startLine'], variable_dict, variable_node)
            
            # * 부모의 노드 크기가 작은 경우  
            else:
                if not small_parent_info:
                    convert_sp_code += f"\n{start_node['node_code']}"
                    small_parent_info = {"startLine": start_node['startLine'], "endLine": start_node['endLine'], "nextLine": 0, "code": start_node['node_code'], "token": start_node['token']}            
                    if not big_parent_info: 
                        context_range.append({"startLine": start_node['startLine'], "endLine": start_node['endLine']}) 
                        variable_dict = await fetch_variable_nodes(start_node['startLine'], variable_dict, variable_node)
                    context_range.append({"startLine": end_node['startLine'], "endLine": end_node['endLine']})
                    variable_dict = await fetch_variable_nodes(end_node['startLine'], variable_dict, variable_node)
                else:
                    context_range.append({"startLine": end_node['startLine'], "endLine": end_node['endLine']})
                    variable_dict = await fetch_variable_nodes(end_node['startLine'], variable_dict, variable_node)


        # * 단일 노드의 경우
        elif not small_parent_info and not big_parent_info:
            convert_sp_code += f"\n{start_node['node_code']}"
            context_range.append({"startLine": start_node['startLine'], "endLine": start_node['endLine']})
            variable_dict = await fetch_variable_nodes(start_node['startLine'], variable_dict, variable_node)
        elif another_big_parent_startLine == start_node['startLine'] and context_range and convert_sp_code: 
            print(f"부모 노드안에 또 다른 부모 노드의 순회 끝 converting 진행 -> 흐름이 섞이지 않게")
            (convert_sp_code, total_tokens, variable_dict, context_range) = await process_converting(convert_sp_code, total_tokens, variable_dict, context_range, connection)
        else:
            print("아무것도 처리되지 않습니다.")
    
    
    # * 마지막 그룹에 대한 처리를 합니다.
    if context_range and convert_sp_code:
        print("순회가 끝났지만 남은 context_range와 convert_sp_code가 있어 converting을 진행합니다.")
        (convert_sp_code, total_tokens, variable_dict, context_range) = await process_converting(convert_sp_code, total_tokens, variable_dict, context_range, connection)


# 역할: 각 노드에 자바 코드를 속성으로 추가하기 위한 준비 및 시작 함수
# 매개변수: 
#   sp_fileName - 스토어드 프로시저 파일 이름
# 반환값: 없음 
async def start_service_processing(sp_fileName):
    
    # * 서비스 스켈레톤
    service_skeleton_code = """
@RestController
public class OgadwService {{
    @PostMapping(path="/calculate")
    public void calculate(@RequestBody OgadwCommand command) {{
            //Here is business logic 
    }}

}}
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
    

    # * 전역 변수 초기화(프로시저 파일, command_class 변수, 서비스 틀)
    global fileName, procedure_variables, service_skeleton
    fileName = convert_to_lower_case_no_underscores(sp_fileName)
    procedure_variables = list(procedure_variable["procedureParameters"])
    service_skeleton = service_skeleton_code


    # * Neo4j 연결 생성
    connection = Neo4jConnection() 
    
    try:
        # * 노드와 관계를 가져오는 쿼리 
        node_query = [
            """
            MATCH (n)
            WHERE NOT (n:ROOT OR n:Variable OR n:DECLARE OR n:Table OR n:CREATE_PROCEDURE_BODY)
            OPTIONAL MATCH (n)-[r]->(m)
            WHERE NOT (m:ROOT OR m:Variable OR m:DECLARE OR m:Table OR m:CREATE_PROCEDURE_BODY)
            RETURN n, r, m
            ORDER BY n.startLine
            """,
            "MATCH (v:Variable) RETURN v"
        ]

        
        # * 쿼리 실행
        results = await connection.execute_queries(node_query)
        

        # * 결과를 함수로 전달
        await process_service_class(results, connection)


    except Exception as e:
        logging.exception(f"An error occurred from neo4j for service creation: {e}")
    finally:
        await connection.close() 


# 서비스 생성을 위한 테스트 모듈입니다.
class TestAnalysisMethod(unittest.IsolatedAsyncioTestCase):
    async def test_create_service(self):
        await start_service_processing("P_B_CAC120_CALC_SUIP_STD")


if __name__ == "__main__":
    unittest.main()