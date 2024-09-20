import ast
import json
import logging
import re
from prompt.service_prompt import convert_service_code
from prompt.parent_service_skeleton_prompt import convert_parent_skeleton
from understand.neo4j_connection import Neo4jConnection
from util.exception import ConvertingError, ExtractCodeError, HandleResultError, LLMCallError, Neo4jError, ProcessResultError, ServiceCreationError, TokenCountError, TraverseCodeError, VariableNodeError


# 역할: 주어진 범위에서 startLine과 endLine을 추출하여, 스토어드 프로시저 코드를 잘라내는 함수
# 매개변수: 
#   - sp_code : 스토어드 프로시저 코드
#   - context_range : 잘라낼 범위를 나타내는 딕셔너리의 리스트
# 반환값: 
#   - join(extracted_lines) : 범위에 맞게 추출된 스토어드 프로시저 코드
def extract_code_within_range(sp_code, context_range):
    try:
        if not (sp_code and context_range):
            return ""

        # * context_range에서 가장 작은 시작 라인과 가장 큰 끝 라인을 찾습니다.
        start_line = min(range_item['startLine'] for range_item in context_range)
        end_line = max(range_item['endLine'] for range_item in context_range)


        # * 코드를 라인별로 분리합니다.
        code_lines = sp_code.split('\n')
        

        # * 지정된 라인 번호를 기준으로 코드를 추출합니다.
        extracted_lines = [
            line for line in code_lines 
            if ':' in line and start_line <= int(line.split(':')[0].split('~')[0].strip()) <= end_line
        ]
        
        return '\n'.join(extracted_lines)
    
    except Exception:
        err_msg = "(전처리) 서비스 코드 생성 과정에서 범위내에 코드 추출 도중 문제가 발생했습니다."
        logging.exception(err_msg)
        raise ExtractCodeError(err_msg)


# 역할: 변수 노드에서 실제로 사용된 변수 노드만 추려서 제공하는 함수
# 매개변수: 
#   - node_id : 노드의 고유 식별자
#   - variable_dict : 현재 사용된 변수 정보를 담은 딕녀서리
#   - variable_node : 모든 변수 노드 리스트
# 반환값: 
#   - variable_dict : 사용된 변수 노드의 목록
async def process_variable_nodes(node_id, variable_dict, variable_node):
    try:

        # * 현재 노드 id를 기준으로 사용된 변수 노드들을 추출합니다.
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
        err_msg = "(전처리) 서비스 코드 생성 과정에서 사용된 변수 노드 추출 도중 문제가 발생했습니다."
        logging.exception(err_msg)
        raise VariableNodeError(err_msg)


# 역할 : 사이즈가 매우 큰 부모 노드를 처리하는 함수
# 매개변수 :
#   - start_line : 노드의 시작 라인
#   - summarized_code : 자식 노드가 요약 처리된 코드
#   - connection : Neo4j 연결 객체
async def process_over_size_node(start_line, summarized_code, connection):

    try:
        # * 노드 업데이트 쿼리를 저장할 리스트
        node_update_query = []


        # * 요약된 코드를 분석하여 결과를 가져옵니다
        analysis_result = convert_parent_skeleton(summarized_code)
        service_code = analysis_result['code']


        # * 노드의 속성으로 Java 코드를 추가하는 쿼리 생성
        query = f"MATCH (n) WHERE n.startLine = {start_line} SET n.java_code = '{service_code.replace('\n', '\\n').replace("'", "\\'")}'"
        node_update_query.append(query)     


        # * 생성된 쿼리를 실행하여 노드에 자바 속성을 추가
        await connection.execute_queries(node_update_query)

    except (Neo4jError, LLMCallError):
        raise
    except Exception:
        err_msg = "(전처리) 서비스 코드 생성 과정에서 사이즈가 큰 노드를 처리 도중 문제가 발생했습니다."
        logging.exception(err_msg)
        raise ProcessResultError(err_msg)


# 역할: llm에게 분석할 스토어드 프로시저 코드를 전달하고 받은 결과의 토큰 수에 따라 처리를 결정하는 함수
# 매개변수:
#   - convert_sp_code : 스토어드 프로시저 코드
#   - current_tokens : 총 토큰 수
#   - variable_dict : 사용된 변수 딕셔너리
#   - context_range : 컨텍스트 범위
#   - connection : Neo4j 연결 객체
#   - procedure_variables: command 클래스에 선언된 변수 목록 
#   - service_skeleton: 서비스 스켈레톤
#   - used_jpa_method_dict: 사용된 Jpa 쿼리 메서드 사전
# 반환값: 
#   - convert_sp_code : 초기화된 스토어드 프로시저 코드
#   - current_tokens : 초기화된 총 토큰 수
#   - variable_dict : 초기화된 변수 딕셔너리
#   - context_range : 초기화된 컨텍스트 범위
#   - used_jpa_method_dict : 초기화된 Jpa 쿼리 메서드 사전
async def process_convert_result(convert_sp_code, current_tokens, variable_dict, context_range, connection, procedure_variables, service_skeleton, used_jpa_method_dict):

    try:
        # * 노드 업데이트 쿼리를 저장할 리스트 및 범위 개수
        range_count = len(context_range)


        # * 전달된 정보를 llm에게 전달하여 결과를 받습니다.
        analysis_result = convert_service_code(convert_sp_code, service_skeleton, variable_dict, procedure_variables, context_range, range_count, used_jpa_method_dict)
        total_tokens = analysis_result['usage_metadata']['total_tokens']
        logging.info(f"토큰 수: {total_tokens}")




        # * 토큰 수가 최대를 넘었다면, 분할 처리를 진행합니다.
        if total_tokens > 4096 and range_count > 0:
            logging.info(" 토큰 초과 분할 시작")
            
            # * 분석할 범위에서 부모 범위를 추출하고 자식 범위들을 반으로 나눕니다.  
            largest_range_index = max(range(len(context_range)), key=lambda i: context_range[i]['endLine'] - context_range[i]['startLine'])
            parent_range = [context_range.pop(largest_range_index)]
            child_ranges = [context_range[:len(context_range)//2], context_range[len(context_range)//2:]]


            # * 분할된 범위를 순회합니다.   
            for current_range in [parent_range] + child_ranges:
                if current_range:
                    

                    # * 현재 범위를 기준으로 사용된 Jpa 쿼리 메서드를 다시 생성합니다. 
                    if current_range == parent_range:
                        current_jpa_methods = [
                            f"{key}: {value}" for key, value in used_jpa_method_dict.items()
                            if int(key.split('_')[-1].split('~')[1]) <= parent_range[0]['endLine']
                        ]
                    else:
                        current_jpa_methods = [
                            f"{key}: {value}" for key, value in used_jpa_method_dict.items()
                            if any(int(key.split('_')[-1].split('~')[0]) <= range_item['startLine'] <= int(key.split('_')[-1].split('~')[1]) for range_item in current_range)
                        ]


                    # * 현재 범위를 기준으로 사용된 변수를 다시 생성합니다. 
                    if current_range == parent_range:
                        current_variables = {
                            key: value for key, value in variable_dict.items()
                            if int(key.split('_')[1]) <= parent_range[0]['endLine']
                        }
                    else:
                        current_variables = {
                            key: value for key, value in variable_dict.items()
                            if any(int(key.split('_')[0]) == range_item['startLine'] for range_item in current_range)
                        }


                    # * 현재 범위의 코드 추출 및 변환을 진행합니다.
                    current_code = extract_code_within_range(convert_sp_code, current_range)
                    current_analysis = convert_service_code(current_code, service_skeleton, current_variables, procedure_variables, current_range, len(current_range), current_jpa_methods)                    
                    total_tokens = current_analysis['usage_metadata']['total_tokens']
                    logging.info(f"분할된 토탈 토큰 수: {total_tokens}")
                    await handle_convert_result(current_analysis['content'], connection)

        else:
            await handle_convert_result(analysis_result['content'], connection)


        # * 다음 사이클을 위해 각 종 변수를 초기화합니다.
        convert_sp_code = ""
        current_tokens = 0
        context_range.clear()
        variable_dict.clear()
        used_jpa_method_dict.clear()

        return (convert_sp_code, current_tokens, variable_dict, context_range, used_jpa_method_dict)
    
    except (ConvertingError, Neo4jError): 
        raise
    except Exception:
        err_msg = "(전처리) 서비스 코드 생성 과정에서 LLM의 결과를 결정하는 도중 문제가 발생했습니다."
        logging.exception(err_msg)
        raise ProcessResultError(err_msg)


# 역할: LLM의 결과를 이용해서 노드에 자바 속성 추가를 위한 사이퍼쿼리를 생성하는 함수
# 매개변수 : 
#   - analysis_result : LLM의 분석 결과
#   - connection : Neo4J의 연결 객체 
# 반환값 : 없음
async def handle_convert_result(analysis_result, connection):
    
    node_update_query = []
    
    try:
        # * 분석 결과 각각의 데이터를 추출하고, 자바 속성 추가를 위한 사이퍼쿼리를 생성합니다.
        for result in analysis_result['analysis']:
            for key, service_code in result.items():
                start_line, end_line = map(int, key.split('~'))
                query = f"MATCH (n) WHERE n.startLine = {start_line} SET n.java_code = '{service_code.replace('\n', '\\n').replace("'", "\\'")}'"
                node_update_query.append(query)        
        

        # * 노드 업데이트 쿼리를 실행
        await connection.execute_queries(node_update_query)

    except Neo4jError: 
        raise
    except Exception:
        err_msg = "(전처리) 서비스 코드 생성 과정에서 LLM의 결과를 처리하는 도중 문제가 발생했습니다."
        logging.exception(err_msg)
        raise HandleResultError(err_msg)


# 역할: 사용된 JPA 쿼리 메서드를 추가하는 함수 
# 매개변수: 
#   - used_jpa_method_dict : 사용된 JPA 쿼리 메서드 사전
#   - start_line : 시작 노드의 시작라인
#   - end_line : 시작 노드의 끝라인.
#   - jpa_method_list : 모든 JPA 쿼리 메서드 목록.
# 반환값: 
#   - used_jpa_method_dict : 사용된 JPA 쿼리 메서드 사전
async def extract_used_jpa_methods(used_jpa_method_dict, jpa_method_list, start_line, end_line):
    for method_dict in jpa_method_list:
        for key, value in method_dict.items():
            method_start, method_end = map(int, key.split('_')[-1].split('~'))
            if (start_line <= method_start <= end_line and
                start_line <= method_end <= end_line):
                used_jpa_method_dict[key] = value
                return used_jpa_method_dict
    return used_jpa_method_dict


# 역할: 자바 속성 추가를 위해 노드를 순회하하는 함수 
# 매개변수: 
#   - node_data_list : 노드와 관계에 대한 정보가 담긴 리스트
#   - connection : Neo4J 연결 객체
#   - service_skeleton : 서비스 클래스의 기본 구조.
#   - jpa_method_list : 사용된 JPA 메서드 목록.
#   - procedure_variable : 프로시저 선언부에서 사용된 변수 정보.
# 반환값: 없음 
async def traverse_node_for_service(node_list, connection, procedure_variables, service_skeleton, jpa_method_list):

    variable_dict = {}                    # 변수 정보를 저장하는 딕셔너리
    context_range = []                    # 분석할 컨텍스트 범위를 저장하는 리스트
    current_tokens = 0                      # 총 토큰 수
    convert_sp_code = ""                  # converting할 프로시저 코드 문자열
    traverse_node = node_list[0]          # 순회할 모든 노드 리스트
    variable_node = node_list[1]          # 변수 노드 리스트
    small_parent_info = {}                # 크기가 작은 부모 노드 정보
    big_parent_info = {}                  # 크기가 큰 부모 노드의 정보
    another_big_parent_startLine = 0      # 부모안에 또 다른 부모의 시작라인
    used_jpa_method_dict = {}             # 사용된 JPA 쿼리 메서드 사전

    try:
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
            if "EXECUTE_IMMDDIATE" in start_node['name']: continue


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
                (convert_sp_code, current_tokens, variable_dict, context_range, used_jpa_method_dict) = await process_convert_result(convert_sp_code, current_tokens, variable_dict, context_range, connection, procedure_variables, service_skeleton, used_jpa_method_dict)
                continue


            # * 가독성을 위해 복잡한 조건을 변수로 분리
            is_big_parent_processed = big_parent_info.get('nextLine', 0) == start_node['startLine']
            is_small_parent_processed = small_parent_info.get('nextLine', 0) == start_node['startLine']
            is_last_child_processed = small_parent_info.get('endLine') and small_parent_info['endLine'] < start_node['startLine']


            # * (큰) 최상위 부모와 같은 레벨인 다음 노드로 넘어갔을 경우, 최상위 부모 정보를 초기화합니다.
            if is_big_parent_processed:
                print(f"큰 부모 노드({big_parent_info['startLine']})의 모든 자식들 순회 완료")
                big_parent_info.clear()


            # * (작은) 최상위 부모와 같은 레벨인 다음 노드로 넘어갔을 경우, 최상위 부모 정보를 초기화합니다.
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
            is_token_limit_exceeded = (current_tokens + node_tokens >= 1000 or (current_tokens + node_tokens >= 1000 and len(context_range) >= 12)) and context_range 
        

            # * 총 토큰 수 검사를 진행합니다.
            if is_token_limit_exceeded:
                print(f"토큰 및 결과 범위 초과로 converting 진행합니다.")
                (convert_sp_code, current_tokens, variable_dict, context_range, used_jpa_method_dict) = await process_convert_result(convert_sp_code, current_tokens, variable_dict, context_range, connection, procedure_variables, service_skeleton, used_jpa_method_dict)
            print(f"토큰 합계 : {current_tokens + node_tokens}, 결과 개수 : {len(context_range)}")
            current_tokens += node_tokens


            # * 현재 노드에서 사용된 JPA 쿼리 메서드를 추출합니다. 
            used_jpa_method_dict = await extract_used_jpa_methods(used_jpa_method_dict, jpa_method_list, start_node['startLine'], start_node['endLine'])


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
                        variable_dict = await process_variable_nodes(end_node['startLine'], variable_dict, variable_node)
                
                # * 부모의 노드 크기가 작은 경우  
                else:
                    if not small_parent_info:
                        convert_sp_code += f"\n{start_node['node_code']}"
                        small_parent_info = {"startLine": start_node['startLine'], "endLine": start_node['endLine'], "nextLine": 0, "code": start_node['node_code'], "token": start_node['token']}            
                        if not big_parent_info: 
                            context_range.append({"startLine": start_node['startLine'], "endLine": start_node['endLine']}) 
                            variable_dict = await process_variable_nodes(start_node['startLine'], variable_dict, variable_node)
                        context_range.append({"startLine": end_node['startLine'], "endLine": end_node['endLine']})
                        variable_dict = await process_variable_nodes(end_node['startLine'], variable_dict, variable_node)
                    else:
                        context_range.append({"startLine": end_node['startLine'], "endLine": end_node['endLine']})
                        variable_dict = await process_variable_nodes(end_node['startLine'], variable_dict, variable_node)


            # * 단일 노드의 경우
            elif not small_parent_info and not big_parent_info:
                convert_sp_code += f"\n{start_node['node_code']}"
                context_range.append({"startLine": start_node['startLine'], "endLine": start_node['endLine']})
                variable_dict = await process_variable_nodes(start_node['startLine'], variable_dict, variable_node)
            elif another_big_parent_startLine == start_node['startLine'] and context_range and convert_sp_code: 
                print(f"부모 노드안에 또 다른 부모 노드의 순회 끝 converting 진행 -> 흐름이 섞이지 않게")
                (convert_sp_code, current_tokens, variable_dict, context_range, used_jpa_method_dict) = await process_convert_result(convert_sp_code, current_tokens, variable_dict, context_range, connection, procedure_variables, service_skeleton, used_jpa_method_dict)
            else:
                print("아무것도 처리되지 않습니다.")
        
        
        # * 마지막 그룹에 대한 처리를 합니다.
        if context_range and convert_sp_code:
            print("순회가 끝났지만 남은 context_range와 convert_sp_code가 있어 converting을 진행합니다.")
            (convert_sp_code, current_tokens, variable_dict, context_range, used_jpa_method_dict) = await process_convert_result(convert_sp_code, current_tokens, variable_dict, context_range, connection, procedure_variables, service_skeleton, used_jpa_method_dict)
    
    except (ConvertingError, Neo4jError): 
        raise
    except Exception:
        err_msg = "(전처리) 서비스 코드 생성 과정에서 노드를 순회하는 도중 문제가 발생했습니다."
        logging.exception(err_msg)
        raise TraverseCodeError(err_msg)



# 역할: 스토어드 프로시저 파일과 ANTLR 분석 파일을 읽어서 분석을 시작하는 메서드입니다.
# 매개변수: 
#   - service_skeleton : 서비스 스켈레톤
#   - jpa_method_list : 사용된 JPA 쿼리 메서드 목록
#   - procedure_variable : Command 클래스에 선언된 변수 목록
# 반환값: 없음 
async def start_service_Preprocessing(service_skeleton, jpa_method_list, procedure_variable):
    
    # * Neo4j 연결 생성
    connection = Neo4jConnection() 
    logging.info("(전처리) 서비스 생성을 시작합니다\n")
    
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
        
        # * 쿼리 실행하여, 노드를 (전처리) 서비스 생성 함수로 전달합니다
        results = await connection.execute_queries(node_query)        
        await traverse_node_for_service(results, connection, procedure_variable, service_skeleton, jpa_method_list)
        logging.info("(전처리) 서비스 생성 과정, 노드에 자바 속성을 추가 완료\n")


    except (ConvertingError, Neo4jError): 
        raise
    except Exception:
        err_msg = "(전처리) 서비스 코드 생성 과정하기 위해 준비하는 도중 문제가 발생했습니다."
        logging.exception(err_msg)
        raise ServiceCreationError(err_msg)
    finally:
        await connection.close()