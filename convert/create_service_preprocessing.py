from collections import defaultdict
import json
import logging
from prompt.service_prompt import convert_service_code
from prompt.parent_service_skeleton_prompt import convert_parent_skeleton
from understand.neo4j_connection import Neo4jConnection
from util.exception import ConvertingError, ExtractCodeError, HandleResultError, LLMCallError, Neo4jError, ProcessResultError, ServiceCreationError, TokenCountError, TraverseCodeError, VariableNodeError


# 역할: 변수 노드에서 실제로 사용된 변수 노드만 추려서 제공하는 함수
# 매개변수: 
#   - node_id : 노드의 고유 식별자
#   - used_variables : 현재 사용된 변수 정보를 담은 딕녀서리
#   - variable_nodes : 모든 변수 노드 리스트
#   - tracking_variables : 변수 정보 추적을 위한 사전
# 반환값: 
#   - used_variables : 사용된 변수 노드의 목록
async def process_variable_nodes(node_id, used_variables, variable_nodes, tracking_variables):
    
    try:
        # * 현재 노드 id를 기준으로 사용된 변수 노드들을 추출합니다.
        for node in variable_nodes:
            for key in node['v']:
                if '_' in key and all(part.isdigit() for part in key.split('_')):
                    start, end = map(int, key.split('_'))
                    if start <= node_id <= end:
                        var_name = node['v']['name']
                        var_type = node['v'].get('type', 'Unknown')
                        var_role = tracking_variables.get(var_name, '초기값(0 또는 "")')                  
                        var_info = {
                            'type': var_type,
                            'name': var_name,
                            'role': var_role
                        }

                        # * 중복 검사: name을 기준으로 중복 체크(TODO 성능이슈)
                        if not any(existing_var['name'] == var_name for existing_var in used_variables):
                            used_variables.append(var_info)
                        break

        return used_variables

    except Exception:
        err_msg = "(전처리) 서비스 코드 생성 과정에서 사용된 변수 노드 추출 도중 문제가 발생했습니다."
        logging.error(err_msg, exc_info=False)
        raise VariableNodeError(err_msg)


# 역할 : 사이즈가 매우 큰 부모 노드를 처리하는 함수
# 매개변수 :
#   - start_line : 노드의 시작 라인
#   - summarized_code : 자식 노드가 요약 처리된 코드
#   - connection : Neo4j 연결 객체
#   - object_name : 패키지 및 프로시저 이름
async def process_over_size_node(start_line, summarized_code, connection, object_name):

    try:
        # * 노드 업데이트 쿼리를 저장할 리스트
        node_update_query = []


        # * 요약된 코드를 분석하여 결과를 가져옵니다
        analysis_result = convert_parent_skeleton(summarized_code)
        service_code = analysis_result['code']


        # * 노드의 속성으로 Java 코드를 추가하는 쿼리 생성
        query = f"MATCH (n) WHERE n.startLine = {start_line} AND n.object_name = '{object_name}' SET n.java_code = '{service_code.replace('\n', '\\n').replace("'", "\\'")}'"
        node_update_query.append(query)     


        # * 생성된 쿼리를 실행하여 노드에 자바 속성을 추가
        await connection.execute_queries(node_update_query)

    except (Neo4jError, LLMCallError):
        raise
    except Exception:
        err_msg = "(전처리) 서비스 코드 생성 과정에서 사이즈가 큰 노드를 처리 도중 문제가 발생했습니다."
        logging.error(err_msg, exc_info=False)
        raise ProcessResultError(err_msg)


# 역할: llm에게 분석할 스토어드 프로시저 코드를 전달하고 받은 결과의 토큰 수에 따라 처리를 결정하는 함수
# 매개변수:
#   - convert_sp_code : 스토어드 프로시저 코드
#   - current_tokens : 총 토큰 수
#   - used_variables : 사용된 변수 리스트
#   - context_range : 컨텍스트 범위
#   - connection : Neo4j 연결 객체
#   - command_class_variable: command 클래스에 선언된 변수 목록 
#   - method_skeleton: 메서드 스켈레톤
#   - used_jpa_method_dict: 사용된 Jpa 쿼리 메서드 사전
#   - tracking_variables : 변수 정보 추적을 위한 사전
#   - object_name : 패키지 및 프로시저 이름
# 반환값: 
#   - convert_sp_code : 초기화된 스토어드 프로시저 코드
#   - current_tokens : 초기화된 총 토큰 수
#   - used_variables : 초기화된 변수 리스트
#   - context_range : 초기화된 컨텍스트 범위
#   - used_jpa_method_dict : 초기화된 Jpa 쿼리 메서드 사전
#   - tracking_variables : 초기화된 변수 정보 추적을 위한 사전
async def process_convert_result(convert_sp_code, current_tokens, used_variables, context_range, connection, command_class_variable, method_skeleton, used_jpa_method_dict, tracking_variables, object_name):

    try:
        # * 노드 업데이트 쿼리를 저장할 리스트 및 범위 개수
        range_count = len(context_range)


        # * 전달된 정보를 llm에게 전달하여 결과를 받고, 결과를 처리하는 함수를 호출합니다.
        analysis_result = convert_service_code(convert_sp_code, method_skeleton, used_variables, command_class_variable, context_range, range_count, used_jpa_method_dict)
        tracking_variables = await handle_convert_result(analysis_result, connection, tracking_variables, object_name)


        # * 다음 사이클을 위해 각 종 변수를 초기화합니다.
        convert_sp_code = ""
        current_tokens = 0
        context_range.clear()
        used_variables.clear()
        used_jpa_method_dict.clear()

        return (convert_sp_code, current_tokens, used_variables, context_range, used_jpa_method_dict, tracking_variables)
    
    except (ConvertingError, Neo4jError): 
        raise
    except Exception:
        err_msg = "(전처리) 서비스 코드 생성 과정에서 LLM의 결과를 결정하는 도중 문제가 발생했습니다."
        logging.error(err_msg, exc_info=False)
        raise ProcessResultError(err_msg)


# 역할: LLM의 결과를 이용해서 노드에 자바 속성 추가를 위한 사이퍼쿼리를 생성하는 함수
# 매개변수 : 
#   - analysis_result : LLM의 분석 결과
#   - connection : Neo4J의 연결 객체 
#   - tracking_variables : 변수 정보 추적을 위한 사전
#   - object_name : 패키지 및 프로시저 이름
# 반환값 :
#   - tracking_variables : 초기화된 변수 정보 추적을 위한 사전
async def handle_convert_result(analysis_result, connection, tracking_variables, object_name):
    
    node_update_query = []
    
    try:
        # * 분석 결과에서 코드와 변수 정보를 추출합니다.
        code_info = analysis_result['analysis'].get('code', {})
        variables_info = analysis_result['analysis'].get('variables', {})
        

        # * 코드 정보를 추출하고, 자바 속성 추가를 위한 사이퍼쿼리를 생성합니다.
        for key, service_code in code_info.items():
            start_line, end_line = map(int, key.split('~'))
            query = f"MATCH (n) WHERE n.startLine = {start_line} AND n.object_name = '{object_name}' SET n.java_code = '{service_code.replace('\n', '\\n').replace("'", "\\'")}'"
            node_update_query.append(query)        


        # * 변수 정보를 tracking_variables에 업데이트합니다.
        for var_name, var_info in variables_info.items():
            tracking_variables[var_name] = var_info       
            query = f"""
            MATCH (n:Variable) 
            WHERE n.object_name = '{object_name}' 
            AND n.name = '{var_name}'
            SET n.value_tracking = {json.dumps(var_info)}
            """
            node_update_query.append(query)


        # * 노드 업데이트 쿼리를 실행
        await connection.execute_queries(node_update_query)


        return tracking_variables

    except Neo4jError: 
        raise
    except Exception:
        err_msg = "(전처리) 서비스 코드 생성 과정에서 LLM의 결과를 처리하는 도중 문제가 발생했습니다."
        logging.error(err_msg, exc_info=False)
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
            method_start, method_end = map(int, key.split('~'))
            if (start_line <= method_start <= end_line and
                start_line <= method_end <= end_line):
                used_jpa_method_dict[key] = value
                break
    return used_jpa_method_dict


# 역할: 자바 속성 추가를 위해 노드를 순회하는 함수 
# 매개변수: 
#   - node_data_list : 노드와 관계에 대한 정보가 담긴 리스트
#   - connection : Neo4J 연결 객체
#   - method_skeleton : 메서드 기본 구조 틀
#   - jpa_method_list : 사용된 JPA 메서드 목록.
#   - command_class_variable : 커맨드 클래스에 선언된 변수 목록
#   - object_name : 패키지 및 프로시저 이름
# 반환값: 없음 
async def traverse_node_for_service(node_list, connection, command_class_variable, method_skeleton, jpa_method_list, object_name):

    used_variables =  []                  # 사용된 변수 정보를 저장하는 리스트
    context_range = []                    # 분석할 컨텍스트 범위를 저장하는 리스트
    current_tokens = 0                    # 총 토큰 수
    convert_sp_code = ""                  # converting할 프로시저 코드 문자열
    traverse_nodes = node_list[0]         # 순회할 모든 노드 리스트
    variable_nodes = node_list[1]         # 모든 변수 노드 리스트
    small_parent_info = {}                # 크기가 작은 부모 노드 정보
    big_parent_info = {}                  # 크기가 큰 부모 노드의 정보
    another_big_parent_startLine = 0      # 부모안에 또 다른 부모의 시작라인
    used_jpa_method_dict = {}             # 사용된 JPA 쿼리 메서드 사전
    tracking_variables = {}               # 변수 정보를 추적하기 위한 사전


    try:
        # * Converting 하기 위한 노드의 순회 시작
        for node in traverse_nodes:
            start_node = node['n']
            relationship = node['r'][1] if node['r'] else "NEXT"
            end_node = node['m']
            node_tokens = 0
            print("\n"+"-" * 40) 
            print(f"시작 노드 : [ 시작 라인 : {start_node['startLine']}, 이름 : ({start_node['name']}), 끝라인: {start_node['endLine']}, 토큰 : {start_node['token']}")
            print(f"관계: {relationship}")
            if end_node: print(f"종료 노드 : [ 시작 라인 : {end_node['startLine']}, 이름 : ({end_node['name']}), 끝라인: {end_node['endLine']}, 토큰 : {end_node['token']}")
            if "EXECUTE_IMMDDIATE" in start_node['name']: continue


            # * 각 부모 노드의 1단계 깊이 자식들 순회 여부를 확인하는 조건
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
                (convert_sp_code, current_tokens, used_variables, context_range, used_jpa_method_dict, tracking_variables) = await process_convert_result(convert_sp_code, current_tokens, used_variables, context_range, connection, command_class_variable, method_skeleton, used_jpa_method_dict, tracking_variables, object_name)
                continue


            # * 각 부모 노드 및 마지막 자식의 처리 여부를 확인하는 조건
            is_big_parent_processed = big_parent_info.get('nextLine', 0) == start_node['startLine']
            is_small_parent_processed = small_parent_info.get('nextLine', 0) == start_node['startLine']
            is_last_child_processed = small_parent_info.get('endLine', 0) and small_parent_info.get('endLine', 0) < start_node['startLine']


            # * (큰) 최상위 부모 처리가 끝나고, 같은 레벨인 다음 노드로 넘어갔을 경우, 최상위 부모 정보를 초기화합니다.
            if is_big_parent_processed:
                print(f"큰 부모 노드({big_parent_info['startLine']})의 모든 자식들 순회 완료")
                big_parent_info.clear()


            # * (작은) 최상위 부모 처리가 끝나고, 같은 레벨인 다음 노드로 넘어갔을 경우, 최상위 부모 정보를 초기화합니다.
            if is_small_parent_processed:
                print(f"작은 부모 노드({small_parent_info['startLine']})의 모든 자식들 순회 완료1")
                small_parent_info.clear()
            elif is_last_child_processed:
                print(f"작은 부모 노드({small_parent_info['startLine']})의 모든 자식들 순회 완료2")
                small_parent_info.clear()


            # * 각 노드의 타입에 따른 조건
            is_big_parent_and_small_child = relationship == "PARENT_OF" and start_node['token'] > 1700 and end_node['token'] < 1700
            is_small_parent = relationship == "PARENT_OF" and start_node['token'] < 1700 and not small_parent_info
            is_single_node = relationship == "NEXT" and not small_parent_info and not big_parent_info


            # * 각 노드의 타입에 따라서 어떤 노드의 토큰을 더 할지를 결정합니다. 
            if is_big_parent_and_small_child:
                node_tokens += end_node['token']
            elif is_small_parent or is_single_node:
                node_tokens += start_node['token']


            # * 총 토큰 수 및 결과 개수 초과 여부를 확인하는 조건
            is_token_limit_exceeded = (current_tokens + node_tokens >= 1500) and context_range 


            # * 총 토큰 수 검사를 진행합니다.
            if is_token_limit_exceeded:
                print(f"토큰 및 결과 범위 초과로 converting 진행합니다.")
                (convert_sp_code, current_tokens, used_variables, context_range, used_jpa_method_dict, tracking_variables) = await process_convert_result(convert_sp_code, current_tokens, used_variables, context_range, connection, command_class_variable, method_skeleton, used_jpa_method_dict, tracking_variables, object_name)
            print(f"토큰 합계 : {current_tokens + node_tokens}, 결과 개수 : {len(context_range)}")
            current_tokens += node_tokens


            # * 특정 부모에 대한 자식 처리 도중 결과 개수 초과로 converting이 되었을 때를 위한 할당   
            if small_parent_info and not convert_sp_code and relationship == "PARENT_OF":
                print(f"다시 부모 정보를 할당")
                convert_sp_code = small_parent_info['code']
                current_tokens = small_parent_info['token']


            # * 관계 타입에 따라 노드의 토큰 수를 파악하여 각 변수값을 할당합니다.  
            if relationship == "PARENT_OF":
                
                # * 부모 노드의 크기가 매우 큰 경우 처리 
                if start_node['token'] >= 1700:
                    if not big_parent_info: 
                        await process_over_size_node(start_node['startLine'], start_node['summarized_code'], connection, object_name)
                        big_parent_info = {"startLine": start_node['startLine'], "nextLine": 0}
                    if end_node['token'] >= 1700:
                        await process_over_size_node(end_node['startLine'], end_node['summarized_code'], connection, object_name)
                    else:
                        another_big_parent_startLine = start_node['startLine']
                        convert_sp_code += f"\n{end_node['node_code']}"
                        context_range.append({"startLine": end_node['startLine'], "endLine": end_node['endLine']})
                        used_variables = await process_variable_nodes(end_node['startLine'], used_variables, variable_nodes, tracking_variables)
                        used_jpa_method_dict = await extract_used_jpa_methods(used_jpa_method_dict, jpa_method_list, end_node['startLine'], end_node['endLine'])

                # * 부모의 노드 크기가 작은 경우  
                else:
                    if not small_parent_info:
                        convert_sp_code += f"\n{start_node['node_code']}"
                        small_parent_info = {"startLine": start_node['startLine'], "endLine": start_node['endLine'], "nextLine": 0, "code": start_node['node_code'], "token": start_node['token']}            
                        if not big_parent_info: 
                            context_range.append({"startLine": start_node['startLine'], "endLine": start_node['endLine']}) 
                            used_variables = await process_variable_nodes(start_node['startLine'], used_variables, variable_nodes, tracking_variables)
                            used_jpa_method_dict = await extract_used_jpa_methods(used_jpa_method_dict, jpa_method_list, start_node['startLine'], start_node['endLine'])
                        context_range.append({"startLine": end_node['startLine'], "endLine": end_node['endLine']})
                        used_variables = await process_variable_nodes(end_node['startLine'], used_variables, variable_nodes, tracking_variables)
                        used_jpa_method_dict = await extract_used_jpa_methods(used_jpa_method_dict, jpa_method_list, end_node['startLine'], end_node['endLine'])
                    else:
                        context_range.append({"startLine": end_node['startLine'], "endLine": end_node['endLine']})
                        used_variables = await process_variable_nodes(end_node['startLine'], used_variables, variable_nodes, tracking_variables)
                        used_jpa_method_dict = await extract_used_jpa_methods(used_jpa_method_dict, jpa_method_list, end_node['startLine'], end_node['endLine'])


            # * 단일 노드의 경우
            elif not small_parent_info and not big_parent_info:
                convert_sp_code += f"\n{start_node['node_code']}"
                context_range.append({"startLine": start_node['startLine'], "endLine": start_node['endLine']})
                used_variables = await process_variable_nodes(start_node['startLine'], used_variables, variable_nodes, tracking_variables)
                used_jpa_method_dict = await extract_used_jpa_methods(used_jpa_method_dict, jpa_method_list, start_node['startLine'], start_node['endLine'])
            elif another_big_parent_startLine == start_node['startLine'] and context_range and convert_sp_code: 
                print(f"부모 노드안에 또 다른 부모 노드의 순회 끝 converting 진행 -> 흐름이 섞이지 않게")
                (convert_sp_code, current_tokens, used_variables, context_range, used_jpa_method_dict, tracking_variables) = await process_convert_result(convert_sp_code, current_tokens, used_variables, context_range, connection, command_class_variable, method_skeleton, used_jpa_method_dict, tracking_variables, object_name)
            else:
                print("아무것도 처리되지 않습니다.")
        
        
        # * 마지막 그룹에 대한 처리를 합니다.
        if context_range and convert_sp_code:
            print("순회가 끝났지만 남은 context_range와 convert_sp_code가 있어 converting을 진행합니다.")
            (convert_sp_code, current_tokens, used_variables, context_range, used_jpa_method_dict, tracking_variables) = await process_convert_result(convert_sp_code, current_tokens, used_variables, context_range, connection, command_class_variable, method_skeleton, used_jpa_method_dict, tracking_variables, object_name)
    
    except (ConvertingError, Neo4jError): 
        raise
    except Exception:
        err_msg = "(전처리) 서비스 코드 생성 과정에서 노드를 순회하는 도중 문제가 발생했습니다."
        logging.error(err_msg, exc_info=False)
        raise TraverseCodeError(err_msg)



# 역할: 각 노드에 Java_code라는 이름으로 자바 코드 속성을 추가하기 위해 순회하는 과정입니다. 
# 매개변수: 
#   - method_skeleton : 메서드 스켈레톤
#   - jpa_method_list : 사용된 JPA 쿼리 메서드 목록
#   - command_class_variable : Command 클래스에 선언된 변수 목록
#   - object_name : 패키지 및 프로시저 이름
# 반환값: 없음 
async def start_service_preprocessing(method_skeleton, command_class_variable, procedure_name, jpa_method_list, object_name):
    
    # * Neo4j 연결 생성
    connection = Neo4jConnection() 
    logging.info(f"[{object_name}] (전처리) 서비스 생성을 시작합니다\n")
    
    
    try:
        # * 노드와 관계를 가져오는 쿼리 
        node_query = [
            f"""
            MATCH (n)
            WHERE n.object_name = '{object_name}'
            AND n.procedure_name = '{procedure_name}'
            AND (n:FUNCTION OR n:PROCEDURE OR n:CREATE_PROCEDURE_BODY)
            AND NOT (n:ROOT OR n:Variable OR n:DECLARE OR n:Table OR n:PACKAGE_BODY OR n:PACKAGE_SPEC OR n:PROCEDURE_SPEC)
            OPTIONAL MATCH (n)-[r]->(m)
            WHERE NOT (m:ROOT OR m:Variable OR m:DECLARE OR m:Table OR m:PACKAGE_BODY OR m:PACKAGE_SPEC OR m:PROCEDURE_SPEC OR m:FUNCTION OR m:PROCEDURE OR m:CREATE_PROCEDURE_BODY)
            AND m.object_name = '{object_name}'
            AND NOT type(r) CONTAINS 'CALLS'
            AND NOT type(r) CONTAINS 'WRITES'
            AND NOT type(r) CONTAINS 'FROM'
            RETURN n, r, m
            ORDER BY n.startLine
            """,
            f"""
            MATCH (p)-[:PARENT_OF]->(d:DECLARE)-[r:SCOPE]->(v:Variable)
            WHERE p.object_name = '{object_name}'
            AND p.procedure_name = '{procedure_name}'
            AND (p:PROCEDURE OR p:CREATE_PROCEDURE_BODY OR p:FUNCTION)
            RETURN v
            """
        ]
        
        # * 쿼리 실행하여, 노드를 (전처리) 서비스 생성 함수로 전달합니다
        results = await connection.execute_queries(node_query)        
        await traverse_node_for_service(results, connection, command_class_variable, method_skeleton, jpa_method_list, object_name)
        logging.info(f"[{object_name}] (전처리) 서비스 생성 과정, 노드에 자바 속성을 추가 완료\n")


    except (ConvertingError, Neo4jError): 
        raise
    except Exception:
        err_msg = "(전처리) 서비스 코드 생성 과정하기 위해 준비하는 도중 문제가 발생했습니다."
        logging.error(err_msg, exc_info=False)
        raise ServiceCreationError(err_msg)
    finally:
        await connection.close()
