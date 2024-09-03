import logging
import tiktoken
from convert.converting_prompt.service_prompt import convert_code
from convert.converting_prompt.parent_skeleton_prompt import convert_parent_skeleton
from cypher.neo4j_connection import Neo4jConnection

# * 인코더 설정 및 파일 이름 초기화
encoder = tiktoken.get_encoding("cl100k_base")


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
async def process_converting(convert_sp_code, total_tokens, variable_dict, context_range, connection, procedure_variables, service_skeleton, jpa_method_list):
    
    try:
        # * 노드 업데이트 쿼리를 저장할 리스트 및 범위 개수
        node_update_query = []
        range_count = len(context_range)


        # * 프로시저 코드를 분석합니다(llm에게 전달)
        analysis_result = convert_code(convert_sp_code, service_skeleton, variable_dict, procedure_variables, context_range, range_count, jpa_method_list)
        logging.info(f"\nsuccessfully converted code\n")


        # * 분석 결과 각각의 데이터를 추출하고, 필요한 변수를 초기화합니다 
        for result in analysis_result['analysis']:
            start_line = result['range']['startLine']
            service_code = result['code']
            query = f"MATCH (n) WHERE n.startLine = {start_line} SET n.java_code = '{service_code.replace('\n', '\\n').replace("'", "\\'")}'"
            node_update_query.append(query)        
        

        # * 노드 업데이트 쿼리를 실행하는 메서드를 호출 
        await process_node_update_java_properties(node_update_query, connection)


        # * 다음 분석 주기를 위해 필요한 변수를 초기화합니다
        convert_sp_code = ""
        total_tokens = 0
        context_range.clear()
        variable_dict.clear()

        return (convert_sp_code, total_tokens, variable_dict, context_range)


    except Exception:
        logging.exception("An error occurred during analysis results processing(converting)")
        raise


# 역할: 각 노드에 자바 속성을 추가하기 위한 로직이 담긴 함수입니다.
# 매개변수: 
#   node_data_list - 노드와 관계에 대한 정보가 담긴 리스트
#   service_skeleton - 서비스 클래스의 기본 구조.
#   jpa_method_list - 사용된 JPA 메서드 목록.
#   procedure_variable - 프로시저 선언부에서 사용된 변수 정보.
# 반환값: 없음 
async def traverse_node_for_service(node_list, connection, procedure_variables, service_skeleton, jpa_method_list):

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
            (convert_sp_code, total_tokens, variable_dict, context_range) = await process_converting(convert_sp_code, total_tokens, variable_dict, context_range, connection, procedure_variables, service_skeleton, jpa_method_list)
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
        is_token_limit_exceeded = (total_tokens + node_tokens >= 1200 or len(context_range) >= 10) and context_range 
    

        # * 총 토큰 수 검사를 진행합니다.
        if is_token_limit_exceeded:
            print(f"토큰 및 결과 범위 초과로 converting 진행합니다.")
            (convert_sp_code, total_tokens, variable_dict, context_range) = await process_converting(convert_sp_code, total_tokens, variable_dict, context_range, connection, procedure_variables, service_skeleton, jpa_method_list)
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
            (convert_sp_code, total_tokens, variable_dict, context_range) = await process_converting(convert_sp_code, total_tokens, variable_dict, context_range, connection, procedure_variables, service_skeleton, jpa_method_list)
        else:
            print("아무것도 처리되지 않습니다.")
    
    
    # * 마지막 그룹에 대한 처리를 합니다.
    if context_range and convert_sp_code:
        print("순회가 끝났지만 남은 context_range와 convert_sp_code가 있어 converting을 진행합니다.")
        (convert_sp_code, total_tokens, variable_dict, context_range) = await process_converting(convert_sp_code, total_tokens, variable_dict, context_range, connection, procedure_variables, service_skeleton, jpa_method_list)




# 역할: 스토어드 프로시저 파일과 ANTLR 분석 파일을 읽어서 분석을 시작하는 메서드입니다.
# 매개변수: 
#   service_skeleton - 서비스 클래스의 기본 구조.
#   jpa_method_list - 사용된 JPA 메서드 목록.
#   procedure_variable - 프로시저 선언부에서 사용된 변수 정보.
# 반환값: 없음 
async def start_service_processing(service_skeleton, jpa_method_list, procedure_variable):
    

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
        await traverse_node_for_service(results, connection, procedure_variable, service_skeleton, jpa_method_list)


    except Exception:
        logging.exception("An error occurred prepare create service class(converting)")
        raise