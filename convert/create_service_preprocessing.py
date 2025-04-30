import json
import logging

from prompt.convert_service_prompt import convert_service_code
from prompt.convert_summarized_service_skeleton_prompt import convert_summarized_code
from understand.neo4j_connection import Neo4jConnection
from util.exception import ConvertingError
from util.utility_tool import extract_used_query_methods


# 역할: 코드 변환의 핵심 함수로, 노드들을 순회하면서 Java 코드로의 변환 작업을 조율합니다.
#
# 매개변수:
#   - traverse_nodes : 순회할 비즈니스 관련 노드 리스트
#   - variable_nodes : 모든 변수 노드 리스트
#   - connection : Neo4j 데이터베이스 연결 객체
#   - command_class_variable : Command 클래스에 정의된 변수들의 정보
#   - service_skeleton : 서비스의 기본 구조 템플릿
#   - query_method_list : 사용 가능한 전체 query 쿼리 메서드 목록
#   - object_name : 처리 중인 패키지/프로시저의 식별자
#   - procedure_name : 처리 중인 프로시저의 이름
#   - sequence_methods : 사용 가능한 시퀀스 메서드 목록
#   - user_id : 사용자 ID
#   - api_key : Claude API 키
async def traverse_node_for_service(traverse_nodes:list, variable_nodes:list, connection:Neo4jConnection, command_class_variable:dict, service_skeleton:str, query_method_list:list, object_name:str, procedure_name:str, sequence_methods:list, user_id:str, api_key:str):

    used_variables =  []                  # 사용된 변수 정보를 저장하는 리스트
    context_range = []                    # 분석할 컨텍스트 범위를 저장하는 리스트
    current_tokens = 0                    # 총 토큰 수
    convert_sp_code = ""                  # converting할 프로시저 코드 문자열
    small_parent_info = {}                # 크기가 작은 부모 노드 정보
    big_parent_info = {}                  # 크기가 큰 부모 노드의 정보
    another_big_parent_startLine = 0      # 부모안에 또 다른 부모의 시작라인
    used_query_method_dict = {}             # 사용된 query 쿼리 메서드를 관리할 사전
    tracking_variables = {}               # 변수 정보를 추적하기 위한 사전


    # 역할: 특정 노드 ID에 해당하는 범위 내에서 실제로 사용된 변수들을 식별하고 추출하는 함수입니다.
    #
    # 매개변수: 
    #   - node_id : 현재 처리 중인 노드의 ID(시작라인) (변수 사용 범위를 확인하기 위한 기준점)
    async def trace_extract_used_variable_nodes(node_id:int):
        nonlocal used_variables, tracking_variables
        
        try:
            # * 모든 변수 노드를 순회하며 현재 node_id에 해당하는 변수들을 추출
            for variable_node in variable_nodes:
                node_data = variable_node['v']
                
                for range_key in node_data:
                    
                    # * 범위 키가 아닌 경우 스킵 (예: name, type 등의 키)
                    if '_' not in range_key or not all(part.isdigit() for part in range_key.split('_')):
                        continue
                        
                    # * 현재 node_id가 변수의 유효 범위 내에 있는지 확인
                    start_id, end_id = map(int, range_key.split('_'))
                    if not (start_id <= node_id <= end_id):
                        continue
                    
                    # * 변수 정보 구성
                    var_name = node_data['name']
                    var_info = {
                        'type': node_data.get('type', 'Unknown'),
                        'name': var_name,
                        'role': tracking_variables.get(var_name, '')
                    }
                    
                    # * 중복되지 않은 변수만 추가
                    if not any(var['name'] == var_name for var in used_variables):
                        used_variables.append(var_info)
                    break

        except Exception as e:
            err_msg = f"(전처리) 서비스 코드 생성 과정에서 사용된 변수 노드 추출 도중 문제가 발생했습니다: {str(e)}"
            logging.error(err_msg)
            raise ConvertingError(err_msg)
    

    # 역할: 토큰 수가 1700개 이상인 대형 부모 노드를 처리하는 특수 함수입니다.
    #
    # 매개변수:
    #   - start_line : 처리할 노드의 시작 라인 번호
    #   - summarized_code : 자식 노드들의 코드가 요약된 형태의 코드 문자열
    # TODO : 프로시저 이름 추가하는 작업이 필요
    async def process_over_size_node(start_line:int, summarized_code:str) -> None:

        try:
            # * 요약된 코드를 분석하여, 요약된 메서드 틀을 생성합니다.
            analysis_result = convert_summarized_code(summarized_code)
            service_code = analysis_result['code']
            escaped_code = service_code.replace('\n', '\\n').replace("'", "\\'")


            # * 노드의 속성에 Java 코드를 추가하는 쿼리 생성합니다.
            query = [f"MATCH (n) "
                    f"WHERE n.startLine = {start_line} AND n.object_name = '{object_name}' AND n.user_id = '{user_id}' "
                    f"SET n.java_code = '{escaped_code}'"]


            # * 생성된 쿼리를 실행합니다.
            await connection.execute_queries(query)

        except ConvertingError:
            raise
        except Exception as e:
            err_msg = f"(전처리) 서비스 코드 생성 과정에서 사이즈가 큰 노드를 처리 도중 문제가 발생했습니다: {str(e)}"
            logging.error(err_msg)
            raise ConvertingError(err_msg)


    # 역할: LLM에 코드 분석을 요청하고 그 결과를 처리하는 중심 함수입니다.
    async def process_service_class_code() -> None:
        nonlocal convert_sp_code, current_tokens, used_variables, context_range, used_query_method_dict, tracking_variables

        try:
            # * 노드 업데이트 쿼리를 저장할 리스트 및 범위 조정
            context_range = [dict(t) for t in {tuple(d.items()) for d in context_range}]
            context_range.sort(key=lambda x: (x['startLine'], x['endLine']))
            range_count = len(context_range)


            # * 전달된 정보를 llm에게 전달하여 결과를 받고, 결과를 처리하는 함수를 호출합니다.
            analysis_result = convert_service_code(
                convert_sp_code, 
                service_skeleton, 
                used_variables, 
                command_class_variable, 
                context_range, range_count, 
                used_query_method_dict,
                sequence_methods,
                api_key
            )
            await handle_convert_result(analysis_result)


            # * 다음 사이클을 위해 각 종 변수를 초기화합니다.
            convert_sp_code = ""
            current_tokens = 0
            context_range.clear()
            used_variables.clear()
            used_query_method_dict.clear()
        
        except ConvertingError:
            raise
        except Exception as e:
            err_msg = f"(전처리) 서비스 코드 생성 과정에서 LLM의 결과를 결정하는 도중 문제가 발생했습니다: {str(e)}"
            logging.error(err_msg)
            raise ConvertingError(err_msg)



    # 역할: LLM이 분석한 결과를 바탕으로 Neo4j 데이터베이스의 노드들을 업데이트하는 함수입니다.
    #
    # 매개변수:
    #   - analysis_result : LLM이 분석한 결과
    #
    # 반환값:
    #   - tracking_variables : 변수 정보를 추적하기 위한 사전
    async def handle_convert_result(analysis_result:dict) -> dict:
        nonlocal tracking_variables
        node_update_query = []
        
        try:
            # * 분석 결과에서 코드와 변수 정보를 추출합니다.
            code_info = analysis_result['analysis'].get('code', {})
            variables_info = analysis_result['analysis'].get('variables', {})
            

            # * 코드 정보를 추출하고, 자바 속성 추가를 위한 사이퍼쿼리를 생성합니다.
            for key, service_code in code_info.items():
                start_line, end_line = map(int, key.replace('-','~').split('~'))
                # escaped_code = service_code.replace('\n', '\\n').replace("'", "\\'")
                node_update_query.append(
                    f"MATCH (n) WHERE n.startLine = {start_line} "
                    f"AND n.object_name = '{object_name}' AND n.endLine = {end_line} AND n.user_id = '{user_id}' "
                    f"SET n.java_code = {json.dumps(service_code)}"
                )    


            # * 변수 정보를 tracking_variables에 업데이트합니다.
            for var_name, var_info in variables_info.items():
                tracking_variables[var_name] = var_info
                node_update_query.append(
                    f"MATCH (n:Variable) "
                    f"WHERE n.object_name = '{object_name}' "
                    f"AND n.procedure_name = '{procedure_name}' "
                    f"AND n.name = '{var_name}' "
                    f"AND n.user_id = '{user_id}' "
                    f"SET n.value_tracking = {json.dumps(var_info)}"
                )


            # * 노드 업데이트 쿼리를 실행
            await connection.execute_queries(node_update_query)

        except ConvertingError: 
            raise
        except Exception as e:
            err_msg = f"(전처리) 서비스 코드 생성 과정에서 LLM의 결과를 처리하는 도중 문제가 발생했습니다: {str(e)}"
            logging.error(err_msg)
            raise ConvertingError(err_msg)
    

    # ! 노드 순회 시작
    # TODO 리팩토링 필요
    try:
        # * Converting 하기 위한 노드의 순회 시작
        for node in traverse_nodes:
            start_node = node['n']
            relationship = node['r'][1] if node['r'] else "NEXT"
            end_node = node['m']
            node_tokens = 0
            print("-" * 40) 
            print(f"시작 노드 : [ 시작 라인 : {start_node['startLine']}, 이름 : ({start_node['name']}), 끝라인: {start_node['endLine']}, 토큰 : {start_node['token']}")
            print(f"관계: {relationship}")
            if end_node: print(f"종료 노드 : [ 시작 라인 : {end_node['startLine']}, 이름 : ({end_node['name']}), 끝라인: {end_node['endLine']}, 토큰 : {end_node['token']}\n")
            if start_node['name'] in ["EXECUTE_IMMDDIATE"]: continue


            # * 각 부모 노드의 1단계 깊이 자식들 순회 여부를 확인하는 조건
            is_small_parent_traverse_1deth = start_node['startLine'] == small_parent_info.get("startLine", 0) and relationship == "NEXT"
            is_big_parent_traverse_1deth = start_node['startLine'] == big_parent_info.get("startLine", 0) and relationship == "NEXT"
            

            # * 현재 노드의 시작라인이 최상위 부모 노드와 같다면, 1단계 깊이 자식들 순회완료로 다음 레벨의 시작라인을 저장
            if is_small_parent_traverse_1deth:
                print(f"작은 부모 노드({start_node['startLine']})의 1단계 깊이 자식들 순회 완료")
                small_parent_info["nextLine"] = end_node['startLine']
                continue
            elif is_big_parent_traverse_1deth:
                print(f"큰 부모 노드({start_node['startLine']})의 1단계 깊이 자식들 순회 완료")
                big_parent_info["nextLine"] = end_node['startLine']
                await process_service_class_code()
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


            # * 각 노드의 타입에 따른 조건(매우 큰 부모 노드 소속인지, 작은 부모 노드 소속인지, 단일 노드 소속인지)
            is_big_parent_and_small_child = relationship == "PARENT_OF" and start_node['token'] > 200 and end_node['token'] < 2000
            is_small_parent = relationship == "PARENT_OF" and start_node['token'] < 2000 and not small_parent_info
            is_single_node = relationship == "NEXT" and not small_parent_info and not big_parent_info


            # * 각 노드의 타입에 따라서 어떤 노드의 토큰을 더 할지를 결정합니다. 
            if is_big_parent_and_small_child:
                node_tokens += end_node['token']
            elif is_small_parent or is_single_node:
                node_tokens += start_node['token']


            # * 총 토큰 수 및 결과 개수 초과 여부를 확인하는 조건
            is_token_limit_exceeded = ((current_tokens + node_tokens >= 1000) or (len(context_range) >= 10)) and context_range 


            # * 총 토큰 수 검사를 진행합니다.
            if is_token_limit_exceeded:
                print(f"토큰 수가 제한값을 초과하여 LLM 분석을 시작합니다. (현재 토큰: {current_tokens})")
                await process_service_class_code()
            print(f"토큰 합계 : {current_tokens + node_tokens}, 결과 개수 : {len(context_range)}")
            current_tokens += node_tokens


            # * 특정 부모에 대한 자식 처리 도중 결과 개수 초과로 converting이 되었을 때 다시 부모 정보를 할당   
            if small_parent_info and not convert_sp_code and relationship == "PARENT_OF":
                print(f"다시 부모 정보를 할당")
                convert_sp_code = small_parent_info['code']
                current_tokens = small_parent_info['token']


            # * 관계 타입에 따라 노드의 토큰 수를 파악하여 각 변수값을 할당합니다.  
            if relationship == "PARENT_OF":
                
                # * 부모 노드의 크기가 매우 큰 경우 처리 
                if start_node['token'] >= 2000:
                    if not big_parent_info: 
                        await process_over_size_node(start_node['startLine'], start_node['summarized_code'])
                        big_parent_info = {"startLine": start_node['startLine'], "nextLine": 0}
                    if end_node['token'] >= 2000:
                        await process_over_size_node(end_node['startLine'], end_node['summarized_code'])
                    else:
                        another_big_parent_startLine = start_node['startLine']
                        convert_sp_code += f"\n{end_node['node_code']}"
                        context_range.append({"startLine": end_node['startLine'], "endLine": end_node['endLine']})
                        await trace_extract_used_variable_nodes(end_node['startLine'])
                        used_query_method_dict =await extract_used_query_methods(end_node['startLine'], end_node['endLine'], query_method_list, used_query_method_dict)

                # * 부모의 노드 크기가 작은 경우  
                else:
                    if not small_parent_info:
                        convert_sp_code += f"\n{start_node['node_code']}"
                        small_parent_info = {"startLine": start_node['startLine'], "endLine": start_node['endLine'], "nextLine": 0, "code": start_node['node_code'], "token": start_node['token']}            
                        if not big_parent_info: 
                            context_range.append({"startLine": start_node['startLine'], "endLine": start_node['endLine']}) 
                            await trace_extract_used_variable_nodes(start_node['startLine'])
                            used_query_method_dict = await extract_used_query_methods(start_node['startLine'], start_node['endLine'], query_method_list, used_query_method_dict)
                        context_range.append({"startLine": end_node['startLine'], "endLine": end_node['endLine']})
                        await trace_extract_used_variable_nodes(end_node['startLine'])
                        used_query_method_dict = await extract_used_query_methods(end_node['startLine'], end_node['endLine'], query_method_list, used_query_method_dict)
                    else:
                        context_range.append({"startLine": end_node['startLine'], "endLine": end_node['endLine']})
                        await trace_extract_used_variable_nodes(end_node['startLine'])
                        used_query_method_dict = await extract_used_query_methods(end_node['startLine'], end_node['endLine'], query_method_list, used_query_method_dict)


            # * 단일 노드의 경우
            elif not small_parent_info and not big_parent_info:
                convert_sp_code += f"\n{start_node['node_code']}"
                context_range.append({"startLine": start_node['startLine'], "endLine": start_node['endLine']})
                await trace_extract_used_variable_nodes(start_node['startLine'])
                used_query_method_dict = await extract_used_query_methods(start_node['startLine'], start_node['endLine'], query_method_list, used_query_method_dict)
            elif another_big_parent_startLine == start_node['startLine'] and context_range and convert_sp_code: 
                print(f"큰 부모 노드 내의 또 다른 부모 노드 처리가 완료되어 LLM 분석을 시작합니다. (코드 흐름 분리)")
                await process_service_class_code()
            else:
                print("현재 노드에 대한 처리가 필요하지 않습니다.")        
        
        # * 마지막 그룹에 대한 처리를 합니다.
        if context_range and convert_sp_code:
            print(f"노드 순회가 완료되었으나 미처리된 코드가 있어 LLM 분석을 시작합니다.")
            await process_service_class_code()
    
    except ConvertingError: 
        raise
    except Exception as e:
        err_msg = f"(전처리) 서비스 코드 생성 과정에서 노드를 순회하는 도중 문제가 발생했습니다: {str(e)}"
        logging.error(err_msg)
        raise ConvertingError(err_msg)

    

# 역할: PL/SQL 프로시저를 Java 서비스 계층의 메서드로 변환하는 전체 프로세스를 관리합니다.
#
# 매개변수: 
#   - service_skeleton : 생성될 Java Service 클래스의 기본 구조
#   - command_class_variable : Command 클래스의 필드 정보
#   - procedure_name : 처리 중인 프로시저의 이름
#   - query_method_list : 사용 가능한 쿼리 메서드 목록
#   - object_name : 처리 중인 패키지/프로시저의 이름
#   - sequence_methods : 사용 가능한 시퀀스 메서드 목록
#   - user_id : 사용자 ID
#   - api_key : Claude API 키
#
# 반환값: 
#   - variable_nodes : 변환 과정에서 사용된 변수 노드 리스트
async def start_service_preprocessing(service_skeleton:str, command_class_variable:dict, procedure_name:str, query_method_list:list, object_name:str, sequence_methods:list, user_id:str, api_key:str) -> None:
    
    connection = Neo4jConnection() 
    logging.info(f"[{object_name}] {procedure_name} 프로시저의 서비스 코드 생성을 시작합니다.")
    
    
    try:
        node_query = [
            # * 노드와 관계를 가져오는 쿼리
            f"""
            MATCH (p)
            WHERE p.object_name = '{object_name}'
            AND p.procedure_name = '{procedure_name}'
            AND p.user_id = '{user_id}'
            AND (p:FUNCTION OR p:PROCEDURE OR p:CREATE_PROCEDURE_BODY)
            MATCH (p)-[:PARENT_OF]->(n)
            WHERE NOT (n:ROOT OR n:Variable OR n:DECLARE OR n:Table 
                  OR n:SPEC)
            OPTIONAL MATCH (n)-[r]->(m)
            WHERE m.object_name = '{object_name}'
            AND m.user_id = '{user_id}'
            AND NOT (m:ROOT OR m:Variable OR m:DECLARE OR m:Table 
                OR m:SPEC)
            AND NOT type(r) CONTAINS 'CALL'
            AND NOT type(r) CONTAINS 'WRITES'
            AND NOT type(r) CONTAINS 'FROM'
            RETURN n, r, m
            ORDER BY n.startLine
            """,
            # * 변수 노드를 조회하는 쿼리
            f"""
            MATCH (n)
            WHERE n.object_name = '{object_name}'
            AND n.procedure_name = '{procedure_name}'
            AND n.user_id = '{user_id}'
            AND (n:DECLARE)
            MATCH (n)-[r:SCOPE]->(v:Variable)
            RETURN v
            """
        ]


        # * 쿼리 실행하여, 노드들을 가져옵니다.
        service_nodes, variable_nodes = await connection.execute_queries(node_query)        


        # * (전처리) 서비스 생성 함수 호출
        await traverse_node_for_service(
            service_nodes, 
            variable_nodes,
            connection, 
            command_class_variable, 
            service_skeleton, 
            query_method_list, 
            object_name, 
            procedure_name,
            sequence_methods,
            user_id,
            api_key
        )

        logging.info(f"[{object_name}] {procedure_name} 프로시저의 서비스 코드 생성이 완료되었습니다.\n")
        return variable_nodes

    except ConvertingError: 
        raise
    except Exception as e:
        err_msg = f"(전처리) 서비스 코드 생성 과정하기 위해 준비하는 도중 문제가 발생했습니다: {str(e)}"
        logging.error(err_msg)
        raise ConvertingError(err_msg)
    finally:
        await connection.close()