import logging
from typing import List, Dict, Tuple
from neo4j.exceptions import Neo4jError, ServiceCreationError
from convert.create_repository import extract_used_variable_nodes
from understand.neo4j_connection import Neo4jConnection
from util.converting_utlis import extract_used_jpa_methods
from util.exception import ConvertingError, HandleResultError, PrepareDataError, ProcessResultError
from prompt.convert_service_prompt import convert_service_code


# 역할 : java_code가 없는 노드들을 조회합니다.
#
# 매개변수:
#   - connection : Neo4j 연결 객체
#
# 반환값:
#   - Tuple[bool, List[Dict]] : 조회 결과
async def get_nodes_without_java_code(connection: Neo4jConnection) -> Tuple[bool, List[Dict]]:

    try:
        query = ["""
        MATCH (n)
        WHERE n.java_code IS NULL
        AND NOT n:Table 
        AND NOT n:ROOT 
        AND NOT n:PACKAGE_SPEC
        AND NOT n:PACKAGE_VARIABLE
        AND NOT n:Variable
        AND NOT n:PROCEDURE_SPEC
        AND NOT n:PACKAGE_BODY
        AND NOT n:FUNCTION
        AND NOT n:SPEC
        AND NOT n:DECLARE
        AND NOT n:PROCEDURE
        RETURN n
        """]

        non_java_code_nodes = (await connection.execute_queries(query))[0]

        if non_java_code_nodes:
            logging.info(f"java_code가 없는 노드 {len(non_java_code_nodes)}개가 발견되었습니다.")
            return True, non_java_code_nodes
        else:
            logging.info("모든 노드가 java_code 속성을 가지고 있습니다.")
            return False, []
    
    except Neo4jError:
        raise
    except Exception:
        err_msg = "java_code가 없는 노드 조회 중 오류가 발생했습니다."
        logging.error(err_msg)
        raise PrepareDataError(err_msg)


# 역할 : 노드들을 순회하여 각 노드에 대해 처리하는 함수입니다.
#
# 매개변수:
#   - service_skeleton : 생성될 서비스의 기본 구조 템플릿
#   - command_class_variable : Command 클래스에 정의된 변수들의 정보
#   - procedure_name : 처리할 프로시저의 이름
#   - jpa_method_list : 사용 가능한 전체 JPA 쿼리 메서드 목록
#   - object_name : 처리 중인 패키지/프로시저의 식별자
async def start_validate_service_preprocessing(variable_nodes:list, service_skeleton: str, command_class_variable: dict, procedure_name: str, jpa_method_list: list, object_name: str) -> None:
    
    connection = Neo4jConnection()
    used_jpa_method_dict = {}
    used_variables = {}
    context_range = []
    current_token = 0
    current_code = ""
    MAX_TOKEN = 1700

    logging.info(f"서비스 전처리 검증을 시작합니다.")

    # 역할 : 누적된 코드를 LLM으로 처리하고 결과를 DB에 업데이트
    async def process_validate_service_class_code():
        nonlocal current_code, current_token, used_variables, context_range, used_jpa_method_dict


        try:
            # * 범위 정보 처리
            context_range = [dict(t) for t in {tuple(d.items()) for d in context_range}]
            context_range.sort(key=lambda x: x['startLine'])
            range_count = len(context_range)
        
            # * LLM 분석 수행
            analysis_result = convert_service_code(
                current_code,
                service_skeleton,
                used_variables,
                command_class_variable,
                context_range,
                range_count,
                used_jpa_method_dict
            )

            # * 결과 처리 및 노드 업데이트
            await handle_convert_result(analysis_result)

        except ConvertingError:
            raise
        except Exception:
            err_msg = "서비스 전처리 검증 과정에서 자바로 전환 중 오류가 발생했습니다."
            logging.error(err_msg)
            raise ProcessResultError(err_msg)


    # 역할 : 결과 처리 및 노드 업데이트
    #
    # 매개변수:     
    #   - analysis_result : LLM이 분석한 결과
    async def handle_convert_result(analysis_result: dict) -> None:
        node_update_query = []
        
        try:
            # * 분석 결과에서 코드 정보를 추출
            code_info = analysis_result['analysis'].get('code', {})
            
            # * 코드 정보를 추출하고, 자바 코드 업데이트를 위한 사이퍼 쿼리 생성
            for key, service_code in code_info.items():
                start_line, end_line = map(int, key.replace('-','~').split('~'))
                escaped_code = service_code.replace('\n', '\\n').replace("'", "\\'")
                
                node_update_query.append(
                    f"MATCH (n) WHERE n.startLine = {start_line} "
                    f"AND n.object_name = '{object_name}' "
                    f"AND n.procedure_name = '{procedure_name}' "
                    f"AND n.endLine = {end_line} "
                    f"SET n.java_code = '{escaped_code}'"
                )    

            # * 노드 업데이트 쿼리 실행
            await connection.execute_queries(node_update_query)

        except ConvertingError:
            raise
        except Exception:
            err_msg = "서비스 검증 과정에서 LLM의 결과를 처리하는 도중 문제가 발생했습니다."
            logging.error(err_msg)
            raise HandleResultError(err_msg)
        

    # ! 메인 로직
    try:
        # * java_code가 없는 노드 확인
        has_nodes, nodes = await get_nodes_without_java_code(connection)
        if not has_nodes:
            return


        # * 각 노드에 대해 처리
        for node in nodes:
            node_data = node['n']
            sp_code = node_data['node_code']
            start_line = node_data['startLine']
            end_line = node_data['endLine']
            token = node_data['token']
            

            # * 변수 정보 추출 및 토큰 계산
            used_variables, variable_token = await extract_used_variable_nodes(start_line, variable_nodes)
            

            # * 토큰 제한 초과시 처리
            if current_token + token + variable_token> MAX_TOKEN and context_range:
                await process_validate_service_class_code()


            # * 현재 코드, 범위, 토큰, JPA 메서드 정보 업데이트
            current_code += f"\n{sp_code}"  # 개행 추가
            current_token += token + variable_token
            used_jpa_method_dict = await extract_used_jpa_methods(start_line, end_line, jpa_method_list, used_jpa_method_dict)
            context_range = [{"startLine": start_line, "endLine": end_line}]

        # * 남은 코드 처리
        if current_code and context_range:
            await process_validate_service_class_code()


    except ConvertingError:
        raise
    except Exception:
        err_msg = "서비스 전처리 검증 과정에서 예상치 못한 오류가 발생했습니다."
        logging.error(err_msg)
        raise ServiceCreationError(err_msg)
    finally:
        await connection.close()