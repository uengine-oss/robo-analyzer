from collections import defaultdict
import logging
from util.exception import TokenCountError, VariableNodeError
from util.token_utils import calculate_code_token


# 역할: 특정 코드 라인에서 사용된 변수들의 정보를 추출합니다.
#
# 매개변수: 
#   - startLine : 분석할 코드의 시작 라인 번호
#   - local_variable_nodes : Neo4j에서 조회한 모든 변수 노드 정보
#
# 반환값: 
#   - used_variables : {'라인범위': ['변수타입: 변수명']} 형식의 딕셔너리
#   - variable_tokens : 추출된 변수 정보의 토큰 수
async def extract_used_variable_nodes(startLine: int, local_variable_nodes: list) -> tuple[dict, int]:
    try:
        used_variables = defaultdict(list)
        
        for variable_node in local_variable_nodes:
            for used_range in variable_node['v']:

                # * 라인 범위 형식 검증 (예: "1_5")
                if not ('_' in used_range and all(part.isdigit() for part in used_range.split('_'))):
                    continue
                    
                # * 시작 라인이 일치하는 경우만 처리
                used_startLine, used_endLine = map(int, used_range.split('_'))
                if used_startLine != startLine:
                    continue
                    
                # * 변수 정보 저장
                range_key = f'{used_startLine}~{used_endLine}'
                var_type = variable_node['v'].get('type', 'Unknown')
                var_name = variable_node['v']['name']
                used_variables[range_key].append(f"{var_type}: {var_name}")
                break

        return used_variables, calculate_code_token(used_variables)
    
    except TokenCountError:
        raise
    except Exception:
        err_msg = "사용된 변수 노드를 추출하는 도중 오류가 발생했습니다."
        logging.error(err_msg)
        raise VariableNodeError(err_msg)
    


# 역할: 특정 노드 범위 내에서 사용된 JPA 쿼리 메서드들을 식별하고 수집하는 함수입니다.
#
# 매개변수:
#   - start_line : 노드의 시작 라인
#   - end_line : 노드의 끝 라인
#   - jpa_method_list : 노드 내에서 사용된 JPA 메서드 목록
#   - used_jpa_method_dict : 사용된 JPA 메서드를 저장할 딕셔너리
#
# 반환값:
#   - used_jpa_method_dict : 사용된 JPA 메서드를 저장한 딕셔너리
async def extract_used_query_methods(start_line:int, end_line:int, jpa_method_list: list[dict], used_jpa_method_dict: dict) -> dict:
    for range_key, method in jpa_method_list.items():

        method_start, method_end = map(int, range_key.split('~'))
        
        # * 현재 범위 내에 있는 JPA 메서드 추출
        if start_line <= method_start <= end_line and start_line <= method_end <= end_line:
            used_jpa_method_dict[range_key] = method
            break

    return used_jpa_method_dict