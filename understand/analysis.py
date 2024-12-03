from collections import defaultdict
import json
import logging
import re
import tiktoken
from prompt.understand_prompt import understand_code
from prompt.understand_variables_prompt import understand_variables
from util.exception import (TokenCountError, ExtractCodeError, SummarizeCodeError, FocusedCodeError, TraverseCodeError, UnderstandingError,
                            RemoveInfoCodeError, ProcessResultError, HandleResultError, LLMCallError, EventRsRqError, CreateNodeError)


encoder = tiktoken.get_encoding("cl100k_base")


# 역할: 스토어드 프로시저 코드의 토큰 수를 계산합니다.
# 매개변수: 
#   - code (str): 토큰화할 스토어드 프로시저 코드
# 반환값: 
#   - int: 계산된 토큰의 총 개수
# 예외:
#   - TokenCountError: 토큰 계산 과정에서 오류 발생 시
def count_tokens_in_text(code):
    
    if not code: return 0

    try:
        # * 코드를 토큰화하고 토큰의 개수를 반환합니다.
        tokens = encoder.encode(code)
        return len(tokens)
    except Exception:
        err_msg = "Understanding 과정에서 토큰 계산 중 오류가 발생했습니다"
        logging.error(err_msg, exc_info=False)
        raise TokenCountError(err_msg)


# 역할: 지정된 라인 범위에 해당하는 스토어드 프로시저 코드를 추출합니다.
# 매개변수: 
#   - code (str): 라인 번호가 포함된 전체 스토어드 프로시저 코드
#   - context_range (list[dict]): 추출할 코드의 범위 정보
# 반환값: 
#   - extracted_code: 지정된 범위의 코드
#   - end_line: 추출된 코드의 마지막 라인 번호
def extract_code_within_range(code, context_range):
    try:
        if not (code and context_range):
            return "", ""

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

        extracted_code = '\n'.join(extracted_lines)
        return extracted_code, end_line
    
    except Exception:
        err_msg = "Understanding 과정에서 범위내에 코드 추출 도중에 오류가 발생했습니다."
        logging.error(err_msg, exc_info=False)
        raise ExtractCodeError(err_msg)


# 역할: PROCEDURE나 FUNCTION 선언 코드에서 이름을 추출하는 함수
# 매개변수: 
#   - code : 프로시저/함수 선언이 포함된 코드
# 반환값: 
#   - match.group(1) : 추출된 프로시저/함수 이름
#   - None : 매칭되지 않을 경우 반환
def extract_procedure_name(code):
    
    # * 첫 번째 줄만 검사
    first_line = code.split('\n')[0]
    
    # * PROCEDURE나 FUNCTION 다음에 오는 이름을 추출하는 정규식
    pattern = r'(?:PROCEDURE|FUNCTION)\s+(\w+)'
    match = re.search(pattern, first_line)
    
    # * 매칭된 결과가 있으면 이름을 반환, 없으면 None 반환
    if match:
        return match.group(1)
    return None


# 역할: 스토어드 프로시저 코드를 요약하는 함수
#      - 자식 노드가 없는 경우: 원본 코드를 그대로 반환
#      - 자식 노드가 있는 경우: 자식 노드 부분을 "... code ..."로 요약하여 반환
# 매개변수: 
#   - file_content (str): 스토어드 프로시저의 전체 소스 코드
#   - node (dict): 현재 처리할 노드 정보
#
# 반환값: 
#   - str: 처리된 코드 문자열
#          - 각 라인은 "{라인번호}: {코드내용}\n" 형식
#          - 자식 노드는 "{라인번호}: ... code ...\n" 형식으로 요약
def extract_and_summarize_code(file_content, node):

    def summarize_code(start_line, end_line, children):

        # * 시작 라인과 끝 라인을 기준으로 스토어드 프로시저 코드 라인을 추출합니다.
        lines = file_content.split('\n')  
        code_lines = lines[start_line-1:end_line]
        summarized_code = []
        last_end_line = start_line - 1

        # * 각 자식 노드에 대해 코드를 추출한 뒤, 요약 처리를 반복합니다.
        for child in children:
            before_child_code = code_lines[last_end_line-start_line+1:child['startLine']-start_line]
            summarized_code.extend([f"{i+last_end_line+1}: {line}\n" for i, line in enumerate(before_child_code)])
            summarized_code.append(f"{child['startLine']}: ... code ...\n")
            last_end_line = child['endLine']

        # * 마지막 자식 노드 이후 부터 끝나는 지점까지의 코드를 추가합니다
        after_last_child_code = code_lines[last_end_line-start_line+1:]
        summarized_code.extend([f"{i+last_end_line+1}: {line}\n" for i, line in enumerate(after_last_child_code)])
        return ''.join(summarized_code)
    
    try:
        # * 자식 노드가 없는 경우, 해당 노드의 코드만 추출합니다.
        if not node.get('children'):
            lines = file_content.split('\n')  
            code_lines = lines[node['startLine']-1:node['endLine']] 
            return ''.join([f"{i+node['startLine']}: {line}\n" for i, line in enumerate(code_lines)])
        else:
            # * 자식 노드가 있는 경우, summarize_code 함수를 호출하여 처리합니다.
            return summarize_code(node['startLine'], node['endLine'], node.get('children', []))
    
    except Exception:
        err_msg = "Understanding 과정에서 코드를 요약하는 도중 오류가 발생했습니다."
        logging.error(err_msg, exc_info=False)
        raise SummarizeCodeError(err_msg)


# 역할: 분석에 필요한 전체 스토어드 프로시저 코드를 생성하는 함수
#      - 현재 스케줄의 코드를 기준으로 시작
#      - 스택에 저장된 상위 스케줄들을 역순으로 검토
#      - "... code ..." 플레이스홀더를 실제 코드로 대체하며 조합
#
# 매개변수: 
#   - current_schedule (dict): 현재 처리 중인 스케줄 정보
#   - schedule_stack (list): 이전에 처리된 스케줄들의 스택
#
# 반환값: 
#   - str: 조합된 전체 스토어드 프로시저 코드
#          - 상위 스케줄들의 구조를 유지하면서 현재 스케줄의 코드가 통합된 형태
#          - 각 라인은 "{라인번호}: {코드내용}" 형식
def create_focused_code(current_schedule, schedule_stack):
    try:
        # * 현재 스케줄의 코드를 초기값으로 설정합니다.
        focused_code = current_schedule["code"]
        current_start_line = current_schedule["startLine"]


        # * 스택을 역순으로 검토하면서 스토어드 프로시저 코드를 조합합니다.
        for schedule in reversed(schedule_stack):
            placeholder = f"{current_start_line}: ... code ..."
            if placeholder in schedule["code"]:

                # * 현재 스케줄의 시작 라인을 플레이스홀더로 사용하여 실제 스토어드 프로시저 코드로 교체합니다.
                focused_code = schedule["code"].replace(placeholder, focused_code, 1)
                current_start_line = schedule["startLine"]

        return focused_code

    except Exception:
        err_msg = "Understanding 과정에서 분석할 코드 생성 도중에 오류가 발생했습니다."
        logging.error(err_msg, exc_info=False)
        raise FocusedCodeError(err_msg)
    

# 역할: 각 구문 별로 스토어드 프로시저 코드를 추출하는 함수
#      - 지정된 시작과 끝 라인 범위의 코드를 추출
#      - 각 라인에 라인 번호를 추가하여 포맷팅
#
# 매개변수: 
#   - file_content (str): 스토어드 프로시저 파일의 전체 내용
#   - start_line (int): 추출할 코드의 시작 라인 번호
#   - end_line (int): 추출할 코드의 끝 라인 번호
#                     (0인 경우 start_line과 동일하게 처리)
#
# 반환값: 
#   - str: 추출된 스토어드 프로시저 코드
#         - 각 라인은 "{라인번호}: {코드내용}" 형식
#         - 예: "1: SELECT *\n2: FROM table"
def extract_node_code(file_content, start_line, end_line):
    try:
        if end_line == 0:
            end_line = start_line

        # * 지정된 라인 번호를 기준으로 코드를 추출합니다.
        lines = file_content.split('\n')
        extracted_lines = lines[start_line-1:end_line]

        # * 추출된 라인들 앞에 라인 번호를 추가하고 하나의 문자열로 연결합니다.
        extracted_node_code = '\n'.join(f"{i + start_line}: {line}" for i, line in enumerate(extracted_lines))
        return extracted_node_code
    
    except Exception:
        err_msg = "Understanding 과정에서 노드에 맞게 코드를 추출 도중에 오류가 발생했습니다."
        logging.error(err_msg, exc_info=False)
        raise ExtractCodeError(err_msg)


# 역할: 테이블 필드 이름에서 중괄호를 제거하고 실제 필드 이름만 추출하는 함수
#      - 필드 이름이 중괄호({})로 감싸져 있는 경우 중괄호 내부 값만 추출
#      - 중괄호가 없는 경우 원본 값 그대로 반환
#
# 매개변수: 
#   - field_name (str): 처리할 테이블 필드 이름
#                       예: "{column_name}" 또는 "column_name"
#
# 반환값: 
#   - str: 정제된 필드 이름
#          - 중괄호가 있는 경우: 중괄호 내부 값
#          - 중괄호가 없는 경우: 원본 값
def clean_field_name(field_name):
    match = re.search(r'\{(.+?)\}', field_name)
    if match:
        return match.group(1)
    return field_name


# 역할: 스토어드 프로시저를 분석하여 Neo4j 사이퍼 쿼리를 생성하는 메인 함수
#      - ANTLR 파서의 분석 결과를 기반으로 코드 구조 파악
#      - 코드를 순회하며 노드와 관계를 표현하는 사이퍼 쿼리 생성
#      - 생성된 쿼리를 이벤트 큐를 통해 전송
#
# 매개변수: 
#   - antlr_data (dict): ANTLR 파서의 AST(Abstract Syntax Tree) 분석 결과
#   - file_content (str): 스토어드 프로시저의 전체 소스 코드
#   - send_queue (asyncio.Queue): 생성된 사이퍼 쿼리를 전송하는 큐
#   - receive_queue (asyncio.Queue): 쿼리 처리 결과를 수신하는 큐
#   - last_line (int): 스토어드 프로시저의 마지막 라인 번호
#   - object_name (str): 스토어드 프로시저의 이름
#   - ddl_tables (dict): 테이블 DDL 정보
#   - has_ddl_info (bool): DDL 정보가 있는지 여부
async def analysis(antlr_data, file_content, send_queue, receive_queue, last_line, object_name, ddl_tables, has_ddl_info):
    schedule_stack = []               # 스케줄 스택
    context_range = []                # LLM이 분석할 스토어드 프로시저의 범위
    cypher_query = []                 # 사이퍼 쿼리를 담을 리스트
    node_statementType = set()        # 노드의 타입을 저장할 세트
    procedure_name = None             # 프로시저 정보를 저장할 딕셔너리
    extract_code = ""                 # 범위 만큼 추출된 스토어드 프로시저
    focused_code = ""                 # 전체적인 스토어드 프로시저 코드
    sp_token_count = 0                # 토큰 수

    logging.info(f"\n[{object_name}] 사이퍼 쿼리 생성 시작\n")


    # 역할: llm에게 분석할 코드를 전달한 뒤, 분석 결과를 처리를 결정하는 함수
    #      - 코드 분석을 위해 LLM 호출
    #      - 분석 결과를 사이퍼 쿼리로 변환하는 함수를 호출
    #      - 분석 완료 후 관련 변수 초기화
    #      - 토큰 수 초과 시 분할 처리
    #
    # 반환값: 
    #   - list: 생성된 사이퍼 쿼리 목록
    async def process_analysis_results():
        nonlocal sp_token_count, context_range, focused_code, extract_code

        try:
            # * context range의 수를 측정하고, 정렬을 진행합니다.
            context_range_count = len(context_range)
            context_range = sorted(context_range, key=lambda x: x['startLine'])
            cypher_queries = []


            # * 분석에 필요한 정보를 llm에게 보냄으로써, 분석을 시작하고, 결과를 처리하는 함수를 호출
            analysis_result = understand_code(extract_code, context_range, context_range_count)        
            cypher_queries = await handle_analysis_result(analysis_result)
            

            # * 다음 분석 주기를 위해 필요한 변수를 초기화합니다
            # TODO 만약 패키지 형태의 경우 프로시저와 함수 처리가 끝났다면 -> 스케줄 스택과, 노드타입 세트 또한 초기화가 필요
            focused_code = ""
            extract_code = ""
            sp_token_count = 0
            context_range.clear()
            return cypher_queries
        
        except UnderstandingError:
            raise
        except Exception:
            err_msg = "Understanding 과정에서 LLM의 결과 처리를 준비 및 시작하는 도중 문제가 발생했습니다."
            logging.error(err_msg, exc_info=False)
            raise ProcessResultError(err_msg)


    # 역할: llm에게 받은 결과를 이용하여, 사이퍼쿼리를 생성하는 함수
    #      - LLM 분석 결과에서 테이블과 필드 정보 추출
    #      - 테이블 노드와 필드 속성 생성 쿼리 생성
    #      - 테이블 간 참조 관계 쿼리 생성
    #      - 코드 구조와 변수 사용 관계 쿼리 생성
    #
    # 매개변수: 
    #   - analysis_result (dict): LLM의 코드 분석 결과
    #       - tableReference (list): 테이블 간 참조 관계
    #       - Tables (dict): 테이블별 필드 정보
    #       - analysis (list): 코드 분석 결과
    #
    # 반환값: 
    #   - list: 생성된 Neo4j 사이퍼 쿼리 목록
    async def handle_analysis_result(analysis_result):
        nonlocal sp_token_count, focused_code, extract_code, context_range
        commands = ["SELECT", "INSERT", "ASSIGNMENT", "UPDATE", "DELETE", "EXECUTE_IMMDDIATE", "IF", "FOR", "COMMIT", "MERGE", "WHILE", "CALL", "PROCEDURE_SPEC", "DECLARE", "PROCEDURE", "FUNCTION", "RETURN", "RAISE", "EXCEPTION"]
        table_fields = defaultdict(set)
                
        try:
            # * llm의 분석 결과에서 변수 및 테이블 정보를 추출하고(ddl정보가 없을 경우에만), 필요한 변수를 초기화합니다 
            table_references = [] if has_ddl_info else analysis_result.get('tableReference', [])
            tables = {} if has_ddl_info else analysis_result.get('Tables', {})


            # * 테이블의 필드를 재구성 및 테이블 생성 사이퍼쿼리를 생성합니다.
            for table, fields in tables.items():
                table_name = table.split('.')[-1]
                table_fields[table_name].update(fields)
                if not fields or '*' in fields:
                    table_query = f"MERGE (t:Table {{name: '{table}', object_name: '{object_name}'}})"
                    cypher_query.append(table_query)
                else:
                    for field in fields:
                        field_name = clean_field_name(field.split(':')[1])
                        field_type = field.split(':')[0]
                        update_query = f"MERGE (t:Table {{name: '{table}', object_name: '{object_name}'}}) WITH t WHERE t.{field_name} IS NULL SET t.{field_name} = '{field_type}'"
                        cypher_query.append(update_query)


            # * 테이블간의 참조 관계를 위한 사이퍼쿼리를 생성합니다.
            for reference in table_references:
                source_table = reference['source'].split('.')[-1]
                target_table = reference['target'].split('.')[-1]
                
                # * 자기 자신의 테이블을 참조하는 경우 무시합니다
                if source_table != target_table:
                    table_reference_query = f"MERGE (source:Table {{name: '{source_table}', object_name: '{object_name}'}}) MERGE (target:Table {{name: '{target_table}', object_name: '{object_name}'}}) MERGE (source)-[:REFERENCES]->(target)"
                    cypher_query.append(table_reference_query)


            # * llm의 분석 결과에서 각 데이터를 추출하고, 필요한 사이퍼쿼리를 생성합니다
            for result in analysis_result['analysis']:
                start_line = result['startLine']
                end_line = result['endLine']
                summary = result['summary']
                tableName = result.get('tableNames', [])
                called_nodes = result.get('calls', [])
                variables = result.get('variables', [])
                var_range = f"{start_line}_{end_line}"


                # * 구문의 타입과 테이블 관계 타입을 얻어냅니다
                statement_type = next((command for command in commands if f"{command}_{start_line}" in node_statementType), None)
                table_relationship_type = "FROM" if statement_type == "SELECT" else "WRITES" if statement_type in ["UPDATE", "INSERT", "DELETE", "MERGE"] else "EXECUTE" if statement_type == "EXECUTE_IMMDDIATE" else None


                # * 구문의 설명(Summary)을 반영하는 사이퍼쿼리를 생성합니다
                summary_query = f"MATCH (n:{statement_type} {{startLine: {start_line}, object_name: '{object_name}'}}) WITH n SET n.summary = {json.dumps(summary)}"
                cypher_query.append(summary_query)


                # * 스케줄 스택에서 있는 코드에서 ...code... 부분을 Summary로 교체해서 업데이트합니다
                for schedule in schedule_stack:
                    pattern = re.compile(rf"^{start_line}: \.\.\. code \.\.\.$", re.MULTILINE)
                    if pattern.search(schedule["code"]):
                        schedule["code"] = pattern.sub(f"{start_line}~{end_line}: {summary}", schedule["code"])
                        break


                # * 변수 노드에 사용된 라인을 나타내는 속성을 추가합니다.
                for var_name in variables:
                    cypher_query.append(f"MATCH (v:Variable {{name: '{var_name}', object_name: '{object_name}', procedure_name: '{procedure_name}'}}) WITH v SET v.`{var_range}` = 'Used'")


                # * CALL 호출 관계를 생성합니다
                if statement_type in ["CALL", "ASSIGNMENT", "EXCEPTION"]:
                    for name in called_nodes:
                        if '.' in name:  # 다른 패키지 호출인 경우
                            package_name, proc_name = name.split('.')
                            call_relation_query = f"MATCH (c:{statement_type} {{startLine: {start_line}, object_name: '{object_name}'}}) WITH c MERGE (p:PROCEDURE {{object_name: '{package_name}', procedure_name: '{proc_name}'}}) MERGE (c)-[:CALLS]->(p)"
                        else:            # 자신 패키지 내부 호출인 경우
                            call_relation_query = f"MATCH (c:{statement_type} {{startLine: {start_line}, object_name: '{object_name}'}}) WITH c MATCH (p) WHERE (p:PROCEDURE OR p:FUNCTION) AND p.object_name = '{object_name}' AND p.procedure_name = '{name}' MERGE (c)-[:CALLS]->(p)"
                        cypher_query.append(call_relation_query)
                    

                # * 테이블과 노드간의 관계를 생성합니다
                if table_relationship_type and tableName:
                    first_table_name = tableName[0].split('.')[-1]
                    table_relationship_query = f"MERGE (n:{statement_type}{{startLine: {start_line}, object_name: '{object_name}'}}) MERGE (t:Table {{name: '{first_table_name}', object_name: '{object_name}'}}) MERGE (n)-[:{table_relationship_type}]->(t)"
                    cypher_query.append(table_relationship_query)

            return cypher_query
        
        except Exception:
            err_msg = "Understanding 과정에서 LLM의 결과를 이용해 사이퍼쿼리를 생성하는 도중 오류가 발생했습니다."
            logging.error(err_msg, exc_info=False)
            raise HandleResultError(err_msg)


    # 역할: 전달된 프로시저 선언부 코드를 분석하여, 변수 노드를 생성하는 함수
    #      - 선언부 코드를 LLM을 통해 분석
    #      - 변수의 타입과 용도를 파악
    #      - 변수 노드와 관련 관계를 생성하는 사이퍼 쿼리 생성
    #
    # 매개변수: 
    #   - declaration_code (str): 분석할 선언부 코드
    #   - node_startLine (int): 선언부의 시작 라인 번호
    #   - statement_type (str): 선언부의 타입
    def process_declaration_part(declaration_code, node_startLine, statement_type):
        try:
            # * 매개변수의 역할을 결정합니다
            role = ('패키지 전역 변수' if statement_type == 'PACKAGE_VARIABLE' else
                    '변수 선언및 초기화' if statement_type == 'DECLARE' else
                    '함수 및 프로시저 입력 매개변수' if statement_type == 'SPEC' else
                    '알 수 없는 매개변수')
            

            # * 변수를 분석하고, 각 타입별로 변수 노드를 생성합니다
            analysis_result = understand_variables(declaration_code, ddl_tables)
            logging.info(f"[{object_name}] 변수 분석 완료\n")
            for variable in analysis_result["variables"]:
                var_parameter_type = variable["parameter_type"]
                var_name = variable["name"]
                var_type = variable["type"]
                
                if statement_type == 'DECLARE':
                    cypher_query.extend([
                        f"MERGE (v:Variable {{name: '{var_name}', object_name: '{object_name}', type: '{var_type}', procedure_name: '{procedure_name}', role: '{role}', scope: 'Local'}}) ",
                        f"MATCH (p:{statement_type} {{startLine: {node_startLine}, object_name: '{object_name}', procedure_name: '{procedure_name}'}}) "
                        f"MATCH (v:Variable {{name: '{var_name}', object_name: '{object_name}', procedure_name: '{procedure_name}'}})"
                        f"MERGE (p)-[:SCOPE]->(v)"
                    ])
                elif statement_type == 'PACKAGE_VARIABLE':
                    cypher_query.extend([
                        f"MERGE (v:Variable {{name: '{var_name}', object_name: '{object_name}', type: '{var_type}', role: '{role}', scope: 'Global', procedure_name: '{procedure_name}'}}) ",
                        f"MATCH (p:{statement_type} {{startLine: {node_startLine}, object_name: '{object_name}', procedure_name: '{procedure_name}'}}) "
                        f"MATCH (v:Variable {{name: '{var_name}', object_name: '{object_name}', scope: 'Global', procedure_name: '{procedure_name}'}})"
                        f"MERGE (p)-[:SCOPE]->(v)"
                    ])
                else:
                    cypher_query.extend([
                        f"MERGE (v:Variable {{name: '{var_name}', object_name: '{object_name}', type: '{var_type}', parameter_type: '{var_parameter_type}', procedure_name: '{procedure_name}', role: '{role}', scope: 'Local'}}) ",
                        f"MATCH (p:{statement_type} {{startLine: {node_startLine}, object_name: '{object_name}', procedure_name: '{procedure_name}'}}) "
                        f"MATCH (v:Variable {{name: '{var_name}', object_name: '{object_name}', procedure_name: '{procedure_name}'}})"
                        f"MERGE (p)-[:SCOPE]->(v)"
                    ])

        except Exception:
            err_msg = "Understanding 과정에서 프로시저 선언부 분석 및 변수 노드 생성 중 오류가 발생했습니다."
            logging.error(err_msg, exc_info=False)
            raise HandleResultError(err_msg)

    
    # 역할: 스토어드 프로시저 코드를 분석하는 메서드를 호출하고, 결과를 이벤트 큐를 통해 처리하는 함수
    #      - 코드 분석 메서드 호출 및 결과 수신
    #      - 분석 결과를 이벤트 큐로 전송
    #      - 처리 완료 응답 대기 및 확인
    #
    # 매개변수: 
    #   - node_end_line (int): 현재 분석 중인 코드 블록의 마지막 라인 번호
    async def signal_for_process_analysis(node_end_line):
        try:
            # * 분석하는 메서드를 호출하고, 기다립니다. 만약 처리가 끝났다면, 분석 완료 이벤트를 송신합니다
            results = await process_analysis_results()
            logging.info(f"[{object_name}] 분석 결과 이벤트 송신")
            await send_queue.put({"type": "analysis_code", "query_data": results, "line_number": node_end_line})

            # * 분석 완료 이벤트를 송신하고, 처리 완료 이벤트를 수신 대기합니다 
            while True:
                response = await receive_queue.get()
                if response['type'] == 'process_completed':
                    logging.info(f"[{object_name}] 분석 결과 처리 완료\n")
                    cypher_query.clear();
                    break;
        
        except UnderstandingError:
            raise
        except Exception:
            err_msg = "Understanding 과정에서 이벤트를 송신하고 수신하는 도중 오류가 발생했습니다."
            logging.error(err_msg, exc_info=False)
            raise EventRsRqError(err_msg)


    # 역할: 스토어드 프로시저 코드를 노드 단위로 순회하며 구조를 분석하는 함수
    #      - 각 노드의 코드를 추출하고 요약
    #      - 노드 간의 부모-자식 관계 생성
    #      - 연속된 노드 간의 실행 순서 관계 생성
    #      - 필요한 경우 LLM 분석 수행
    #
    # 매개변수: 
    #   - node (dict): 분석할 현재 노드
    #   - schedule_stack (list): 처리된 스케줄들의 스택
    #   - parent_startLine (int, optional): 부모 노드의 시작 라인 번호
    #   - parent_statementType (str, optional): 부모 노드의 타입
    async def traverse(node, schedule_stack, parent_startLine=None, parent_statementType=None):
        nonlocal focused_code, sp_token_count, extract_code, procedure_name

        # * 분석에 필요한 필요한 정보를 준비하거나 할당합니다
        start_line, end_line, statement_type = node['startLine'], node['endLine'], node['type']
        summarized_code = extract_and_summarize_code(file_content, node)
        node_code = extract_node_code(file_content, start_line, end_line)
        node_size = count_tokens_in_text(node_code)
        children = node.get('children', [])

        # * 현재 노드의 정보를 저장합니다
        current_schedule = {
            "startLine": start_line,
            "endLine": end_line,
            "code": summarized_code,
            "child": children,
            "type": statement_type
        }

        # * 프로시저 노드의 경우, 프로시저 정보를 저장합니다
        if statement_type in ["PROCEDURE", "CREATE_PROCEDURE_BODY", "FUNCTION"]:
            procedure_name = extract_procedure_name(node_code)


        # * focused_code에서 분석할 범위를 기준으로 잘라냅니다.
        extract_code, line_number = extract_code_within_range(focused_code, context_range)


        # * 노드 크기 및 토큰 수 체크를 하여, 분석 여부를 결정합니다
        sp_token_count = count_tokens_in_text(extract_code)
        if (node_size >= 1000 and context_range and node_size + sp_token_count >= 1300) or (sp_token_count >= 1300 and context_range):
            await signal_for_process_analysis(line_number)


        # * focused_code가 없으면 새로 생성하고, 만약 있다면 확장합니다
        if not focused_code:
            focused_code = create_focused_code(current_schedule, schedule_stack) 
        else:
            placeholder = f"{start_line}: ... code ..."
            focused_code = focused_code.replace(placeholder, summarized_code, 1)


        # * 노드의 타입에 따라서 사이퍼쿼리를 생성 및 해당 노드의 범위를 분석할 범위를 저장합니다
        if not children and statement_type not in ["DECLARE", "PROCEDURE_SPEC", "SPEC", "PACKAGE_VARIABLE"]:
            context_range.append({"startLine": start_line, "endLine": end_line})
            cypher_query.append(f"MERGE (n:{statement_type} {{startLine: {start_line}, object_name: '{object_name}'}}) SET n.endLine = {end_line}, n.name = '{statement_type}[{start_line}]', n.node_code = '{node_code.replace("'", "\\'")}', n.token = {node_size}, n.procedure_name = '{procedure_name}'")
        else:
            if statement_type == "ROOT":
                cypher_query.append(f"MERGE (n:{statement_type} {{startLine: {start_line}, object_name: '{object_name}'}}) SET n.endLine = {end_line}, n.name = '{object_name}', n.summary = '최상위 시작노드'")
            elif statement_type == "PROCEDURE":
                cypher_query.append(f"MERGE (n:{statement_type} {{procedure_name: '{procedure_name}', object_name: '{object_name}'}}) SET n.startLine = {start_line}, n.endLine = {end_line}, n.name = '{statement_type}[{start_line}]', n.summarized_code = '{summarized_code.replace('\n', '\\n').replace("'", "\\'")}', n.node_code = '{node_code.replace('\n', '\\n').replace("'", "\\'")}', n.token = {node_size}")
            else:
                cypher_query.append(f"MERGE (n:{statement_type} {{startLine: {start_line}, object_name: '{object_name}'}}) SET n.endLine = {end_line}, n.name = '{statement_type}[{start_line}]', n.summarized_code = '{summarized_code.replace('\n', '\\n').replace("'", "\\'")}', n.node_code = '{node_code.replace('\n', '\\n').replace("'", "\\'")}', n.token = {node_size}, n.procedure_name = '{procedure_name}'")


        # * 현재 노드가 프로시저 선언부인 경우, 변수 노드를 생성합니다
        if (procedure_name and statement_type in ["SPEC", "DECLARE"]) or statement_type == "PACKAGE_VARIABLE":
            process_declaration_part(node_code, start_line, statement_type)


        # * 스케줄 스택에 현재 스케줄을 넣고, 노드의 타입을 세트에 저장합니다
        schedule_stack.append(current_schedule)
        node_statementType.add(f"{statement_type}_{start_line}")


        # * 부모가 있을 경우, 부모 노드와 현재 노드의 parent_of 라는 관계를 생성합니다
        if parent_statementType:
            cypher_query.append(f"""
                MATCH (parent:{parent_statementType} {{startLine: {parent_startLine}, object_name: '{object_name}'}})
                WITH parent
                MATCH (child:{statement_type} {{startLine: {start_line}, object_name: '{object_name}'}})
                MERGE (parent)-[:PARENT_OF]->(child)
            """)
        prev_statement = prev_id = None


        # * 현재 노드가 자식이 있는 경우, 해당 자식을 순회하면서 traverse함수를 (재귀적으로) 호출하고 처리합니다
        for child in children:
            await traverse(child, schedule_stack, start_line, statement_type)
            
            # * 현재 노드와 이전의 같은 레벨의 노드간의 NEXT 관계를 위한 사이퍼쿼리를 생성합니다.
            if prev_id and prev_statement not in ["FUNCTION", "PROCEDURE", "PACKAGE_VARIABLE", "PROCEDURE_SPEC"]:
                cypher_query.append(f"""
                    MATCH (prev:{prev_statement} {{startLine: {prev_id}, object_name: '{object_name}'}})
                    WITH prev
                    MATCH (current:{child['type']} {{startLine: {child['startLine']}, object_name: '{object_name}'}})
                    MERGE (prev)-[:NEXT]->(current)
                """)
            prev_statement, prev_id = child['type'], child['startLine']


        # * 부모 노드의 자식들이 모두 처리가 끝났다면, 부모 노드도 context_range에 포함합니다. (만약 프로시저, 함수 노드라면 분석을 진행합니다)
        if children:
            if statement_type in ["PROCEDURE", "FUNCTION", "CREATE_PROCEDURE_BODY"] and context_range and focused_code:
                extract_code, line_number = extract_code_within_range(focused_code, context_range)
                logging.info(f"[{procedure_name}] 프로시저 끝 분석 시작\n")
                await signal_for_process_analysis(last_line)
            elif statement_type not in ["CREATE_PROCEDURE_BODY", "ROOT", "PACKAGE_SPEC", "PACKAGE_BODY", "PROCEDURE", "PROCEDURE_SPEC", "FUNCTION", "DECLARE", "BODY", "TRY"]:
                context_range.append({"startLine": start_line, "endLine": end_line})


        # * 조건하에 필요없는 스케줄 스택을 제거합니다 
        schedule_stack[:] = filter(lambda schedule: schedule['child'] and schedule['endLine'] > current_schedule['startLine'], schedule_stack)
        

    try:
        # * traverse 함수를 호출하여, 스토어드 프로시저 분석을 위한 노드 순회를 시작합니다
        await traverse(antlr_data, schedule_stack)

        # * 마지막 노드 그룹에 대한 처리를 합니다
        if context_range and focused_code:
            extract_code, _ = extract_code_within_range(focused_code, context_range)
            await signal_for_process_analysis(last_line)


        # * 뷴석이 끝났다는 의미를 가진 이벤트를 송신합니다.
        logging.info(f"[{object_name}] 전체 분석 완료")
        await send_queue.put({"type": "end_analysis"})

    except UnderstandingError as e:
        await send_queue.put({'type': 'error', 'message': str(e)})
        raise
    except Exception:
        err_msg = "Understanding 과정에서 Traverse로 스토어드 프로시저 코드를 순회하는 도중 오류가 발생했습니다."
        logging.error(err_msg, exc_info=False)
        await send_queue.put({'type': 'error', 'message': err_msg})
        raise TraverseCodeError(err_msg)