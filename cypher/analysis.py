import asyncio
from collections import defaultdict
import logging
import re
import tiktoken
from cypher.cypher_prompt.understand_prompt import understand_code
encoder = tiktoken.get_encoding("cl100k_base")


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


# 역할: 각 노드의 스토어드 프로시저 코드의 첫 라인에서 식별되는 키워드로 타입을 얻어냅니다
# 매개변수: 
#      - code : 특정 노드의 스토어드 프로시저 코드
# 반환값: 식별된 키워드
def identify_first_line_keyword(code):
    try:
        first_line = code.split('\n')[0]   
        
        # * '숫자: 숫자:' 형태 이후의 첫 단어를 기준으로 키워드를 식별합니다.
        pattern = re.compile(r"\d+:\s*\d+:\s*(DECLARE|SELECT|INSERT|UPDATE|DELETE|EXECUTE IMMEDIATE|CREATE OR REPLACE PROCEDURE|IF|FOR|COMMIT|MERGE|WHILE)\b")
        match = re.search(pattern, first_line)

        if match:
            first_keyword = match.group(1)
            return "OPERATION" if first_keyword == "EXECUTE IMMEDIATE" else first_keyword
        return "ASSIGN"
    except Exception:
        logging.exception("Error occurred while identifying first line keyword(understanding)")
        raise


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


# 역할: 전달된 노드의 스토어드 프로시저 코드에서 자식이 있을 경우 자식 부분을 요약합니다.
# 매개변수: 
#   file_content - 스토어드 프로시저 파일 전체 내용
#   node - (시작 라인, 끝 라인, 코드, 자식)을 포함한 노드 정보
# 반환값: 자식 코드들이 요약 처리된 노드 코드
def extract_and_summarize_code(file_content, node):

    def summarize_code(start_line, end_line, children):

        # * 시작 라인과 끝 라인을 기준으로 스토어드 프로시저 코드 라인을 추출합니다.
        code_lines = file_content[start_line-1:end_line]
        summarized_code = []
        last_end_line = start_line - 1


        # * 각 자식 노드에 대해 코드를 추출한 뒤, 요약 처리를 반복합니다.
        for child in children:
            before_child_code = code_lines[last_end_line-start_line+1:child['startLine']-start_line]
            summarized_code.extend([f"{i+last_end_line+1}: {line}" for i, line in enumerate(before_child_code)])
            summarized_code.append(f"{child['startLine']}: ... code ...\n")
            last_end_line = child['endLine']
        
        # * 마지막 자식 노드의 끝나는 지점 이후부터 노드의 끝나는 지점까지의 코드를 추가합니다
        after_last_child_code = code_lines[last_end_line-start_line+1:]
        summarized_code.extend([f"{i+last_end_line+1}: {line}" for i, line in enumerate(after_last_child_code)])
        return ''.join(summarized_code)
    
    try:
        # * 자식 노드가 없는 경우, 해당 노드의 코드만을 추출합니다.
        if not node.get('children'):
            code_lines = file_content[node['startLine']-1:node['endLine']]
            return ''.join([f"{i+node['startLine']}: {line}" for i, line in enumerate(code_lines)])
        else:
            # * 자식 노드가 있는 경우, summarize_code 함수를 호출하여 처리합니다.
            return summarize_code(node['startLine'], node['endLine'], node.get('children', []))
    except Exception:
        logging.exception("during summarize code unexpected error occurred(understanding)")
        raise


# 역할: 현재 스케줄에서 시작하여 스택에 있는 모든 스케줄을 역순으로 검토하면서 필요한 스토어드 프로시저 코드를 조합합니다.
# 매개변수: 
#      - current_schedule (dict): 현재 처리 중인 스케줄 정보
#      - schedule_stack (list): 처리된 스케줄들의 스택
# 반환값: 분석에 사용될 스토어드 프로시저 코드.
def create_focused_code(current_schedule, schedule_stack):
    try:
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
        logging.exception("An error occurred while creating focused code(understanding)")
        raise


# 역할: 전달된 스토어드 프로시저 코드에서 불필요한 정보를 제거합니다.
# 매개변수: 
#      - code : 스토어드 프로시저 코드
# 반환값: 불필요한 정보가 제거된 스토어드 프로시저 코드.
def remove_unnecessary_information(code):
    try:
        if not code: return "" 

        # * 모든 주석을 제거합니다.
        code = re.sub(r'/\*.*?\*/', '', code, flags=re.DOTALL)
        code = re.sub(r'--.*$', '', code, flags=re.MULTILINE)
        # code = re.sub(r'^[\d\s:]*$', '', code, flags=re.MULTILINE)
        return code
    
    except Exception:
        logging.exception("Error during code placeholder removal(converting)")
        raise


# 역할: 특정 노드의 스토어드 프로시저 코드를 구하는 함수입니다.
# 매개변수: 
#     - file_content : 스토어드 프로시저 파일 전체 내용
#     - start_line : 시작 라인 번호
#     - end_line : 끝 라인 번호
# 반환값: 범위에 맞게 추출된 스토어드 프로시저 코드.
def extract_node_code(file_content, start_line, end_line):
    try:
        # * 지정된 라인 번호를 기준으로 코드를 추출합니다.
        extracted_lines = file_content[start_line-1:end_line]

        # * 추출된 라인들 앞에 라인 번호를 추가하고 하나의 문자열로 연결합니다.
        numbered_lines = [f"{i + start_line}: {line}" for i, line in enumerate(extracted_lines)]
        return ''.join(numbered_lines)
    except Exception:
        logging.exception("Error occurred while extracting node code")
        raise


# 역할: 프로시저 노드 코드에서 필요한 코드 부분만 추출하는 메서드입니다.
# 매개변수: 
#      - procedure_code : 프로시저 노드 부분의 스토어드 프로시저 코드.
# 반환값: 프로시저 노드 코드에서 변수 선언 부분만 필터링된 코드.
def process_procedure_node(procedure_code):
    try:

        # * AS 키워드를 찾아서 AS 이후 모든 라인을 제거합니다.
        as_index = procedure_code.find(' AS')
        if as_index != -1:
            newline_after_as = procedure_code.find('\n', as_index)
            procedure_code = procedure_code[:newline_after_as]


        # * 모든 주석 제거
        procedure_code = re.sub(r'/\*.*?\*/', '', procedure_code, flags=re.DOTALL)
        procedure_code = re.sub(r'--.*$', '', procedure_code, flags=re.MULTILINE)

        # * 처리된 코드에서 마지막 라인 번호 추출
        last_line = procedure_code.strip().split('\n')[-1]
        last_line_number = int(last_line.split(':')[0].strip())

        return procedure_code, last_line_number

    except Exception:
        logging.exception("Error during code placeholder removal(understanding)")
        raise


# 역할: 주어진 데이터(스토어드 프로시저 파일, ANTLR 분석 결과 파일)를 분석하여 사이퍼 쿼리를 생성을 시작하는 함수
# 매개변수: 
#   data - 분석할 데이터 구조(ANTLR)
#   file_content - 분석할 스토어드 프로시저 파일의 내용.
#   send_queue - 이벤트 송신큐
#   receive_queue - 이벤트 수신큐,
#   procedure_variable - 분석할 스토어드 프로시저 파일의 끝 라인
# 반환값 : 없음
async def analysis(data, file_content, send_queue, receive_queue, last_line):
    schedule_stack = []               # 스케줄 스택
    context_range = []                # LLM이 분석할 스토어드 프로시저의 범위
    cypher_query = []                 # 사이퍼 쿼리를 담을 리스트
    node_statementType = set()        # 노드의 타입을 저장할 세트
    extract_code = ""                 # 범위 만큼 추출된 스토어드 프로시저
    clean_code= ""                    # 불필요한 정보(주석)가 제거된 스토어드 프로시저
    focused_code = ""                 # 전체적인 스토어드 프로시저 코드의 틀
    token_count = 0                   # 토큰 수
    LLM_count = 0                     # LLM 호출 횟수


    logging.info("\n Start creating a cypher query \n")


    # 역할: llm에게 분석할 코드를 전달한 뒤, 해당 결과를 바탕으로 사이퍼 쿼리를 생성합니다
    # 반환값 : 생성된 사이퍼쿼리
    async def process_analysis_results():
        nonlocal clean_code, token_count, LLM_count, focused_code, extract_code
        commands = ["DECLARE", "SELECT", "INSERT", "ASSIGN", "UPDATE", "DELETE", "OPERATION", "CREATE_PROCEDURE_BODY", "IF", "FOR", "COMMIT", "MERGE", "WHILE"]
        
        try:
            # * 전달된 코드를 llm에게 보냄으로써, 분석을 시작합니다
            context_range_count = len(context_range)
            analysis_result = understand_code(clean_code, context_range, context_range_count)
            LLM_count += 1
            

            # * llm의 분석 결과에서 변수 및 테이블 정보를 추출하고, 필요한 변수를 초기화합니다 
            table_references = analysis_result.get('tableReference', [])
            tables = analysis_result.get('Tables', [])
            variables = analysis_result.get('variable', [])
            update_variables = True


            # * llm의 분석 결과에서 각 데이터를 추출하고, 필요한 변수를 초기화합니다 
            for result in analysis_result['analysis']:
                start_line = result['startLine']
                if start_line == 0: continue
                end_line = result['endLine']
                summary = result['summary']
                table_names = result.get('tableName', [])

                table_relationship_type = None
                variable_relationship_type = None
                statement_type = None
                first_table_name = None


                # * 스케줄 스택에서 있는 코드에서 ...code... 부분을 Summary로 교체해서 업데이트합니다
                for schedule in schedule_stack:
                    pattern = re.compile(rf"^{start_line}: \.\.\. code \.\.\.$", re.MULTILINE)
                    if pattern.search(schedule["code"]):
                        schedule["code"] = pattern.sub(f"{start_line}~{end_line}: {summary}", schedule["code"])
                        break


                # * statement_type을 얻어냅니다
                for command in commands:
                    key = f"{command}_{start_line}"
                    if key in node_statementType:
                        if command == "SELECT":
                            table_relationship_type = "FROM"
                        elif command in ["UPDATE", "INSERT", "DELETE", "MERGE"]:
                            table_relationship_type = "WRITES"
                        elif command == "OPERATION":
                            table_relationship_type = "EXECUTE"
                        statement_type = command
                        break


                # * 변수와 노드의 관계를 생성하기 위한 관계를 생성합니다
                if statement_type in ["CREATE_PROCEDURE_BODY", "DECLARE"]:
                    variable_relationship_type = "SCOPE"


                # * 변수 및 노드를 생성하거나, 변수와 노드간의 관계를 생성합니다
                for start_line, variables in variables.items():
                    for variable in variables:
                        var_name = variable['name']
                        var_role = variable['role'].replace("'", "\\'")
                        var_type = variable['type']
                        var_startLine = start_line
                    
                        # * SCOPE일 때만 변수 노드를 생성합니다.
                        if variable_relationship_type == "SCOPE":
                            variable_query = f"MERGE (v:Variable {{name: '{var_name}'}}) SET v.role_{var_startLine} = '{var_role}', v.type = '{var_type}'"
                            cypher_query.append(variable_query)
                            variable_relationship_query = f"MERGE (n:{statement_type} {{startLine: {var_startLine}}}) MERGE (v:Variable {{name: '{var_name}'}}) MERGE (n)-[:{variable_relationship_type}]->(v)"
                            cypher_query.append(variable_relationship_query)
                        
                        # * 기존 변수 노드의 값을 업데이트합니다.
                        elif update_variables:
                            variable_update_query = f"MATCH (v:Variable {{name: '{var_name}'}}) SET v.role_{var_startLine} = '{var_role}'"
                            cypher_query.append(variable_update_query)
                            update_variables = False
                


                # * 테이블별로 필드 타입과 필드명을 저장할 중첩 defaultdict를 생성합니다.
                table_fields = defaultdict(lambda: defaultdict(set))



                # * 테이블 노드와 관계를 생성하기 위해 테이블의 정보를 재구성합니다.
                for name in table_names:
                    table_name = name.split('.')[-1]
                    for table_dict in tables:
                        matching_fields = next((fields for full_name, fields in table_dict.items() if full_name.split('.')[-1] == table_name), None)
                        if matching_fields:
                            for field in matching_fields:
                                field_type, field_name = field.split(':', 1)
                                table_fields[table_name][field_type].add(field_name)
                            break  # 테이블 정보를 찾았으므로 루프 종료
                                
                    if first_table_name is None:
                        first_table_name = table_name



                # * 테이블 및 테이블과 노드간의 관계 생성을 위한 사이퍼쿼리를 생성합니다. (필드가 * 이거나 없는 경우 테이블만 생성)
                for table, type_fields in table_fields.items():
                    if not type_fields or '*' in type_fields:
                        table_query = f"MERGE (t:Table {{name: '{table}'}})"
                    else:  
                        fields_update_string = ", ".join([f"t.{field_type} = {list(field_names)}" for field_type, field_names in type_fields.items()])
                        table_query = f"MERGE (t:Table {{name: '{table}'}}) SET {fields_update_string}"
                    cypher_query.append(table_query)

                    # * 테이블과 노드간의 관계를 생성합니다
                    if table_relationship_type:
                        table_relationship_query = f"MERGE (n:{statement_type} {{startLine: {start_line}}}) MERGE (t:Table {{name: '{first_table_name}'}}) MERGE (n)-[:{table_relationship_type}]->(t)"
                        cypher_query.append(table_relationship_query)


                # * 테이블간의 참조 관계를 위한 사이퍼쿼리를 생성합니다. (.이후에 오는 테이블 이름을 추출합니다)
                for reference in table_references:
                    source_table = reference['source'].split('.')[-1]
                    target_table = reference['target'].split('.')[-1]
                    
                    # * 자기 자신의 테이블을 참조하는 경우 무시합니다
                    if source_table != target_table:
                        table_reference_query = f"MERGE (source:Table {{name: '{source_table}'}}) MERGE (target:Table {{name: '{target_table}'}}) MERGE (source)-[:REFERENCES]->(target)"
                        cypher_query.append(table_reference_query)


            # * 다음 분석 주기를 위해 필요한 변수를 초기화합니다
            focused_code = ""
            clean_code = ""
            extract_code = ""
            token_count = 0
            update_variables = True
            context_range.clear()
            return cypher_query
        except Exception:
            logging.error("An error occurred during analysis results processing(understanding)")
            raise

    
    # 역할: 토큰 수가 최대치를 초과할 경우, 분석하는 메서드를 호출하고, 결과를 이벤트 큐에 담아서 전송합니다.
    # 매개변수 :
    # 
    async def signal_for_process_analysis(node_end_line):
        try:
            # * 분석하는 메서드를 호출하고, 기다립니다. 만약 처리가 끝났다면, 분석 완료 이벤트를 송신합니다
            cypher_query_task = asyncio.create_task(process_analysis_results())
            results = await asyncio.gather(cypher_query_task)
            logging.info("Event Send")
            await send_queue.put({"type": "analysis_code", "query_data": results[0], "line_number": node_end_line})
            

            # * 분석 완료 이벤트를 송신하고, 처리 완료 이벤트를 수신 대기합니다 
            while True:
                response = await receive_queue.get()
                if response['type'] == 'process_completed':
                    logging.info("\nEnd Process Event Received\n")
                    cypher_query.clear();
                    break;
        
        except Exception:
            logging.exception(f"An error occurred during signal_for_process_analysis(understanding)")
            raise


    # 스토어드 프로시저와 ANTLR의 분석결과를 이용하여, 재귀적으로 노드를 순회하면서 구조를 탐색합니다.
    # 매개변수: 
    #   node - 분석할 노드.
    #   schedule_stack - 처리된 스케줄들의 스택.
    #   send_queue - 이벤트 송신큐.
    #   receive_queue - 이벤트 수신큐
    #   parent_alias - 부모 노드의 별칭 (기본값 None)
    #   parent_id - 현재 노드의 부모 노드 ID.
    #   parent_statementType - 현잰 노드의 부모 노드 타입
    # 반환값: 없음
    async def traverse(node, schedule_stack, send_queue, receive_queue, parent_alias=None, parent_id=None, parent_statementType=None):
        nonlocal focused_code, token_count, clean_code, extract_code
        
        # * 분석에 필요한 필요한 정보를 준비하거나 할당합니다
        summarized_code = remove_unnecessary_information(extract_and_summarize_code(file_content, node))
        node_code = remove_unnecessary_information(extract_node_code(file_content, node['startLine'], node['endLine']))
        statementType = "ROOT" if node['startLine'] == 0 else node['type'] if node['type'] in ["DECLARE", "CREATE_PROCEDURE_BODY"] else identify_first_line_keyword(summarized_code)
        node_size = count_tokens_in_text(node_code)
        children = node.get('children', [])
        node_alias = f"n{node['startLine']}"
        current_schedule = {
            "startLine": node['startLine'],
            "endLine": node['endLine'],
            "code": summarized_code,
            "child": children,
        }


        # * CREATE_PROCEDURE_BODY는 별도로 처리하는 메서드를 호출(그래프를 순차적으로 보여주기 위함)
        if statementType == "CREATE_PROCEDURE_BODY":
            clean_code, last_line_number = process_procedure_node(summarized_code)
            context_range.append({"startLine": node['startLine'], "endLine": last_line_number})
            cypher_query.append(f"CREATE ({node_alias}:{statementType}{{startLine: {node['startLine']}, endLine: {node['endLine']}, name: '{statementType}[{node['startLine']}]', summarzied_code: '{summarized_code.replace('\n', '\\n').replace("'", "\\'")}', node_code: '{node_code.replace('\n', '\\n').replace("'", "\\'")}', clean_code: '{clean_code.replace('\n', '\\n').replace("'", "\\'")}', token: {node_size}}})")
            node_statementType.add(f"{statementType}_{node['startLine']}")
            signal_task = asyncio.create_task(signal_for_process_analysis(node['endLine']))
            await asyncio.gather(signal_task)


        # * focused_code에서 분석할 범위를 기준으로 잘라내고, 불필요한 정보를 제거합니다
        extract_code = extract_code_within_range(focused_code, context_range)
        clean_code = remove_unnecessary_information(extract_code)


        # * 노드 크기 및 토큰 수 체크를 하여, 분석 여부를 결정합니다
        token_count = count_tokens_in_text(clean_code)
        if (node_size >= 1200 and context_range) or (token_count >= 900 and context_range) or (len(context_range) > 12):
            signal_task = asyncio.create_task(signal_for_process_analysis(node['endLine']))
            await asyncio.gather(signal_task)


        # * focused_code가 없으면 새로 생성하고, 만약 있다면 확장합니다
        if not focused_code:
            focused_code = create_focused_code(current_schedule, schedule_stack) 
        else:
            placeholder = f"{node['startLine']}: ... code ..."
            focused_code = focused_code.replace(placeholder, summarized_code, 1)


        # * 노드의 사이퍼쿼리를 생성 및 해당 노드의 범위를 분석할 범위를 저장
        if not children:
            context_range.append({"startLine": node['startLine'], "endLine": node['endLine']})
            cypher_query.append(f"MERGE ({node_alias}:{statementType}{{startLine: {node['startLine']}}}) ON CREATE SET {node_alias}.endLine = {node['endLine']}, {node_alias}.name = '{statementType}[{node['startLine']}]', {node_alias}.node_code = '{node_code.replace('\n', '\\n').replace("'", "\\'")}', {node_alias}.token = {node_size}")
        else:
            cypher_query.append(f"MERGE ({node_alias}:{statementType}{{startLine: {node['startLine']}}}) ON CREATE SET {node_alias}.endLine = {node['endLine']}, {node_alias}.name = '{statementType}[{node['startLine']}]', {node_alias}.summarized_code = '{summarized_code.replace('\n', '\\n').replace("'", "\\'")}', {node_alias}.node_code = '{node_code.replace('\n', '\\n').replace("'", "\\'")}', {node_alias}.token = {node_size}")


        # * 스케줄 스택에 현재 스케줄을 넣고, 노드의 타입을 세트에 저장합니다
        schedule_stack.append(current_schedule)
        node_statementType.add(f"{statementType}_{node['startLine']}")


        # * 부모 변수가 있을 경우(부모가 존재할 경우), 부모 노드와 현재 노드의 parent_of 라는 관계를 생성합니다
        if parent_alias:
            cypher_query.append(f"MATCH ({parent_alias}:{parent_statementType} {{startLine: {parent_id}}}) WITH {parent_alias} MATCH ({node_alias}:{statementType} {{startLine: {node['startLine']}}}) MERGE ({parent_alias})-[:PARENT_OF]->({node_alias})")
        prev_alias = None
        prev_id = None


        # * 현재 노드가 자식이 있는 경우, 해당 자식을 순회하면서 traverse함수를 (재귀적으로) 호출하고 처리합니다
        for child in children:
            node_explore_task = asyncio.create_task(traverse(child, schedule_stack, send_queue, receive_queue, node_alias, node['startLine'], statementType))
            await asyncio.gather(node_explore_task)

            if prev_alias:
                cypher_query.append(f"MATCH ({prev_alias} {{startLine: {prev_id}}}) WITH {prev_alias} MATCH (n{child['startLine']} {{startLine: {child['startLine']}}}) MERGE ({prev_alias})-[:NEXT]->(n{child['startLine']})")
            prev_alias = f"n{child['startLine']}"
            prev_id = child['startLine']


        # * 부모 노드가 가진 자식들이 모두 처리가 끝났다면, 부모 노드도 context_range에 포함합니다
        if children and node['type'] == "STATEMENT":
            context_range.append({"startLine": node['startLine'], "endLine": node['endLine']})
        

        # * 조건하에 필요없는 스케줄 스택을 제거합니다 
        schedule_stack[:] = filter(lambda schedule: schedule['child'] and schedule['endLine'] > current_schedule['startLine'], schedule_stack)
        

    try:
        # * traverse 함수를 호출하여, PLSQL 분석을 위한 노드 순회를 시작합니다
        start_analysis_task = asyncio.create_task(traverse(data, schedule_stack, send_queue, receive_queue, None, None))
        await asyncio.gather(start_analysis_task)


        # * 마지막 노드 그룹에 대한 처리를 합니다
        if context_range and focused_code is not None:
            extract_code = extract_code_within_range(focused_code, context_range)
            clean_code = remove_unnecessary_information(extract_code)
            signal_task = asyncio.create_task(signal_for_process_analysis(last_line))
            await asyncio.gather(signal_task)

        logging.info("\nLLM 호출 횟수 : " + str(LLM_count))
        await send_queue.put({"type": "end_analysis"})

    except Exception:
        logging.exception("An error occurred during the analysis process(understanding)")
        raise