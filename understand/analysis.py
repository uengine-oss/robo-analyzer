import asyncio
from collections import defaultdict
import logging
import re
import tiktoken
from prompt.understand_prompt import understand_code
from util.exception import (TokenCountError, ExtractCodeError, SummarizeCodeError, FocusedCodeError, TraverseCodeError, UnderstandingError,
                            RemoveInfoCodeError, ProcessResultError, HandleResultError, LLMCallError, EventRsRqError, CreateNodeError)


encoder = tiktoken.get_encoding("cl100k_base")


# 역할: 전달된 스토어드 프로시저 코드의 토큰의 개수를 계산하는 함수
# 매개변수: 
#   - code : 토큰을 계산할 스토어드 프로시저 코드
# 반환값: 
#   - len(tokens) : 계산된 토큰의 수
def count_tokens_in_text(code):
    
    if not code: return 0

    try:
        # * 코드를 토큰화하고 토큰의 개수를 반환합니다.
        tokens = encoder.encode(code)
        return len(tokens)
    except Exception:
        err_msg = "Understanding 과정에서 토큰 계산 중 오류가 발생했습니다"
        logging.exception(err_msg)
        raise TokenCountError(err_msg)


# 역할: 주어진 범위에서 startLine과 endLine을 추출한 뒤, 스토어드 프로시저 코드를 잘라내는 함수
# 매개변수: 
#   - code : 스토어드 프로시저 코드
#   - context_range : 잘라낼 범위를 나타내는 딕셔너리의 리스트
# 반환값: 
#   - extracted_code : 범위에 맞게 추출된 스토어드 프로시저 코드
#   - end_line : 추출된 스토어드 프로시저 코드의 마지막 라인 번호
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
        logging.exception(err_msg)
        raise ExtractCodeError(err_msg)


# 역할: 전달된 노드의 스토어드 프로시저 코드에서 자식이 있을 경우 자식 부분을 요약하는 함수
# 매개변수: 
#   - file_content : 스토어드 프로시저 파일 전체 내용
#   - node : 노드(시작 라인, 끝 라인, 코드, 자식)
# 반환값: 
#   - 자식 코드들이 요약 처리된 코드 및 원래 자기 자신 코드
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
        err_msg = "Understanding 과정에서 코드를 요약하는 도중 오류가 발생했습니다."
        logging.exception(err_msg)
        raise SummarizeCodeError(err_msg)


# 역할: 현재 스케줄에서 시작하여 스택에 있는 모든 스케줄을 역순으로 검토하면서 필요한 스토어드 프로시저 코드를 조합하는 함수
# 매개변수: 
#   - current_schedule : 현재 처리 중인 스케줄(노드) 정보
#   - schedule_stack : 처리된 스케줄(노드)들의 스택
# 반환값: 
#   - focused_code : 분석에 사용될 스토어드 프로시저 코드.
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
        err_msg = "Understanding 과정에서 분석할 코드 생성 도중에 오류가 발생했습니다."
        logging.exception(err_msg)
        raise FocusedCodeError(err_msg)


# 역할: 전달된 스토어드 프로시저 코드에서 불필요한 정보를 제거하는 함수
# 매개변수: 
#   - code : 스토어드 프로시저 코드
# 반환값: 
#   - clean_code : 불필요한 정보가 제거된 스토어드 프로시저 코드.
def remove_unnecessary_information(code):
    try:
        if not code: return "" 


        # * 프로시저 코드내에 모든 주석을 제거합니다.
        clean_code = re.sub(r'/\*.*?\*/', '', code, flags=re.DOTALL)
        clean_code = re.sub(r'--.*$', '', clean_code, flags=re.MULTILINE)
        # code = re.sub(r'^[\d\s:]*$', '', code, flags=re.MULTILINE)
        return clean_code
    
    except Exception:
        err_msg = "Understanding 과정에서 불필요한 정보를 제거하는 도중 오류가 발생했습니다."
        logging.exception(err_msg)
        raise RemoveInfoCodeError(err_msg)


# 역할: 특정 노드의 스토어드 프로시저 코드를 추출하는 함수
# 매개변수: 
#   - file_content : 스토어드 프로시저 파일 전체 내용
#   - start_line : 시작 라인 번호
#   - end_line : 끝 라인 번호
# 반환값: 
#   - extracted_node_code : 범위에 맞게 추출된 스토어드 프로시저 코드.
def extract_node_code(file_content, start_line, end_line):
    try:
        # * 지정된 라인 번호를 기준으로 코드를 추출합니다.
        extracted_lines = file_content[start_line-1:end_line]


        # * 추출된 라인들 앞에 라인 번호를 추가하고 하나의 문자열로 연결합니다.
        extracted_node_code = ''.join(f"{i + start_line}: {line}" for i, line in enumerate(extracted_lines))
        return extracted_node_code
    
    except Exception:
        err_msg = "Understanding 과정에서 노드에 맞게 코드를 추출 도중에 오류가 발생했습니다."
        logging.exception(err_msg)
        raise ExtractCodeError(err_msg)


# 역할: 프로시저 선언 노드 코드에서 필요한 코드 부분만 추출하는 함수
# 매개변수: 
#   - sp_code : 프로시저 선언 노드 부분의 코드
# 반환값: 
#   - procedure_code : 입력 매개변수 선언 부분만 필터링된 코드
def filter_procedure_declare_code(procedure_code):
    try:

        # * 프로시저 키워드를 찾아서 해당 이후 라인 부터 시작합니다.
        create_index = procedure_code.find('CREATE OR REPLACE PROCEDURE')
        if create_index != -1:
            newline_after_create = procedure_code.find('\n', create_index)
            procedure_code = procedure_code[newline_after_create + 1:]


        # * AS 키워드를 찾아서 AS를 포함한 줄부터 모든 라인을 제거합니다.
        as_index = procedure_code.find('AS')
        if as_index != -1:
            newline_before_as = procedure_code.rfind('\n', 0, as_index)
            procedure_code = procedure_code[:newline_before_as]


        # * 모든 주석 제거
        procedure_code = re.sub(r'/\*.*?\*/', '', procedure_code, flags=re.DOTALL)
        procedure_code = re.sub(r'--.*$', '', procedure_code, flags=re.MULTILINE)

        return procedure_code

    except Exception:
        err_msg = "Understanding 과정에서 프로시저 노드 코드를 추출 도중에 오류가 발생했습니다."
        logging.exception(err_msg)
        raise ExtractCodeError(err_msg)


# 역할: 주어진 데이터(스토어드 프로시저 파일, ANTLR 분석 결과 파일)를 분석하여 사이퍼 쿼리를 생성을 시작하는 함수
# 매개변수: 
#   - antlr_data : 분석할 데이터 구조(ANTLR)
#   - file_content : 분석할 스토어드 프로시저 파일의 내용.
#   - send_queue : 이벤트 송신큐
#   - receive_queue : 이벤트 수신큐,
#   - last_line : 분석할 스토어드 프로시저 파일의 끝 라인
# 반환값 : 없음
async def analysis(antlr_data, file_content, send_queue, receive_queue, last_line):
    schedule_stack = []               # 스케줄 스택
    context_range = []                # LLM이 분석할 스토어드 프로시저의 범위
    cypher_query = []                 # 사이퍼 쿼리를 담을 리스트
    node_statementType = set()        # 노드의 타입을 저장할 세트
    extract_code = ""                 # 범위 만큼 추출된 스토어드 프로시저
    clean_code= ""                    # 불필요한 정보(주석)가 제거된 스토어드 프로시저
    focused_code = ""                 # 전체적인 스토어드 프로시저 코드의 틀
    sp_token_count = 0                # 토큰 수
    LLM_count = 0                     # LLM 호출 횟수


    logging.info("\n Start creating a cypher query \n")


    # 역할: llm에게 분석할 코드를 전달한 뒤, 총 토큰 수에 따라서 어떻게 처리 할 지 결정하는 함수
    # 매개변수 : 없음
    # 반환값 : 
    #   - cypher_queries: 생성된 사이퍼쿼리 목록
    async def process_analysis_results():
        nonlocal clean_code, sp_token_count, LLM_count, context_range

        try:
            # * context range의 수를 측정하고, 정렬을 진행합니다.
            context_range_count = len(context_range)
            context_range = sorted(context_range, key=lambda x: x['startLine'])
            LLM_count += 1


            # * 분석에 필요한 정보를 llm에게 보냄으로써, 분석을 시작합니다
            analysis_result, prompt_template = understand_code(clean_code, context_range, context_range_count)
            combined_context = f"{prompt_template}\n{context_range}\n{analysis_result}"
            combined_context_tokens = count_tokens_in_text(combined_context)
            logging.info(f"토큰 수 : {combined_context_tokens + sp_token_count}")
            
            
            # * 토큰 수가 초과하였는지 검사합니다.
            if combined_context_tokens + sp_token_count <= 4096:
                return await handle_analysis_result(analysis_result)


            # * 토큰 수가 초과된 경우 처리로 부모 범위를 찾고, 분석 범위를 절반으로 나눕니다.
            cypher_queries = []
            largest_range_index = max(range(context_range_count), key=lambda i: context_range[i]['endLine'] - context_range[i]['startLine'])
            parent_range = context_range.pop(largest_range_index)
            half_point = context_range_count // 2
            

            # * 절반으로 나뉘어진 context_range를 처리합니다
            for sub_range in [context_range[:half_point], context_range[half_point:]]:
                if sub_range:
                    sub_clean_code, _ = extract_code_within_range(clean_code, sub_range)
                    sub_analysis_result, _ = understand_code(sub_clean_code, sub_range, len(sub_range))
                    cypher_queries.extend(await handle_analysis_result(sub_analysis_result))
            

            # * 마지막으로 부모 범위를 처리합니다.
            parent_schedule = next((schedule for schedule in schedule_stack if schedule['startLine'] == parent_range['startLine']), None)
            if parent_schedule:
                parent_clean_code = parent_schedule['code']
                parent_analysis_result, _ = understand_code(parent_clean_code, [parent_range], 1)
                cypher_queries.extend(await handle_analysis_result(parent_analysis_result))

            return cypher_queries
        
        except UnderstandingError:
            raise
        except Exception:
            err_msg = "Understanding 과정에서 LLM의 결과 처리를 준비 및 시작하는 도중 문제가 발생했습니다."
            logging.exception(err_msg)
            raise ProcessResultError(err_msg)
        

    # 역할: llm에게 받은 결과를 이용하여, 사이퍼쿼리를 생성하는 함수
    # 매개변수 : 
    #   - analysis_result : llm의 분석 결과
    # 반환값 : 
    #   - cypher_queries: 생성된 사이퍼쿼리 목록
    async def handle_analysis_result(analysis_result):
        nonlocal clean_code, sp_token_count, focused_code, extract_code, context_range
        commands = ["SELECT", "INSERT", "ASSIGNMENT", "UPDATE", "DELETE", "EXECUTE_IMMDDIATE", "IF", "FOR", "COMMIT", "MERGE", "WHILE"]
        table_fields = defaultdict(set)
                
        try:
            # * llm의 분석 결과에서 변수 및 테이블 정보를 추출하고, 필요한 변수를 초기화합니다 
            table_references = analysis_result.get('tableReference', [])
            tables = analysis_result.get('Tables', {})
            variables_list = analysis_result.get('variable', {})


            # * 변수 노드의 정보(역할)을 업데이트합니다.
            for var_startLine, variables in variables_list.items():
                sanitized_var_startLine = var_startLine.replace('~', '_')
                for variable in variables:
                    var_name = variable['name']
                    var_role = variable['role'].replace("'", "\\'")
                    variable_update_query = f"MATCH (v:Variable {{name: '{var_name}'}}) SET v.`{sanitized_var_startLine}` = '{var_role}'"
                    cypher_query.append(variable_update_query)

            
            # * 테이블의 필드를 재구성 및 테이블 생성 사이퍼쿼리를 생성합니다.
            for table, fields in tables.items():
                table_name = table.split('.')[-1]
                table_fields[table_name].update(fields)
                if not fields or '*' in fields:
                    table_query = f"MERGE (t:Table {{name: '{table}'}})"
                else:  
                    fields_update_string = ", ".join([f"t.{field.split(':')[1]} = '{field.split(':')[0]}'" for field in fields])
                    table_query = f"MERGE (t:Table {{name: '{table}'}}) SET {fields_update_string}"
                cypher_query.append(table_query)


            # * 테이블간의 참조 관계를 위한 사이퍼쿼리를 생성합니다.
            for reference in table_references:
                source_table = reference['source'].split('.')[-1]
                target_table = reference['target'].split('.')[-1]
                
                # * 자기 자신의 테이블을 참조하는 경우 무시합니다
                if source_table != target_table:
                    table_reference_query = f"MERGE (source:Table {{name: '{source_table}'}}) MERGE (target:Table {{name: '{target_table}'}}) MERGE (source)-[:REFERENCES]->(target)"
                    cypher_query.append(table_reference_query)


            # * llm의 분석 결과에서 각 데이터를 추출하고, 필요한 변수를 초기화합니다 
            for result in analysis_result['analysis']:
                start_line = result['startLine']
                if start_line == 0: continue
                end_line = result['endLine']
                summary = result['summary']
                tableName = result.get('tableName', [])


                # * 스케줄 스택에서 있는 코드에서 ...code... 부분을 Summary로 교체해서 업데이트합니다
                for schedule in schedule_stack:
                    pattern = re.compile(rf"^{start_line}: \.\.\. code \.\.\.$", re.MULTILINE)
                    if pattern.search(schedule["code"]):
                        schedule["code"] = pattern.sub(f"{start_line}~{end_line}: {summary}", schedule["code"])
                        break


                # * 구문의 타입과 테이블 관계 타입을 얻어냅니다
                statement_type = next((command for command in commands if f"{command}_{start_line}" in node_statementType), None)
                table_relationship_type = "FROM" if statement_type == "SELECT" else "WRITES" if statement_type in ["UPDATE", "INSERT", "DELETE", "MERGE"] else "EXECUTE" if statement_type == "EXECUTE_IMMDDIATE" else None

                
                # * 테이블과 노드간의 관계를 생성합니다
                if table_relationship_type and tableName:
                    first_table_name = tableName[0].split('.')[-1]
                    table_relationship_query = f"MERGE (n:{statement_type}{{startLine: {start_line}}}) MERGE (t:Table {{name: '{first_table_name}'}}) MERGE (n)-[:{table_relationship_type}]->(t)"
                    cypher_query.append(table_relationship_query)


            # * 다음 분석 주기를 위해 필요한 변수를 초기화합니다
            focused_code = ""
            clean_code = ""
            extract_code = ""
            sp_token_count = 0
            context_range.clear()
            return cypher_query
        
        except Exception:
            err_msg = "Understanding 과정에서 LLM의 결과를 이용해 사이퍼쿼리를 생성하는 도중 오류가 발생했습니다."
            logging.exception(err_msg)
            raise HandleResultError(err_msg)

    
    # 역할: 스토어드 프로시저 코드를 분석하는 메서드를 호출하고, 결과를 이벤트 큐에 담아서 전송하고 응답을 받습니다.
    # 매개변수 :
    #   - node_end_line : 현재 분석중인 마지막 라인번호
    # 반환값 : 없음
    async def signal_for_process_analysis(node_end_line):
        try:
            # * 분석하는 메서드를 호출하고, 기다립니다. 만약 처리가 끝났다면, 분석 완료 이벤트를 송신합니다
            results = await process_analysis_results()
            logging.info("Event Send")
            logging.debug(f"Cypher queries: {results}")
            await send_queue.put({"type": "analysis_code", "query_data": results, "line_number": node_end_line})
            

            # * 분석 완료 이벤트를 송신하고, 처리 완료 이벤트를 수신 대기합니다 
            while True:
                response = await receive_queue.get()
                if response['type'] == 'process_completed':
                    logging.info("Processed Event Received")
                    cypher_query.clear();
                    break;
        
        except UnderstandingError:
            raise
        except Exception:
            err_msg = "Understanding 과정에서 이벤트를 송신하고 수신하는 도중 오류가 발생했습니다."
            logging.exception(err_msg)
            raise EventRsRqError(err_msg)


    # 역할: 변수 노드를 생성하기 위한 함수입니다.
    # 매개변수 : 
    #   - filtered_code : Declare 또는 sp_code의 변수 선언 부분만 필터링된 코드
    #   - startLine : 노드의 시작라인
    #   - statementType : 노드의 타입
    # 반환값 : 없음 
    def create_variable_node(filtered_code, startLine, statementType):
        try:
            # * 전달된 코드에서 공백으로 각 라인의 단어를 추출합니다.
            for line in filtered_code.split('\n'):
                parts = line.split()
                if len(parts) < 3: continue  
                var_name = parts[2]
                var_type = parts[4] if startLine == 1 and 'IN' in parts else parts[3]


                # * 추출된 변수 이름과 타입을 이용하여, 변수 노드를 생성하는 사이퍼쿼리를 작성합니다.
                variable_query = f"MERGE (v:Variable {{name: '{var_name}'}}) SET v.type = '{var_type}'"
                cypher_query.append(variable_query)

                # * 변수 노드의 관계(어디에 선언되었는지) 사이퍼쿼리를 작성합니다.
                variable_relationship_query = f"MERGE (n:{statementType} {{startLine: {startLine}}}) MERGE (v:Variable {{name: '{var_name}'}}) MERGE (n)-[:SCOPE]->(v)"
                cypher_query.append(variable_relationship_query)
        
        except Exception:
            err_msg = "Understanding 과정에서 변수 노드를 생성을 위한 사이퍼쿼리 생성 및 실행 도중 오류가 발생했습니다."
            logging.exception(err_msg)
            raise CreateNodeError(err_msg)


    # 역할 : 스토어드 프로시저 코드를 노드를 순회하면서 구조를 분석하는 함수
    # 매개변수: 
    #   node - 분석할 노드
    #   schedule_stack - 처리된 스케줄(노드)들의 스택
    #   send_queue - 이벤트 송신큐
    #   receive_queue - 이벤트 수신큐
    #   parent_alias - 부모 노드의 별칭 
    #   parent_startLine - 부모 노드 ID.
    #   parent_statementType - 부모 노드 타입
    # 반환값: 없음
    async def traverse(node, schedule_stack, send_queue, receive_queue, parent_alias=None, parent_startLine=None, parent_statementType=None):
        nonlocal focused_code, sp_token_count, clean_code, extract_code

        # * 분석에 필요한 필요한 정보를 준비하거나 할당합니다
        start_line, end_line, statement_type = node['startLine'], node['endLine'], node['type']
        summarized_code = extract_and_summarize_code(file_content, node)
        summarized_code = remove_unnecessary_information(summarized_code)
        node_code = extract_node_code(file_content, start_line, end_line)
        node_code = remove_unnecessary_information(node_code)
        node_size = count_tokens_in_text(node_code)
        children = node.get('children', [])
        node_alias = f"n{start_line}"
        current_schedule = {
            "startLine": start_line,
            "endLine": end_line,
            "code": summarized_code,
            "child": children,
            "type": statement_type
        }


        # * 변수 노드를 생성하기 위한 작업
        if statement_type in ["CREATE_PROCEDURE_BODY", "DECLARE"]:
            filtered_code = filter_procedure_declare_code(summarized_code)
            create_variable_node(filtered_code, start_line, statement_type)


        # * focused_code에서 분석할 범위를 기준으로 잘라내고, 불필요한 정보를 제거합니다
        extract_code, line_number = extract_code_within_range(focused_code, context_range)
        clean_code = remove_unnecessary_information(extract_code)


        # * 노드 크기 및 토큰 수 체크를 하여, 분석 여부를 결정합니다
        sp_token_count = count_tokens_in_text(clean_code)
        if (node_size >= 100 and context_range) or (sp_token_count >= 100 and context_range) or (len(context_range) > 12):
            await signal_for_process_analysis(line_number)


        # * focused_code가 없으면 새로 생성하고, 만약 있다면 확장합니다
        if not focused_code:
            focused_code = create_focused_code(current_schedule, schedule_stack) 
        else:
            placeholder = f"{node['startLine']}: ... code ..."
            focused_code = focused_code.replace(placeholder, summarized_code, 1)


        # * 노드의 사이퍼쿼리를 생성 및 해당 노드의 범위를 분석할 범위를 저장
        if not children and statement_type not in ["CREATE_PROCEDURE_BODY", "DECLARE", "ROOT"]:
            context_range.append({"startLine": start_line, "endLine": end_line})
            cypher_query.append(f"MERGE ({node_alias}:{statement_type}{{startLine: {start_line}}}) SET {node_alias}.endLine = {end_line}, {node_alias}.name = '{statement_type}[{start_line}]', {node_alias}.node_code = '{node_code.replace('\n', '\\n').replace("'", "\\'")}', {node_alias}.token = {node_size}")
        else:
            cypher_query.append(f"MERGE ({node_alias}:{statement_type}{{startLine: {start_line}}}) SET {node_alias}.endLine = {end_line}, {node_alias}.name = '{statement_type}[{start_line}]', {node_alias}.summarized_code = '{summarized_code.replace('\n', '\\n').replace("'", "\\'")}', {node_alias}.node_code = '{node_code.replace('\n', '\\n').replace("'", "\\'")}', {node_alias}.token = {node_size}")


        # * 스케줄 스택에 현재 스케줄을 넣고, 노드의 타입을 세트에 저장합니다
        schedule_stack.append(current_schedule)
        node_statementType.add(f"{statement_type}_{start_line}")


        # * 부모 변수가 있을 경우(부모가 존재할 경우), 부모 노드와 현재 노드의 parent_of 라는 관계를 생성합니다
        if parent_alias:
            cypher_query.append(f"MATCH ({parent_alias}:{parent_statementType} {{startLine: {parent_startLine}}}) WITH {parent_alias} MATCH ({node_alias}:{statement_type} {{startLine: {start_line}}}) MERGE ({parent_alias})-[:PARENT_OF]->({node_alias})")
        prev_alias = prev_id = None


        # * 현재 노드가 자식이 있는 경우, 해당 자식을 순회하면서 traverse함수를 (재귀적으로) 호출하고 처리합니다
        for child in children:
            await traverse(child, schedule_stack, send_queue, receive_queue, node_alias, node['startLine'], node['type'])
            
            # * 현재 노드와 이전의 같은 레벨의 노드간의 NEXT 관계를 위한 사이퍼쿼리를 생성합니다.
            if prev_alias:
                cypher_query.append(f"MATCH ({prev_alias} {{startLine: {prev_id}}}) WITH {prev_alias} MATCH (n{child['startLine']} {{startLine: {child['startLine']}}}) MERGE ({prev_alias})-[:NEXT]->(n{child['startLine']})")
            prev_alias, prev_id = f"n{child['startLine']}", child['startLine']



        # * 부모 노드가 가진 자식들이 모두 처리가 끝났다면, 부모 노드도 context_range에 포함합니다
        if children and statement_type not in ["CREATE_PROCEDURE_BODY", "DECLARE", "ROOT"]:
            context_range.append({"startLine": start_line, "endLine": end_line})
        

        # * 조건하에 필요없는 스케줄 스택을 제거합니다 
        schedule_stack[:] = filter(lambda schedule: schedule['child'] and schedule['endLine'] > current_schedule['startLine'], schedule_stack)
        

    try:
        # * traverse 함수를 호출하여, 스토어드 프로시저 분석을 위한 노드 순회를 시작합니다
        await traverse(antlr_data, schedule_stack, send_queue, receive_queue, None, None)

        # * 마지막 노드 그룹에 대한 처리를 합니다
        if context_range and focused_code is not None:
            extract_code, _ = extract_code_within_range(focused_code, context_range)
            clean_code = remove_unnecessary_information(extract_code)
            await signal_for_process_analysis(last_line)


        # * 뷴석이 끝났다는 의미를 가진 이벤트를 송신합니다.
        logging.info("LLM 호출 횟수 : " + str(LLM_count))
        await send_queue.put({"type": "end_analysis"})

    except UnderstandingError as e:
        await send_queue.put({'type': 'error', 'message': str(e)})
        raise
    except Exception:
        err_msg = "Understanding 과정에서 Traverse로 스토어드 프로시저 코드를 순회하는 도중 오류가 발생했습니다."
        logging.exception(err_msg)
        await send_queue.put({'type': 'error', 'message': err_msg})
        raise TraverseCodeError(err_msg)