import asyncio
from collections import defaultdict
import json
import logging
import re
from prompt.understand_summarized_prompt import understand_summary
import tiktoken
from prompt.understand_prompt import understand_code
from prompt.understand_variables_prompt import understand_variables
from util.exception import (LLMCallError, UnderstandingError, ProcessAnalyzeCodeError)
from util.utility_tool import calculate_code_token


encoder = tiktoken.get_encoding("cl100k_base")



# ==================== 섹션: 상수 정의 ====================
# 본 모듈 전반에서 사용하는 구문 타입/분석 제어용 상수를 정의합니다.
PROCEDURE_TYPES = ["PROCEDURE", "FUNCTION", "CREATE_PROCEDURE_BODY", "TRIGGER"]
NON_ANALYSIS_TYPES = ["CREATE_PROCEDURE_BODY", "ROOT", "PROCEDURE","FUNCTION", "DECLARE", "TRIGGER"]
NON_NEXT_RECURSIVE_TYPES = ["FUNCTION", "PROCEDURE", "PACKAGE_VARIABLE", "TRIGGER"]
NON_CHILD_ANALYSIS_TYPES = ["PACKAGE_VARIABLE","DECLARE", "SPEC"]



# ==================== 섹션: 유틸리티 헬퍼 ====================
# 공통적으로 사용하는 문자열 처리, 범위 추출, 토큰 기준 판단 등의 헬퍼입니다.
def get_statement_type(start_line: int, end_line: int, node_statement_types: set[str]) -> str | None:
    """역할:
    - 저장된 `"TYPE_start_end"` 표기 집합에서 `(start_line, end_line)`과 일치하는 항목을 찾아 TYPE을 반환합니다.

    매개변수:
    - start_line (int): 구문 노드의 시작 라인 번호.
    - end_line (int): 구문 노드의 종료 라인 번호.
    - node_statement_types (set[str]): `"TYPE_start_end"` 형식의 식별자 문자열 집합.

    반환값:
    - Optional[str]: 매칭되는 TYPE 문자열. 매칭이 없으면 None.
    """
    entry = next((e for e in node_statement_types if e.endswith(f"_{start_line}_{end_line}")), None)
    return entry.rsplit('_', 2)[0] if entry else None


def get_table_relationship(statement_type: str | None) -> str | None:
    """역할:
    - 구문 타입을 테이블 관계 라벨로 매핑합니다. SELECT→FROM, DML→WRITES, EXECUTE_IMMEDIATE→EXECUTE.

    매개변수:
    - statement_type (Optional[str]): 구문 타입 라벨.

    반환값:
    - Optional[str]: 테이블 관계 라벨(FROM/WRITES/EXECUTE). 매핑되지 않으면 None.
    """
    if statement_type == "SELECT":
        return "FROM"
    if statement_type in ["UPDATE", "INSERT", "DELETE", "MERGE", "FETCH"]:
        return "WRITES"
    if statement_type == "EXECUTE_IMMEDIATE":
        return "EXECUTE"
    return None


def is_over_token_limit(node_token: int, sp_token: int, context_len: int) -> bool:
    """역할:
    - 토큰 임계치(개별 노드/누적/범위 개수) 도달 여부를 판단해 배치 플러시 필요성을 결정합니다.

    매개변수:
    - node_token (int): 현재 노드 코드의 토큰 수.
    - sp_token (int): 누적된 스토어드 프로시저 컨텍스트 토큰 수.
    - context_len (int): 누적된 분석 범위(context_range) 구간 수.

    반환값:
    - bool: 임계치 도달 시 True, 아니면 False.
    """
    return (
        (node_token >= 1000 and context_len and node_token + sp_token >= 1000)
        or (sp_token >= 1000 and context_len)
        or (context_len >= 10 and sp_token >= 500)
    )


def escape_for_cypher_multiline(text: str) -> str:
    """역할:
    - Cypher 쿼리에 안전하게 포함되도록 개행과 작은따옴표를 이스케이프합니다.

    매개변수:
    - text (str): 원본 문자열.

    반환값:
    - str: 이스케이프 처리된 문자열.
    """
    return text.replace('\n', '\\n').replace("'", "\\'")
    

def extract_code_within_range(code: str, context_range: list[dict]) -> tuple[str, int]:
    """역할:
    - 라인 번호 접두가 포함된 `code`에서 `context_range`의 최소 시작~최대 종료 라인 사이만 발췌합니다.

    매개변수:
    - code (str): 라인 번호 접두가 포함된 누적 코드 문자열.
    - context_range (list[dict]): `{"startLine": int, "endLine": int}` 형태의 구간 리스트.

    반환값:
    - tuple[str, int]: (발췌된 코드 문자열, 최종 end_line).
    """
    try:
        if not (code and context_range):
            return "", ""

        start_line = min(range_item['startLine'] for range_item in context_range)
        end_line = max(range_item['endLine'] for range_item in context_range)
        code_lines = code.split('\n')
        line_number_pattern = r'^(\d+)(?:~\d+)?:\s'
        
        extracted_lines = []
        for line in code_lines:
            match = re.match(line_number_pattern, line)
            if match:
                line_number = int(match.group(1))
                if start_line <= line_number <= end_line:
                    extracted_lines.append(line)

        extracted_code = '\n'.join(extracted_lines)
        return extracted_code, end_line
    
    except Exception as e:
        err_msg = f"Understanding 과정에서 범위내에 코드 추출 도중에 오류가 발생했습니다: {str(e)}"
        logging.error(err_msg)
        raise ProcessAnalyzeCodeError(err_msg)


def get_procedure_name(code: str) -> str:
    """역할:
    - PL/SQL 선언부(또는 CREATE 구문)에서 PROCEDURE/FUNCTION/TRIGGER의 이름을 정규식으로 추출합니다.

    매개변수:
    - code (str): 라인 번호 접두가 포함될 수 있는 원본 코드 조각.

    반환값:
    - Optional[str]: 발견된 객체명. 없으면 None.
    """
    try:
        normalized = re.sub(r'^\d+\s*:\s*', '', code, flags=re.MULTILINE)

        pattern = re.compile(
            r"\b(?:CREATE\s+(?:OR\s+REPLACE\s+)?)?"
            r"(?:PROCEDURE|FUNCTION|TRIGGER)\s+"
            r"((?:\"[^\"]+\"|[A-Za-z_][\w$#]*)\s*\.\s*)?" 
            r"(\"[^\"]+\"|[A-Za-z_][\w$#]*)",
            re.IGNORECASE
        )

        match = pattern.search(normalized)
        if not match:
            fallback = re.compile(
                r"\b(?:PROCEDURE|FUNCTION|TRIGGER)\s+"
                r"((?:\"[^\"]+\"|[A-Za-z_][\w$#]*)\s*\.\s*)?"
                r"(\"[^\"]+\"|[A-Za-z_][\w$#]*)",
                re.IGNORECASE
            )
            match = fallback.search(normalized)

        if match:
            raw_name = match.group(2)
            return raw_name[1:-1] if raw_name.startswith('"') and raw_name.endswith('"') else raw_name

        return None
    except Exception as e:
        logging.error(f"프로시저/함수/트리거 명 추출 중 오류: {str(e)}")
        return None


def summarize_with_placeholders(file_content: str, node: dict) -> str:
    """역할:
    - 노드 범위의 코드를 가져오되 자식 범위는 `"start: ... code ..."` 플레이스홀더로 치환합니다.
    - 라인 번호 접두를 유지하여 추후 라인 매핑을 가능케 합니다.

    매개변수:
    - file_content (str): 전체 파일 내용.
    - node (dict): `{"startLine": int, "endLine": int, "children": list[dict]}` 형태의 노드.

    반환값:
    - str: 요약된 코드 문자열.
    """

    def summarize_code(start_line, end_line, children):

        lines = file_content.split('\n')  
        code_lines = lines[start_line-1:end_line]
        summarized_code = []
        last_end_line = start_line - 1
        line_number_pattern = r'^\d+\s*:'

        for child in children:
            before_child_code = code_lines[last_end_line-start_line+1:child['startLine']-start_line]
            
            for i, line in enumerate(before_child_code):
                line_number = i + last_end_line + 1
                if re.match(line_number_pattern, line):
                    summarized_code.append(f"{line}\n")
                else:
                    summarized_code.append(f"{line_number}: {line}\n")
            
            summarized_code.append(f"{child['startLine']}: ... code ...\n")
            last_end_line = child['endLine']

        after_last_child_code = code_lines[last_end_line-start_line+1:]
        
        for i, line in enumerate(after_last_child_code):
            line_number = i + last_end_line + 1
            if re.match(line_number_pattern, line):
                summarized_code.append(f"{line}\n")
            else:
                summarized_code.append(f"{line_number}: {line}\n")
        
        return ''.join(summarized_code)
    

    try:
        if not node.get('children'):
            lines = file_content.split('\n')  
            code_lines = lines[node['startLine']-1:node['endLine']] 
            line_number_pattern = r'^\d+\s*:'

            result = []
            for i, line in enumerate(code_lines):
                line_number = i + node['startLine']
                if re.match(line_number_pattern, line):
                    result.append(f"{line}\n")
                else:
                    result.append(f"{line_number}: {line}\n")
            return ''.join(result)
        else:
            return summarize_code(node['startLine'], node['endLine'], node.get('children', []))
    
    except Exception as e:
        err_msg = f"Understanding 과정에서 코드를 요약하는 도중 오류가 발생했습니다: {str(e)}"
        logging.error(err_msg)
        raise ProcessAnalyzeCodeError(err_msg)


def build_sp_code(current_schedule: dict, schedule_stack: list) -> str:
    """역할:
    - 현재 스케줄에서 시작하여 상위 스케줄을 역순 적용, ...code... 플레이스홀더를 실제 요약 코드로 치환하여, 실제 분석할 sp 코드를 생성합니다

    매개변수:
    - current_schedule (dict): 현재 노드의 요약 스케줄.
    - schedule_stack (list[dict]): 상위 노드들의 요약 스케줄 스택.

    반환값:
    - str: 분석할 sp 코드.
    """
    try:
        focused_code = current_schedule["code"]
        current_start_line = current_schedule["startLine"]
        for schedule in reversed(schedule_stack):
            placeholder = f"{current_start_line}: ... code ..."
            if placeholder in schedule["code"]:
                focused_code = schedule["code"].replace(placeholder, focused_code, 1)
                current_start_line = schedule["startLine"]

        return focused_code

    except Exception as e:
        err_msg = f"Understanding 과정에서 분석할 코드 생성 도중에 오류가 발생했습니다: {str(e)}"
        logging.error(err_msg)
        raise ProcessAnalyzeCodeError(err_msg)
    

def get_original_node_code(file_content: str, start_line: int, end_line: int) -> str:
    """역할:
    - 지정 라인 범위를 추출하고 각 라인에 라인 번호 접두(`"N: "`)가 존재하도록 보장합니다.

    매개변수:
    - file_content (str): 전체 파일 내용.
    - start_line (int): 추출 시작 라인.
    - end_line (int): 추출 종료 라인(0이면 시작 라인과 동일 처리).

    반환값:
    - str: 라인 번호 접두가 보장된 텍스트.
    """
    try:
        if end_line == 0:
            end_line = start_line
        lines = file_content.split('\n')
        extracted_lines = lines[start_line-1:end_line]
        line_number_pattern = r'^\d+\s*:'
        extracted_node_code = []
        for i, line in enumerate(extracted_lines):
            if re.match(line_number_pattern, line):
                extracted_node_code.append(line)
            else:
                extracted_node_code.append(f"{i + start_line}: {line}")
        
        return '\n'.join(extracted_node_code)
    
    except Exception as e:
        err_msg = f"Understanding 과정에서 노드에 맞게 코드를 추출 도중에 오류가 발생했습니다: {str(e)}"
        logging.error(err_msg)
        raise ProcessAnalyzeCodeError(err_msg)


def clean_field_name(field_name: str) -> str:
    """역할:
    - `"{TYPE:NAME}"` 또는 유사 표기에서 NAME만 추출합니다.

    매개변수:
    - field_name (str): 원본 필드 표기 문자열.

    반환값:
    - str: 추출된 이름 또는 원본 문자열.
    """
    match = re.search(r'\{(.+?)\}', field_name)
    if match:
        return match.group(1)
    return field_name


# ==================== 섹션: 분석 Understanding 파이프라인(엔트리 포인트) ====================
class Analyzer:
    """분석 파이프라인을 담당하는 상태 보유형 클래스.

    역할:
    - ANTLR AST를 DFS로 순회하며 요약 코드 누적, 배치 플러시, LLM 분석, Neo4j 사이퍼 생성까지 전체 흐름을 관리합니다.
    - 중첩 함수와 nonlocal 공유 상태를 제거하여 가독성과 유지보수성을 향상합니다.

    보장 사항:
    - 기존 기능/사이드이펙트(큐 프로토콜, 사이퍼 쿼리, 토큰 임계치, 요약/관계 생성)와 완전 동일하게 동작합니다.
    """

    def __init__(self, antlr_data: dict, file_content: str, send_queue: asyncio.Queue, receive_queue: asyncio.Queue, last_line: int, object_name: str, ddl_tables: dict, has_ddl_info: bool, user_id: str, api_key: str, locale: str):
        """생성자

        매개변수:
        - antlr_data: ANTLR 파서가 생성한 AST 루트 노드
        - file_content: 라인 번호 접두가 포함된 원본 코드 텍스트
        - send_queue: 생성된 사이퍼 쿼리 배치를 송신하는 큐
        - receive_queue: 사이퍼 처리 완료 신호를 수신하는 큐
        - last_line: 파일 마지막 라인 번호(잔여 배치 플러시용)
        - object_name: 패키지/오브젝트 이름(Neo4j 키)
        - ddl_tables: DDL로부터 파싱된 테이블/필드 메타
        - has_ddl_info: DDL 정보 보유 여부
        - user_id: 사용자 식별자(Neo4j 파티셔닝)
        - api_key: LLM 호출에 사용할 API 키
        - locale: 로케일 코드('ko'|'en')
        """
        self.antlr_data = antlr_data
        self.file_content = file_content
        self.send_queue = send_queue
        self.receive_queue = receive_queue
        self.last_line = last_line
        self.object_name = object_name
        self.ddl_tables = ddl_tables
        self.has_ddl_info = has_ddl_info
        self.user_id = user_id
        self.api_key = api_key
        self.locale = locale

        self.schedule_stack = []
        self.context_range = []
        self.cypher_query = []
        self.summary_dict = {}
        self.node_statement_types = set()
        self.procedure_name = None
        self.extract_code = ""
        self.focused_code = ""
        self.sp_token_count = 0


    async def run(self):
        """전체 분석 파이프라인 실행.

        - DFS 순회 시작→잔여 배치 플러시→완료 이벤트 송신까지 담당합니다.
        - 오류 발생 시 큐로 에러 이벤트를 전송하고 예외를 전파합니다.
        """
        logging.info(f"[{self.object_name}] 사이퍼 쿼리 생성 시작 (last_line={self.last_line})")
        try:
            await self.analyze_statement_tree(self.antlr_data, self.schedule_stack)

            if self.context_range and self.focused_code:
                self.extract_code, _ = extract_code_within_range(self.focused_code, self.context_range)
                await self.send_analysis_event_and_wait(self.last_line)
            logging.info(f"[{self.object_name}] 전체 분석 완료")
            await self.send_queue.put({"type": "end_analysis"})

        except UnderstandingError as e:
            await self.send_queue.put({'type': 'error', 'message': str(e)})
            raise
        except Exception as e:
            err_msg = f"Understanding 과정에서 Traverse로 스토어드 프로시저 코드를 순회하는 도중 오류가 발생했습니다: {str(e)}"
            logging.error(err_msg)
            await self.send_queue.put({'type': 'error', 'message': err_msg})
            raise ProcessAnalyzeCodeError(err_msg)


    async def execute_analysis_and_reset_state(self, statement_type: str) -> list:
        """누적 컨텍스트를 LLM에 전달해 실제 분석을 실행하고, 내부 상태를 초기화합니다.

        매개변수:
        - statement_type: 플러시 기준 상위 구문 타입(PROCEDURE/FUNCTION 등)

        반환값:
        - list[str]: 생성된 사이퍼 쿼리 문자열 리스트
        """
        try:
            context_range_count = len(self.context_range)
            self.context_range = sorted(self.context_range, key=lambda x: x['startLine'])

            analysis_result = understand_code(self.extract_code, self.context_range, context_range_count, self.procedure_name, self.api_key, self.locale)
            cypher_queries = await self.process_analysis_output_to_cypher(analysis_result)

            actual_count = len(analysis_result["analysis"])
            if actual_count != context_range_count:
                logging.error(f"분석 결과 개수가 일치하지 않습니다. 예상: {context_range_count}, 실제: {actual_count}")

            if statement_type in PROCEDURE_TYPES:
                logging.info(f"[{self.object_name}] {self.procedure_name} 프로시저의 요약 정보 추출 완료")
                summary = understand_summary(self.summary_dict, self.api_key, self.locale)
                self.cypher_query.append(f"""
                    MATCH (n:{statement_type})
                    WHERE n.object_name = '{self.object_name}'
                        AND n.procedure_name = '{self.procedure_name}'
                        AND n.user_id = '{self.user_id}'
                    SET n.summary = {json.dumps(summary['summary'])}
                """)
                self.schedule_stack.clear()
                self.node_statement_types.clear()
                self.summary_dict.clear()

            self.focused_code = ""
            self.extract_code = ""
            self.sp_token_count = 0
            self.context_range.clear()
            return cypher_queries

        except UnderstandingError:
            raise
        except Exception as e:
            err_msg = f"Understanding 과정에서 LLM의 결과 처리를 준비 및 시작하는 도중 문제가 발생했습니다: {str(e)}"
            logging.error(err_msg)
            raise ProcessAnalyzeCodeError(err_msg)


    async def process_analysis_output_to_cypher(self, analysis_result: dict) -> list:
        """LLM 분석 결과를 처리하여 Neo4j 사이퍼 쿼리를 생성합니다.

        매개변수:
        - analysis_result: LLM 분석 결과(JSON 호환 dict)

        반환값:
        - list[str]: 현재까지 누적된 사이퍼 쿼리 리스트
        """
        table_fields = defaultdict(set)
        try:
            table_references = [] if self.has_ddl_info else analysis_result.get('tableReference', [])
            tables = {} if self.has_ddl_info else analysis_result.get('Tables', {})

            for table, fields in tables.items():
                qualified = table.strip().upper()
                name_part = qualified.split('.')[-1]
                schema_part = qualified.split('.')[0] if '.' in qualified else None
                table_fields[name_part].update(fields)
                if fields and '*' not in fields:
                    for field in fields:
                        field_name = clean_field_name(field.split(':')[1])
                        field_type = field.split(':')[0]
                        match_clause = (
                            f"MATCH (t:Table {{name: '{name_part}', schema: '{schema_part}', user_id: '{self.user_id}'}})"
                            if schema_part else
                            f"MATCH (t:Table {{name: '{name_part}', user_id: '{self.user_id}'}})"
                        )
                        update_query = f"""
                            {match_clause}
                            WITH t
                            WHERE t.{field_name} IS NULL
                            SET t.{field_name} = '{field_type}'
                        """
                        self.cypher_query.append(update_query)

            for reference in table_references:
                src_qualified = reference['source'].strip().upper()
                tgt_qualified = reference['target'].strip().upper()
                source_table = src_qualified.split('.')[-1]
                target_table = tgt_qualified.split('.')[-1]
                source_schema = src_qualified.split('.')[0] if '.' in src_qualified else None
                target_schema = tgt_qualified.split('.')[0] if '.' in tgt_qualified else None

                if source_table != target_table:
                    match_source = (
                        f"MATCH (source:Table {{name: '{source_table}', schema: '{source_schema}', user_id: '{self.user_id}'}})"
                        if source_schema else
                        f"MATCH (source:Table {{name: '{source_table}', user_id: '{self.user_id}'}})"
                    )
                    match_target = (
                        f"MATCH (target:Table {{name: '{target_table}', schema: '{target_schema}', user_id: '{self.user_id}'}})"
                        if target_schema else
                        f"MATCH (target:Table {{name: '{target_table}', user_id: '{self.user_id}'}})"
                    )
                    table_reference_query = f"""
                    {match_source}
                    WITH source
                    {match_target}
                    MERGE (source)-[:REFERENCES]->(target)
                    """
                    self.cypher_query.append(table_reference_query)

            for result in analysis_result['analysis']:
                start_line = result['startLine']
                end_line = result['endLine']
                summary = result['summary']
                tableName = result.get('tableNames', [])
                called_nodes = result.get('calls', [])
                variables = result.get('variables', [])
                var_range = f"{start_line}_{end_line}"

                statement_type = get_statement_type(start_line, end_line, self.node_statement_types)
                table_relationship_type = get_table_relationship(statement_type)

                summary_key = f"{statement_type}_{start_line}_{end_line}"
                self.summary_dict[summary_key] = summary

                summary_query = f"""
                    MATCH (n:{statement_type} {{startLine: {start_line}, object_name: '{self.object_name}', user_id: '{self.user_id}'}})
                    SET n.summary = {json.dumps(summary)}
                """
                self.cypher_query.append(summary_query)

                for schedule in self.schedule_stack:
                    pattern = re.compile(rf"^{start_line}: \.\.\. code \.\.\.$", re.MULTILINE)
                    if pattern.search(schedule["code"]):
                        schedule["code"] = pattern.sub(f"{start_line}~{end_line}: {summary}", schedule["code"])
                        break

                for var_name in variables:
                    variable_usage_query = f"""
                        MATCH (v:Variable {{name: '{var_name}', object_name: '{self.object_name}', procedure_name: '{self.procedure_name}', user_id: '{self.user_id}'}})
                        SET v.`{var_range}` = 'Used'
                    """
                    self.cypher_query.append(variable_usage_query)

                if statement_type in ["CALL", "ASSIGNMENT"]:
                    if statement_type == "ASSIGNMENT" and called_nodes:
                        label_change_query = f"""
                            MATCH (a:ASSIGNMENT {{startLine: {start_line}, object_name: '{self.object_name}', user_id: '{self.user_id}'}})
                            REMOVE a:ASSIGNMENT
                            SET a:CALL, a.name = 'CALL[{start_line}]'
                        """
                        self.cypher_query.append(label_change_query)
                        statement_type = "CALL"

                    if called_nodes:
                        for name in called_nodes:
                            if '.' in name:
                                package_name, proc_name = name.split('.')
                                package_name = package_name.upper()
                                proc_name = proc_name.upper()

                                call_relation_query = f"""
                                    MATCH (c:{statement_type} {{startLine: {start_line}, object_name: '{self.object_name}', user_id: '{self.user_id}'}}) 
                                    OPTIONAL MATCH (p)
                                    WHERE (p:PROCEDURE OR p:FUNCTION)
                                    AND p.object_name = '{package_name}' 
                                    AND p.procedure_name = '{proc_name}'
                                    AND p.user_id = '{self.user_id}'
                                    WITH c, p
                                    FOREACH(ignoreMe IN CASE WHEN p IS NULL THEN [1] ELSE [] END |
                                        CREATE (new:PROCEDURE:FUNCTION {{object_name: '{package_name}', procedure_name: '{proc_name}', user_id: '{self.user_id}'}})
                                        MERGE (c)-[:CALL {{scope: 'external'}}]->(new)
                                    )
                                    FOREACH(ignoreMe IN CASE WHEN p IS NOT NULL THEN [1] ELSE [] END |
                                        MERGE (c)-[:CALL {{scope: 'external'}}]->(p)
                                    )
                                """
                                self.cypher_query.append(call_relation_query)
                            else:
                                call_relation_query = f"""
                                    MATCH (c:{statement_type} {{startLine: {start_line}, object_name: '{self.object_name}', user_id: '{self.user_id}'}})
                                    WITH c
                                    MATCH (p {{object_name: '{self.object_name}', procedure_name: '{name}', user_id: '{self.user_id}'}} )
                                    WHERE p:PROCEDURE OR p:FUNCTION
                                    MERGE (c)-[:CALL {{scope: 'internal'}}]->(p)
                                """
                                self.cypher_query.append(call_relation_query)

                if table_relationship_type and tableName:
                    qualified = tableName[0].strip().upper()
                    name_part = qualified.split('.')[-1]
                    schema_part = qualified.split('.')[0] if '.' in qualified else None
                    match_table = (
                        f"MATCH (t:Table {{name: '{name_part}', schema: '{schema_part}', user_id: '{self.user_id}'}})"
                        if schema_part else
                        f"MATCH (t:Table {{name: '{name_part}', user_id: '{self.user_id}'}})"
                    )
                    table_relationship_query = f"""
                        MERGE (n:{statement_type} {{startLine: {start_line}, object_name: '{self.object_name}', user_id: '{self.user_id}'}})
                        WITH n
                        {match_table}
                        MERGE (n)-[:{table_relationship_type}]->(t)
                    """
                    self.cypher_query.append(table_relationship_query)

            return self.cypher_query

        except Exception as e:
            err_msg = f"Understanding 과정에서 LLM의 결과를 이용해 사이퍼쿼리를 생성하는 도중 오류가 발생했습니다: {str(e)}"
            logging.error(err_msg)
            raise ProcessAnalyzeCodeError(err_msg)


    def analyze_variable_declarations(self, declaration_code: str, node_startLine: int, statement_type: str):
        """변수 선언부(SPEC/DECLARE/PACKAGE_VARIABLE)를 분석하고 Variable 노드/Scope 관계를 생성합니다.

        매개변수:
        - declaration_code: 선언부 코드 조각
        - node_startLine: 선언 노드 시작 라인
        - statement_type: 선언 유형(SPEC/DECLARE/PACKAGE_VARIABLE)
        """
        try:
            role = ('패키지 전역 변수' if statement_type == 'PACKAGE_VARIABLE' else
                    '변수 선언및 초기화' if statement_type == 'DECLARE' else
                    '함수 및 프로시저 입력 매개변수' if statement_type == 'SPEC' else
                    '알 수 없는 매개변수')
            analysis_result = understand_variables(declaration_code, self.ddl_tables, self.api_key, self.locale)
            logging.info(f"[{self.object_name}] {self.procedure_name}의 변수 분석 완료")
            var_summary = json.dumps(analysis_result.get("summary", "unknown"))
            for variable in analysis_result["variables"]:
                var_parameter_type = variable["parameter_type"]
                var_name = variable["name"]
                var_type = variable["type"]
                var_value = variable["value"]
                var_value = '' if var_value is None else var_value

                if statement_type == 'DECLARE':
                    self.cypher_query.extend([
                        f"MERGE (v:Variable {{name: '{var_name}', object_name: '{self.object_name}', type: '{var_type}', procedure_name: '{self.procedure_name}', role: '{role}', scope: 'Local', value: {json.dumps(var_value)}, user_id: '{self.user_id}'}}) ",
                        f"MATCH (p:{statement_type} {{startLine: {node_startLine}, object_name: '{self.object_name}', procedure_name: '{self.procedure_name}', user_id: '{self.user_id}'}}) ",
                        f"SET p.summary = {var_summary}",
                        f"WITH p ",
                        f"MATCH (v:Variable {{name: '{var_name}', object_name: '{self.object_name}', procedure_name: '{self.procedure_name}', user_id: '{self.user_id}'}})",
                        f"MERGE (p)-[:SCOPE]->(v)"
                    ])
                elif statement_type == 'PACKAGE_VARIABLE':
                    self.cypher_query.extend([
                        f"MERGE (v:Variable {{name: '{var_name}', object_name: '{self.object_name}', type: '{var_type}', role: '{role}', scope: 'Global', value: {json.dumps(var_value)}, user_id: '{self.user_id}'}}) ",
                        f"MATCH (p:{statement_type} {{startLine: {node_startLine}, object_name: '{self.object_name}', user_id: '{self.user_id}'}}) ",
                        f"SET p.summary = {var_summary}",
                        f"WITH p ",
                        f"MATCH (v:Variable {{name: '{var_name}', object_name: '{self.object_name}', scope: 'Global', user_id: '{self.user_id}'}})",
                        f"MERGE (p)-[:SCOPE]->(v)"
                    ])
                else:
                    self.cypher_query.extend([
                        f"MERGE (v:Variable {{name: '{var_name}', object_name: '{self.object_name}', type: '{var_type}', parameter_type: '{var_parameter_type}', procedure_name: '{self.procedure_name}', role: '{role}', scope: 'Local', value: {json.dumps(var_value)}, user_id: '{self.user_id}'}}) ",
                        f"MATCH (p:{statement_type} {{startLine: {node_startLine}, object_name: '{self.object_name}', procedure_name: '{self.procedure_name}', user_id: '{self.user_id}'}}) ",
                        f"SET p.summary = {var_summary}",
                        f"WITH p ",
                        f"MATCH (v:Variable {{name: '{var_name}', object_name: '{self.object_name}', procedure_name: '{self.procedure_name}', user_id: '{self.user_id}'}})",
                        f"MERGE (p)-[:SCOPE]->(v)"
                    ])

        except LLMCallError:
            raise
        except Exception as e:
            err_msg = f"Understanding 과정에서 프로시저 선언부 분석 및 변수 노드 생성 중 오류가 발생했습니다: {str(e)}"
            logging.error(err_msg)
            raise ProcessAnalyzeCodeError(err_msg)


    async def send_analysis_event_and_wait(self, node_end_line: int, statement_type: str = None):
        """분석 결과 이벤트를 송신하고 처리 완료 이벤트를 수신할 때까지 대기합니다.

        매개변수:
        - node_end_line: 해당 배치의 기준이 되는 마지막 라인 번호
        - statement_type: 플러시 기준 상위 구문 타입
        """
        try:
            results = await self.execute_analysis_and_reset_state(statement_type)
            logging.info(f"[{self.object_name}] {self.procedure_name} 프로시저 분석 결과 이벤트 송신")
            await self.send_queue.put({"type": "analysis_code", "query_data": results, "line_number": node_end_line})

            while True:
                response = await self.receive_queue.get()
                if response['type'] == 'process_completed':
                    logging.info(f"[{self.object_name}] {self.procedure_name} 프로시저 분석 결과 처리 완료\n")
                    self.cypher_query.clear();
                    break;

        except UnderstandingError:
            raise
        except Exception as e:
            err_msg = f"Understanding 과정에서 이벤트를 송신하고 수신하는 도중 오류가 발생했습니다: {str(e)}"
            logging.error(err_msg)
            raise ProcessAnalyzeCodeError(err_msg)


    async def analyze_statement_tree(self, node: dict, schedule_stack: list, parent_startLine: int = None, parent_statementType: str = None):
        """문(statement) 트리를 분석하며 노드/관계 생성, 요약 조립, 배치 플러시를 수행합니다.

        매개변수:
        - node: 현재 방문할 노드
        - schedule_stack: 상위 노드들의 요약 스케줄 스택
        - parent_startLine: 부모 노드 시작 라인
        - parent_statementType: 부모 노드 타입
        """
        start_line, end_line, statement_type = node['startLine'], node['endLine'], node['type']
        summarized_code = summarize_with_placeholders(self.file_content, node)
        node_code = get_original_node_code(self.file_content, start_line, end_line)
        node_size = calculate_code_token(node_code)
        children = node.get('children', [])

        current_schedule = {
            "startLine": start_line,
            "endLine": end_line,
            "code": summarized_code,
            "child": children,
            "type": statement_type
        }

        if statement_type in PROCEDURE_TYPES:
            self.procedure_name = get_procedure_name(node_code)
            logging.info(f"[{self.object_name}] {self.procedure_name} 프로시저 분석 시작")

        self.extract_code, line_number = extract_code_within_range(self.focused_code, self.context_range)

        self.sp_token_count = calculate_code_token(self.extract_code)
        if is_over_token_limit(node_size, self.sp_token_count, len(self.context_range)):
            await self.send_analysis_event_and_wait(line_number)

        if not self.focused_code:
            self.focused_code = build_sp_code(current_schedule, schedule_stack)
        else:
            placeholder = f"{start_line}: ... code ..."
            self.focused_code = self.focused_code.replace(placeholder, summarized_code, 1)

        if not children and statement_type not in NON_CHILD_ANALYSIS_TYPES:
            self.context_range.append({"startLine": start_line, "endLine": end_line})
            self.cypher_query.append(f"""
                MERGE (n:{statement_type} {{startLine: {start_line}, object_name: '{self.object_name}', user_id: '{self.user_id}'}})
                SET n.endLine = {end_line},
                    n.name = '{statement_type}[{start_line}]',
                    n.node_code = '{node_code.replace("'", "\\'")}',
                    n.token = {node_size},
                    n.procedure_name = '{self.procedure_name}'
            """)
        else:
            if statement_type == "ROOT":
                root_summary = 'Root Start Node' if self.locale == 'en' else '최상위 시작노드'
                self.cypher_query.append(f"""
                    MERGE (n:{statement_type} {{startLine: {start_line}, object_name: '{self.object_name}', user_id: '{self.user_id}'}})
                    SET n.endLine = {end_line},
                        n.name = '{self.object_name}',
                        n.summary = '{root_summary}'
                """)
            elif statement_type in ["PROCEDURE", "FUNCTION"]:
                self.cypher_query.append(f"""
                    MERGE (n:{statement_type} {{procedure_name: '{self.procedure_name}', object_name: '{self.object_name}', user_id: '{self.user_id}'}})
                    SET n.startLine = {start_line},
                        n.endLine = {end_line},
                        n.name = '{statement_type}[{start_line}]',
                        n.summarized_code = '{escape_for_cypher_multiline(summarized_code)}',
                        n.node_code = '{escape_for_cypher_multiline(node_code)}',
                        n.token = {node_size}
                    WITH n
                    REMOVE n:{('FUNCTION' if statement_type == 'PROCEDURE' else 'PROCEDURE')}
                """)
            else:
                self.cypher_query.append(f"""
                    MERGE (n:{statement_type} {{startLine: {start_line}, object_name: '{self.object_name}', user_id: '{self.user_id}'}})
                    SET n.endLine = {end_line},
                        n.name = '{statement_type}[{start_line}]',
                        n.summarized_code = '{escape_for_cypher_multiline(summarized_code)}',
                        n.node_code = '{escape_for_cypher_multiline(node_code)}',
                        n.token = {node_size},
                        n.procedure_name = '{self.procedure_name}'
                """)

        if (self.procedure_name and statement_type in ["SPEC", "DECLARE"]) or statement_type == "PACKAGE_VARIABLE":
            self.analyze_variable_declarations(node_code, start_line, statement_type)

        schedule_stack.append(current_schedule)
        self.node_statement_types.add(f"{statement_type}_{start_line}_{end_line}")

        if parent_statementType:
            self.cypher_query.append(f"""
                MATCH (parent:{parent_statementType} {{startLine: {parent_startLine}, object_name: '{self.object_name}', user_id: '{self.user_id}'}})
                WITH parent
                MATCH (child:{statement_type} {{startLine: {start_line}, object_name: '{self.object_name}', user_id: '{self.user_id}'}})
                MERGE (parent)-[:PARENT_OF]->(child)
            """)
        prev_statement = prev_id = None

        for child in children:
            await self.analyze_statement_tree(child, schedule_stack, start_line, statement_type)

            if prev_id and prev_statement not in NON_NEXT_RECURSIVE_TYPES:
                self.cypher_query.append(f"""
                    MATCH (prev:{prev_statement} {{startLine: {prev_id}, object_name: '{self.object_name}', user_id: '{self.user_id}'}})
                    WITH prev
                    MATCH (current:{child['type']} {{startLine: {child['startLine']}, object_name: '{self.object_name}', user_id: '{self.user_id}'}})
                    MERGE (prev)-[:NEXT]->(current)
                """)
            prev_statement, prev_id = child['type'], child['startLine']

        if children:
            if (statement_type in PROCEDURE_TYPES) and (self.context_range and self.focused_code):
                self.extract_code, line_number = extract_code_within_range(self.focused_code, self.context_range)
                logging.info(f"[{self.object_name}] {self.procedure_name} 프로시저 끝 분석 시작")
                await self.send_analysis_event_and_wait(line_number, statement_type)
            elif statement_type not in NON_ANALYSIS_TYPES:
                self.context_range.append({"startLine": start_line, "endLine": end_line})

        schedule_stack[:] = filter(lambda schedule: schedule['child'] and schedule['endLine'] > current_schedule['startLine'], schedule_stack)

