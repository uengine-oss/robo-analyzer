import asyncio
import json
import logging
import re
from prompt.understand_summarized_prompt import understand_summary
from prompt.understand_prompt import understand_code
from prompt.understand_variables_prompt import understand_variables
from util.exception import (LLMCallError, UnderstandingError, ProcessAnalyzeCodeError)
from util.utility_tool import calculate_code_token, parse_table_identifier


# ==================== 섹션: 상수 정의 ====================
PROCEDURE_TYPES = ("PROCEDURE", "FUNCTION", "CREATE_PROCEDURE_BODY", "TRIGGER")
NON_ANALYSIS_TYPES = frozenset(["CREATE_PROCEDURE_BODY", "FILE", "PROCEDURE", "FUNCTION", "DECLARE", "TRIGGER", "FOLDER", "SPEC"])
NON_NEXT_RECURSIVE_TYPES = frozenset(["FUNCTION", "PROCEDURE", "PACKAGE_VARIABLE", "TRIGGER"])
LINE_NUMBER_PATTERN = re.compile(r'^\d+\s*:')
PROCEDURE_PATTERN = re.compile(
    r"\b(?:CREATE\s+(?:OR\s+REPLACE\s+)?)?"
    r"(?:PROCEDURE|FUNCTION|TRIGGER)\s+"
    r"((?:\"[^\"]+\"|[A-Za-z_][\w$#]*)"
    r"(?:\s*\.\s*(?:\"[^\"]+\"|[A-Za-z_][\w$#]*)){0,2})",
    re.IGNORECASE
)
_TABLE_RELATIONSHIP_MAP = {
    "SELECT": "FROM", "FETCH": "FROM",
    "UPDATE": "WRITES", "INSERT": "WRITES", "DELETE": "WRITES", "MERGE": "WRITES",
    "EXECUTE_IMMEDIATE": "EXECUTE", "ASSIGNMENT": "EXECUTE", "CALL": "EXECUTE"
}
_LINE_NUMBER_WITH_RANGE_PATTERN = re.compile(r'^(\d+)(?:~\d+)?:\s')
_LINE_NUMBER_PREFIX_PATTERN = re.compile(r'^\d+\s*:\s*', re.MULTILINE)
_DOT_SPLIT_PATTERN = re.compile(r"\s*\.\s*")
_CYPHER_ESCAPE_TABLE = str.maketrans({'\n': '\\n', "'": "\\'"})
_VARIABLE_ROLE_MAP = {
    'PACKAGE_VARIABLE': '패키지 전역 변수',
    'DECLARE': '변수 선언및 초기화',
    'SPEC': '함수 및 프로시저 입력 매개변수'
}

# ==================== 섹션: 유틸리티 헬퍼 ====================
# 공통적으로 사용하는 문자열 처리, 범위 추출, 토큰 기준 판단 등의 헬퍼입니다.
def get_statement_type(start_line: int, end_line: int, node_statement_types: set[str]) -> str | None:
    """저장된 TYPE_start_end 표기에서 TYPE 추출 (최적화: suffix 직접 생성)"""
    suffix = f"_{start_line}_{end_line}"
    entry = next((e for e in node_statement_types if e.endswith(suffix)), None)
    return entry.rsplit('_', 2)[0] if entry else None


def get_table_relationship(statement_type: str | None) -> str | None:
    """구문 타입을 테이블 관계 라벨로 매핑 (O(1) 딕셔너리 조회)"""
    return _TABLE_RELATIONSHIP_MAP.get(statement_type) if statement_type else None


def is_over_token_limit(node_token: int, sp_token: int, context_len: int) -> bool:
    """토큰 임계치 도달 여부 판단 (배치 플러시 필요성 결정)"""
    return (context_len >= 10) or (context_len and (
        (sp_token >= 1000) or (node_token >= 1000 and node_token + sp_token >= 1000)
    ))


def escape_for_cypher_multiline(text: str) -> str:
    """Cypher 쿼리용 이스케이프 (최적화: translate 사용)"""
    return text.translate(_CYPHER_ESCAPE_TABLE)


def extract_code_within_range(code: str, context_range: list[dict]) -> tuple[str, int]:
    """라인 번호 접두가 포함된 code에서 context_range의 범위만 발췌"""
    try:
        if not context_range:
            return "", 0
        if not code:
            return "", max(r['endLine'] for r in context_range)

        start_line = min(r['startLine'] for r in context_range)
        end_line = max(r['endLine'] for r in context_range)
        
        # 리스트 컴프리헨션으로 최적화
        extracted_lines = [
            line for line in code.split('\n')
            if (match := _LINE_NUMBER_WITH_RANGE_PATTERN.match(line))
            and start_line <= int(match.group(1)) <= end_line
        ]

        return '\n'.join(extracted_lines), end_line
    
    except Exception as e:
        err_msg = f"Understanding 과정에서 범위내에 코드 추출 도중에 오류가 발생했습니다: {str(e)}"
        logging.error(err_msg)
        raise ProcessAnalyzeCodeError(err_msg)

def get_procedure_name(code: str) -> tuple[str | None, str | None]:
    """PL/SQL 선언부에서 PROCEDURE/FUNCTION/TRIGGER 이름 추출"""
    try:
        normalized = _LINE_NUMBER_PREFIX_PATTERN.sub('', code)
        match = PROCEDURE_PATTERN.search(normalized)
        if not match:
            return None, None

        parts = [p.strip().strip('"') for p in _DOT_SPLIT_PATTERN.split(match.group(1))]
        parts_len = len(parts)
        
        if parts_len == 3:
            return parts[0], f"{parts[1]}.{parts[2]}"
        if parts_len == 2:
            return parts[0], parts[1]
        if parts_len == 1:
            return None, parts[0]
        return None, None
    except Exception as e:
        logging.error(f"프로시저/함수/트리거 명 추출 중 오류: {str(e)}")
        return None, None


def _format_line_with_number(line: str, line_number: int) -> str:
    """라인 번호 접두 처리 헬퍼"""
    return f"{line}\n" if LINE_NUMBER_PATTERN.match(line) else f"{line_number}: {line}\n"


def summarize_with_placeholders(file_content: str, node: dict) -> str:
    """노드 범위의 코드를 가져오되 자식 범위는 플레이스홀더로 치환"""
    try:
        start_line, end_line = node['startLine'], node['endLine']
        code_lines = file_content.split('\n')[start_line-1:end_line]
        children = node.get('children')
        
        if not children:
            return ''.join(_format_line_with_number(line, i + start_line) for i, line in enumerate(code_lines))
        
        # 자식이 있는 경우 - 최적화: extend 사용
        summarized_code = []
        last_end_line = start_line - 1
        offset = start_line
        
        for child in children:
            # 자식 이전 코드
            child_start = child['startLine']
            before_start_idx = last_end_line - offset + 1
            before_end_idx = child_start - offset
            
            summarized_code.extend(
                _format_line_with_number(line, i + last_end_line + 1)
                for i, line in enumerate(code_lines[before_start_idx:before_end_idx])
            )
            
            summarized_code.append(f"{child_start}: ... code ...\n")
            last_end_line = child['endLine']
        
        # 마지막 자식 이후 코드
        summarized_code.extend(
            _format_line_with_number(line, i + last_end_line + 1)
            for i, line in enumerate(code_lines[last_end_line-offset+1:])
        )
        
        return ''.join(summarized_code)
    
    except Exception as e:
        err_msg = f"Understanding 과정에서 코드를 요약하는 도중 오류가 발생했습니다: {str(e)}"
        logging.error(err_msg)
        raise ProcessAnalyzeCodeError(err_msg)


def build_sp_code(current_schedule: dict, schedule_stack: list) -> str:
    """현재 스케줄에서 시작하여 상위 스케줄을 역순 적용, 플레이스홀더를 실제 코드로 치환"""
    try:
        focused_code = current_schedule["code"]
        current_start_line = current_schedule["startLine"]
        
        for schedule in reversed(schedule_stack):
            placeholder = f"{current_start_line}: ... code ..."
            schedule_code = schedule["code"]
            
            if placeholder in schedule_code:
                focused_code = schedule_code.replace(placeholder, focused_code, 1)
                current_start_line = schedule["startLine"]
            else:
                # 요약 앵커 치환 (정규식 컴파일 캐싱)
                if f"{current_start_line}~" in schedule_code:
                    anchor_pat = re.compile(rf"^{current_start_line}~\d+:\s.*$", re.MULTILINE)
                    if anchor_pat.search(schedule_code):
                        focused_code = anchor_pat.sub(focused_code, schedule_code, count=1)
                        current_start_line = schedule["startLine"]
        
        return focused_code

    except Exception as e:
        err_msg = f"Understanding 과정에서 분석할 코드 생성 도중에 오류가 발생했습니다: {str(e)}"
        logging.error(err_msg)
        raise ProcessAnalyzeCodeError(err_msg)
    

def get_original_node_code(file_content: str, start_line: int, end_line: int) -> str:
    """지정 라인 범위를 추출하고 라인 번호 접두 보장"""
    try:
        end_line = end_line if end_line else start_line
        lines = file_content.split('\n')[start_line-1:end_line]
        
        return '\n'.join(
            line if LINE_NUMBER_PATTERN.match(line) else f"{i + start_line}: {line}"
            for i, line in enumerate(lines)
        )
    
    except Exception as e:
        err_msg = f"Understanding 과정에서 노드에 맞게 코드를 추출 도중에 오류가 발생했습니다: {str(e)}"
        logging.error(err_msg)
        raise ProcessAnalyzeCodeError(err_msg)


# ==================== 섹션: 분석 Understanding 파이프라인(엔트리 포인트) ====================
class Analyzer:
    """분석 파이프라인을 담당하는 상태 보유형 클래스.

    역할:
    - ANTLR AST를 DFS로 순회하며 요약 코드 누적, 배치 플러시, LLM 분석, Neo4j 사이퍼 생성까지 전체 흐름을 관리합니다.
    - 중첩 함수와 nonlocal 공유 상태를 제거하여 가독성과 유지보수성을 향상합니다.

    보장 사항:
    - 기존 기능/사이드이펙트(큐 프로토콜, 사이퍼 쿼리, 토큰 임계치, 요약/관계 생성)와 완전 동일하게 동작합니다.
    """

    __slots__ = (
        'antlr_data', 'file_content', 'send_queue', 'receive_queue', 'last_line',
        'folder_name', 'file_name', 'user_id', 'api_key', 'locale', 'dbms', 'project_name',
        'folder_file', 'node_base_props', 'folder_props', 'table_base_props',
        'schedule_stack', 'context_range', 'cypher_query', 'summary_dict', 
        'node_statement_types', 'procedure_name', 'extract_code', 'focused_code', 
        'sp_token_count', 'schema_name'
    )

    def __init__(self, antlr_data: dict, file_content: str, send_queue: asyncio.Queue, receive_queue: asyncio.Queue, last_line: int, folder_name: str, file_name: str, user_id: str, api_key: str, locale: str, dbms: str, project_name: str):
        """Analyzer 초기화"""
        self.antlr_data = antlr_data
        self.file_content = file_content
        self.send_queue = send_queue
        self.receive_queue = receive_queue
        self.last_line = last_line
        self.folder_name = folder_name
        self.file_name = file_name
        self.user_id = user_id
        self.api_key = api_key
        self.locale = locale
        self.dbms = (dbms or 'postgres').lower()
        self.project_name = project_name or ''
        
        # 자주 사용하는 문자열 캐싱
        self.folder_file = f"{folder_name}-{file_name}"
        
        # Cypher 쿼리 공통 속성 (반복 사용)
        self.node_base_props = f"folder_name: '{folder_name}', file_name: '{file_name}', user_id: '{user_id}', project_name: '{project_name or ''}'"
        self.folder_props = f"user_id: '{user_id}', name: '{folder_name}', project_name: '{project_name or ''}'"
        self.table_base_props = f"user_id: '{user_id}'"  # Table MERGE용 캐싱
        
        # 분석 상태
        self.schedule_stack = []
        self.context_range = []
        self.cypher_query = []
        self.summary_dict = {}
        self.node_statement_types = set()
        self.procedure_name = None
        self.extract_code = ""
        self.focused_code = ""
        self.sp_token_count = 0
        self.schema_name = None


    def _build_table_merge(self, table_name: str, schema: str = '') -> str:
        """Table MERGE 구문 생성 헬퍼 (최적화: 캐시된 속성 사용)"""
        schema_part = f", schema: '{schema}'" if schema else ""
        return f"MERGE (t:Table {{{self.table_base_props}, name: '{table_name}'{schema_part}, db: '{self.dbms}', project_name: '{self.project_name}'}})"

    async def run(self):
        """전체 분석 파이프라인 실행"""
        logging.info(f"📋 [{self.folder_file}] 코드 분석 시작 (총 {self.last_line}줄)")
        try:
            await self.analyze_statement_tree(self.antlr_data, self.schedule_stack)

            if self.context_range and self.focused_code:
                self.extract_code, _ = extract_code_within_range(self.focused_code, self.context_range)
                await self.send_analysis_event_and_wait(self.last_line)
            
            logging.info(f"✅ [{self.folder_file}] 코드 분석 완료")
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
        """LLM 분석 실행 및 상태 초기화"""
        try:
            # context_range 정렬 (in-place)
            context_range = self.context_range
            context_range.sort(key=lambda x: x['startLine'])
            context_range_count = len(context_range)

            analysis_result = understand_code(self.extract_code, context_range, context_range_count, self.api_key, self.locale)
            cypher_queries = await self.process_analysis_output_to_cypher(analysis_result)

            # 결과 개수 검증
            if (actual_count := len(analysis_result["analysis"])) != context_range_count:
                logging.error(f"분석 결과 개수가 일치하지 않습니다. 예상: {context_range_count}, 실제: {actual_count}")

            if statement_type in PROCEDURE_TYPES:
                logging.info(f"[{self.folder_file}] {self.procedure_name} 프로시저의 요약 정보 추출 완료")
                summary = understand_summary(self.summary_dict, self.api_key, self.locale)
                self.cypher_query.append(f"""
                    MATCH (n:{statement_type} {{procedure_name: '{self.procedure_name}', {self.node_base_props}}})
                    SET n.summary = {json.dumps(summary['summary'])}
                """)
                self.schedule_stack.clear()
                self.node_statement_types.clear()
                self.summary_dict.clear()

            # 상태 초기화
            self.focused_code = self.extract_code = ""
            self.sp_token_count = 0
            context_range.clear()
            return cypher_queries

        except UnderstandingError:
            raise
        except Exception as e:
            err_msg = f"Understanding 과정에서 LLM의 결과 처리를 준비 및 시작하는 도중 문제가 발생했습니다: {str(e)}"
            logging.error(err_msg)
            raise ProcessAnalyzeCodeError(err_msg)


    async def process_analysis_output_to_cypher(self, analysis_result: dict) -> list:
        """LLM 분석 결과를 처리하여 Neo4j 사이퍼 쿼리를 생성"""
        try:
            analysis_list = analysis_result['analysis']
            node_statement_types = self.node_statement_types
            node_base_props = self.node_base_props
            schedule_stack = self.schedule_stack
            summary_dict = self.summary_dict
            cypher_query = self.cypher_query
            
            for result in analysis_list:
                start_line, end_line, summary = result['startLine'], result['endLine'], result['summary']
                local_tables, db_links, called_nodes, variables = (
                    result.get('localTables', []), result.get('dbLinks', []),
                    result.get('calls', []), result.get('variables', [])
                )

                statement_type = get_statement_type(start_line, end_line, node_statement_types)
                table_relationship_type = get_table_relationship(statement_type)

                # Summary 저장 및 쿼리 생성 (최적화: 키 재사용)
                summary_key = f"{statement_type}_{start_line}_{end_line}"
                summary_json = json.dumps(summary)
                summary_dict[summary_key] = summary
                cypher_query.append(f"""
                    MATCH (n:{statement_type} {{startLine: {start_line}, {node_base_props}}})
                    SET n.summary = {summary_json}
                """)

                # schedule["code"] 플레이스홀더 치환 (최적화: 정규식 최소화)
                placeholder_str = f"{start_line}: ... code ..."
                for schedule in schedule_stack:
                    if placeholder_str in schedule["code"]:
                        schedule["code"] = schedule["code"].replace(placeholder_str, f"{start_line}~{end_line}: {summary}", 1)
                        break

                # Variable 사용 기록 (최적화: 반복 접근 제거)
                if variables:
                    var_range = f"{start_line}_{end_line}"
                    procedure_name = self.procedure_name
                    for var_name in variables:
                        cypher_query.append(f"""
                            MATCH (v:Variable {{name: '{var_name}', procedure_name: '{procedure_name}', {node_base_props}}})
                            SET v.`{var_range}` = 'Used'
                        """)

                # CALL/ASSIGNMENT 처리 (최적화: 로컬 변수 캐싱)
                if called_nodes and statement_type in ("CALL", "ASSIGNMENT"):
                    if statement_type == "ASSIGNMENT":
                        cypher_query.append(f"""
                            MATCH (a:ASSIGNMENT {{startLine: {start_line}, {node_base_props}}})
                            REMOVE a:ASSIGNMENT
                            SET a:CALL, a.name = 'CALL[{start_line}]'
                        """)
                        statement_type = "CALL"

                    user_id, project_name = self.user_id, self.project_name
                    for name in called_nodes:
                        if '.' in name:
                            package_name, proc_name = name.upper().split('.')
                            cypher_query.append(f"""
                                MATCH (c:{statement_type} {{startLine: {start_line}, {node_base_props}}}) 
                                OPTIONAL MATCH (p)
                                WHERE (p:PROCEDURE OR p:FUNCTION)
                                AND p.folder_name = '{package_name}' 
                                AND p.procedure_name = '{proc_name}'
                                AND p.user_id = '{user_id}'
                                WITH c, p
                                FOREACH(ignoreMe IN CASE WHEN p IS NULL THEN [1] ELSE [] END |
                                    CREATE (new:PROCEDURE:FUNCTION {{folder_name: '{package_name}', procedure_name: '{proc_name}', user_id: '{user_id}', project_name: '{project_name}'}})
                                    MERGE (c)-[:CALL {{scope: 'external'}}]->(new)
                                )
                                FOREACH(ignoreMe IN CASE WHEN p IS NOT NULL THEN [1] ELSE [] END |
                                    MERGE (c)-[:CALL {{scope: 'external'}}]->(p)
                                )
                            """)
                        else:
                            cypher_query.append(f"""
                                MATCH (c:{statement_type} {{startLine: {start_line}, {node_base_props}}})
                                WITH c
                                MATCH (p {{procedure_name: '{name}', {node_base_props}}})
                                WHERE p:PROCEDURE OR p:FUNCTION
                                MERGE (c)-[:CALL {{scope: 'internal'}}]->(p)
                            """)

                # Table 관계 처리 (최적화: 중복 MERGE 제거)
                if table_relationship_type and local_tables:
                    folder_name, folder_props, dbms = self.folder_name, self.folder_props, self.dbms
                    node_merge = f"MERGE (n:{statement_type} {{startLine: {start_line}, {node_base_props}}})"
                    folder_merge = f"MERGE (folder:SYSTEM {{{folder_props}}})"
                    
                    for tn in local_tables:
                        if qualified := str(tn).strip().upper():
                            schema_part, name_part, _ = parse_table_identifier(qualified)
                            
                            cypher_query.append(f"""
                                {node_merge}
                                WITH n
                                {self._build_table_merge(name_part, schema_part)}
                                ON CREATE SET t.folder_name = '{folder_name}'
                                ON MATCH  SET t.folder_name = CASE WHEN coalesce(t.folder_name,'') = '' THEN '{folder_name}' ELSE t.folder_name END
                                WITH n, t
                                {folder_merge}
                                MERGE (folder)-[:CONTAINS]->(t)
                                SET t.db = coalesce(t.db, '{dbms}')
                                MERGE (n)-[:{table_relationship_type}]->(t)
                            """)

                # FK 관계 처리 (최적화: 빈 체크 + 캐싱)
                fk_relations = result.get('fkRelations', [])
                if fk_relations:
                    user_id, dbms, project_name = self.user_id, self.dbms, self.project_name
                    
                    for fk in fk_relations:
                        src_table = (fk.get('sourceTable') or '').strip().upper()
                        tgt_table = (fk.get('targetTable') or '').strip().upper()
                        src_columns = [
                            (col or '').strip()
                            for col in (fk.get('sourceColumns') or [])
                            if col is not None and str(col).strip()
                        ]
                        tgt_columns = [
                            (col or '').strip()
                            for col in (fk.get('targetColumns') or [])
                            if col is not None and str(col).strip()
                        ]
                        if not (src_table and tgt_table and src_columns and tgt_columns):
                            continue

                        src_schema, src_table_name, _ = parse_table_identifier(src_table)
                        tgt_schema, tgt_table_name, _ = parse_table_identifier(tgt_table)

                        # Table FK 관계
                        src_t_props = f"user_id: '{user_id}', schema: '{src_schema or ''}', name: '{src_table_name}', db: '{dbms}', project_name: '{project_name}'"
                        tgt_t_props = f"user_id: '{user_id}', schema: '{tgt_schema or ''}', name: '{tgt_table_name}', db: '{dbms}', project_name: '{project_name}'"
                        
                        cypher_query.append(
                            f"MATCH (st:Table {{{src_t_props}}})\n"
                            f"MATCH (tt:Table {{{tgt_t_props}}})\n"
                            f"SET st.db = coalesce(st.db, 'postgres'), tt.db = coalesce(tt.db, 'postgres')\n"
                            f"MERGE (st)-[:FK_TO_TABLE]->(tt)"
                        )

                        # Column FK 관계
                        for src_column, tgt_column in zip(src_columns, tgt_columns):
                            if not (src_column and tgt_column):
                                continue
                            src_fqn = '.'.join(filter(None, [src_schema, src_table_name, src_column])).lower()
                            tgt_fqn = '.'.join(filter(None, [tgt_schema, tgt_table_name, tgt_column])).lower()
                            
                            cypher_query.append(
                                f"MATCH (sc:Column {{user_id: '{user_id}', name: '{src_column}', fqn: '{src_fqn}'}})\n"
                                f"MATCH (dc:Column {{user_id: '{user_id}', name: '{tgt_column}', fqn: '{tgt_fqn}'}})\n"
                                f"MERGE (sc)-[:FK_TO]->(dc)"
                            )

                # DBLink 처리 (최적화: 빈 체크 + 캐싱)
                if table_relationship_type and db_links:
                    user_id, project_name = self.user_id, self.project_name
                    
                    for link_item in db_links:
                        mode = (link_item.get('mode') or 'r').lower()
                        name = (link_item.get('name') or '').strip().upper()
                        schema_part, name_part, link_name = parse_table_identifier(name)

                        merge_table = self._build_table_merge(name_part, schema_part).replace(f", db: '{self.dbms}'", "")
                        
                        cypher_query.append(f"""
                            MERGE (n:{statement_type} {{startLine: {start_line}, {node_base_props}}})
                            WITH n
                            {merge_table}
                            ON CREATE SET t.folder_name = ''
                            SET t.db_link = '{link_name}'
                            WITH n, t
                            MERGE (l:DBLink {{user_id: '{user_id}', name: '{link_name}', project_name: '{project_name}'}})
                            MERGE (l)-[:CONTAINS]->(t)
                            MERGE (n)-[:DB_LINK {{mode: '{mode}'}}]->(t)
                        """)

            return self.cypher_query

        except Exception as e:
            err_msg = f"Understanding 과정에서 LLM의 결과를 이용해 사이퍼쿼리를 생성하는 도중 오류가 발생했습니다: {str(e)}"
            logging.error(err_msg)
            raise ProcessAnalyzeCodeError(err_msg)


    
    def analyze_variable_declarations(self, declaration_code: str, node_startLine: int, statement_type: str):
        """변수 선언부 분석 및 Variable 노드 생성"""
        try:
            role = _VARIABLE_ROLE_MAP.get(statement_type, '알 수 없는 매개변수')
            
            logging.info(f"[{self.folder_file}] {self.procedure_name}의 변수 분석 시작")
            analysis_result = understand_variables(declaration_code, self.api_key, self.locale)
            logging.info(f"[{self.folder_file}] {self.procedure_name}의 변수 분석 완료")
            
            var_summary = json.dumps(analysis_result.get("summary", "unknown"))
            variables = analysis_result["variables"]
            is_package = statement_type == 'PACKAGE_VARIABLE'
            scope = 'Global' if is_package else 'Local'
            
            # 공통 부분 캐싱
            node_base = self.node_base_props
            folder_props = self.folder_props
            procedure_name = self.procedure_name
            
            cypher_query = self.cypher_query
            
            # node_match_props 사전 계산 (반복 제거)
            if is_package:
                node_match_props = f"startLine: {node_startLine}, {node_base}"
                base_var_props = f"{node_base}, role: '{role}', scope: '{scope}'"
            else:
                node_match_props = f"startLine: {node_startLine}, procedure_name: '{procedure_name}', {node_base}"
                base_var_props = f"{node_base}, procedure_name: '{procedure_name}', role: '{role}', scope: '{scope}'"
            
            for variable in variables:
                var_name, var_type, var_param_type = variable["name"], variable["type"], variable["parameter_type"]
                var_value = variable["value"] if variable["value"] is not None else ''
                
                cypher_query.append(f"""
                    MERGE (v:Variable {{name: '{var_name}', {base_var_props}, type: '{var_type}', parameter_type: '{var_param_type}', value: {json.dumps(var_value)}}})
                    WITH v
                    MATCH (p:{statement_type} {{{node_match_props}}})
                    SET p.summary = {var_summary}
                    WITH p, v
                    MERGE (p)-[:SCOPE]->(v)
                    WITH v
                    MERGE (folder:SYSTEM {{{folder_props}}})
                    MERGE (folder)-[:CONTAINS]->(v)
                """)

        except LLMCallError:
            raise
        except Exception as e:
            err_msg = f"Understanding 과정에서 프로시저 선언부 분석 및 변수 노드 생성 중 오류가 발생했습니다: {str(e)}"
            logging.error(err_msg)
            raise ProcessAnalyzeCodeError(err_msg)


    async def send_analysis_event_and_wait(self, node_end_line: int, statement_type: str = None):
        """분석 결과 이벤트를 송신하고 처리 완료 대기"""
        try:
            logging.info(f"🤖 [{self.folder_file}] AI 분석 시작")
            results = await self.execute_analysis_and_reset_state(statement_type)
            logging.info(f"📤 [{self.folder_file}] 분석 결과 전송 (Cypher 쿼리 {len(results)}개)")
            await self.send_queue.put({"type": "analysis_code", "query_data": results, "line_number": node_end_line})

            while True:
                response = await self.receive_queue.get()
                if response['type'] == 'process_completed':
                    logging.info(f"✅ [{self.folder_name}] NEO4J 저장 완료\n")
                    self.cypher_query.clear()
                    break

        except UnderstandingError:
            raise
        except Exception as e:
            err_msg = f"Understanding 과정에서 이벤트를 송신하고 수신하는 도중 오류가 발생했습니다: {str(e)}"
            logging.error(err_msg)
            raise ProcessAnalyzeCodeError(err_msg)


    async def analyze_statement_tree(self, node: dict, schedule_stack: list, parent_startLine: int = None, parent_statementType: str = None):
        """문 트리를 분석하며 노드/관계 생성, 요약 조립, 배치 플러시 수행"""
        start_line, end_line, statement_type = node['startLine'], node['endLine'], node['type']
        children = node.get('children', [])
        has_children = 'true' if children else 'false'
        
        # 코드 생성 (메모리 최적화)
        summarized_code = summarize_with_placeholders(self.file_content, node)
        node_code = get_original_node_code(self.file_content, start_line, end_line)
        node_size = calculate_code_token(node_code)
        
        logging.info(f"🚀 노드: {statement_type} {start_line}-{end_line} (크기:{node_size}, 자식:{has_children})")

        # current_schedule 생성 (최적화: 직접 할당)
        current_schedule = {
            "startLine": start_line, "endLine": end_line, "code": summarized_code,
            "child": children, "type": statement_type
        }

        if statement_type in PROCEDURE_TYPES:
            self.schema_name, self.procedure_name = get_procedure_name(node_code)
            logging.info(f"🚀 프로시저/함수/트리거 이름: {self.procedure_name}")

        # 토큰 리밋 체크 전 extract_code 생성
        context_range = self.context_range
        if context_range:
            self.extract_code, line_number = extract_code_within_range(self.focused_code, context_range)
            self.sp_token_count = calculate_code_token(self.extract_code)
            
            if is_over_token_limit(node_size, self.sp_token_count, len(context_range)):
                logging.info(f"⚠️ [{self.folder_file}] 리미트 도달, 중간 분석 실행 (토큰: {self.sp_token_count})")
                await self.send_analysis_event_and_wait(line_number)
        else:
            self.extract_code = ""

        # focused_code 업데이트 (최적화: 중복 조건 제거)
        placeholder = f"{start_line}: ... code ..."
        if not self.focused_code or placeholder not in self.focused_code:
            self.focused_code = build_sp_code(current_schedule, schedule_stack)
        else:
            self.focused_code = self.focused_code.replace(placeholder, summarized_code, 1)

        # 노드 생성 쿼리 (최적화: 공통 부분 캐싱)
        node_base_props = self.node_base_props
        folder_props = self.folder_props
        cypher_query = self.cypher_query
        
        if not children and statement_type not in NON_ANALYSIS_TYPES:
            context_range.append({"startLine": start_line, "endLine": end_line})
            escaped_code = node_code.replace("'", "\\'")
            cypher_query.append(f"""
                MERGE (n:{statement_type} {{startLine: {start_line}, {node_base_props}}})
                SET n.endLine = {end_line}, n.name = '{statement_type}[{start_line}]', n.node_code = '{escaped_code}',
                    n.token = {node_size}, n.procedure_name = '{self.procedure_name}', n.has_children = {has_children}
                WITH n
                MERGE (folder:SYSTEM {{{folder_props}}})
                MERGE (folder)-[:CONTAINS]->(n)
            """)
        else:
            # escape 호출은 필요한 경우에만 (최적화)
            escaped_code, escaped_summary = escape_for_cypher_multiline(node_code), escape_for_cypher_multiline(summarized_code)
            
            if statement_type == "FILE":
                file_summary = 'File Start Node' if self.locale == 'en' else '파일 노드'
                cypher_query.append(f"""
                    MERGE (n:{statement_type} {{startLine: {start_line}, {node_base_props}}})
                    SET n.endLine = {end_line}, n.name = '{self.file_name}', n.summary = '{file_summary}', n.has_children = {has_children}
                    WITH n
                    MERGE (folder:SYSTEM {{{folder_props}}})
                    MERGE (folder)-[:CONTAINS]->(n)
                """)
            elif statement_type in ("PROCEDURE", "FUNCTION"):
                remove_label = 'FUNCTION' if statement_type == 'PROCEDURE' else 'PROCEDURE'
                cypher_query.append(f"""
                    MERGE (n:{statement_type} {{procedure_name: '{self.procedure_name}', {node_base_props}}})
                    SET n.startLine = {start_line}, n.endLine = {end_line}, n.name = '{statement_type}[{start_line}]',
                        n.summarized_code = '{escaped_summary}', n.node_code = '{escaped_code}', n.token = {node_size}, n.has_children = {has_children}
                    WITH n
                    REMOVE n:{remove_label}
                    WITH n
                    MERGE (folder:SYSTEM {{{folder_props}}})
                    MERGE (folder)-[:CONTAINS]->(n)
                """)
            else:
                cypher_query.append(f"""
                    MERGE (n:{statement_type} {{startLine: {start_line}, {node_base_props}}})
                    SET n.endLine = {end_line}, n.name = '{statement_type}[{start_line}]', n.summarized_code = '{escaped_summary}',
                        n.node_code = '{escaped_code}', n.token = {node_size}, n.procedure_name = '{self.procedure_name}', n.has_children = {has_children}
                    WITH n
                    MERGE (folder:SYSTEM {{{folder_props}}})
                    MERGE (folder)-[:CONTAINS]->(n)
                """)

        # 변수 선언부 분석 (최적화: 튜플 사용)
        if statement_type == "PACKAGE_VARIABLE" or (self.procedure_name and statement_type in ("SPEC", "DECLARE")):
            self.analyze_variable_declarations(node_code, start_line, statement_type)

        schedule_stack.append(current_schedule)
        self.node_statement_types.add(f"{statement_type}_{start_line}_{end_line}")

        # PARENT_OF 관계 생성
        if parent_statementType:
            cypher_query.append(f"""
                MATCH (parent:{parent_statementType} {{startLine: {parent_startLine}, {node_base_props}}})
                WITH parent
                MATCH (child:{statement_type} {{startLine: {start_line}, {node_base_props}}})
                MERGE (parent)-[:PARENT_OF]->(child)
            """)
        
        # 자식 노드 순회 및 NEXT 관계 생성 (최적화: 캐싱 + 조건 개선)
        if children:
            prev_statement = prev_id = None
            cypher_query = self.cypher_query
            non_next_types = NON_NEXT_RECURSIVE_TYPES
            
            for child in children:
                await self.analyze_statement_tree(child, schedule_stack, start_line, statement_type)

                if prev_id and prev_statement not in non_next_types:
                    child_type, child_start = child['type'], child['startLine']
                    cypher_query.append(f"""
                        MATCH (prev:{prev_statement} {{startLine: {prev_id}, {node_base_props}}})
                        WITH prev
                        MATCH (current:{child_type} {{startLine: {child_start}, {node_base_props}}})
                        MERGE (prev)-[:NEXT]->(current)
                    """)
                prev_statement, prev_id = child['type'], child['startLine']
            
            # 중간 플러시 또는 context_range 추가
            if statement_type in PROCEDURE_TYPES and context_range and self.focused_code:
                self.extract_code, line_number = extract_code_within_range(self.focused_code, context_range)
                logging.info(f"📤 [{self.folder_file}] 중간 플러시 실행 (토큰: {self.sp_token_count})")
                await self.send_analysis_event_and_wait(line_number, statement_type)
            elif statement_type not in NON_ANALYSIS_TYPES:
                context_range.append({"startLine": start_line, "endLine": end_line})

        # schedule_stack 필터링 (최적화: start_line 재사용)
        schedule_stack[:] = [s for s in schedule_stack if s['child'] and s['endLine'] > start_line]

