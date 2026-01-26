"""DBMS 코드 분석기 - PL/SQL AST → Neo4j 그래프

프로시저/함수 분석에 필요한 정보를 추출합니다.

분석 파이프라인:
1. AST 수집 (StatementCollector)
2. 정적 그래프 생성 (PROCEDURE, FUNCTION 노드)
3. DML 문 분석 (테이블/컬럼 관계)
4. LLM 배치 분석 (요약, 변수 타입)
5. 프로시저 요약 및 User Story 생성

리팩토링: BaseAstProcessor 상속으로 공통 로직 재사용
"""

from __future__ import annotations

import asyncio
import json
import logging
import re
from typing import Any, Dict, List, Optional, Tuple, Set

from config.settings import settings
from util.rule_loader import RuleLoader
# Exceptions: 모든 커스텀 예외는 RuntimeError로 대체됨
from util.text_utils import calculate_code_token, escape_for_cypher, parse_table_identifier, log_process

from analyzer.strategy.base.statement_node import StatementNode
from analyzer.strategy.base.batch import AnalysisBatch
from analyzer.strategy.base.processor import BaseAstProcessor


# ==================== 상수 정의 ====================
# 노드 타입 분류
PROCEDURE_TYPES = frozenset(["PROCEDURE", "FUNCTION", "CREATE_PROCEDURE_BODY", "TRIGGER", "BEGIN"])
NON_ANALYSIS_TYPES = frozenset(["CREATE_PROCEDURE_BODY", "FILE", "PROCEDURE", "FUNCTION", "DECLARE", "TRIGGER", "SPEC"])
NON_NEXT_RECURSIVE_TYPES = frozenset(["FUNCTION", "PROCEDURE", "PACKAGE_VARIABLE", "TRIGGER"])
DML_STATEMENT_TYPES = frozenset(["SELECT", "INSERT", "UPDATE", "DELETE", "MERGE", "EXECUTE_IMMEDIATE", "FETCH", "CREATE_TEMP_TABLE", "CTE", "OPEN_CURSOR", "CURSOR_VARIABLE"])
VARIABLE_DECLARATION_TYPES = frozenset(["PACKAGE_VARIABLE", "DECLARE", "SPEC"])

# 관계 매핑
TABLE_RELATIONSHIP_MAP = {"r": "FROM", "w": "WRITES"}
VARIABLE_ROLE_MAP = {
    "PACKAGE_VARIABLE": "패키지 전역 변수",
    "DECLARE": "변수 선언및 초기화",
    "SPEC": "함수 및 프로시저 입력 매개변수",
}

# 설정에서 가져오는 상수
STATIC_QUERY_BATCH_SIZE = settings.batch.static_query_batch_size
VARIABLE_CONCURRENCY = settings.concurrency.variable_concurrency
MAX_BATCH_TOKEN = settings.batch.max_batch_token
MAX_CONCURRENCY = settings.concurrency.max_concurrency
MAX_SUMMARY_CHUNK_TOKEN = settings.batch.max_summary_chunk_token
MAX_CONTEXT_TOKEN = settings.batch.max_context_token
PARENT_EXPAND_THRESHOLD = settings.batch.parent_expand_threshold

# 정규식 패턴
LINE_NUMBER_PATTERN = re.compile(r"^\d+\s*:")


# ==================== 데이터 클래스 ====================
class ProcedureInfo:
    """프로시저/함수 정보"""
    __slots__ = ('key', 'procedure_type', 'procedure_name', 'schema_name', 'start_line', 'end_line', 'pending_nodes')
    
    def __init__(
        self,
        key: str,
        procedure_type: str,
        procedure_name: str,
        schema_name: Optional[str],
        start_line: int,
        end_line: int,
        pending_nodes: int = 0,
    ):
        self.key = key
        self.procedure_type = procedure_type
        self.procedure_name = procedure_name
        self.schema_name = schema_name
        self.start_line = start_line
        self.end_line = end_line
        self.pending_nodes = pending_nodes


# ==================== 헬퍼 함수 ====================
def get_procedure_name_from_code(code: str) -> Tuple[Optional[str], Optional[str]]:
    """코드 문자열에서 스키마/프로시저 이름을 추출합니다."""
    pattern = re.compile(
        r"\b(?:CREATE\s+(?:OR\s+REPLACE\s+)?)?(?:PROCEDURE|FUNCTION|TRIGGER)\s+"
        r"((?:\"[^\"]+\"|[A-Za-z_][\w$#]*)"
        r"(?:\s*\.\s*(?:\"[^\"]+\"|[A-Za-z_][\w$#]*)){0,2})",
        re.IGNORECASE,
    )
    prefix_pattern = re.compile(r"^\d+\s*:\s*")
    normalized = prefix_pattern.sub("", code)
    match = pattern.search(normalized)
    if not match:
        return None, None
    parts = [segment.strip().strip('"') for segment in re.split(r"\s*\.\s*", match.group(1))]
    if len(parts) == 3:
        return parts[0], f"{parts[1]}.{parts[2]}"
    if len(parts) == 2:
        return parts[0], parts[1]
    if parts:
        return None, parts[0]
    return None, None


def build_statement_name(node_type: str, start_line: int) -> str:
    """노드 타입과 시작 라인을 조합한 식별자 문자열을 생성합니다."""
    return f"{node_type}[{start_line}]"


# ==================== RuleLoader 헬퍼 ====================
def _rule_loader() -> RuleLoader:
    return RuleLoader(target_lang="dbms")


def analyze_code(code: str, context: str, ranges: list, count: int, api_key: str, locale: str) -> Dict[str, Any]:
    """코드 분석 (컨텍스트와 코드 분리 전달)"""
    inputs = {"code": code, "ranges": ranges, "count": count, "locale": locale}
    if context.strip():
        inputs["context"] = context
    return _rule_loader().execute(
        "analysis",
        inputs,
        api_key,
    )


def analyze_dml_tables(code: str, context: str, ranges: list, api_key: str, locale: str) -> Dict[str, Any]:
    """DML 테이블 분석 (컨텍스트와 코드 분리 전달)"""
    inputs = {"code": code, "ranges": ranges, "locale": locale}
    if context.strip():
        inputs["context"] = context
    return _rule_loader().execute(
        "dml",
        inputs,
        api_key,
    )


def analyze_summary_only(summaries: dict, api_key: str, locale: str, previous_summary: str = "") -> Dict[str, Any]:
    """프로시저/함수 전체 요약 생성 (Summary만)."""
    return _rule_loader().execute(
        "procedure_summary_only",
        {"summaries": summaries, "locale": locale, "previous_summary": previous_summary},
        api_key,
    )


def analyze_user_story(summary: str, api_key: str, locale: str) -> Dict[str, Any]:
    """프로시저/함수 User Story + AC 생성."""
    return _rule_loader().execute(
        "procedure_user_story",
        {"summary": summary, "locale": locale},
        api_key,
    )


def summarize_table_metadata(
    table_name: str,
    table_sentences: list,
    column_sentences: dict,
    column_metadata: dict,
    api_key: str,
    locale: str,
) -> Dict[str, Any]:
    return _rule_loader().execute(
        "table_summary",
        {
            "table_name": table_name,
            "table_sentences": table_sentences,
            "column_sentences": column_sentences,
            "column_metadata": column_metadata,
            "locale": locale,
        },
        api_key,
    )


def analyze_variables(declaration_code: str, api_key: str, locale: str) -> Dict[str, Any]:
    return _rule_loader().execute(
        "variables",
        {"declaration_code": declaration_code, "locale": locale},
        api_key,
    )


def extract_parent_context(skeleton_code: str, ancestor_context: str, api_key: str, locale: str) -> str:
    """부모 노드의 핵심 컨텍스트를 추출합니다."""
    result = _rule_loader().execute(
        "parent_context",
        {"skeleton_code": skeleton_code, "ancestor_context": ancestor_context, "locale": locale},
        api_key,
    )

    if isinstance(result, dict):
        return result.get("context_summary", "").strip()
    raise ValueError(f"parent_context 규칙이 dict가 아닌 값을 반환했습니다: {type(result)}")


# ==================== 노드 수집기 ====================
class StatementCollector:
    """AST를 후위순회하여 `StatementNode`와 프로시저 정보를 수집합니다.
    
    file_content는 더 이상 필요하지 않음 - AST JSON의 code 속성 사용.
    """
    def __init__(self, antlr_data: Dict[str, Any], directory: str, file_name: str):
        """수집기에 필요한 AST 데이터와 파일 메타 정보를 초기화합니다."""
        self.antlr_data = antlr_data
        self.directory = directory
        self.file_name = file_name
        self.nodes: List[StatementNode] = []
        self.procedures: Dict[str, ProcedureInfo] = {}
        self._node_id = 0

    def _parse_code_to_lines(self, code: str, start_line: int, end_line: int) -> List[Tuple[int, str]]:
        """JSON code 속성을 [(line_no, text), ...] 형태로 파싱합니다.
        
        Args:
            code: '1: CREATE...\n2: ...' 또는 '1: CREATE...\r\n2: ...' 형태의 문자열
            start_line: 노드 시작 라인 (fallback용)
            end_line: 노드 종료 라인 (fallback용)
            
        Returns:
            [(line_no, text), ...] 형태의 튜플 리스트
        """
        if not code:
            return []
        
        # \r\n 또는 \n으로 분리
        lines = code.replace('\r\n', '\n').split('\n')
        parsed_lines: List[Tuple[int, str]] = []
        
        for line in lines:
            if not line:
                continue
            # '123: text' 형태 파싱
            match = re.match(r'^(\d+):\s?(.*)', line)
            if match:
                line_no = int(match.group(1))
                text = match.group(2)
                parsed_lines.append((line_no, text))
            else:
                # 매칭 실패 시 전체 라인을 텍스트로 (fallback)
                if parsed_lines:
                    last_no = parsed_lines[-1][0]
                    parsed_lines.append((last_no + 1, line))
                else:
                    parsed_lines.append((start_line, line))
        
        return parsed_lines

    def collect(self) -> Tuple[List[StatementNode], Dict[str, ProcedureInfo]]:
        """AST 전역을 후위 순회하여 노드 목록과 프로시저 정보를 생성합니다."""
        # 루트 노드부터 후위순회합니다 (자식 → 부모 순서 보장)
        self._visit(self.antlr_data, current_proc=None, current_type=None, current_schema=None)
        return self.nodes, self.procedures

    def _make_proc_key(self, procedure_name: Optional[str], start_line: int) -> str:
        """프로시저 고유키를 생성합니다."""
        base = procedure_name or f"anonymous_{start_line}"
        return f"{self.directory}:{self.file_name}:{base}:{start_line}"

    def _should_treat_as_procedure(self, node_type: str, current_proc: Optional[str]) -> bool:
        """노드 타입이 프로시저로 처리되어야 하는지 판단합니다."""
        if node_type not in PROCEDURE_TYPES:
            return False
        if node_type == "BEGIN":
            return current_proc is None
        return True

    def _visit(
        self,
        node: Dict[str, Any],
        current_proc: Optional[str],
        current_type: Optional[str],
        current_schema: Optional[str],
    ) -> Optional[StatementNode]:
        """재귀적으로 AST를 내려가며 StatementNode를 생성하고 부모-자식 관계를 구축합니다."""
        start_line = node['startLine']
        end_line = node['endLine']
        node_type = node['type']
        children = node.get('children', []) or []

        child_nodes: List[StatementNode] = []
        procedure_key = current_proc
        procedure_type = current_type
        schema_name = current_schema

        # AST JSON의 code 속성에서 라인 정보 추출
        # code 형식: "1: CREATE...\r\n2: ..."
        raw_code = node.get('code', '')
        line_entries = self._parse_code_to_lines(raw_code, start_line, end_line)
        code = '\n'.join(f"{line_no}: {text}" for line_no, text in line_entries)

        # 프로시저 타입 처리: PROCEDURE/FUNCTION/TRIGGER/BEGIN
        if self._should_treat_as_procedure(node_type, current_proc):
            if node_type == "BEGIN":
                procedure_key = self._make_proc_key(None, start_line)
                procedure_type = "BEGIN"
                schema_name = None
                proc_name = f"anonymous_{start_line}"
            else:
                # JSON에서 name, schema 직접 추출 (정규식 추출보다 정확)
                name_from_json = node.get('name')
                schema_from_json = node.get('schema')
                
                # JSON에 name이 있으면 사용, 없으면 기존 정규식 fallback
                if name_from_json:
                    name_candidate = name_from_json
                    schema_candidate = schema_from_json
                else:
                    # fallback: 기존 정규식 추출 (deprecated)
                    schema_candidate, name_candidate = get_procedure_name_from_code(code)
                
                procedure_key = self._make_proc_key(name_candidate, start_line)
                procedure_type = node_type
                schema_name = schema_candidate
                proc_name = name_candidate or procedure_key
            
            if procedure_key not in self.procedures:
                self.procedures[procedure_key] = ProcedureInfo(
                    key=procedure_key,
                    procedure_type=procedure_type,
                    procedure_name=proc_name,
                    schema_name=schema_name,
                    start_line=start_line,
                    end_line=end_line,
                )
                log_process("ANALYZE", "COLLECT", f"📋 프로시저 선언 발견: {proc_name} (라인 {start_line}~{end_line})")

        for child in children:
            child_node = self._visit(child, procedure_key, procedure_type, schema_name)
            if child_node is not None:
                child_nodes.append(child_node)

        # 분석 가능 여부 계산 (원본과 동일하게 NON_ANALYSIS_TYPES 기준)
        analyzable = node_type not in NON_ANALYSIS_TYPES
        token = calculate_code_token(code)
        dml = node_type in DML_STATEMENT_TYPES
        has_children = bool(child_nodes)

        # 현재 프로시저 정보 조회
        proc_info = self.procedures.get(procedure_key) if procedure_key else None
        proc_name = proc_info.procedure_name if proc_info else None

        self._node_id += 1
        statement_node = StatementNode(
            node_id=self._node_id,
            start_line=start_line,
            end_line=end_line,
            node_type=node_type,
            code=code,
            token=token,
            has_children=has_children,
            analyzable=analyzable,
            # 통합 필드
            unit_key=procedure_key,
            unit_name=proc_name,
            unit_kind=procedure_type,
            # DBMS 전용 필드
            schema_name=schema_name,
            dml=dml,
            # AST JSON 메타데이터 (선택적)
            signature=node.get('signature'),
            parameters=node.get('parameters'),
            lines=line_entries,
        )
        for child_node in child_nodes:
            child_node.parent = statement_node
        statement_node.children.extend(child_nodes)

        # 프로시저 요약 완료 시점을 판별하기 위해 pending 노드 수를 추적합니다.
        # analyzable=True인 노드는 배치에 포함되므로, completion_event는 배치 완료 시에만 설정
        # analyzable=False인 노드는 배치에 포함되지 않으므로, 수집 시 처리
        if not analyzable:
            # 배치에 포함되지 않는 노드는 수집 시 summary + completion_event 설정
            statement_node.summary = statement_node.get_raw_code()
            statement_node.completion_event.set()
        elif procedure_key and procedure_key in self.procedures:
            # 프로시저에 속한 분석 대상 노드
            self.procedures[procedure_key].pending_nodes += 1
        # else: analyzable=True이지만 procedure_key 없음 → 배치에서 LLM 분석 후 completion_event 설정됨

        self.nodes.append(statement_node)
        log_process("ANALYZE", "COLLECT", f"✅ {node_type} 노드 수집 완료: 라인 {start_line}~{end_line}, 토큰 {token}, 자식 {len(child_nodes)}개")
        return statement_node


# ==================== AST 프로세서 본체 ====================
class DbmsAstProcessor(BaseAstProcessor):
    """DBMS AST 처리 및 LLM 분석 파이프라인
    
    BaseAstProcessor를 상속하여 공통 파이프라인 재사용.
    DBMS 전용 로직만 구현.
    """
    def __init__(
        self,
        antlr_data: dict,
        directory: str,
        file_name: str,
        api_key: str,
        locale: str,
        dbms: str,
        last_line: int,
        default_schema: str = "public",
        ddl_table_metadata: Optional[Dict[Tuple[str, str], Dict[str, Any]]] = None,
        name_case: str = "original",
    ):
        """DBMS Analyzer 초기화
        
        file_content는 더 이상 필요하지 않음 - AST JSON의 code 속성 사용.
        """
        super().__init__(
            antlr_data=antlr_data,
            directory=directory,
            file_name=file_name,
            api_key=api_key,
            locale=locale,
            last_line=last_line,
        )
        
        self.dbms = (dbms or 'postgres').lower()
        self.default_schema = default_schema
        self._ddl_table_metadata = ddl_table_metadata or {}
        self.name_case = (name_case or 'original').lower()  # original, uppercase, lowercase
        
        self.table_base_props = ""
        
        # 테이블/컬럼 설명 요약용 저장소 (DML 분석에서 수집)
        self._table_summary_store: Dict[Tuple[str, str], Dict[str, Any]] = {}

    def _apply_name_case(self, name: str) -> str:
        """메타데이터 대소문자 변환 적용
        
        Args:
            name: 변환할 이름 (테이블명, 컬럼명, 스키마명 등)
        
        Returns:
            변환된 이름
        """
        if not name:
            return name
        if self.name_case == "uppercase":
            return name.upper()
        elif self.name_case == "lowercase":
            return name.lower()
        return name  # original: 그대로 반환

    # =========================================================================
    # BaseAstProcessor 추상 메서드 구현
    # =========================================================================
    
    def _collect_nodes(self) -> Tuple[List[StatementNode], Dict[str, ProcedureInfo]]:
        """AST 수집"""
        collector = StatementCollector(
            self.antlr_data, self.directory, self.file_name
        )
        return collector.collect()

    def _get_excluded_context_types(self) -> Set[str]:
        """컨텍스트 생성에서 제외할 노드 타입"""
        return PROCEDURE_TYPES

    def _use_dml_ranges(self) -> bool:
        """DBMS는 DML 범위 포함"""
        return True

    async def _extract_parent_context(self, skeleton_code: str, ancestor_context: str) -> str:
        """부모 컨텍스트 추출"""
        return await asyncio.to_thread(
            extract_parent_context,
            skeleton_code,
            ancestor_context,
            self.api_key,
            self.locale,
        )

    def _build_static_node_queries(self, node: StatementNode) -> List[str]:
        """정적 노드 생성 쿼리 리스트를 반환합니다."""
        queries: List[str] = []
        label = node.node_type
        
        # name 속성 결정
        if label == "FILE":
            node_name = self.file_name
        elif label in PROCEDURE_TYPES and node.unit_name:
            node_name = node.unit_name
        else:
            node_name = f"{label}[{node.start_line}]"
        
        escaped_name = escape_for_cypher(node_name)
        has_children = "true" if node.has_children else "false"
        escaped_code = escape_for_cypher(node.code)
        
        base_set = [
            f"__cy_n__.endLine = {node.end_line}",
            f"__cy_n__.name = '{escaped_name}'",
            f"__cy_n__.node_code = '{escaped_code}'",
            f"__cy_n__.token = {node.token}",
            f"__cy_n__.has_children = {has_children}",
        ]
        
        # AST JSON 메타데이터 속성 추가 (있는 경우만)
        if node.signature:
            base_set.append(f"__cy_n__.signature = '{escape_for_cypher(node.signature)}'")
        if node.parameters:
            base_set.append(f"__cy_n__.parameters = '{escape_for_cypher(node.parameters)}'")
        
        # PROCEDURE/FUNCTION: procedure_name, schema_name, procedure_type 속성 추가
        if label in PROCEDURE_TYPES and node.unit_name:
            base_set.append(f"__cy_n__.procedure_name = '{escape_for_cypher(node.unit_name)}'")
            base_set.append(f"__cy_n__.procedure_type = '{label}'")
            if node.schema_name:
                base_set.append(f"__cy_n__.schema_name = '{escape_for_cypher(node.schema_name)}'")
        elif node.unit_name:
            base_set.append(f"__cy_n__.procedure_name = '{escape_for_cypher(node.unit_name)}'")
            if node.schema_name:
                base_set.append(f"__cy_n__.schema_name = '{escape_for_cypher(node.schema_name)}'")
        
        if node.has_children:
            escaped_placeholder = escape_for_cypher(node.get_placeholder_code())
            base_set.append(f"__cy_n__.summarized_code = '{escaped_placeholder}'")
        
        base_set_str = ", ".join(base_set)
        
        # PROCEDURE/FUNCTION 노드: MERGE로 생성 (중복 방지)
        if label in PROCEDURE_TYPES and node.unit_name:
            escaped_proc_name = escape_for_cypher(node.unit_name)
            escaped_schema = escape_for_cypher(node.schema_name or "")
            schema_match = f"schema_name: '{escaped_schema}', " if node.schema_name else ""
            queries.append(
                f"MERGE (__cy_n__:{label} {{{schema_match}procedure_name: '{escaped_proc_name}'}})\n"
                f"SET __cy_n__.startLine = {node.start_line}, __cy_n__.directory = '{escape_for_cypher(self.full_directory)}', __cy_n__.file_name = '{self.file_name}', {base_set_str}\n"
                f"RETURN __cy_n__"
            )
        else:
            queries.append(
                f"MERGE (__cy_n__:{label} {{startLine: {node.start_line}, {self.node_base_props}}})\n"
                f"SET {base_set_str}\n"
                f"RETURN __cy_n__"
            )
        return queries

    def _build_relationship_queries(self) -> List[str]:
        """정적 관계 쿼리 (CONTAINS, PARENT_OF, NEXT)를 생성합니다."""
        queries: List[str] = []
        
        for node in self._nodes or []:
            for child in node.children:
                if node.node_type == "FILE" and child.node_type in PROCEDURE_TYPES:
                    queries.append(self._build_contains_query(node, child))
                else:
                    queries.append(self._build_parent_of_query(node, child))
            
            # NEXT 관계
            prev = None
            for child in node.children:
                if prev:
                    queries.append(self._build_next_query(prev, child))
                prev = child
        
        return queries
    
    def _build_next_query(self, prev: StatementNode, current: StatementNode) -> str:
        """NEXT 관계 쿼리"""
        return (
            f"MATCH (__cy_prev__:{prev.node_type} {{startLine: {prev.start_line}, {self.node_base_props}}})\n"
            f"MATCH (__cy_curr__:{current.node_type} {{startLine: {current.start_line}, {self.node_base_props}}})\n"
            f"MERGE (__cy_prev__)-[__cy_r__:NEXT]->(__cy_curr__)\n"
            f"RETURN __cy_r__"
        )

    async def _run_preprocessing(self) -> List[str]:
        """변수 선행 처리"""
        return await self._analyze_variable_nodes()

    async def _invoke_llm(self, batch: AnalysisBatch) -> Tuple[Optional[Dict[str, Any]], Optional[Dict[str, Any]]]:
        """LLM 호출 (일반 분석 + DML 분석)"""
        general_task = None
        if batch.ranges:
            code, context = batch.build_payload()
            general_task = asyncio.to_thread(
                analyze_code,
                code,
                context,
                batch.ranges,
                len(batch.ranges),
                self.api_key,
                self.locale,
            )

        table_task = None
        dml_payload = batch.build_dml_payload()
        if dml_payload and batch.dml_ranges:
            code, context = dml_payload
            table_task = asyncio.to_thread(
                analyze_dml_tables,
                code,
                context,
                batch.dml_ranges,
                self.api_key,
                self.locale,
            )

        if general_task and table_task:
            # asyncio.gather는 list를 반환하므로 tuple로 변환
            results = await asyncio.gather(general_task, table_task)
            return tuple(results)
        if general_task:
            return await general_task, None
        if table_task:
            return None, await table_task
        raise RuntimeError("LLM 분석 대상이 없습니다")

    def _build_analysis_queries(
        self, 
        batch: AnalysisBatch, 
        llm_result: Any,
        unit_summary_store: Optional[Dict[str, Dict[str, str]]] = None,
    ) -> List[str]:
        """LLM 분석 결과를 쿼리로 변환"""
        queries: List[str] = []
        
        # llm_result는 (general_result, table_result) 튜플이어야 함
        if not isinstance(llm_result, tuple):
            raise RuntimeError(f"배치#{batch.batch_id} llm_result가 tuple이 아님: {type(llm_result).__name__}")
        
        general_result, table_result = llm_result
        
        # 일반 분석 결과 처리 (None 허용 - 테이블 분석만 있는 경우)
        general_result = self.validate_dict_result(
            general_result, "general_result", batch.batch_id, allow_none=True
        )
        if general_result:  # 빈 dict이면 스킵
            analysis_list = general_result.get("analysis") or []
            for node, analysis in zip(batch.nodes, analysis_list):
                if not analysis:
                    continue
                
                # Summary 업데이트
                summary = analysis.get("summary") or ""
                if summary:
                    escaped_summary = escape_for_cypher(str(summary))
                    escaped_code = escape_for_cypher(node.code)
                    node_name = build_statement_name(node.node_type, node.start_line)
                    escaped_node_name = escape_for_cypher(node_name)
                    
                    queries.append(
                        f"MATCH (__cy_n__:{node.node_type} {{startLine: {node.start_line}, {self.node_base_props}}})\n"
                        f"SET __cy_n__.endLine = {node.end_line}, __cy_n__.name = '{escaped_node_name}', "
                        f"__cy_n__.summary = '{escaped_summary}', __cy_n__.node_code = '{escaped_code}', "
                        f"__cy_n__.token = {node.token}, __cy_n__.procedure_name = '{escape_for_cypher(node.unit_name or '')}', "
                        f"__cy_n__.has_children = {'true' if node.has_children else 'false'}\n"
                        f"RETURN __cy_n__"
                    )
                    
                    # 프로시저별 summary 저장
                    if unit_summary_store is not None and node.unit_key:
                        if node.unit_key in unit_summary_store:
                            key = f"{node.node_type}_{node.start_line}_{node.end_line}"
                            unit_summary_store[node.unit_key][key] = summary
                
                # CALL 관계 생성
                for call_name in analysis.get('calls', []) or []:
                    if '.' in call_name:
                        package_raw, proc_raw = call_name.split('.', 1)
                        package_name = escape_for_cypher(package_raw.strip())
                        proc_name = escape_for_cypher(proc_raw.strip())
                        queries.append(
                            f"MATCH (__cy_c__:{node.node_type} {{startLine: {node.start_line}, {self.node_base_props}}})\n"
                            f"MERGE (__cy_target__:PROCEDURE {{directory: '{package_name}', procedure_name: '{proc_name}'}})\n"
                            f"MERGE (__cy_c__)-[__cy_r__:CALL {{scope: 'external'}}]->(__cy_target__)\n"
                            f"RETURN __cy_c__, __cy_target__, __cy_r__"
                        )
                    else:
                        escaped_call = escape_for_cypher(call_name)
                        queries.append(
                            f"MATCH (__cy_c__:{node.node_type} {{startLine: {node.start_line}, {self.node_base_props}}})\n"
                            f"MATCH (__cy_p__ {{procedure_name: '{escaped_call}', {self.node_base_props}}})\n"
                            f"WHERE __cy_p__:PROCEDURE OR __cy_p__:FUNCTION\n"
                            f"MERGE (__cy_c__)-[__cy_r__:CALL {{scope: 'internal'}}]->(__cy_p__)\n"
                            f"RETURN __cy_c__, __cy_p__, __cy_r__"
                        )
                
                # 변수 사용 마킹
                for var_name in analysis.get('variables', []) or []:
                    queries.append(
                        f"MATCH (__cy_v__:Variable {{name: '{escape_for_cypher(var_name)}', {self.node_base_props}}})\n"
                        f"SET __cy_v__.`{node.start_line}_{node.end_line}` = 'Used'\n"
                        f"RETURN __cy_v__"
                    )
        
        # 테이블 분석 결과 처리 (None 허용 - 일반 분석만 있는 경우)
        table_result = self.validate_dict_result(
            table_result, "table_result", batch.batch_id, allow_none=True
        )
        if table_result:  # 빈 dict이면 스킵
            table_queries = self._build_table_queries(batch, table_result)
            queries.extend(table_queries)
        
        return queries

    async def _process_unit_summaries(
        self, 
        unit_summary_store: Dict[str, Dict[str, str]]
    ) -> List[str]:
        """프로시저별 summary 처리 + 테이블/컬럼 설명 보강"""
        queries: List[str] = []
        
        procedures = self._unit_info
        if not procedures:
            # 프로시저가 없어도 테이블/컬럼 설명 보강은 실행해야 함
            table_queries = await self._finalize_table_summaries()
            queries.extend(table_queries)
            return queries
        
        for proc_key, info in procedures.items():
            summaries = unit_summary_store.get(proc_key, {})
            if not summaries:
                continue
            
            # 프로시저 최상위 노드 찾기
            proc_root = next(
                (n for n in (self._nodes or []) 
                 if n.unit_key == proc_key and n.parent is None),
                None,
            )
            if proc_root and not proc_root.ok:
                log_process("ANALYZE", "SUMMARY", f"⚠️ {info.procedure_name}: 하위 분석 실패로 최종 summary 생성 스킵")
                continue
            
            # 청크 분할
            chunks = self._split_summaries_by_token(summaries, MAX_SUMMARY_CHUNK_TOKEN)
            if not chunks:
                continue
            
            log_process("ANALYZE", "SUMMARY", f"📦 {info.procedure_name}: summary 청크 분할 ({len(chunks)}개)")
            
            # 청크별 처리 (실패 시 예외 발생 → 전체 분석 중단)
            async def process_chunk(chunk: dict) -> str:
                result = await asyncio.to_thread(
                    analyze_summary_only, chunk, self.api_key, self.locale, ""
                )
                validated = self.validate_dict_result(result, "청크 분석")
                return validated.get('summary', '')
            
            chunk_results = await asyncio.gather(*[process_chunk(c) for c in chunks])
            chunk_results = [r for r in chunk_results if r]
            
            if not chunk_results:
                raise RuntimeError(f"{info.procedure_name}: 청크 처리 결과가 모두 비어있음")
            
            # 청크 통합
            if len(chunk_results) == 1:
                final_summary = chunk_results[0]
            else:
                combined = {f"CHUNK_{i+1}": s for i, s in enumerate(chunk_results)}
                result = await asyncio.to_thread(
                    analyze_summary_only, combined, self.api_key, self.locale, ""
                )
                validated = self.validate_dict_result(result, "청크 통합")
                final_summary = validated.get('summary') or "\n\n".join(chunk_results)
            
            log_process("ANALYZE", "SUMMARY", f"✅ {info.procedure_name}: summary 통합 완료")
            
            # User Story 생성 (실패 시 예외 발생)
            all_user_stories = []
            if final_summary:
                us_result = await asyncio.to_thread(
                    analyze_user_story, final_summary, self.api_key, self.locale
                )
                validated = self.validate_dict_result(us_result, "User Story")
                all_user_stories = validated.get('user_stories', []) or []
            
            # Neo4j 쿼리 생성
            summary_json = json.dumps(final_summary, ensure_ascii=False)
            queries.append(
                f"MATCH (__cy_n__:{info.procedure_type} {{procedure_name: '{escape_for_cypher(info.procedure_name)}', {self.node_base_props}}})\n"
                f"SET __cy_n__.summary = {summary_json}\n"
                f"RETURN __cy_n__"
            )
            
            # User Story 노드 생성
            proc_name_escaped = escape_for_cypher(info.procedure_name)
            for us_idx, us in enumerate(all_user_stories, 1):
                us_id = us.get('id', f"US-{us_idx}")
                role = escape_for_cypher(us.get('role', ''))
                goal = escape_for_cypher(us.get('goal', ''))
                benefit = escape_for_cypher(us.get('benefit', ''))
                
                queries.append(
                    f"MATCH (__cy_p__:{info.procedure_type} {{procedure_name: '{proc_name_escaped}', {self.node_base_props}}})\n"
                    f"MERGE (__cy_us__:UserStory {{id: '{us_id}', procedure_name: '{proc_name_escaped}', {self.node_base_props}}})\n"
                    f"SET __cy_us__.role = '{role}', __cy_us__.goal = '{goal}', __cy_us__.benefit = '{benefit}'\n"
                    f"MERGE (__cy_p__)-[__cy_r__:HAS_USER_STORY]->(__cy_us__)\n"
                    f"RETURN __cy_p__, __cy_us__, __cy_r__"
                )
                
                # Acceptance Criteria 노드
                for ac_idx, ac in enumerate(us.get('acceptance_criteria', []), 1):
                    if not isinstance(ac, dict):
                        continue
                    ac_id = ac.get('id', f"AC-{us_idx}-{ac_idx}")
                    ac_title = escape_for_cypher(ac.get('title', ''))
                    ac_given = json.dumps(ac.get('given', []), ensure_ascii=False)
                    ac_when = json.dumps(ac.get('when', []), ensure_ascii=False)
                    ac_then = json.dumps(ac.get('then', []), ensure_ascii=False)
                    
                    queries.append(
                        f"MATCH (__cy_us__:UserStory {{id: '{us_id}', {self.node_base_props}}})\n"
                        f"MERGE (__cy_ac__:AcceptanceCriteria {{id: '{ac_id}', user_story_id: '{us_id}', {self.node_base_props}}})\n"
                        f"SET __cy_ac__.title = '{ac_title}', __cy_ac__.given = {ac_given}, __cy_ac__.when = {ac_when}, __cy_ac__.then = {ac_then}\n"
                        f"MERGE (__cy_us__)-[__cy_r__:HAS_AC]->(__cy_ac__)\n"
                        f"RETURN __cy_us__, __cy_ac__, __cy_r__"
                    )
            
            us_count = len(all_user_stories)
            log_process("ANALYZE", "SUMMARY", f"✅ {info.procedure_name}: User Story {us_count}개 생성")
        
        # 테이블/컬럼 설명 요약 처리
        table_queries = await self._finalize_table_summaries()
        queries.extend(table_queries)
        
        return queries

    # =========================================================================
    # DBMS 전용 메서드
    # =========================================================================

    async def _analyze_variable_nodes(self) -> List[str]:
        """변수 선언 노드를 분석하고 쿼리를 생성합니다."""
        queries: List[str] = []
        variable_nodes = [n for n in (self._nodes or []) if n.node_type in VARIABLE_DECLARATION_TYPES]
        
        if not variable_nodes:
            return queries
        
        semaphore = asyncio.Semaphore(VARIABLE_CONCURRENCY)
        
        async def analyze_one(node: StatementNode) -> List[str]:
            """변수 분석 (실패 시 예외 발생 → 전체 분석 중단)"""
            async with semaphore:
                result = await asyncio.to_thread(
                    analyze_variables, node.code, self.api_key, self.locale
                )
                return self._build_variable_queries(node, result)
        
        results = await asyncio.gather(*[analyze_one(n) for n in variable_nodes])
        for r in results:
            queries.extend(r)
        
        return queries

    def _build_variable_queries(self, node: StatementNode, result: Dict[str, Any]) -> List[str]:
        """변수 분석 결과를 쿼리로 변환"""
        queries: List[str] = []
        
        if not isinstance(result, dict):
            raise RuntimeError(f"변수 분석 결과가 dict가 아님 (node={node.start_line}): {type(result).__name__}")
        
        variables = result.get("variables") or []
        if not variables:
            return queries
        
        node_match = f"MATCH (__cy_n__:{node.node_type} {{startLine: {node.start_line}, {self.node_base_props}}})"
        
        for var in variables:
            var_name = var.get("name", "")
            var_type = var.get("type", "")
            var_role = var.get("role", "")
            var_desc = var.get("description", "")
            
            if not var_name:
                continue
            
            escaped_name = escape_for_cypher(var_name)
            escaped_type = escape_for_cypher(var_type)
            escaped_role = escape_for_cypher(VARIABLE_ROLE_MAP.get(var_role, var_role))
            escaped_desc = escape_for_cypher(var_desc)
            
            queries.append(
                f"{node_match}\n"
                f"MERGE (__cy_v__:Variable {{name: '{escaped_name}', {self.node_base_props}}})\n"
                f"SET __cy_v__.type = '{escaped_type}', __cy_v__.role = '{escaped_role}', __cy_v__.description = '{escaped_desc}'\n"
                f"MERGE (__cy_n__)-[:DECLARES]->(__cy_v__)\n"
                f"RETURN __cy_v__"
            )
        
        return queries

    def _build_table_queries(
        self,
        batch: AnalysisBatch,
        table_result: Dict[str, Any]
    ) -> List[str]:
        """DML 테이블 분석 결과를 쿼리로 변환"""
        queries: List[str] = []
        node_map: Dict[Tuple[int, int], StatementNode] = {
            (node.start_line, node.end_line): node for node in batch.nodes
        }
        ranges = table_result.get('ranges', []) or []
        
        for range_entry in ranges:
            start_line = range_entry.get('startLine')
            end_line = range_entry.get('endLine')
            tables = range_entry.get('tables') or []
            
            try:
                start_line = int(start_line)
                end_line = int(end_line)
            except (TypeError, ValueError) as e:
                raise RuntimeError(f"LLM 테이블 분석 결과에 잘못된 라인 번호: startLine={range_entry.get('startLine')}, endLine={range_entry.get('endLine')}") from e
            
            node = node_map.get((start_line, end_line))
            if not node:
                continue
            
            node_merge = f"MATCH (__cy_n__:{node.node_type} {{startLine: {node.start_line}, {self.node_base_props}}})"
            
            # CREATE_TEMP_TABLE 처리
            if node.node_type == 'CREATE_TEMP_TABLE':
                for entry in tables:
                    table_name = (entry.get('table') or '').strip()
                    if not table_name:
                        continue
                    schema_part_raw, name_part_raw, _ = parse_table_identifier(table_name)
                    # 대소문자 변환 적용
                    schema_part = self._apply_name_case(schema_part_raw)
                    name_part = self._apply_name_case(name_part_raw)
                    queries.append(
                        f"{node_merge}\n"
                        f"SET __cy_n__:Table, __cy_n__.name = '{escape_for_cypher(name_part)}', "
                        f"__cy_n__.schema = '{escape_for_cypher(schema_part)}', __cy_n__.db = '{self.dbms}'\n"
                        f"RETURN __cy_n__"
                    )
                continue
            
            # 일반 DML 테이블 처리
            for entry in tables:
                table_name = (entry.get('table') or '').strip()
                if not table_name:
                    continue
                
                schema_part_raw, name_part_raw, db_link_value = parse_table_identifier(table_name)
                
                # DDL 캐시에서 원본 대소문자 조회
                # LLM이 반환한 테이블명이 DDL에 존재하면 DDL의 대소문자를 사용
                # 이렇게 하면 DDL 테이블과 동일한 노드에 업데이트됨
                effective_schema = schema_part_raw if schema_part_raw else self.default_schema
                ddl_lookup_key = (effective_schema.lower() if effective_schema else 'public', name_part_raw.lower())
                ddl_meta = self._ddl_table_metadata.get(ddl_lookup_key, {})
                
                # DDL 캐시에서 조회 성공 여부
                skip_case_conversion = False
                if ddl_meta and ddl_meta.get('original_name'):
                    # DDL에 존재하는 테이블: DDL의 원본 대소문자 사용
                    schema_part = ddl_meta.get('original_schema', self._apply_name_case(effective_schema or 'public'))
                    name_part = ddl_meta.get('original_name')
                    skip_case_conversion = True  # 이미 DDL에서 변환된 값이므로 다시 변환하지 않음
                else:
                    # DDL에 없는 테이블: name_case 변환 적용
                    schema_part = self._apply_name_case(schema_part_raw)
                    name_part = self._apply_name_case(name_part_raw)
                
                access_mode = (entry.get('accessMode') or entry.get('mode') or 'r').lower()
                rel_types = []
                if 'r' in access_mode:
                    rel_types.append(TABLE_RELATIONSHIP_MAP.get('r', 'FROM'))
                if 'w' in access_mode:
                    rel_types.append(TABLE_RELATIONSHIP_MAP.get('w', 'WRITES'))
                
                table_merge = self._build_table_merge(name_part, schema_part, preserve_vars=['__cy_n__'], skip_case_conversion=skip_case_conversion)
                
                table_desc_raw = entry.get('tableDescription') or entry.get('description') or ''
                bucket_key = self._record_table_summary(schema_part, name_part, table_desc_raw, skip_case_conversion=skip_case_conversion)
                
                table_query = f"{node_merge}\nWITH __cy_n__\n{table_merge}\nSET __cy_t__.db = coalesce(__cy_t__.db, '{self.dbms}')"
                
                if db_link_value:
                    table_query += f"\nSET __cy_t__.db_link = COALESCE(__cy_t__.db_link, '{db_link_value}')"
                
                for i, rel_type in enumerate(rel_types):
                    table_query += f"\nMERGE (__cy_n__)-[__cy_r{i}__:{rel_type}]->(__cy_t__)"
                
                table_query += "\nRETURN __cy_n__, __cy_t__"
                queries.append(table_query)
                
                # 컬럼 처리 (컬럼용은 preserve_vars=None으로 별도 생성)
                table_merge_for_column = self._build_table_merge(name_part, schema_part, preserve_vars=None, skip_case_conversion=skip_case_conversion)
                
                # DDL 컬럼 메타데이터 조회 (원본 대소문자 사용을 위해)
                ddl_columns = ddl_meta.get('columns', {}) if ddl_meta else {}
                
                for column in entry.get('columns', []) or []:
                    column_name_raw = (column.get('name') or '').strip()
                    if not column_name_raw:
                        continue
                    
                    # DDL 캐시에서 컬럼의 원본 대소문자 조회
                    # DDL 컬럼은 이미 name_case가 적용된 이름으로 저장됨
                    ddl_col_meta = ddl_columns.get(column_name_raw.upper() if self.name_case == 'uppercase' else column_name_raw)
                    if ddl_col_meta is None:
                        # 대소문자 무관하게 검색
                        for ddl_col_name in ddl_columns.keys():
                            if ddl_col_name.lower() == column_name_raw.lower():
                                column_name = ddl_col_name  # DDL의 원본 대소문자 사용
                                break
                        else:
                            column_name = self._apply_name_case(column_name_raw)
                    else:
                        # DDL에서 찾은 컬럼명 사용 (이미 변환됨)
                        column_name = column_name_raw.upper() if self.name_case == 'uppercase' else column_name_raw
                    
                    raw_dtype = column.get('dtype') or ''
                    raw_column_desc = (column.get('description') or column.get('comment') or '').strip()
                    
                    self._record_column_summary(
                        bucket_key,
                        column_name,
                        raw_column_desc,
                        dtype=raw_dtype,
                        nullable=column.get('nullable', True),
                        examples=column.get('examples') or [],
                    )
                    
                    col_type = escape_for_cypher(raw_dtype)
                    col_desc = escape_for_cypher(raw_column_desc)
                    nullable = 'true' if column.get('nullable', True) else 'false'
                    escaped_col_name = escape_for_cypher(column_name)
                    
                    # 대소문자 변환이 이미 _build_table_merge에서 적용됨
                    converted_name_part = self._apply_name_case(name_part)
                    converted_schema_part = self._apply_name_case(schema_part) if schema_part else None
                    
                    # 컬럼명에 특수문자가 있을 수 있으므로 모두 이스케이프
                    escaped_name_part = escape_for_cypher(converted_name_part)
                    escaped_column_name_for_fqn = escape_for_cypher(column_name)
                    
                    if converted_schema_part:
                        escaped_schema_part = escape_for_cypher(converted_schema_part)
                        fqn = escape_for_cypher('.'.join(filter(None, [converted_schema_part, converted_name_part, column_name])).lower())
                        # Column MERGE: fqn 기준 (고유키)
                        queries.append(
                            f"{table_merge_for_column}\nWITH __cy_t__\n"
                            f"MERGE (__cy_c__:Column {{fqn: '{fqn}'}})\n"
                            f"SET __cy_c__.name = '{escaped_col_name}', __cy_c__.dtype = '{col_type}', "
                            f"__cy_c__.description = '{col_desc}', __cy_c__.description_source = 'procedure', __cy_c__.nullable = '{nullable}'\n"
                            f"MERGE (__cy_t__)-[__cy_r__:HAS_COLUMN]->(__cy_c__)\n"
                            f"RETURN __cy_t__, __cy_c__, __cy_r__"
                        )
                    else:
                        # schema가 없는 경우 동적 fqn 계산 대신 정적 fqn 사용
                        # (CASE WHEN 구문은 컬럼명 특수문자로 인해 구문 오류 발생 가능)
                        fqn = escape_for_cypher('.'.join(filter(None, [converted_name_part, column_name])).lower())
                        # Column MERGE: fqn 기준
                        queries.append(
                            f"{table_merge_for_column}\nWITH __cy_t__\n"
                            f"MERGE (__cy_c__:Column {{fqn: '{fqn}'}})\n"
                            f"ON CREATE SET __cy_c__.name = '{escaped_col_name}', __cy_c__.dtype = '{col_type}', "
                            f"__cy_c__.description = '{col_desc}', __cy_c__.description_source = 'procedure', __cy_c__.nullable = '{nullable}'\n"
                            f"MERGE (__cy_t__)-[__cy_r__:HAS_COLUMN]->(__cy_c__)\n"
                            f"RETURN __cy_t__, __cy_c__, __cy_r__"
                        )
            
            # DBLink 처리
            for link_item in range_entry.get('dbLinks', []) or []:
                link_name_raw = (link_item.get('name') or '').strip()
                if not link_name_raw:
                    continue
                mode = escape_for_cypher((link_item.get('mode') or 'r').lower())
                schema_link_raw, name_link_raw, link_name = parse_table_identifier(link_name_raw)
                # 대소문자 변환 적용
                schema_link = self._apply_name_case(schema_link_raw)
                name_link = self._apply_name_case(name_link_raw)
                escaped_link_name = escape_for_cypher(link_name)
                remote_merge = self._build_table_merge(name_link, schema_link)
                queries.append(
                    f"{remote_merge}\nSET __cy_t__.db_link = '{escaped_link_name}'\n"
                    f"WITH __cy_t__\n"
                    f"MERGE (__cy_l__:DBLink {{name: '{escaped_link_name}'}})\n"
                    f"MERGE (__cy_l__)-[__cy_r1__:CONTAINS]->(__cy_t__)\n"
                    f"WITH __cy_t__, __cy_l__\n{node_merge}\n"
                    f"MERGE (__cy_n__)-[__cy_r2__:DB_LINK {{mode: '{mode}'}}]->(__cy_t__)\n"
                    f"RETURN __cy_l__, __cy_t__, __cy_n__"
                )
            
            # FK 관계 처리
            fk_relations = range_entry.get('fkRelations', []) or []
            for relation in fk_relations:
                src_table = (relation.get('sourceTable') or '').strip()
                tgt_table = (relation.get('targetTable') or '').strip()
                src_columns = [c.strip() for c in (relation.get('sourceColumns') or []) if c]
                tgt_columns = [c.strip() for c in (relation.get('targetColumns') or []) if c]
                
                if not (src_table and tgt_table and src_columns and tgt_columns):
                    continue
                
                src_schema_raw, src_name_raw, _ = parse_table_identifier(src_table)
                tgt_schema_raw, tgt_name_raw, _ = parse_table_identifier(tgt_table)
                
                # 대소문자 변환 적용
                src_schema = self._apply_name_case(src_schema_raw)
                src_name = self._apply_name_case(src_name_raw)
                tgt_schema = self._apply_name_case(tgt_schema_raw)
                tgt_name = self._apply_name_case(tgt_name_raw)
                
                # schema가 없으면 default_schema 사용 (테이블 생성과 일관성 유지)
                effective_src_schema = src_schema if src_schema else self._apply_name_case(self.default_schema)
                effective_tgt_schema = tgt_schema if tgt_schema else self._apply_name_case(self.default_schema)
                
                src_props = f"schema: '{escape_for_cypher(effective_src_schema)}', name: '{escape_for_cypher(src_name)}', db: '{self.dbms}'"
                tgt_props = f"schema: '{escape_for_cypher(effective_tgt_schema)}', name: '{escape_for_cypher(tgt_name)}', db: '{self.dbms}'"
                
                # 각 FK 매핑마다 별도의 FK_TO_TABLE 관계 생성
                # 속성: sourceColumn, targetColumn, type, source
                # source='procedure': 스토어드 프로시저 분석에서 추출 (점선 표시)
                for src_col, tgt_col in zip(src_columns, tgt_columns):
                    # 컬럼명에도 대소문자 변환 적용
                    escaped_src_col = escape_for_cypher(self._apply_name_case(src_col))
                    escaped_tgt_col = escape_for_cypher(self._apply_name_case(tgt_col))
                    
                    fk_query = (
                        f"MATCH (__cy_st__:Table {{{src_props}}})\n"
                        f"MATCH (__cy_tt__:Table {{{tgt_props}}})\n"
                        f"MERGE (__cy_st__)-[__cy_r__:FK_TO_TABLE {{sourceColumn: '{escaped_src_col}', targetColumn: '{escaped_tgt_col}'}}]->(__cy_tt__)\n"
                        f"ON CREATE SET __cy_r__.type = 'many_to_one', __cy_r__.source = 'procedure'\n"
                        f"RETURN __cy_st__, __cy_tt__, __cy_r__"
                    )
                    queries.append(fk_query)
                
                # Column 간 FK_TO 관계도 생성
                # source='procedure': 스토어드 프로시저 분석에서 추출 (점선 표시)
                for src_col, tgt_col in zip(src_columns, tgt_columns):
                    # 컬럼명에도 대소문자 변환 적용
                    converted_src_col = self._apply_name_case(src_col)
                    converted_tgt_col = self._apply_name_case(tgt_col)
                    # fqn 생성 시에도 effective_schema 사용 (테이블 생성과 일관성 유지)
                    src_fqn = escape_for_cypher('.'.join(filter(None, [effective_src_schema, src_name, converted_src_col])).lower())
                    tgt_fqn = escape_for_cypher('.'.join(filter(None, [effective_tgt_schema, tgt_name, converted_tgt_col])).lower())
                    fk_col_query = (
                        f"MATCH (__cy_sc__:Column {{fqn: '{src_fqn}'}})\n"
                        f"MATCH (__cy_dc__:Column {{fqn: '{tgt_fqn}'}})\n"
                        f"MERGE (__cy_sc__)-[__cy_r__:FK_TO]->(__cy_dc__)\n"
                        f"ON CREATE SET __cy_r__.source = 'procedure'\n"
                        f"RETURN __cy_sc__, __cy_dc__, __cy_r__"
                    )
                    queries.append(fk_col_query)
        
        return queries
    
    def _build_table_merge(self, table_name: str, schema: Optional[str], preserve_vars: Optional[List[str]] = None, skip_case_conversion: bool = False) -> str:
        """테이블 MERGE 쿼리 (Schema 노드 및 BELONGS_TO 관계 포함)
        
        DDL 처리와 일관성을 위해 schema가 없으면 default_schema 사용.
        default_schema도 없으면 'public' 사용.
        Schema 노드를 먼저 생성하고 Table이 Schema에 BELONGS_TO 관계로 연결됨.
        
        Args:
            table_name: 테이블 이름
            schema: 스키마 이름 (없으면 default_schema 사용)
            preserve_vars: WITH 절에서 유지할 변수 목록 (예: ['__cy_n__'] -> WITH __cy_n__, __cy_s__)
            skip_case_conversion: True면 대소문자 변환을 건너뜀 (이미 변환된 값인 경우)
        """
        # schema가 없으면 default_schema 사용, default_schema도 없으면 'public'
        effective_schema = schema if schema else (self.default_schema if self.default_schema else 'public')
        
        # 대소문자 변환 적용 (skip_case_conversion이 False인 경우에만)
        if not skip_case_conversion:
            effective_schema = self._apply_name_case(effective_schema)
            converted_table_name = self._apply_name_case(table_name)
        else:
            converted_table_name = table_name
        
        schema_value = escape_for_cypher(effective_schema)
        escaped_name = escape_for_cypher(converted_table_name)
        
        # WITH 절 구성: preserve_vars가 있으면 해당 변수들도 함께 유지
        if preserve_vars:
            with_vars = ", ".join(preserve_vars + ["__cy_s__"])
        else:
            with_vars = "__cy_s__"
        
        # Schema MERGE + Table MERGE + BELONGS_TO 관계
        # MERGE 키: db, schema, name만 사용 (같은 스키마/테이블명이면 같은 노드)
        return (
            f"MERGE (__cy_s__:Schema {{db: '{self.dbms}', name: '{schema_value}'}})\n"
            f"WITH {with_vars}\n"
            f"MERGE (__cy_t__:Table {{name: '{escaped_name}', schema: '{schema_value}', db: '{self.dbms}'}})\n"
            f"MERGE (__cy_t__)-[:BELONGS_TO]->(__cy_s__)"
        )

    def _record_table_summary(self, schema: Optional[str], name: str, description: Optional[str], skip_case_conversion: bool = False) -> Tuple[str, str]:
        """테이블 설명 누적
        
        테이블 생성 시 _build_table_merge에서 default_schema를 사용하므로,
        여기서도 동일하게 처리하여 MATCH 쿼리가 정확히 매칭되도록 함.
        
        중요: _apply_name_case를 적용하여 Neo4j에 저장된 테이블명과 일치시켜야 함.
        
        Args:
            schema: 스키마 이름
            name: 테이블 이름  
            description: 테이블 설명
            skip_case_conversion: True면 대소문자 변환을 건너뜀 (DDL 캐시에서 이미 변환된 값인 경우)
        """
        # 테이블 생성 시 schema 처리와 일관성 유지 (default_schema 사용)
        effective_schema = schema if schema else (self.default_schema if self.default_schema else 'public')
        
        # 대소문자 변환 적용 (skip_case_conversion이 False인 경우에만)
        if skip_case_conversion:
            schema_key = effective_schema
            name_key = name
        else:
            schema_key = self._apply_name_case(effective_schema)
            name_key = self._apply_name_case(name)
        
        bucket = self._table_summary_store.get((schema_key, name_key))
        if bucket is None:
            bucket = {"summaries": set(), "columns": {}}
            self._table_summary_store[(schema_key, name_key)] = bucket
        text = (description or '').strip()
        if text:
            bucket["summaries"].add(text)
        return (schema_key, name_key)
    
    def _record_column_summary(
        self,
        table_key: Tuple[str, str],
        column_name: str,
        description: Optional[str],
        dtype: Optional[str] = None,
        nullable: Optional[bool] = None,
        examples: Optional[List[str]] = None,
    ):
        """컬럼 설명 누적
        
        중요: 컬럼명에 _apply_name_case를 적용하여 Neo4j에 저장된 컬럼명과 일치시켜야 함.
        """
        text = (description or '').strip()
        bucket = self._table_summary_store.setdefault(table_key, {"summaries": set(), "columns": {}})
        columns = bucket["columns"]
        # 대소문자 변환 적용 (DDL 처리와 동일하게)
        canonical = self._apply_name_case(column_name)
        entry = columns.get(canonical)
        if entry is None:
            entry = {"name": canonical, "summaries": set(), "dtype": (dtype or ''), "nullable": True if nullable is None else bool(nullable), "examples": set()}
            columns[canonical] = entry
        if dtype is not None and not entry.get("dtype"):
            entry["dtype"] = dtype
        if nullable is not None:
            entry["nullable"] = bool(nullable)
        if text:
            entry["summaries"].add(text)
        if examples:
            for v in examples:
                if v is not None:
                    s = str(v).strip()
                    if s:
                        entry["examples"].add(s)
    
    async def _finalize_table_summaries(self) -> List[str]:
        """테이블/컬럼 설명 요약"""
        log_process("ANALYZE", "TABLE_SUMMARY", f"📊 테이블 요약 시작: {len(self._table_summary_store)}개 테이블")
        if not self._table_summary_store:
            log_process("ANALYZE", "TABLE_SUMMARY", "⚠️ 테이블 요약 대상 없음 (store 비어있음)")
            return []
        
        tasks = [
            self._summarize_table(table_key, data)
            for table_key, data in list(self._table_summary_store.items())
        ]
        if not tasks:
            return []
        
        results = await asyncio.gather(*tasks, return_exceptions=True)
        all_queries: List[str] = []
        for result in results:
            if isinstance(result, Exception):
                log_process("ANALYZE", "TABLE_SUMMARY", f"❌ 테이블 요약 오류: {result}", logging.ERROR)
            elif result:
                all_queries.extend(result)
        
        self._table_summary_store.clear()
        return all_queries
    
    async def _summarize_table(self, table_key: Tuple[str, str], data: Dict[str, Any]) -> List[str]:
        """테이블 요약 처리"""
        schema_key, name_key = table_key
        
        ddl_key = (schema_key.lower(), name_key.lower())
        ddl_meta = self._ddl_table_metadata.get(ddl_key, {})
        ddl_description = (ddl_meta.get('description') or '').strip()
        ddl_columns = ddl_meta.get('columns') or {}
        
        summaries = list(data.get('summaries') or [])
        columns_map = data.get('columns') or {}
        column_sentences = {
            entry['name']: list(entry['summaries'])
            for entry in columns_map.values()
            if entry.get('summaries')
        }
        
        if ddl_description:
            summaries.insert(0, f"[DDL 메타데이터] {ddl_description}")
        
        for col_name, ddl_col in ddl_columns.items():
            ddl_col_desc = (ddl_col.get('description') or '').strip()
            if ddl_col_desc and col_name not in column_sentences:
                column_sentences[col_name] = [f"[DDL 메타데이터] {ddl_col_desc}"]
            elif ddl_col_desc and col_name in column_sentences:
                column_sentences[col_name].insert(0, f"[DDL 메타데이터] {ddl_col_desc}")
        
        if not summaries and not column_sentences:
            return []
        
        table_display = f"{schema_key}.{name_key}" if schema_key else name_key
        column_metadata = {}
        for entry in columns_map.values():
            col_name = entry['name']
            ddl_col = ddl_columns.get(col_name, {})
            column_metadata[col_name] = {
                "dtype": entry.get("dtype") or ddl_col.get("dtype") or "",
                "nullable": bool(entry.get("nullable", True)) if entry.get("nullable") is not None else ddl_col.get("nullable", True),
                "examples": sorted(list(entry.get("examples") or []))[:5],
            }
        
        for col_name, ddl_col in ddl_columns.items():
            if col_name not in column_metadata:
                column_metadata[col_name] = {
                    "dtype": ddl_col.get("dtype") or "",
                    "nullable": ddl_col.get("nullable", True),
                    "examples": [],
                }
        
        result = await asyncio.to_thread(
            summarize_table_metadata,
            table_display,
            summaries,
            column_sentences,
            column_metadata,
            self.api_key,
            self.locale,
        )
        
        if not isinstance(result, dict):
            raise RuntimeError(f"테이블 요약 결과가 dict가 아님 ({schema_key}.{name_key}): {type(result).__name__}")
        
        queries: List[str] = []
        llm_table_desc = (result.get('tableDescription') or '').strip()
        escaped_schema = escape_for_cypher(schema_key)
        escaped_name = escape_for_cypher(name_key)
        # MATCH 조건: db, schema, name만 사용 (스키마/테이블명이 같으면 같은 노드로 취급)
        table_props = (
            f"schema: '{escaped_schema}', name: '{escaped_name}', db: '{self.dbms}'"
        )
        
        if llm_table_desc:
            escaped_llm_table_desc = escape_for_cypher(llm_table_desc)
            # 프로시저 분석 결과는 analyzed_description에 항상 저장
            # 기존 description이 비어있을 때만 description에도 저장 + description_source='procedure' 설정
            # description_source는 description이 비어있을 때만 'procedure'로 설정
            queries.append(
                f"MATCH (__cy_t__:Table {{{table_props}}})\n"
                f"SET __cy_t__.analyzed_description = '{escaped_llm_table_desc}'\n"
                f"WITH __cy_t__\n"
                f"WHERE __cy_t__.description IS NULL OR __cy_t__.description = ''\n"
                f"SET __cy_t__.description = '{escaped_llm_table_desc}', __cy_t__.description_source = 'procedure'\n"
                f"RETURN __cy_t__"
            )
        
        for column_info in result.get('columns', []) or []:
            column_name = (column_info.get('name') or '').strip()
            llm_column_desc = (column_info.get('description') or '').strip()
            if not column_name or not llm_column_desc:
                continue
            
            # fqn과 column_name 모두 이스케이프 필요 (특수문자 포함 가능)
            escaped_column_name = escape_for_cypher(column_name)
            fqn = '.'.join(filter(None, [schema_key, name_key, column_name])).lower()
            escaped_fqn = escape_for_cypher(fqn)
            # MATCH 조건: fqn 기준
            column_props = f"fqn: '{escaped_fqn}'"
            escaped_llm_column_desc = escape_for_cypher(llm_column_desc)
            # 프로시저 분석 결과는 analyzed_description에 항상 저장
            # 기존 description이 비어있을 때만 description에도 저장 + description_source='procedure' 설정
            queries.append(
                f"MATCH (__cy_c__:Column {{{column_props}}})\n"
                f"SET __cy_c__.analyzed_description = '{escaped_llm_column_desc}'\n"
                f"WITH __cy_c__\n"
                f"WHERE __cy_c__.description IS NULL OR __cy_c__.description = ''\n"
                f"SET __cy_c__.description = '{escaped_llm_column_desc}', __cy_c__.description_source = 'procedure'\n"
                f"RETURN __cy_c__"
            )
        
        return queries
