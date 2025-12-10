"""리팩터링된 Understanding 파이프라인의 핵심 구현.

이 모듈은 AST 수집, 배치 계획, 병렬 LLM 호출, Neo4j 반영까지의 전 과정을
비동기 파이프라인으로 구성한다. 함수마다 docstring을 제공하여 흐름을
처음 접하는 개발자도 전체 단계와 데이터 이동을 빠르게 파악할 수 있도록 한다.
"""

from __future__ import annotations

import asyncio
import json
import logging
import re
import os
from dataclasses import dataclass, field
from typing import Any, Dict, List, Optional, Tuple

from understand.rules import understand_code
from util.exception import LLMCallError, ProcessAnalyzeCodeError, UnderstandingError
from util.utility_tool import calculate_code_token, escape_for_cypher, log_process
from understand.strategy.base_strategy import UnderstandingStrategy
from understand.strategy.dbms_strategy import DbmsUnderstandingStrategy


# ==================== 상수 정의 ====================
STATIC_QUERY_BATCH_SIZE = 40
VARIABLE_CONCURRENCY = int(os.getenv('VARIABLE_CONCURRENCY', '5'))
LINE_NUMBER_PATTERN = re.compile(r"^\d+\s*:")
MAX_BATCH_TOKEN = 1000
MAX_CONCURRENCY = int(os.getenv('MAX_CONCURRENCY', '5'))

# ==================== 데이터 클래스 ====================
@dataclass(slots=True)
class StatementNode:
    """평탄화된 AST 노드를 표현합니다.

    - 수집 단계에서 모든 노드를 생성합니다.
    - 이후 배치가 만들어질 때 이 객체를 그대로 사용합니다.
    - LLM 요약이 끝나면 `summary`와 `completion_event`가 채워집니다.
    """
    node_id: int
    start_line: int
    end_line: int
    node_type: str
    code: str
    token: int
    has_children: bool
    procedure_key: Optional[str]
    procedure_type: Optional[str]
    procedure_name: Optional[str]
    schema_name: Optional[str]
    analyzable: bool
    dml: bool
    lines: List[Tuple[int, str]] = field(default_factory=list)
    parent: Optional[StatementNode] = None
    children: List[StatementNode] = field(default_factory=list)
    summary: Optional[str] = None
    completion_event: asyncio.Event = field(init=False, repr=False)

    def __post_init__(self):
        object.__setattr__(self, "completion_event", asyncio.Event())

    def get_raw_code(self) -> str:
        """라인 번호를 포함하여 노드의 원문 코드를 반환합니다."""
        return '\n'.join(f"{line_no}: {text}" for line_no, text in self.lines)

    def get_compact_code(self) -> str:
        """자식 요약을 포함한 부모 코드(LLM 입력용)를 생성합니다."""
        if not self.children:
            return self.code

        result_lines: List[str] = []
        line_index = 0
        total_lines = len(self.lines)
        sorted_children = sorted(self.children, key=lambda child: child.start_line)

        for child in sorted_children:
            # 자식 이전의 부모 고유 코드를 그대로 복사합니다.
            while line_index < total_lines and self.lines[line_index][0] < child.start_line:
                line_no, text = self.lines[line_index]
                result_lines.append(f"{line_no}: {text}")
                line_index += 1

            # 자식 구간은 자식 요약으로 대체합니다 (없으면 기본 placeholder).
            if child.summary:
                child_summary = child.summary.strip()
                summary_line = f"{child.start_line}~{child.end_line}: {child_summary}"
            else:
                log_process("UNDERSTAND", "COLLECT", f"⚠️ 부모 {self.start_line}~{self.end_line}의 자식 {child.start_line}~{child.end_line} 요약 없음 - 원문 보관")
                summary_line = '\n'.join(
                    f"{line_no}: {text}"
                    for line_no, text in child.lines
                ).strip()

            result_lines.append(summary_line)

            # 자식 구간 원본 코드는 건너뜁니다.
            while line_index < total_lines and self.lines[line_index][0] <= child.end_line:
                line_index += 1

        # 마지막 자식 이후 부모 코드가 남아 있다면 추가합니다.
        while line_index < total_lines:
            line_no, text = self.lines[line_index]
            result_lines.append(f"{line_no}: {text}")
            line_index += 1

        return '\n'.join(result_lines)

    def get_placeholder_code(self) -> str:
        """자식 구간을 placeholder로 유지한 코드를 반환합니다."""
        if not self.children:
            return self.code

        result_lines: List[str] = []
        line_index = 0
        total_lines = len(self.lines)
        sorted_children = sorted(self.children, key=lambda child: child.start_line)

        for child in sorted_children:
            while line_index < total_lines and self.lines[line_index][0] < child.start_line:
                line_no, text = self.lines[line_index]
                result_lines.append(f"{line_no}: {text}")
                line_index += 1

            result_lines.append(f"{child.start_line}: ...code...")

            while line_index < total_lines and self.lines[line_index][0] <= child.end_line:
                line_index += 1

        while line_index < total_lines:
            line_no, text = self.lines[line_index]
            result_lines.append(f"{line_no}: {text}")
            line_index += 1

        return '\n'.join(result_lines)


@dataclass(slots=True)
class ProcedureInfo:
    key: str
    procedure_type: str
    procedure_name: str
    schema_name: Optional[str]
    start_line: int
    end_line: int
    pending_nodes: int = 0


@dataclass(slots=True)
class AnalysisBatch:
    batch_id: int
    nodes: List[StatementNode]
    ranges: List[Dict[str, int]]
    dml_ranges: List[Dict[str, int]]
    progress_line: int

    def build_general_payload(self) -> str:
        """일반 LLM 호출용으로 노드들의 compact 코드를 결합합니다."""
        return '\n\n'.join(node.get_compact_code() for node in self.nodes)

    def build_dml_payload(self) -> Optional[str]:
        """DML 노드만 추린 원문 코드를 결합하여 테이블 분석 프롬프트에 전달합니다."""
        dml_nodes = [node for node in self.nodes if node.dml]
        if not dml_nodes:
            return None
        return '\n\n'.join(
            node.get_compact_code() if node.has_children else node.get_raw_code()
            for node in dml_nodes
        )


@dataclass(slots=True)
class BatchResult:
    """LLM 호출 결과를 배치 단위로 보관하는 단순 컨테이너."""
    batch: AnalysisBatch
    general_result: Optional[Dict[str, Any]]
    table_result: Optional[Dict[str, Any]]


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


def get_original_node_code(file_content: str, start_line: int, end_line: int) -> str:
    """파일 전체 문자열에서 특정 구간을 라인 번호와 함께 잘라 반환합니다."""
    lines = file_content.split('\n')[start_line - 1:end_line]
    result: List[str] = []
    for index, line in enumerate(lines, start=start_line):
        if LINE_NUMBER_PATTERN.match(line):
            result.append(line)
        else:
            result.append(f"{index}: {line}")
    return '\n'.join(result)


def build_statement_name(node_type: str, start_line: int) -> str:
    """노드 타입과 시작 라인을 조합한 식별자 문자열을 생성합니다."""
    return f"{node_type}[{start_line}]"


def escape_summary(summary: str) -> str:
    """LLM 요약 문자열을 JSON-safe 형태로 변환합니다."""
    return json.dumps(summary)


# ==================== 노드 수집기 ====================
class StatementCollector:
    """AST를 후위순회하여 `StatementNode`와 프로시저 정보를 수집합니다."""
    def __init__(
        self,
        antlr_data: Dict[str, Any],
        file_content: str,
        folder_name: str,
        file_name: str,
        statement_kinds: Dict[str, Any],
    ):
        """수집기에 필요한 AST 데이터, 파일 메타, 구문 정의를 초기화합니다."""
        self.antlr_data = antlr_data
        self.file_content = file_content
        self.folder_name = folder_name
        self.file_name = file_name
        self.procedure_types = statement_kinds["procedure_types"]
        self.non_analysis_types = statement_kinds["non_analysis_types"]
        self.non_next_recursive_types = statement_kinds["non_next_recursive_types"]
        self.dml_statement_types = statement_kinds["dml_statement_types"]
        self.variable_role_map = statement_kinds["variable_role_map"]
        self.variable_declaration_types = statement_kinds["variable_declaration_types"]
        self.nodes: List[StatementNode] = []
        self.procedures: Dict[str, ProcedureInfo] = {}
        self._node_id = 0
        self._file_lines = file_content.split('\n')

    def collect(self) -> Tuple[List[StatementNode], Dict[str, ProcedureInfo]]:
        """AST 전역을 후위 순회하여 노드 목록과 프로시저 정보를 생성합니다."""
        # 루트 노드부터 후위순회합니다 (자식 → 부모 순서 보장)
        self._visit(self.antlr_data, current_proc=None, current_type=None, current_schema=None)
        return self.nodes, self.procedures

    def _make_proc_key(self, procedure_name: Optional[str], start_line: int) -> str:
        """프로시저 고유키를 생성합니다."""
        base = procedure_name or f"anonymous_{start_line}"
        return f"{self.folder_name}:{self.file_name}:{base}:{start_line}"

    def _visit(
        self,
        node: Dict[str, Any],
        current_proc: Optional[str],
        current_type: Optional[str],
        current_schema: Optional[str],
    ) -> Optional[StatementNode]:
        """재귀적으로 AST를 내려가며 StatementNode를 생성하고 부모-자식 관계를 구축합니다."""
        # 각 노드의 기본 메타데이터를 확보합니다.
        start_line = node['startLine']
        end_line = node['endLine']
        node_type = node['type']
        children = node.get('children', []) or []

        child_nodes: List[StatementNode] = []
        procedure_key = current_proc
        procedure_type = current_type
        schema_name = current_schema

        # LLM 입력 및 요약 생성에 활용할 원본 코드를 라인 단위로 준비합니다.
        line_entries = [
            (line_no, self._file_lines[line_no - 1] if 0 <= line_no - 1 < len(self._file_lines) else '')
            for line_no in range(start_line, end_line + 1)
        ]
        code = '\n'.join(f"{line_no}: {text}" for line_no, text in line_entries)

        if node_type in self.procedure_types:
            # 프로시저/함수 루트라면 이름/스키마를 추출하여 별도 버킷을 만듭니다.
            # 생성된 procedure_key는 하위 노드와 요약 결과를 묶는 기준 키로 사용됩니다.
            schema_candidate, name_candidate = get_procedure_name_from_code(code)
            procedure_key = self._make_proc_key(name_candidate, start_line)
            procedure_type = node_type
            schema_name = schema_candidate
            if procedure_key not in self.procedures:
                self.procedures[procedure_key] = ProcedureInfo(
                    key=procedure_key,
                    procedure_type=node_type,
                    procedure_name=name_candidate or procedure_key,
                    schema_name=schema_candidate,
                    start_line=start_line,
                    end_line=end_line,
                )
                proc_name_log = name_candidate or procedure_key
                log_process("UNDERSTAND", "COLLECT", f"📋 프로시저 선언 발견: {proc_name_log} (라인 {start_line}~{end_line})")

        for child in children:
            child_node = self._visit(child, procedure_key, procedure_type, schema_name)
            if child_node is not None:
                child_nodes.append(child_node)

        # 후속 단계에서 활용할 분석 가능 여부 및 토큰 정보를 계산합니다.
        analyzable = node_type not in self.non_analysis_types
        token = calculate_code_token(code)
        dml = node_type in self.dml_statement_types
        has_children = bool(child_nodes)

        self._node_id += 1
        statement_node = StatementNode(
            node_id=self._node_id,
            start_line=start_line,
            end_line=end_line,
            node_type=node_type,
            code=code,
            token=token,
            has_children=has_children,
            procedure_key=procedure_key,
            procedure_type=procedure_type,
            procedure_name=self.procedures.get(procedure_key).procedure_name if procedure_key in self.procedures else None,
            schema_name=schema_name,
            analyzable=analyzable,
            dml=dml,
            lines=line_entries,
        )
        for child_node in child_nodes:
            child_node.parent = statement_node
        statement_node.children.extend(child_nodes)

        # 프로시저 요약 완료 시점을 판별하기 위해 pending 노드 수를 추적합니다.
        if analyzable and procedure_key and procedure_key in self.procedures:
            self.procedures[procedure_key].pending_nodes += 1
        else:
            statement_node.completion_event.set()

        self.nodes.append(statement_node)
        log_process("UNDERSTAND", "COLLECT", f"✅ {node_type} 노드 수집 완료: 라인 {start_line}~{end_line}, 토큰 {token}, 자식 {len(child_nodes)}개")
        return statement_node


# ==================== 배치 플래너 ====================
class BatchPlanner:
    """수집된 노드를 토큰 한도 내에서 배치로 묶습니다."""
    def __init__(self, token_limit: int = MAX_BATCH_TOKEN):
        """토큰 한도를 지정하여 배치 생성기를 초기화합니다."""
        self.token_limit = token_limit

    def plan(self, nodes: List[StatementNode], folder_file: str) -> List[AnalysisBatch]:
        """토큰 한도를 넘지 않도록 노드를 분할하여 분석 배치를 생성합니다."""
        batches: List[AnalysisBatch] = []
        current_nodes: List[StatementNode] = []
        current_tokens = 0
        batch_id = 1

        for node in nodes:
            if not node.analyzable:
                continue

            # 부모 노드는 자식 요약이 준비된 후 단독으로 실행되므로 즉시 배치를 확정합니다.
            if node.has_children:
                # 부모 노드는 자식 요약이 모두 준비된 상태에서 단독으로 LLM에 전달합니다.
                if current_nodes:
                    # 현재까지 누적된 리프 배치를 먼저 확정합니다.
                    log_process("UNDERSTAND", "BATCH", f"📦 [leaf] 배치 #{batch_id} 확정: 리프 노드 {len(current_nodes)}개 (토큰 {current_tokens}/{self.token_limit})")
                    batches.append(self._create_batch(batch_id, current_nodes))
                    batch_id += 1
                    current_nodes = []
                    current_tokens = 0

                log_process("UNDERSTAND", "BATCH", f"📦 [parent] 배치 #{batch_id} 확정: 부모 노드 단독 실행 (라인 {node.start_line}~{node.end_line}, 토큰 {node.token})")
                batches.append(self._create_batch(batch_id, [node]))
                batch_id += 1
                continue

            # 현재 배치가 토큰 한도를 초과한다면 쌓인 리프 노드들을 먼저 실행합니다.
            if current_nodes and current_tokens + node.token > self.token_limit:
                # 토큰 한도를 초과하기 직전 배치를 확정합니다.
                log_process("UNDERSTAND", "BATCH", f"📦 [leaf] 배치 #{batch_id} 확정: 토큰 한도 도달로 선 실행 (누적 {current_tokens}/{self.token_limit})")
                batches.append(self._create_batch(batch_id, current_nodes))
                batch_id += 1
                current_nodes = []
                current_tokens = 0

            current_nodes.append(node)
            current_tokens += node.token

        if current_nodes:
            # 남아 있는 노드가 있으면 마무리 배치로 추가합니다.
            log_process("UNDERSTAND", "BATCH", f"📦 [leaf] 배치 #{batch_id} 확정: 마지막 리프 노드 {len(current_nodes)}개 (토큰 {current_tokens}/{self.token_limit})")
            batches.append(self._create_batch(batch_id, current_nodes))

        return batches

    def _create_batch(self, batch_id: int, nodes: List[StatementNode]) -> AnalysisBatch:
        """배치 ID와 노드 리스트로 AnalysisBatch 객체를 생성합니다."""
        # LLM 호출과 진행률 표시를 위해 범위 정보를 미리 계산합니다.
        ranges = [{"startLine": node.start_line, "endLine": node.end_line} for node in nodes]
        dml_ranges = [
            {"startLine": node.start_line, "endLine": node.end_line, "type": node.node_type}
            for node in nodes
            if node.dml
        ]
        # 진행률 표시는 배치 내 가장 마지막 라인 기준으로 업데이트합니다.
        progress_line = max(node.end_line for node in nodes)
        return AnalysisBatch(
            batch_id=batch_id,
            nodes=nodes,
            ranges=ranges,
            dml_ranges=dml_ranges,
            progress_line=progress_line,
        )


# ==================== Analyzer 본체 ====================
class Analyzer:
    """Understanding 파이프라인의 엔트리 포인트.

    1. AST를 평탄화(`StatementCollector`).
    2. 토큰 기준으로 배치를 생성(`BatchPlanner`).
    3. LLM 워커를 통해 병렬 분석(`LLMInvoker`).
    4. 결과를 순차 적용하고 요약(`ApplyManager`).
    """
    def __init__(
        self,
        antlr_data: dict,
        file_content: str,
        send_queue: asyncio.Queue,
        receive_queue: asyncio.Queue,
        last_line: int,
        folder_name: str,
        file_name: str,
        user_id: str,
        api_key: str,
        locale: str,
        dbms: str,
        project_name: str,
        strategy: Optional[UnderstandingStrategy] = None,
    ):
        """Analyzer가 파일 분석에 필요한 모든 컨텍스트를 초기화합니다."""
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
        self.strategy = strategy or DbmsUnderstandingStrategy()

        # 전략으로부터 구문 정의를 받아 공통 구성에 저장
        kinds = self.strategy.statement_rules()
        self.procedure_types = kinds["procedure_types"]
        self.non_analysis_types = kinds["non_analysis_types"]
        self.non_next_recursive_types = kinds["non_next_recursive_types"]
        self.dml_statement_types = kinds["dml_statement_types"]
        self.variable_role_map = kinds["variable_role_map"]
        self.variable_declaration_types = kinds["variable_declaration_types"]
        self.variable_concurrency = VARIABLE_CONCURRENCY

        self.folder_file = f"{folder_name}-{file_name}"
        self.node_base_props = (
            f"folder_name: '{folder_name}', file_name: '{file_name}', user_id: '{user_id}', project_name: '{self.project_name}'"
        )
        self.folder_props = (
            f"user_id: '{user_id}', name: '{folder_name}', project_name: '{self.project_name}'"
        )
        self.table_base_props = f"user_id: '{user_id}'"
        self.max_workers = MAX_CONCURRENCY

    async def _initialize_static_graph(self, nodes: List[StatementNode]):
        """파일 분석 전에 정적 노드/관계를 생성합니다."""
        if not nodes:
            return
        # 1) 노드 본문을 Neo4j에 미리 생성하고
        await self._create_static_nodes(nodes)
        # 2) 부모/형제 관계를 선반영하며
        await self._create_relationships(nodes)
        # 3) 변수 선언은 별도 프롬프트로 병렬 처리합니다.
        await self.strategy.process_variables(self, nodes)

    async def _create_static_nodes(self, nodes: List[StatementNode]):
        """각 StatementNode에 대응하는 기본 노드를 Neo4j에 생성합니다."""
        queries: List[str] = []
        for node in nodes:
            # StatementNode 단위로 MERGE 쿼리 묶음을 생성합니다.
            queries.extend(self._build_static_node_queries(node))
            if len(queries) >= STATIC_QUERY_BATCH_SIZE:
                # 일정량이 쌓이면 즉시 전송하여 큐를 비웁니다.
                await self._send_static_queries(queries, node.end_line)
                queries.clear()
        if queries:
            # 마지막 남은 쿼리 묶음도 전송합니다.
            await self._send_static_queries(queries, nodes[-1].end_line)

    def _build_static_node_queries(self, node: StatementNode) -> List[str]:
        """정적 노드 생성을 위한 Cypher 쿼리 리스트를 반환합니다."""
        queries: List[str] = []
        label = node.node_type
        node_name = self.file_name if label == "FILE" else build_statement_name(label, node.start_line)
        escaped_name = escape_for_cypher(node_name)
        has_children = 'true' if node.has_children else 'false'
        procedure_name = escape_for_cypher(node.procedure_name or '')

        if not node.children and label not in self.non_analysis_types:
            # 리프 노드이면서 분석 대상이면 요약 전 node_code를 포함해 저장합니다.
            escaped_code = escape_for_cypher(node.code)
            queries.append(
                f"MERGE (n:{label} {{startLine: {node.start_line}, {self.node_base_props}}})\n"
                f"SET n.endLine = {node.end_line}, n.name = '{escaped_name}', n.node_code = '{escaped_code}',\n"
                f"    n.token = {node.token}, n.procedure_name = '{procedure_name}', n.has_children = {has_children}\n"
                f"WITH n\n"
                f"MERGE (folder:SYSTEM {{{self.folder_props}}})\n"
                f"MERGE (folder)-[:CONTAINS]->(n)"
            )
            return queries

        escaped_code = escape_for_cypher(node.code)

        if label == "FILE":
            file_summary = 'File Start Node' if self.locale == 'en' else '파일 노드'
            queries.append(
                f"MERGE (n:{label} {{startLine: {node.start_line}, {self.node_base_props}}})\n"
                f"SET n.endLine = {node.end_line}, n.name = '{self.file_name}', n.summary = '{escape_for_cypher(file_summary)}',\n"
                f"    n.has_children = {has_children}\n"
                f"WITH n\n"
                f"MERGE (folder:SYSTEM {{{self.folder_props}}})\n"
                f"MERGE (folder)-[:CONTAINS]->(n)"
            )
        else:
            placeholder_fragment = ""
            if node.has_children:
                # 부모 노드는 summarized_code를 미리 기록해 둡니다.
                escaped_placeholder = escape_for_cypher(node.get_placeholder_code())
                placeholder_fragment = f", n.summarized_code = '{escaped_placeholder}'"
            queries.append(
                f"MERGE (n:{label} {{startLine: {node.start_line}, {self.node_base_props}}})\n"
                f"SET n.endLine = {node.end_line}, n.name = '{escaped_name}'{placeholder_fragment},\n"
                f"    n.node_code = '{escaped_code}', n.token = {node.token}, n.procedure_name = '{procedure_name}', n.has_children = {has_children}\n"
                f"WITH n\n"
                f"MERGE (folder:SYSTEM {{{self.folder_props}}})\n"
                f"MERGE (folder)-[:CONTAINS]->(n)"
            )
        return queries

    async def _create_relationships(self, nodes: List[StatementNode]):
        """PARENT_OF / NEXT 관계를 생성합니다."""
        queries: List[str] = []
        for node in nodes:
            for child in node.children:
                # 부모-자식 구조를 유지하기 위한 관계를 생성합니다.
                queries.append(self._build_parent_relationship_query(node, child))
                if len(queries) >= STATIC_QUERY_BATCH_SIZE:
                    await self._send_static_queries(queries, child.end_line)
                    queries.clear()

            prev_node: Optional[StatementNode] = None
            for child in node.children:
                if prev_node and prev_node.node_type not in self.non_next_recursive_types:
                    # 동일 부모 아래 형제 노드 간 순서를 NEXT 관계로 기록합니다.
                    queries.append(self._build_next_relationship_query(prev_node, child))
                    if len(queries) >= STATIC_QUERY_BATCH_SIZE:
                        await self._send_static_queries(queries, child.end_line)
                        queries.clear()
                prev_node = child

        if queries:
            await self._send_static_queries(queries, nodes[-1].end_line)

    def _build_parent_relationship_query(self, parent: StatementNode, child: StatementNode) -> str:
        """부모와 자식 노드 사이의 PARENT_OF 관계 쿼리를 작성합니다."""
        parent_match = f"MATCH (parent:{parent.node_type} {{startLine: {parent.start_line}, {self.node_base_props}}})"
        child_match = f"MATCH (child:{child.node_type} {{startLine: {child.start_line}, {self.node_base_props}}})"
        return f"{parent_match}\n{child_match}\nMERGE (parent)-[:PARENT_OF]->(child)"

    def _build_next_relationship_query(self, prev_node: StatementNode, current_node: StatementNode) -> str:
        """형제 노드 사이의 NEXT 관계 쿼리를 작성합니다."""
        prev_match = f"MATCH (prev:{prev_node.node_type} {{startLine: {prev_node.start_line}, {self.node_base_props}}})"
        curr_match = f"MATCH (current:{current_node.node_type} {{startLine: {current_node.start_line}, {self.node_base_props}}})"
        return f"{prev_match}\n{curr_match}\nMERGE (prev)-[:NEXT]->(current)"

    async def _send_static_queries(self, queries: List[str], progress_line: int):
        """정적 그래프 초기화 쿼리를 큐로 전송하고 완료 시까지 기다립니다."""
        if not queries:
            return
        await self.send_queue.put({
            "type": "analysis_code",
            "query_data": queries,
            "line_number": progress_line,
        })
        while True:
            response = await self.receive_queue.get()
            if response.get('type') == 'process_completed':
                break

    async def run(self):
        """파일 단위 Understanding 파이프라인을 실행합니다."""
        log_process("UNDERSTAND", "START", f"🚀 {self.folder_file} 분석 시작 (총 {self.last_line}줄)")
        try:
            collector = StatementCollector(
                self.antlr_data,
                self.file_content,
                self.folder_name,
                self.file_name,
                {
                    "procedure_types": self.procedure_types,
                    "non_analysis_types": self.non_analysis_types,
                    "non_next_recursive_types": self.non_next_recursive_types,
                    "dml_statement_types": self.dml_statement_types,
                    "variable_role_map": self.variable_role_map,
                    "variable_declaration_types": self.variable_declaration_types,
                },
            )
            # 1) AST를 평탄화하여 StatementNode 목록을 얻습니다.
            nodes, procedures = collector.collect()
            # 2) 분석 전 Neo4j에 정적 구조를 초기화합니다.
            await self._initialize_static_graph(nodes)
            # 2-1) 전략 실행 컨텍스트 주입 (Neo4j 쿼리 생성에 필요한 메타 포함)
            self.strategy.prepare_context(
                node_base_props=self.node_base_props,
                folder_props=self.folder_props,
                table_base_props=self.table_base_props,
                user_id=self.user_id,
                project_name=self.project_name,
                folder_name=self.folder_name,
                file_name=self.file_name,
                dbms=self.dbms,
                api_key=self.api_key,
                locale=self.locale,
                procedures=procedures,
                send_queue=self.send_queue,
                receive_queue=self.receive_queue,
                file_last_line=self.last_line,
            )

            planner = BatchPlanner()
            # 3) 노드를 토큰 기준으로 배치 단위로 분할합니다.
            batches = planner.plan(nodes, self.folder_file)

            if not batches:
                # 분석할 노드가 없다면 즉시 종료 이벤트만 전송합니다.
                await self.send_queue.put({"type": "end_analysis"})
                return

            semaphore = asyncio.Semaphore(min(self.max_workers, len(batches)))

            async def worker(batch: AnalysisBatch):
                # 부모 노드가 포함된 배치라면 자식 완료를 기다립니다.
                await self._wait_for_dependencies(batch)
                batch_kind = "parent" if any(n.has_children for n in batch.nodes) else "leaf"
                async with semaphore:
                    log_process("UNDERSTAND", "LLM", f"🤖 [{batch_kind}] 배치 #{batch.batch_id} LLM 요청: 노드 {len(batch.nodes)}개 ({self.folder_file})")
                    # LLM 호출은 일반 요약과 테이블 요약을 동시에 요청합니다.
                    general, table = await self.strategy.invoke_batch(batch)
                await self.strategy.apply_batch(batch, general, table)

            await asyncio.gather(*(worker(batch) for batch in batches))
            # 모든 배치 제출이 끝나면 요약/테이블 설명 후처리를 마무리합니다.
            await self.strategy.finalize()

            log_process("UNDERSTAND", "DONE", f"✅ {self.folder_file} 분석 완료")
            await self.send_queue.put({"type": "end_analysis"})

        except (UnderstandingError, LLMCallError) as exc:
            log_process("UNDERSTAND", "ERROR", "❌ Understanding 파이프라인에서 예외 발생", logging.ERROR, exc)
            await self.send_queue.put({'type': 'error', 'message': str(exc)})
            raise
        except Exception as exc:
            err_msg = f"Understanding 과정에서 오류가 발생했습니다: {exc}"
            log_process("UNDERSTAND", "ERROR", f"❌ {err_msg}", logging.ERROR, exc)
            await self.send_queue.put({'type': 'error', 'message': err_msg})
            raise ProcessAnalyzeCodeError(err_msg)

    async def _wait_for_dependencies(self, batch: AnalysisBatch):
        """부모 배치가 실행되기 전에 자식 노드 요약이 모두 완료되었는지 확인합니다."""
        # 부모 노드가 LLM에 전달되기 전 자식 요약이 모두 끝났는지 확인합니다.
        waiters = []
        for node in batch.nodes:
            for child in node.children:
                if child.analyzable:
                    # 자식 노드의 completion_event를 모아 비동기적으로 대기합니다.
                    waiters.append(child.completion_event.wait())
        if waiters:
            log_process("UNDERSTAND", "WAIT", f"⏳ [parent] 배치 #{batch.batch_id}: 자식 {len(waiters)}개 요약 완료 대기")
            await asyncio.gather(*waiters)

