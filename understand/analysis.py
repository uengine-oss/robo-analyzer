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
from dataclasses import dataclass, field
from typing import Any, Dict, List, Optional, Tuple

from prompt.understand_prompt import understand_code
from prompt.understand_summarized_prompt import understand_summary
from prompt.understand_variables_prompt import understand_variables
from prompt.understand_dml_table_prompt import understand_dml_tables
from prompt.understand_table_summary_prompt import summarize_table_metadata
from util.exception import LLMCallError, ProcessAnalyzeCodeError, UnderstandingError
from util.utility_tool import calculate_code_token, escape_for_cypher, parse_table_identifier


# ==================== 상수 정의 ====================
PROCEDURE_TYPES = ("PROCEDURE", "FUNCTION", "CREATE_PROCEDURE_BODY", "TRIGGER")
NON_ANALYSIS_TYPES = frozenset(["CREATE_PROCEDURE_BODY", "FILE", "PROCEDURE", "FUNCTION", "DECLARE", "TRIGGER", "FOLDER", "SPEC"])
NON_NEXT_RECURSIVE_TYPES = frozenset(["FUNCTION", "PROCEDURE", "PACKAGE_VARIABLE", "TRIGGER"])
DML_STATEMENT_TYPES = frozenset(["SELECT", "INSERT", "UPDATE", "DELETE", "MERGE", "EXECUTE_IMMEDIATE", "FETCH"])
DML_RELATIONSHIP_MAP = {
    "SELECT": "FROM",
    "FETCH": "FROM",
    "UPDATE": "WRITES",
    "INSERT": "WRITES",
    "DELETE": "WRITES",
    "MERGE": "WRITES",
    "EXECUTE": "EXECUTE",
    "EXECUTE_IMMEDIATE": "EXECUTE",
}
VARIABLE_ROLE_MAP = {
    "PACKAGE_VARIABLE": "패키지 전역 변수",
    "DECLARE": "변수 선언및 초기화",
    "SPEC": "함수 및 프로시저 입력 매개변수",
}
VARIABLE_DECLARATION_TYPES = frozenset(["PACKAGE_VARIABLE", "DECLARE", "SPEC"])
STATIC_QUERY_BATCH_SIZE = 40
VARIABLE_CONCURRENCY = 5
LINE_NUMBER_PATTERN = re.compile(r"^\d+\s*:")
MAX_BATCH_TOKEN = 1000
MAX_CONCURRENCY = 5


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
                logging.info(
                    "[수집] 자식 요약 없음, 원문 유지 (부모 %s~%s → 자식 %s~%s)",
                    self.start_line,
                    self.end_line,
                    child.start_line,
                    child.end_line,
                )
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

            result_lines.append(f"{child.start_line}: ... code ...")

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
        return '\n\n'.join(node.get_raw_code() for node in dml_nodes)


@dataclass(slots=True)
class BatchResult:
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
    def __init__(self, antlr_data: Dict[str, Any], file_content: str, folder_name: str, file_name: str):
        """수집기에 필요한 AST 데이터와 파일 메타 정보를 초기화합니다."""
        self.antlr_data = antlr_data
        self.file_content = file_content
        self.folder_name = folder_name
        self.file_name = file_name
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
        start_line = node['startLine']
        end_line = node['endLine']
        node_type = node['type']
        children = node.get('children', []) or []

        child_nodes: List[StatementNode] = []
        procedure_key = current_proc
        procedure_type = current_type
        schema_name = current_schema

        # 라인 단위 원본 텍스트를 미리 잘라 둡니다 (compact code 생성 시 재사용)
        line_entries = [
            (line_no, self._file_lines[line_no - 1] if 0 <= line_no - 1 < len(self._file_lines) else '')
            for line_no in range(start_line, end_line + 1)
        ]
        code = '\n'.join(f"{line_no}: {text}" for line_no, text in line_entries)

        if node_type in PROCEDURE_TYPES:
            # 프로시저/함수 루트라면 이름/스키마를 추출하여 별도 버킷을 만듭니다.
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
                logging.info("[수집] 프로시저 선언: %s (라인 %s~%s)", proc_name_log, start_line, end_line)

        for child in children:
            child_node = self._visit(child, procedure_key, procedure_type, schema_name)
            if child_node is not None:
                child_nodes.append(child_node)

        analyzable = node_type not in NON_ANALYSIS_TYPES
        token = calculate_code_token(code)
        dml = node_type in DML_STATEMENT_TYPES
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

        if analyzable and procedure_key and procedure_key in self.procedures:
            self.procedures[procedure_key].pending_nodes += 1
        else:
            statement_node.completion_event.set()

        self.nodes.append(statement_node)
        logging.info(
            "[수집] %s 노드 처리 (라인 %s~%s, 토큰 %s, 자식 %s개)",
            node_type,
            start_line,
            end_line,
            token,
            len(child_nodes),
        )
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

            if node.has_children:
                # 부모 노드는 자식 요약이 모두 준비된 상태에서 단독으로 LLM에 전달합니다.
                if current_nodes:
                    logging.info(
                        "[배치] #%s 리프 %s개 확정 (토큰 %s/%s)",
                        batch_id,
                        len(current_nodes),
                        current_tokens,
                        self.token_limit,
                    )
                    batches.append(self._create_batch(batch_id, current_nodes))
                    batch_id += 1
                    current_nodes = []
                    current_tokens = 0

                logging.info(
                    "[배치] #%s 부모 노드 단독 실행 (라인 %s~%s, 토큰 %s)",
                    batch_id,
                    node.start_line,
                    node.end_line,
                    node.token,
                )
                batches.append(self._create_batch(batch_id, [node]))
                batch_id += 1
                continue

            if current_nodes and current_tokens + node.token > self.token_limit:
                # 토큰 한도를 초과하기 직전 배치를 확정합니다.
                logging.info(
                    "[배치] #%s 토큰 한도 도달, 먼저 실행 (누적 %s/%s)",
                    batch_id,
                    current_tokens,
                    self.token_limit,
                )
                batches.append(self._create_batch(batch_id, current_nodes))
                batch_id += 1
                current_nodes = []
                current_tokens = 0

            current_nodes.append(node)
            current_tokens += node.token

        if current_nodes:
            # 남아 있는 노드가 있으면 마무리 배치로 추가합니다.
            logging.info(
                "[배치] #%s 마지막 리프 %s개 확정 (토큰 %s/%s)",
                batch_id,
                len(current_nodes),
                current_tokens,
                self.token_limit,
            )
            batches.append(self._create_batch(batch_id, current_nodes))

        return batches

    def _create_batch(self, batch_id: int, nodes: List[StatementNode]) -> AnalysisBatch:
        """배치 ID와 노드 리스트로 AnalysisBatch 객체를 생성합니다."""
        ranges = [{"startLine": node.start_line, "endLine": node.end_line} for node in nodes]
        dml_ranges = [
            {"startLine": node.start_line, "endLine": node.end_line, "type": node.node_type}
            for node in nodes
            if node.dml
        ]
        progress_line = max(node.end_line for node in nodes)
        return AnalysisBatch(
            batch_id=batch_id,
            nodes=nodes,
            ranges=ranges,
            dml_ranges=dml_ranges,
            progress_line=progress_line,
        )


# ==================== LLM 호출 ====================
class LLMInvoker:
    """배치를 입력 받아 일반 요약/DML 메타 분석을 병렬 호출합니다."""
    def __init__(self, api_key: str, locale: str):
        """호출에 사용할 API 키와 로케일을 보관합니다."""
        self.api_key = api_key
        self.locale = locale

    async def invoke(self, batch: AnalysisBatch) -> Tuple[Optional[Dict[str, Any]], Optional[Dict[str, Any]]]:
        """배치에 포함된 범위를 일반 LLM/테이블 LLM에 각각 전달합니다."""
        general_task = None
        if batch.ranges:
            general_task = asyncio.to_thread(
                understand_code,
                batch.build_general_payload(),
                batch.ranges,
                len(batch.ranges),
                self.api_key,
                self.locale,
            )

        table_task = None
        dml_payload = batch.build_dml_payload()
        if dml_payload and batch.dml_ranges:
            table_task = asyncio.to_thread(
                understand_dml_tables,
                dml_payload,
                batch.dml_ranges,
                self.api_key,
                self.locale,
            )

        if general_task and table_task:
            return await asyncio.gather(general_task, table_task)
        if general_task:
            return await general_task, None
        if table_task:
            return None, await table_task
        return None, None


# ==================== 적용 매니저 ====================
class ApplyManager:
    """LLM 결과를 순서대로 적용하고, 요약/테이블 설명을 후처리합니다."""
    def __init__(
        self,
        node_base_props: str,
        folder_props: str,
        table_base_props: str,
        user_id: str,
        project_name: str,
        folder_name: str,
        file_name: str,
        dbms: str,
        api_key: str,
        locale: str,
        procedures: Dict[str, ProcedureInfo],
        send_queue: asyncio.Queue,
        receive_queue: asyncio.Queue,
        file_last_line: int,
    ):
        """Neo4j 반영 시 필요한 메타데이터와 동기화 큐를 초기화합니다."""
        self.node_base_props = node_base_props
        self.folder_props = folder_props
        self.table_base_props = table_base_props
        self.user_id = user_id
        self.project_name = project_name
        self.folder_name = folder_name
        self.file_name = file_name
        self.dbms = dbms
        self.api_key = api_key
        self.locale = locale
        self.procedures = procedures
        self.send_queue = send_queue
        self.receive_queue = receive_queue
        self.file_last_line = file_last_line
        self.folder_file = f"{folder_name}-{file_name}"

        self._pending: Dict[int, BatchResult] = {}
        self._summary_store: Dict[str, Dict[str, Any]] = {key: {} for key in procedures}
        self._next_batch_id = 1
        self._lock = asyncio.Lock()
        self._table_summary_store: Dict[Tuple[str, str], Dict[str, Any]] = {}

    async def submit(self, batch: AnalysisBatch, general: Optional[Dict[str, Any]], table: Optional[Dict[str, Any]]):
        """워커가 batch 처리를 마친 뒤 Apply 큐에 등록합니다."""
        async with self._lock:
            self._pending[batch.batch_id] = BatchResult(batch=batch, general_result=general, table_result=table)
            await self._flush_ready()

    async def finalize(self):
        """모든 배치가 적용된 후 프로시저/테이블 요약을 마무리합니다."""
        async with self._lock:
            await self._flush_ready(force=True)
        await self._finalize_remaining_procedures()
        await self._finalize_table_summaries()

    async def _flush_ready(self, force: bool = False):
        """배치 ID 순서대로 적용 가능 여부를 확인합니다."""
        while self._next_batch_id in self._pending:
            result = self._pending.pop(self._next_batch_id)
            await self._apply_batch(result)
            self._next_batch_id += 1

        if force and self._pending:
            for batch_id in sorted(self._pending):
                result = self._pending.pop(batch_id)
                await self._apply_batch(result)

    async def _apply_batch(self, result: BatchResult):
        """LLM 결과를 Neo4j 쿼리로 변환하고 요약 저장소를 업데이트합니다."""
        if not result.general_result:
            general_items: List[Dict[str, Any]] = []
        else:
            general_items = result.general_result.get('analysis', [])

        cypher_queries: List[str] = []
        summary_nodes = list(zip(result.batch.nodes, general_items))
        processed_nodes: set[int] = set()

        for node, analysis in summary_nodes:
            if not analysis:
                logging.info(
                    "[적용] 요약 없음, 건너뜀 (라인 %s~%s)",
                    node.start_line,
                    node.end_line,
                )
                node.completion_event.set()
                continue
            logging.info(
                "[적용] 요약 반영 (라인 %s~%s)",
                node.start_line,
                node.end_line,
            )
            # 요약 결과를 Neo4j 쿼리로 변환하고, 프로시저 요약 버킷에 기록합니다.
            cypher_queries.extend(self._build_node_queries(node, analysis))
            self._update_summary_store(node, analysis)
            processed_nodes.add(node.node_id)

        # LLM이 빈 결과를 주더라도 completion_event는 항상 set 됩니다.
        for node in result.batch.nodes:
            if node.node_id not in processed_nodes and node.completion_event.is_set() is False:
                node.completion_event.set()

        if result.table_result:
            cypher_queries.extend(self._build_table_queries(result.batch, result.table_result))

        if cypher_queries:
            logging.info(
                "[적용] %s 쿼리 전송 (%s개)",
                self.folder_file,
                len(cypher_queries),
            )
        await self._send_queries(cypher_queries, result.batch.progress_line)
        logging.info(
            "[적용] 배치 #%s 완료 (노드 %s개, 테이블 %s)",
            result.batch.batch_id,
            len(result.batch.nodes),
            '있음' if result.table_result else '없음',
        )

    def _build_node_queries(self, node: StatementNode, analysis: Dict[str, Any]) -> List[str]:
        """일반 노드 요약 결과를 Neo4j 쿼리 리스트로 변환합니다."""
        queries: List[str] = []
        summary_value = analysis.get('summary')
        summary = summary_value if isinstance(summary_value, str) else ''
        node.summary = summary if summary else None
        escaped_summary = escape_summary(summary)
        escaped_code = escape_for_cypher(node.code)
        node_name = build_statement_name(node.node_type, node.start_line)
        escaped_node_name = escape_for_cypher(node_name)

        # 자식이 있는 부모 노드는 LLM이 반환한 요약 문자열을 그대로 사용합니다.
        # 이미 `escape_summary`를 통해 JSON-safe 문자열이 만들어져 있으므로 추가 이스케이프 없이 사용합니다.
        escaped_summary_text = escaped_summary

        base_fields: List[str] = [
            f"n.endLine = {node.end_line}",
            f"n.name = '{escaped_node_name}'",
            f"n.summary = {escaped_summary_text}",
            f"n.node_code = '{escaped_code}'",
            f"n.token = {node.token}",
            f"n.procedure_name = '{escape_for_cypher(node.procedure_name or '')}'",
            f"n.has_children = {'true' if node.has_children else 'false'}",
        ]

        if node.has_children:
            escaped_placeholder = escape_for_cypher(node.get_placeholder_code())
            base_fields.append(f"n.summarized_code = '{escaped_placeholder}'")

        base_set = ", ".join(base_fields)

        queries.append(
            f"MERGE (n:{node.node_type} {{startLine: {node.start_line}, {self.node_base_props}}})\n"
            f"SET {base_set}\n"
            f"WITH n\n"
            f"MERGE (folder:SYSTEM {{{self.folder_props}}})\n"
            f"MERGE (folder)-[:CONTAINS]->(n)"
        )

        node.completion_event.set()

        for var_name in analysis.get('variables', []) or []:
            queries.append(
                f"MATCH (v:Variable {{name: '{escape_for_cypher(var_name)}', {self.node_base_props}}})\n"
                f"SET v.`{node.start_line}_{node.end_line}` = 'Used'"
            )

        for call_name in analysis.get('calls', []) or []:
            if '.' in call_name:
                package_name, proc_name = call_name.upper().split('.')
                queries.append(
                    f"MATCH (c:{node.node_type} {{startLine: {node.start_line}, {self.node_base_props}}})\n"
                    f"OPTIONAL MATCH (p)\n"
                    f"WHERE (p:PROCEDURE OR p:FUNCTION)\n"
                    f"  AND p.folder_name = '{package_name}'\n"
                    f"  AND p.procedure_name = '{proc_name}'\n"
                    f"  AND p.user_id = '{self.user_id}'\n"
                    f"WITH c, p\n"
                    f"FOREACH(_ IN CASE WHEN p IS NULL THEN [1] ELSE [] END |\n"
                    f"    CREATE (new:PROCEDURE:FUNCTION {{folder_name: '{package_name}', procedure_name: '{proc_name}', user_id: '{self.user_id}', project_name: '{self.project_name}'}})\n"
                    f"    MERGE (c)-[:CALL {{scope: 'external'}}]->(new))\n"
                    f"FOREACH(_ IN CASE WHEN p IS NOT NULL THEN [1] ELSE [] END |\n"
                    f"    MERGE (c)-[:CALL {{scope: 'external'}}]->(p))"
                )
            else:
                queries.append(
                    f"MATCH (c:{node.node_type} {{startLine: {node.start_line}, {self.node_base_props}}})\n"
                    f"WITH c\n"
                    f"MATCH (p {{procedure_name: '{escape_for_cypher(call_name)}', {self.node_base_props}}})\n"
                    f"WHERE p:PROCEDURE OR p:FUNCTION\n"
                    f"MERGE (c)-[:CALL {{scope: 'internal'}}]->(p)"
                )

        return queries

    def _build_table_queries(self, batch: AnalysisBatch, table_result: Dict[str, Any]) -> List[str]:
        """DML 테이블 분석 결과를 Neo4j 쿼리 리스트로 변환합니다."""
        queries: List[str] = []
        node_map: Dict[Tuple[int, int], StatementNode] = {
            (node.start_line, node.end_line): node for node in batch.nodes
        }
        for entry in table_result.get('tables', []) or []:
            start_line = entry.get('startLine')
            end_line = entry.get('endLine')
            if start_line is None or end_line is None:
                continue
            node = node_map.get((start_line, end_line))
            if not node:
                continue
            table_name = (entry.get('table') or '').strip().upper()
            if not table_name:
                continue

            relationship = DML_RELATIONSHIP_MAP.get((entry.get('dmlType') or '').upper())
            schema_part, name_part, db_link_value = parse_table_identifier(table_name)
            table_merge = self._build_table_merge(name_part, schema_part)
            node_merge = f"MERGE (n:{node.node_type} {{startLine: {node.start_line}, {self.node_base_props}}})"
            folder_merge = f"MERGE (folder:SYSTEM {{{self.folder_props}}})"

            bucket_key = self._record_table_summary(schema_part, name_part, entry.get('tableDescription'))

            # 1) 테이블 노드와 폴더 연결, DML 관계까지 설정
            base_table_query = (
                f"{node_merge}\n"
                f"WITH n\n"
                f"{table_merge}\n"
                f"ON CREATE SET t.folder_name = '{self.folder_name}'\n"
                f"ON MATCH SET t.folder_name = CASE WHEN coalesce(t.folder_name,'') = '' THEN '{self.folder_name}' ELSE t.folder_name END\n"
                f"WITH n, t\n"
                f"{folder_merge}\n"
                f"MERGE (folder)-[:CONTAINS]->(t)\n"
                f"SET t.db = coalesce(t.db, '{self.dbms}')"
            )

            if db_link_value:
                base_table_query += f"\nSET t.db_link = COALESCE(t.db_link, '{db_link_value}')"

            if relationship:
                base_table_query += f"\nMERGE (n)-[:{relationship}]->(t)"

            queries.append(base_table_query)

            # 2) 컬럼 노드 및 HAS_COLUMN 관계 생성
            for column in entry.get('columns', []) or []:
                column_name = (column.get('name') or '').strip()
                if not column_name:
                    continue
                col_type = escape_for_cypher(column.get('dtype') or '')
                raw_column_desc = (column.get('description') or column.get('comment') or '').strip()
                self._record_column_summary(bucket_key, column_name, raw_column_desc)
                col_description = escape_for_cypher(raw_column_desc)
                nullable_flag = 'true' if column.get('nullable', True) else 'false'
                fqn = '.'.join(filter(None, [schema_part, name_part, column_name])).lower()
                column_merge_key = (
                    f"`user_id`: '{self.user_id}', `fqn`: '{fqn}', `project_name`: '{self.project_name}'"
                )
                escaped_column_name = escape_for_cypher(column_name)
                queries.append(
                    f"{table_merge}\n"
                    f"ON CREATE SET t.folder_name = '{self.folder_name}'\n"
                    f"ON MATCH SET t.folder_name = CASE WHEN coalesce(t.folder_name,'') = '' THEN '{self.folder_name}' ELSE t.folder_name END\n"
                    f"WITH t\n"
                    f"MERGE (c:Column {{{column_merge_key}}})\n"
                    f"SET c.`name` = '{escaped_column_name}', c.`dtype` = '{col_type}', c.`description` = '{col_description}', c.`nullable` = '{nullable_flag}', c.`fqn` = '{fqn}'\n"
                    f"WITH t, c\n"
                    f"MERGE (t)-[:HAS_COLUMN]->(c)"
                )

            # 3) DB 링크 노드 연결
            for link_item in entry.get('dbLinks', []) or []:
                link_name_raw = (link_item.get('name') or '').strip().upper()
                if not link_name_raw:
                    continue
                mode = (link_item.get('mode') or 'r').lower()
                schema_link, name_link, link_name = parse_table_identifier(link_name_raw)
                remote_merge = self._build_table_merge(name_link, schema_link).replace(f", db: '{self.dbms}'", "")
                queries.append(
                    f"{remote_merge}\n"
                    f"ON CREATE SET t.folder_name = ''\n"
                    f"SET t.db_link = '{link_name}'\n"
                    f"WITH t\n"
                    f"MERGE (l:DBLink {{user_id: '{self.user_id}', name: '{link_name}', project_name: '{self.project_name}'}})\n"
                    f"MERGE (l)-[:CONTAINS]->(t)\n"
                    f"WITH t\n"
                    f"{node_merge}\n"
                    f"MERGE (n)-[:DB_LINK {{mode: '{mode}'}}]->(t)"
                )

            # 4) 외래키(테이블/컬럼) 관계 생성
            for relation in entry.get('fkRelations', []) or []:
                src_table = (relation.get('sourceTable') or '').strip().upper()
                tgt_table = (relation.get('targetTable') or '').strip().upper()
                src_column = (relation.get('sourceColumn') or '').strip()
                tgt_column = (relation.get('targetColumn') or '').strip()
                if not (src_table and tgt_table and src_column and tgt_column):
                    continue
                src_schema, src_table_name, _ = parse_table_identifier(src_table)
                tgt_schema, tgt_table_name, _ = parse_table_identifier(tgt_table)
                src_props = (
                    f"user_id: '{self.user_id}', schema: '{src_schema or ''}', name: '{src_table_name}', db: '{self.dbms}', project_name: '{self.project_name}'"
                )
                tgt_props = (
                    f"user_id: '{self.user_id}', schema: '{tgt_schema or ''}', name: '{tgt_table_name}', db: '{self.dbms}', project_name: '{self.project_name}'"
                )
                queries.append(
                    f"MATCH (st:Table {{{src_props}}})\n"
                    f"MATCH (tt:Table {{{tgt_props}}})\n"
                    f"MERGE (st)-[:FK_TO_TABLE]->(tt)"
                )
                src_fqn = '.'.join(filter(None, [src_schema, src_table_name, src_column])).lower()
                tgt_fqn = '.'.join(filter(None, [tgt_schema, tgt_table_name, tgt_column])).lower()
                queries.append(
                    f"MATCH (sc:Column {{user_id: '{self.user_id}', name: '{src_column}', fqn: '{src_fqn}'}})\n"
                    f"MATCH (dc:Column {{user_id: '{self.user_id}', name: '{tgt_column}', fqn: '{tgt_fqn}'}})\n"
                    f"MERGE (sc)-[:FK_TO]->(dc)"
                )

        return queries

    def _update_summary_store(self, node: StatementNode, analysis: Dict[str, Any]):
        """프로시저 요약 후보를 저장하고 완료된 노드 수를 갱신합니다."""
        if not node.procedure_key or node.procedure_key not in self.procedures:
            return
        summary_entry = analysis.get('summary')
        if summary_entry is None:
            return
        key = f"{node.node_type}_{node.start_line}_{node.end_line}"
        self._summary_store[node.procedure_key][key] = summary_entry
        info = self.procedures[node.procedure_key]
        if info.pending_nodes > 0:
            info.pending_nodes -= 1
        if info.pending_nodes == 0:
            asyncio.create_task(self._finalize_procedure_summary(info))

    async def _finalize_procedure_summary(self, info: ProcedureInfo):
        if info.key not in self._summary_store:
            return
        summaries = self._summary_store.pop(info.key, {})
        if not summaries:
            return
        try:
            summary_result = await asyncio.to_thread(understand_summary, summaries, self.api_key, self.locale)
        except Exception as exc:  # pragma: no cover - defensive
            logging.error("프로시저 요약 생성 중 오류: %s", exc)
            return

        summary_value = summary_result.get('summary') if isinstance(summary_result, dict) else None
        if summary_value is None:
            return

        summary_json = json.dumps(summary_value, ensure_ascii=False)
        query = (
            f"MATCH (n:{info.procedure_type} {{procedure_name: '{escape_for_cypher(info.procedure_name)}', {self.node_base_props}}})\n"
            f"SET n.summary = {summary_json}"
        )
        await self._send_queries([query], info.end_line)
        logging.info(
            "[적용] 프로시저 요약 완료: %s (%s)",
            info.procedure_name,
            self.folder_file,
        )

    async def _finalize_remaining_procedures(self):
        """아직 요약이 남아 있는 프로시저가 있다면 마지막으로 처리합니다."""
        for key, info in list(self.procedures.items()):
            if info.pending_nodes == 0 and key in self._summary_store and self._summary_store[key]:
                await self._finalize_procedure_summary(info)

    async def _send_queries(self, queries: List[str], progress_line: int):
        """분석 큐에 쿼리를 전달하고 처리가 끝날 때까지 대기합니다."""
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
        logging.info("[적용] %s Neo4j 저장 완료", self.folder_name)

    def _build_table_merge(self, table_name: str, schema: Optional[str]) -> str:
        schema_part = f", schema: '{schema}'" if schema else ""
        return (
            f"MERGE (t:Table {{{self.table_base_props}, name: '{table_name}'{schema_part}, db: '{self.dbms}', project_name: '{self.project_name}'}})"
        )

    def _record_table_summary(self, schema: Optional[str], name: str, description: Optional[str]) -> Tuple[str, str]:
        """테이블 설명 문장을 버킷에 누적합니다."""
        schema_key = (schema or '').upper()
        name_key = name.upper()
        bucket = self._table_summary_store.get((schema_key, name_key))
        if bucket is None:
            bucket = {"summaries": set(), "columns": {}}
            self._table_summary_store[(schema_key, name_key)] = bucket
        text = (description or '').strip()
        if text:
            bucket["summaries"].add(text)
        return (schema_key, name_key)

    def _record_column_summary(self, table_key: Tuple[str, str], column_name: str, description: Optional[str]):
        """컬럼 설명 문장을 버킷에 누적합니다."""
        text = (description or '').strip()
        if not text:
            return
        bucket = self._table_summary_store.setdefault(table_key, {"summaries": set(), "columns": {}})
        columns = bucket["columns"]
        canonical = column_name.upper()
        entry = columns.get(canonical)
        if entry is None:
            entry = {"name": column_name, "summaries": set()}
            columns[canonical] = entry
        entry["summaries"].add(text)

    async def _finalize_table_summaries(self):
        """버킷에 모은 테이블/컬럼 설명을 병렬로 요약합니다."""
        if not self._table_summary_store:
            return
        tasks = [
            self._summarize_table(table_key, data)
            for table_key, data in list(self._table_summary_store.items())
        ]
        if tasks:
            await asyncio.gather(*tasks)
        self._table_summary_store.clear()

    async def _summarize_table(self, table_key: Tuple[str, str], data: Dict[str, Any]):
        """테이블/컬럼 설명 버킷을 기반으로 LLM 요약을 생성합니다."""
        schema_key, name_key = table_key
        summaries = list(data.get('summaries') or [])
        columns_map = data.get('columns') or {}
        column_sentences = {
            entry['name']: list(entry['summaries'])
            for entry in columns_map.values()
            if entry.get('summaries')
        }
        if not summaries and not column_sentences:
            return

        table_display = f"{schema_key}.{name_key}" if schema_key else name_key
        result = await asyncio.to_thread(
            summarize_table_metadata,
            table_display,
            summaries,
            column_sentences,
            self.api_key,
            self.locale,
        )

        if not isinstance(result, dict):
            return

        queries: List[str] = []
        table_desc = (result.get('tableDescription') or '').strip()
        schema_prop = schema_key
        table_props = (
            f"user_id: '{self.user_id}', schema: '{schema_prop}', name: '{name_key}', db: '{self.dbms}', project_name: '{self.project_name}'"
        )

        if table_desc:
            # 테이블 설명을 최신 요약으로 덮어씁니다.
            queries.append(
                f"MATCH (t:Table {{{table_props}}})\nSET t.description = '{escape_for_cypher(table_desc)}'"
            )

        for column_info in result.get('columns', []) or []:
            column_name = (column_info.get('name') or '').strip()
            column_desc = (column_info.get('description') or '').strip()
            if not column_name or not column_desc:
                continue
            fqn = '.'.join(filter(None, [schema_prop, name_key, column_name])).lower()
            column_props = (
                f"user_id: '{self.user_id}', name: '{column_name}', fqn: '{fqn}', project_name: '{self.project_name}'"
            )
            queries.append(
                # 컬럼 역할 설명을 최종 요약으로 갱신합니다.
                f"MATCH (c:Column {{{column_props}}})\nSET c.description = '{escape_for_cypher(column_desc)}'"
            )

        if queries:
            await self._send_queries(queries, self.file_last_line)


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
        await self._create_static_nodes(nodes)
        await self._create_relationships(nodes)
        await self._process_variable_nodes(nodes)

    async def _create_static_nodes(self, nodes: List[StatementNode]):
        """각 StatementNode에 대응하는 기본 노드를 Neo4j에 생성합니다."""
        queries: List[str] = []
        for node in nodes:
            queries.extend(self._build_static_node_queries(node))
            if len(queries) >= STATIC_QUERY_BATCH_SIZE:
                await self._send_static_queries(queries, node.end_line)
                queries.clear()
        if queries:
            await self._send_static_queries(queries, nodes[-1].end_line)

    def _build_static_node_queries(self, node: StatementNode) -> List[str]:
        """정적 노드 생성을 위한 Cypher 쿼리 리스트를 반환합니다."""
        queries: List[str] = []
        label = node.node_type
        node_name = self.file_name if label == "FILE" else build_statement_name(label, node.start_line)
        escaped_name = escape_for_cypher(node_name)
        has_children = 'true' if node.has_children else 'false'
        procedure_name = escape_for_cypher(node.procedure_name or '')

        if not node.children and label not in NON_ANALYSIS_TYPES:
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
                queries.append(self._build_parent_relationship_query(node, child))
                if len(queries) >= STATIC_QUERY_BATCH_SIZE:
                    await self._send_static_queries(queries, child.end_line)
                    queries.clear()

            prev_node: Optional[StatementNode] = None
            for child in node.children:
                if prev_node and prev_node.node_type not in NON_NEXT_RECURSIVE_TYPES:
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

    async def _process_variable_nodes(self, nodes: List[StatementNode]):
        """변수 선언 노드를 병렬로 분석하여 Variable 노드와 연결합니다."""
        targets = [node for node in nodes if node.node_type in VARIABLE_DECLARATION_TYPES]
        if not targets:
            return

        proc_labels = sorted({node.procedure_name or "" for node in targets})
        if proc_labels:
            label_text = ', '.join(label for label in proc_labels if label) or '익명 프로시저'
            logging.info(
                "[변수] 변수 분석 시작: %s (%s)",
                label_text,
                self.folder_file,
            )

        semaphore = asyncio.Semaphore(VARIABLE_CONCURRENCY)

        async def worker(node: StatementNode):
            async with semaphore:
                try:
                    result = await asyncio.to_thread(
                        understand_variables,
                        node.get_raw_code(),
                        self.api_key,
                        self.locale,
                    )
                except Exception as exc:  # pragma: no cover - defensive logging
                    logging.error("변수 선언 분석 중 오류: %s", exc)
                    return

                queries = self._build_variable_queries(node, result)
                if queries:
                    await self._send_static_queries(queries, node.end_line)

        await asyncio.gather(*(worker(node) for node in targets))
        if proc_labels:
            logging.info(
                "[변수] 변수 분석 완료: %s (%s)",
                label_text,
                self.folder_file,
            )

    def _build_variable_queries(self, node: StatementNode, analysis: Dict[str, Any]) -> List[str]:
        """변수 분석 결과를 Neo4j 쿼리로 변환합니다."""
        if not isinstance(analysis, dict):
            return []

        variables = analysis.get("variables") or []
        summary_payload = analysis.get("summary")
        summary_json = json.dumps(summary_payload if summary_payload is not None else "", ensure_ascii=False)

        role = VARIABLE_ROLE_MAP.get(node.node_type, "알 수 없는 매개변수")
        scope = "Global" if node.node_type == "PACKAGE_VARIABLE" else "Local"

        node_props = self.node_base_props
        folder_props = self.folder_props
        procedure_name = escape_for_cypher(node.procedure_name or '')

        if node.node_type == "PACKAGE_VARIABLE":
            node_match = f"startLine: {node.start_line}, {node_props}"
            base_var_props = f"{node_props}, role: '{role}', scope: '{scope}'"
        else:
            node_match = f"startLine: {node.start_line}, procedure_name: '{procedure_name}', {node_props}"
            base_var_props = f"{node_props}, procedure_name: '{procedure_name}', role: '{role}', scope: '{scope}'"

        queries: List[str] = []
        queries.append(
            f"MATCH (p:{node.node_type} {{{node_match}}})\nSET p.summary = {summary_json}"
        )

        for variable in variables:
            name_raw = (variable.get("name") or '').strip()
            if not name_raw:
                continue

            name = escape_for_cypher(name_raw)
            var_type = escape_for_cypher(variable.get("type") or '')
            param_type = escape_for_cypher(variable.get("parameter_type") or '')
            value_json = json.dumps(variable.get("value") if variable.get("value") is not None else "", ensure_ascii=False)

            queries.append(
                f"MERGE (v:Variable {{name: '{name}', {base_var_props}, type: '{var_type}', parameter_type: '{param_type}', value: {value_json}}})\n"
                f"WITH v\n"
                f"MATCH (p:{node.node_type} {{{node_match}}})\n"
                f"MERGE (p)-[:SCOPE]->(v)\n"
                f"WITH v\n"
                f"MERGE (folder:SYSTEM {{{folder_props}}})\n"
                f"MERGE (folder)-[:CONTAINS]->(v)"
            )

        return queries

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
        logging.info("[진행] %s 분석 시작 (총 %s줄)", self.folder_file, self.last_line)
        try:
            collector = StatementCollector(self.antlr_data, self.file_content, self.folder_name, self.file_name)
            nodes, procedures = collector.collect()
            await self._initialize_static_graph(nodes)
            planner = BatchPlanner()
            batches = planner.plan(nodes, self.folder_file)

            if not batches:
                # 분석할 노드가 없다면 즉시 종료 이벤트만 전송합니다.
                await self.send_queue.put({"type": "end_analysis"})
                return

            # 1) LLM 워커 / 2) 적용 관리자 준비
            invoker = LLMInvoker(self.api_key, self.locale)
            apply_manager = ApplyManager(
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

            semaphore = asyncio.Semaphore(min(self.max_workers, len(batches)))

            async def worker(batch: AnalysisBatch):
                # 부모 노드가 포함된 배치라면 자식 완료를 기다립니다.
                await self._wait_for_dependencies(batch)
                async with semaphore:
                    logging.info(
                        "[LLM] 배치 #%s 요청 (노드 %s개, %s)",
                        batch.batch_id,
                        len(batch.nodes),
                        self.folder_file,
                    )
                    general, table = await invoker.invoke(batch)
                await apply_manager.submit(batch, general, table)

            await asyncio.gather(*(worker(batch) for batch in batches))
            # 모든 배치 제출이 끝나면 요약/테이블 설명 후처리를 마무리합니다.
            await apply_manager.finalize()

            logging.info("[진행] %s 분석 완료", self.folder_file)
            await self.send_queue.put({"type": "end_analysis"})

        except (UnderstandingError, LLMCallError) as exc:
            logging.error("Understanding 오류: %s", exc)
            await self.send_queue.put({'type': 'error', 'message': str(exc)})
            raise
        except Exception as exc:
            err_msg = f"Understanding 과정에서 오류가 발생했습니다: {exc}"
            logging.exception(err_msg)
            await self.send_queue.put({'type': 'error', 'message': err_msg})
            raise ProcessAnalyzeCodeError(err_msg)

    async def _wait_for_dependencies(self, batch: AnalysisBatch):
        """부모 배치가 실행되기 전에 자식 노드 요약이 모두 완료되었는지 확인합니다."""
        # 부모 노드가 LLM에 전달되기 전 자식 요약이 모두 끝났는지 확인합니다.
        waiters = []
        for node in batch.nodes:
            for child in node.children:
                if child.analyzable:
                    waiters.append(child.completion_event.wait())
        if waiters:
            logging.info(
                "[대기] 배치 #%s 자식 요약 대기 (%s개)",
                batch.batch_id,
                len(waiters),
            )
            await asyncio.gather(*waiters)

