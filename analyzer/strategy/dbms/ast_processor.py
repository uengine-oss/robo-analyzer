"""DBMS 코드 분석기 - PL/SQL AST → Neo4j 그래프

프로시저/함수 분석에 필요한 정보를 추출합니다.

분석 파이프라인:
1. AST 수집 (StatementCollector)
2. 정적 그래프 생성 (PROCEDURE, FUNCTION 노드)
3. DML 문 분석 (테이블/컬럼 관계)
4. LLM 배치 분석 (요약, 변수 타입)
5. 프로시저 요약 및 User Story 생성
"""

from __future__ import annotations

import asyncio
import json
import logging
import re
from dataclasses import dataclass, field
from typing import Any, Dict, List, Optional, Tuple

from config.settings import settings
from util.rule_loader import RuleLoader
from util.exception import LLMCallError, CodeProcessError, AnalysisError
from util.utility_tool import calculate_code_token, escape_for_cypher, parse_table_identifier, log_process


# ==================== 상수 정의 ====================
# 노드 타입 분류
PROCEDURE_TYPES = ("PROCEDURE", "FUNCTION", "CREATE_PROCEDURE_BODY", "TRIGGER")
NON_ANALYSIS_TYPES = frozenset(["CREATE_PROCEDURE_BODY", "FILE", "PROCEDURE", "FUNCTION", "DECLARE", "TRIGGER", "SPEC"])
NON_NEXT_RECURSIVE_TYPES = frozenset(["FUNCTION", "PROCEDURE", "PACKAGE_VARIABLE", "TRIGGER"])
DML_STATEMENT_TYPES = frozenset(["SELECT", "INSERT", "UPDATE", "DELETE", "MERGE", "EXECUTE_IMMEDIATE", "FETCH", "CREATE_TEMP_TABLE", "CTE", "OPEN_CURSOR"])
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

# 정규식 패턴
LINE_NUMBER_PATTERN = re.compile(r"^\d+\s*:")


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
                log_process("ANALYZE", "COLLECT", f"⚠️ 부모 {self.start_line}~{self.end_line}의 자식 {child.start_line}~{child.end_line} 요약 없음 - 원문 보관")
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


# ==================== RuleLoader 헬퍼 ====================
def _rule_loader() -> RuleLoader:
    return RuleLoader(target_lang="dbms")


def analyze_code(code: str, ranges: list, count: int, api_key: str, locale: str) -> Dict[str, Any]:
    return _rule_loader().execute(
        "analysis",
        {"code": code, "ranges": ranges, "count": count, "locale": locale},
        api_key,
    )


def analyze_dml_tables(code: str, ranges: list, api_key: str, locale: str) -> Dict[str, Any]:
    return _rule_loader().execute(
        "dml",
        {"code": code, "ranges": ranges, "locale": locale},
        api_key,
    )


def analyze_summary_only(summaries: dict, api_key: str, locale: str, previous_summary: str = "") -> Dict[str, Any]:
    """프로시저/함수 전체 요약 생성 (Summary만).
    
    Args:
        summaries: 하위 블록들의 요약 딕셔너리
        예: {"SELECT_10_12": "주문 정보를 조회합니다", "IF_14_18": "주문 상태가 '완료'이면 포인트를 적립합니다"}
        또는 이전 청크의 summary 문자열
    """
    return _rule_loader().execute(
        "procedure_summary_only",
        {"summaries": summaries, "locale": locale, "previous_summary": previous_summary},
        api_key,
    )


def analyze_user_story(summary: str, api_key: str, locale: str) -> Dict[str, Any]:
    """프로시저/함수 User Story + AC 생성.
    
    Args:
        summary: 프로시저/함수의 상세 요약 (문자열)
        api_key: LLM API 키
        locale: 출력 언어
    """
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


# ==================== 노드 수집기 ====================
class StatementCollector:
    """AST를 후위순회하여 `StatementNode`와 프로시저 정보를 수집합니다."""
    def __init__(self, antlr_data: Dict[str, Any], file_content: str, directory: str, file_name: str):
        """수집기에 필요한 AST 데이터와 파일 메타 정보를 초기화합니다."""
        self.antlr_data = antlr_data
        self.file_content = file_content
        self.directory = directory
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
        return f"{self.directory}:{self.file_name}:{base}:{start_line}"

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

        if node_type in PROCEDURE_TYPES:
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
                log_process("ANALYZE", "COLLECT", f"📋 프로시저 선언 발견: {proc_name_log} (라인 {start_line}~{end_line})")

        for child in children:
            child_node = self._visit(child, procedure_key, procedure_type, schema_name)
            if child_node is not None:
                child_nodes.append(child_node)

        # 후속 단계에서 활용할 분석 가능 여부 및 토큰 정보를 계산합니다.
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

        # 프로시저 요약 완료 시점을 판별하기 위해 pending 노드 수를 추적합니다.
        if analyzable and procedure_key and procedure_key in self.procedures:
            self.procedures[procedure_key].pending_nodes += 1
        else:
            statement_node.completion_event.set()

        self.nodes.append(statement_node)
        log_process("ANALYZE", "COLLECT", f"✅ {node_type} 노드 수집 완료: 라인 {start_line}~{end_line}, 토큰 {token}, 자식 {len(child_nodes)}개")
        return statement_node


# ==================== 배치 플래너 ====================
class BatchPlanner:
    """수집된 노드를 토큰 한도 내에서 배치로 묶습니다."""
    def __init__(self, token_limit: int = MAX_BATCH_TOKEN):
        """토큰 한도를 지정하여 배치 생성기를 초기화합니다."""
        self.token_limit = token_limit

    def plan(self, nodes: List[StatementNode], system_file: str) -> List[AnalysisBatch]:
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
                    log_process("ANALYZE", "BATCH", f"📦 배치 #{batch_id} 확정: 리프 노드 {len(current_nodes)}개 (토큰 {current_tokens}/{self.token_limit})")
                    batches.append(self._create_batch(batch_id, current_nodes))
                    batch_id += 1
                    current_nodes = []
                    current_tokens = 0

                log_process("ANALYZE", "BATCH", f"📦 배치 #{batch_id} 확정: 부모 노드 단독 실행 (라인 {node.start_line}~{node.end_line}, 토큰 {node.token})")
                batches.append(self._create_batch(batch_id, [node]))
                batch_id += 1
                continue

            # 현재 배치가 토큰 한도를 초과한다면 쌓인 리프 노드들을 먼저 실행합니다.
            if current_nodes and current_tokens + node.token > self.token_limit:
                # 토큰 한도를 초과하기 직전 배치를 확정합니다.
                log_process("ANALYZE", "BATCH", f"📦 배치 #{batch_id} 확정: 토큰 한도 도달로 선 실행 (누적 {current_tokens}/{self.token_limit})")
                batches.append(self._create_batch(batch_id, current_nodes))
                batch_id += 1
                current_nodes = []
                current_tokens = 0

            current_nodes.append(node)
            current_tokens += node.token

        if current_nodes:
            # 남아 있는 노드가 있으면 마무리 배치로 추가합니다.
            log_process("ANALYZE", "BATCH", f"📦 배치 #{batch_id} 확정: 마지막 리프 노드 {len(current_nodes)}개 (토큰 {current_tokens}/{self.token_limit})")
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
            # 일반 요약은 노드 compact code를 기반으로 동기식 호출을 스레드로 위임합니다.
            general_task = asyncio.to_thread(
                analyze_code,
                batch.build_general_payload(),
                batch.ranges,
                len(batch.ranges),
                self.api_key,
                self.locale,
            )

        table_task = None
        dml_payload = batch.build_dml_payload()
        if dml_payload and batch.dml_ranges:
            # DML 분석은 별도의 프롬프트로 병렬 실행하여 테이블 메타데이터를 수집합니다.
            table_task = asyncio.to_thread(
                analyze_dml_tables,
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
        # 분석할 대상이 없으면 예외 발생
        raise AnalysisError("LLM 분석 대상이 없습니다 (일반 분석 및 테이블 분석 모두 없음)")


# ==================== 적용 매니저 ====================
class ApplyManager:
    """LLM 결과를 순서대로 적용하고, 요약/테이블 설명을 후처리합니다."""
    def __init__(
        self,
        node_base_props: str,
        table_base_props: str,
        user_id: str,
        project_name: str,
        directory: str,
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
        self.table_base_props = table_base_props
        self.user_id = user_id
        self.project_name = project_name
        self.directory = directory
        self.file_name = file_name
        self.dbms = dbms
        self.api_key = api_key
        self.locale = locale
        self.procedures = procedures
        self.send_queue = send_queue
        self.receive_queue = receive_queue
        self.file_last_line = file_last_line
        # full_directory: 디렉토리 + 파일명 (로그 및 참조용)
        self.full_directory = f"{directory}/{file_name}" if directory else file_name

        self._pending: Dict[int, BatchResult] = {}
        self._summary_store: Dict[str, Dict[str, Any]] = {key: {} for key in procedures}
        self._next_batch_id = 1
        self._lock = asyncio.Lock()
        self._table_summary_store: Dict[Tuple[str, str], Dict[str, Any]] = {}

    async def submit(self, batch: AnalysisBatch, general: Optional[Dict[str, Any]], table: Optional[Dict[str, Any]]):
        """워커가 batch 처리를 마친 뒤 Apply 큐에 등록합니다."""
        async with self._lock:
            # 순서 보장을 위해 배치 결과를 임시 저장소에 넣고
            self._pending[batch.batch_id] = BatchResult(batch=batch, general_result=general, table_result=table)
            # 준비된 배치를 즉시 적용합니다.
            await self._flush_ready()

    async def finalize(self):
        """모든 배치가 적용된 후 프로시저/테이블 요약을 마무리합니다."""
        async with self._lock:
            # 남은 배치가 있다면 순서에 맞춰 마저 적용합니다.
            await self._flush_ready(force=True)
        await self._finalize_remaining_procedures()
        await self._finalize_table_summaries()

    async def _flush_ready(self, force: bool = False):
        """배치 ID 순서대로 적용 가능 여부를 확인합니다."""
        while self._next_batch_id in self._pending:
            # 다음 순번에 맞는 배치를 순차적으로 꺼내 적용합니다.
            result = self._pending.pop(self._next_batch_id)
            await self._apply_batch(result)
            self._next_batch_id += 1

        if force and self._pending:
            for batch_id in sorted(self._pending):
                # force=True 시 남은 배치를 정렬하여 적용합니다.
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
        
        # 분석 정보 수집 (스트림 메시지용)
        analyzed_node_info: Optional[Dict[str, Any]] = None
        first_summary: str = ""

        for node, analysis in summary_nodes:
            if not analysis:
                log_process("ANALYZE", "APPLY", f"⚠️ LLM이 {node.start_line}~{node.end_line} 구간에 요약을 반환하지 않음 - 건너뜀")
                node.completion_event.set()
                continue
            log_process("ANALYZE", "APPLY", f"✅ {node.start_line}~{node.end_line} 구간 요약을 Neo4j 그래프에 반영")
            
            # 첫 번째 분석 결과의 정보 저장
            if not analyzed_node_info:
                first_summary = str(analysis.get('summary', ''))[:100]
                analyzed_node_info = {
                    "type": node.node_type,
                    "name": node.procedure_name or node.name or f"Line {node.start_line}",
                    "summary": first_summary,
                    "line_range": f"{node.start_line}-{node.end_line}",
                }
            
            # LLM 결과를 Neo4j 쿼리로 변환하고 내부 요약 저장소를 갱신합니다.
            cypher_queries.extend(self._build_node_queries(node, analysis))
            self._update_summary_store(node, analysis)
            processed_nodes.add(node.node_id)

        # LLM이 빈 결과를 주더라도 completion_event는 항상 set 됩니다.
        for node in result.batch.nodes:
            if node.node_id not in processed_nodes and node.completion_event.is_set() is False:
                node.completion_event.set()

        if result.table_result:
            # 테이블 분석 결과가 있으면 추가로 테이블 관련 쿼리를 생성합니다.
            cypher_queries.extend(self._build_table_queries(result.batch, result.table_result))

        if cypher_queries:
            log_process("ANALYZE", "APPLY", f"📤 {self.full_directory}에 Cypher 쿼리 {len(cypher_queries)}건 전송")
        await self._send_queries(cypher_queries, result.batch.progress_line, analyzed_node_info)
        log_process("ANALYZE", "APPLY", f"✅ 배치 #{result.batch.batch_id} 적용 완료: 노드 {len(result.batch.nodes)}개, 테이블 분석 {'있음' if result.table_result else '없음'}")

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

        # 기본 노드 속성은 MERGE 후 SET 절에서 일괄 갱신합니다.
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
            # 부모 노드는 자식 요약을 placeholder로 보관하여 재요약 시 활용합니다.
            escaped_placeholder = escape_for_cypher(node.get_placeholder_code())
            base_fields.append(f"n.summarized_code = '{escaped_placeholder}'")

        base_set = ", ".join(base_fields)

        queries.append(
            f"MERGE (n:{node.node_type} {{startLine: {node.start_line}, {self.node_base_props}}})\n"
            f"SET {base_set}\n"
            f"RETURN n"
        )

        node.completion_event.set()

        for var_name in analysis.get('variables', []) or []:
            # 요약에서 변수 사용을 감지했다면 Variable 노드에 마킹합니다.
            queries.append(
                f"MATCH (v:Variable {{name: '{escape_for_cypher(var_name)}', {self.node_base_props}}})\n"
                f"SET v.`{node.start_line}_{node.end_line}` = 'Used'\n"
                f"RETURN v"
            )

        for call_name in analysis.get('calls', []) or []:
            if '.' in call_name:
                package_raw, proc_raw = call_name.split('.', 1)
                package_name = escape_for_cypher(package_raw.strip())
                proc_name = escape_for_cypher(proc_raw.strip())
                # 패키지.프로시저 호출은 외부 스코프로 간주하고 존재 여부에 따라 노드를 생성합니다.
                queries.append(
                    f"MATCH (c:{node.node_type} {{startLine: {node.start_line}, {self.node_base_props}}})\n"
                    f"OPTIONAL MATCH (p)\n"
                    f"WHERE (p:PROCEDURE OR p:FUNCTION)\n"
                    f"  AND p.directory = '{package_name}'\n"
                    f"  AND p.procedure_name = '{proc_name}'\n"
                    f"  AND p.user_id = '{self.user_id}'\n"
                    f"WITH c, p\n"
                    f"MERGE (target:PROCEDURE:FUNCTION {{directory: '{package_name}', procedure_name: '{proc_name}', user_id: '{self.user_id}', project_name: '{self.project_name}'}})\n"
                    f"MERGE (c)-[r:CALL {{scope: 'external'}}]->(target)\n"
                    f"RETURN c, target, r"
                )
            else:
                escaped_call = escape_for_cypher(call_name)
                queries.append(
                    f"MATCH (c:{node.node_type} {{startLine: {node.start_line}, {self.node_base_props}}})\n"
                    f"WITH c\n"
                    f"MATCH (p {{procedure_name: '{escaped_call}', {self.node_base_props}}})\n"
                    f"WHERE p:PROCEDURE OR p:FUNCTION\n"
                    # 동일 파일 내 호출은 internal scope로 연결합니다.
                    f"MERGE (c)-[r:CALL {{scope: 'internal'}}]->(p)\n"
                    f"RETURN c, p, r"
                )

        return queries

    def _build_table_queries(self, batch: AnalysisBatch, table_result: Dict[str, Any]) -> List[str]:
        """DML 테이블 분석 결과를 Neo4j 쿼리 리스트로 변환합니다."""
        queries: List[str] = []
        node_map: Dict[Tuple[int, int], StatementNode] = {
            (node.start_line, node.end_line): node for node in batch.nodes
        }
        normalized_ranges: List[Dict[str, Any]] = list(table_result.get('ranges', []))

        # range 결과를 순회하며 각 구간의 메타데이터를 적용합니다.
        for range_entry in normalized_ranges:
            start_line_raw = range_entry.get('startLine')
            end_line_raw = range_entry.get('endLine')
            tables = range_entry.get('tables') or []

            try:
                start_line = int(start_line_raw)
                end_line = int(end_line_raw)
            except (TypeError, ValueError) as e:
                raise AnalysisError(
                    f"LLM 응답의 라인 번호가 유효하지 않습니다: startLine={start_line_raw}, endLine={end_line_raw}"
                ) from e

            node = node_map.get((start_line, end_line))
            if not node:
                raise AnalysisError(
                    f"LLM 응답의 라인 범위에 해당하는 노드를 찾을 수 없습니다: {start_line}~{end_line}"
                )

            if node.node_type == 'CREATE_TEMP_TABLE':
                for entry in tables:
                    table_name = (entry.get('table') or '').strip()
                    if not table_name:
                        continue
                    schema_part, name_part, _ = parse_table_identifier(table_name)
                    # 임시 테이블 생성은 테이블 노드 자체에 속성을 저장합니다.
                    node_merge = f"MERGE (n:{node.node_type} {{startLine: {node.start_line}, {self.node_base_props}}})"
                    queries.append(
                        f"{node_merge}\n"
                        f"SET n:Table, n.name = '{escape_for_cypher(name_part)}', n.schema = '{escape_for_cypher(schema_part)}', "
                        f"n.db = '{self.dbms}'\n"
                        f"RETURN n"
                    )
                continue

            node_merge_base = f"MERGE (n:{node.node_type} {{startLine: {node.start_line}, {self.node_base_props}}})"

            # 테이블별 정보를 순회하여 MERGE 및 관계를 생성합니다.
            for entry in tables:
                table_name = (entry.get('table') or '').strip()
                if not table_name:
                    continue

                schema_part, name_part, db_link_value = parse_table_identifier(table_name)

                access_mode_raw = (entry.get('accessMode') or '').lower()
                relationship_targets: List[str] = []
                if 'r' in access_mode_raw:
                    relationship_targets.append(TABLE_RELATIONSHIP_MAP['r'])
                if 'w' in access_mode_raw:
                    relationship_targets.append(TABLE_RELATIONSHIP_MAP['w'])
                table_merge = self._build_table_merge(name_part, schema_part)

                # 테이블 설명은 후속 요약을 위해 버킷에 누적합니다.
                bucket_key = self._record_table_summary(schema_part, name_part, entry.get('tableDescription'))

                # 1) 테이블 노드와 DML 관계까지 설정
                base_table_query = (
                    f"{node_merge_base}\n"
                    f"WITH n\n"
                    f"{table_merge}\n"
                    f"SET t.db = coalesce(t.db, '{self.dbms}')"
                )

                if db_link_value:
                    base_table_query += f"\nSET t.db_link = COALESCE(t.db_link, '{db_link_value}')"

                rel_vars = []
                node_vars = ["n", "t"]
                for i, relationship in enumerate(relationship_targets):
                    rel_var = f"r{i}"
                    rel_vars.append(rel_var)
                    # 읽기/쓰기 모드를 Neo4j 관계로 표현합니다.
                    base_table_query += f"\nMERGE (n)-[{rel_var}:{relationship}]->(t)"

                # 노드와 관계를 모두 반환
                if rel_vars:
                    base_table_query += f"\nRETURN {', '.join(node_vars)}, {', '.join(rel_vars)}"
                else:
                    base_table_query += f"\nRETURN {', '.join(node_vars)}"
                queries.append(base_table_query)

                # 2) 컬럼 노드 및 HAS_COLUMN 관계 생성
                for column in entry.get('columns', []) or []:
                    column_name = (column.get('name') or '').strip()
                    if not column_name:
                        continue
                    raw_dtype = (column.get('dtype') or '')
                    col_type = escape_for_cypher(raw_dtype or '')
                    raw_column_desc = (column.get('description') or column.get('comment') or '').strip()
                    # 컬럼 설명/메타/예시 값을 테이블 버킷에 적재하여 후속 요약에 활용합니다.
                    self._record_column_summary(
                        bucket_key,
                        column_name,
                        raw_column_desc,
                        dtype=raw_dtype,
                        nullable=column.get('nullable', True),
                        examples=(column.get('examples') or [])
                    )
                    col_description = escape_for_cypher(raw_column_desc)
                    nullable_flag = 'true' if column.get('nullable', True) else 'false'
                    escaped_column_name = escape_for_cypher(column_name)

                    if schema_part:
                        # 스키마가 있으면 fqn으로 MERGE (기존 방식)
                        fqn = '.'.join(filter(None, [schema_part, name_part, column_name])).lower()
                        column_merge_key = (
                            f"`user_id`: '{self.user_id}', `fqn`: '{fqn}', `project_name`: '{self.project_name}'"
                        )
                        queries.append(
                            f"{table_merge}\n"
                            f"WITH t\n"
                            f"MERGE (c:Column {{{column_merge_key}}})\n"
                            f"SET c.`name` = '{escaped_column_name}', c.`dtype` = '{col_type}', c.`description` = '{col_description}', c.`nullable` = '{nullable_flag}', c.`fqn` = '{fqn}'\n"
                            f"WITH t, c\n"
                            f"MERGE (t)-[r:HAS_COLUMN]->(c)\n"
                            f"RETURN t, c, r"
                        )
                    else:
                        # 스키마가 없으면 테이블의 schema를 기반으로 fqn을 동적 계산하여 MERGE
                        # 기존 컬럼이 있으면 찾고, 없으면 생성 (항상 관계 반환)
                        queries.append(
                            f"{table_merge}\n"
                            f"WITH t, lower(case when t.schema <> '' and t.schema IS NOT NULL then t.schema + '.' + '{name_part}' + '.' + '{column_name}' else '{name_part}' + '.' + '{column_name}' end) as fqn\n"
                            f"MERGE (c:Column {{`user_id`: '{self.user_id}', `fqn`: fqn, `project_name`: '{self.project_name}'}})\n"
                            f"ON CREATE SET c.`name` = '{escaped_column_name}', c.`dtype` = '{col_type}', c.`description` = '{col_description}', c.`nullable` = '{nullable_flag}'\n"
                            f"ON MATCH SET c.`name` = '{escaped_column_name}', c.`dtype` = CASE WHEN c.`dtype` = '' OR c.`dtype` IS NULL THEN '{col_type}' ELSE c.`dtype` END\n"
                            f"WITH t, c\n"
                            f"MERGE (t)-[r:HAS_COLUMN]->(c)\n"
                            f"RETURN t, c, r"
                        )

            # 3) DB 링크 노드 연결 (범위 단위)
            for link_item in range_entry.get('dbLinks', []) or []:
                link_name_raw = (link_item.get('name') or '').strip()
                if not link_name_raw:
                    continue
                mode = (link_item.get('mode') or 'r').lower()
                schema_link, name_link, link_name = parse_table_identifier(link_name_raw)
                remote_merge = self._build_table_merge(name_link, schema_link)
                queries.append(
                    f"{remote_merge}\n"
                    f"SET t.db_link = '{link_name}'\n"
                    f"WITH t\n"
                    f"MERGE (l:DBLink {{user_id: '{self.user_id}', name: '{link_name}', project_name: '{self.project_name}'}})\n"
                    f"MERGE (l)-[r1:CONTAINS]->(t)\n"
                    f"WITH t, l, r1\n"
                    f"{node_merge_base}\n"
                    f"MERGE (n)-[r2:DB_LINK {{mode: '{mode}'}}]->(t)\n"
                    f"RETURN r1, r2"
                )

            # 4) 참조 관계(테이블/컬럼) 생성 (범위 단위)
            for relation in range_entry.get('fkRelations', []) or []:
                src_table = (relation.get('sourceTable') or '').strip()
                tgt_table = (relation.get('targetTable') or '').strip()
                src_columns = [
                    (column or '').strip()
                    for column in (relation.get('sourceColumns') or [])
                    if column is not None and str(column).strip()
                ]
                tgt_columns = [
                    (column or '').strip()
                    for column in (relation.get('targetColumns') or [])
                    if column is not None and str(column).strip()
                ]
                if not (src_table and tgt_table and src_columns and tgt_columns):
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
                    f"MERGE (st)-[r:FK_TO_TABLE]->(tt)\n"
                    f"RETURN st, tt, r"
                )
                for src_column, tgt_column in zip(src_columns, tgt_columns):
                    if not (src_column and tgt_column):
                        continue
                    src_fqn = '.'.join(filter(None, [src_schema, src_table_name, src_column])).lower()
                    tgt_fqn = '.'.join(filter(None, [tgt_schema, tgt_table_name, tgt_column])).lower()
                    queries.append(
                        f"MATCH (sc:Column {{user_id: '{self.user_id}', name: '{src_column}', fqn: '{src_fqn}', project_name: '{self.project_name}'}})\n"
                        f"MATCH (dc:Column {{user_id: '{self.user_id}', name: '{tgt_column}', fqn: '{tgt_fqn}', project_name: '{self.project_name}'}})\n"
                        f"MERGE (sc)-[r:FK_TO]->(dc)\n"
                        f"RETURN sc, dc, r"
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

    def _split_summaries_by_token(self, summaries: dict, max_token: int) -> List[dict]:
        """토큰 기준으로 summaries를 청크로 분할합니다.
        
        Args:
            summaries: 하위 블록 요약 딕셔너리
            max_token: 청크당 최대 토큰 수
        
        Returns:
            청크 리스트 (각 청크는 dict)
        """
        if not summaries:
            return []
        
        chunks = []
        current_chunk = {}
        current_tokens = 0
        
        for key, value in summaries.items():
            # 현재 항목의 토큰 계산 (key + value)
            item_text = f"{key}: {value}"
            item_tokens = calculate_code_token(item_text)
            
            # 현재 청크에 추가하면 토큰 한도 초과하는 경우
            if current_tokens + item_tokens > max_token and current_chunk:
                chunks.append(current_chunk)
                current_chunk = {}
                current_tokens = 0
            
            current_chunk[key] = value
            current_tokens += item_tokens
        
        # 마지막 청크 추가
        if current_chunk:
            chunks.append(current_chunk)
        
        return chunks

    async def _finalize_procedure_summary(self, info: ProcedureInfo):
        """프로시저 전체 요약 + User Story + AC 생성.
        
        처리 흐름:
        1. 토큰 기준으로 summaries를 청크로 분할
        2. 각 청크를 병렬로 처리하여 summary 생성
        3. 생성된 summary들을 하나로 합치기
        4. 최종 summary로 User Story 생성
        5. Neo4j에 저장
        """
        if info.key not in self._summary_store:
            return
        summaries = self._summary_store.pop(info.key, {})
        if not summaries:
            return
        
        all_user_stories = []
        final_summary = ""
        
        try:
            # 1단계: 토큰 기준으로 청크 분할
            chunks = self._split_summaries_by_token(summaries, MAX_SUMMARY_CHUNK_TOKEN)
            
            if not chunks:
                return
            
            log_process("ANALYZE", "SUMMARY", f"📦 {info.procedure_name}: summary 청크 분할 완료 ({len(chunks)}개 청크)")
            
            # 2단계: 각 청크를 병렬로 처리하여 summary 생성 및 User Story 생성
            async def process_chunk(chunk_idx: int, chunk: dict) -> Tuple[str, List[Dict[str, Any]]]:
                """청크를 처리하여 summary와 User Story 생성 (병렬 처리용)."""
                chunk_tokens = calculate_code_token(json.dumps(chunk, ensure_ascii=False))
                log_process("ANALYZE", "SUMMARY", f"  → 청크 {chunk_idx + 1}/{len(chunks)} 처리 시작 (토큰: {chunk_tokens})")
                
                # Summary 생성
                summary_result = await asyncio.to_thread(
                    analyze_summary_only,
                    chunk,
                    self.api_key,
                    self.locale,
                    ""  # 병렬 처리이므로 이전 summary 없음
                )
                
                if isinstance(summary_result, dict):
                    chunk_summary = summary_result.get('summary', '')
                else:
                    chunk_summary = ""
                
                # 각 청크의 summary로 User Story 생성
                chunk_user_stories = []
                if chunk_summary:
                    user_story_result = await asyncio.to_thread(
                        analyze_user_story,
                        chunk_summary,
                        self.api_key,
                        self.locale
                    )
                    if isinstance(user_story_result, dict):
                        chunk_user_stories = user_story_result.get('user_stories', []) or []
                
                return chunk_summary, chunk_user_stories
            
            # 모든 청크를 병렬로 처리
            chunk_tasks = [process_chunk(idx, chunk) for idx, chunk in enumerate(chunks)]
            chunk_results_raw = await asyncio.gather(*chunk_tasks)
            
            # 결과 추출
            chunk_results = []
            for chunk_summary, chunk_user_stories in chunk_results_raw:
                if chunk_summary:
                    chunk_results.append(chunk_summary)
                if chunk_user_stories:
                    all_user_stories.extend(chunk_user_stories)
            
            if not chunk_results:
                return
            
            # 3단계: 모든 청크의 summary를 하나로 합치기
            if len(chunk_results) == 1:
                final_summary = chunk_results[0]
            else:
                # 여러 청크의 summary를 딕셔너리로 변환하여 합치기
                combined_summaries = {}
                for idx, chunk_summary in enumerate(chunk_results):
                    combined_summaries[f"CHUNK_{idx + 1}"] = chunk_summary
                
                # 합친 summary를 다시 LLM에 전달하여 최종 요약 생성
                final_summary_result = await asyncio.to_thread(
                    analyze_summary_only,
                    combined_summaries,
                    self.api_key,
                    self.locale,
                    ""
                )
                if isinstance(final_summary_result, dict):
                    final_summary = final_summary_result.get('summary', "\n\n".join(chunk_results))
                else:
                    final_summary = "\n\n".join(chunk_results)
            
            log_process("ANALYZE", "SUMMARY", f"✅ {info.procedure_name}: summary 통합 완료")
            
            # 4단계: 최종 summary로도 User Story 생성 (청크별 User Story와 함께 수집)
            final_user_story_result = await asyncio.to_thread(
                analyze_user_story,
                final_summary,
                self.api_key,
                self.locale
            )
            
            if isinstance(final_user_story_result, dict):
                final_user_stories = final_user_story_result.get('user_stories', []) or []
                all_user_stories.extend(final_user_stories)
            
            if all_user_stories:
                log_process("ANALYZE", "SUMMARY", f"✅ {info.procedure_name}: User Story {len(all_user_stories)}개")
            else:
                log_process("ANALYZE", "SUMMARY", f"✅ {info.procedure_name}: User Story 없음")
                
        except Exception as exc:  # pragma: no cover - defensive
            log_process("ANALYZE", "SUMMARY", f"❌ {info.procedure_name} 프로시저 요약 생성 중 오류 발생", logging.ERROR, exc)
            return

        if not final_summary:
            return

        # 5단계: Neo4j에 summary 저장 및 User Story/AC를 노드와 관계로 저장
        summary_json = json.dumps(final_summary, ensure_ascii=False)
        
        # Summary 저장
        summary_query = (
            f"MATCH (n:{info.procedure_type} {{procedure_name: '{escape_for_cypher(info.procedure_name)}', {self.node_base_props}}})\n"
            f"SET n.summary = {summary_json}\n"
            f"RETURN n"
        )
        
        queries = [summary_query]
        
        # User Story와 AC를 노드와 관계로 저장 (유효한 User Story가 있는 경우만)
        if all_user_stories:
            procedure_name_escaped = escape_for_cypher(info.procedure_name)
            for us_idx, us in enumerate(all_user_stories, 1):
                us_id = us.get('id', f"US-{us_idx}")
                role = escape_for_cypher(us.get('role', ''))
                goal = escape_for_cypher(us.get('goal', ''))
                benefit = escape_for_cypher(us.get('benefit', ''))
                
                # User Story 노드 생성 및 관계
                us_query = (
                    f"MATCH (p:{info.procedure_type} {{procedure_name: '{procedure_name_escaped}', {self.node_base_props}}})\n"
                    f"MERGE (us:UserStory {{id: '{us_id}', procedure_name: '{procedure_name_escaped}', {self.node_base_props}}})\n"
                    f"SET us.role = '{role}',\n"
                    f"    us.goal = '{goal}',\n"
                    f"    us.benefit = '{benefit}'\n"
                    f"MERGE (p)-[r:HAS_USER_STORY]->(us)\n"
                    f"RETURN p, us, r"
                )
                queries.append(us_query)
                
                # Acceptance Criteria 노드 생성 및 관계
                acs = us.get('acceptance_criteria', [])
                for ac_idx, ac in enumerate(acs, 1):
                    if not isinstance(ac, dict):
                        continue
                    ac_id = ac.get('id', f"AC-{us_idx}-{ac_idx}")
                    ac_title = escape_for_cypher(ac.get('title', ''))
                    ac_given = json.dumps(ac.get('given', []), ensure_ascii=False)
                    ac_when = json.dumps(ac.get('when', []), ensure_ascii=False)
                    ac_then = json.dumps(ac.get('then', []), ensure_ascii=False)
                    
                    ac_query = (
                        f"MATCH (us:UserStory {{id: '{us_id}', {self.node_base_props}}})\n"
                        f"MERGE (ac:AcceptanceCriteria {{id: '{ac_id}', user_story_id: '{us_id}', {self.node_base_props}}})\n"
                        f"SET ac.title = '{ac_title}',\n"
                        f"    ac.given = {ac_given},\n"
                        f"    ac.when = {ac_when},\n"
                        f"    ac.then = {ac_then}\n"
                        f"MERGE (us)-[r:HAS_AC]->(ac)\n"
                        f"RETURN us, ac, r"
                    )
                    queries.append(ac_query)
        
        await self._send_queries(queries, info.end_line)
        
        # User Story 개수 로깅
        us_count = len(all_user_stories) if all_user_stories else 0
        log_process("ANALYZE", "SUMMARY", f"✅ {info.procedure_name} 프로시저 요약 + User Story({us_count}개) Neo4j 반영 완료 ({self.full_directory})")

    async def _finalize_remaining_procedures(self):
        """아직 요약이 남아 있는 프로시저가 있다면 마지막으로 처리합니다."""
        for key, info in list(self.procedures.items()):
            if info.pending_nodes == 0 and key in self._summary_store and self._summary_store[key]:
                await self._finalize_procedure_summary(info)

    async def _send_queries(
        self,
        queries: List[str],
        progress_line: int,
        analysis_info: Optional[Dict[str, Any]] = None
    ):
        """분석 큐에 쿼리를 전달하고 처리가 끝날 때까지 대기합니다."""
        if not queries:
            return
        event = {
            "type": "analysis_code",
            "query_data": queries,
            "line_number": progress_line,
        }
        if analysis_info:
            event["analysis_info"] = analysis_info
        await self.send_queue.put(event)
        while True:
            response = await self.receive_queue.get()
            if response.get('type') == 'process_completed':
                break
        log_process("ANALYZE", "APPLY", f"✅ {self.full_directory}에 대한 Neo4j 반영 완료")

    def _build_table_merge(self, table_name: str, schema: Optional[str]) -> str:
        schema_value = schema or ''
        # 스키마가 빈 문자열이면 MERGE 조건에서 제외 (테이블명만으로 조회)
        schema_part = f", schema: '{schema_value}'" if schema_value else ""
        return (
            f"MERGE (t:Table {{{self.table_base_props}, name: '{table_name}'{schema_part}, db: '{self.dbms}', project_name: '{self.project_name}'}})"
        )

    def _record_table_summary(self, schema: Optional[str], name: str, description: Optional[str]) -> Tuple[str, str]:
        """테이블 설명 문장을 버킷에 누적합니다."""
        schema_key = schema or ''
        name_key = name
        bucket = self._table_summary_store.get((schema_key, name_key))
        if bucket is None:
            # 테이블별 요약을 합산하기 위해 summaries/columns 구조를 초기화합니다.
            bucket = {"summaries": set(), "columns": {}}
            self._table_summary_store[(schema_key, name_key)] = bucket
        text = (description or '').strip()
        if text:
            # 중복 문장은 set을 이용해 자동으로 제거합니다.
            bucket["summaries"].add(text)
        return (schema_key, name_key)

    def _record_column_summary(self, table_key: Tuple[str, str], column_name: str, description: Optional[str], dtype: Optional[str] = None, nullable: Optional[bool] = None, examples: Optional[List[str]] = None):
        """컬럼 설명과 메타데이터(dtype/nullable/예시값)를 버킷에 누적합니다."""
        text = (description or '').strip()
        bucket = self._table_summary_store.setdefault(table_key, {"summaries": set(), "columns": {}})
        columns = bucket["columns"]
        canonical = column_name
        entry = columns.get(canonical)
        if entry is None:
            entry = {"name": column_name, "summaries": set(), "dtype": (dtype or ''), "nullable": True if nullable is None else bool(nullable), "examples": set()}
            columns[canonical] = entry
        # 메타데이터 최신화
        if dtype is not None and not entry.get("dtype"):
            entry["dtype"] = dtype
        if nullable is not None:
            entry["nullable"] = bool(nullable)
        if text:
            entry["summaries"].add(text)
        if examples:
            for v in examples:
                if v is None:
                    continue
                s = str(v).strip()
                if s:
                    entry["examples"].add(s)

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
        # 컬럼 메타데이터를 구성합니다.
        column_metadata = {
            entry['name']: {
                "dtype": entry.get("dtype") or "",
                "nullable": bool(entry.get("nullable", True)),
                "examples": sorted(list(entry.get("examples") or []))[:5],
            }
            for entry in columns_map.values()
        }

        # 테이블/컬럼 설명을 단일 프롬프트로 묶어 배치 요약을 수행합니다.
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
                f"MATCH (t:Table {{{table_props}}})\nSET t.description = '{escape_for_cypher(table_desc)}'\nRETURN t"
            )

        # detailDescription(사람이 읽을 수 있는 텍스트) 적용
        detail_text = result.get('detailDescription') or ''
        if isinstance(detail_text, str) and detail_text.strip():
            queries.append(
                f"MATCH (t:Table {{{table_props}}})\nSET t.detailDescription = '{escape_for_cypher(detail_text.strip())}'\nRETURN t"
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
                f"MATCH (c:Column {{{column_props}}})\nSET c.description = '{escape_for_cypher(column_desc)}'\nRETURN c"
            )

        if queries:
            await self._send_queries(queries, self.file_last_line)


# ==================== AST 프로세서 본체 ====================
class DbmsAstProcessor:
    """DBMS AST 처리 및 LLM 분석 파이프라인

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
        directory: str,
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
        # Windows 경로 구분자(\\)를 /로 변환하여 일관성 유지
        normalized_dir = directory.replace('\\', '/') if directory else ''
        self.directory = normalized_dir
        self.file_name = file_name
        self.user_id = user_id
        self.api_key = api_key
        self.locale = locale
        self.dbms = (dbms or 'postgres').lower()
        self.project_name = project_name or ''
        # full_directory: 디렉토리 + 파일명 (Neo4j directory 속성으로 사용)
        self.full_directory = f"{normalized_dir}/{file_name}" if normalized_dir else file_name

        self.node_base_props = (
            f"directory: '{escape_for_cypher(self.full_directory)}', file_name: '{file_name}', user_id: '{user_id}', project_name: '{self.project_name}'"
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
        await self._process_variable_nodes(nodes)
        # 4) 정적 그래프 초기화 완료 알림
        await self.send_queue.put({"type": "static_complete"})
        while True:
            resp = await self.receive_queue.get()
            if resp.get("type") == "process_completed":
                break

    async def _create_static_nodes(self, nodes: List[StatementNode]):
        """각 StatementNode에 대응하는 기본 노드를 Neo4j에 생성합니다."""
        queries: List[str] = []
        current_batch_nodes: List[StatementNode] = []
        
        for node in nodes:
            # StatementNode 단위로 MERGE 쿼리 묶음을 생성합니다.
            queries.extend(self._build_static_node_queries(node))
            current_batch_nodes.append(node)
            
            if len(queries) >= STATIC_QUERY_BATCH_SIZE:
                # 일정량이 쌓이면 즉시 전송하여 큐를 비웁니다.
                node_info = self._build_batch_node_info(current_batch_nodes)
                await self._send_static_queries(queries, node.end_line, node_info)
                queries.clear()
                current_batch_nodes.clear()
                
        if queries:
            # 마지막 남은 쿼리 묶음도 전송합니다.
            node_info = self._build_batch_node_info(current_batch_nodes)
            await self._send_static_queries(queries, nodes[-1].end_line, node_info)

    def _build_batch_node_info(self, nodes: List[StatementNode]) -> Dict[str, Any]:
        """배치의 노드들 정보를 요약합니다."""
        if not nodes:
            return {}
        
        # 노드 타입별 집계
        type_counts: Dict[str, int] = {}
        for node in nodes:
            type_counts[node.node_type] = type_counts.get(node.node_type, 0) + 1
        
        # 첫 번째 의미 있는 노드 정보
        first_node = nodes[0]
        for node in nodes:
            if node.node_type not in ("FILE",):
                first_node = node
                break
        
        return {
            "type": first_node.node_type,
            "name": first_node.procedure_name or first_node.name or f"Line {first_node.start_line}",
            "start_line": first_node.start_line,
            "node_count": len(nodes),
            "type_summary": type_counts,
        }

    def _build_static_node_queries(self, node: StatementNode) -> List[str]:
        """정적 노드 생성을 위한 Cypher 쿼리 리스트를 반환합니다."""
        queries: List[str] = []
        label = node.node_type
        node_name = self.file_name if label == "FILE" else build_statement_name(label, node.start_line)
        escaped_name = escape_for_cypher(node_name)
        has_children = 'true' if node.has_children else 'false'
        procedure_name = escape_for_cypher(node.procedure_name or '')

        if not node.children and label not in NON_ANALYSIS_TYPES:
            # 리프 노드이면서 분석 대상이면 요약 전 node_code를 포함해 저장합니다.
            escaped_code = escape_for_cypher(node.code)
            queries.append(
                f"MERGE (n:{label} {{startLine: {node.start_line}, {self.node_base_props}}})\n"
                f"SET n.endLine = {node.end_line}, n.name = '{escaped_name}', n.node_code = '{escaped_code}',\n"
                f"    n.token = {node.token}, n.procedure_name = '{procedure_name}', n.has_children = {has_children}\n"
                f"RETURN n"
            )
            return queries

        escaped_code = escape_for_cypher(node.code)

        if label == "FILE":
            file_summary = 'File Start Node' if self.locale == 'en' else '파일 노드'
            queries.append(
                f"MERGE (n:{label} {{startLine: {node.start_line}, {self.node_base_props}}})\n"
                f"SET n.endLine = {node.end_line}, n.name = '{self.file_name}', n.summary = '{escape_for_cypher(file_summary)}',\n"
                f"    n.has_children = {has_children}\n"
                f"RETURN n"
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
                f"RETURN n"
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
                if prev_node and prev_node.node_type not in NON_NEXT_RECURSIVE_TYPES:
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
        return f"{parent_match}\n{child_match}\nMERGE (parent)-[r:PARENT_OF]->(child)\nRETURN parent, child, r"

    def _build_next_relationship_query(self, prev_node: StatementNode, current_node: StatementNode) -> str:
        """형제 노드 사이의 NEXT 관계 쿼리를 작성합니다."""
        prev_match = f"MATCH (prev:{prev_node.node_type} {{startLine: {prev_node.start_line}, {self.node_base_props}}})"
        curr_match = f"MATCH (current:{current_node.node_type} {{startLine: {current_node.start_line}, {self.node_base_props}}})"
        return f"{prev_match}\n{curr_match}\nMERGE (prev)-[r:NEXT]->(current)\nRETURN prev, current, r"

    async def _process_variable_nodes(self, nodes: List[StatementNode]):
        """변수 선언 노드를 병렬로 분석하여 Variable 노드와 연결합니다."""
        targets = [node for node in nodes if node.node_type in VARIABLE_DECLARATION_TYPES]
        if not targets:
            return

        proc_labels = sorted({node.procedure_name or "" for node in targets})
        if proc_labels:
            label_text = ', '.join(label for label in proc_labels if label) or '익명 프로시저'
            log_process("ANALYZE", "VAR", f"🔍 변수 선언 분석 시작: {label_text} ({self.full_directory})")

        semaphore = asyncio.Semaphore(VARIABLE_CONCURRENCY)

        async def worker(node: StatementNode):
            async with semaphore:
                try:
                    # 변수 선언 코드를 개별적으로 프롬프트에 전달합니다.
                    result = await asyncio.to_thread(
                        analyze_variables,
                        node.get_raw_code(),
                        self.api_key,
                        self.locale,
                    )
                except Exception as exc:  # pragma: no cover - defensive logging
                    log_process("ANALYZE", "VAR", f"❌ {node.node_type} ({node.start_line}~{node.end_line}) 변수 분석 중 오류 발생", logging.ERROR, exc)
                    return

                queries = self._build_variable_queries(node, result)
                if queries:
                    # 변수 쿼리는 정적 그래프 초기화 단계에서 즉시 반영합니다.
                    await self._send_static_queries(queries, node.end_line)

        await asyncio.gather(*(worker(node) for node in targets))
        if proc_labels:
            log_process("ANALYZE", "VAR", f"✅ 변수 선언 분석 완료: {label_text} ({self.full_directory})")

    def _build_variable_queries(self, node: StatementNode, analysis: Dict[str, Any]) -> List[str]:
        """변수 분석 결과를 Neo4j 쿼리로 변환합니다."""
        if not isinstance(analysis, dict):
            raise AnalysisError(f"변수 분석 결과가 유효하지 않습니다 (node={node.start_line}): {type(analysis)}")

        variables = analysis.get("variables") or []
        summary_payload = analysis.get("summary")
        summary_json = json.dumps(summary_payload if summary_payload is not None else "", ensure_ascii=False)

        role = VARIABLE_ROLE_MAP.get(node.node_type, "알 수 없는 매개변수")
        scope = "Global" if node.node_type == "PACKAGE_VARIABLE" else "Local"

        node_props = self.node_base_props
        procedure_name = escape_for_cypher(node.procedure_name or '')

        if node.node_type == "PACKAGE_VARIABLE":
            node_match = f"startLine: {node.start_line}, {node_props}"
            base_var_props = f"{node_props}, role: '{role}', scope: '{scope}'"
        else:
            node_match = f"startLine: {node.start_line}, procedure_name: '{procedure_name}', {node_props}"
            base_var_props = f"{node_props}, procedure_name: '{procedure_name}', role: '{role}', scope: '{scope}'"

        queries: List[str] = []
        # 변수 요약은 선언 노드 자체 summary 필드에 저장합니다.
        queries.append(
            f"MATCH (p:{node.node_type} {{{node_match}}})\nSET p.summary = {summary_json}\nRETURN p"
        )

        for variable in variables:
            name_raw = (variable.get("name") or '').strip()
            if not name_raw:
                continue

            name = escape_for_cypher(name_raw)
            var_type = escape_for_cypher(variable.get("type") or '')
            param_type = escape_for_cypher(variable.get("parameter_type") or '')
            value_json = json.dumps(variable.get("value") if variable.get("value") is not None else "", ensure_ascii=False)

            # Variable 노드를 생성/갱신하고 선언 노드와 SCOPE 관계를 연결합니다.
            queries.append(
                f"MERGE (v:Variable {{name: '{name}', {base_var_props}, type: '{var_type}', parameter_type: '{param_type}', value: {value_json}}})\n"
                f"WITH v\n"
                f"MATCH (p:{node.node_type} {{{node_match}}})\n"
                f"MERGE (p)-[r1:SCOPE]->(v)\n"
                f"RETURN v, p, r1"
            )

        return queries

    async def _send_static_queries(
        self,
        queries: List[str],
        progress_line: int,
        node_info: Optional[Dict[str, Any]] = None
    ):
        """정적 그래프 초기화 쿼리를 큐로 전송하고 완료 시까지 기다립니다."""
        if not queries:
            return
        event = {
            "type": "static_graph",
            "query_data": queries,
            "line_number": progress_line,
        }
        if node_info:
            event["node_info"] = node_info
        await self.send_queue.put(event)
        while True:
            response = await self.receive_queue.get()
            if response.get('type') == 'process_completed':
                break

    async def run(self):
        """파일 단위 분석 파이프라인을 실행합니다."""
        log_process("ANALYZE", "START", f"🚀 {self.full_directory} 분석 시작 (총 {self.last_line}줄)")
        try:
            collector = StatementCollector(self.antlr_data, self.file_content, self.directory, self.file_name)
            # 1) AST를 평탄화하여 StatementNode 목록을 얻습니다.
            nodes, procedures = collector.collect()
            # 2) 분석 전 Neo4j에 정적 구조를 초기화합니다.
            await self._initialize_static_graph(nodes)
            planner = BatchPlanner()
            # 3) 노드를 토큰 기준으로 배치 단위로 분할합니다.
            batches = planner.plan(nodes, self.full_directory)

            if not batches:
                # 분석할 노드가 없다면 즉시 종료 이벤트만 전송합니다.
                await self.send_queue.put({"type": "end_analysis"})
                return

            # LLM 분석 시작 알림 (총 배치 수 전달)
            await self.send_queue.put({"type": "llm_start", "total_batches": len(batches)})
            while True:
                resp = await self.receive_queue.get()
                if resp.get("type") == "process_completed":
                    break

            # 1) LLM 워커 / 2) 적용 관리자 준비
            invoker = LLMInvoker(self.api_key, self.locale)
            apply_manager = ApplyManager(
                node_base_props=self.node_base_props,
                table_base_props=self.table_base_props,
                user_id=self.user_id,
                project_name=self.project_name,
                directory=self.directory,
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
                    log_process("ANALYZE", "LLM", f"🤖 배치 #{batch.batch_id} LLM 요청: 노드 {len(batch.nodes)}개 ({self.full_directory})")
                    # LLM 호출은 일반 요약과 테이블 요약을 동시에 요청합니다.
                    general, table = await invoker.invoke(batch)
                await apply_manager.submit(batch, general, table)

            await asyncio.gather(*(worker(batch) for batch in batches))
            # 모든 배치 제출이 끝나면 요약/테이블 설명 후처리를 마무리합니다.
            await apply_manager.finalize()

            log_process("ANALYZE", "DONE", f"✅ {self.full_directory} 분석 완료")
            await self.send_queue.put({"type": "end_analysis"})

        except (AnalysisError, LLMCallError) as exc:
            log_process("ANALYZE", "ERROR", f"❌ 분석 파이프라인 예외: {exc}", logging.ERROR, exc)
            await self.send_queue.put({'type': 'error', 'message': str(exc)})
            raise
        except Exception as exc:
            err_msg = f"분석 과정에서 예기치 못한 오류 발생: {exc}"
            log_process("ANALYZE", "ERROR", f"❌ {err_msg}", logging.ERROR, exc)
            await self.send_queue.put({'type': 'error', 'message': err_msg})
            raise CodeProcessError(err_msg) from exc

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
            log_process("ANALYZE", "WAIT", f"⏳ 배치 #{batch.batch_id}가 부모 분석 시작 전 자식 {len(waiters)}개 요약 완료 대기")
            await asyncio.gather(*waiters)