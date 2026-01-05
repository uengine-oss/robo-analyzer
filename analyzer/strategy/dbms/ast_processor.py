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

# 정규식 패턴
LINE_NUMBER_PATTERN = re.compile(r"^\d+\s*:")


# ==================== 데이터 클래스 ====================
@dataclass(slots=True)
class StatementNode:
    """평탄화된 AST 노드를 표현합니다.

    - 수집 단계에서 모든 노드를 생성합니다.
    - 이후 배치가 만들어질 때 이 객체를 그대로 사용합니다.
    - LLM 요약이 끝나면 `summary`와 `completion_event`가 채워집니다.
    - `ok` 플래그로 성공 여부를 추적합니다 (자식 실패 시 부모도 False).
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
    ok: bool = True  # LLM 분석 성공 여부 (자식 실패 시 부모도 False)
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


# ==================== AST 프로세서 본체 ====================
class DbmsAstProcessor:
    """DBMS AST 처리 및 LLM 분석 파이프라인
    
    2단계 분석 지원 (Framework와 동일):
    - Phase 1: build_static_graph_queries() - 정적 그래프 쿼리 생성
    - Phase 2: run_llm_analysis() - LLM 분석 후 업데이트 쿼리 생성
    """
    def __init__(
        self,
        antlr_data: dict,
        file_content: str,
        directory: str,
        file_name: str,
        user_id: str,
        api_key: str,
        locale: str,
        dbms: str,
        project_name: str,
        last_line: int,
        default_schema: str = "public",
        ddl_table_metadata: Optional[Dict[Tuple[str, str], Dict[str, Any]]] = None,
    ):
        """Analyzer가 파일 분석에 필요한 모든 컨텍스트를 초기화합니다."""
        self.antlr_data = antlr_data
        self.file_content = file_content
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
        self.default_schema = default_schema  # 스키마 미식별 시 사용할 기본 스키마
        self._ddl_table_metadata = ddl_table_metadata or {}  # DDL 메타데이터 캐시 (메모리)
        # full_directory: 디렉토리 + 파일명 (Neo4j directory 속성으로 사용)
        self.full_directory = f"{normalized_dir}/{file_name}" if normalized_dir else file_name

        self.node_base_props = (
            f"directory: '{escape_for_cypher(self.full_directory)}', file_name: '{file_name}', user_id: '{user_id}', project_name: '{self.project_name}'"
        )
        self.table_base_props = f"user_id: '{user_id}'"
        self.max_workers = MAX_CONCURRENCY
        self.file_last_line = last_line
        
        # AST 수집 결과 캐시 (Phase 1에서 수집, Phase 2에서 사용)
        self._nodes: Optional[List[StatementNode]] = None
        self._procedures: Optional[Dict[str, ProcedureInfo]] = None
        
        # 테이블/컬럼 설명 요약용 저장소 (DML 분석에서 수집)
        self._table_summary_store: Dict[Tuple[str, str], Dict[str, Any]] = {}

    # =========================================================================
    # Phase 1: 정적 그래프 쿼리 생성
    # =========================================================================
    
    def build_static_graph_queries(self) -> List[str]:
        """[Phase 1] AST를 수집하고 정적 그래프 쿼리를 생성합니다.
        
        Returns:
            정적 노드 및 관계 생성 쿼리 리스트
        """
        log_process("ANALYZE", "PHASE1", f"🏗️ {self.full_directory} 정적 그래프 생성")
        
        # AST 수집
        collector = StatementCollector(
            self.antlr_data, self.file_content, self.directory, self.file_name
        )
        self._nodes, self._procedures = collector.collect()
        
        if not self._nodes:
            log_process("ANALYZE", "PHASE1", f"⚠️ {self.full_directory}: 분석 대상 노드 없음")
            return []
        
        # 정적 노드 쿼리 생성
        queries: List[str] = []
        file_node = None
        for node in self._nodes:
            queries.extend(self._build_static_node_queries(node))
            if node.node_type == "FILE":
                file_node = node
        
        # Project → File (CONTAINS) 관계 생성
        if file_node:
            queries.extend(self._build_project_file_relationship())
        
        # 관계 쿼리 생성
        queries.extend(self._build_relationship_queries())
        
        log_process("ANALYZE", "PHASE1", f"✅ {self.full_directory}: {len(queries)}개 쿼리 생성")
        return queries

    def _build_static_node_queries(self, node: StatementNode) -> List[str]:
        """정적 노드 생성 쿼리 리스트를 반환합니다."""
        queries: List[str] = []
        label = node.node_type
        
        # name 속성 결정: PROCEDURE/FUNCTION는 실제 이름, 그 외는 타입[라인번호]
        if label == "FILE":
            node_name = self.file_name
        elif label in PROCEDURE_TYPES and node.procedure_name:
            node_name = node.procedure_name
        else:
            node_name = f"{label}[{node.start_line}]"
        
        escaped_name = escape_for_cypher(node_name)
        has_children = "true" if node.has_children else "false"
        escaped_code = escape_for_cypher(node.code)
        
        base_set = [
            f"n.endLine = {node.end_line}",
            f"n.name = '{escaped_name}'",
            f"n.node_code = '{escaped_code}'",
            f"n.token = {node.token}",
            f"n.has_children = {has_children}",
        ]
        
        # PROCEDURE/FUNCTION: procedure_name, schema_name, procedure_type 속성 추가
        if label in PROCEDURE_TYPES and node.procedure_name:
            base_set.append(f"n.procedure_name = '{escape_for_cypher(node.procedure_name)}'")
            base_set.append(f"n.procedure_type = '{label}'")
            if node.schema_name:
                base_set.append(f"n.schema_name = '{escape_for_cypher(node.schema_name)}'")
        # 그 외 노드: 소속 프로시저 정보 저장
        elif node.procedure_name:
            base_set.append(f"n.procedure_name = '{escape_for_cypher(node.procedure_name)}'")
            if node.schema_name:
                base_set.append(f"n.schema_name = '{escape_for_cypher(node.schema_name)}'")
        
        if node.has_children:
            escaped_placeholder = escape_for_cypher(node.get_placeholder_code())
            base_set.append(f"n.summarized_code = '{escaped_placeholder}'")
        
        base_set_str = ", ".join(base_set)
        
        # PROCEDURE/FUNCTION 노드: MERGE로 생성 (중복 방지)
        if label in PROCEDURE_TYPES and node.procedure_name:
            escaped_proc_name = escape_for_cypher(node.procedure_name)
            escaped_schema = escape_for_cypher(node.schema_name or "")
            schema_match = f"schema_name: '{escaped_schema}', " if node.schema_name else ""
            queries.append(
                f"MERGE (n:{label} {{{schema_match}procedure_name: '{escaped_proc_name}', user_id: '{self.user_id}', project_name: '{self.project_name}'}})\n"
                f"SET n.startLine = {node.start_line}, n.directory = '{escape_for_cypher(self.full_directory)}', n.file_name = '{self.file_name}', {base_set_str}\n"
                f"RETURN n"
            )
        else:
            queries.append(
                f"MERGE (n:{label} {{startLine: {node.start_line}, {self.node_base_props}}})\n"
                f"SET {base_set_str}\n"
                f"RETURN n"
            )
        return queries

    def _build_project_file_relationship(self) -> List[str]:
        """Project → File (CONTAINS) 관계 쿼리를 생성합니다."""
        escaped_file_name = escape_for_cypher(self.file_name)
        escaped_dir = escape_for_cypher(self.full_directory)
        return [
            f"MATCH (p:Project {{user_id: '{self.user_id}', name: '{escape_for_cypher(self.project_name)}'}})\n"
            f"MATCH (f:FILE {{startLine: 1, directory: '{escaped_dir}', file_name: '{escaped_file_name}', user_id: '{self.user_id}', project_name: '{self.project_name}'}})\n"
            f"MERGE (p)-[r:CONTAINS]->(f)\n"
            f"RETURN r"
        ]

    def _build_relationship_queries(self) -> List[str]:
        """정적 관계 쿼리 (CONTAINS, PARENT_OF, NEXT)를 생성합니다.
        
        규칙:
        - File → PROCEDURE/FUNCTION/TRIGGER (최상위 타입만): CONTAINS
        - 그 외 부모-자식: PARENT_OF
        - 형제 관계: NEXT
        """
        queries: List[str] = []
        
        for node in self._nodes or []:
            # File → 최상위 타입(PROCEDURE/FUNCTION/TRIGGER)만 CONTAINS, 그 외: PARENT_OF
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
    
    def _build_contains_query(self, parent: StatementNode, child: StatementNode) -> str:
        """CONTAINS 관계 쿼리를 생성합니다 (File → 직접 자식만)."""
        return (
            f"MATCH (parent:{parent.node_type} {{startLine: {parent.start_line}, {self.node_base_props}}})\n"
            f"MATCH (child:{child.node_type} {{startLine: {child.start_line}, {self.node_base_props}}})\n"
            f"MERGE (parent)-[r:CONTAINS]->(child)\n"
            f"RETURN r"
        )

    # =========================================================================
    # Phase 2: LLM 분석
    # =========================================================================
    
    async def run_llm_analysis(self) -> Tuple[List[str], int, List[Dict[str, Any]]]:
        """[Phase 2] LLM 분석을 실행하고 업데이트 쿼리를 생성합니다.
        
        중요: 자식→부모 요약 의존성을 보장하기 위해 completion_event 기반 대기
        - 부모 노드는 자식 노드의 completion_event를 기다린 후 실행
        - leaf 노드는 바로 실행, parent 노드는 자식 완료 후 실행
        
        Returns:
            (분석 결과 업데이트 쿼리 리스트, 실패한 배치 수, 실패 상세 정보 리스트)
        """
        if self._nodes is None:
            raise AnalysisError(f"Phase 1이 먼저 실행되어야 합니다: {self.file_name}")
        
        log_process("ANALYZE", "PHASE2", f"🤖 {self.full_directory} LLM 분석 시작")
        
        all_queries: List[str] = []
        failed_batch_count = 0
        all_failed_details: List[Dict[str, Any]] = []
        
        # 변수 선행 처리
        variable_queries = await self._analyze_variable_nodes()
        all_queries.extend(variable_queries)
        
        # 배치 분석
        planner = BatchPlanner()
        batches = planner.plan(self._nodes, self.full_directory)
        
        if not batches:
            log_process("ANALYZE", "PHASE2", f"⚠️ {self.full_directory}: 분석 대상 배치 없음")
            return all_queries, 0, []
        
        log_process("ANALYZE", "PHASE2", f"📊 배치 {len(batches)}개 (completion_event 기반 의존성 보장)")
        
        # 프로시저별 summary 수집용 저장소 (배치 처리 전에 초기화)
        procedure_summary_store: Dict[str, Dict[str, str]] = {key: {} for key in (self._procedures or {})}
        
        # LLM 호출 및 결과 처리
        invoker = LLMInvoker(self.api_key, self.locale)
        
        async def process_batch(batch: AnalysisBatch, semaphore: asyncio.Semaphore) -> Tuple[List[str], Dict[str, Any]]:
            """배치 처리 후 쿼리와 분석 결과 반환. 노드에 summary도 설정.
            
            핵심: 부모 노드는 자식 completion_event를 기다린 후 실행됨
            → 깊이 계산 없이 자연스럽게 leaf → parent 순서 보장
            
            중요: 
            - try/finally로 completion_event.set()을 보장하여 데드락 방지
            - 자식 중 ok=False가 있으면 부모도 ok=False (불완전 요약 전파)
            """
            batch_failed = False
            async with semaphore:
                try:
                    # 1. 배치 내 모든 노드의 자식 완료를 기다림 (기존 방식 복원)
                    for node in batch.nodes:
                        if node.has_children:
                            for child in node.children:
                                await child.completion_event.wait()
                                # 자식 중 하나라도 실패하면 부모도 불완전
                                if not child.ok:
                                    node.ok = False
                    
                    log_process("ANALYZE", "LLM", f"배치 #{batch.batch_id} 처리 중 ({len(batch.nodes)}개 노드)")
                    general_result, table_result = await invoker.invoke(batch)
                    
                    # 2. 노드에 summary 설정
                    if general_result:
                        analysis_list = general_result.get("analysis") or []
                        for node, analysis in zip(batch.nodes, analysis_list):
                            if analysis:
                                node.summary = analysis.get("summary") or ""
                    
                    queries = self._build_analysis_queries(batch, general_result, table_result, procedure_summary_store)
                    return queries, {"batch": batch, "general_result": general_result}
                except Exception:
                    # 배치 실패 시 모든 노드를 ok=False로 마킹
                    batch_failed = True
                    for node in batch.nodes:
                        node.ok = False
                    raise
                finally:
                    # 3. 무조건 completion_event 설정 (실패해도 부모가 대기하지 않도록)
                    for node in batch.nodes:
                        node.completion_event.set()
        
        def collect_results(batch_results: list, batches_list: List[AnalysisBatch], level_name: str) -> Tuple[int, List[Dict[str, Any]]]:
            """배치 결과를 수집하고 (실패 수, 실패 상세 정보) 반환."""
            nonlocal all_queries
            fail_count = 0
            failed_details: List[Dict[str, Any]] = []
            
            for i, result in enumerate(batch_results):
                if isinstance(result, Exception):
                    batch = batches_list[i] if i < len(batches_list) else None
                    batch_id = batch.batch_id if batch else i
                    node_ranges = ", ".join(f"L{n.start_line}-{n.end_line}" for n in batch.nodes) if batch else "unknown"
                    error_msg = str(result)[:100]  # 최대 100자
                    
                    log_process("ANALYZE", "ERROR", f"[{level_name}] 배치 #{batch_id} 실패 ({node_ranges}): {error_msg}", logging.ERROR)
                    fail_count += 1
                    failed_details.append({
                        "batch_id": batch_id,
                        "node_ranges": node_ranges,
                        "error": error_msg
                    })
                else:
                    queries, _ = result
                    all_queries.extend(queries)
            return fail_count, failed_details
        
        # 모든 배치 병렬 실행 (completion_event가 순서 보장)
        # - leaf 배치: 자식이 없으므로 바로 실행
        # - parent 배치: 자식 completion_event.wait() 후 실행 → 자연스럽게 순서 보장
        semaphore = asyncio.Semaphore(min(self.max_workers, len(batches)))
        batch_results = await asyncio.gather(
            *[process_batch(b, semaphore) for b in batches],
            return_exceptions=True
        )
        fail_count, failed_details = collect_results(batch_results, batches, "LLM")
        failed_batch_count += fail_count
        all_failed_details.extend(failed_details)
        
        # 프로시저별 summary 처리
        if self._procedures:
            proc_queries = await self._process_procedure_summaries(procedure_summary_store)
            all_queries.extend(proc_queries)
        
        # 테이블/컬럼 설명 요약 처리
        table_queries = await self._finalize_table_summaries()
        all_queries.extend(table_queries)
        
        # 실패 통계 로깅
        if failed_batch_count > 0:
            log_process("ANALYZE", "PHASE2", f"⚠️ {self.full_directory}: {failed_batch_count}개 배치 실패", logging.WARNING)
        
        log_process("ANALYZE", "PHASE2", f"✅ {self.full_directory}: {len(all_queries)}개 업데이트 쿼리")
        return all_queries, failed_batch_count, all_failed_details

    async def _analyze_variable_nodes(self) -> List[str]:
        """변수 선언 노드를 분석하고 쿼리를 생성합니다."""
        queries: List[str] = []
        variable_nodes = [n for n in (self._nodes or []) if n.node_type in VARIABLE_DECLARATION_TYPES]
        
        if not variable_nodes:
            return queries
        
        semaphore = asyncio.Semaphore(VARIABLE_CONCURRENCY)
        
        async def analyze_one(node: StatementNode) -> List[str]:
            async with semaphore:
                try:
                    result = await asyncio.to_thread(
                        analyze_variables, node.code, self.api_key, self.locale
                    )
                    return self._build_variable_queries(node, result)
                except Exception as e:
                    log_process("ANALYZE", "VARIABLE", f"❌ 변수 분석 실패 (node={node.start_line}): {e}", logging.ERROR, e)
                    return []
        
        results = await asyncio.gather(*[analyze_one(n) for n in variable_nodes])
        for r in results:
            queries.extend(r)
        
        return queries

    def _build_variable_queries(self, node: StatementNode, result: Dict[str, Any]) -> List[str]:
        """변수 분석 결과를 Neo4j 쿼리로 변환합니다."""
        queries: List[str] = []
        
        if not isinstance(result, dict):
            return queries
        
        variables = result.get("variables") or []
        if not variables:
            return queries
        
        node_match = (
            f"MATCH (n:{node.node_type} {{startLine: {node.start_line}, {self.node_base_props}}})"
        )
        
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
            
            # 변수 노드 생성 및 관계 연결
            queries.append(
                f"{node_match}\n"
                f"MERGE (v:Variable {{name: '{escaped_name}', {self.node_base_props}}})\n"
                f"SET v.type = '{escaped_type}', v.role = '{escaped_role}', v.description = '{escaped_desc}'\n"
                f"MERGE (n)-[:DECLARES]->(v)\n"
                f"RETURN v"
            )
        
        return queries

    def _build_analysis_queries(
        self,
        batch: AnalysisBatch,
        general_result: Optional[Dict[str, Any]],
        table_result: Optional[Dict[str, Any]],
        procedure_summary_store: Optional[Dict[str, Dict[str, str]]] = None,
    ) -> List[str]:
        """LLM 분석 결과를 MATCH 기반 업데이트 쿼리로 변환합니다.
        
        처리 항목:
        - 노드 summary 업데이트
        - CALL 관계 생성 (internal/external scope)
        - 변수 사용 마킹
        - 테이블/컬럼/FK/DBLink 관계 생성
        """
        queries: List[str] = []
        
        # 일반 분석 결과 처리
        if general_result:
            analysis_list = general_result.get("analysis") or []
            for node, analysis in zip(batch.nodes, analysis_list):
                if not analysis:
                    continue
                
                # 1) Summary 업데이트
                summary = analysis.get("summary") or ""
                if summary:
                    escaped_summary = escape_for_cypher(str(summary))
                    escaped_code = escape_for_cypher(node.code)
                    node_name = build_statement_name(node.node_type, node.start_line)
                    escaped_node_name = escape_for_cypher(node_name)
                    
                    queries.append(
                        f"MATCH (n:{node.node_type} {{startLine: {node.start_line}, {self.node_base_props}}})\n"
                        f"SET n.endLine = {node.end_line}, n.name = '{escaped_node_name}', "
                        f"n.summary = '{escaped_summary}', n.node_code = '{escaped_code}', "
                        f"n.token = {node.token}, n.procedure_name = '{escape_for_cypher(node.procedure_name or '')}', "
                        f"n.has_children = {'true' if node.has_children else 'false'}\n"
                        f"RETURN n"
                    )
                    
                    # 프로시저별 summary 저장소 업데이트
                    if procedure_summary_store is not None and node.procedure_key:
                        if node.procedure_key in procedure_summary_store:
                            key = f"{node.node_type}_{node.start_line}_{node.end_line}"
                            procedure_summary_store[node.procedure_key][key] = summary
                
                # 2) CALL 관계 생성
                for call_name in analysis.get('calls', []) or []:
                    if '.' in call_name:
                        # 외부 호출: 패키지.프로시저 형태
                        package_raw, proc_raw = call_name.split('.', 1)
                        package_name = escape_for_cypher(package_raw.strip())
                        proc_name = escape_for_cypher(proc_raw.strip())
                        queries.append(
                            f"MATCH (c:{node.node_type} {{startLine: {node.start_line}, {self.node_base_props}}})\n"
                            f"MERGE (target:PROCEDURE {{directory: '{package_name}', procedure_name: '{proc_name}', "
                            f"user_id: '{self.user_id}', project_name: '{self.project_name}'}})\n"
                            f"MERGE (c)-[r:CALL {{scope: 'external'}}]->(target)\n"
                            f"RETURN c, target, r"
                        )
                    else:
                        # 내부 호출: 같은 파일 내 프로시저
                        escaped_call = escape_for_cypher(call_name)
                        queries.append(
                            f"MATCH (c:{node.node_type} {{startLine: {node.start_line}, {self.node_base_props}}})\n"
                            f"MATCH (p {{procedure_name: '{escaped_call}', {self.node_base_props}}})\n"
                            f"WHERE p:PROCEDURE OR p:FUNCTION\n"
                            f"MERGE (c)-[r:CALL {{scope: 'internal'}}]->(p)\n"
                            f"RETURN c, p, r"
                        )
                
                # 3) 변수 사용 마킹
                for var_name in analysis.get('variables', []) or []:
                    queries.append(
                        f"MATCH (v:Variable {{name: '{escape_for_cypher(var_name)}', {self.node_base_props}}})\n"
                        f"SET v.`{node.start_line}_{node.end_line}` = 'Used'\n"
                        f"RETURN v"
                    )
        
        # 테이블 분석 결과 처리
        if table_result:
            table_queries = self._build_table_queries(batch, table_result)
            queries.extend(table_queries)
        
        return queries

    async def _process_procedure_summaries(
        self,
        procedure_summary_store: Dict[str, Dict[str, str]]
    ) -> List[str]:
        """프로시저별 summary를 청크 기반으로 처리하여 최종 summary + User Story 생성.
        
        처리 흐름:
        1. 토큰 기준으로 summaries를 청크로 분할
        2. 각 청크를 병렬로 처리하여 summary 생성
        3. 생성된 summary들을 하나로 합치기
        4. 최종 summary로 User Story 생성
        """
        queries: List[str] = []
        
        if not self._procedures:
            return queries
        
        for proc_key, info in self._procedures.items():
            summaries = procedure_summary_store.get(proc_key, {})
            if not summaries:
                continue
            
            # 프로시저 최상위 노드 찾기 (하위 분석 실패 확인용)
            proc_root = next(
                (n for n in (self._nodes or []) 
                 if n.procedure_key == proc_key and n.parent is None),
                None,
            )
            # 하위 노드 중 실패가 있으면 최종 summary/UserStory 스킵
            if proc_root and not proc_root.ok:
                log_process("ANALYZE", "SUMMARY", f"⚠️ {info.procedure_name}: 하위 분석 실패로 최종 summary 생성 스킵")
                continue
            
            try:
                # 1단계: 토큰 기준으로 청크 분할
                chunks = self._split_summaries_by_token(summaries, MAX_SUMMARY_CHUNK_TOKEN)
                
                if not chunks:
                    continue
                
                log_process("ANALYZE", "SUMMARY", f"📦 {info.procedure_name}: summary 청크 분할 ({len(chunks)}개)")
                
                # 2단계: 각 청크를 병렬로 처리하여 summary 생성
                async def process_chunk(chunk: dict) -> str:
                    result = await asyncio.to_thread(
                        analyze_summary_only, chunk, self.api_key, self.locale, ""
                    )
                    if isinstance(result, dict):
                        return result.get('summary', '')
                    return ""
                
                chunk_results = await asyncio.gather(*[process_chunk(c) for c in chunks])
                chunk_results = [r for r in chunk_results if r]
                
                if not chunk_results:
                    continue
                
                # 3단계: 모든 청크의 summary를 하나로 합치기
                if len(chunk_results) == 1:
                    final_summary = chunk_results[0]
                else:
                    combined = {f"CHUNK_{i+1}": s for i, s in enumerate(chunk_results)}
                    result = await asyncio.to_thread(
                        analyze_summary_only, combined, self.api_key, self.locale, ""
                    )
                    if isinstance(result, dict):
                        final_summary = result.get('summary', "\n\n".join(chunk_results))
                    else:
                        final_summary = "\n\n".join(chunk_results)
                
                log_process("ANALYZE", "SUMMARY", f"✅ {info.procedure_name}: summary 통합 완료")
                
                # 4단계: User Story 생성
                all_user_stories = []
                if final_summary:
                    us_result = await asyncio.to_thread(
                        analyze_user_story, final_summary, self.api_key, self.locale
                    )
                    if isinstance(us_result, dict):
                        all_user_stories = us_result.get('user_stories', []) or []
                
                # 5단계: Neo4j 쿼리 생성
                summary_json = json.dumps(final_summary, ensure_ascii=False)
                queries.append(
                    f"MATCH (n:{info.procedure_type} {{procedure_name: '{escape_for_cypher(info.procedure_name)}', {self.node_base_props}}})\n"
                    f"SET n.summary = {summary_json}\n"
                    f"RETURN n"
                )
                
                # User Story 노드 및 관계 생성
                proc_name_escaped = escape_for_cypher(info.procedure_name)
                for us_idx, us in enumerate(all_user_stories, 1):
                    us_id = us.get('id', f"US-{us_idx}")
                    role = escape_for_cypher(us.get('role', ''))
                    goal = escape_for_cypher(us.get('goal', ''))
                    benefit = escape_for_cypher(us.get('benefit', ''))
                    
                    queries.append(
                        f"MATCH (p:{info.procedure_type} {{procedure_name: '{proc_name_escaped}', {self.node_base_props}}})\n"
                        f"MERGE (us:UserStory {{id: '{us_id}', procedure_name: '{proc_name_escaped}', {self.node_base_props}}})\n"
                        f"SET us.role = '{role}', us.goal = '{goal}', us.benefit = '{benefit}'\n"
                        f"MERGE (p)-[r:HAS_USER_STORY]->(us)\n"
                        f"RETURN p, us, r"
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
                            f"MATCH (us:UserStory {{id: '{us_id}', {self.node_base_props}}})\n"
                            f"MERGE (ac:AcceptanceCriteria {{id: '{ac_id}', user_story_id: '{us_id}', {self.node_base_props}}})\n"
                            f"SET ac.title = '{ac_title}', ac.given = {ac_given}, ac.when = {ac_when}, ac.then = {ac_then}\n"
                            f"MERGE (us)-[r:HAS_AC]->(ac)\n"
                            f"RETURN us, ac, r"
                        )
                
                us_count = len(all_user_stories)
                log_process("ANALYZE", "SUMMARY", f"✅ {info.procedure_name}: User Story {us_count}개 생성")
                
            except Exception as exc:
                log_process("ANALYZE", "SUMMARY", f"❌ {info.procedure_name} 프로시저 요약 오류", logging.ERROR, exc)
        
        return queries
    
    def _split_summaries_by_token(self, summaries: dict, max_token: int) -> List[dict]:
        """토큰 기준으로 summaries를 청크로 분할합니다."""
        if not summaries:
            return []
        
        chunks = []
        current_chunk = {}
        current_tokens = 0
        
        for key, value in summaries.items():
            item_text = f"{key}: {value}"
            item_tokens = calculate_code_token(item_text)
            
            if current_tokens + item_tokens > max_token and current_chunk:
                chunks.append(current_chunk)
                current_chunk = {}
                current_tokens = 0
            
            current_chunk[key] = value
            current_tokens += item_tokens
        
        if current_chunk:
            chunks.append(current_chunk)
        
        return chunks

    def _build_parent_of_query(self, parent: StatementNode, child: StatementNode) -> str:
        """부모-자식 관계 쿼리를 생성합니다."""
        return (
            f"MATCH (parent:{parent.node_type} {{startLine: {parent.start_line}, {self.node_base_props}}})\n"
            f"MATCH (child:{child.node_type} {{startLine: {child.start_line}, {self.node_base_props}}})\n"
            f"MERGE (parent)-[r:PARENT_OF]->(child)\n"
            f"RETURN r"
        )

    def _build_next_query(self, prev: StatementNode, current: StatementNode) -> str:
        """형제 관계 쿼리를 생성합니다."""
        return (
            f"MATCH (prev:{prev.node_type} {{startLine: {prev.start_line}, {self.node_base_props}}})\n"
            f"MATCH (current:{current.node_type} {{startLine: {current.start_line}, {self.node_base_props}}})\n"
            f"MERGE (prev)-[r:NEXT]->(current)\n"
            f"RETURN r"
        )

    def _build_table_queries(
        self,
        batch: AnalysisBatch,
        table_result: Dict[str, Any]
    ) -> List[str]:
        """DML 테이블 분석 결과를 Neo4j 쿼리 리스트로 변환합니다.
        
        처리 항목:
        - 테이블 노드 및 DML 관계 (FROM/INTO)
        - 컬럼 노드 및 HAS_COLUMN 관계
        - DBLink 처리
        - FK 관계 (FK_TO, FK_TO_TABLE)
        """
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
            except (TypeError, ValueError):
                continue
            
            node = node_map.get((start_line, end_line))
            if not node:
                continue
            
            node_merge = f"MATCH (n:{node.node_type} {{startLine: {node.start_line}, {self.node_base_props}}})"
            
            # CREATE_TEMP_TABLE 처리
            if node.node_type == 'CREATE_TEMP_TABLE':
                for entry in tables:
                    table_name = (entry.get('table') or '').strip()
                    if not table_name:
                        continue
                    schema_part, name_part, _ = parse_table_identifier(table_name)
                    queries.append(
                        f"{node_merge}\n"
                        f"SET n:Table, n.name = '{escape_for_cypher(name_part)}', "
                        f"n.schema = '{escape_for_cypher(schema_part)}', n.db = '{self.dbms}'\n"
                        f"RETURN n"
                    )
                continue
            
            # 일반 DML 테이블 처리
            for entry in tables:
                table_name = (entry.get('table') or '').strip()
                if not table_name:
                    continue
                
                schema_part, name_part, db_link_value = parse_table_identifier(table_name)
                
                # 접근 모드에 따른 관계 타입 결정
                access_mode = (entry.get('accessMode') or entry.get('mode') or 'r').lower()
                rel_types = []
                if 'r' in access_mode:
                    rel_types.append(TABLE_RELATIONSHIP_MAP.get('r', 'FROM'))
                if 'w' in access_mode:
                    rel_types.append(TABLE_RELATIONSHIP_MAP.get('w', 'WRITES'))
                
                table_merge = self._build_table_merge(name_part, schema_part)
                
                # 테이블 설명을 버킷에 누적 (후속 요약용)
                table_desc_raw = entry.get('tableDescription') or entry.get('description') or ''
                bucket_key = self._record_table_summary(schema_part, name_part, table_desc_raw)
                
                # 테이블 노드 및 관계 생성
                table_query = f"{node_merge}\nWITH n\n{table_merge}\nSET t.db = coalesce(t.db, '{self.dbms}')"
                
                if db_link_value:
                    table_query += f"\nSET t.db_link = COALESCE(t.db_link, '{db_link_value}')"
                
                for i, rel_type in enumerate(rel_types):
                    table_query += f"\nMERGE (n)-[r{i}:{rel_type}]->(t)"
                
                table_query += "\nRETURN n, t"
                queries.append(table_query)
                
                # 컬럼 처리
                for column in entry.get('columns', []) or []:
                    column_name = (column.get('name') or '').strip()
                    if not column_name:
                        continue
                    
                    raw_dtype = column.get('dtype') or ''
                    raw_column_desc = (column.get('description') or column.get('comment') or '').strip()
                    
                    # 컬럼 설명/메타를 버킷에 누적 (후속 요약용)
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
                    
                    if schema_part:
                        fqn = '.'.join(filter(None, [schema_part, name_part, column_name])).lower()
                        queries.append(
                            f"{table_merge}\nWITH t\n"
                            f"MERGE (c:Column {{user_id: '{self.user_id}', fqn: '{fqn}', project_name: '{self.project_name}'}})\n"
                            f"SET c.name = '{escaped_col_name}', c.dtype = '{col_type}', "
                            f"c.description = '{col_desc}', c.nullable = '{nullable}'\n"
                            f"MERGE (t)-[r:HAS_COLUMN]->(c)\n"
                            f"RETURN t, c, r"
                        )
                    else:
                        queries.append(
                            f"{table_merge}\n"
                            f"WITH t, lower(case when t.schema <> '' and t.schema IS NOT NULL "
                            f"then t.schema + '.' + '{name_part}' + '.' + '{column_name}' "
                            f"else '{name_part}' + '.' + '{column_name}' end) as fqn\n"
                            f"MERGE (c:Column {{user_id: '{self.user_id}', fqn: fqn, project_name: '{self.project_name}'}})\n"
                            f"ON CREATE SET c.name = '{escaped_col_name}', c.dtype = '{col_type}', "
                            f"c.description = '{col_desc}', c.nullable = '{nullable}'\n"
                            f"MERGE (t)-[r:HAS_COLUMN]->(c)\n"
                            f"RETURN t, c, r"
                        )
            
            # DBLink 처리
            for link_item in range_entry.get('dbLinks', []) or []:
                link_name_raw = (link_item.get('name') or '').strip()
                if not link_name_raw:
                    continue
                mode = (link_item.get('mode') or 'r').lower()
                schema_link, name_link, link_name = parse_table_identifier(link_name_raw)
                remote_merge = self._build_table_merge(name_link, schema_link)
                queries.append(
                    f"{remote_merge}\nSET t.db_link = '{link_name}'\n"
                    f"WITH t\n"
                    f"MERGE (l:DBLink {{user_id: '{self.user_id}', name: '{link_name}', project_name: '{self.project_name}'}})\n"
                    f"MERGE (l)-[r1:CONTAINS]->(t)\n"
                    f"WITH t, l\n{node_merge}\n"
                    f"MERGE (n)-[r2:DB_LINK {{mode: '{mode}'}}]->(t)\n"
                    f"RETURN l, t, n"
                )
            
            # FK 관계 처리
            for relation in range_entry.get('fkRelations', []) or []:
                src_table = (relation.get('sourceTable') or '').strip()
                tgt_table = (relation.get('targetTable') or '').strip()
                src_columns = [c.strip() for c in (relation.get('sourceColumns') or []) if c]
                tgt_columns = [c.strip() for c in (relation.get('targetColumns') or []) if c]
                
                if not (src_table and tgt_table and src_columns and tgt_columns):
                    continue
                
                src_schema, src_name, _ = parse_table_identifier(src_table)
                tgt_schema, tgt_name, _ = parse_table_identifier(tgt_table)
                
                src_props = f"user_id: '{self.user_id}', schema: '{src_schema or ''}', name: '{src_name}', db: '{self.dbms}', project_name: '{self.project_name}'"
                tgt_props = f"user_id: '{self.user_id}', schema: '{tgt_schema or ''}', name: '{tgt_name}', db: '{self.dbms}', project_name: '{self.project_name}'"
                
                # 테이블 간 FK 관계
                queries.append(
                    f"MATCH (st:Table {{{src_props}}})\n"
                    f"MATCH (tt:Table {{{tgt_props}}})\n"
                    f"MERGE (st)-[r:FK_TO_TABLE]->(tt)\n"
                    f"RETURN st, tt, r"
                )
                
                # 컬럼 간 FK 관계
                for src_col, tgt_col in zip(src_columns, tgt_columns):
                    src_fqn = '.'.join(filter(None, [src_schema, src_name, src_col])).lower()
                    tgt_fqn = '.'.join(filter(None, [tgt_schema, tgt_name, tgt_col])).lower()
                    queries.append(
                        f"MATCH (sc:Column {{user_id: '{self.user_id}', fqn: '{src_fqn}', project_name: '{self.project_name}'}})\n"
                        f"MATCH (dc:Column {{user_id: '{self.user_id}', fqn: '{tgt_fqn}', project_name: '{self.project_name}'}})\n"
                        f"MERGE (sc)-[r:FK_TO]->(dc)\n"
                        f"RETURN sc, dc, r"
                    )
        
        return queries
    
    def _build_table_merge(self, table_name: str, schema: Optional[str]) -> str:
        """테이블 MERGE 쿼리를 생성합니다.
        
        스키마가 없으면 default_schema를 사용합니다.
        """
        schema_value = schema or self.default_schema
        return (
            f"MERGE (t:Table {{{self.table_base_props}, name: '{table_name}', schema: '{schema_value}', db: '{self.dbms}', project_name: '{self.project_name}'}})"
        )
    
    # =========================================================================
    # 테이블/컬럼 설명 요약 처리
    # =========================================================================
    
    def _record_table_summary(self, schema: Optional[str], name: str, description: Optional[str]) -> Tuple[str, str]:
        """테이블 설명 문장을 버킷에 누적합니다.
        
        스키마가 없으면 default_schema를 사용합니다.
        """
        schema_key = schema or self.default_schema
        name_key = name
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
        """컬럼 설명과 메타데이터를 버킷에 누적합니다."""
        text = (description or '').strip()
        bucket = self._table_summary_store.setdefault(table_key, {"summaries": set(), "columns": {}})
        columns = bucket["columns"]
        canonical = column_name
        entry = columns.get(canonical)
        if entry is None:
            entry = {"name": column_name, "summaries": set(), "dtype": (dtype or ''), "nullable": True if nullable is None else bool(nullable), "examples": set()}
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
        """버킷에 모은 테이블/컬럼 설명을 병렬로 요약합니다."""
        if not self._table_summary_store:
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
        """테이블/컬럼 설명 버킷을 기반으로 LLM 요약을 생성합니다.
        
        DDL 메타데이터를 LLM 입력에 포함하여 통합된 description을 생성합니다.
        """
        schema_key, name_key = table_key
        
        # DDL 메타데이터 조회 (메모리 캐시) - 먼저 조회하여 체크에 활용
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
        
        # DDL description이 있으면 summaries에 추가 (LLM 입력에 포함)
        if ddl_description:
            summaries.insert(0, f"[DDL 메타데이터] {ddl_description}")
        
        # DDL 컬럼 description도 column_sentences에 추가
        for col_name, ddl_col in ddl_columns.items():
            ddl_col_desc = (ddl_col.get('description') or '').strip()
            if ddl_col_desc and col_name not in column_sentences:
                column_sentences[col_name] = [f"[DDL 메타데이터] {ddl_col_desc}"]
            elif ddl_col_desc and col_name in column_sentences:
                column_sentences[col_name].insert(0, f"[DDL 메타데이터] {ddl_col_desc}")
        
        # DDL 메타데이터나 DML 분석 결과가 하나라도 있어야 처리
        if not summaries and not column_sentences:
            return []
        
        # DDL 컬럼 정보를 column_metadata에 병합
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
        
        # DDL 컬럼도 column_metadata에 추가 (DML에서 발견되지 않은 컬럼)
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
            return []
        
        queries: List[str] = []
        # LLM이 생성한 tableDescription을 그대로 description에 할당
        llm_table_desc = (result.get('tableDescription') or '').strip()
        schema_prop = schema_key
        table_props = (
            f"user_id: '{self.user_id}', schema: '{schema_prop}', name: '{name_key}', db: '{self.dbms}', project_name: '{self.project_name}'"
        )
        
        if llm_table_desc:
            queries.append(
                f"MATCH (t:Table {{{table_props}}})\nSET t.description = '{escape_for_cypher(llm_table_desc)}'\nRETURN t"
            )
        
        # 컬럼 description 처리
        for column_info in result.get('columns', []) or []:
            column_name = (column_info.get('name') or '').strip()
            llm_column_desc = (column_info.get('description') or '').strip()
            if not column_name or not llm_column_desc:
                continue
            
            fqn = '.'.join(filter(None, [schema_prop, name_key, column_name])).lower()
            column_props = (
                f"user_id: '{self.user_id}', name: '{column_name}', fqn: '{fqn}', project_name: '{self.project_name}'"
            )
            queries.append(
                f"MATCH (c:Column {{{column_props}}})\nSET c.description = '{escape_for_cypher(llm_column_desc)}'\nRETURN c"
            )
        
        return queries
