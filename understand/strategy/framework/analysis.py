"""Java/Framework Understanding 파이프라인 구현.

이 모듈은 Java 코드를 분석하여 클래스 다이어그램 생성에 필요한 정보를
Neo4j 그래프로 구축합니다. DBMS 분석 파이프라인과 동일한 구조를 따릅니다.

주요 흐름:
1. AST 수집 (StatementCollector)
2. 정적 그래프 초기화
3. 선행 처리 (병렬):
   - 상속/구현 관계 추출 (EXTENDS, IMPLEMENTS 노드)
   - 필드 정보 추출 (FIELD 노드)
4. 배치 분석 (LLM 호출)
5. 클래스 요약 생성
"""

from __future__ import annotations

import asyncio
import logging
import os
import re
from dataclasses import dataclass, field
from typing import Any, Dict, List, Optional, Tuple

from util.rule_loader import RuleLoader
from util.exception import LLMCallError, ProcessAnalyzeCodeError, UnderstandingError
from util.utility_tool import calculate_code_token, escape_for_cypher, log_process


# ==================== 상수 정의 ====================
NON_ANALYSIS_TYPES = frozenset(["FILE", "PACKAGE", "IMPORT"])
CLASS_TYPES = frozenset(["CLASS", "INTERFACE", "ENUM", "RECORD", "ANNOTATION_TYPE"])
INHERITANCE_TYPES = frozenset(["EXTENDS", "IMPLEMENTS"])
FIELD_TYPES = frozenset(["FIELD"])
METHOD_TYPES = frozenset(["METHOD", "CONSTRUCTOR"])
MAX_BATCH_TOKEN = int(os.getenv("FRAMEWORK_MAX_BATCH_TOKEN", "1000"))
MAX_CONCURRENCY = int(os.getenv("FRAMEWORK_MAX_CONCURRENCY", "5"))
INHERITANCE_CONCURRENCY = int(os.getenv("INHERITANCE_CONCURRENCY", "5"))
FIELD_CONCURRENCY = int(os.getenv("FIELD_CONCURRENCY", "5"))
METHOD_CONCURRENCY = int(os.getenv("METHOD_CONCURRENCY", "5"))
STATIC_QUERY_BATCH_SIZE = 40
LINE_NUMBER_PATTERN = re.compile(r"^(\d+)\s*:")


# ===== RuleLoader 헬퍼 =====
def _rule_loader() -> RuleLoader:
    return RuleLoader(target_lang="framework", domain="understand")


def understand_code(code: str, ranges: list, count: int, api_key: str, locale: str) -> Dict[str, Any]:
    """코드 범위별 분석 - summary, calls, variables 추출."""
    return _rule_loader().execute(
        "analysis",
        {"code": code, "ranges": ranges, "count": count, "locale": locale},
        api_key,
    )


def understand_class_summary(summaries: dict, api_key: str, locale: str) -> Dict[str, Any]:
    """클래스 전체 요약 생성."""
    return _rule_loader().execute(
        "class_summary",
        {"summaries": summaries, "locale": locale},
        api_key,
    )


def understand_inheritance(declaration_code: str, api_key: str, locale: str) -> Dict[str, Any]:
    """상속/구현 관계 추출."""
    return _rule_loader().execute(
        "inheritance",
        {"declaration_code": declaration_code, "locale": locale},
        api_key,
    )


def understand_field(declaration_code: str, api_key: str, locale: str) -> Dict[str, Any]:
    """필드 정보 추출."""
    return _rule_loader().execute(
        "field",
        {"declaration_code": declaration_code, "locale": locale},
        api_key,
    )


def understand_method(declaration_code: str, api_key: str, locale: str) -> Dict[str, Any]:
    """메서드 시그니처 분석 - 파라미터/반환 타입 추출."""
    return _rule_loader().execute(
        "method",
        {"declaration_code": declaration_code, "locale": locale},
        api_key,
    )


# ==================== 데이터 클래스 ====================
@dataclass(slots=True)
class StatementNode:
    """평탄화된 AST 노드를 표현합니다."""
    node_id: int
    start_line: int
    end_line: int
    node_type: str
    code: str
    token: int
    has_children: bool
    analyzable: bool
    class_key: Optional[str]
    class_name: Optional[str]
    class_kind: Optional[str]
    lines: List[Tuple[int, str]] = field(default_factory=list)
    parent: Optional["StatementNode"] = None
    children: List["StatementNode"] = field(default_factory=list)
    summary: Optional[str] = None
    completion_event: asyncio.Event = field(init=False, repr=False)

    def __post_init__(self):
        object.__setattr__(self, "completion_event", asyncio.Event())

    def get_raw_code(self) -> str:
        """라인 번호를 포함하여 노드의 원문 코드를 반환합니다."""
        return "\n".join(f"{ln}: {text}" for ln, text in self.lines)

    def get_compact_code(self) -> str:
        """자식 구간은 자식 요약(없으면 placeholder)으로 치환한 코드."""
        if not self.children:
            return self.get_raw_code()
        result: List[str] = []
        idx = 0
        total = len(self.lines)
        sorted_children = sorted(self.children, key=lambda c: c.start_line)
        for child in sorted_children:
            while idx < total and self.lines[idx][0] < child.start_line:
                ln, text = self.lines[idx]
                result.append(f"{ln}: {text}")
                idx += 1
            if child.summary:
                result.append(f"{child.start_line}~{child.end_line}: {child.summary.strip()}")
            else:
                log_process(
                    "UNDERSTAND",
                    "COLLECT",
                    f"⚠️ 부모 {self.start_line}~{self.end_line}의 자식 {child.start_line}~{child.end_line} 요약 없음 - placeholder 사용",
                )
                result.append(f"{child.start_line}: ...code...")
            while idx < total and self.lines[idx][0] <= child.end_line:
                idx += 1
        while idx < total:
            ln, text = self.lines[idx]
            result.append(f"{ln}: {text}")
            idx += 1
        return "\n".join(result)

    def get_placeholder_code(self) -> str:
        """자식 구간을 placeholder로 유지한 코드를 반환합니다."""
        if not self.children:
            return self.get_raw_code()
        result: List[str] = []
        idx = 0
        total = len(self.lines)
        sorted_children = sorted(self.children, key=lambda c: c.start_line)
        for child in sorted_children:
            while idx < total and self.lines[idx][0] < child.start_line:
                ln, text = self.lines[idx]
                result.append(f"{ln}: {text}")
                idx += 1
            result.append(f"{child.start_line}: ...code...")
            while idx < total and self.lines[idx][0] <= child.end_line:
                idx += 1
        while idx < total:
            ln, text = self.lines[idx]
            result.append(f"{ln}: {text}")
            idx += 1
        return "\n".join(result)

    def get_code_with_assigns_only(self) -> str:
        """메서드 시그니처 + ASSIGN/NEW_INSTANCE 자식만 포함된 코드 (중첩 포함)."""
        if not self.children:
            return self.get_raw_code()

        target_types = {"ASSIGN", "NEW_INSTANCE"}

        def find_targets(node: "StatementNode") -> List["StatementNode"]:
            """재귀적으로 ASSIGN, NEW_INSTANCE 자식을 찾습니다."""
            targets = []
            for child in node.children:
                if child.node_type in target_types:
                    targets.append(child)
                targets.extend(find_targets(child))
            return targets

        result = [f"{self.lines[0][0]}: {self.lines[0][1]}"]  # 시그니처
        for target in sorted(find_targets(self), key=lambda n: n.start_line):
            for ln, text in target.lines:
                result.append(f"{ln}: {text}")
        result.append(f"{self.lines[-1][0]}: {self.lines[-1][1]}")  # 닫는 괄호
        return "\n".join(result)


@dataclass(slots=True)
class ClassInfo:
    """클래스/인터페이스 정보를 저장합니다."""
    key: str
    name: str
    kind: str
    node_start: int
    node_end: int
    pending_nodes: int = 0
    finalized: bool = False


@dataclass(slots=True)
class AnalysisBatch:
    """분석 배치 정보."""
    batch_id: int
    nodes: List[StatementNode]
    ranges: List[Dict[str, int]]
    progress_line: int

    def build_payload(self) -> str:
        """LLM 호출용 코드 페이로드 생성."""
        return "\n\n".join(
            node.get_compact_code() if node.has_children else node.get_raw_code()
            for node in self.nodes
        )


@dataclass(slots=True)
class BatchResult:
    """배치 처리 결과."""
    batch: AnalysisBatch
    general_result: Optional[Dict[str, Any]]


# ==================== 노드 수집기 ====================
class StatementCollector:
    """AST를 후위순회하여 StatementNode와 클래스 정보를 수집합니다."""

    def __init__(self, antlr_data: Dict[str, Any], file_content: str, system_name: str, file_name: str):
        self.antlr_data = antlr_data
        self.file_content = file_content
        self.system_name = system_name
        self.file_name = file_name
        self.nodes: List[StatementNode] = []
        self.classes: Dict[str, ClassInfo] = {}
        self._node_id = 0
        self._file_lines = file_content.split("\n")

    def collect(self) -> Tuple[List[StatementNode], Dict[str, ClassInfo]]:
        """AST 전역을 후위 순회하여 노드 목록과 클래스 정보를 생성합니다."""
        self._visit(self.antlr_data, None, None, None, None)
        return self.nodes, self.classes

    def _make_class_key(self, class_name: Optional[str], start_line: int) -> str:
        """클래스 고유키를 생성합니다."""
        base = class_name or f"anonymous_{start_line}"
        return f"{self.system_name}:{self.file_name}:{base}:{start_line}"

    def _extract_class_name(self, code: str, node_type: str) -> Optional[str]:
        """코드에서 클래스/인터페이스 이름을 추출합니다."""
        patterns = {
            "CLASS": r"\bclass\s+(\w+)",
            "INTERFACE": r"\binterface\s+(\w+)",
            "ENUM": r"\benum\s+(\w+)",
            "RECORD": r"\brecord\s+(\w+)",
            "ANNOTATION_TYPE": r"@interface\s+(\w+)",
        }
        pattern = patterns.get(node_type)
        if pattern:
            match = re.search(pattern, code, re.IGNORECASE)
            if match:
                return match.group(1)
        return None

    def _visit(
        self,
        node: Dict[str, Any],
        parent: Optional[StatementNode],
        current_class: Optional[str],
        current_class_name: Optional[str],
        current_class_kind: Optional[str],
    ) -> Optional[StatementNode]:
        """재귀적으로 AST를 내려가며 StatementNode를 생성합니다."""
        node_type = node["type"]
        start_line = node["startLine"]
        end_line = node["endLine"]
        children = node.get("children", []) or []

        # 코드 추출
        line_entries = [
            (ln, self._file_lines[ln - 1] if 0 < ln <= len(self._file_lines) else "")
            for ln in range(start_line, end_line + 1)
        ]
        code = "\n".join(f"{ln}: {txt}" for ln, txt in line_entries)

        class_key = current_class
        class_name = current_class_name
        class_kind = current_class_kind

        # 클래스/인터페이스 노드 처리
        if node_type in CLASS_TYPES:
            extracted_name = self._extract_class_name(code, node_type)
            class_key = self._make_class_key(extracted_name, start_line)
            class_name = extracted_name
            class_kind = node_type
            if class_key not in self.classes:
                self.classes[class_key] = ClassInfo(
                    key=class_key,
                    name=extracted_name or class_key,
                    kind=node_type,
                    node_start=start_line,
                    node_end=end_line,
                )
                log_process("UNDERSTAND", "COLLECT", f"📋 클래스 발견: {extracted_name} ({node_type}, 라인 {start_line}~{end_line})")

        # 자식 노드 수집
        child_nodes: List[StatementNode] = []
        for ch in children:
            cn = self._visit(ch, None, class_key, class_name, class_kind)
            if cn:
                child_nodes.append(cn)

        # 분석 가능 여부 판단 (FIELD는 선행 처리에서 ASSOCIATION으로 처리됨)
        analyzable = node_type not in NON_ANALYSIS_TYPES and node_type not in CLASS_TYPES and node_type not in FIELD_TYPES
        token = calculate_code_token(code)

        self._node_id += 1
        st = StatementNode(
            node_id=self._node_id,
            start_line=start_line,
            end_line=end_line,
            node_type=node_type,
            code=code,
            token=token,
            has_children=bool(child_nodes),
            analyzable=analyzable,
            class_key=class_key,
            class_name=class_name,
            class_kind=class_kind,
            lines=line_entries,
        )
        for c in child_nodes:
            c.parent = st
        st.children.extend(child_nodes)

        # 분석 대상 노드 카운트
        if analyzable and class_key and class_key in self.classes:
            self.classes[class_key].pending_nodes += 1

        if not analyzable and node_type not in CLASS_TYPES:
            st.completion_event.set()

        self.nodes.append(st)
        log_process(
            "UNDERSTAND",
            "COLLECT",
            f"✅ {node_type} 노드 수집 완료: 라인 {start_line}~{end_line}, 토큰 {token}, 자식 {len(child_nodes)}개",
        )
        return st


# ==================== 배치 플래너 ====================
class BatchPlanner:
    """수집된 노드를 토큰 한도 내에서 배치로 묶습니다."""

    def __init__(self, token_limit: int = MAX_BATCH_TOKEN):
        self.token_limit = token_limit

    def plan(self, nodes: List[StatementNode]) -> List[AnalysisBatch]:
        """토큰 한도를 넘지 않도록 노드를 분할하여 분석 배치를 생성합니다."""
        batches: List[AnalysisBatch] = []
        current: List[StatementNode] = []
        tokens = 0
        batch_id = 1

        for n in nodes:
            if not n.analyzable:
                continue
            if n.has_children:
                if current:
                    batches.append(self._create(batch_id, current))
                    log_process(
                        "UNDERSTAND",
                        "BATCH",
                        f"📦 배치 #{batch_id} 확정: 리프 노드 {len(current)}개 (토큰 {tokens}/{self.token_limit})",
                    )
                    batch_id += 1
                    current = []
                    tokens = 0
                batches.append(self._create(batch_id, [n]))
                log_process(
                    "UNDERSTAND",
                    "BATCH",
                    f"📦 배치 #{batch_id} 확정: 부모 노드 단독 (라인 {n.start_line}~{n.end_line}, 토큰 {n.token})",
                )
                batch_id += 1
                continue
            if current and tokens + n.token > self.token_limit:
                batches.append(self._create(batch_id, current))
                log_process(
                    "UNDERSTAND",
                    "BATCH",
                    f"📦 배치 #{batch_id} 확정: 토큰 한도 도달 (누적 {tokens}/{self.token_limit})",
                )
                batch_id += 1
                current = []
                tokens = 0
            current.append(n)
            tokens += n.token

        if current:
            batches.append(self._create(batch_id, current))
            log_process(
                "UNDERSTAND",
                "BATCH",
                f"📦 배치 #{batch_id} 확정: 마지막 리프 노드 {len(current)}개 (토큰 {tokens}/{self.token_limit})",
            )
        return batches

    def _create(self, batch_id: int, nodes: List[StatementNode]) -> AnalysisBatch:
        """배치 객체를 생성합니다."""
        ranges = [{"startLine": n.start_line, "endLine": n.end_line} for n in nodes]
        progress = max(n.end_line for n in nodes)
        return AnalysisBatch(batch_id=batch_id, nodes=nodes, ranges=ranges, progress_line=progress)


# ==================== LLM 호출 ====================
class LLMInvoker:
    """배치를 입력 받아 코드 분석을 호출합니다."""

    def __init__(self, api_key: str, locale: str):
        self.api_key = api_key
        self.locale = locale

    async def invoke(self, batch: AnalysisBatch) -> Optional[Dict[str, Any]]:
        """배치 코드를 LLM에 전달하여 분석 결과를 얻습니다."""
        if not batch.ranges:
            return None
        return await asyncio.to_thread(
            understand_code,
            batch.build_payload(),
            batch.ranges,
            len(batch.ranges),
            self.api_key,
            self.locale,
        )


# ==================== 적용 매니저 ====================
class ApplyManager:
    """LLM 결과를 순서대로 적용하고 클래스 요약을 생성합니다."""

    def __init__(
        self,
        send_queue: asyncio.Queue,
        receive_queue: asyncio.Queue,
        file_last_line: int,
        nodes: List[StatementNode],
        node_base_props: str,
        system_props: str,
        classes: Dict[str, ClassInfo],
        api_key: str,
        locale: str,
        user_id: str,
        project_name: str,
        system_name: str,
        file_name: str,
    ):
        self.send_queue = send_queue
        self.receive_queue = receive_queue
        self.file_last_line = file_last_line
        self._nodes = nodes
        self.node_base_props = node_base_props
        self.system_props = system_props
        self.classes = classes
        self.api_key = api_key
        self.locale = locale
        self.user_id = user_id
        self.project_name = project_name
        self.system_name = system_name
        self.file_name = file_name
        self.system_file = f"{system_name}-{file_name}"

        self._pending: Dict[int, BatchResult] = {}
        self._next_batch_id = 1
        self._lock = asyncio.Lock()
        self._finalized_classes: set[str] = set()
        self._class_summary_store: Dict[str, Dict[str, Any]] = {key: {} for key in classes}

    async def submit(self, batch: AnalysisBatch, general_result: Optional[Dict[str, Any]]):
        """워커가 batch 처리를 마친 뒤 Apply 큐에 등록합니다."""
        async with self._lock:
            self._pending[batch.batch_id] = BatchResult(batch=batch, general_result=general_result)
            await self._flush_ready()

    async def finalize(self):
        """모든 배치가 적용된 후 클래스 요약을 마무리합니다."""
        async with self._lock:
            await self._flush_ready(force=True)
        await self._finalize_remaining_classes()

    async def _flush_ready(self, force: bool = False):
        """배치 ID 순서대로 적용합니다."""
        while self._next_batch_id in self._pending:
            result = self._pending.pop(self._next_batch_id)
            await self._apply_batch(result)
            self._next_batch_id += 1
        if force and self._pending:
            for bid in sorted(self._pending):
                result = self._pending.pop(bid)
                await self._apply_batch(result)

    async def _apply_batch(self, result: BatchResult):
        """LLM 결과를 Neo4j 쿼리로 변환하고 적용합니다."""
        queries: List[str] = []
        analysis_list = (result.general_result.get("analysis") or []) if result.general_result else []

        for node, analysis in zip(result.batch.nodes, analysis_list):
            if not analysis:
                log_process("UNDERSTAND", "APPLY", f"⚠️ {node.start_line}~{node.end_line} 구간 요약 없음 - 건너뜀")
                node.completion_event.set()
                continue

            summary = analysis.get("summary") or ""
            node.summary = summary
            escaped_summary = escape_for_cypher(str(summary))
            queries.append(
                f"MATCH (n:{node.node_type} {{startLine: {node.start_line}, {self.node_base_props}}}) "
                f"SET n.summary = '{escaped_summary}' "
                f"RETURN n"
            )
            log_process("UNDERSTAND", "APPLY", f"✅ {node.start_line}~{node.end_line} 구간 요약 반영")

            # 메서드 호출 관계
            # 타겟 노드: DBMS 패턴 - OPTIONAL MATCH로 기존 노드 찾고, 없으면 CREATE
            for call_name in analysis.get("calls", []) or []:
                escaped_call = escape_for_cypher(call_name)
                if "." in call_name:
                    parts = call_name.split(".", 1)
                    target_type = escape_for_cypher(parts[0])
                    method_name = escape_for_cypher(parts[1])
                    queries.append(
                        f"MATCH (c:{node.node_type} {{startLine: {node.start_line}, {self.node_base_props}}})\n"
                        f"OPTIONAL MATCH (existing)\n"
                        f"WHERE (existing:CLASS OR existing:INTERFACE)\n"
                        f"  AND toLower(existing.class_name) = toLower('{target_type}')\n"
                        f"  AND existing.user_id = '{self.user_id}'\n"
                        f"  AND existing.project_name = '{self.project_name}'\n"
                        f"WITH c, existing\n"
                        f"FOREACH(_ IN CASE WHEN existing IS NULL THEN [1] ELSE [] END |\n"
                        f"    CREATE (:CLASS:INTERFACE {{class_name: '{target_type}', name: '{target_type}', user_id: '{self.user_id}', project_name: '{self.project_name}'}}))\n"
                        f"WITH c\n"
                        f"MATCH (t)\n"
                        f"WHERE (t:CLASS OR t:INTERFACE)\n"
                        f"  AND toLower(t.class_name) = toLower('{target_type}')\n"
                        f"  AND t.user_id = '{self.user_id}'\n"
                        f"  AND t.project_name = '{self.project_name}'\n"
                        f"MERGE (c)-[r:CALLS {{method: '{method_name}'}}]->(t)\n"
                        f"RETURN c, t, r"
                    )
                else:
                    queries.append(
                        f"MATCH (c:{node.node_type} {{startLine: {node.start_line}, {self.node_base_props}}})\n"
                        f"MATCH (m:METHOD {{name: '{escaped_call}', {self.node_base_props}}})\n"
                        f"MERGE (c)-[r:CALLS]->(m)\n"
                        f"RETURN c, m, r"
                    )

            # 로컬 변수 의존 관계 (DEPENDENCY) - 연관 관계가 없을 때만
            for dep_type in analysis.get("localDependencies", []) or []:
                escaped_dep = escape_for_cypher(dep_type)
                if not escaped_dep:
                    continue
                # 소속 클래스에서 타겟 클래스로 DEPENDENCY 관계 생성 (연관 관계가 없을 때만)
                if node.class_kind and node.parent:
                    queries.append(
                        f"MATCH (src:{node.class_kind} {{startLine: {node.parent.start_line}, {self.node_base_props}}})\n"
                        f"OPTIONAL MATCH (existing)\n"
                        f"WHERE (existing:CLASS OR existing:INTERFACE)\n"
                        f"  AND toLower(existing.class_name) = toLower('{escaped_dep}')\n"
                        f"  AND existing.user_id = '{self.user_id}'\n"
                        f"  AND existing.project_name = '{self.project_name}'\n"
                        f"WITH src, existing\n"
                        f"FOREACH(_ IN CASE WHEN existing IS NULL THEN [1] ELSE [] END |\n"
                        f"    CREATE (:CLASS:INTERFACE {{class_name: '{escaped_dep}', name: '{escaped_dep}', user_id: '{self.user_id}', project_name: '{self.project_name}'}}))\n"
                        f"WITH src\n"
                        f"MATCH (dst)\n"
                        f"WHERE (dst:CLASS OR dst:INTERFACE)\n"
                        f"  AND toLower(dst.class_name) = toLower('{escaped_dep}')\n"
                        f"  AND dst.user_id = '{self.user_id}'\n"
                        f"  AND dst.project_name = '{self.project_name}'\n"
                        f"  AND NOT (src)-[:ASSOCIATION|AGGREGATION|COMPOSITION]->(dst)\n"
                        f"MERGE (src)-[r:DEPENDENCY {{usage: 'local', source_member: '{node.node_type}[{node.start_line}]'}}]->(dst)\n"
                        f"RETURN src, dst, r"
                    )

            self._update_class_store(node, analysis)
            node.completion_event.set()

        # completion_event 미설정 노드 처리
        for node in result.batch.nodes:
            if not node.completion_event.is_set():
                node.completion_event.set()

        if queries:
            await self._send_queries(queries, result.batch.progress_line)
        log_process("UNDERSTAND", "APPLY", f"✅ 배치 #{result.batch.batch_id} 적용 완료")

    def _update_class_store(self, node: StatementNode, analysis: Dict[str, Any]):
        """클래스 요약 후보를 저장합니다."""
        if not node.class_key or node.class_key not in self.classes:
            return
        summary_entry = analysis.get("summary")
        if summary_entry:
            key = f"{node.node_type}_{node.start_line}_{node.end_line}"
            self._class_summary_store[node.class_key][key] = summary_entry
        info = self.classes[node.class_key]
        if info.pending_nodes > 0:
            info.pending_nodes -= 1
        if info.pending_nodes == 0 and info.key not in self._finalized_classes:
            asyncio.create_task(self._finalize_class_summary(info))

    async def _finalize_class_summary(self, info: ClassInfo):
        """클래스 요약을 생성하고 Neo4j에 반영합니다."""
        if info.key in self._finalized_classes:
            return
        self._finalized_classes.add(info.key)

        class_node = next(
            (n for n in self._nodes if n.start_line == info.node_start and n.node_type == info.kind),
            None,
        )
        if not class_node:
            return

        summaries = self._class_summary_store.pop(info.key, {})
        if not summaries:
            class_node.completion_event.set()
            return

        try:
            result = await asyncio.to_thread(
                understand_class_summary,
                summaries,
                self.api_key,
                self.locale,
            )
        except Exception as exc:
            log_process("UNDERSTAND", "SUMMARY", f"❌ 클래스 요약 생성 오류: {info.name}", logging.ERROR, exc)
            class_node.completion_event.set()
            return

        summary_value = result.get("summary") if isinstance(result, dict) else None
        if not summary_value:
            log_process("UNDERSTAND", "SUMMARY", f"⚠️ 클래스 요약 없음: {info.name}")
            class_node.completion_event.set()
            return

        escaped_summary = escape_for_cypher(str(summary_value))
        query = (
            f"MATCH (n:{info.kind} {{startLine: {info.node_start}, {self.node_base_props}}}) "
            f"SET n.summary = '{escaped_summary}' "
            f"RETURN n"
        )
        await self._send_queries([query], info.node_end)
        class_node.summary = str(summary_value)
        class_node.completion_event.set()
        log_process("UNDERSTAND", "SUMMARY", f"✅ 클래스 요약 완료: {info.name}")

    async def _finalize_remaining_classes(self):
        """남은 클래스 요약을 처리합니다."""
        for key, info in list(self.classes.items()):
            if info.pending_nodes == 0 and key not in self._finalized_classes:
                await self._finalize_class_summary(info)

    async def _send_queries(self, queries: List[str], progress_line: int):
        """쿼리를 전송하고 완료를 대기합니다."""
        if not queries:
            return
        await self.send_queue.put({
            "type": "analysis_code",
            "query_data": queries,
            "line_number": progress_line,
        })
        while True:
            resp = await self.receive_queue.get()
            if resp.get("type") == "process_completed":
                break
        log_process("UNDERSTAND", "APPLY", f"✅ Neo4j 반영 완료 (라인 {progress_line})")


# ==================== Analyzer 본체 ====================
class FrameworkAnalyzer:
    """Framework Understanding 파이프라인의 엔트리 포인트."""

    def __init__(
        self,
        antlr_data: dict,
        file_content: str,
        send_queue: asyncio.Queue,
        receive_queue: asyncio.Queue,
        last_line: int,
        system_name: str,
        file_name: str,
        user_id: str,
        api_key: str,
        locale: str,
        project_name: str,
    ):
        self.antlr_data = antlr_data
        self.file_content = file_content
        self.send_queue = send_queue
        self.receive_queue = receive_queue
        self.last_line = last_line
        self.system_name = system_name
        self.file_name = file_name
        self.user_id = user_id
        self.api_key = api_key
        self.locale = locale
        self.project_name = project_name
        self.max_workers = MAX_CONCURRENCY
        self.system_file = f"{system_name}-{file_name}"

        self.node_base_props = (
            f"system_name: '{system_name}', file_name: '{file_name}', "
            f"user_id: '{user_id}', project_name: '{project_name}'"
        )
        self.system_props = (
            f"user_id: '{user_id}', system_name: '{system_name}', project_name: '{project_name}'"
        )

    async def run(self):
        """파일 단위 Understanding 파이프라인을 실행합니다."""
        log_process("UNDERSTAND", "START", f"🚀 {self.system_file} 분석 시작 (총 {self.last_line}줄)")
        try:
            # 1. AST 수집
            collector = StatementCollector(self.antlr_data, self.file_content, self.system_name, self.file_name)
            nodes, classes = collector.collect()

            # 2. 정적 그래프 초기화
            await self._initialize_static_graph(nodes)

            # 3. 선행 처리 (병렬): 상속/구현 + 필드
            await self._process_preprocessing(nodes)

            # 4. 배치 분석
            planner = BatchPlanner()
            batches = planner.plan(nodes)

            if not batches:
                await self.send_queue.put({"type": "end_analysis"})
                return

            # LLM 분석 시작 알림 (총 배치 수 전달)
            await self.send_queue.put({"type": "llm_start", "total_batches": len(batches)})
            while True:
                resp = await self.receive_queue.get()
                if resp.get("type") == "process_completed":
                    break

            invoker = LLMInvoker(self.api_key, self.locale)
            apply_manager = ApplyManager(
                send_queue=self.send_queue,
                receive_queue=self.receive_queue,
                file_last_line=self.last_line,
                nodes=nodes,
                node_base_props=self.node_base_props,
                system_props=self.system_props,
                classes=classes,
                api_key=self.api_key,
                locale=self.locale,
                user_id=self.user_id,
                project_name=self.project_name,
                system_name=self.system_name,
                file_name=self.file_name,
            )

            semaphore = asyncio.Semaphore(min(self.max_workers, len(batches)))

            async def worker(batch: AnalysisBatch):
                await self._wait_for_dependencies(batch)
                async with semaphore:
                    log_process(
                        "UNDERSTAND",
                        "LLM",
                        f"🤖 배치 #{batch.batch_id} LLM 요청: 노드 {len(batch.nodes)}개 ({self.system_file})",
                    )
                    general_result = await invoker.invoke(batch)
                await apply_manager.submit(batch, general_result)

            await asyncio.gather(*(worker(b) for b in batches))
            await apply_manager.finalize()

            log_process("UNDERSTAND", "DONE", f"✅ {self.system_file} 분석 완료")
            await self.send_queue.put({"type": "end_analysis"})

        except (UnderstandingError, LLMCallError) as exc:
            log_process("UNDERSTAND", "ERROR", "❌ Understanding 파이프라인 예외", logging.ERROR, exc)
            await self.send_queue.put({"type": "error", "message": str(exc)})
            raise
        except Exception as exc:
            err_msg = f"Understanding 과정에서 오류 발생: {exc}"
            log_process("UNDERSTAND", "ERROR", f"❌ {err_msg}", logging.ERROR, exc)
            await self.send_queue.put({"type": "error", "message": err_msg})
            raise ProcessAnalyzeCodeError(err_msg)

    async def _wait_for_dependencies(self, batch: AnalysisBatch):
        """부모 노드 분석 전 자식 완료 대기."""
        waiters = []
        for n in batch.nodes:
            for ch in n.children:
                if ch.analyzable:
                    waiters.append(ch.completion_event.wait())
        if waiters:
            log_process(
                "UNDERSTAND",
                "WAIT",
                f"⏳ 배치 #{batch.batch_id}가 자식 {len(waiters)}개 완료 대기",
            )
            await asyncio.gather(*waiters)

    # ===== 정적 그래프 초기화 =====
    async def _initialize_static_graph(self, nodes: List[StatementNode]):
        """파일 분석 전에 정적 노드/관계를 생성합니다."""
        if not nodes:
            return
        await self._create_static_nodes(nodes)
        await self._create_relationships(nodes)
        # 정적 그래프 초기화 완료 알림
        await self.send_queue.put({"type": "static_complete"})
        while True:
            resp = await self.receive_queue.get()
            if resp.get("type") == "process_completed":
                break

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
        """정적 노드 생성 쿼리 리스트를 반환합니다."""
        queries: List[str] = []
        label = node.node_type
        
        # name은 타입[라인번호] 형식 (DBMS와 동일)
        if label == "FILE":
            node_name = self.file_name
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

        # CLASS/INTERFACE 등: class_name과 type 속성 추가
        if label in CLASS_TYPES and node.class_name:
            base_set.append(f"n.class_name = '{escape_for_cypher(node.class_name)}'")
            base_set.append(f"n.type = '{label}'")
        # 그 외 노드: 소속 클래스명 저장
        elif node.class_name:
            base_set.append(f"n.class_name = '{escape_for_cypher(node.class_name)}'")

        if node.has_children:
            escaped_placeholder = escape_for_cypher(node.get_placeholder_code())
            base_set.append(f"n.summarized_code = '{escaped_placeholder}'")

        base_set_str = ", ".join(base_set)
        
        # CLASS/INTERFACE 노드: DBMS 패턴 - OPTIONAL MATCH로 기존 노드 찾고, 없으면 CREATE
        if label in CLASS_TYPES and node.class_name:
            escaped_class_name = escape_for_cypher(node.class_name)
            other_label = "INTERFACE" if label == "CLASS" else "CLASS"
            # 기존 노드 찾기 (CLASS 또는 INTERFACE 레이블 중 하나라도 있으면 매칭) - 대소문자 무시
            queries.append(
                f"OPTIONAL MATCH (existing)\n"
                f"WHERE (existing:CLASS OR existing:INTERFACE)\n"
                f"  AND toLower(existing.class_name) = toLower('{escaped_class_name}')\n"
                f"  AND existing.user_id = '{self.user_id}'\n"
                f"  AND existing.project_name = '{self.project_name}'\n"
                f"WITH existing\n"
                f"FOREACH(_ IN CASE WHEN existing IS NULL THEN [1] ELSE [] END |\n"
                f"    CREATE (:CLASS:INTERFACE {{class_name: '{escaped_class_name}', user_id: '{self.user_id}', project_name: '{self.project_name}'}}))\n"
                f"WITH 1 as dummy\n"
                f"MATCH (n)\n"
                f"WHERE (n:CLASS OR n:INTERFACE)\n"
                f"  AND toLower(n.class_name) = toLower('{escaped_class_name}')\n"
                f"  AND n.user_id = '{self.user_id}'\n"
                f"  AND n.project_name = '{self.project_name}'\n"
                f"SET n:{label}, n.startLine = {node.start_line}, n.system_name = '{self.system_name}', n.file_name = '{self.file_name}', {base_set_str}\n"
                f"REMOVE n:{other_label}\n"
                f"WITH n\n"
                f"MERGE (system:SYSTEM {{{self.system_props}}})\n"
                f"MERGE (system)-[r:CONTAINS]->(n)\n"
                f"RETURN n, system, r"
            )
        else:
            queries.append(
                f"MERGE (n:{label} {{startLine: {node.start_line}, {self.node_base_props}}})\n"
                f"SET {base_set_str}\n"
                f"WITH n\n"
                f"MERGE (system:SYSTEM {{{self.system_props}}})\n"
                f"MERGE (system)-[r:CONTAINS]->(n)\n"
                f"RETURN n, system, r"
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

            prev = None
            for child in node.children:
                if prev:
                    queries.append(self._build_next_relationship_query(prev, child))
                    if len(queries) >= STATIC_QUERY_BATCH_SIZE:
                        await self._send_static_queries(queries, child.end_line)
                        queries.clear()
                prev = child
        if queries:
            await self._send_static_queries(queries, nodes[-1].end_line)

    def _build_parent_relationship_query(self, parent: StatementNode, child: StatementNode) -> str:
        """PARENT_OF 관계 쿼리."""
        return (
            f"MATCH (p:{parent.node_type} {{startLine: {parent.start_line}, {self.node_base_props}}})\n"
            f"MATCH (c:{child.node_type} {{startLine: {child.start_line}, {self.node_base_props}}})\n"
            f"MERGE (p)-[r:PARENT_OF]->(c)\n"
            f"RETURN p, c, r"
        )

    def _build_next_relationship_query(self, prev_node: StatementNode, current_node: StatementNode) -> str:
        """NEXT 관계 쿼리."""
        return (
            f"MATCH (prev:{prev_node.node_type} {{startLine: {prev_node.start_line}, {self.node_base_props}}})\n"
            f"MATCH (curr:{current_node.node_type} {{startLine: {current_node.start_line}, {self.node_base_props}}})\n"
            f"MERGE (prev)-[r:NEXT]->(curr)\n"
            f"RETURN prev, curr, r"
        )

    async def _send_static_queries(self, queries: List[str], progress_line: int):
        """정적 그래프 쿼리 전송."""
        if not queries:
            return
        await self.send_queue.put({
            "type": "static_graph",  # 정적 그래프 초기화는 별도 타입으로 구분
            "query_data": queries,
            "line_number": progress_line,
        })
        while True:
            resp = await self.receive_queue.get()
            if resp.get("type") == "process_completed":
                break

    # ===== 선행 처리: 상속/구현 + 필드 + 메서드 (병렬) =====
    async def _process_preprocessing(self, nodes: List[StatementNode]):
        """상속/구현, 필드, 메서드 노드를 병렬로 처리합니다."""
        inheritance_nodes = [n for n in nodes if n.node_type in INHERITANCE_TYPES]
        field_nodes = [n for n in nodes if n.node_type in FIELD_TYPES]
        method_nodes = [n for n in nodes if n.node_type in METHOD_TYPES]

        log_process("UNDERSTAND", "PREPROCESS", f"🔍 선행 처리 시작: 상속/구현 {len(inheritance_nodes)}개, 필드 {len(field_nodes)}개, 메서드 {len(method_nodes)}개")

        # 1단계: 상속/구현 + 필드 병렬 처리 (ASSOCIATION 생성)
        await asyncio.gather(
            self._process_inheritance_nodes(inheritance_nodes),
            self._process_field_nodes(field_nodes, nodes),
        )

        # 2단계: 메서드 처리 (ASSOCIATION → AGGREGATION/COMPOSITION 변경)
        await self._process_method_nodes(method_nodes)

        log_process("UNDERSTAND", "PREPROCESS", f"✅ 선행 처리 완료")

    async def _process_inheritance_nodes(self, nodes: List[StatementNode]):
        """상속/구현 노드를 병렬로 분석합니다."""
        if not nodes:
            return

        log_process("UNDERSTAND", "INHERITANCE", f"🔍 상속/구현 관계 분석 시작: {len(nodes)}개 노드")
        semaphore = asyncio.Semaphore(INHERITANCE_CONCURRENCY)

        async def worker(node: StatementNode):
            async with semaphore:
                try:
                    result = await asyncio.to_thread(
                        understand_inheritance,
                        node.get_raw_code(),
                        self.api_key,
                        self.locale,
                    )
                except Exception as exc:
                    log_process("UNDERSTAND", "INHERITANCE", f"❌ 상속/구현 분석 오류: 라인 {node.start_line}", logging.ERROR, exc)
                    return

                queries = self._build_inheritance_queries(node, result)
                if queries:
                    await self._send_static_queries(queries, node.end_line)

        await asyncio.gather(*(worker(n) for n in nodes))
        log_process("UNDERSTAND", "INHERITANCE", f"✅ 상속/구현 관계 분석 완료")

    def _build_inheritance_queries(self, node: StatementNode, analysis: Dict[str, Any]) -> List[str]:
        """상속/구현 분석 결과를 Neo4j 쿼리로 변환합니다."""
        if not isinstance(analysis, dict):
            return []

        queries: List[str] = []
        relations = analysis.get("relations") or []

        for rel in relations:
            to_type = escape_for_cypher(rel.get("toType") or "")
            rel_type = rel.get("relationType") or "EXTENDS"
            to_type_kind = escape_for_cypher(rel.get("toTypeKind") or ("INTERFACE" if rel_type == "IMPLEMENTS" else "CLASS"))

            if not to_type:
                continue

            # 소스 클래스 노드 매칭
            src_match = f"MATCH (src:{node.class_kind or 'CLASS'} {{startLine: {node.parent.start_line if node.parent else node.start_line}, {self.node_base_props}}})"

            # 타겟 노드: DBMS 패턴 - OPTIONAL MATCH로 기존 노드 찾고, 없으면 CREATE (대소문자 무시)
            queries.append(
                f"{src_match}\n"
                f"OPTIONAL MATCH (existing)\n"
                f"WHERE (existing:CLASS OR existing:INTERFACE)\n"
                f"  AND toLower(existing.class_name) = toLower('{to_type}')\n"
                f"  AND existing.user_id = '{self.user_id}'\n"
                f"  AND existing.project_name = '{self.project_name}'\n"
                f"WITH src, existing\n"
                f"FOREACH(_ IN CASE WHEN existing IS NULL THEN [1] ELSE [] END |\n"
                f"    CREATE (:CLASS:INTERFACE {{class_name: '{to_type}', name: '{to_type}', user_id: '{self.user_id}', project_name: '{self.project_name}'}}))\n"
                f"WITH src\n"
                f"MATCH (dst)\n"
                f"WHERE (dst:CLASS OR dst:INTERFACE)\n"
                f"  AND toLower(dst.class_name) = toLower('{to_type}')\n"
                f"  AND dst.user_id = '{self.user_id}'\n"
                f"  AND dst.project_name = '{self.project_name}'\n"
                f"MERGE (src)-[r:{rel_type}]->(dst)\n"
                f"RETURN src, dst, r"
            )

        return queries

    async def _process_field_nodes(self, field_nodes: List[StatementNode], all_nodes: List[StatementNode]):
        """필드 노드를 병렬로 분석합니다."""
        if not field_nodes:
            return

        log_process("UNDERSTAND", "FIELD", f"🔍 필드 정보 분석 시작: {len(field_nodes)}개 노드")
        semaphore = asyncio.Semaphore(FIELD_CONCURRENCY)

        async def worker(node: StatementNode):
            async with semaphore:
                try:
                    result = await asyncio.to_thread(
                        understand_field,
                        node.get_raw_code(),
                        self.api_key,
                        self.locale,
                    )
                except Exception as exc:
                    log_process("UNDERSTAND", "FIELD", f"❌ 필드 분석 오류: 라인 {node.start_line}", logging.ERROR, exc)
                    return

                queries = self._build_field_queries(node, result)
                if queries:
                    await self._send_static_queries(queries, node.end_line)

        await asyncio.gather(*(worker(n) for n in field_nodes))
        log_process("UNDERSTAND", "FIELD", f"✅ 필드 정보 분석 완료")

    def _build_field_queries(self, node: StatementNode, analysis: Dict[str, Any]) -> List[str]:
        """필드 분석 결과를 Neo4j 쿼리로 변환합니다."""
        if not isinstance(analysis, dict):
            return []

        queries: List[str] = []
        fields = analysis.get("fields") or []

        for field_info in fields:
            field_name = escape_for_cypher(field_info.get("field_name") or "")
            field_type = escape_for_cypher(field_info.get("field_type") or "")
            target_class_raw = field_info.get("target_class")
            target_class = escape_for_cypher(target_class_raw) if target_class_raw else None
            visibility = escape_for_cypher(field_info.get("visibility") or "private")
            is_static = "true" if field_info.get("is_static") else "false"
            is_final = "true" if field_info.get("is_final") else "false"
            multiplicity = escape_for_cypher(field_info.get("multiplicity") or "1")
            association_type = field_info.get("association_type") or "ASSOCIATION"

            if not field_name:
                continue

            # FIELD 노드 속성 업데이트
            # target_class가 있으면 클래스 타입 필드 (연관 관계 대상)
            target_class_set = f", f.target_class = '{target_class}'" if target_class else ""
            queries.append(
                f"MATCH (f:FIELD {{startLine: {node.start_line}, {self.node_base_props}}})\n"
                f"SET f.name = '{field_name}', f.field_type = '{field_type}', "
                f"f.visibility = '{visibility}', f.is_static = {is_static}, f.is_final = {is_final}{target_class_set}\n"
                f"RETURN f"
            )

            # 연관 관계 생성 (ASSOCIATION, AGGREGATION, COMPOSITION)
            # 타겟 노드: DBMS 패턴 - OPTIONAL MATCH로 기존 노드 찾고, 없으면 CREATE (대소문자 무시)
            if target_class:
                src_match = f"MATCH (src:{node.class_kind or 'CLASS'} {{startLine: {node.parent.start_line if node.parent else node.start_line}, {self.node_base_props}}})"
                queries.append(
                    f"{src_match}\n"
                    f"OPTIONAL MATCH (existing)\n"
                    f"WHERE (existing:CLASS OR existing:INTERFACE)\n"
                    f"  AND toLower(existing.class_name) = toLower('{target_class}')\n"
                    f"  AND existing.user_id = '{self.user_id}'\n"
                    f"  AND existing.project_name = '{self.project_name}'\n"
                    f"WITH src, existing\n"
                    f"FOREACH(_ IN CASE WHEN existing IS NULL THEN [1] ELSE [] END |\n"
                    f"    CREATE (:CLASS:INTERFACE {{class_name: '{target_class}', name: '{target_class}', user_id: '{self.user_id}', project_name: '{self.project_name}'}}))\n"
                    f"WITH src\n"
                    f"MATCH (dst)\n"
                    f"WHERE (dst:CLASS OR dst:INTERFACE)\n"
                    f"  AND toLower(dst.class_name) = toLower('{target_class}')\n"
                    f"  AND dst.user_id = '{self.user_id}'\n"
                    f"  AND dst.project_name = '{self.project_name}'\n"
                    f"MERGE (src)-[r:{association_type} {{source_member: '{field_name}', multiplicity: '{multiplicity}'}}]->(dst)\n"
                    f"RETURN src, dst, r"
                )

        return queries

    async def _process_method_nodes(self, method_nodes: List[StatementNode]):
        """메서드 노드를 병렬로 분석합니다 - 파라미터/반환 타입 추출."""
        if not method_nodes:
            return

        log_process("UNDERSTAND", "METHOD", f"🔍 메서드 시그니처 분석 시작: {len(method_nodes)}개 노드")
        semaphore = asyncio.Semaphore(METHOD_CONCURRENCY)

        async def worker(node: StatementNode):
            async with semaphore:
                try:
                    # 메서드 시그니처 + ASSIGN 구문만 포함된 코드 전달
                    code_for_analysis = node.get_code_with_assigns_only() if node.has_children else node.get_raw_code()
                    result = await asyncio.to_thread(
                        understand_method,
                        code_for_analysis,
                        self.api_key,
                        self.locale,
                    )
                except Exception as exc:
                    log_process("UNDERSTAND", "METHOD", f"❌ 메서드 분석 오류: 라인 {node.start_line}", logging.ERROR, exc)
                    return

                queries = self._build_method_queries(node, result)
                if queries:
                    await self._send_static_queries(queries, node.end_line)

        await asyncio.gather(*(worker(n) for n in method_nodes))
        log_process("UNDERSTAND", "METHOD", f"✅ 메서드 시그니처 분석 완료")

    def _build_method_queries(self, node: StatementNode, analysis: Dict[str, Any]) -> List[str]:
        """메서드 분석 결과를 Neo4j 쿼리로 변환합니다."""
        if not isinstance(analysis, dict):
            return []

        queries: List[str] = []
        
        method_name = escape_for_cypher(analysis.get("method_name") or "")
        return_type = escape_for_cypher(analysis.get("return_type") or "void")
        visibility = escape_for_cypher(analysis.get("visibility") or "public")
        is_static = "true" if analysis.get("is_static") else "false"
        method_kind = escape_for_cypher(analysis.get("method_type") or "normal")
        parameters = analysis.get("parameters") or []
        dependencies = analysis.get("dependencies") or []

        # METHOD 노드에 시그니처 정보 저장
        queries.append(
            f"MATCH (m:{node.node_type} {{startLine: {node.start_line}, {self.node_base_props}}})\n"
            f"SET m.methodName = '{method_name}', m.returnType = '{return_type}', "
            f"m.visibility = '{visibility}', m.isStatic = {is_static}, "
            f"m.method_type = '{method_kind}'\n"
            f"RETURN m"
        )

        # 각 파라미터를 개별 Parameter 노드로 저장
        for idx, param in enumerate(parameters):
            param_name = escape_for_cypher(param.get("name") or "")
            param_type = escape_for_cypher(param.get("type") or "")
            if not param_name:
                continue
            queries.append(
                f"MATCH (m:{node.node_type} {{startLine: {node.start_line}, {self.node_base_props}}})\n"
                f"MERGE (p:Parameter {{name: '{param_name}', methodStartLine: {node.start_line}, {self.node_base_props}}})\n"
                f"SET p.type = '{param_type}', p.index = {idx}\n"
                f"MERGE (m)-[r:HAS_PARAMETER]->(p)\n"
                f"RETURN m, p, r"
            )

        # 의존 관계 생성 (DEPENDENCY) - 연관 관계가 없을 때만
        # 타겟 노드: DBMS 패턴 - OPTIONAL MATCH로 기존 노드 찾고, 없으면 CREATE
        for dep in dependencies:
            target_type = escape_for_cypher(dep.get("target_class") or "")
            usage = escape_for_cypher(dep.get("usage") or "parameter")

            if not target_type:
                continue

            src_match = f"MATCH (src:{node.class_kind or 'CLASS'} {{startLine: {node.parent.start_line if node.parent else node.start_line}, {self.node_base_props}}})"
            queries.append(
                f"{src_match}\n"
                f"OPTIONAL MATCH (existing)\n"
                f"WHERE (existing:CLASS OR existing:INTERFACE)\n"
                f"  AND toLower(existing.class_name) = toLower('{target_type}')\n"
                f"  AND existing.user_id = '{self.user_id}'\n"
                f"  AND existing.project_name = '{self.project_name}'\n"
                f"WITH src, existing\n"
                f"FOREACH(_ IN CASE WHEN existing IS NULL THEN [1] ELSE [] END |\n"
                f"    CREATE (:CLASS:INTERFACE {{class_name: '{target_type}', name: '{target_type}', user_id: '{self.user_id}', project_name: '{self.project_name}'}}))\n"
                f"WITH src\n"
                f"MATCH (dst)\n"
                f"WHERE (dst:CLASS OR dst:INTERFACE)\n"
                f"  AND toLower(dst.class_name) = toLower('{target_type}')\n"
                f"  AND dst.user_id = '{self.user_id}'\n"
                f"  AND dst.project_name = '{self.project_name}'\n"
                f"  AND NOT (src)-[:ASSOCIATION|AGGREGATION|COMPOSITION]->(dst)\n"
                f"MERGE (src)-[r:DEPENDENCY {{usage: '{usage}', source_member: '{method_name}'}}]->(dst)\n"
                f"RETURN src, dst, r"
            )

        # 필드 할당 패턴에 따른 연관 관계 세분화 (ASSOCIATION → AGGREGATION/COMPOSITION)
        field_assignments = analysis.get("field_assignments") or []
        src_start_line = node.parent.start_line if node.parent else node.start_line
        for assign in field_assignments:
            field_name = escape_for_cypher(assign.get("field_name") or "")
            value_source = assign.get("value_source") or ""

            if not field_name or not value_source:
                continue

            # value_source에 따른 관계 타입 결정
            new_rel_type = "AGGREGATION" if value_source == "parameter" else "COMPOSITION"

            # FIELD 노드의 target_class이 있으면 (클래스 타입 필드) 기존 ASSOCIATION을 변경
            queries.append(
                f"MATCH (field:FIELD {{name: '{field_name}', {self.node_base_props}}})\n"
                f"WHERE field.target_class IS NOT NULL\n"
                f"MATCH (src:{node.class_kind or 'CLASS'} {{startLine: {src_start_line}, {self.node_base_props}}})"
                f"-[r:ASSOCIATION {{source_member: '{field_name}'}}]->(dst)\n"
                f"WITH src, dst, COALESCE(r.multiplicity, '1') AS mult, r\n"
                f"DELETE r\n"
                f"MERGE (src)-[r2:{new_rel_type} {{source_member: '{field_name}', multiplicity: mult}}]->(dst)\n"
                f"RETURN src, dst, r2"
            )

        return queries


# 이전 버전 호환을 위한 별칭
Analyzer = FrameworkAnalyzer
