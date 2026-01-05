"""Framework 코드 분석기 - Java/Kotlin AST → Neo4j 그래프

클래스 다이어그램 생성에 필요한 정보를 추출합니다.

분석 파이프라인:
1. AST 수집 (StatementCollector)
2. 정적 그래프 생성 (CLASS, METHOD, FIELD 노드)
3. 상속/구현 관계 추출 (EXTENDS, IMPLEMENTS)
4. LLM 배치 분석 (요약, 메서드 콜 추출)
5. 클래스 요약 및 User Story 생성
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
from util.utility_tool import calculate_code_token, escape_for_cypher, log_process


# ==================== 상수 정의 ====================
# 노드 타입 분류
NON_ANALYSIS_TYPES = frozenset(["FILE", "PACKAGE", "IMPORT"])
CLASS_TYPES = frozenset(["CLASS", "INTERFACE", "ENUM"])
INHERITANCE_TYPES = frozenset(["EXTENDS", "IMPLEMENTS"])
FIELD_TYPES = frozenset(["FIELD"])
METHOD_TYPES = frozenset(["METHOD", "CONSTRUCTOR"])
METHOD_SIGNATURE_TYPES = frozenset(["METHOD_SIGNATURE"])

# 설정에서 가져오는 상수
MAX_BATCH_TOKEN = settings.batch.framework_max_batch_token
MAX_CONCURRENCY = settings.concurrency.framework_max_concurrency
INHERITANCE_CONCURRENCY = settings.concurrency.inheritance_concurrency
FIELD_CONCURRENCY = settings.concurrency.field_concurrency
METHOD_CONCURRENCY = settings.concurrency.method_concurrency
STATIC_QUERY_BATCH_SIZE = settings.batch.static_query_batch_size
MAX_SUMMARY_CHUNK_TOKEN = settings.batch.max_summary_chunk_token
MAX_CONTEXT_TOKEN = settings.batch.max_context_token
PARENT_EXPAND_THRESHOLD = settings.batch.parent_expand_threshold

# Java 표준 라이브러리 및 기본 타입 - 클래스 생성 제외 대상
JAVA_BUILTIN_TYPES = frozenset([
    # 기본 타입 및 래퍼
    "int", "long", "double", "float", "boolean", "char", "byte", "short", "void",
    "Integer", "Long", "Double", "Float", "Boolean", "Character", "Byte", "Short",
    # 기본 클래스
    "String", "Object", "Class", "Enum", "System", "Math", "Runtime",
    # 컬렉션
    "List", "ArrayList", "LinkedList", "Set", "HashSet", "TreeSet", "LinkedHashSet",
    "Map", "HashMap", "TreeMap", "LinkedHashMap", "ConcurrentHashMap",
    "Collection", "Collections", "Arrays", "Iterator", "Iterable",
    "Queue", "Deque", "Stack", "Vector", "PriorityQueue",
    # 유틸리티
    "Optional", "Stream", "Collectors", "Comparator", "Comparable",
    "Date", "Calendar", "LocalDate", "LocalTime", "LocalDateTime", "Instant",
    "UUID", "Random", "Scanner", "Pattern", "Matcher",
    # 예외
    "Exception", "RuntimeException", "Throwable", "Error",
    "IOException", "SQLException", "NullPointerException", "IllegalArgumentException",
    # I/O
    "File", "Path", "Files", "InputStream", "OutputStream", "Reader", "Writer",
    "BufferedReader", "BufferedWriter", "PrintWriter", "FileReader", "FileWriter",
    # 기타
    "StringBuilder", "StringBuffer", "BigDecimal", "BigInteger",
    "Logger", "Log", "LogFactory",
])

# 유틸리티/헬퍼 클래스 패턴 - CALLS 관계 생성 제외 대상
# (프로젝트에 존재하더라도 비즈니스 로직 관점에서 중요하지 않은 클래스)
UTILITY_CLASS_PATTERNS = frozenset([
    "Debug", "Logger", "Log", "LogFactory", "LogManager",
    "Utils", "Utility", "Utilities", "Helper", "Helpers",
    "Constants", "Config", "Configuration", "Settings",
    "Validator", "Validation", "Formatter", "Converter",
    "StringUtils", "DateUtils", "NumberUtils", "CollectionUtils",
    "Assert", "Assertions", "Preconditions", "Check",
])


# ==================== 데이터 클래스 ====================
@dataclass(slots=True)
class StatementNode:
    """평탄화된 AST 노드를 표현합니다.
    
    - 수집 단계에서 모든 노드를 생성합니다.
    - 이후 배치가 만들어질 때 이 객체를 그대로 사용합니다.
    - LLM 요약이 끝나면 `summary`와 `completion_event`가 채워집니다.
    - `ok` 플래그로 성공 여부를 추적합니다 (자식 실패 시 부모도 False).
    - `context`는 부모 컨텍스트 추출 결과를 저장합니다.
    """
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
    context: Optional[str] = None  # 부모 컨텍스트 (자식 분석 시 전달됨)
    ok: bool = True  # LLM 분석 성공 여부 (자식 실패 시 부모도 False)
    completion_event: asyncio.Event = field(init=False, repr=False)
    context_ready_event: asyncio.Event = field(init=False, repr=False)

    def __post_init__(self):
        object.__setattr__(self, "completion_event", asyncio.Event())
        object.__setattr__(self, "context_ready_event", asyncio.Event())

    def get_raw_code(self) -> str:
        """라인 번호를 포함하여 노드의 원문 코드를 반환합니다."""
        return "\n".join(f"{ln}: {text}" for ln, text in self.lines)

    def get_compact_code(self) -> str:
        """자식 구간은 자식 요약(없으면 placeholder)으로 치환한 코드를 반환합니다.
        
        DBMS 방식처럼 단순 순회로 처리합니다.
        """
        if not self.children:
            return self.get_raw_code()

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

            # 자식 구간은 자식 요약으로 대체합니다 (없으면 placeholder).
            if child.summary:
                child_summary = child.summary.strip()
                summary_line = f"{child.start_line}~{child.end_line}: {child_summary}"
            else:
                summary_line = f"{child.start_line}: ...code..."

            result_lines.append(summary_line)

            # 자식 구간 원본 코드는 건너뜁니다.
            while line_index < total_lines and self.lines[line_index][0] <= child.end_line:
                line_index += 1

        # 마지막 자식 이후 부모 코드가 남아 있다면 추가합니다.
        while line_index < total_lines:
            line_no, text = self.lines[line_index]
            result_lines.append(f"{line_no}: {text}")
            line_index += 1

        return "\n".join(result_lines)

    def get_placeholder_code(self, include_assigns: bool = False) -> str:
        """자식 구간을 placeholder(...code...)로 치환한 코드를 반환합니다.
        
        기본 동작:
        - 메서드 시그니처, 상속, 구현 관계는 원문 유지
        - 나머지 모든 자식은 ...code...로 치환
        
        Args:
            include_assigns: True이면 ASSIGNMENT/NEW_INSTANCE 노드를 재귀적으로 찾아서 원문 유지
                            (if문, for문 등은 제거되고 ASSIGN/NEW_INSTANCE만 남음)
        """
        if not self.children:
            return self.get_raw_code()
        
        # 항상 원문 유지할 노드 타입: 상속/구현 관계, 메서드 시그니처
        PRESERVE_TYPES = INHERITANCE_TYPES | METHOD_TYPES | METHOD_SIGNATURE_TYPES
        
        # include_assigns=True이면 ASSIGNMENT/NEW_INSTANCE를 재귀적으로 수집
        assign_node_set: set[Tuple[int, int]] = set()
        if include_assigns:
            ASSIGN_TYPES = {"ASSIGNMENT", "NEW_INSTANCE"}
            
            def find_assign_nodes_recursive(node: "StatementNode") -> List["StatementNode"]:
                """재귀적으로 ASSIGNMENT, NEW_INSTANCE 노드를 수집합니다."""
                results = []
                for child in node.children:
                    if child.node_type in ASSIGN_TYPES:
                        results.append(child)
                    # 자식의 자식도 재귀적으로 탐색
                    results.extend(find_assign_nodes_recursive(child))
                return results
            
            assign_nodes = find_assign_nodes_recursive(self)
            assign_node_set = {(n.start_line, n.end_line) for n in assign_nodes}
        
        result_lines: List[str] = []
        line_index = 0
        total_lines = len(self.lines)
        sorted_children = sorted(self.children, key=lambda child: child.start_line)
        
        for child in sorted_children:
            # 자식 이전의 부모 코드를 그대로 출력
            while line_index < total_lines and self.lines[line_index][0] < child.start_line:
                line_no, text = self.lines[line_index]
                result_lines.append(f"{line_no}: {text}")
                line_index += 1
            
            # 원문 유지할 노드: 메서드 시그니처, 상속/구현, 또는 ASSIGNMENT/NEW_INSTANCE
            child_span = (child.start_line, child.end_line)
            should_preserve = (
                child.node_type in PRESERVE_TYPES or 
                (include_assigns and child_span in assign_node_set)
            )
            
            if should_preserve:
                # 원문 그대로 출력
                while line_index < total_lines and self.lines[line_index][0] <= child.end_line:
                    line_no, text = self.lines[line_index]
                    result_lines.append(f"{line_no}: {text}")
                    line_index += 1
            else:
                # 나머지 자식은 ...code...로 치환
                result_lines.append(f"{child.start_line}: ...code...")
                while line_index < total_lines and self.lines[line_index][0] <= child.end_line:
                    line_index += 1
        
        # 마지막 자식 이후 부모 코드가 남아 있다면 추가
        while line_index < total_lines:
            line_no, text = self.lines[line_index]
            result_lines.append(f"{line_no}: {text}")
            line_index += 1
        
        return "\n".join(result_lines)

    def get_code_with_assigns_only(self) -> str:
        """메서드 시그니처 + ASSIGNMENT/NEW_INSTANCE 자식만 포함된 코드를 반환합니다.
        
        get_placeholder_code(include_assigns=True)를 호출합니다.
        """
        return self.get_placeholder_code(include_assigns=True)

    def get_skeleton_code(self) -> str:
        """자식 구간을 .... 로 압축한 스켈레톤 코드를 반환합니다.
        
        연속된 자식 구간은 하나의 .... 로 압축됩니다.
        부모 컨텍스트 추출용으로 사용됩니다.
        """
        if not self.children:
            return self.get_raw_code()

        result_lines: List[str] = []
        sorted_children = sorted(self.children, key=lambda child: child.start_line)
        in_child_block = False

        for line_no, text in self.lines:
            is_child_line = any(
                child.start_line <= line_no <= child.end_line
                for child in sorted_children
            )

            if is_child_line:
                if not in_child_block:
                    result_lines.append("    ....")
                    in_child_block = True
                # 연속된 자식 라인은 스킵
            else:
                in_child_block = False
                result_lines.append(f"{line_no}: {text}")

        return "\n".join(result_lines)

    def get_ancestor_context(self, max_tokens: int = MAX_CONTEXT_TOKEN) -> str:
        """조상 노드들의 컨텍스트를 결합하여 반환합니다.
        
        가장 가까운 조상부터 토큰 상한까지 누적합니다.
        """
        if not self.parent:
            return ""

        context_parts: List[str] = []
        remaining = max_tokens
        current = self.parent

        while current and remaining > 0:
            # 부모의 context가 있으면 사용 (LLM이 생성한 핵심 컨텍스트)
            if current.context:
                ctx_tokens = calculate_code_token(current.context)
                if ctx_tokens <= remaining:
                    context_parts.insert(0, current.context)
                    remaining -= ctx_tokens
                else:
                    # 토큰 초과 시 중단
                    break
            current = current.parent

        if not context_parts:
            return ""

        return "[CONTEXT]\n" + "\n---\n".join(context_parts) + "\n[/CONTEXT]\n"

    def needs_context_generation(self) -> bool:
        """이 노드가 컨텍스트 생성이 필요한 부모 노드인지 확인합니다.
        
        조건:
        - has_children = True (자식이 있음)
        - analyzable = True (분석 대상)
        - node_type이 CLASS_TYPES가 아님 (클래스는 제외)
        """
        return (
            self.has_children
            and self.analyzable
            and self.node_type not in CLASS_TYPES
        )


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

    def build_payload(self) -> Tuple[str, str]:
        """LLM 호출용 코드와 컨텍스트를 분리하여 반환합니다.
        
        Returns:
            (code, context) 튜플 - 코드와 컨텍스트를 분리
        """
        code_parts: List[str] = []
        context_parts: List[str] = []
        
        for node in self.nodes:
            code = node.get_compact_code() if node.has_children else node.get_raw_code()
            code_parts.append(code)
            
            context = node.get_ancestor_context()
            if context:
                context_parts.append(context)
            else:
                context_parts.append("")
        
        return "\n\n".join(code_parts), "\n\n".join(context_parts)


@dataclass(slots=True)
class BatchResult:
    """배치 처리 결과 (calls 배열은 general_result에 통합됨)."""
    batch: AnalysisBatch
    general_result: Optional[Dict[str, Any]]


# ==================== 헬퍼 함수 ====================
def _is_valid_class_name_for_calls(name: str) -> bool:
    """calls 관계 생성에 유효한 클래스명인지 검증.
    
    가짜 클래스 생성을 방지하기 위해:
    - Java 표준 라이브러리 제외
    - 유틸리티/헬퍼 클래스 제외 (Debug, Logger, Utils 등)
    - 소문자만으로 된 짧은 이름(변수명으로 보이는 것) 제외
    - 한 글자 이름 제외
    """
    if not name:
        return False
    
    # Java 표준 라이브러리 제외
    if name in JAVA_BUILTIN_TYPES:
        return False
    
    # 유틸리티/헬퍼 클래스 제외 (비즈니스 로직 관점에서 중요하지 않음)
    if name in UTILITY_CLASS_PATTERNS:
        return False
    
    # 한 글자 이름 제외 (i, j, k, o, e 등 반복 변수)
    if len(name) == 1:
        return False
    
    # 소문자로만 시작하고 3글자 이하인 것 제외 (변수명으로 보임)
    if name[0].islower() and len(name) <= 3:
        return False
    
    # 모두 소문자인 짧은 이름 제외 (item, items, list, map 등)
    if name.islower() and len(name) <= 6:
        return False
    
    return True


# ==================== RuleLoader 헬퍼 ====================
def _rule_loader() -> RuleLoader:
    return RuleLoader(target_lang="framework")


def analyze_code(code: str, context: str, ranges: list, count: int, api_key: str, locale: str) -> Dict[str, Any]:
    """코드 범위별 분석 - summary, calls, variables 추출 (컨텍스트와 코드 분리 전달)."""
    inputs = {"code": code, "ranges": ranges, "count": count, "locale": locale}
    if context.strip():
        inputs["context"] = context
    return _rule_loader().execute(
        "analysis",
        inputs,
        api_key,
    )


def analyze_class_summary_only(summaries: dict, api_key: str, locale: str, previous_summary: str = "") -> Dict[str, Any]:
    """클래스 전체 요약 생성 (Summary만).
    
    Args:
        summaries: 멤버 분석 결과 딕셔너리
        api_key: LLM API 키
        locale: 출력 언어
        previous_summary: 이전 청크의 요약 결과 (대용량 처리 시)
    """
    return _rule_loader().execute(
        "class_summary_only",
        {"summaries": summaries, "locale": locale, "previous_summary": previous_summary},
        api_key,
    )


def analyze_class_user_story(summary: str, api_key: str, locale: str) -> Dict[str, Any]:
    """클래스 User Story + AC 생성.
    
    Args:
        summary: 클래스의 상세 요약
        api_key: LLM API 키
        locale: 출력 언어
    """
    return _rule_loader().execute(
        "class_user_story",
        {"summary": summary, "locale": locale},
        api_key,
    )


def analyze_inheritance(declaration_code: str, api_key: str, locale: str) -> Dict[str, Any]:
    """상속/구현 관계 추출."""
    return _rule_loader().execute(
        "inheritance",
        {"declaration_code": declaration_code, "locale": locale},
        api_key,
    )


def analyze_field(declaration_code: str, api_key: str, locale: str) -> Dict[str, Any]:
    """필드 정보 추출."""
    return _rule_loader().execute(
        "field",
        {"declaration_code": declaration_code, "locale": locale},
        api_key,
    )


def analyze_method(declaration_code: str, api_key: str, locale: str) -> Dict[str, Any]:
    """메서드 시그니처 분석 - 파라미터/반환 타입 추출."""
    return _rule_loader().execute(
        "method",
        {"declaration_code": declaration_code, "locale": locale},
        api_key,
    )


def analyze_parent_context(skeleton_code: str, ancestor_context: str, api_key: str, locale: str) -> Dict[str, Any]:
    """부모 노드의 스켈레톤 코드에서 핵심 컨텍스트를 추출합니다."""
    return _rule_loader().execute(
        "parent_context",
        {"skeleton_code": skeleton_code, "ancestor_context": ancestor_context, "locale": locale},
        api_key,
    )
# ==================== 노드 수집기 ====================
class StatementCollector:
    """AST를 후위순회하여 StatementNode와 클래스 정보를 수집합니다."""

    def __init__(self, antlr_data: Dict[str, Any], file_content: str, directory: str, file_name: str):
        self.antlr_data = antlr_data
        self.file_content = file_content
        self.directory = directory
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
        return f"{self.directory}:{self.file_name}:{base}:{start_line}"

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
                log_process("ANALYZE", "COLLECT", f"📋 클래스 발견: {extracted_name} ({node_type}, 라인 {start_line}~{end_line})")

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
            "ANALYZE",
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
        current_nodes: List[StatementNode] = []
        current_tokens = 0
        batch_id = 1

        for node in nodes:
            if not node.analyzable:
                continue
            if node.has_children:
                if current_nodes:
                    batches.append(self._create_batch(batch_id, current_nodes))
                    log_process(
                        "ANALYZE",
                        "BATCH",
                        f"📦 배치 #{batch_id} 확정: 리프 노드 {len(current_nodes)}개 (토큰 {current_tokens}/{self.token_limit})",
                    )
                    batch_id += 1
                    current_nodes = []
                    current_tokens = 0
                batches.append(self._create_batch(batch_id, [node]))
                log_process(
                    "ANALYZE",
                    "BATCH",
                    f"📦 배치 #{batch_id} 확정: 부모 노드 단독 (라인 {node.start_line}~{node.end_line}, 토큰 {node.token})",
                )
                batch_id += 1
                continue
            if current_nodes and current_tokens + node.token > self.token_limit:
                batches.append(self._create_batch(batch_id, current_nodes))
                log_process(
                    "ANALYZE",
                    "BATCH",
                    f"📦 배치 #{batch_id} 확정: 토큰 한도 도달 (누적 {current_tokens}/{self.token_limit})",
                )
                batch_id += 1
                current_nodes = []
                current_tokens = 0
            current_nodes.append(node)
            current_tokens += node.token

        if current_nodes:
            batches.append(self._create_batch(batch_id, current_nodes))
            log_process(
                "ANALYZE",
                "BATCH",
                f"📦 배치 #{batch_id} 확정: 마지막 리프 노드 {len(current_nodes)}개 (토큰 {current_tokens}/{self.token_limit})",
            )
        return batches

    def _create_batch(self, batch_id: int, nodes: List[StatementNode]) -> AnalysisBatch:
        """배치 ID와 노드 리스트로 AnalysisBatch 객체를 생성합니다 (DBMS 스타일과 동일)."""
        ranges = [{"startLine": node.start_line, "endLine": node.end_line} for node in nodes]
        progress = max(node.end_line for node in nodes)
        return AnalysisBatch(
            batch_id=batch_id, 
            nodes=nodes, 
            ranges=ranges, 
            progress_line=progress
        )


# ==================== LLM 호출 ====================
class LLMInvoker:
    """배치를 LLM에 전달하여 분석 결과를 얻습니다.
    
    calls 배열은 analysis.yaml 프롬프트에 통합되어 
    분석 결과의 analysis[].calls 필드로 반환됩니다.
    """

    def __init__(self, api_key: str, locale: str):
        self.api_key = api_key
        self.locale = locale

    async def invoke(self, batch: AnalysisBatch) -> Optional[Dict[str, Any]]:
        """배치 코드를 LLM에 전달하여 분석 결과를 얻습니다.
        
        Returns:
            분석 결과 딕셔너리 (analysis 배열 포함, 각 요소에 calls 배열 포함)
        """
        if not batch.ranges:
            raise AnalysisError(f"배치 #{batch.batch_id}에 분석할 범위가 없습니다")

        code, context = batch.build_payload()
        result = await asyncio.to_thread(
            analyze_code,
            code,
            context,
            batch.ranges,
            len(batch.ranges),
            self.api_key,
            self.locale,
        )
        return result


# ==================== AST 프로세서 본체 ====================
class FrameworkAstProcessor:
    """Framework AST 처리 및 LLM 분석 파이프라인
    
    2단계 분석 지원:
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
        project_name: str,
        last_line: int,
    ):
        self.antlr_data = antlr_data
        self.file_content = file_content
        self.last_line = last_line
        self.directory = directory
        self.file_name = file_name
        self.user_id = user_id
        self.api_key = api_key
        self.locale = locale
        self.project_name = project_name
        self.max_workers = MAX_CONCURRENCY
        
        # full_directory: 디렉토리 + 파일명 (Neo4j directory 속성으로 사용)
        normalized_dir = directory.replace('\\', '/') if directory else ''
        self.full_directory = f"{normalized_dir}/{file_name}" if normalized_dir else file_name

        self.node_base_props = (
            f"directory: '{escape_for_cypher(self.full_directory)}', file_name: '{file_name}', "
            f"user_id: '{user_id}', project_name: '{project_name}'"
        )
        
        # AST 수집 결과 캐시 (Phase 1에서 수집, Phase 2에서 사용)
        self._nodes: Optional[List[StatementNode]] = None
        self._classes: Optional[Dict[str, ClassInfo]] = None
        self._field_type_cache: Optional[Dict[str, Dict[str, str]]] = None

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
        self._nodes, self._classes = collector.collect()
        
        if not self._nodes:
            raise AnalysisError(f"분석 대상 노드가 없습니다: {self.full_directory}")
        
        # 필드 타입 캐시 초기화
        self._field_type_cache = {key: {} for key in self._classes} if self._classes else {}
        
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
        
        # 관계 쿼리 생성 (HAS_METHOD, HAS_FIELD, CONTAINS, PARENT_OF)
        queries.extend(self._build_relationship_queries())
        
        log_process("ANALYZE", "PHASE1", f"✅ {self.full_directory}: {len(queries)}개 쿼리 생성")
        return queries

    async def _generate_parent_contexts(self) -> None:
        """부모 노드들의 컨텍스트를 top-down 방식으로 생성합니다.
        
        컨텍스트 생성이 필요한 부모 노드들에 대해:
        1. 부모의 context_ready_event를 기다림 (조상 컨텍스트 필요)
        2. 스켈레톤 코드 + 조상 컨텍스트로 LLM 호출
        3. 결과를 node.context에 저장
        4. context_ready_event 설정
        """
        if not self._nodes:
            return

        # 컨텍스트 생성이 필요한 노드 필터링
        context_nodes = [n for n in self._nodes if n.needs_context_generation()]
        
        if not context_nodes:
            # 컨텍스트 생성 필요 없으면 모든 노드의 context_ready_event 설정
            for node in self._nodes:
                node.context_ready_event.set()
            return

        log_process("ANALYZE", "CONTEXT", f"📝 부모 컨텍스트 생성: {len(context_nodes)}개 노드")

        # 깊이 순으로 정렬 (얕은 노드 먼저)
        def get_depth(node: StatementNode) -> int:
            depth = 0
            current = node.parent
            while current:
                depth += 1
                current = current.parent
            return depth

        context_nodes.sort(key=get_depth)

        semaphore = asyncio.Semaphore(self.max_workers)

        async def generate_context(node: StatementNode) -> None:
            async with semaphore:
                try:
                    # 부모의 컨텍스트가 준비될 때까지 대기
                    if node.parent:
                        await node.parent.context_ready_event.wait()

                    # 스켈레톤 코드 생성
                    skeleton = node.get_skeleton_code()
                    
                    # 조상 컨텍스트 가져오기
                    ancestor_ctx = node.get_ancestor_context()

                    # LLM 호출 (skeleton_code와 ancestor_context 분리 전달)
                    result = await asyncio.to_thread(
                        analyze_parent_context, skeleton, ancestor_ctx, self.api_key, self.locale
                    )

                    # 컨텍스트 저장
                    if isinstance(result, dict):
                        node.context = result.get("context_summary", "")
                    else:
                        # dict가 아닌 경우 예외 발생 (호출부에서 로그 남김)
                        raise ValueError(f"parent_context 규칙이 dict가 아닌 값을 반환했습니다: {type(result)}")

                except Exception as e:
                    log_process("ANALYZE", "CONTEXT", f"❌ 컨텍스트 생성 실패 (치명적): {node.node_type}[{node.start_line}]: {e}", logging.ERROR)
                    # 컨텍스트 없이 분석하면 변수/객체 해석 오류 등으로 결과가 엉망이 됨
                    # 예외를 다시 발생시켜서 실패를 명확히 표시
                    raise
                finally:
                    # 항상 context_ready_event 설정 (자식이 대기하지 않도록)
                    node.context_ready_event.set()

        # 컨텍스트 생성이 필요 없는 노드는 바로 event 설정
        context_node_set = set(n.node_id for n in context_nodes)
        for node in self._nodes:
            if node.node_id not in context_node_set:
                node.context_ready_event.set()

        # 병렬로 컨텍스트 생성
        await asyncio.gather(*[generate_context(n) for n in context_nodes])

        log_process("ANALYZE", "CONTEXT", f"✅ 부모 컨텍스트 생성 완료")

    async def run_llm_analysis(self) -> Tuple[List[str], int, List[Dict[str, Any]]]:
        """[Phase 2] LLM 분석을 실행하고 업데이트 쿼리를 생성합니다.
        
        중요: 자식→부모 요약 의존성을 보장하기 위해 completion_event 기반 대기
        - 부모 노드는 자식 노드의 completion_event를 기다린 후 실행
        - leaf 노드는 바로 실행, parent 노드는 자식 완료 후 실행
        
        컨텍스트 전달:
        - Phase 1.5: 부모 노드의 컨텍스트를 먼저 생성
        - Phase 2: 자식 분석 시 부모 컨텍스트를 전달
        
        Returns:
            (분석 결과 업데이트 쿼리 리스트, 실패한 배치 수, 실패 상세 정보 리스트)
        """
        if self._nodes is None:
            raise AnalysisError(f"Phase 1이 먼저 실행되어야 합니다: {self.file_name}")
        
        log_process("ANALYZE", "PHASE2", f"🤖 {self.full_directory} LLM 분석 시작")
        
        all_queries: List[str] = []
        failed_batch_count = 0
        all_failed_details: List[Dict[str, Any]] = []
        
        # 선행 처리: 상속/구현 + 필드 + 메서드
        preprocessing_queries = await self._run_preprocessing()
        all_queries.extend(preprocessing_queries)
        
        # Phase 1.5: 부모 컨텍스트 생성 (자식 분석 전에 먼저 실행)
        await self._generate_parent_contexts()
        
        # 배치 분석
        planner = BatchPlanner()
        batches = planner.plan(self._nodes)
        
        if not batches:
            log_process("ANALYZE", "PHASE2", f"⚠️ {self.full_directory}: 분석 대상 배치 없음")
            return all_queries, 0, []
        
        log_process("ANALYZE", "PHASE2", f"📊 배치 {len(batches)}개 (completion_event 기반 의존성 보장)")
        
        # 클래스별 summary 수집용 저장소
        class_summary_store: Dict[str, Dict[str, str]] = {key: {} for key in (self._classes or {})}
        
        # LLM 호출 및 결과 처리
        invoker = LLMInvoker(self.api_key, self.locale)
        
        async def process_batch(batch: AnalysisBatch, semaphore: asyncio.Semaphore) -> Tuple[List[str], Dict[str, Any]]:
            """배치 처리 후 쿼리와 분석 결과 반환. 노드에 summary도 설정.
            
            핵심: 부모 노드는 자식 completion_event를 기다린 후 실행됨
            → 깊이 계산 없이 자연스럽게 leaf → parent 순서 보장
            
            중요: 
            - try/finally로 completion_event.set()을 보장하여 데드락 방지
            - 자식 중 ok=False가 있으면 부모도 ok=False (불완전 요약 전파)
            - 부모 컨텍스트가 준비될 때까지 대기
            """
            batch_failed = False
            async with semaphore:
                try:
                    # 0. 각 노드의 부모 컨텍스트가 준비될 때까지 대기
                    for node in batch.nodes:
                        await node.context_ready_event.wait()
                    
                    # 1. 배치 내 모든 노드의 자식 완료를 기다림
                    for node in batch.nodes:
                        if node.has_children:
                            for child in node.children:
                                await child.completion_event.wait()
                                # 자식 중 하나라도 실패하면 부모도 불완전
                                if not child.ok:
                                    node.ok = False
                    
                    log_process("ANALYZE", "LLM", f"배치 #{batch.batch_id} 처리 중 ({len(batch.nodes)}개 노드)")
                    result = await invoker.invoke(batch)
                    
                    # 2. 노드에 summary 설정
                    if result:
                        analysis_list = result.get("analysis") or []
                        for node, analysis in zip(batch.nodes, analysis_list):
                            if analysis:
                                node.summary = analysis.get("summary") or ""
                    
                    queries = self._build_analysis_queries(batch, result)
                    return queries, {"batch": batch, "result": result}
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
                    queries, batch_data = result
                    all_queries.extend(queries)
                    
                    # 클래스별 summary 수집
                    batch_obj = batch_data["batch"]
                    llm_result = batch_data["result"]
                    if llm_result:
                        analysis_list = llm_result.get("analysis") or []
                        for node, analysis in zip(batch_obj.nodes, analysis_list):
                            if not analysis:
                                continue
                            summary = analysis.get("summary") or ""
                            if summary and node.class_key and node.class_key in class_summary_store:
                                key = f"{node.node_type}_{node.start_line}_{node.end_line}"
                                class_summary_store[node.class_key][key] = summary
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
        
        # 클래스별 summary 처리 (청크 기반 + User Story)
        if self._classes:
            class_queries = await self._process_class_summaries(class_summary_store)
            all_queries.extend(class_queries)
        
        # 실패 통계 로깅
        if failed_batch_count > 0:
            log_process("ANALYZE", "PHASE2", f"⚠️ {self.full_directory}: {failed_batch_count}개 배치 실패", logging.WARNING)
        
        log_process("ANALYZE", "PHASE2", f"✅ {self.full_directory}: {len(all_queries)}개 업데이트 쿼리")
        return all_queries, failed_batch_count, all_failed_details
    
    async def _process_class_summaries(self, class_summary_store: Dict[str, Dict[str, str]]) -> List[str]:
        """클래스별 summary를 청크 기반으로 처리하여 최종 summary + User Story 생성.
        
        Args:
            class_summary_store: 클래스별 노드 summary 저장소
            
        Returns:
            생성된 Neo4j 쿼리 리스트
        """
        queries: List[str] = []
        
        if not self._classes:
            return queries
        
        for class_key, info in self._classes.items():
            summaries = class_summary_store.get(class_key, {})
            if not summaries:
                continue
            
            # 클래스 노드 찾기
            class_node = next(
                (n for n in self._nodes if n.start_line == info.node_start and n.node_type == info.kind),
                None,
            )
            if not class_node:
                continue
            
            # 하위 노드 중 실패가 있으면 최종 summary/UserStory 스킵
            if not class_node.ok:
                log_process("ANALYZE", "SUMMARY", f"⚠️ {info.name}: 하위 분석 실패로 최종 summary 생성 스킵")
                continue
            
            all_user_stories: List[Dict[str, Any]] = []
            final_summary = ""
            
            try:
                # 1단계: 토큰 기준으로 청크 분할
                chunks = self._split_summaries_by_token(summaries, MAX_SUMMARY_CHUNK_TOKEN)
                
                if not chunks:
                    continue
                
                log_process("ANALYZE", "SUMMARY", f"📦 {info.name}: summary 청크 분할 완료 ({len(chunks)}개 청크)")
                
                # 2단계: 각 청크를 병렬로 처리하여 summary만 생성 (User Story는 최종 summary에서만 생성)
                async def process_chunk(chunk_idx: int, chunk: dict) -> str:
                    chunk_tokens = calculate_code_token(json.dumps(chunk, ensure_ascii=False))
                    log_process("ANALYZE", "SUMMARY", f"  → 청크 {chunk_idx + 1}/{len(chunks)} 처리 시작 (토큰: {chunk_tokens})")
                    
                    # Summary 생성
                    summary_result = await asyncio.to_thread(
                        analyze_class_summary_only,
                        chunk,
                        self.api_key,
                        self.locale,
                        ""
                    )
                    
                    chunk_summary = ""
                    if isinstance(summary_result, dict):
                        chunk_summary = summary_result.get('summary', '')
                    
                    return chunk_summary
                
                # 모든 청크를 병렬로 처리
                chunk_results_raw = await asyncio.gather(
                    *[process_chunk(idx, chunk) for idx, chunk in enumerate(chunks)]
                )
                
                # 결과 추출
                chunk_results = []
                for chunk_summary in chunk_results_raw:
                    if chunk_summary:
                        chunk_results.append(chunk_summary)
                
                if not chunk_results:
                    continue
                
                # 3단계: 모든 청크의 summary를 하나로 합치기
                if len(chunk_results) == 1:
                    final_summary = chunk_results[0]
                else:
                    combined_summaries = {f"CHUNK_{idx + 1}": s for idx, s in enumerate(chunk_results)}
                    final_summary_result = await asyncio.to_thread(
                        analyze_class_summary_only,
                        combined_summaries,
                        self.api_key,
                        self.locale,
                        ""
                    )
                    if isinstance(final_summary_result, dict):
                        final_summary = final_summary_result.get('summary', "\n\n".join(chunk_results))
                    else:
                        final_summary = "\n\n".join(chunk_results)
                
                log_process("ANALYZE", "SUMMARY", f"✅ {info.name}: summary 통합 완료")
                
                # 4단계: 최종 summary로 User Story 생성 (중복 방지를 위해 최종 summary에서만 생성)
                if final_summary:
                    user_story_result = await asyncio.to_thread(
                        analyze_class_user_story,
                        final_summary,
                        self.api_key,
                        self.locale
                    )
                    if isinstance(user_story_result, dict):
                        all_user_stories = user_story_result.get('user_stories', []) or []
                
                if all_user_stories:
                    log_process("ANALYZE", "SUMMARY", f"✅ {info.name}: User Story {len(all_user_stories)}개")
                else:
                    log_process("ANALYZE", "SUMMARY", f"✅ {info.name}: User Story 없음")
                
            except Exception as exc:
                log_process("ANALYZE", "SUMMARY", f"❌ 클래스 요약 생성 오류: {info.name}", logging.ERROR, exc)
                continue
            
            if not final_summary:
                continue
            
            # Neo4j 쿼리 생성
            escaped_summary = escape_for_cypher(str(final_summary))
            
            # Summary 저장
            queries.append(
                f"MATCH (n:{info.kind} {{startLine: {info.node_start}, {self.node_base_props}}})\n"
                f"SET n.summary = '{escaped_summary}'\n"
                f"RETURN n"
            )
            
            # User Story + AC 저장
            if all_user_stories:
                class_name_escaped = escape_for_cypher(info.name)
                for us_idx, us in enumerate(all_user_stories, 1):
                    us_id = us.get('id', f"US-{us_idx}")
                    role = escape_for_cypher(us.get('role', ''))
                    goal = escape_for_cypher(us.get('goal', ''))
                    benefit = escape_for_cypher(us.get('benefit', ''))
                    
                    # UserStory 노드 생성 및 관계
                    queries.append(
                        f"MATCH (c:{info.kind} {{startLine: {info.node_start}, {self.node_base_props}}})\n"
                        f"MERGE (us:UserStory {{id: '{escape_for_cypher(us_id)}', class_name: '{class_name_escaped}', {self.node_base_props}}})\n"
                        f"SET us.role = '{role}', us.goal = '{goal}', us.benefit = '{benefit}'\n"
                        f"MERGE (c)-[:HAS_USER_STORY]->(us)\n"
                        f"RETURN us"
                    )
                    
                    # AcceptanceCriteria 노드 생성 및 관계
                    for ac_idx, ac in enumerate(us.get('acceptance_criteria', []) or [], 1):
                        ac_id = ac.get('id', f"AC-{us_idx}-{ac_idx}")
                        ac_title = escape_for_cypher(ac.get('title', ''))
                        ac_given = escape_for_cypher(ac.get('given', ''))
                        ac_when = escape_for_cypher(ac.get('when', ''))
                        ac_then = escape_for_cypher(ac.get('then', ''))
                        
                        queries.append(
                            f"MATCH (us:UserStory {{id: '{escape_for_cypher(us_id)}', class_name: '{class_name_escaped}', {self.node_base_props}}})\n"
                            f"MERGE (ac:AcceptanceCriteria {{id: '{escape_for_cypher(ac_id)}', user_story_id: '{escape_for_cypher(us_id)}', {self.node_base_props}}})\n"
                            f"SET ac.title = '{ac_title}', ac.given = '{ac_given}', ac.when = '{ac_when}', ac.then = '{ac_then}'\n"
                            f"MERGE (us)-[:HAS_AC]->(ac)\n"
                            f"RETURN ac"
                        )
        
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

    def _build_analysis_queries(
        self, 
        batch: AnalysisBatch, 
        result: Optional[Dict[str, Any]]
    ) -> List[str]:
        """LLM 분석 결과를 MATCH 기반 업데이트 쿼리로 변환합니다."""
        queries: List[str] = []
        
        if not result:
            return queries
        
        analysis_list = result.get("analysis") or []
        
        for node, analysis in zip(batch.nodes, analysis_list):
            if not analysis:
                continue
            
            # 요약 업데이트 (MATCH + SET)
            summary = analysis.get("summary") or ""
            if summary:
                escaped_summary = escape_for_cypher(str(summary))
                queries.append(
                    f"MATCH (n:{node.node_type} {{startLine: {node.start_line}, {self.node_base_props}}})\n"
                    f"SET n.summary = '{escaped_summary}'\n"
                    f"RETURN n"
                )
            
            # DEPENDENCY 관계 (localDependencies)
            for dep in analysis.get("localDependencies", []) or []:
                if not dep:
                    continue
                dep_type = dep.get("type", "") if isinstance(dep, dict) else str(dep)
                if not dep_type or not _is_valid_class_name_for_calls(dep_type):
                    log_process("ANALYZE", "DEPENDENCY", f"⚠️ 유효하지 않은 의존 타입 제외: {dep_type} (node={node.start_line})", logging.DEBUG)
                    continue
                source_member = dep.get("sourceMember", "unknown") if isinstance(dep, dict) else "unknown"
                
                # class_kind와 parent 확인
                if not node.class_kind:
                    log_process("ANALYZE", "DEPENDENCY", f"⚠️ class_kind가 None: {dep_type} (node={node.start_line}, type={node.node_type})", logging.DEBUG)
                    continue
                if not node.parent:
                    log_process("ANALYZE", "DEPENDENCY", f"⚠️ parent가 None: {dep_type} (node={node.start_line}, type={node.node_type})", logging.DEBUG)
                    continue
                
                # 클래스 노드 찾기 (class_kind와 parent.start_line 사용)
                queries.append(
                    f"MATCH (src:{node.class_kind} {{startLine: {node.parent.start_line}, {self.node_base_props}}})\n"
                    f"MATCH (dst) WHERE (dst:CLASS OR dst:INTERFACE OR dst:ENUM)\n"
                    f"  AND toLower(dst.class_name) = toLower('{escape_for_cypher(dep_type)}')\n"
                    f"  AND dst.user_id = '{self.user_id}' AND dst.project_name = '{self.project_name}'\n"
                    f"  AND src <> dst AND NOT (src)-[:ASSOCIATION|COMPOSITION]->(dst)\n"
                    f"MERGE (src)-[r:DEPENDENCY {{usage: 'local', source_member: '{escape_for_cypher(source_member)}'}}]->(dst)\n"
                    f"RETURN r"
                )
                log_process("ANALYZE", "DEPENDENCY", f"✅ DEPENDENCY 관계 생성: {node.class_kind} -> {dep_type} (sourceMember={source_member})", logging.DEBUG)
            
            # CALLS 관계 (calls 배열 - 프롬프트 통합)
            for call_str in analysis.get("calls", []) or []:
                if not call_str or not isinstance(call_str, str):
                    continue
                parts = call_str.split(".", 1)
                if len(parts) != 2:
                    continue
                target_class, method_name = parts
                
                if not _is_valid_class_name_for_calls(target_class):
                    continue
                
                if node.class_kind and node.parent:
                    queries.append(
                        f"MATCH (src:{node.class_kind} {{startLine: {node.parent.start_line}, {self.node_base_props}}})\n"
                        f"MATCH (dst) WHERE (dst:CLASS OR dst:INTERFACE OR dst:ENUM)\n"
                        f"  AND toLower(dst.class_name) = toLower('{escape_for_cypher(target_class)}')\n"
                        f"  AND dst.user_id = '{self.user_id}' AND dst.project_name = '{self.project_name}'\n"
                        f"MERGE (src)-[r:CALLS {{method: '{escape_for_cypher(method_name)}'}}]->(dst)\n"
                        f"RETURN r"
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
        """정적 관계 쿼리 (HAS_METHOD, HAS_FIELD, CONTAINS, PARENT_OF)를 생성합니다.
        
        규칙:
        - File → CLASS/INTERFACE/ENUM (최상위 타입만): CONTAINS
        - Class → Method: HAS_METHOD
        - Class → Field: HAS_FIELD
        - 그 외 부모-자식: PARENT_OF
        """
        queries: List[str] = []
        
        for node in self._nodes or []:
            if not node.parent:
                continue
            
            # 부모-자식 관계 생성
            parent = node.parent
            
            # File → 최상위 타입(CLASS/INTERFACE/ENUM)만 CONTAINS
            if parent.node_type == "FILE" and node.node_type in CLASS_TYPES:
                queries.append(self._build_contains_query(parent, node))
            # Class → Method: HAS_METHOD
            elif node.node_type in METHOD_TYPES:
                queries.append(self._build_has_method_query(parent, node))
            # Class → Field: HAS_FIELD
            elif node.node_type in FIELD_TYPES:
                queries.append(self._build_has_field_query(parent, node))
            # 그 외: PARENT_OF
            else:
                queries.append(self._build_parent_of_query(parent, node))
        
        return queries
    
    def _build_contains_query(self, parent: StatementNode, child: StatementNode) -> str:
        """CONTAINS 관계 쿼리를 생성합니다 (File → 직접 자식만)."""
        return (
                f"MATCH (p:{parent.node_type} {{startLine: {parent.start_line}, {self.node_base_props}}})\n"
            f"MATCH (c:{child.node_type} {{startLine: {child.start_line}, {self.node_base_props}}})\n"
            f"MERGE (p)-[r:CONTAINS]->(c)\n"
                f"RETURN r"
            )
        
    def _build_has_method_query(self, parent: StatementNode, child: StatementNode) -> str:
        """HAS_METHOD 관계 쿼리를 생성합니다."""
        return (
            f"MATCH (p:{parent.node_type} {{startLine: {parent.start_line}, {self.node_base_props}}})\n"
            f"MATCH (c:{child.node_type} {{startLine: {child.start_line}, {self.node_base_props}}})\n"
            f"MERGE (p)-[r:HAS_METHOD]->(c)\n"
            f"RETURN r"
        )
    
    def _build_has_field_query(self, parent: StatementNode, child: StatementNode) -> str:
        """HAS_FIELD 관계 쿼리를 생성합니다."""
        return (
            f"MATCH (p:{parent.node_type} {{startLine: {parent.start_line}, {self.node_base_props}}})\n"
            f"MATCH (c:{child.node_type} {{startLine: {child.start_line}, {self.node_base_props}}})\n"
            f"MERGE (p)-[r:HAS_FIELD]->(c)\n"
            f"RETURN r"
        )
    
    def _build_parent_of_query(self, parent: StatementNode, child: StatementNode) -> str:
        """PARENT_OF 관계 쿼리를 생성합니다."""
        return (
            f"MATCH (p:{parent.node_type} {{startLine: {parent.start_line}, {self.node_base_props}}})\n"
            f"MATCH (c:{child.node_type} {{startLine: {child.start_line}, {self.node_base_props}}})\n"
            f"MERGE (p)-[r:PARENT_OF]->(c)\n"
            f"RETURN r"
        )

    async def _run_preprocessing(self) -> List[str]:
        """선행 처리: 상속/구현, 필드, 메서드 분석 후 쿼리 생성."""
        queries: List[str] = []
        
        # 상속/구현, 필드, 메서드 노드 분류
        inheritance_nodes = []
        field_nodes = []
        method_nodes = []
        
        for node in self._nodes or []:
            if node.node_type in INHERITANCE_TYPES:
                inheritance_nodes.append(node)
            elif node.node_type in FIELD_TYPES:
                field_nodes.append(node)
            elif node.node_type in METHOD_SIGNATURE_TYPES:
                method_nodes.append(node)
        
        # 병렬 처리
        tasks = []
        
        if inheritance_nodes:
            tasks.append(self._analyze_inheritance_nodes(inheritance_nodes))
        if field_nodes:
            tasks.append(self._analyze_field_nodes(field_nodes))
        if method_nodes:
            tasks.append(self._analyze_method_nodes(method_nodes))
        
        if tasks:
            results = await asyncio.gather(*tasks, return_exceptions=True)
            for result in results:
                if isinstance(result, list):
                    queries.extend(result)
                elif isinstance(result, Exception):
                    log_process("ANALYZE", "PREPROCESS", f"선행 처리 오류: {result}", logging.WARNING)
        
        return queries

    async def _analyze_inheritance_nodes(self, nodes: List[StatementNode]) -> List[str]:
        """상속/구현 노드 분석."""
        queries: List[str] = []
        semaphore = asyncio.Semaphore(INHERITANCE_CONCURRENCY)
        
        async def analyze_one(node: StatementNode) -> List[str]:
            async with semaphore:
                try:
                    result = await asyncio.to_thread(
                        analyze_inheritance, node.code, self.api_key, self.locale
                    )
                    return self._build_inheritance_queries(node, result)
                except Exception as e:
                    log_process("ANALYZE", "INHERITANCE", f"❌ 상속 분석 실패 (node={node.start_line}): {e}", logging.ERROR, e)
                    raise
        
        results = await asyncio.gather(*[analyze_one(n) for n in nodes])
        for r in results:
            queries.extend(r)
        
        return queries

    async def _analyze_field_nodes(self, nodes: List[StatementNode]) -> List[str]:
        """필드 노드 분석."""
        queries: List[str] = []
        semaphore = asyncio.Semaphore(FIELD_CONCURRENCY)
        
        async def analyze_one(node: StatementNode) -> List[str]:
            async with semaphore:
                try:
                    result = await asyncio.to_thread(
                        analyze_field, node.code, self.api_key, self.locale
                    )
                    return self._build_field_queries(node, result)
                except Exception as e:
                    log_process("ANALYZE", "FIELD", f"❌ 필드 분석 실패 (node={node.start_line}): {e}", logging.ERROR, e)
                    raise
        
        results = await asyncio.gather(*[analyze_one(n) for n in nodes])
        for r in results:
            queries.extend(r)
        
        return queries

    async def _analyze_method_nodes(self, nodes: List[StatementNode]) -> List[str]:
        """메서드 시그니처 분석."""
        queries: List[str] = []
        semaphore = asyncio.Semaphore(METHOD_CONCURRENCY)
        
        async def analyze_one(node: StatementNode) -> List[str]:
            async with semaphore:
                try:
                    result = await asyncio.to_thread(
                        analyze_method, node.code, self.api_key, self.locale
                    )
                    return self._build_method_queries(node, result)
                except Exception as e:
                    log_process("ANALYZE", "METHOD", f"❌ 메서드 분석 실패 (node={node.start_line}): {e}", logging.ERROR, e)
                    raise
        
        results = await asyncio.gather(*[analyze_one(n) for n in nodes])
        for r in results:
            queries.extend(r)
        
        return queries

    # ===== 쿼리 빌더 메서드 =====
    def _build_static_node_queries(self, node: StatementNode) -> List[str]:
        """정적 노드 생성 쿼리 리스트를 반환합니다."""
        queries: List[str] = []
        label = node.node_type
        
        # name 속성 결정: CLASS/INTERFACE/METHOD는 실제 이름, 그 외는 타입[라인번호]
        if label == "FILE":
            node_name = self.file_name
        elif label in CLASS_TYPES and node.class_name:
            node_name = node.class_name
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
        
        # CLASS/INTERFACE/ENUM 노드: MERGE로 생성 (중복 방지)
        # 새 아키텍처에서는 Phase 1에서 모든 클래스가 먼저 생성되므로 TEMP 노드 패턴 제거
        if label in ("CLASS", "INTERFACE", "ENUM") and node.class_name:
            escaped_class_name = escape_for_cypher(node.class_name)
            queries.append(
                f"MERGE (n:{label} {{class_name: '{escaped_class_name}', user_id: '{self.user_id}', project_name: '{self.project_name}'}})\n"
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

    def _build_inheritance_queries(self, node: StatementNode, analysis: Dict[str, Any]) -> List[str]:
        """상속/구현 분석 결과를 Neo4j 쿼리로 변환합니다."""
        if not isinstance(analysis, dict):
            raise AnalysisError(f"상속 분석 결과가 유효하지 않습니다 (node={node.start_line}): {type(analysis)}")

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

            # Phase 1에서 모든 클래스가 생성되므로 TEMP 노드 생성 없이 MATCH만 사용
            # 존재하지 않는 클래스(외부 라이브러리 등)에 대한 관계는 생성되지 않음
            queries.append(
                f"{src_match}\n"
                f"MATCH (dst) WHERE (dst:CLASS OR dst:INTERFACE OR dst:ENUM)\n"
                f"  AND toLower(dst.class_name) = toLower('{to_type}')\n"
                f"  AND dst.user_id = '{self.user_id}'\n"
                f"  AND dst.project_name = '{self.project_name}'\n"
                f"MERGE (src)-[r:{rel_type}]->(dst)\n"
                f"RETURN src, dst, r"
            )

        return queries

    def _build_field_queries(self, node: StatementNode, analysis: Dict[str, Any]) -> List[str]:
        """필드 분석 결과를 Neo4j 쿼리로 변환합니다."""
        if not isinstance(analysis, dict):
            raise AnalysisError(f"필드 분석 결과가 유효하지 않습니다 (node={node.start_line}): {type(analysis)}")

        queries: List[str] = []
        fields = analysis.get("fields") or []

        for field_info in fields:
            field_name = escape_for_cypher(field_info.get("field_name") or "")
            field_type_raw = field_info.get("field_type") or ""
            field_type = escape_for_cypher(field_type_raw)
            target_class_raw = field_info.get("target_class")
            target_class = escape_for_cypher(target_class_raw) if target_class_raw else None
            visibility = escape_for_cypher(field_info.get("visibility") or "private")
            is_static = "true" if field_info.get("is_static") else "false"
            is_final = "true" if field_info.get("is_final") else "false"
            multiplicity = escape_for_cypher(field_info.get("multiplicity") or "1")
            association_type = field_info.get("association_type") or "ASSOCIATION"

            if not field_name:
                continue

            # 필드 타입 캐시 업데이트 (Collection/Map 필터링용)
            if node.class_key and node.class_key in self._field_type_cache:
                # escape 전 원본 필드명과 타입 저장
                original_field_name = field_info.get("field_name") or ""
                self._field_type_cache[node.class_key][original_field_name] = field_type_raw

            # FIELD 노드 속성 업데이트
            # target_class가 있으면 클래스 타입 필드 (연관 관계 대상)
            target_class_set = f", f.target_class = '{target_class}'" if target_class else ""
            queries.append(
                f"MATCH (f:FIELD {{startLine: {node.start_line}, {self.node_base_props}}})\n"
                f"SET f.name = '{field_name}', f.field_type = '{field_type}', "
                f"f.visibility = '{visibility}', f.is_static = {is_static}, f.is_final = {is_final}{target_class_set}\n"
                f"RETURN f"
            )

            # 연관 관계 생성 (ASSOCIATION, COMPOSITION)
            # Phase 1에서 모든 클래스가 생성되므로 TEMP 노드 생성 없이 MATCH만 사용
            if target_class:
                src_match = f"MATCH (src:{node.class_kind or 'CLASS'} {{startLine: {node.parent.start_line if node.parent else node.start_line}, {self.node_base_props}}})"
                queries.append(
                    f"{src_match}\n"
                    f"MATCH (dst) WHERE (dst:CLASS OR dst:INTERFACE OR dst:ENUM)\n"
                    f"  AND toLower(dst.class_name) = toLower('{target_class}')\n"
                    f"  AND dst.user_id = '{self.user_id}'\n"
                    f"  AND dst.project_name = '{self.project_name}'\n"
                    f"MERGE (src)-[r:{association_type} {{source_member: '{field_name}', multiplicity: '{multiplicity}'}}]->(dst)\n"
                    f"RETURN src, dst, r"
                )

        return queries

    def _build_method_queries(self, node: StatementNode, analysis: Dict[str, Any]) -> List[str]:
        """메서드 분석 결과를 Neo4j 쿼리로 변환합니다."""
        if not isinstance(analysis, dict):
            raise AnalysisError(f"메서드 분석 결과가 유효하지 않습니다 (node={node.start_line}): {type(analysis)}")

        queries: List[str] = []
        
        method_name = escape_for_cypher(analysis.get("method_name") or "")
        return_type = escape_for_cypher(analysis.get("return_type") or "void")
        visibility = escape_for_cypher(analysis.get("visibility") or "public")
        is_static = "true" if analysis.get("is_static") else "false"
        method_kind = escape_for_cypher(analysis.get("method_type") or "normal")
        parameters = analysis.get("parameters") or []
        dependencies = analysis.get("dependencies") or []

        # METHOD 노드에 시그니처 정보 저장 (name도 methodName으로 설정)
        queries.append(
            f"MATCH (m:{node.node_type} {{startLine: {node.start_line}, {self.node_base_props}}})\n"
            f"SET m.name = '{method_name}', m.return_type = '{return_type}', "
            f"m.visibility = '{visibility}', m.is_static = {is_static}, "
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
                # Parameter 노드 속성명은 snake_case로 통일
                f"MERGE (p:Parameter {{name: '{param_name}', method_start_line: {node.start_line}, {self.node_base_props}}})\n"
                f"SET p.type = '{param_type}', p.index = {idx}\n"
                f"MERGE (m)-[r:HAS_PARAMETER]->(p)\n"
                f"RETURN m, p, r"
            )

        # 의존 관계 생성 (DEPENDENCY) - 연관 관계가 없을 때만
        # Phase 1에서 모든 클래스가 생성되므로 TEMP 노드 생성 없이 MATCH만 사용
        for dep in dependencies:
            target_type = escape_for_cypher(dep.get("target_class") or "")
            usage = escape_for_cypher(dep.get("usage") or "parameter")
            is_value_object_cypher = "true" if dep.get("is_value_object") else "false"

            if not target_type:
                continue

            src_match = f"MATCH (src:{node.class_kind or 'CLASS'} {{startLine: {node.parent.start_line if node.parent else node.start_line}, {self.node_base_props}}})"
            queries.append(
                f"{src_match}\n"
                f"MATCH (dst) WHERE (dst:CLASS OR dst:INTERFACE OR dst:ENUM)\n"
                f"  AND toLower(dst.class_name) = toLower('{target_type}')\n"
                f"  AND dst.user_id = '{self.user_id}'\n"
                f"  AND dst.project_name = '{self.project_name}'\n"
                f"  AND src <> dst\n"
                f"  AND NOT (src)-[:ASSOCIATION|COMPOSITION]->(dst)\n"
                f"MERGE (src)-[r:DEPENDENCY {{usage: '{usage}', source_member: '{method_name}'}}]->(dst)\n"
                f"SET r.is_value_object = {is_value_object_cypher}\n"
                f"RETURN src, dst, r"
            )

        # 필드 할당 패턴에 따른 연관 관계 세분화 (ASSOCIATION → COMPOSITION)
        field_assignments = analysis.get("field_assignments") or []
        src_start_line = node.parent.start_line if node.parent else node.start_line
        for assign in field_assignments:
            field_name = escape_for_cypher(assign.get("field_name") or "")
            value_source = assign.get("value_source") or ""

            if not field_name or not value_source:
                continue

            # value_source가 "new"인 경우에만 COMPOSITION으로 변경 (parameter는 ASSOCIATION 유지)
            if value_source == "new":
                # FIELD 노드의 target_class가 있으면 (클래스 타입 필드) 기존 ASSOCIATION을 COMPOSITION으로 변경
                queries.append(
                    f"MATCH (field:FIELD {{name: '{field_name}', {self.node_base_props}}})\n"
                    f"WHERE field.target_class IS NOT NULL\n"
                    f"MATCH (src:{node.class_kind or 'CLASS'} {{startLine: {src_start_line}, {self.node_base_props}}})"
                    f"-[r:ASSOCIATION {{source_member: '{field_name}'}}]->(dst)\n"
                    f"WITH src, dst, COALESCE(r.multiplicity, '1') AS mult, r\n"
                    f"DELETE r\n"
                    f"MERGE (src)-[r2:COMPOSITION {{source_member: '{field_name}', multiplicity: mult}}]->(dst)\n"
                    f"RETURN src, dst, r2"
                )

        return queries
