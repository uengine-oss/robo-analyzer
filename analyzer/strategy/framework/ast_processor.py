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
METHOD_CALL_TYPES = frozenset(["METHOD_CALL", "METHOD_INVOCATION", "CALL"])
METHOD_CALL_CANDIDATE_TYPES = frozenset(["ASSIGNMENT", "RETURN", "VARIABLE"])

# 설정에서 가져오는 상수
MAX_BATCH_TOKEN = settings.batch.framework_max_batch_token
MAX_CONCURRENCY = settings.concurrency.framework_max_concurrency
INHERITANCE_CONCURRENCY = settings.concurrency.inheritance_concurrency
FIELD_CONCURRENCY = settings.concurrency.field_concurrency
METHOD_CONCURRENCY = settings.concurrency.method_concurrency
STATIC_QUERY_BATCH_SIZE = settings.batch.static_query_batch_size
MAX_SUMMARY_CHUNK_TOKEN = settings.batch.max_summary_chunk_token

# 정규식 패턴
LINE_NUMBER_PATTERN = re.compile(r"^(\d+)\s*:")
METHOD_CALL_PATTERN = re.compile(r'\w+\.\w+\s*\(')

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

# Collection/Map 타입 프리픽스 - 필드 타입 기반 method_call 필터링용
COLLECTION_TYPE_PREFIXES = (
    # Map 계열
    "Map<", "HashMap<", "LinkedHashMap<", "TreeMap<", "ConcurrentHashMap<",
    "Hashtable<", "WeakHashMap<", "IdentityHashMap<", "EnumMap<",
    # List 계열
    "List<", "ArrayList<", "LinkedList<", "CopyOnWriteArrayList<", "Vector<",
    # Set 계열
    "Set<", "HashSet<", "TreeSet<", "LinkedHashSet<", "EnumSet<",
    "ConcurrentSkipListSet<", "CopyOnWriteArraySet<",
    # 기타 Collection 계열
    "Collection<", "Queue<", "Deque<", "Stack<", "PriorityQueue<",
    "ArrayDeque<", "ConcurrentLinkedQueue<", "BlockingQueue<",
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
        """LLM 호출용 코드 페이로드를 생성합니다."""
        return "\n\n".join(
            node.get_compact_code() if node.has_children else node.get_raw_code()
            for node in self.nodes
        )

    def get_parent_code(self) -> str:
        """배치 노드들의 부모 코드를 가져옴 (컨텍스트용)."""
        if not self.nodes:
            return ""
        first_node = self.nodes[0]
        if first_node.parent:
            return (
                first_node.parent.get_placeholder_code()
                if first_node.parent.has_children
                else first_node.parent.get_raw_code()
            )
        return ""


@dataclass(slots=True)
class BatchResult:
    """배치 처리 결과 (calls 배열은 general_result에 통합됨)."""
    batch: AnalysisBatch
    general_result: Optional[Dict[str, Any]]


# ==================== 헬퍼 함수 ====================
def _is_method_call_node(node: StatementNode) -> bool:
    """노드가 메서드 호출을 포함하는지 확인합니다.
    
    - METHOD_CALL 타입 노드 (직접적인 메서드 호출)
    - 메서드 호출이 나타날 수 있는 노드 타입이면서 메서드 호출 패턴이 포함된 노드
    """
    if node.node_type in METHOD_CALL_TYPES:
        return True
    if node.node_type in METHOD_CALL_CANDIDATE_TYPES:
        return METHOD_CALL_PATTERN.search(node.code) is not None
    return False


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


def analyze_code(code: str, ranges: list, count: int, api_key: str, locale: str, parent_code: str = "") -> Dict[str, Any]:
    """코드 범위별 분석 - summary, calls, variables 추출."""
    return _rule_loader().execute(
        "analysis",
        {"code": code, "ranges": ranges, "count": count, "locale": locale, "parent_code": parent_code},
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


# method_call.yaml 프롬프트가 analysis.yaml에 통합되어 더 이상 필요 없음
# calls 배열은 분석 결과의 analysis[].calls 필드에서 추출


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
        method_call_ranges = [
            {"startLine": node.start_line, "endLine": node.end_line, "type": node.node_type, "code": node.code}
            for node in nodes
            if _is_method_call_node(node)
        ]
        progress = max(node.end_line for node in nodes)
        return AnalysisBatch(
            batch_id=batch_id, 
            nodes=nodes, 
            ranges=ranges, 
            method_call_ranges=method_call_ranges,
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

        result = await asyncio.to_thread(
            analyze_code,
            batch.build_payload(),
            batch.ranges,
            len(batch.ranges),
            self.api_key,
            self.locale,
            batch.get_parent_code(),
        )
        return result


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
        classes: Dict[str, ClassInfo],
        api_key: str,
        locale: str,
        user_id: str,
        project_name: str,
        directory: str,
        file_name: str,
    ):
        self.send_queue = send_queue
        self.receive_queue = receive_queue
        self.file_last_line = file_last_line
        self._nodes = nodes
        self.node_base_props = node_base_props
        self.classes = classes
        self.api_key = api_key
        self.locale = locale
        self.user_id = user_id
        self.project_name = project_name
        # directory는 이미 full_directory 형태 (파일명 포함)
        self.directory = directory
        self.file_name = file_name

        self._pending: Dict[int, BatchResult] = {}
        self._next_batch_id = 1
        self._lock = asyncio.Lock()
        self._finalized_classes: set[str] = set()
        self._class_summary_store: Dict[str, Dict[str, Any]] = {key: {} for key in classes}
        # 필드 타입 캐시: class_key → {field_name: field_type}
        # Collection/Map 타입 필드의 메서드 호출 필터링에 사용
        self._field_type_cache: Dict[str, Dict[str, str]] = {key: {} for key in classes}

    async def submit(
        self, 
        batch: AnalysisBatch, 
        analysis_result: Optional[Dict[str, Any]],
    ):
        """워커가 batch 처리를 마친 뒤 Apply 큐에 등록합니다."""
        async with self._lock:
            self._pending[batch.batch_id] = BatchResult(
                batch=batch, 
                general_result=analysis_result,
            )
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
        
        # 분석 정보 수집 (스트림 메시지용)
        analyzed_node_info: Optional[Dict[str, Any]] = None

        for node, analysis in zip(result.batch.nodes, analysis_list):
            if not analysis:
                log_process("ANALYZE", "APPLY", f"⚠️ {node.start_line}~{node.end_line} 구간 요약 없음 - 건너뜀")
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
            log_process("ANALYZE", "APPLY", f"✅ {node.start_line}~{node.end_line} 구간 요약 반영")
            
            # 첫 번째 분석 결과의 정보 저장
            if not analyzed_node_info:
                analyzed_node_info = {
                    "type": node.node_type,
                    "name": node.class_name or f"Line {node.start_line}",
                    "summary": str(summary)[:100],
                    "line_range": f"{node.start_line}-{node.end_line}",
                }

            # 로컬 변수 의존 관계 (DEPENDENCY) - 연관 관계가 없을 때만
            # localDependencies는 객체 배열: [{"type": "타입명", "sourceMember": "메서드명"}]
            #
            # ✅ 관계 중복 방지 정책:
            # - (src)-[:DEPENDENCY {usage:'local'}]->(dst) 관계는 src->dst당 1개만 유지
            # - 의존 발생 위치는 r.source_members(List<String>)에 누적
            for dep in analysis.get("localDependencies", []) or []:
                if not dep:
                    continue
                
                # LLM 응답 형식 처리 (문자열 또는 객체)
                if isinstance(dep, str):
                    dep_type = dep
                    source_member = "unknown"
                else:
                    dep_type = dep.get("type", "")
                    source_member = dep.get("sourceMember", "") or "unknown"
                
                if not dep_type:
                    continue
                    
                # 유효하지 않은 클래스명이면 DEPENDENCY 관계 생성 건너뜀
                if not _is_valid_class_name_for_calls(dep_type):
                    log_process("ANALYZE", "APPLY", f"⚠️ 유효하지 않은 의존 대상 제외: {dep_type}")
                    continue
                    
                escaped_dep = escape_for_cypher(dep_type)
                escaped_source = escape_for_cypher(source_member)
                # 소속 클래스에서 타겟 클래스로 DEPENDENCY 관계 생성 (기존 클래스가 있을 때만)
                if node.class_kind and node.parent:
                    queries.append(
                        f"MATCH (src:{node.class_kind} {{startLine: {node.parent.start_line}, {self.node_base_props}}})\n"
                        f"MATCH (dst)\n"
                        f"WHERE (dst:CLASS OR dst:INTERFACE OR dst:ENUM)\n"
                        f"  AND toLower(dst.class_name) = toLower('{escaped_dep}')\n"
                        f"  AND dst.user_id = '{self.user_id}'\n"
                        f"  AND dst.project_name = '{self.project_name}'\n"
                        f"  AND src <> dst\n"  # 자기 자신 의존 방지
                        f"  AND NOT (src)-[:ASSOCIATION|COMPOSITION]->(dst)\n"
                        f"MERGE (src)-[r:DEPENDENCY {{usage: 'local', source_member: '{escaped_source}'}}]->(dst)\n"
                        f"RETURN src, dst, r"
                    )

            # calls 배열 처리 (프롬프트에 통합됨)
            # 형식: ["target.methodName", "target2.methodName2"]
            calls = analysis.get("calls", []) or []
            for call_str in calls:
                if not call_str or not isinstance(call_str, str):
                    continue
                # "target.methodName" 형식 파싱
                parts = call_str.split(".", 1)
                if len(parts) != 2:
                    continue
                target_class, method_name = parts
                
                # 유효성 검사
                if not _is_valid_class_name_for_calls(target_class):
                    log_process("ANALYZE", "CALLS", f"⚠️ 제외 (표준/유틸리티): {call_str}")
                    continue
                
                # CALLS 관계 쿼리 생성 (MATCH 기반)
                escaped_target = escape_for_cypher(target_class)
                escaped_method = escape_for_cypher(method_name)
                
                if node.class_kind and node.parent:
                    queries.append(
                        f"MATCH (src:{node.class_kind} {{startLine: {node.parent.start_line}, {self.node_base_props}}})\n"
                        f"MATCH (dst)\n"
                        f"WHERE (dst:CLASS OR dst:INTERFACE OR dst:ENUM)\n"
                        f"  AND toLower(dst.class_name) = toLower('{escaped_target}')\n"
                        f"  AND dst.user_id = '{self.user_id}'\n"
                        f"  AND dst.project_name = '{self.project_name}'\n"
                        f"MERGE (src)-[r:CALLS {{method: '{escaped_method}'}}]->(dst)\n"
                        f"RETURN src, dst, r"
                    )

            self._update_class_store(node, analysis)
            node.completion_event.set()

        # completion_event 미설정 노드 처리
        for node in result.batch.nodes:
            if not node.completion_event.is_set():
                node.completion_event.set()

        if queries:
            await self._send_queries(queries, result.batch.progress_line, analyzed_node_info)
        log_process("ANALYZE", "APPLY", f"✅ 배치 #{result.batch.batch_id} 적용 완료")

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
        """클래스 요약 + User Story + AC 생성.
        
        대용량 summary가 있을 경우 청크로 나누어 처리하고,
        이전 청크 결과를 다음 청크에 전달하여 연속성을 유지합니다.
        """
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

        all_user_stories = []
        final_summary = ""

        try:
            # 1단계: 토큰 기준으로 청크 분할
            def _split_summaries_by_token(summaries: dict, max_token: int) -> List[dict]:
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
            
            chunks = _split_summaries_by_token(summaries, MAX_SUMMARY_CHUNK_TOKEN)
            
            if not chunks:
                class_node.completion_event.set()
                return
            
            log_process("ANALYZE", "SUMMARY", f"📦 {info.name}: summary 청크 분할 완료 ({len(chunks)}개 청크)")
            
            # 2단계: 각 청크를 병렬로 처리하여 summary 생성 및 User Story 생성
            async def process_chunk(chunk_idx: int, chunk: dict) -> Tuple[str, List[Dict[str, Any]]]:
                """청크를 처리하여 summary와 User Story 생성 (병렬 처리용)."""
                chunk_tokens = calculate_code_token(json.dumps(chunk, ensure_ascii=False))
                log_process("ANALYZE", "SUMMARY", f"  → 청크 {chunk_idx + 1}/{len(chunks)} 처리 시작 (토큰: {chunk_tokens})")
                
                # Summary 생성
                summary_result = await asyncio.to_thread(
                    analyze_class_summary_only,
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
                        analyze_class_user_story,
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
                class_node.completion_event.set()
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
            
            # 4단계: 최종 summary로도 User Story 생성 (청크별 User Story와 함께 수집)
            final_user_story_result = await asyncio.to_thread(
                analyze_class_user_story,
                final_summary,
                self.api_key,
                self.locale
            )
            
            if isinstance(final_user_story_result, dict):
                final_user_stories = final_user_story_result.get('user_stories', []) or []
                all_user_stories.extend(final_user_stories)
            
            if all_user_stories:
                log_process("ANALYZE", "SUMMARY", f"✅ {info.name}: User Story {len(all_user_stories)}개")
            else:
                log_process("ANALYZE", "SUMMARY", f"✅ {info.name}: User Story 없음")
                
        except Exception as exc:
            log_process("ANALYZE", "SUMMARY", f"❌ 클래스 요약 생성 오류: {info.name}", logging.ERROR, exc)
            class_node.completion_event.set()
            raise AnalysisError(f"클래스 요약 생성 실패 ({info.name}): {exc}") from exc

        if not final_summary:
            class_node.completion_event.set()
            return

        # Neo4j에 summary 저장 및 User Story/AC를 노드와 관계로 저장
        escaped_summary = escape_for_cypher(str(final_summary))
        
        # Summary 저장
        summary_query = (
            f"MATCH (n:{info.kind} {{startLine: {info.node_start}, {self.node_base_props}}})\n"
            f"SET n.summary = '{escaped_summary}'\n"
            f"RETURN n"
        )
        
        queries = [summary_query]
        
        # User Story와 AC를 노드와 관계로 저장 (유효한 User Story가 있는 경우만)
        if all_user_stories:
            class_name_escaped = escape_for_cypher(info.name)
            for us_idx, us in enumerate(all_user_stories, 1):
                us_id = us.get('id', f"US-{us_idx}")
                role = escape_for_cypher(us.get('role', ''))
                goal = escape_for_cypher(us.get('goal', ''))
                benefit = escape_for_cypher(us.get('benefit', ''))
                
                # User Story 노드 생성 및 관계
                us_query = (
                    f"MATCH (c:{info.kind} {{startLine: {info.node_start}, {self.node_base_props}}})\n"
                    f"MERGE (us:UserStory {{id: '{us_id}', class_name: '{class_name_escaped}', {self.node_base_props}}})\n"
                    f"SET us.role = '{role}',\n"
                    f"    us.goal = '{goal}',\n"
                    f"    us.benefit = '{benefit}'\n"
                    f"MERGE (c)-[r:HAS_USER_STORY]->(us)\n"
                    f"RETURN c, us, r"
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
        
        await self._send_queries(queries, info.node_end)
        class_node.summary = str(final_summary)
        class_node.completion_event.set()
        
        # User Story 개수 로깅
        us_count = len(all_user_stories) if all_user_stories else 0
        log_process("ANALYZE", "SUMMARY", f"✅ 클래스 요약 + User Story({us_count}개) 완료: {info.name}")

    async def _finalize_remaining_classes(self):
        """남은 클래스 요약을 처리합니다."""
        for key, info in list(self.classes.items()):
            if info.pending_nodes == 0 and key not in self._finalized_classes:
                await self._finalize_class_summary(info)

    async def _send_queries(
        self,
        queries: List[str],
        progress_line: int,
        analysis_info: Optional[Dict[str, Any]] = None
    ):
        """쿼리를 전송하고 완료를 대기합니다."""
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
            resp = await self.receive_queue.get()
            if resp.get("type") == "process_completed":
                break
        log_process("ANALYZE", "APPLY", f"✅ Neo4j 반영 완료 (라인 {progress_line})")


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
        # 레거시 호환성 (이벤트 기반 모드용 - 선택적)
        send_queue: Optional[asyncio.Queue] = None,
        receive_queue: Optional[asyncio.Queue] = None,
    ):
        self.antlr_data = antlr_data
        self.file_content = file_content
        self.send_queue = send_queue
        self.receive_queue = receive_queue
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
        
        # 정적 노드 쿼리 생성
        queries: List[str] = []
        for node in self._nodes:
            queries.extend(self._build_static_node_queries(node))
        
        # 관계 쿼리 생성 (HAS_METHOD, HAS_FIELD, CONTAINS 등)
        queries.extend(self._build_relationship_queries())
        
        log_process("ANALYZE", "PHASE1", f"✅ {self.full_directory}: {len(queries)}개 쿼리 생성")
        return queries

    async def run_llm_analysis(self) -> List[str]:
        """[Phase 2] LLM 분석을 실행하고 업데이트 쿼리를 생성합니다.
        
        Returns:
            분석 결과 업데이트 쿼리 리스트 (MATCH 기반)
        """
        if self._nodes is None:
            raise AnalysisError(f"Phase 1이 먼저 실행되어야 합니다: {self.file_name}")
        
        log_process("ANALYZE", "PHASE2", f"🤖 {self.full_directory} LLM 분석 시작")
        
        all_queries: List[str] = []
        
        # 선행 처리: 상속/구현 + 필드 + 메서드
        preprocessing_queries = await self._run_preprocessing()
        all_queries.extend(preprocessing_queries)
        
        # 배치 분석
        planner = BatchPlanner()
        batches = planner.plan(self._nodes)
        
        if not batches:
            log_process("ANALYZE", "PHASE2", f"⚠️ {self.full_directory}: 분석 대상 배치 없음")
            return all_queries
        
        # LLM 호출 및 결과 처리
        invoker = LLMInvoker(self.api_key, self.locale)
        semaphore = asyncio.Semaphore(min(self.max_workers, len(batches)))
        
        async def process_batch(batch: AnalysisBatch) -> List[str]:
            async with semaphore:
                log_process("ANALYZE", "LLM", f"배치 #{batch.batch_id} 처리 중 ({len(batch.nodes)}개 노드)")
                result = await invoker.invoke(batch)
                return self._build_analysis_queries(batch, result)
        
        # 병렬 처리
        batch_results = await asyncio.gather(
            *[process_batch(b) for b in batches],
            return_exceptions=True
        )
        
        for i, result in enumerate(batch_results):
            if isinstance(result, Exception):
                log_process("ANALYZE", "ERROR", f"배치 #{i+1} 처리 실패: {result}", logging.ERROR)
            else:
                all_queries.extend(result)
        
        log_process("ANALYZE", "PHASE2", f"✅ {self.full_directory}: {len(all_queries)}개 업데이트 쿼리")
        return all_queries

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
                    continue
                source_member = dep.get("sourceMember", "unknown") if isinstance(dep, dict) else "unknown"
                
                if node.class_kind and node.parent:
                    queries.append(
                        f"MATCH (src:{node.class_kind} {{startLine: {node.parent.start_line}, {self.node_base_props}}})\n"
                        f"MATCH (dst) WHERE (dst:CLASS OR dst:INTERFACE OR dst:ENUM)\n"
                        f"  AND toLower(dst.class_name) = toLower('{escape_for_cypher(dep_type)}')\n"
                        f"  AND dst.user_id = '{self.user_id}' AND dst.project_name = '{self.project_name}'\n"
                        f"  AND src <> dst AND NOT (src)-[:ASSOCIATION|COMPOSITION]->(dst)\n"
                        f"MERGE (src)-[r:DEPENDENCY {{usage: 'local', source_member: '{escape_for_cypher(source_member)}'}}]->(dst)\n"
                        f"RETURN r"
                    )
            
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

    def _build_relationship_queries(self) -> List[str]:
        """정적 관계 쿼리 (HAS_METHOD, HAS_FIELD, CONTAINS)를 생성합니다."""
        queries: List[str] = []
        
        for node in self._nodes or []:
            if not node.parent:
                continue
            
            # 부모-자식 관계 생성
            parent = node.parent
            if node.node_type in METHOD_TYPES:
                rel_type = "HAS_METHOD"
            elif node.node_type in FIELD_TYPES:
                rel_type = "HAS_FIELD"
            else:
                rel_type = "CONTAINS"
            
            queries.append(
                f"MATCH (p:{parent.node_type} {{startLine: {parent.start_line}, {self.node_base_props}}})\n"
                f"MATCH (c:{node.node_type} {{startLine: {node.start_line}, {self.node_base_props}}})\n"
                f"MERGE (p)-[r:{rel_type}]->(c)\n"
                f"RETURN r"
            )
        
        return queries

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

    # ===== 레거시 호환성 메서드 (이벤트 기반 모드) =====
    async def run(self):
        """파일 단위 분석 파이프라인을 실행합니다 (레거시 이벤트 기반 모드)."""
        log_process("ANALYZE", "START", f"🚀 {self.full_directory} 분석 시작 (총 {self.last_line}줄)")
        try:
            # 1. AST 수집
            collector = StatementCollector(self.antlr_data, self.file_content, self.directory, self.file_name)
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
                classes=classes,
                api_key=self.api_key,
                locale=self.locale,
                user_id=self.user_id,
                project_name=self.project_name,
                directory=self.directory,
                file_name=self.file_name,
            )

            semaphore = asyncio.Semaphore(min(self.max_workers, len(batches)))

            async def worker(batch: AnalysisBatch):
                await self._wait_for_dependencies(batch)
                async with semaphore:
                    log_process(
                        "ANALYZE",
                        "LLM",
                        f"🤖 배치 #{batch.batch_id} LLM 요청: 노드 {len(batch.nodes)}개 ({self.full_directory})",
                    )
                    analysis_result = await invoker.invoke(batch)
                await apply_manager.submit(batch, analysis_result)

            await asyncio.gather(*(worker(b) for b in batches))
            await apply_manager.finalize()

            log_process("ANALYZE", "DONE", f"✅ {self.full_directory} 분석 완료")
            await self.send_queue.put({"type": "end_analysis"})

        except (AnalysisError, LLMCallError) as exc:
            log_process("ANALYZE", "ERROR", f"❌ 분석 파이프라인 예외: {exc}", logging.ERROR, exc)
            await self.send_queue.put({"type": "error", "message": str(exc)})
            raise
        except Exception as exc:
            err_msg = f"분석 과정에서 예기치 못한 오류 발생: {exc}"
            log_process("ANALYZE", "ERROR", f"❌ {err_msg}", logging.ERROR, exc)
            await self.send_queue.put({"type": "error", "message": err_msg})
            raise CodeProcessError(err_msg) from exc

    async def _wait_for_dependencies(self, batch: AnalysisBatch):
        """부모 노드 분석 전 자식 완료 대기."""
        waiters = []
        for n in batch.nodes:
            for ch in n.children:
                if ch.analyzable:
                    waiters.append(ch.completion_event.wait())
        if waiters:
            log_process(
                "ANALYZE",
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
        current_batch_nodes: List[StatementNode] = []
        
        for node in nodes:
            queries.extend(self._build_static_node_queries(node))
            current_batch_nodes.append(node)
            
            if len(queries) >= STATIC_QUERY_BATCH_SIZE:
                node_info = self._build_batch_node_info(current_batch_nodes)
                await self._send_static_queries(queries, node.end_line, node_info)
                queries.clear()
                current_batch_nodes.clear()
                
        if queries:
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
        
        # 첫 번째 의미 있는 노드 정보 (CLASS, INTERFACE, METHOD 등)
        first_node = nodes[0]
        for node in nodes:
            if node.node_type in CLASS_TYPES or node.class_name:
                first_node = node
                break
        
        return {
            "type": first_node.node_type,
            "name": first_node.class_name or f"Line {first_node.start_line}",
            "start_line": first_node.start_line,
            "node_count": len(nodes),
            "type_summary": type_counts,
        }

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
        """부모와 자식 노드 사이의 PARENT_OF 관계 쿼리를 작성합니다 (DBMS 스타일과 동일)."""
        parent_match = f"MATCH (parent:{parent.node_type} {{startLine: {parent.start_line}, {self.node_base_props}}})"
        child_match = f"MATCH (child:{child.node_type} {{startLine: {child.start_line}, {self.node_base_props}}})"
        return f"{parent_match}\n{child_match}\nMERGE (parent)-[r:PARENT_OF]->(child)\nRETURN parent, child, r"

    def _build_next_relationship_query(self, prev_node: StatementNode, current_node: StatementNode) -> str:
        """형제 노드 사이의 NEXT 관계 쿼리를 작성합니다 (DBMS 스타일과 동일)."""
        prev_match = f"MATCH (prev:{prev_node.node_type} {{startLine: {prev_node.start_line}, {self.node_base_props}}})"
        curr_match = f"MATCH (current:{current_node.node_type} {{startLine: {current_node.start_line}, {self.node_base_props}}})"
        return f"{prev_match}\n{curr_match}\nMERGE (prev)-[r:NEXT]->(current)\nRETURN prev, current, r"

    async def _send_static_queries(
        self,
        queries: List[str],
        progress_line: int,
        node_info: Optional[Dict[str, Any]] = None
    ):
        """정적 그래프 쿼리 전송."""
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
            resp = await self.receive_queue.get()
            if resp.get("type") == "process_completed":
                break

    # ===== 선행 처리: 상속/구현 + 필드 + 메서드 (병렬) =====
    async def _process_preprocessing(self, nodes: List[StatementNode]):
        """상속/구현, 필드, 메서드 노드를 병렬로 처리합니다."""
        inheritance_nodes = [n for n in nodes if n.node_type in INHERITANCE_TYPES]
        field_nodes = [n for n in nodes if n.node_type in FIELD_TYPES]
        method_nodes = [n for n in nodes if n.node_type in METHOD_TYPES]

        log_process("ANALYZE", "PREPROCESS", f"🔍 선행 처리 시작: 상속/구현 {len(inheritance_nodes)}개, 필드 {len(field_nodes)}개, 메서드 {len(method_nodes)}개")

        # 1단계: 상속/구현 + 필드 병렬 처리 (ASSOCIATION 생성)
        await asyncio.gather(
            self._process_inheritance_nodes(inheritance_nodes),
            self._process_field_nodes(field_nodes, nodes),
        )

        # 2단계: 메서드 처리 (ASSOCIATION → COMPOSITION 변경)
        await self._process_method_nodes(method_nodes)

        log_process("ANALYZE", "PREPROCESS", f"✅ 선행 처리 완료")

    async def _process_inheritance_nodes(self, nodes: List[StatementNode]):
        """상속/구현 노드를 병렬로 분석합니다."""
        if not nodes:
            return

        log_process("ANALYZE", "INHERITANCE", f"🔍 상속/구현 관계 분석 시작: {len(nodes)}개 노드")
        semaphore = asyncio.Semaphore(INHERITANCE_CONCURRENCY)

        async def worker(node: StatementNode):
            async with semaphore:
                try:
                    result = await asyncio.to_thread(
                        analyze_inheritance,
                        node.get_raw_code(),
                        self.api_key,
                        self.locale,
                    )
                except Exception as exc:
                    log_process("ANALYZE", "INHERITANCE", f"❌ 상속/구현 분석 오류: 라인 {node.start_line}", logging.ERROR, exc)
                    raise

                queries = self._build_inheritance_queries(node, result)
                if queries:
                    await self._send_static_queries(queries, node.end_line)

        await asyncio.gather(*(worker(n) for n in nodes))
        log_process("ANALYZE", "INHERITANCE", f"✅ 상속/구현 관계 분석 완료")

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

    async def _process_field_nodes(self, field_nodes: List[StatementNode], all_nodes: List[StatementNode]):
        """필드 노드를 병렬로 분석합니다."""
        if not field_nodes:
            return

        log_process("ANALYZE", "FIELD", f"🔍 필드 정보 분석 시작: {len(field_nodes)}개 노드")
        semaphore = asyncio.Semaphore(FIELD_CONCURRENCY)

        async def worker(node: StatementNode):
            async with semaphore:
                try:
                    result = await asyncio.to_thread(
                        analyze_field,
                        node.get_raw_code(),
                        self.api_key,
                        self.locale,
                    )
                except Exception as exc:
                    log_process("ANALYZE", "FIELD", f"❌ 필드 분석 오류: 라인 {node.start_line}", logging.ERROR, exc)
                    raise

                queries = self._build_field_queries(node, result)
                if queries:
                    await self._send_static_queries(queries, node.end_line)

        await asyncio.gather(*(worker(n) for n in field_nodes))
        log_process("ANALYZE", "FIELD", f"✅ 필드 정보 분석 완료")

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

    async def _process_method_nodes(self, method_nodes: List[StatementNode]):
        """메서드 노드를 병렬로 분석합니다 - 파라미터/반환 타입 추출."""
        if not method_nodes:
            return

        log_process("ANALYZE", "METHOD", f"🔍 메서드 시그니처 분석 시작: {len(method_nodes)}개 노드")
        semaphore = asyncio.Semaphore(METHOD_CONCURRENCY)

        async def worker(node: StatementNode):
            async with semaphore:
                try:
                    # 메서드 시그니처 + ASSIGNMENT 구문만 포함된 코드 전달
                    code_for_analysis = node.get_code_with_assigns_only() if node.has_children else node.get_raw_code()
                    result = await asyncio.to_thread(
                        analyze_method,
                        code_for_analysis,
                        self.api_key,
                        self.locale,
                    )
                except Exception as exc:
                    log_process("ANALYZE", "METHOD", f"❌ 메서드 분석 오류: 라인 {node.start_line}", logging.ERROR, exc)
                    raise

                queries = self._build_method_queries(node, result)
                if queries:
                    await self._send_static_queries(queries, node.end_line)

        await asyncio.gather(*(worker(n) for n in method_nodes))
        log_process("ANALYZE", "METHOD", f"✅ 메서드 시그니처 분석 완료")

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
