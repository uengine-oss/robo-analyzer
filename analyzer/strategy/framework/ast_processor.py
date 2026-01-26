"""Framework 코드 분석기 - Java/Kotlin AST → Neo4j 그래프

클래스 다이어그램 생성에 필요한 정보를 추출합니다.

분석 파이프라인:
1. AST 수집 (StatementCollector)
2. 정적 그래프 생성 (CLASS, METHOD, FIELD 노드)
3. 상속/구현 관계 추출 (EXTENDS, IMPLEMENTS)
4. LLM 배치 분석 (요약, 메서드 콜 추출)
5. 클래스 요약 및 User Story 생성

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
from util.text_utils import calculate_code_token, escape_for_cypher, log_process

from analyzer.strategy.base.statement_node import StatementNode
from analyzer.strategy.base.batch import AnalysisBatch
from analyzer.strategy.base.processor import BaseAstProcessor


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
UTILITY_CLASS_PATTERNS = frozenset([
    "Debug", "Logger", "Log", "LogFactory", "LogManager",
    "Utils", "Utility", "Utilities", "Helper", "Helpers",
    "Constants", "Config", "Configuration", "Settings",
    "Validator", "Validation", "Formatter", "Converter",
    "StringUtils", "DateUtils", "NumberUtils", "CollectionUtils",
    "Assert", "Assertions", "Preconditions", "Check",
])


# ==================== 데이터 클래스 ====================
class ClassInfo:
    """클래스/인터페이스 정보"""
    __slots__ = ('key', 'name', 'kind', 'node_start', 'node_end', 'pending_nodes', 'finalized')
    
    def __init__(
        self,
        key: str,
        name: str,
        kind: str,
        node_start: int,
        node_end: int,
        pending_nodes: int = 0,
        finalized: bool = False,
    ):
        self.key = key
        self.name = name
        self.kind = kind
        self.node_start = node_start
        self.node_end = node_end
        self.pending_nodes = pending_nodes
        self.finalized = finalized


# ==================== 헬퍼 함수 ====================
def _is_valid_class_name_for_calls(name: str) -> bool:
    """calls 관계 생성에 유효한 클래스명인지 검증."""
    if not name:
        return False
    if name in JAVA_BUILTIN_TYPES:
        return False
    if name in UTILITY_CLASS_PATTERNS:
        return False
    if len(name) == 1:
        return False
    if name[0].islower() and len(name) <= 3:
        return False
    if name.islower() and len(name) <= 6:
        return False
    return True


# ==================== RuleLoader 헬퍼 ====================
def _rule_loader() -> RuleLoader:
    return RuleLoader(target_lang="framework")


def analyze_code(code: str, context: str, ranges: list, count: int, api_key: str, locale: str) -> Dict[str, Any]:
    """코드 범위별 분석"""
    inputs = {"code": code, "ranges": ranges, "count": count, "locale": locale}
    if context.strip():
        inputs["context"] = context
    return _rule_loader().execute(
        "analysis",
        inputs,
        api_key,
    )


def analyze_class_summary_only(summaries: dict, api_key: str, locale: str, previous_summary: str = "") -> Dict[str, Any]:
    """클래스 전체 요약 생성 (Summary만)."""
    return _rule_loader().execute(
        "class_summary_only",
        {"summaries": summaries, "locale": locale, "previous_summary": previous_summary},
        api_key,
    )


def analyze_class_user_story(summary: str, api_key: str, locale: str) -> Dict[str, Any]:
    """클래스 User Story + AC 생성."""
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
    """메서드 시그니처 분석."""
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
    """AST를 후위순회하여 StatementNode와 클래스 정보를 수집합니다.
    
    file_content는 더 이상 필요하지 않음 - AST JSON의 code 속성 사용.
    """

    def __init__(self, antlr_data: Dict[str, Any], directory: str, file_name: str):
        self.antlr_data = antlr_data
        self.directory = directory
        self.file_name = file_name
        self.nodes: List[StatementNode] = []
        self.classes: Dict[str, ClassInfo] = {}
        self._node_id = 0

    def _parse_code_to_lines(self, code: str, start_line: int, end_line: int) -> List[Tuple[int, str]]:
        """JSON code 속성을 [(line_no, text), ...] 형태로 파싱합니다.
        
        Args:
            code: '1: public class...\n2: ...' 또는 '1: public class...\r\n2: ...' 형태의 문자열
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

        # AST JSON의 code 속성에서 라인 정보 추출
        raw_code = node.get('code', '')
        line_entries = self._parse_code_to_lines(raw_code, start_line, end_line)
        code = "\n".join(f"{ln}: {txt}" for ln, txt in line_entries)

        class_key = current_class
        class_name = current_class_name
        class_kind = current_class_kind

        # 클래스/인터페이스 노드 처리
        if node_type in CLASS_TYPES:
            # JSON에서 name 직접 추출 (정규식 추출보다 정확)
            name_from_json = node.get('name')
            if name_from_json:
                extracted_name = name_from_json
            else:
                # fallback: 기존 정규식 추출 (deprecated)
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

        # 분석 가능 여부 판단
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
            # 통합 필드
            unit_key=class_key,
            unit_name=class_name,
            unit_kind=class_kind,
            # AST JSON 메타데이터 (선택적)
            signature=node.get('signature'),
            modifiers=node.get('modifiers'),
            return_type=node.get('returnType'),
            parameters=node.get('parameters'),
            generic_type=node.get('genericType'),
            extends_type=node.get('extendsType'),
            implements_types=node.get('implementsTypes'),
            field_type=node.get('fieldType'),
            lines=line_entries,
        )
        for c in child_nodes:
            c.parent = st
        st.children.extend(child_nodes)

        # 분석 대상 노드 카운트
        # analyzable=True인 노드는 배치에 포함되므로, completion_event는 배치 완료 시에만 설정
        # analyzable=False인 노드는 배치에 포함되지 않으므로, 수집 시 처리
        if not analyzable and node_type not in CLASS_TYPES:
            # 배치에 포함되지 않는 노드는 수집 시 summary + completion_event 설정
            st.summary = st.get_raw_code()
            st.completion_event.set()
        elif analyzable and class_key and class_key in self.classes:
            # 클래스에 속한 분석 대상 노드
            self.classes[class_key].pending_nodes += 1
        # else: analyzable=True이지만 class_key 없음
        # → 배치에서 LLM 분석 후 completion_event 설정됨

        self.nodes.append(st)
        log_process(
            "ANALYZE",
            "COLLECT",
            f"✅ {node_type} 노드 수집 완료: 라인 {start_line}~{end_line}, 토큰 {token}, 자식 {len(child_nodes)}개",
        )
        return st


# ==================== AST 프로세서 본체 ====================
class FrameworkAstProcessor(BaseAstProcessor):
    """Framework AST 처리 및 LLM 분석 파이프라인
    
    BaseAstProcessor를 상속하여 공통 파이프라인 재사용.
    Framework 전용 로직만 구현.
    """

    def __init__(
        self,
        antlr_data: dict,
        directory: str,
        file_name: str,
        api_key: str,
        locale: str,
        last_line: int,
    ):
        """Framework Analyzer 초기화
        
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
        
        # 필드 타입 캐시
        self._field_type_cache: Optional[Dict[str, Dict[str, str]]] = None

    # =========================================================================
    # BaseAstProcessor 추상 메서드 구현
    # =========================================================================
    
    def _collect_nodes(self) -> Tuple[List[StatementNode], Dict[str, ClassInfo]]:
        """AST 수집"""
        collector = StatementCollector(
            self.antlr_data, self.directory, self.file_name
        )
        nodes, classes = collector.collect()
        
        # 필드 타입 캐시 초기화
        self._field_type_cache = {key: {} for key in classes} if classes else {}
        
        return nodes, classes

    def _get_excluded_context_types(self) -> Set[str]:
        """컨텍스트 생성에서 제외할 노드 타입"""
        return CLASS_TYPES

    async def _extract_parent_context(self, skeleton_code: str, ancestor_context: str) -> str:
        """부모 컨텍스트 추출"""
        result = await asyncio.to_thread(
            analyze_parent_context, skeleton_code, ancestor_context, self.api_key, self.locale
        )
        if isinstance(result, dict):
            return result.get("context_summary", "")
        raise ValueError(f"parent_context 규칙이 dict가 아닌 값을 반환했습니다: {type(result)}")

    def _build_static_node_queries(self, node: StatementNode) -> List[str]:
        """정적 노드 생성 쿼리 리스트를 반환합니다."""
        queries: List[str] = []
        label = node.node_type
        
        # name 속성 결정
        if label == "FILE":
            node_name = self.file_name
        elif label in CLASS_TYPES and node.unit_name:
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
        if node.modifiers:
            base_set.append(f"__cy_n__.modifiers = '{escape_for_cypher(node.modifiers)}'")
        if node.return_type:
            base_set.append(f"__cy_n__.returnType = '{escape_for_cypher(node.return_type)}'")
        if node.parameters:
            base_set.append(f"__cy_n__.parameters = '{escape_for_cypher(node.parameters)}'")
        if node.generic_type:
            base_set.append(f"__cy_n__.genericType = '{escape_for_cypher(node.generic_type)}'")
        if node.extends_type:
            base_set.append(f"__cy_n__.extendsType = '{escape_for_cypher(node.extends_type)}'")
        if node.implements_types:
            base_set.append(f"__cy_n__.implementsTypes = '{escape_for_cypher(node.implements_types)}'")
        if node.field_type:
            base_set.append(f"__cy_n__.fieldType = '{escape_for_cypher(node.field_type)}'")

        # CLASS/INTERFACE 등: class_name과 type 속성 추가
        if label in CLASS_TYPES and node.unit_name:
            base_set.append(f"__cy_n__.class_name = '{escape_for_cypher(node.unit_name)}'")
            base_set.append(f"__cy_n__.type = '{label}'")
        elif node.unit_name:
            base_set.append(f"__cy_n__.class_name = '{escape_for_cypher(node.unit_name)}'")

        if node.has_children:
            # Framework용 preserve_types 설정
            preserve_types = INHERITANCE_TYPES | METHOD_TYPES | METHOD_SIGNATURE_TYPES
            escaped_placeholder = escape_for_cypher(node.get_placeholder_code(preserve_types))
            base_set.append(f"__cy_n__.summarized_code = '{escaped_placeholder}'")

        base_set_str = ", ".join(base_set)
        
        # CLASS/INTERFACE/ENUM 노드: MERGE로 생성 (중복 방지)
        if label in ("CLASS", "INTERFACE", "ENUM") and node.unit_name:
            escaped_class_name = escape_for_cypher(node.unit_name)
            queries.append(
                f"MERGE (__cy_n__:{label} {{class_name: '{escaped_class_name}'}})\n"
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
        """정적 관계 쿼리 (HAS_METHOD, HAS_FIELD, CONTAINS, PARENT_OF)를 생성합니다."""
        queries: List[str] = []
        
        for node in self._nodes or []:
            if not node.parent:
                continue
            
            parent = node.parent
            
            # File → 최상위 타입만 CONTAINS
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
    
    def _build_has_method_query(self, parent: StatementNode, child: StatementNode) -> str:
        """HAS_METHOD 관계 쿼리"""
        return (
            f"MATCH (__cy_p__:{parent.node_type} {{startLine: {parent.start_line}, {self.node_base_props}}})\n"
            f"MATCH (__cy_c__:{child.node_type} {{startLine: {child.start_line}, {self.node_base_props}}})\n"
            f"MERGE (__cy_p__)-[__cy_r__:HAS_METHOD]->(__cy_c__)\n"
            f"RETURN __cy_r__"
        )
    
    def _build_has_field_query(self, parent: StatementNode, child: StatementNode) -> str:
        """HAS_FIELD 관계 쿼리"""
        return (
            f"MATCH (__cy_p__:{parent.node_type} {{startLine: {parent.start_line}, {self.node_base_props}}})\n"
            f"MATCH (__cy_c__:{child.node_type} {{startLine: {child.start_line}, {self.node_base_props}}})\n"
            f"MERGE (__cy_p__)-[__cy_r__:HAS_FIELD]->(__cy_c__)\n"
            f"RETURN __cy_r__"
        )
    
    async def _run_preprocessing(self) -> List[str]:
        """선행 처리: 상속/구현, 필드, 메서드 분석"""
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
                    # 선행 처리 실패 시 즉시 중단
                    raise RuntimeError(f"선행 처리 실패: {result}") from result
        
        return queries

    async def _invoke_llm(self, batch: AnalysisBatch) -> Optional[Dict[str, Any]]:
        """LLM 호출 (일반 분석만)"""
        if not batch.ranges:
            raise RuntimeError(f"배치 #{batch.batch_id}에 분석할 범위가 없습니다")

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

    def _build_analysis_queries(
        self, 
        batch: AnalysisBatch, 
        llm_result: Any,
        unit_summary_store: Optional[Dict[str, Dict[str, str]]] = None,
    ) -> List[str]:
        """LLM 분석 결과를 쿼리로 변환"""
        queries: List[str] = []
        
        # 타입 검증 (실패 시 예외 발생 → 전체 분석 중단)
        llm_result = self.validate_dict_result(llm_result, "llm_result", batch.batch_id)
        analysis_list = llm_result.get("analysis") or []
        
        for node, analysis in zip(batch.nodes, analysis_list):
            if not analysis:
                continue
            
            # 요약 업데이트
            summary = analysis.get("summary") or ""
            if summary:
                escaped_summary = escape_for_cypher(str(summary))
                queries.append(
                    f"MATCH (__cy_n__:{node.node_type} {{startLine: {node.start_line}, {self.node_base_props}}})\n"
                    f"SET __cy_n__.summary = '{escaped_summary}'\n"
                    f"RETURN __cy_n__"
                )
                
                # 클래스별 summary 저장
                if unit_summary_store is not None and node.unit_key:
                    if node.unit_key in unit_summary_store:
                        key = f"{node.node_type}_{node.start_line}_{node.end_line}"
                        unit_summary_store[node.unit_key][key] = summary
            
            # DEPENDENCY 관계 (localDependencies)
            for dep in analysis.get("localDependencies", []) or []:
                if not dep:
                    continue
                dep_type = dep.get("type", "") if isinstance(dep, dict) else str(dep)
                if not dep_type or not _is_valid_class_name_for_calls(dep_type):
                    continue
                source_member = dep.get("sourceMember", "unknown") if isinstance(dep, dict) else "unknown"
                
                if not node.unit_kind or not node.parent:
                    continue
                
                queries.append(
                    f"MATCH (__cy_src__:{node.unit_kind} {{startLine: {node.parent.start_line}, {self.node_base_props}}})\n"
                    f"MATCH (__cy_dst__) WHERE (__cy_dst__:CLASS OR __cy_dst__:INTERFACE OR __cy_dst__:ENUM)\n"
                    f"  AND toLower(__cy_dst__.class_name) = toLower('{escape_for_cypher(dep_type)}')\n"
                    f"  AND __cy_src__ <> __cy_dst__ AND NOT (__cy_src__)-[:ASSOCIATION|COMPOSITION]->(__cy_dst__)\n"
                    f"MERGE (__cy_src__)-[__cy_r__:DEPENDENCY {{usage: 'local', source_member: '{escape_for_cypher(source_member)}'}}]->(__cy_dst__)\n"
                    f"RETURN __cy_r__"
                )
            
            # CALLS 관계
            for call_str in analysis.get("calls", []) or []:
                if not call_str or not isinstance(call_str, str):
                    continue
                parts = call_str.split(".", 1)
                if len(parts) != 2:
                    continue
                target_class, method_name = parts
                
                if not _is_valid_class_name_for_calls(target_class):
                    continue
                
                if node.unit_kind and node.parent:
                    queries.append(
                        f"MATCH (__cy_src__:{node.unit_kind} {{startLine: {node.parent.start_line}, {self.node_base_props}}})\n"
                        f"MATCH (__cy_dst__) WHERE (__cy_dst__:CLASS OR __cy_dst__:INTERFACE OR __cy_dst__:ENUM)\n"
                        f"  AND toLower(__cy_dst__.class_name) = toLower('{escape_for_cypher(target_class)}')\n"
                        f"MERGE (__cy_src__)-[__cy_r__:CALLS {{method: '{escape_for_cypher(method_name)}'}}]->(__cy_dst__)\n"
                        f"RETURN __cy_r__"
                    )
        
        return queries

    async def _process_unit_summaries(
        self, 
        unit_summary_store: Dict[str, Dict[str, str]]
    ) -> List[str]:
        """클래스별 summary 처리"""
        queries: List[str] = []
        
        classes = self._unit_info
        if not classes:
            return queries
        
        for class_key, info in classes.items():
            summaries = unit_summary_store.get(class_key, {})
            if not summaries:
                continue
            
            # 클래스 노드 찾기
            class_node = next(
                (n for n in self._nodes if n.start_line == info.node_start and n.node_type == info.kind),
                None,
            )
            if not class_node:
                continue
            
            if not class_node.ok:
                log_process("ANALYZE", "SUMMARY", f"⚠️ {info.name}: 하위 분석 실패로 최종 summary 생성 스킵")
                continue
            
            all_user_stories: List[Dict[str, Any]] = []
            final_summary = ""
            
            # 청크 분할
            chunks = self._split_summaries_by_token(summaries, MAX_SUMMARY_CHUNK_TOKEN)
            if not chunks:
                continue
            
            log_process("ANALYZE", "SUMMARY", f"📦 {info.name}: summary 청크 분할 완료 ({len(chunks)}개 청크)")
            
            # 청크별 처리 (실패 시 예외 발생 → 전체 분석 중단)
            async def process_chunk(chunk_idx: int, chunk: dict) -> str:
                chunk_tokens = calculate_code_token(json.dumps(chunk, ensure_ascii=False))
                log_process("ANALYZE", "SUMMARY", f"  → 청크 {chunk_idx + 1}/{len(chunks)} 처리 시작 (토큰: {chunk_tokens})")
                
                summary_result = await asyncio.to_thread(
                    analyze_class_summary_only, chunk, self.api_key, self.locale, ""
                )
                validated = self.validate_dict_result(summary_result, "청크 분석")
                return validated.get('summary', '')
            
            chunk_results_raw = await asyncio.gather(
                *[process_chunk(idx, chunk) for idx, chunk in enumerate(chunks)]
            )
            
            chunk_results = [r for r in chunk_results_raw if r]
            
            if not chunk_results:
                raise RuntimeError(f"{info.name}: 청크 처리 결과가 모두 비어있음")
            
            # 청크 통합
            if len(chunk_results) == 1:
                final_summary = chunk_results[0]
            else:
                combined_summaries = {f"CHUNK_{idx + 1}": s for idx, s in enumerate(chunk_results)}
                result = await asyncio.to_thread(
                    analyze_class_summary_only, combined_summaries, self.api_key, self.locale, ""
                )
                validated = self.validate_dict_result(result, "청크 통합")
                final_summary = validated.get('summary') or "\n\n".join(chunk_results)
            
            log_process("ANALYZE", "SUMMARY", f"✅ {info.name}: summary 통합 완료")
            
            # User Story 생성 (실패 시 예외 발생)
            if final_summary:
                us_result = await asyncio.to_thread(
                    analyze_class_user_story, final_summary, self.api_key, self.locale
                )
                validated = self.validate_dict_result(us_result, "User Story")
                all_user_stories = validated.get('user_stories', []) or []
            
            if all_user_stories:
                log_process("ANALYZE", "SUMMARY", f"✅ {info.name}: User Story {len(all_user_stories)}개")
            else:
                log_process("ANALYZE", "SUMMARY", f"✅ {info.name}: User Story 없음")
            
            if not final_summary:
                continue
            
            # Neo4j 쿼리 생성
            escaped_summary = escape_for_cypher(str(final_summary))
            
            queries.append(
                f"MATCH (__cy_n__:{info.kind} {{startLine: {info.node_start}, {self.node_base_props}}})\n"
                f"SET __cy_n__.summary = '{escaped_summary}'\n"
                f"RETURN __cy_n__"
            )
            
            # User Story 노드 생성
            if all_user_stories:
                class_name_escaped = escape_for_cypher(info.name)
                for us_idx, us in enumerate(all_user_stories, 1):
                    us_id = us.get('id', f"US-{us_idx}")
                    role = escape_for_cypher(us.get('role', ''))
                    goal = escape_for_cypher(us.get('goal', ''))
                    benefit = escape_for_cypher(us.get('benefit', ''))
                    
                    queries.append(
                        f"MATCH (__cy_c__:{info.kind} {{startLine: {info.node_start}, {self.node_base_props}}})\n"
                        f"MERGE (__cy_us__:UserStory {{id: '{escape_for_cypher(us_id)}', class_name: '{class_name_escaped}', {self.node_base_props}}})\n"
                        f"SET __cy_us__.role = '{role}', __cy_us__.goal = '{goal}', __cy_us__.benefit = '{benefit}'\n"
                        f"MERGE (__cy_c__)-[:HAS_USER_STORY]->(__cy_us__)\n"
                        f"RETURN __cy_us__"
                    )
                    
                    for ac_idx, ac in enumerate(us.get('acceptance_criteria', []) or [], 1):
                        ac_id = ac.get('id', f"AC-{us_idx}-{ac_idx}")
                        ac_title = escape_for_cypher(ac.get('title', ''))
                        ac_given = escape_for_cypher(ac.get('given', ''))
                        ac_when = escape_for_cypher(ac.get('when', ''))
                        ac_then = escape_for_cypher(ac.get('then', ''))
                        
                        queries.append(
                            f"MATCH (__cy_us__:UserStory {{id: '{escape_for_cypher(us_id)}', class_name: '{class_name_escaped}', {self.node_base_props}}})\n"
                            f"MERGE (__cy_ac__:AcceptanceCriteria {{id: '{escape_for_cypher(ac_id)}', user_story_id: '{escape_for_cypher(us_id)}', {self.node_base_props}}})\n"
                            f"SET __cy_ac__.title = '{ac_title}', __cy_ac__.given = '{ac_given}', __cy_ac__.when = '{ac_when}', __cy_ac__.then = '{ac_then}'\n"
                            f"MERGE (__cy_us__)-[:HAS_AC]->(__cy_ac__)\n"
                            f"RETURN __cy_ac__"
                        )
        
        return queries
    
    # =========================================================================
    # Framework 전용 메서드
    # =========================================================================

    async def _analyze_inheritance_nodes(self, nodes: List[StatementNode]) -> List[str]:
        """상속/구현 노드 분석"""
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
        """필드 노드 분석"""
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
        """메서드 시그니처 분석"""
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

    def _build_inheritance_queries(self, node: StatementNode, analysis: Dict[str, Any]) -> List[str]:
        """상속/구현 분석 결과를 쿼리로 변환"""
        if not isinstance(analysis, dict):
            raise RuntimeError(f"상속 분석 결과가 유효하지 않습니다 (node={node.start_line}): {type(analysis)}")

        queries: List[str] = []
        relations = analysis.get("relations") or []

        for rel in relations:
            to_type = escape_for_cypher(rel.get("toType") or "")
            rel_type = rel.get("relationType") or "EXTENDS"

            if not to_type:
                continue

            src_match = f"MATCH (__cy_src__:{node.unit_kind or 'CLASS'} {{startLine: {node.parent.start_line if node.parent else node.start_line}, {self.node_base_props}}})"

            queries.append(
                f"{src_match}\n"
                f"MATCH (__cy_dst__) WHERE (__cy_dst__:CLASS OR __cy_dst__:INTERFACE OR __cy_dst__:ENUM)\n"
                f"  AND toLower(__cy_dst__.class_name) = toLower('{to_type}')\n"
                f"MERGE (__cy_src__)-[__cy_r__:{rel_type}]->(__cy_dst__)\n"
                f"RETURN __cy_src__, __cy_dst__, __cy_r__"
            )

        return queries

    def _build_field_queries(self, node: StatementNode, analysis: Dict[str, Any]) -> List[str]:
        """필드 분석 결과를 쿼리로 변환"""
        if not isinstance(analysis, dict):
            raise RuntimeError(f"필드 분석 결과가 유효하지 않습니다 (node={node.start_line}): {type(analysis)}")

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

            # 필드 타입 캐시 업데이트
            if node.unit_key and self._field_type_cache and node.unit_key in self._field_type_cache:
                original_field_name = field_info.get("field_name") or ""
                self._field_type_cache[node.unit_key][original_field_name] = field_type_raw

            target_class_set = f", __cy_f__.target_class = '{target_class}'" if target_class else ""
            queries.append(
                f"MATCH (__cy_f__:FIELD {{startLine: {node.start_line}, {self.node_base_props}}})\n"
                f"SET __cy_f__.name = '{field_name}', __cy_f__.field_type = '{field_type}', "
                f"__cy_f__.visibility = '{visibility}', __cy_f__.is_static = {is_static}, __cy_f__.is_final = {is_final}{target_class_set}\n"
                f"RETURN __cy_f__"
            )

            if target_class:
                src_match = f"MATCH (__cy_src__:{node.unit_kind or 'CLASS'} {{startLine: {node.parent.start_line if node.parent else node.start_line}, {self.node_base_props}}})"
                queries.append(
                    f"{src_match}\n"
                    f"MATCH (__cy_dst__) WHERE (__cy_dst__:CLASS OR __cy_dst__:INTERFACE OR __cy_dst__:ENUM)\n"
                    f"  AND toLower(__cy_dst__.class_name) = toLower('{target_class}')\n"
                    f"MERGE (__cy_src__)-[__cy_r__:{association_type} {{source_member: '{field_name}', multiplicity: '{multiplicity}'}}]->(__cy_dst__)\n"
                    f"RETURN __cy_src__, __cy_dst__, __cy_r__"
                )

        return queries

    def _build_method_queries(self, node: StatementNode, analysis: Dict[str, Any]) -> List[str]:
        """메서드 분석 결과를 쿼리로 변환"""
        if not isinstance(analysis, dict):
            raise RuntimeError(f"메서드 분석 결과가 유효하지 않습니다 (node={node.start_line}): {type(analysis)}")

        queries: List[str] = []
        
        method_name = escape_for_cypher(analysis.get("method_name") or "")
        return_type = escape_for_cypher(analysis.get("return_type") or "void")
        visibility = escape_for_cypher(analysis.get("visibility") or "public")
        is_static = "true" if analysis.get("is_static") else "false"
        method_kind = escape_for_cypher(analysis.get("method_type") or "normal")
        parameters = analysis.get("parameters") or []
        dependencies = analysis.get("dependencies") or []

        queries.append(
            f"MATCH (__cy_m__:{node.node_type} {{startLine: {node.start_line}, {self.node_base_props}}})\n"
            f"SET __cy_m__.name = '{method_name}', __cy_m__.return_type = '{return_type}', "
            f"__cy_m__.visibility = '{visibility}', __cy_m__.is_static = {is_static}, "
            f"__cy_m__.method_type = '{method_kind}'\n"
            f"RETURN __cy_m__"
        )

        # 파라미터 노드
        for idx, param in enumerate(parameters):
            param_name = escape_for_cypher(param.get("name") or "")
            param_type = escape_for_cypher(param.get("type") or "")
            if not param_name:
                continue
            queries.append(
                f"MATCH (__cy_m__:{node.node_type} {{startLine: {node.start_line}, {self.node_base_props}}})\n"
                f"MERGE (__cy_p__:Parameter {{name: '{param_name}', method_start_line: {node.start_line}, {self.node_base_props}}})\n"
                f"SET __cy_p__.type = '{param_type}', __cy_p__.index = {idx}\n"
                f"MERGE (__cy_m__)-[__cy_r__:HAS_PARAMETER]->(__cy_p__)\n"
                f"RETURN __cy_m__, __cy_p__, __cy_r__"
            )

        # DEPENDENCY 관계
        for dep in dependencies:
            target_type = escape_for_cypher(dep.get("target_class") or "")
            usage = escape_for_cypher(dep.get("usage") or "parameter")
            is_value_object_cypher = "true" if dep.get("is_value_object") else "false"

            if not target_type:
                continue

            src_match = f"MATCH (__cy_src__:{node.unit_kind or 'CLASS'} {{startLine: {node.parent.start_line if node.parent else node.start_line}, {self.node_base_props}}})"
            queries.append(
                f"{src_match}\n"
                f"MATCH (__cy_dst__) WHERE (__cy_dst__:CLASS OR __cy_dst__:INTERFACE OR __cy_dst__:ENUM)\n"
                f"  AND toLower(__cy_dst__.class_name) = toLower('{target_type}')\n"
                f"  AND __cy_src__ <> __cy_dst__\n"
                f"  AND NOT (__cy_src__)-[:ASSOCIATION|COMPOSITION]->(__cy_dst__)\n"
                f"MERGE (__cy_src__)-[__cy_r__:DEPENDENCY {{usage: '{usage}', source_member: '{method_name}'}}]->(__cy_dst__)\n"
                f"SET __cy_r__.is_value_object = {is_value_object_cypher}\n"
                f"RETURN __cy_src__, __cy_dst__, __cy_r__"
            )

        # 필드 할당 패턴
        field_assignments = analysis.get("field_assignments") or []
        src_start_line = node.parent.start_line if node.parent else node.start_line
        for assign in field_assignments:
            field_name = escape_for_cypher(assign.get("field_name") or "")
            value_source = assign.get("value_source") or ""

            if not field_name or not value_source:
                continue

            if value_source == "new":
                queries.append(
                    f"MATCH (__cy_field__:FIELD {{name: '{field_name}', {self.node_base_props}}})\n"
                    f"WHERE __cy_field__.target_class IS NOT NULL\n"
                    f"MATCH (__cy_src__:{node.unit_kind or 'CLASS'} {{startLine: {src_start_line}, {self.node_base_props}}})"
                    f"-[__cy_r__:ASSOCIATION {{source_member: '{field_name}'}}]->(__cy_dst__)\n"
                    f"WITH __cy_src__, __cy_dst__, COALESCE(__cy_r__.multiplicity, '1') AS mult, __cy_r__\n"
                    f"DELETE __cy_r__\n"
                    f"MERGE (__cy_src__)-[__cy_r2__:COMPOSITION {{source_member: '{field_name}', multiplicity: mult}}]->(__cy_dst__)\n"
                    f"RETURN __cy_src__, __cy_dst__, __cy_r2__"
                )

        return queries
