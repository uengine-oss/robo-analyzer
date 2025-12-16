import asyncio
import logging
import os
import re
import textwrap
from collections import defaultdict, deque
from dataclasses import dataclass, field
from typing import Callable
from understand.neo4j_connection import Neo4jConnection
from util.exception import ConvertingError
from util.utility_tool import (
    build_rule_based_path, save_file, log_process
)
from util.rule_loader import RuleLoader
from convert.dbms.create_dbms_skeleton import start_dbms_skeleton


# ----- 상수 정의 -----
TOKEN_THRESHOLD = int(os.getenv('DBMS_TOKEN_THRESHOLD', '1000'))
CODE_PLACEHOLDER = "...code..."
DML_PLACEHOLDER_PATTERN = re.compile(
    r'(?P<indent>^[ \t]*)(?P<label>(?P<start>\d+)):\s*\.\.\.\s*code\s*\.\.\.',
    re.IGNORECASE | re.MULTILINE
)
DML_TYPES = frozenset(["SELECT", "INSERT", "UPDATE", "DELETE", "FETCH", "MERGE", "JOIN", "ALL_UNION", "UNION", "FOR"])
MAX_CONVERSION_CONCURRENCY = int(os.getenv('DBMS_MAX_CONCURRENCY', '5'))


@dataclass(slots=True)
class ChildFragment:
    sequence: int
    code: str
    start: int | None
    end: int | None


@dataclass(slots=True)
class ParentEntry:
    start: int
    end: int
    code: str
    is_dml: bool
    parent: "ParentEntry | None"
    sequence: int
    is_root: bool = False
    children: list[ChildFragment] = field(default_factory=list)
    pending_children: int = 0
    closed: bool = False
    finalized: bool = False


@dataclass(slots=True)
class ConversionWorkItem:
    work_id: int
    sequence: int
    code: str
    start: int
    end: int
    parent: ParentEntry | None
    parent_code: str
    token_count: int


class SpAccumulator:
    """리프/소형 노드 코드를 임계값까지 누적하는 버퍼."""

    __slots__ = ('parts', 'token_total', 'start_line', 'end_line')

    def __init__(self) -> None:
        self.clear()

    def clear(self) -> None:
        self.parts: list[str] = []
        self.token_total = 0
        self.start_line: int | None = None
        self.end_line: int | None = None

    def append(self, code: str, token: int, start_line: int, end_line: int) -> bool:
        snippet = (code or '').strip()
        if not snippet:
            return False
        self.parts.append(snippet)
        self.token_total += token
        if self.start_line is None or start_line < self.start_line:
            self.start_line = start_line
        if self.end_line is None or end_line > self.end_line:
            self.end_line = end_line
        return True

    def has_data(self) -> bool:
        return bool(self.parts)

    def should_flush_with(self, incoming_token: int | None, token_limit: int) -> bool:
        if not self.parts or incoming_token is None:
            return False
        return (self.token_total + incoming_token) >= token_limit

    def part_count(self) -> int:
        return len(self.parts)

    def consume(self) -> tuple[str, int | None, int | None, int, int]:
        if not self.parts:
            return "", None, None, 0, 0
        code = '\n'.join(self.parts)
        start = self.start_line
        end = self.end_line
        tokens = self.token_total
        part_count = len(self.parts)
        self.clear()
        return code, start, end, tokens, part_count


class ConversionWorkQueue:
    """LLM 호출을 병렬로 실행하기 위한 작업 큐."""

    __slots__ = ('rule_loader', 'api_key', 'locale', 'max_workers', 'items')

    def __init__(self, rule_loader: RuleLoader, api_key: str, locale: str, max_workers: int) -> None:
        self.rule_loader = rule_loader
        self.api_key = api_key
        self.locale = locale
        self.max_workers = max(1, max_workers)
        self.items: list[ConversionWorkItem] = []

    def reset(self) -> None:
        self.items.clear()

    def enqueue(self, item: ConversionWorkItem) -> None:
        self.items.append(item)

    def queued_count(self) -> int:
        return len(self.items)

    async def drain(self, completion_handler: Callable[[ConversionWorkItem, str], None]) -> None:
        if not self.items:
            return

        semaphore = asyncio.Semaphore(self.max_workers)

        async def worker(item: ConversionWorkItem) -> None:
            async with semaphore:
                result = await asyncio.to_thread(
                    self.rule_loader.execute,
                    role_name='dbms_conversion',
                    inputs={
                        'code': item.code,
                        'locale': self.locale,
                        'parent_code': item.parent_code
                    },
                    api_key=self.api_key
                )
            generated_code = (result.get('code') or '').strip()
            completion_handler(item, generated_code)

        await asyncio.gather(*(worker(item) for item in self.items))
        self.items.clear()


# ----- DBMS 변환 클래스 -----
class DbmsConversionGenerator:
    """
    DBMS 변환 전체 라이프사이클 관리
    - 단일 컨텍스트 누적 방식으로 타겟 DBMS 코드 생성
    - 대용량 부모(토큰≥1000, 자식 보유) 스켈레톤 관리
    - 토큰 임계 도달 시 LLM 분석 수행
    """
    __slots__ = (
        'traverse_nodes', 'system_name', 'file_name', 'procedure_name',
        'user_id', 'api_key', 'locale', 'project_name', 'target', 'skeleton_code',
        'merged_chunks', 'parent_stack',
        'rule_loader', 'sequence_counter',
        'work_id_counter', 'max_workers', 'root_entry',
        'sp_accumulator', 'work_queue'
    )

    def __init__(self, traverse_nodes: list, system_name: str, file_name: str,
                 procedure_name: str, user_id: str, api_key: str, locale: str, 
                 project_name: str = "demo", target: str = "oracle",
                 skeleton_code: str | None = None):
        self.traverse_nodes = traverse_nodes
        self.system_name = system_name
        self.file_name = file_name
        self.procedure_name = procedure_name
        self.user_id = user_id
        self.api_key = api_key
        self.locale = locale
        self.project_name = project_name or "demo"
        self.target = target
        self.skeleton_code = (skeleton_code or "").strip()

        # 상태 초기화
        self.merged_chunks = []
        self.parent_stack = []
        
        # Rule 파일 로더 (target으로 디렉토리 찾음)
        self.rule_loader = RuleLoader(target_lang=target)
        self.sequence_counter = 0
        self.work_id_counter = 0
        self.max_workers = MAX_CONVERSION_CONCURRENCY
        self.root_entry: ParentEntry | None = None
        self.sp_accumulator = SpAccumulator()
        self.work_queue = ConversionWorkQueue(self.rule_loader, self.api_key, self.locale, self.max_workers)

    # ----- 공개 메서드 -----

    @staticmethod
    def _resolve_node_type(node_labels: list | None, node: dict) -> str:
        raw_type = node_labels[0] if node_labels else node.get('name', 'UNKNOWN')
        raw_type = str(raw_type)
        return raw_type.split('[')[0] if '[' in raw_type else raw_type

    @staticmethod
    def _safe_int(value) -> int:
        try:
            return int(value)
        except (TypeError, ValueError):
            return 0

    def _ensure_root_entry(self) -> None:
        if self.root_entry is not None:
            return
        self.root_entry = ParentEntry(
            start=0,
            end=float('inf'),
            code="",
            is_dml=False,
            parent=None,
            sequence=0,
            is_root=True
        )

    def _reset_state(self) -> None:
        self._ensure_root_entry()
        if self.root_entry:
            self.root_entry.children.clear()
            self.root_entry.pending_children = 0
            self.root_entry.closed = False
            self.root_entry.finalized = False
        self.parent_stack.clear()
        self.sp_accumulator.clear()
        self.merged_chunks = []
        self.sequence_counter = 0
        self.work_id_counter = 0
        self.work_queue.reset()

    def _next_sequence(self) -> int:
        self.sequence_counter += 1
        return self.sequence_counter

    def _next_work_id(self) -> int:
        self.work_id_counter += 1
        return self.work_id_counter

    @staticmethod
    def _register_pending_child(parent_entry: ParentEntry | None) -> None:
        if parent_entry is None:
            return
        parent_entry.pending_children += 1

    def _record_child_fragment(
        self,
        parent_entry: ParentEntry | None,
        code: str,
        start_line: int | None,
        end_line: int | None,
        sequence: int
    ) -> None:
        if parent_entry is None:
            if code and code.strip():
                self.merged_chunks.append(code)
            else:
                log_process("DBMS", "CONVERT", f"⚠️ 루트 구간 {start_line}~{end_line}에 빈 변환 결과 반환 - 최종 코드에서 제외", logging.WARNING)
            return

        if code and code.strip():
            fragment = ChildFragment(
                sequence=sequence,
                code=code,
                start=start_line,
                end=end_line
            )
            parent_entry.children.append(fragment)
        else:
            log_process("DBMS", "CONVERT", f"⚠️ 자식 구간 {start_line}~{end_line}에 빈 변환 결과 반환 - 부모 {parent_entry.start}~{parent_entry.end}에 적용할 코드 없음", logging.WARNING)

        if parent_entry.pending_children > 0:
            parent_entry.pending_children -= 1
        else:
            log_process("DBMS", "CONVERT", f"⚠️ 부모 {parent_entry.start}~{parent_entry.end}의 미처리 자식 수가 음수 - Neo4j 데이터 확인 필요", logging.WARNING)

        self._try_finalize_parent(parent_entry)

    def _try_finalize_parent(self, entry: ParentEntry | None) -> None:
        if entry is None or entry.finalized or not entry.closed or entry.pending_children > 0:
            return

        if entry.is_root:
            ordered = [
                fragment.code
                for fragment in sorted(entry.children, key=lambda frag: frag.sequence)
                if fragment.code and fragment.code.strip()
            ]
            self.merged_chunks = ordered
            entry.children.clear()
            entry.finalized = True
            log_process("DBMS", "CONVERT", f"🎉 모든 변환 완료: 루트에 {len(ordered)}개 코드 블록 병합하여 최종 본문 구성")
            return

        children = sorted(entry.children, key=lambda frag: frag.sequence)
        merged_code = entry.code
        if entry.is_dml:
            merged_code = self._merge_dml_children(merged_code, children)
        else:
            merged_code = self._merge_regular_children(merged_code, children)

        merged_code = merged_code.strip()
        entry.finalized = True
        entry.children.clear()
        self._record_child_fragment(entry.parent, merged_code, entry.start, entry.end, entry.sequence)

    async def _process_work_queue(self) -> None:
        await self.work_queue.drain(self._handle_work_completion)

    def _handle_work_completion(self, item: ConversionWorkItem, generated_code: str) -> None:
        parent_entry = item.parent
        if parent_entry is None:
            return
        code_len = len(generated_code) if generated_code else 0
        log_process(
            "DBMS",
            "CONVERT",
            f"✅ 변환 완료 (work #{item.work_id}): 자식 {item.start}~{item.end} 결과({code_len}자)를 부모 {parent_entry.start}~{parent_entry.end}에 반영"
        )
        self._record_child_fragment(parent_entry, generated_code, item.start, item.end, item.sequence)

    async def generate(self) -> str:
        """
        전체 노드를 순회하며 타겟 DBMS 코드 생성
        
        Returns:
            str: 최종 병합된 코드
        """
        log_process("DBMS", "START", f"🚀 DBMS 변환 시작: {self.system_name}/{self.file_name} → {self.target.upper()}")
        self._reset_state()

        # 중복 제거: 같은 라인 범위는 한 번만 처리
        seen_nodes = set()
        node_count = 0
        for record in self.traverse_nodes:
            node = record['n']
            start_line = self._safe_int(node.get('startLine'))
            end_line = self._safe_int(node.get('endLine'))

            node_key = (start_line, end_line)
            if node_key in seen_nodes:
                continue
            seen_nodes.add(node_key)
            node_count += 1
            await self._process_node(record)

        await self._finalize_remaining()

        if self.root_entry:
            self.root_entry.closed = True
            self._try_finalize_parent(self.root_entry)

        await self._process_work_queue()

        if self.root_entry and not self.root_entry.finalized:
            self._try_finalize_parent(self.root_entry)
        if self.root_entry and self.root_entry.pending_children:
            log_process("DBMS", "CONVERT", f"⚠️ 루트에 아직 {self.root_entry.pending_children}개 자식 코드 미처리 - Neo4j 데이터 누락 여부 확인 필요", logging.WARNING)

        log_process("DBMS", "DONE", f"✅ 변환 완료: 총 {node_count}개 노드 처리")
        return self._final_output()

    # ----- 노드 처리 -----

    async def _process_node(self, record: dict) -> None:
        """단일 노드 처리"""
        node = record['n']
        node_labels = record.get('nodeLabels', [])
        node_type = self._resolve_node_type(node_labels, node)
        has_children = bool(node.get('has_children', False))
        token = self._safe_int(node.get('token'))
        start_line = self._safe_int(node.get('startLine'))
        end_line = self._safe_int(node.get('endLine'))
        relationship = record['r'][1] if record.get('r') else 'NEXT'

        # 노드 처리 로그
        node_kind = "부모" if has_children else "리프"
        stack_info = f", 부모 스택 깊이 {len(self.parent_stack)}" if self.parent_stack else ""
        log_process("DBMS", "LEAF" if not has_children else "PARENT", f"🔍 {node_type} ({start_line}~{end_line}) {node_kind} 노드 분석 중 - 토큰 {token}{stack_info}")

        # 부모 경계 체크
        while self.parent_stack and start_line > self.parent_stack[-1].end:
            if self.sp_accumulator.has_data():
                await self._analyze_and_merge()
            await self._finalize_parent()

        # 노드 타입별 처리
        is_large_parent = token >= TOKEN_THRESHOLD and has_children
        is_large_leaf = token >= TOKEN_THRESHOLD and not has_children

        if is_large_parent:
            # 큰 노드 처리 전에 쌓인 작은 노드들 먼저 변환
            if self.sp_accumulator.has_data():
                await self._analyze_and_merge()
            
            log_process("DBMS", "PARENT", f"🏗️ 대용량 부모 노드 발견: {node_type} ({start_line}~{end_line}, 토큰 {token}) - 스켈레톤 생성 후 부모 스택에 추가 (현재 깊이 {len(self.parent_stack)})")
            await self._handle_large_node(node, node_labels, start_line, end_line, token)
        else:
            appended = False
            if is_large_leaf:
                if self.sp_accumulator.has_data():
                    await self._analyze_and_merge()
            else:
                await self._flush_pending_accumulation(token)

            appended = self._handle_small_node(node, node_type, start_line, end_line, token)

            if appended and self._is_within_dml_parent():
                await self._analyze_and_merge()

        # 임계값 체크
        if is_large_leaf:
            log_process("DBMS", "CONVERT", f"⚡ 단독 대용량 리프 노드 즉시 변환: {node_type} ({start_line}~{end_line}, 토큰 {token})")
            await self._analyze_and_merge()
        elif self.sp_accumulator.token_total >= TOKEN_THRESHOLD:
            log_process("DBMS", "CONVERT", f"📊 토큰 임계값 도달: 누적 토큰 {self.sp_accumulator.token_total} ≥ {TOKEN_THRESHOLD} - 지금까지 모은 구간을 변환합니다")
            await self._analyze_and_merge()

    # ----- 대용량 노드 처리 -----

    async def _handle_large_node(
        self,
        node: dict,
        node_labels: list,
        start_line: int,
        end_line: int,
        token: int
    ) -> None:
        """대용량 노드(자식 있음, 토큰≥1000) 처리"""
        summarized = (node.get('summarized_code') or '').strip()
        if not summarized:
            log_process("DBMS", "PARENT", f"⚠️ {start_line}~{end_line} 구간에 요약 코드가 없어 스켈레톤 생성을 건너뜁니다", logging.WARNING)
            return

        node_type = self._resolve_node_type(node_labels, node)
        is_dml_node = str(node_type).upper() in DML_TYPES

        # LLM으로 스켈레톤 생성 (Rule 파일 사용)
        result = self.rule_loader.execute(
            role_name='dbms_summarized_dml' if is_dml_node else 'dbms_summarized',
            inputs={
                'summarized_code': summarized,
                'locale': self.locale
            },
            api_key=self.api_key
        )
        skeleton = result['code']

        parent_entry = self.parent_stack[-1] if self.parent_stack else self.root_entry
        entry = ParentEntry(
            start=start_line,
            end=end_line,
            code=skeleton,
            is_dml=is_dml_node,
            parent=parent_entry,
            sequence=self._next_sequence()
        )
        if parent_entry is not None:
            self._register_pending_child(parent_entry)
        self.parent_stack.append(entry)
        dml_info = " (DML)" if is_dml_node else ""
        log_process("DBMS", "PARENT", f"✅ {node_type} ({entry.start}~{entry.end}) 스켈레톤 생성 완료{dml_info} - 부모 스택 깊이 {len(self.parent_stack)}")

    # ----- 소형 노드 처리 -----

    def _handle_small_node(self, node: dict, node_type: str, start_line: int, end_line: int, token: int) -> bool:
        """소형 노드 또는 리프 노드 처리"""
        node_code = (node.get('node_code') or '').strip()
        if not node_code:
            return False

        appended = self.sp_accumulator.append(node_code, token, start_line, end_line)
        if not appended:
            return False

        # 로그는 최소화 - 누적 정보만 간단히 표시
        return True

    async def _flush_pending_accumulation(self, incoming_token: int) -> None:
        """다음 노드 추가 전에 임계값 초과 여부 확인"""
        if self.sp_accumulator.should_flush_with(incoming_token, TOKEN_THRESHOLD):
            current_tokens = self.sp_accumulator.token_total
            next_total = current_tokens + (incoming_token or 0)
            log_process("DBMS", "CONVERT", f"📊 다음 노드 추가 시 토큰 초과 예상: 현재 {current_tokens} + 다음 {incoming_token} = {next_total} ≥ {TOKEN_THRESHOLD} - 기존 누적분 먼저 변환")
            await self._analyze_and_merge()

    # ----- 부모 관리 -----

    async def _finalize_parent(self) -> None:
        """현재 부모 마무리"""
        if not self.parent_stack:
            return

        entry = self.parent_stack.pop()
        log_process("DBMS", "PARENT", f"🔚 부모 노드 처리 완료: {entry.start}~{entry.end} (자식 {len(entry.children)}개 병합 예정, 스택 깊이 {len(self.parent_stack)})")
        entry.closed = True
        self._try_finalize_parent(entry)

    # ----- 분석 및 병합 -----

    async def _analyze_and_merge(self) -> None:
        """LLM 분석 및 타겟 DBMS 코드 병합"""
        if not self.sp_accumulator.has_data():
            return

        sp_code, child_start, child_end, token_total, part_count = self.sp_accumulator.consume()
        parent_entry = self.parent_stack[-1] if self.parent_stack else self.root_entry
        
        # 부모 정보 구성
        if parent_entry and not parent_entry.is_root:
            parent_info = f"부모 {parent_entry.start}~{parent_entry.end}"
            target = "부모 children"
        else:
            parent_info = "루트"
            target = "최종 코드"
        
        log_process("DBMS", "CONVERT", f"🚀 변환 시작: 라인 {child_start}~{child_end} ({part_count}개 조각, {token_total} 토큰) → {target} ({parent_info})")

        parent_code = parent_entry.code if parent_entry else ""
        if parent_entry:
            self._register_pending_child(parent_entry)
        child_start = child_start if child_start is not None else 0
        child_end = child_end if child_end is not None else child_start
        work_item = ConversionWorkItem(
            work_id=self._next_work_id(),
            sequence=self._next_sequence(),
            code=sp_code,
            start=child_start,
            end=child_end,
            parent=parent_entry,
            parent_code=parent_code,
            token_count=token_total
        )
        self.work_queue.enqueue(work_item)

    def _final_output(self) -> str:
        """누적된 최상위 코드를 단일 문자열로 반환"""
        return "\n".join(self.merged_chunks).strip()

    def _merge_regular_children(self, code: str, children: list[ChildFragment]) -> str:
        """비-DML 부모 placeholder 처리"""
        ordered_children = [
            fragment.code for fragment in children or []
            if fragment.code and fragment.code.strip()
        ]
        child_block = "\n".join(ordered_children).strip()

        if CODE_PLACEHOLDER in code:
            if child_block:
                indented = textwrap.indent(child_block, '    ')
                return code.replace(CODE_PLACEHOLDER, f"\n{indented}\n", 1)
            return code.replace(CODE_PLACEHOLDER, "", 1)

        if not child_block:
            return code

        indented = textwrap.indent(child_block, '    ')
        return f"{code}\n{indented}"

    def _merge_dml_children(self, code: str, children: list[ChildFragment]) -> str:
        """DML 스켈레톤 placeholder에 자식 코드를 주입"""
        children_by_start: dict[int, deque] = defaultdict(deque)
        fallback_children: deque = deque()

        for fragment in children or []:
            payload = {
                'code': fragment.code,
                'start': fragment.start,
                'end': fragment.end
            }

            raw_start = payload.get('start')
            try:
                start_line = int(raw_start) if raw_start is not None else None
            except (TypeError, ValueError):
                start_line = None
            if start_line is None:
                fallback_children.append(payload)
            else:
                children_by_start[start_line].append(payload)

        placeholders = list(DML_PLACEHOLDER_PATTERN.finditer(code))
        placeholder_starts = [int(match.group('start')) for match in placeholders]
        total_children = sum(len(queue) for queue in children_by_start.values()) + len(fallback_children)
        log_process("DBMS", "PARENT", f"🔗 DML 병합 시작: {len(placeholders)}개 placeholder({placeholder_starts})에 {total_children}개 자식 코드 매핑")

        def _replacement(match: re.Match) -> str:
            indent = match.group('indent') or ''
            start = int(match.group('start'))
            label = match.group('label')

            queue = children_by_start.get(start)
            child_queue = children_by_start.get(start)
            child = child_queue.popleft() if child_queue else None
            if child_queue is not None and not child_queue:
                children_by_start.pop(start, None)

            if not child:
                remaining_starts = sorted(children_by_start.keys()) or ['없음']
                remaining_count = sum(len(queue) for queue in children_by_start.values()) + len(fallback_children)
                log_process("DBMS", "PARENT", f"⚠️ DML placeholder {label} (라인 {start})와 매칭되는 자식 없음 - 남은 후보: {remaining_starts} (총 {remaining_count}개)", logging.WARNING)
                return match.group(0)

            child_code = (child.get('code') or '').strip()
            if not child_code:
                log_process("DBMS", "PARENT", f"⚠️ DML placeholder {label}에 연결된 {child.get('start')}~{child.get('end')} 코드가 비어있음 - placeholder 유지", logging.WARNING)
                return match.group(0)

            return textwrap.indent(child_code, indent)

        merged_code = DML_PLACEHOLDER_PATTERN.sub(_replacement, code)

        residual_entries: list[dict] = []
        for start_line in sorted(children_by_start.keys()):
            residual_entries.extend(children_by_start[start_line])
        residual_entries.extend(fallback_children)

        if residual_entries:
            residual = "\n".join(
                (entry.get('code') or '').strip()
                for entry in residual_entries
                if entry.get('code')
            ).strip()
            if residual:
                merged_code = f"{merged_code.rstrip()}\n{residual}"
                log_process("DBMS", "PARENT", f"⚠️ DML placeholder보다 자식 {len(residual_entries)}개가 많아 하단에 residual 블록으로 추가", logging.WARNING)
            else:
                log_process("DBMS", "PARENT", f"⚠️ DML placeholder보다 자식 {len(residual_entries)}개가 남았지만 모두 빈 문자열이라 제외", logging.WARNING)

        return merged_code

    def _is_within_dml_parent(self) -> bool:
        """현재 스택 최상단이 DML 부모인지 확인"""
        return bool(self.parent_stack and self.parent_stack[-1].is_dml)

    # ----- 마무리 -----

    async def _finalize_remaining(self) -> None:
        """남은 데이터 정리"""
        if self.parent_stack:
            if self.sp_accumulator.has_data():
                await self._analyze_and_merge()
            while self.parent_stack:
                await self._finalize_parent()
        elif self.sp_accumulator.has_data():
            await self._analyze_and_merge()

    async def _save_target_file(self, base_name: str) -> str:
        """타겟 DBMS 파일 자동 저장"""
        try:
            # 저장 경로 설정
            base_path = build_rule_based_path(
                self.project_name,
                self.user_id,
                self.target,
                'dbms_conversion',
                system_name=self.system_name
            )
            
            body_code = self._final_output().strip()
            header_code = self.skeleton_code.strip()

            parts = [part for part in [header_code, body_code] if part]
            final_code = "\n\n".join(parts).rstrip() + "\n"

            # 파일 저장
            await save_file(
                content=final_code,
                filename=f"{base_name}.sql",
                base_path=base_path
            )
            
            log_process("DBMS", "SAVE", f"💾 {self.target.upper()} 파일 저장 완료: {base_path}/{base_name}.sql")
            
            return final_code
            
        except Exception as e:
            log_process("DBMS", "ERROR", f"❌ {self.target.upper()} 파일 저장 실패: {e}", logging.ERROR, e)
            raise ConvertingError(f"{self.target.upper()} 파일 저장 중 오류: {str(e)}")


# ----- 진입점 함수 -----
async def start_dbms_conversion(
    system_name: str,
    file_name: str,
    procedure_name: str,
    project_name: str,
    user_id: str,
    api_key: str,
    locale: str,
    target: str = "oracle"
) -> str:
    """
    DBMS 변환 시작 (단일 함수 호출용)
    """
    result = await start_dbms_conversion_steps(
        system_name, file_name, procedure_name,
        project_name, user_id, api_key, locale, target
    )
    return result["converted_code"]


async def start_dbms_conversion_steps(
    system_name: str,
    file_name: str,
    procedure_name: str,
    project_name: str,
    user_id: str,
    api_key: str,
    locale: str,
    target: str = "oracle",
    on_step: callable = None
) -> dict:
    """
    DBMS 변환 (단계별 콜백 지원)
    
    Args:
        system_name: 시스템명
        file_name: 파일명
        procedure_name: 프로시저 이름
        project_name: 프로젝트 이름
        user_id: 사용자 ID
        api_key: LLM API 키
        locale: 로케일
        target: 타겟 DBMS (oracle, postgresql)
        on_step: 단계 콜백 함수 (step: int, name: str, done: bool) -> None
    
    Returns:
        dict: {
            "skeleton_code": str,
            "converted_code": str,
            "procedure_name": str
        }
    
    Raises:
        ConvertingError: 변환 중 오류 발생 시
    """
    connection = Neo4jConnection()
    
    log_process("DBMS", "START", f"🚀 DBMS 변환 준비: {system_name}/{file_name} → {target.upper()}")

    try:
        # Step 1: 스켈레톤 생성
        if on_step:
            on_step(1, "skeleton", False)
        
        skeleton_code = await start_dbms_skeleton(
            system_name=system_name,
            file_name=file_name,
            procedure_name=procedure_name,
            project_name=project_name,
            user_id=user_id,
            api_key=api_key,
            locale=locale,
            target=target
        )
        
        if on_step:
            on_step(1, "skeleton", True)

        # Step 2: Neo4j 노드 조회 및 본문 변환
        if on_step:
            on_step(2, "body", False)
        
        # Neo4j 쿼리
        query_results = await connection.execute_queries([
            f"""
            MATCH (p:PROCEDURE {{
              system_name: '{system_name}',
              file_name: '{file_name}',
              procedure_name: '{procedure_name}',
              user_id: '{user_id}'
            }})
            
            CALL {{
              WITH p
              MATCH (p)-[:PARENT_OF]->(c)
              WHERE NOT c:DECLARE AND NOT c:Table AND NOT c:SPEC
                AND c.token < 1000
              WITH c, labels(c) AS cLabels, coalesce(toInteger(c.startLine), 0) AS sortKey
              RETURN c AS n, cLabels AS nodeLabels, NULL AS r, NULL AS m, sortKey
              
              UNION ALL
              
              WITH p
              MATCH (p)-[:PARENT_OF]->(c)
              WHERE NOT c:DECLARE AND NOT c:Table AND NOT c:SPEC
                AND coalesce(toInteger(c.token), 0) >= 1000
              WITH c
              MATCH path = (c)-[:PARENT_OF*0..]->(n)
              WHERE NOT n:DECLARE AND NOT n:Table AND NOT n:SPEC
              WITH n, path, nodes(path) AS pathNodes
              WHERE ALL(i IN range(0, size(pathNodes)-2) 
                        WHERE coalesce(toInteger(pathNodes[i].token), 0) >= 1000)
              OPTIONAL MATCH (n)-[r]->(m {{
                system_name: '{system_name}', file_name: '{file_name}', user_id: '{user_id}'
              }})
              WHERE r IS NULL
                 OR ( NOT (m:DECLARE OR m:Table OR m:SPEC)
                      AND none(x IN ['CALL','WRITES','FROM'] WHERE type(r) CONTAINS x) )
              WITH n, labels(n) AS nLabels, r, m, coalesce(toInteger(n.startLine), 0) AS sortKey
              RETURN DISTINCT n, nLabels AS nodeLabels, r, m, sortKey
            }}
            
            RETURN n, nodeLabels, r, m
            ORDER BY sortKey, coalesce(toInteger(n.token), 0), id(n)
            """
        ])
        dbms_nodes = query_results[0] if query_results else []

        # 변환 수행
        generator = DbmsConversionGenerator(
            dbms_nodes,
            system_name,
            file_name,
            procedure_name,
            user_id,
            api_key,
            locale,
            project_name,
            target,
            skeleton_code
        )

        await generator.generate()
        
        # 파일 저장
        base_name = file_name.rsplit(".", 1)[0]
        converted_code = await generator._save_target_file(base_name)
        
        if on_step:
            on_step(2, "body", True)

        log_process("DBMS", "DONE", f"✅ {base_name} 변환 완료")
        
        return {
            "skeleton_code": skeleton_code,
            "converted_code": converted_code,
            "procedure_name": procedure_name
        }

    except ConvertingError:
        raise
    except Exception as e:
        err_msg = f"DBMS 변환 중 오류: {str(e)}"
        log_process("DBMS", "ERROR", f"❌ {err_msg}", logging.ERROR, e)
        raise ConvertingError(err_msg)
    finally:
        await connection.close()

