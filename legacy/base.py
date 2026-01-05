"""분석 전략 기본 인터페이스 및 공통 프레임

모든 분석 전략(Framework, DBMS)의 기본 인터페이스와 공통 로직을 정의합니다.

주요 구성:
- AnalyzerStrategy: 추상 기본 인터페이스
- BaseStreamingAnalyzer: 공통 프레임 담당 (Neo4j 초기화, 리소스 정리 등)
- AnalysisStats: 분석 통계 (의미 기반 필드)
- 공통 유틸리티 함수들

설계 원칙:
- 공통 프레임(바깥 뼈대)은 BaseStreamingAnalyzer가 담당
- 내부 파이프라인(분석 단계, 순서)은 각 전략(Dbms/Framework)이 책임
- User Story Phase는 부모가 출력 규칙을 통제, 전략은 데이터 조회만 담당
"""

from abc import ABC, abstractmethod
from dataclasses import dataclass, field
from typing import Any, AsyncGenerator, Optional, List
import logging

from analyzer.neo4j_client import Neo4jClient
from util.stream_utils import (
    emit_message,
    emit_error,
    emit_complete,
    emit_data,
)
from util.exception import AnalysisError, CodeProcessError
from util.utility_tool import log_process


class AnalyzerStrategy(ABC):
    """분석 전략 기본 인터페이스
    
    Framework(Java/Kotlin)와 DBMS(PL/SQL) 분석을 위한 전략 패턴.
    
    사용법:
        strategy = AnalyzerFactory.create("framework")
        async for chunk in strategy.analyze(file_names, orchestrator):
            yield chunk
    """

    @abstractmethod
    async def analyze(
        self,
        file_names: list[tuple[str, str]],
        orchestrator: Any,
        **kwargs,
    ) -> AsyncGenerator[bytes, None]:
        """파일 목록을 분석하여 결과를 스트리밍합니다.
        
        Args:
            file_names: [(directory, file_name), ...] 튜플 리스트
            orchestrator: ServiceOrchestrator 인스턴스
            **kwargs: 추가 옵션
            
        Yields:
            NDJSON 형식의 바이트 스트림
        """
        raise NotImplementedError

    @staticmethod
    def calc_progress(current_line: int, total_lines: int) -> int:
        """현재 진행률 계산 (0-99%)
        
        Args:
            current_line: 현재 처리 중인 라인
            total_lines: 전체 라인 수
            
        Returns:
            진행률 (0-99)
        """
        if total_lines <= 0:
            return 0
        return min(int((current_line / total_lines) * 100), 99)


@dataclass
class AnalysisStats:
    """분석 통계 정보
    
    각 필드는 의미가 명확하게 정의됨:
    - files_*: 파일 처리 현황
    - ddl_*: DDL 처리 결과 (DBMS 전용)
    - static_*: 정적 그래프 생성 결과
    - llm_*: LLM 분석 결과
    - total_*: 전체 합계
    - failed_files: 실패한 파일 목록 (토큰 절감을 위한 추적)
    """
    # 파일 처리 현황
    files_total: int = 0
    files_completed: int = 0
    files_failed: int = 0
    failed_files: List[str] = field(default_factory=list)
    
    # DDL 처리 결과 (DBMS 전용)
    ddl_tables: int = 0
    ddl_columns: int = 0
    ddl_fks: int = 0
    
    # 정적 그래프 생성 결과
    static_nodes_created: int = 0
    static_rels_created: int = 0
    
    # LLM 분석 결과
    llm_batches_executed: int = 0
    llm_batches_failed: int = 0  # 실패한 배치 수 (스트림에 표시됨)
    llm_nodes_updated: int = 0
    llm_rels_created: int = 0
    
    # 전체 합계 (자동 계산용)
    total_nodes: int = 0
    total_rels: int = 0
    
    def add_graph_result(self, graph: dict, is_static: bool = False) -> None:
        """그래프 결과에서 통계 누적
        
        Args:
            graph: Neo4j 그래프 결과
            is_static: True이면 정적 그래프, False이면 LLM 분석 결과
        """
        node_count = len(graph.get("Nodes", []))
        rel_count = len(graph.get("Relationships", []))
        
        self.total_nodes += node_count
        self.total_rels += rel_count
        
        if is_static:
            self.static_nodes_created += node_count
            self.static_rels_created += rel_count
        else:
            self.llm_nodes_updated += node_count
            self.llm_rels_created += rel_count
    
    def add_ddl_result(self, tables: int, columns: int, fks: int) -> None:
        """DDL 처리 결과 누적"""
        self.ddl_tables += tables
        self.ddl_columns += columns
        self.ddl_fks += fks
        # DDL도 정적 그래프에 포함
        self.total_nodes += tables + columns
        self.total_rels += fks
    
    def mark_file_failed(self, file_name: str, reason: str = "") -> None:
        """파일 실패 기록 (토큰 절감을 위해 추적)"""
        self.files_failed += 1
        self.failed_files.append(f"{file_name}: {reason}" if reason else file_name)


class BaseStreamingAnalyzer(AnalyzerStrategy):
    """스트리밍 분석 공통 프레임
    
    부모 클래스가 책임지는 것 (공통 프레임):
    1. 분석 시작/완료 메시지
    2. Neo4j 초기화 및 제약조건 보장
    3. 기존 결과 존재 여부 확인 (증분/신규)
    4. 전략별 파이프라인 실행 위임
    5. User Story Phase 출력 (공통 규칙)
    6. 예외 처리 및 리소스 정리 (close)
    
    이 레벨에서는 "어떻게" 분석하는지는 모른다.
    오직 "언제 무엇을 호출한다"만 책임진다.
    """

    # =========================================================================
    # 전략별로 오버라이드해야 하는 추상 속성/메서드
    # =========================================================================
    
    @property
    @abstractmethod
    def strategy_name(self) -> str:
        """전략 이름 (예: "DBMS", "프레임워크")"""
        raise NotImplementedError
    
    @property
    @abstractmethod
    def strategy_emoji(self) -> str:
        """전략 아이콘 이모지"""
        raise NotImplementedError
    
    @property
    @abstractmethod
    def file_type_description(self) -> str:
        """분석 대상 파일 타입 설명 (예: "SQL 파일", "Java/Kotlin 파일")"""
        raise NotImplementedError

    @abstractmethod
    async def run_pipeline(
        self,
        file_names: list[tuple[str, str]],
        client: Neo4jClient,
        orchestrator: Any,
        stats: AnalysisStats,
    ) -> AsyncGenerator[bytes, None]:
        """전략별 분석 파이프라인 실행
        
        각 전략이 책임지는 것:
        - 내부 분석 단계의 순서와 방식
        - 병렬 처리 구조
        - 노드/관계 생성 의미 정의
        
        Args:
            file_names: 분석 대상 파일 리스트
            client: Neo4j 클라이언트
            orchestrator: 서비스 오케스트레이터
            stats: 분석 통계 (업데이트 필요)
            
        Yields:
            NDJSON 바이트 스트림
        """
        raise NotImplementedError

    @abstractmethod
    async def build_user_story_doc(
        self,
        client: Neo4jClient,
        orchestrator: Any,
    ) -> Optional[str]:
        """User Story 문서 생성 (전략별 데이터 조회)
        
        전략은 "어떤 데이터를 어떻게 조회할지"만 책임진다.
        출력 형식은 부모 클래스가 통제한다.
        
        Args:
            client: Neo4j 클라이언트
            orchestrator: 서비스 오케스트레이터
            
        Returns:
            생성된 User Story 문서 문자열 또는 None
        """
        raise NotImplementedError

    # =========================================================================
    # 공통 프레임 (Template Method 패턴)
    # =========================================================================

    async def analyze(
        self,
        file_names: list[tuple[str, str]],
        orchestrator: Any,
        **kwargs,
    ) -> AsyncGenerator[bytes, None]:
        """파일 목록을 분석하여 결과를 스트리밍합니다.
        
        공통 프레임 흐름:
        1. 분석 시작 메시지
        2. Neo4j 초기화
        3. 증분/신규 모드 판단
        4. 전략별 파이프라인 위임 (run_pipeline)
        5. User Story Phase (공통)
        6. 완료 메시지 및 리소스 정리
        """
        client = Neo4jClient()
        stats = AnalysisStats()
        stats.files_total = len(file_names)

        try:
            # ========== 1. 분석 시작 ==========
            async for chunk in self._emit_analysis_start(orchestrator, stats.files_total):
                yield chunk

            # ========== 2. Neo4j 초기화 ==========
            await client.ensure_constraints()
            yield emit_message("🔌 Neo4j 데이터베이스 연결 완료")

            # ========== 2-1. Project 노드 생성 (한 번만) ==========
            await self._ensure_project_node(client, orchestrator)

            # ========== 3. 증분/신규 모드 판단 ==========
            async for chunk in self._emit_analysis_mode(client, orchestrator, file_names):
                yield chunk

            # ========== 4. 전략별 파이프라인 실행 ==========
            async for chunk in self.run_pipeline(file_names, client, orchestrator, stats):
                yield chunk

            # ========== 5. User Story Phase (공통) ==========
            async for chunk in self._emit_user_story_phase(client, orchestrator):
                yield chunk

            # ========== 6. 완료 메시지 ==========
            async for chunk in self._emit_analysis_complete(stats):
                yield chunk
            yield emit_complete()

        except AnalysisError as e:
            log_process("ANALYZE", "ERROR", f"분석 오류: {e}", logging.ERROR, e)
            yield emit_error(str(e))
            raise
        except Exception as e:
            error_msg = f"예상치 못한 오류: {e}"
            log_process("ANALYZE", "ERROR", error_msg, logging.ERROR, e)
            yield emit_error(error_msg)
            raise CodeProcessError(error_msg) from e
        finally:
            await client.close()

    # =========================================================================
    # User Story Phase 공통 구현
    # =========================================================================

    async def _emit_user_story_phase(
        self,
        client: Neo4jClient,
        orchestrator: Any,
    ) -> AsyncGenerator[bytes, None]:
        """User Story 문서 생성 단계 (공통)
        
        출력 형식:
        ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
        📝 [최종 단계] User Story 문서 생성
        ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
           ✓ User Story 문서 생성 완료
        """
        yield emit_message("")
        yield self.emit_separator()
        yield self.emit_phase_header(0, "📝 User Story 문서 생성")
        yield self.emit_separator()
        
        try:
            user_story_doc = await self.build_user_story_doc(client, orchestrator)
            
            if user_story_doc:
                yield emit_data(
                    graph={"Nodes": [], "Relationships": []},
                    line_number=0,
                    analysis_progress=100,
                    current_file="user_stories.md",
                    user_story_document=user_story_doc,
                    event_type="user_story_document",
                )
                yield emit_message("   ✓ User Story 문서 생성 완료")
            else:
                yield self.emit_skip("추출할 User Story 없음")
                
        except AnalysisError as e:
            yield self.emit_warning(f"User Story 생성 실패: {e}")
            log_process("ANALYZE", "USER_STORY", f"User Story 생성 실패: {e}", logging.WARNING)

    # =========================================================================
    # 공통 메시지 emit 헬퍼
    # =========================================================================

    async def _emit_analysis_start(
        self,
        orchestrator: Any,
        total_files: int,
    ) -> AsyncGenerator[bytes, None]:
        """분석 시작 메시지 출력"""
        yield emit_message(f"{self.strategy_emoji} {self.strategy_name} 코드 분석을 시작합니다")
        yield emit_message(f"📦 프로젝트: {orchestrator.project_name}")
        yield emit_message(f"📊 분석 대상: {total_files}개 {self.file_type_description}")

    async def _emit_analysis_mode(
        self,
        client: Neo4jClient,
        orchestrator: Any,
        file_names: list[tuple[str, str]],
    ) -> AsyncGenerator[bytes, None]:
        """증분/신규 분석 모드 메시지 출력"""
        if await client.check_nodes_exist(orchestrator.user_id, file_names):
            yield emit_message("🔄 이전 분석 결과 발견 → 증분 업데이트 모드")
        else:
            yield emit_message("🆕 새로운 분석 시작")

    async def _emit_analysis_complete(
        self,
        stats: AnalysisStats,
    ) -> AsyncGenerator[bytes, None]:
        """분석 완료 메시지 출력"""
        yield emit_message("")
        yield emit_message("━" * 50)
        yield emit_message("✅ 모든 분석이 완료되었습니다!")
        yield emit_message(f"   📊 총 노드: {stats.total_nodes}개")
        yield emit_message(f"   🔗 총 관계: {stats.total_rels}개")
        
        # 파일 처리 현황
        if stats.files_total > 0:
            yield emit_message(f"   📁 파일: {stats.files_completed}/{stats.files_total}개 성공")
        
        # 실패 파일 목록 (토큰 낭비 방지를 위해 명시)
        if stats.files_failed > 0:
            yield emit_message(f"   ❌ 실패: {stats.files_failed}개 파일")
            for failed in stats.failed_files[:5]:  # 최대 5개만 표시
                yield emit_message(f"      • {failed}")
            if len(stats.failed_files) > 5:
                yield emit_message(f"      • ... 외 {len(stats.failed_files) - 5}개")
        
        # 상세 통계 (0이 아닌 경우만)
        if stats.ddl_tables > 0:
            yield emit_message(f"   🗂️ DDL: 테이블 {stats.ddl_tables}개, 컬럼 {stats.ddl_columns}개, FK {stats.ddl_fks}개")
        if stats.static_nodes_created > 0:
            yield emit_message(f"   🏗️ 정적 그래프: 노드 {stats.static_nodes_created}개, 관계 {stats.static_rels_created}개")
        if stats.llm_batches_executed > 0:
            yield emit_message(f"   🤖 AI 분석: {stats.llm_batches_executed}개 배치, 관계 {stats.llm_rels_created}개")
        
        yield emit_message("━" * 50)

    # =========================================================================
    # Project 노드 관리
    # =========================================================================

    async def _ensure_project_node(self, client: Neo4jClient, orchestrator: Any) -> None:
        """Project 노드를 생성합니다 (중복 방지).
        
        Args:
            client: Neo4j 클라이언트
            orchestrator: 서비스 오케스트레이터
        """
        from util.utility_tool import escape_for_cypher
        
        project_name = escape_for_cypher(orchestrator.project_name)
        user_id = escape_for_cypher(orchestrator.user_id)
        
        query = (
            f"MERGE (p:Project {{user_id: '{user_id}', name: '{project_name}'}})\n"
            f"SET p.name = '{project_name}'\n"
            f"RETURN p"
        )
        await client.execute_queries([query])
        log_process("ANALYZE", "PROJECT", f"Project 노드 생성/확인: {project_name}")

    # =========================================================================
    # 공통 유틸리티 메서드
    # =========================================================================

    @staticmethod
    def emit_phase_header(phase_num: int, title: str, detail: str = "") -> bytes:
        """단계 헤더 메시지 생성
        
        예: 🏗️ [1단계] AST 구조 그래프 생성 (10개 파일)
        """
        phase_title = f"[{phase_num}단계] " if phase_num > 0 else ""
        return emit_message(f"{phase_title}{title}" + (f" ({detail})" if detail else ""))

    @staticmethod
    def emit_separator() -> bytes:
        """구분선 메시지"""
        return emit_message("━" * 50)

    @staticmethod
    def emit_file_start(file_idx: int, total: int, file_name: str) -> bytes:
        """파일 처리 시작 메시지
        
        예: 📄 [1/10] OrderService.java
        """
        return emit_message(f"📄 [{file_idx}/{total}] {file_name}")

    @staticmethod
    def emit_node_created(node_type: str, node_name: str, line: Optional[int] = None) -> bytes:
        """노드 생성 메시지
        
        예:  → CLASS 노드: OrderService (Line 15)
        """
        line_info = f" (Line {line})" if line else ""
        return emit_message(f"   → {node_type} 노드: {node_name}{line_info}")

    @staticmethod
    def emit_relationship_created(rel_type: str, source: str, target: str) -> bytes:
        """관계 생성 메시지
        
        예:  → CALLS 관계: OrderService → OrderRepository
        """
        return emit_message(f"   → {rel_type} 관계: {source} → {target}")

    @staticmethod
    def emit_phase_complete(phase_num: int, summary: str) -> bytes:
        """단계 완료 메시지
        
        예: ✅ 1단계 완료: 구조 노드 25개 생성
        """
        return emit_message(f"   ✅ {phase_num}단계 완료: {summary}")

    @staticmethod
    def emit_skip(reason: str) -> bytes:
        """건너뛰기 메시지
        
        예: ℹ️ DDL 파일 없음 → 스키마 처리 건너뜀
        """
        return emit_message(f"ℹ️ {reason}")

    @staticmethod
    def emit_warning(message: str) -> bytes:
        """경고 메시지"""
        return emit_message(f"⚠️ {message}")

    @staticmethod
    def emit_file_error(file_name: str, error: str) -> bytes:
        """파일 오류 메시지"""
        return emit_message(f"   ❌ 오류 발생 ({file_name}): {error}")

    @staticmethod
    def emit_unknown_event(event_type: str) -> bytes:
        """알 수 없는 이벤트 타입 경고 메시지
        
        조용히 지나가는 흐름을 방지하기 위해 반드시 출력.
        """
        return emit_message(f"   ⚠️ 알 수 없는 이벤트 타입 수신: {event_type}")