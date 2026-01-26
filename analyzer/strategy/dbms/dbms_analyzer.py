"""DBMS 코드 분석 전략 - PL/SQL, 프로시저, 함수 등

AST 기반 PL/SQL 코드 분석 → Neo4j 그래프 생성.

분석 흐름 (Framework와 동일한 2단계 + DDL):
1. [Phase 1] DDL 처리 + 모든 파일 AST 그래프 생성 (병렬)
2. [Phase 2] 모든 파일 LLM 분석 (병렬)
3. [Phase 3] User Story 문서 생성 (BaseStreamingAnalyzer 공통)

Phase 로직은 각 phase 파일에 분리되어 있습니다:
- ddl_phase.py: DDL 처리 (Phase 0)
- ast_phase.py: AST 그래프 생성 (Phase 1)
- llm_phase.py: LLM 분석 (Phase 2)
- vector_phase.py: 벡터라이징 (Phase 4)
- lineage_phase.py: 리니지 분석 (Phase 5)
"""

import asyncio
import json
import logging
import os
from typing import Any, AsyncGenerator, Optional, List, Dict, Tuple

import aiofiles

from analyzer.neo4j_client import Neo4jClient
from analyzer.strategy.base_analyzer import BaseStreamingAnalyzer, AnalysisStats
from analyzer.strategy.base.file_context import FileStatus, FileAnalysisContext
from analyzer.pipeline_control import pipeline_controller, PipelinePhase
from config.settings import settings
from util.stream_event import (
    emit_message,
    emit_phase_event,
)
from util.text_utils import (
    log_process,
    generate_user_story_document,
)

# Phase 파일들에서 import
from analyzer.strategy.dbms.ddl_phase import run_ddl_phase
from analyzer.strategy.dbms.ast_phase import run_phase1
from analyzer.strategy.dbms.llm_phase import run_phase2
from analyzer.strategy.dbms.metadata_phase import run_metadata_phase
from analyzer.strategy.dbms.vector_phase import run_vectorize_phase
from analyzer.strategy.dbms.lineage_phase import run_lineage_phase


class DbmsAnalyzer(BaseStreamingAnalyzer):
    """DBMS 코드 분석 전략
    
    2단계 분석 + DDL 처리 (Framework와 동일):
    - Phase 0: DDL 처리
    - Phase 1: 모든 파일 AST 그래프 생성 (병렬)
    - Phase 2: 모든 파일 LLM 분석 (병렬) - Phase1 실패 파일 제외
    - Phase 3: 테이블/컬럼 설명 보강
    - Phase 4: 벡터라이징
    - Phase 5: 리니지 분석
    - User Story 문서 생성 (부모 클래스 공통)
    """

    # =========================================================================
    # 전략 메타데이터 (BaseStreamingAnalyzer 구현)
    # =========================================================================
    
    @property
    def strategy_name(self) -> str:
        return "DBMS"
    
    @property
    def strategy_emoji(self) -> str:
        return "🗄️"
    
    @property
    def file_type_description(self) -> str:
        return "SQL 파일"

    def __init__(self):
        self._cypher_lock = asyncio.Lock()
        self._file_semaphore: Optional[asyncio.Semaphore] = None
        self._ddl_schemas: set[str] = set()  # DDL에서 수집된 스키마 Set
        # DDL 메타데이터 캐시: {(schema, table_name): {description, columns}}
        self._ddl_table_metadata: Dict[Tuple[str, str], Dict[str, Any]] = {}

    # =========================================================================
    # 메인 파이프라인 (BaseStreamingAnalyzer 구현)
    # =========================================================================

    async def run_pipeline(
        self,
        file_names: list[tuple[str, str]],
        client: Neo4jClient,
        orchestrator: Any,
        stats: AnalysisStats,
    ) -> AsyncGenerator[bytes, None]:
        """DBMS 분석 파이프라인 실행
        
        흐름 (Framework와 동일):
        1. DDL 처리 + 파일 로드 (병렬)
        2. Phase 1: 모든 파일 AST 그래프 생성 (병렬)
        3. Phase 2: 모든 파일 LLM 분석 (병렬) - Phase1 실패 파일 제외
        
        Note: User Story Phase는 부모 클래스에서 처리
        """
        total_files = len(file_names)
        self._file_semaphore = asyncio.Semaphore(settings.concurrency.file_concurrency)
        
        # 파이프라인 상태 초기화
        pipeline_controller.reset()
        pipeline_state = pipeline_controller.get_state()

        # LLM 캐시 상태 표시
        if settings.llm.cache_enabled:
            cache_path = settings.llm.cache_db_path
            if not os.path.isabs(cache_path):
                cache_path = os.path.join(settings.path.base_dir, cache_path)
            cache_exists = os.path.exists(cache_path)
            cache_size = os.path.getsize(cache_path) if cache_exists else 0
            cache_size_str = f"{cache_size / 1024:.1f}KB" if cache_size < 1024*1024 else f"{cache_size / (1024*1024):.1f}MB"
            yield emit_message(f"🗄️ LLM 캐시: 활성화 ({cache_size_str if cache_exists else '신규'})")
        else:
            yield emit_message("🔄 LLM 캐시: 비활성화 (매번 새로운 LLM 호출)")

        if total_files > 0:
            yield emit_message(f"⚡ 병렬 처리: 파일 {settings.concurrency.file_concurrency}개 동시")

        # ========== Phase 0: DDL 처리 ==========
        pipeline_state.set_phase(PipelinePhase.DDL_PROCESSING, "DDL 파일 처리 중", 0)
        yield emit_phase_event(0, "DDL 처리", "started", 0, {"canPause": True})
        
        async for chunk in run_ddl_phase(self, client, orchestrator, stats):
            yield chunk
        
        yield emit_phase_event(0, "DDL 처리", "completed", 100)
        
        # DDL 후 일시정지 체크
        if not await pipeline_state.wait_if_paused():
            yield emit_message("⏹️ 파이프라인이 중단되었습니다")
            pipeline_state.set_phase(PipelinePhase.CANCELLED)
            return

        # DDL만 있는 경우 (소스 파일 없음) - Phase 1,2 스킵
        if total_files == 0:
            yield emit_message("")
            yield emit_message("📋 DDL 파일만 처리되었습니다 (소스 파일 없음)")
            pipeline_state.set_phase(PipelinePhase.COMPLETED)
            return

        # ========== Phase 1: AST 그래프 생성 ==========
        pipeline_state.set_phase(PipelinePhase.AST_GENERATION, "AST 구조 그래프 생성 중", 0)
        yield emit_phase_event(1, "AST 구조 생성", "started", 0, {"canPause": True})
        
        yield emit_message("")
        yield self.emit_separator()
        yield self.emit_phase_header(1, "🏗️ AST 구조 그래프 생성", f"{total_files}개 파일 병렬")
        yield self.emit_separator()

        contexts = await self._load_all_files(file_names, orchestrator)
        yield emit_message(f"   ✓ {len(contexts)}개 파일 로드 완료")

        async for chunk in run_phase1(self, contexts, client, orchestrator, stats):
            yield chunk

        # Phase 1 결과 요약
        ph1_ok_count = sum(1 for c in contexts if c.status == FileStatus.PH1_OK)
        ph1_fail_count = sum(1 for c in contexts if c.status == FileStatus.PH1_FAIL)
        
        yield emit_message("")
        yield self.emit_phase_complete(1, f"{stats.static_nodes_created}개 노드 생성")
        yield emit_phase_event(1, "AST 구조 생성", "completed", 100, {"nodes": stats.static_nodes_created})
        
        if ph1_fail_count > 0:
            yield self.emit_warning(f"Phase 1 실패: {ph1_fail_count}개 파일 → Phase 2 스킵 (토큰 절감)")

        # Phase 1 후 일시정지 체크
        if not await pipeline_state.wait_if_paused():
            yield emit_message("⏹️ 파이프라인이 중단되었습니다")
            pipeline_state.set_phase(PipelinePhase.CANCELLED)
            return

        # ========== Phase 2: LLM 분석 ==========
        ph2_targets = [c for c in contexts if c.status == FileStatus.PH1_OK]
        
        pipeline_state.set_phase(PipelinePhase.LLM_ANALYSIS, "AI 분석 중", 0)
        yield emit_phase_event(2, "AI 분석", "started", 0, {"canPause": True, "files": len(ph2_targets)})
        
        yield emit_message("")
        yield self.emit_separator()
        yield self.emit_phase_header(2, "🤖 AI 분석", f"{len(ph2_targets)}개 파일 병렬")
        yield self.emit_separator()
        
        if ph1_fail_count > 0:
            yield emit_message(f"   ℹ️ {ph1_fail_count}개 파일은 Phase 1 실패로 스킵됨 (토큰 절감)")

        async for chunk in run_phase2(self, ph2_targets, client, orchestrator, stats):
            yield chunk

        yield emit_message("")
        yield self.emit_phase_complete(2, f"{stats.llm_batches_executed}개 분석 완료")
        yield emit_phase_event(2, "AI 분석", "completed", 100, {"batches": stats.llm_batches_executed})
        
        # Phase 2 후 일시정지 체크
        if not await pipeline_state.wait_if_paused():
            yield emit_message("⏹️ 파이프라인이 중단되었습니다")
            pipeline_state.set_phase(PipelinePhase.CANCELLED)
            return
        
        # ========== Phase 3: 테이블/컬럼 설명 보강 ==========
        # Note: 실제 테이블 요약은 Phase 2의 run_llm_analysis 내에서 이미 수행됨
        # 여기서는 진행 상태만 표시
        pipeline_state.set_phase(PipelinePhase.TABLE_ENRICHMENT, "테이블/컬럼 설명 보강 중", 0)
        yield emit_phase_event(3, "테이블 설명 보강", "started", 0, {"canPause": True})
        yield self.emit_phase_header(3, "📊 테이블/컬럼 설명 보강", "LLM 분석 결과 적용")
        
        # 테이블 요약 결과 카운트 (이미 Phase 2에서 수행됨)
        table_count = sum(
            1 for ctx in ph2_targets 
            if ctx.processor and hasattr(ctx.processor, '_table_summary_store') 
            and ctx.processor._table_summary_store
        )
        
        yield emit_message(f"   ✅ 테이블/컬럼 설명 보강 완료")
        yield self.emit_phase_complete(3, "설명 보강 완료")
        yield emit_phase_event(3, "테이블 설명 보강", "completed", 100)
        
        # Phase 3 후 일시정지 체크
        if not await pipeline_state.wait_if_paused():
            yield emit_message("⏹️ 파이프라인이 중단되었습니다")
            pipeline_state.set_phase(PipelinePhase.CANCELLED)
            return

        # ========== Phase 3.5: 메타데이터 보강 (Text2SQL 기반) ==========
        # Text2SQL API를 통해 샘플 데이터 조회 후 LLM으로 설명 생성 + FK 추론
        yield emit_message("")
        yield self.emit_separator()
        yield self.emit_phase_header(3.5, "📋 메타데이터 보강", "Text2SQL 기반 설명 생성")
        yield self.emit_separator()
        
        async for chunk in run_metadata_phase(self, client, orchestrator, stats):
            yield chunk
        
        yield emit_message("")
        
        # Phase 3.5 후 일시정지 체크
        if not await pipeline_state.wait_if_paused():
            yield emit_message("⏹️ 파이프라인이 중단되었습니다")
            pipeline_state.set_phase(PipelinePhase.CANCELLED)
            return

        # ========== Phase 4: 벡터라이징 (임베딩 생성) ==========
        pipeline_state.set_phase(PipelinePhase.VECTORIZING, "테이블/컬럼 벡터라이징 중", 0)
        yield emit_phase_event(4, "벡터라이징", "started", 0, {"canPause": True})
        yield emit_message("")
        yield self.emit_separator()
        yield self.emit_phase_header(4, "🔢 벡터라이징", "임베딩 생성")
        yield self.emit_separator()
        
        async for chunk in run_vectorize_phase(self, client, orchestrator, stats):
            yield chunk
        
        yield emit_message("")
        yield self.emit_phase_complete(4, "벡터라이징 완료")
        yield emit_phase_event(4, "벡터라이징", "completed", 100, {
            "tables_vectorized": stats.tables_vectorized,
            "columns_vectorized": stats.columns_vectorized
        })
        
        # Phase 4 후 일시정지 체크
        if not await pipeline_state.wait_if_paused():
            yield emit_message("⏹️ 파이프라인이 중단되었습니다")
            pipeline_state.set_phase(PipelinePhase.CANCELLED)
            return
        
        # ========== Phase 5: 리니지 분석 (ETL 패턴 감지) ==========
        pipeline_state.set_phase(PipelinePhase.LINEAGE_ANALYSIS, "데이터 리니지 분석 중", 0)
        yield emit_phase_event(5, "리니지 분석", "started", 0, {"canPause": True})
        yield emit_message("")
        yield self.emit_separator()
        yield self.emit_phase_header(5, "🔗 데이터 리니지 분석", "ETL 패턴 감지")
        yield self.emit_separator()
        
        async for chunk in run_lineage_phase(self, client, orchestrator, stats):
            yield chunk
        
        yield emit_message("")
        yield self.emit_phase_complete(5, "리니지 분석 완료")
        yield emit_phase_event(5, "리니지 분석", "completed", 100, {
            "etl_count": getattr(stats, 'etl_count', 0),
            "data_flows": getattr(stats, 'data_flows', 0)
        })
        
        # Phase 5 후 일시정지 체크
        if not await pipeline_state.wait_if_paused():
            yield emit_message("⏹️ 파이프라인이 중단되었습니다")
            pipeline_state.set_phase(PipelinePhase.CANCELLED)
            return

    # =========================================================================
    # User Story 문서 생성 (BaseStreamingAnalyzer 구현)
    # =========================================================================

    async def build_user_story_doc(
        self,
        client: Neo4jClient,
        orchestrator: Any,
    ) -> Optional[str]:
        """분석된 프로시저에서 User Story 문서 생성"""
        query = """
            MATCH (__cy_n__)
            WHERE (__cy_n__:PROCEDURE OR __cy_n__:FUNCTION OR __cy_n__:TRIGGER)
              AND __cy_n__.summary IS NOT NULL
            OPTIONAL MATCH (__cy_n__)-[:HAS_USER_STORY]->(__cy_us__:UserStory)
            OPTIONAL MATCH (__cy_us__)-[:HAS_AC]->(__cy_ac__:AcceptanceCriteria)
            WITH __cy_n__, __cy_us__, collect(DISTINCT {
                id: __cy_ac__.id,
                title: __cy_ac__.title,
                given: __cy_ac__.given,
                when: __cy_ac__.when,
                then: __cy_ac__.then
            }) AS acceptance_criteria
            WITH __cy_n__, collect(DISTINCT {
                id: __cy_us__.id,
                role: __cy_us__.role,
                goal: __cy_us__.goal,
                benefit: __cy_us__.benefit,
                acceptance_criteria: acceptance_criteria
            }) AS user_stories
            RETURN __cy_n__.procedure_name AS name, 
                   __cy_n__.summary AS summary,
                   user_stories AS user_stories, 
                   labels(__cy_n__)[0] AS type
            ORDER BY __cy_n__.file_name, __cy_n__.startLine
        """
        
        async with self._cypher_lock:
            results = await client.execute_queries([query])
        
        # DDL만 있는 경우 또는 분석 결과가 없는 경우 None 반환 (오류 대신)
        if not results or not results[0]:
            log_process("ANALYZE", "USER_STORY", "User Story 생성 스킵: 분석된 프로시저/함수가 없습니다", logging.INFO)
            return None
        
        filtered = [
            r for r in results[0]
            if r.get("summary") or (r.get("user_stories") and len(r["user_stories"]) > 0)
        ]
        
        if not filtered:
            return None
        
        log_process("ANALYZE", "USER_STORY", f"User Story 생성 | 대상={len(filtered)}개 프로시저")
        return generate_user_story_document(
            results=filtered,
            source_name="ROBO",
            source_type="DBMS 프로시저/함수",
        )

    # =========================================================================
    # 파일 로드
    # =========================================================================

    async def _load_all_files(
        self,
        file_names: list[tuple[str, str]],
        orchestrator: Any,
    ) -> List[FileAnalysisContext]:
        """모든 파일의 AST JSON을 병렬로 로드합니다.
        
        source 파일은 더 이상 읽지 않습니다 - AST JSON의 code 속성 사용.
        """
        
        async def load_single(directory: str, file_name: str) -> FileAnalysisContext:
            base_name = os.path.splitext(file_name)[0]
            ast_path = os.path.join(orchestrator.dirs["analysis"], directory, f"{base_name}.json")

            async with aiofiles.open(ast_path, "r", encoding="utf-8") as ast_file:
                ast_content = await ast_file.read()
                return FileAnalysisContext(
                    directory=directory,
                    file_name=file_name,
                    ast_data=json.loads(ast_content),
                )

        tasks = [load_single(d, f) for d, f in file_names]
        return await asyncio.gather(*tasks)
