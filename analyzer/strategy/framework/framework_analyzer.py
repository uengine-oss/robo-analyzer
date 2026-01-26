"""Framework 코드 분석 전략 - Java, Kotlin 등

AST 기반 Java 코드 분석 → Neo4j 클래스 다이어그램 그래프 생성.

분석 흐름 (2단계 + 이중 병렬):
1. [Phase 1] 모든 파일 AST 그래프 생성 (병렬)
   - 정적 노드 생성: CLASS, INTERFACE, METHOD, FIELD
   - 정적 관계 생성: HAS_METHOD, HAS_FIELD, CONTAINS
   
2. [Phase 2] 모든 파일 LLM 분석 (파일 병렬 + 청크 병렬)
   - 코드 요약 및 분석
   - CALLS 관계 생성 (MATCH로 기존 노드 조회)
   - DEPENDENCY 관계 생성
   
3. [Phase 3] User Story 문서 생성 (BaseStreamingAnalyzer 공통)

파일 상태 관리:
- Phase1 실패 파일은 Phase2 스킵 (토큰 절감)
- 파일별 SUCCESS/FAILED/SKIPPED 상태 추적

Phase 로직은 각 phase 파일에 분리되어 있습니다:
- ast_phase.py: AST 그래프 생성 (Phase 1)
- llm_phase.py: LLM 분석 (Phase 2)
"""

import asyncio
import json
import logging
import os
from typing import Any, AsyncGenerator, Optional, List

import aiofiles

from analyzer.neo4j_client import Neo4jClient
from analyzer.strategy.base_analyzer import BaseStreamingAnalyzer, AnalysisStats
from analyzer.strategy.base.file_context import FileStatus, FileAnalysisContext
from config.settings import settings
from util.stream_event import emit_message
from util.text_utils import (
    generate_user_story_document,
    log_process,
)

# Phase 파일들에서 import
from analyzer.strategy.framework.ast_phase import run_phase1
from analyzer.strategy.framework.llm_phase import run_phase2


class FrameworkAnalyzer(BaseStreamingAnalyzer):
    """Java/Framework 코드 분석 전략
    
    2단계 분석 + 이중 병렬 처리:
    - Phase 1: 모든 파일 AST 그래프 생성 (병렬)
    - Phase 2: 모든 파일 LLM 분석 (병렬) - Phase1 실패 파일 제외
    - Phase 3: User Story 문서 생성 (부모 클래스 공통)
    
    파이프라인 특성:
    - 병렬 처리: 파일 단위로 동시 분석
    - 동시성 보호: Cypher 쿼리 락 사용
    - 프로세서 재사용: Phase 1에서 생성한 프로세서를 Phase 2에서 재사용
    - 토큰 절감: Phase1 실패 파일은 Phase2 스킵
    """

    # =========================================================================
    # 전략 메타데이터 (BaseStreamingAnalyzer 구현)
    # =========================================================================
    
    @property
    def strategy_name(self) -> str:
        return "프레임워크"
    
    @property
    def strategy_emoji(self) -> str:
        return "🚀"
    
    @property
    def file_type_description(self) -> str:
        return "Java/Kotlin 파일"

    def __init__(self):
        self._cypher_lock = asyncio.Lock()  # Cypher 쿼리 동시성 보호
        self._file_semaphore: Optional[asyncio.Semaphore] = None

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
        """Framework 분석 파이프라인 실행
        
        흐름:
        1. 파일 로드 (병렬)
        2. Phase 1: AST 그래프 생성 (병렬)
        3. Phase 2: LLM 분석 (병렬) - Phase1 실패 파일 제외 (토큰 절감)
        
        Note: User Story Phase는 부모 클래스에서 처리
        """
        total_files = len(file_names)
        self._file_semaphore = asyncio.Semaphore(settings.concurrency.file_concurrency)

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

        yield emit_message(f"⚡ 병렬 처리: 파일 {settings.concurrency.file_concurrency}개 동시")

        # ========== 파일 로드 ==========
        yield emit_message("")
        yield self.emit_separator()
        yield self.emit_phase_header(1, "🏗️ AST 구조 그래프 생성", f"{total_files}개 파일 병렬")
        yield self.emit_separator()

        contexts = await self._load_all_files(file_names, orchestrator)
        yield emit_message(f"   ✓ {len(contexts)}개 파일 로드 완료")

        # ========== Phase 1: AST 그래프 생성 (병렬) ==========
        async for chunk in run_phase1(self, contexts, client, orchestrator, stats):
            yield chunk

        # Phase 1 결과 요약
        ph1_ok_count = sum(1 for c in contexts if c.status == FileStatus.PH1_OK)
        ph1_fail_count = sum(1 for c in contexts if c.status == FileStatus.PH1_FAIL)
        
        yield emit_message("")
        yield self.emit_phase_complete(1, f"{stats.static_nodes_created}개 노드 생성")
        if ph1_fail_count > 0:
            yield self.emit_warning(f"Phase 1 실패: {ph1_fail_count}개 파일 → Phase 2 스킵 (토큰 절감)")

        # ========== Phase 2: LLM 분석 (병렬) - Phase1 성공 파일만 ==========
        ph2_targets = [c for c in contexts if c.status == FileStatus.PH1_OK]
        
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

    # =========================================================================
    # User Story 문서 생성 (BaseStreamingAnalyzer 구현)
    # =========================================================================

    async def build_user_story_doc(
        self,
        client: Neo4jClient,
        orchestrator: Any,
    ) -> Optional[str]:
        """분석된 클래스/인터페이스에서 User Story 문서 생성"""
        query = """
            MATCH (__cy_n__)
            WHERE (__cy_n__:CLASS OR __cy_n__:INTERFACE)
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
            RETURN __cy_n__.class_name AS name, 
                   __cy_n__.summary AS summary,
                   user_stories AS user_stories, 
                   labels(__cy_n__)[0] AS type
            ORDER BY __cy_n__.file_name, __cy_n__.startLine
        """
        
        async with self._cypher_lock:
            results = await client.execute_queries([query])
        
        if not results or not results[0]:
            log_process(
                "ANALYZE", "USER_STORY",
                "User Story 생성 스킵: 분석된 클래스/인터페이스가 없습니다",
                logging.INFO
            )
            return None
        
        filtered = [
            r for r in results[0]
            if r.get("summary") or (r.get("user_stories") and len(r["user_stories"]) > 0)
        ]
        
        if not filtered:
            return None
        
        log_process("ANALYZE", "USER_STORY", f"User Story 생성 | 대상={len(filtered)}개 클래스")
        return generate_user_story_document(
            results=filtered,
            source_name="ROBO",
            source_type="Java 클래스/인터페이스",
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
