"""DBMS 코드 분석 전략 - PL/SQL, 프로시저, 함수 등

AST 기반 PL/SQL 코드 분석 → Neo4j 그래프 생성.

분석 흐름 (Framework와 동일한 2단계 + DDL):
1. [Phase 1] DDL 처리 + 모든 파일 AST 그래프 생성 (병렬)
2. [Phase 2] 모든 파일 LLM 분석 (병렬)
3. [Phase 3] User Story 문서 생성 (BaseStreamingAnalyzer 공통)
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
from analyzer.strategy.dbms.ast_processor import DbmsAstProcessor
from analyzer.pipeline_control import pipeline_controller, PipelinePhase
from config.settings import settings
from util.exception import AnalysisError
from util.rule_loader import RuleLoader
from util.utility_tool import escape_for_cypher
from util.stream_utils import (
    emit_data,
    emit_message,
    emit_phase_event,
    format_graph_result,
)
from util.utility_tool import (
    escape_for_cypher,
    log_process,
    parse_table_identifier,
    generate_user_story_document,
    split_ddl_into_chunks,
    calculate_code_token,
)
from util.embedding_client import EmbeddingClient
from util.ddl_parser import parse_ddl as regex_parse_ddl
from analyzer.lineage_analyzer import LineageAnalyzer, LineageInfo


class DbmsAnalyzer(BaseStreamingAnalyzer):
    """DBMS 코드 분석 전략
    
    2단계 분석 + DDL 처리 (Framework와 동일):
    - Phase 1: DDL 처리 + 모든 파일 AST 그래프 생성 (병렬)
    - Phase 2: 모든 파일 LLM 분석 (병렬) - Phase1 실패 파일 제외
    - Phase 3: User Story 문서 생성 (부모 클래스 공통)
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
        
        async for chunk in self._run_ddl_phase(client, orchestrator, stats):
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

        async for chunk in self._run_phase1(contexts, client, orchestrator, stats):
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

        async for chunk in self._run_phase2(ph2_targets, client, orchestrator, stats):
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

        # ========== Phase 4: 벡터라이징 (임베딩 생성) ==========
        pipeline_state.set_phase(PipelinePhase.VECTORIZING, "테이블/컬럼 벡터라이징 중", 0)
        yield emit_phase_event(4, "벡터라이징", "started", 0, {"canPause": True})
        yield emit_message("")
        yield self.emit_separator()
        yield self.emit_phase_header(4, "🔢 벡터라이징", "임베딩 생성")
        yield self.emit_separator()
        
        async for chunk in self._run_vectorize_phase(client, orchestrator, stats):
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
        
        async for chunk in self._run_lineage_phase(client, orchestrator, stats):
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
    # DDL 처리
    # =========================================================================

    async def _run_ddl_phase(
        self,
        client: Neo4jClient,
        orchestrator: Any,
        stats: AnalysisStats,
    ) -> AsyncGenerator[bytes, None]:
        """DDL 파일 처리 - 테이블/컬럼 스키마 생성"""
        ddl_files = self._list_ddl_files(orchestrator)
        
        if not ddl_files:
            yield self.emit_skip("DDL 파일 없음 → 스키마 처리 건너뜀")
            return
        
        ddl_count = len(ddl_files)
        yield emit_message("")
        yield self.emit_separator()
        yield self.emit_phase_header(0, "📋 DDL 스키마 수집", f"{ddl_count}개 DDL")
        yield self.emit_separator()
        
        ddl_dir = orchestrator.dirs["ddl"]
        
        for idx, ddl_file in enumerate(ddl_files, 1):
            yield emit_message("")
            yield self.emit_file_start(idx, ddl_count, ddl_file)
            
            # 파일 단위 진행률: 각 파일이 (idx-1)/ddl_count ~ idx/ddl_count 구간 차지
            file_base_progress = int(((idx - 1) / ddl_count) * 100)
            file_end_progress = int((idx / ddl_count) * 100)
            
            # _process_ddl은 이제 AsyncGenerator - 메시지와 최종 결과를 yield
            ddl_graph = None
            ddl_stats = {"tables": 0, "columns": 0, "fks": 0}
            
            async for item in self._process_ddl(
                ddl_path=os.path.join(ddl_dir, ddl_file),
                client=client,
                file_name=ddl_file,
                orchestrator=orchestrator,
                emit_progress=True,
                file_base_progress=file_base_progress,
                file_end_progress=file_end_progress,
            ):
                if isinstance(item, tuple):
                    # 최종 결과 (ddl_graph, ddl_stats)
                    ddl_graph, ddl_stats = item
                else:
                    # 진행 상황 메시지 (bytes)
                    yield item
            
            if ddl_stats["tables"]:
                yield emit_message(f"   ✓ Table 노드: {ddl_stats['tables']}개")
            if ddl_stats["columns"]:
                yield emit_message(f"   ✓ Column 노드: {ddl_stats['columns']}개")
            if ddl_stats["fks"]:
                yield emit_message(f"   ✓ FK 관계: {ddl_stats['fks']}개")
            
            # 파일 완료 시 진행률 업데이트
            yield emit_phase_event(0, "DDL 처리", "running", file_end_progress)
            
            stats.add_ddl_result(ddl_stats["tables"], ddl_stats["columns"], ddl_stats["fks"])
            
            if ddl_graph and (ddl_graph.get("Nodes") or ddl_graph.get("Relationships")):
                yield emit_data(
                    graph=ddl_graph,
                    line_number=0,
                    analysis_progress=0,
                    current_file=f"DDL-{ddl_file}",
                )
        
        yield emit_message("")
        yield emit_message("📊 DDL 처리 완료:")
        yield emit_message(f"   • 테이블: {stats.ddl_tables}개")
        yield emit_message(f"   • 컬럼: {stats.ddl_columns}개")
        yield emit_message(f"   • FK: {stats.ddl_fks}개")

    def _list_ddl_files(self, orchestrator: Any) -> list[str]:
        """DDL 파일 목록 조회
        
        DDL 디렉토리가 없거나 파일이 없으면 빈 리스트 반환 (경고 처리, 에러 아님)
        """
        ddl_dir = orchestrator.dirs.get("ddl", "")
        if not ddl_dir:
            log_process("ANALYZE", "DDL", "DDL 디렉토리 설정 없음 - DDL 처리 생략")
            return []
        if not os.path.isdir(ddl_dir):
            # DDL 디렉토리가 없으면 경고만 하고 빈 리스트 반환
            log_process("ANALYZE", "DDL", f"DDL 디렉토리 없음: {ddl_dir} - DDL 처리 생략")
            return []
        try:
            files = sorted(
                f for f in os.listdir(ddl_dir)
                if os.path.isfile(os.path.join(ddl_dir, f))
            )
            if not files:
                # DDL 파일이 없으면 경고만 하고 빈 리스트 반환
                log_process("ANALYZE", "DDL", f"DDL 디렉토리에 파일 없음: {ddl_dir} - DDL 처리 생략")
                return []
            log_process("ANALYZE", "DDL", f"DDL 파일 발견: {len(files)}개")
            return files
        except OSError as e:
            log_process("ANALYZE", "DDL", f"DDL 디렉토리 읽기 실패: {ddl_dir} - {e}")
            return []

    def _apply_name_case(self, name: str, name_case: str) -> str:
        """메타데이터 대소문자 변환 적용
        
        Args:
            name: 변환할 이름 (테이블명, 컬럼명, 스키마명 등)
            name_case: 변환 옵션 (original, uppercase, lowercase)
        
        Returns:
            변환된 이름
        """
        if not name:
            return name
        if name_case == "uppercase":
            return name.upper()
        elif name_case == "lowercase":
            return name.lower()
        return name  # original: 그대로 반환

    async def _process_ddl(
        self,
        ddl_path: str,
        client: Neo4jClient,
        file_name: str,
        orchestrator: Any,
        emit_progress: bool = True,
        use_llm: bool = False,  # 기본값: 정규식 파서 사용 (빠름)
        file_base_progress: int = 0,  # 파일 시작 진행률
        file_end_progress: int = 100,  # 파일 종료 진행률
    ) -> AsyncGenerator[bytes | tuple[dict, dict], None]:
        """DDL 파일 처리 및 테이블/컬럼 노드 생성 (스트리밍)
        
        Args:
            use_llm: True면 LLM 사용, False면 정규식 파서 사용 (기본: False, 빠른 파싱)
            file_base_progress: 이 파일 처리 시작 시 전체 진행률 (0-100)
            file_end_progress: 이 파일 처리 완료 시 전체 진행률 (0-100)
        
        Yields:
            bytes: 진행 상황 메시지 (emit_message)
            tuple[dict, dict]: 최종 결과 (ddl_graph, ddl_stats) - 마지막에 한 번만
        """
        import re
        ddl_stats = {"tables": 0, "columns": 0, "fks": 0}
        
        # 진행률 범위 계산 (파일 내에서 파싱 50%, 저장 50% 비율)
        file_range = file_end_progress - file_base_progress
        parsing_end = file_base_progress + int(file_range * 0.5)
        saving_start = parsing_end
        saving_end = file_end_progress
        
        async with aiofiles.open(ddl_path, "r", encoding="utf-8") as f:
            ddl_content = await f.read()
        
        total_tokens = calculate_code_token(ddl_content)
        
        # ========================================
        # 정규식 파서 사용 (기본값 - 빠름)
        # ========================================
        if not use_llm:
            if emit_progress:
                yield emit_message(f"   ⚡ 정규식 파서 사용 (빠른 모드)")
                yield emit_phase_event(
                    phase_num=0,
                    phase_name="DDL 처리",
                    status="in_progress",
                    progress=file_base_progress + int(file_range * 0.1),
                    details={"mode": "regex", "tokens": total_tokens}
                )
            
            try:
                # 정규식 파서로 한 번에 파싱 (매우 빠름)
                parsed = await asyncio.to_thread(regex_parse_ddl, ddl_content)
                all_parsed_results = parsed.get("analysis", [])
                
                table_count = len(all_parsed_results)
                if emit_progress:
                    # 처음 5개 테이블명 미리보기
                    table_names = [t.get("table", {}).get("name", "?") for t in all_parsed_results[:5]]
                    preview = ", ".join(table_names)
                    if table_count > 5:
                        preview += f" 외 {table_count - 5}개"
                    
                    yield emit_message(f"   ✅ 파싱 완료: {table_count}개 테이블 ({preview})")
                    yield emit_phase_event(
                        phase_num=0,
                        phase_name="DDL 처리",
                        status="in_progress",
                        progress=parsing_end,
                        details={"tables_parsed": table_count, "mode": "regex"}
                    )
                    
            except Exception as e:
                if emit_progress:
                    yield emit_message(f"   ❌ 정규식 파싱 실패: {str(e)[:80]}")
                raise AnalysisError(f"DDL 정규식 파싱 실패: {e}")
        
        # ========================================
        # LLM 파서 사용 (use_llm=True인 경우)
        # ========================================
        else:
            # 대용량 DDL 청크 분할
            ddl_chunks = split_ddl_into_chunks(ddl_content)
            chunk_count = len(ddl_chunks)
            
            if chunk_count > 1 and emit_progress:
                yield emit_message(f"   📦 대용량 DDL 분할: {total_tokens:,} 토큰 → {chunk_count}개 청크")
            
            loader = RuleLoader(target_lang="dbms")
            
            # CREATE TABLE 패턴 (청크에서 테이블명 추출용)
            table_pattern = re.compile(
                r'CREATE\s+(?:TABLE|VIEW)\s+(?:IF\s+NOT\s+EXISTS\s+)?([\w\."]+)',
                re.IGNORECASE
            )
            
            # 청크별 LLM 호출 및 결과 병합
            all_parsed_results: List[Dict] = []
            total_tables_parsed = 0
            
            for chunk_idx, chunk in enumerate(ddl_chunks, 1):
                chunk_tokens = calculate_code_token(chunk)
                
                # 청크에 포함된 테이블명 추출 (미리보기용)
                tables_in_chunk_raw = table_pattern.findall(chunk)
                tables_preview = [t.replace('"', '').split('.')[-1] for t in tables_in_chunk_raw[:3]]
                preview_str = ", ".join(tables_preview)
                if len(tables_in_chunk_raw) > 3:
                    preview_str += f" 외 {len(tables_in_chunk_raw) - 3}개"
                
                # 진행률 계산 (청크 기준)
                progress_percent = int((chunk_idx - 1) / chunk_count * 100)
                
                if emit_progress:
                    yield emit_message(f"   🔄 [{chunk_idx}/{chunk_count}] 파싱 중: {preview_str}")
                    yield emit_phase_event(
                        phase_num=0,
                        phase_name="DDL 처리",
                        status="in_progress",
                        progress=progress_percent,
                        details={"chunk": chunk_idx, "total_chunks": chunk_count, "current_tables": preview_str}
                    )
                
                try:
                    # LLM 호출 (DDL 파싱용 빠른 모델)
                    chunk_parsed = await asyncio.to_thread(
                        loader.execute,
                        "ddl",
                        {"ddl_content": chunk, "locale": orchestrator.locale},
                        orchestrator.api_key,
                        model="gpt-4.1-mini",
                    )
                    tables_in_chunk = len(chunk_parsed.get("analysis", []))
                    all_parsed_results.extend(chunk_parsed.get("analysis", []))
                    total_tables_parsed += tables_in_chunk
                    
                    # 파싱된 테이블명 표시
                    parsed_table_names = [
                        t.get("table", {}).get("name", "?") 
                        for t in chunk_parsed.get("analysis", [])[:5]
                    ]
                    parsed_preview = ", ".join(parsed_table_names)
                    if tables_in_chunk > 5:
                        parsed_preview += f" 외 {tables_in_chunk - 5}개"
                    
                    progress_percent = int(chunk_idx / chunk_count * 100)
                    
                    if emit_progress:
                        yield emit_message(f"   ✅ [{chunk_idx}/{chunk_count}] 완료: {tables_in_chunk}개 테이블 ({parsed_preview})")
                        yield emit_phase_event(
                            phase_num=0,
                            phase_name="DDL 처리",
                            status="in_progress",
                            progress=progress_percent,
                            details={"chunk": chunk_idx, "total_chunks": chunk_count, "tables_parsed": total_tables_parsed}
                        )
                    
                except Exception as e:
                    if emit_progress:
                        yield emit_message(f"   ❌ [{chunk_idx}/{chunk_count}] 실패: {str(e)[:80]}")
                    raise AnalysisError(f"DDL 청크 {chunk_idx} 파싱 실패: {e}")
        
        # 병합된 결과를 parsed로 사용
        parsed = {"analysis": all_parsed_results}
        
        # db 속성은 DML 처리(ast_processor)와 일관성을 위해 소문자로 변환
        db_name = (orchestrator.target or 'postgres').lower()
        
        # 대소문자 변환 옵션
        name_case = getattr(orchestrator, 'name_case', 'original')

        # ===========================================
        # UNWIND 배치용 데이터 수집 (개별 쿼리 대신)
        # ===========================================
        schemas_data = []  # 스키마 데이터
        tables_data = []   # 테이블 데이터
        columns_data = []  # 컬럼 데이터
        fks_data = []      # FK 관계 데이터
        
        # 중복 방지용 세트
        seen_schemas = set()
        seen_tables = set()

        for table_info in parsed.get("analysis", []):
            table = table_info.get("table", {})
            columns = table_info.get("columns", [])
            foreign_keys = table_info.get("foreignKeys", [])
            primary_keys = [
                str(pk).strip().upper()
                for pk in (table_info.get("primaryKeys") or [])
                if pk
            ]

            # 원본 값에서 따옴표 제거 후 대소문자 변환 적용
            schema_raw = (table.get("schema") or "").strip()
            table_name_raw = (table.get("name") or "").strip()
            comment = (table.get("comment") or "").strip()
            table_type = (table.get("table_type") or "BASE TABLE").strip().upper()
            
            # parse_table_identifier로 따옴표 제거 및 스키마/테이블 분리
            qualified = f"{schema_raw}.{table_name_raw}" if schema_raw else table_name_raw
            parsed_schema, parsed_name, _ = parse_table_identifier(qualified)
            
            # name_case 옵션에 따라 대소문자 변환 적용
            schema = self._apply_name_case(parsed_schema if parsed_schema else "public", name_case)
            parsed_name = self._apply_name_case(parsed_name, name_case)
            
            # DDL에서 발견된 스키마 수집 (name_case 적용된 값으로 저장)
            if schema and schema.lower() != 'public':
                self._ddl_schemas.add(schema)
            
            # 스키마 데이터 수집 (중복 방지)
            schema_key = (db_name, schema)
            if schema_key not in seen_schemas:
                seen_schemas.add(schema_key)
                schemas_data.append({
                    "db": db_name,
                    "name": schema
                })
            
            # 테이블 데이터 수집 (중복 방지)
            table_key = (db_name, schema, parsed_name)
            if table_key not in seen_tables:
                seen_tables.add(table_key)
                tables_data.append({
                    "db": db_name,
                    "schema": schema,
                    "name": parsed_name,
                    "description": escape_for_cypher(comment),
                    "description_source": "ddl" if comment else "",
                    "table_type": table_type
                })
                ddl_stats["tables"] += 1
            
            # DDL 메타데이터 캐시 저장 (메모리)
            column_metadata = {}
            for col in columns:
                col_name_raw = (col.get("name") or "").strip()
                if not col_name_raw:
                    continue
                col_name = self._apply_name_case(col_name_raw, name_case)
                col_comment = (col.get("comment") or "").strip()
                column_metadata[col_name] = {
                    "description": col_comment,
                    "dtype": (col.get("dtype") or col.get("type") or "").strip(),
                    "nullable": col.get("nullable", True),
                }
            
            cache_key = (schema.lower(), parsed_name.lower())
            self._ddl_table_metadata[cache_key] = {
                "description": comment,
                "columns": column_metadata,
                "original_schema": schema,
                "original_name": parsed_name,
            }

            # 컬럼 데이터 수집
            for col in columns:
                col_name_raw = (col.get("name") or "").strip()
                if not col_name_raw:
                    continue
                
                col_name = self._apply_name_case(col_name_raw, name_case)
                col_type = (col.get("dtype") or col.get("type") or "").strip()
                col_nullable = col.get("nullable", True)
                col_comment = (col.get("comment") or "").strip()
                fqn = ".".join(filter(None, [schema, parsed_name, col_name])).lower()
                
                col_data = {
                    "fqn": escape_for_cypher(fqn),
                    "name": escape_for_cypher(col_name),
                    "dtype": escape_for_cypher(col_type),
                    "description": escape_for_cypher(col_comment),
                    "description_source": "ddl" if col_comment else "",
                    "nullable": col_nullable,
                    "table_db": db_name,
                    "table_schema": schema,
                    "table_name": parsed_name
                }
                if col_name_raw.upper() in primary_keys:
                    col_data["pk_constraint"] = f"{parsed_name}_pkey"
                
                columns_data.append(col_data)
                ddl_stats["columns"] += 1

            # FK 관계 데이터 수집
            for fk in foreign_keys:
                src_col_raw = (fk.get("column") or "").strip()
                ref = (fk.get("ref") or "").strip()
                if not src_col_raw or not ref or "." not in ref:
                    continue

                ref_table_part, ref_col_raw = ref.rsplit(".", 1)
                ref_schema_parsed, ref_table_raw, _ = parse_table_identifier(ref_table_part)
                ref_schema_final = self._apply_name_case(ref_schema_parsed or schema, name_case)
                ref_table = self._apply_name_case(ref_table_raw, name_case)
                src_col = self._apply_name_case(src_col_raw, name_case)
                ref_col = self._apply_name_case(ref_col_raw, name_case)

                fks_data.append({
                    "from_db": db_name,
                    "from_schema": schema,
                    "from_table": parsed_name,
                    "from_column": escape_for_cypher(src_col),
                    "to_db": db_name,
                    "to_schema": ref_schema_final or "",
                    "to_table": ref_table or "",
                    "to_column": escape_for_cypher(ref_col)
                })
                ddl_stats["fks"] += 1

        # ===========================================
        # UNWIND 배치 실행 (7~8번의 Neo4j 호출로 완료!)
        # ===========================================
        if emit_progress:
            yield emit_message(f"   💾 UNWIND 배치 저장 시작: {ddl_stats['tables']}개 테이블, {ddl_stats['columns']}개 컬럼, {ddl_stats['fks']}개 FK")
            yield emit_phase_event(
                phase_num=0,
                phase_name="DDL 처리",
                status="in_progress",
                progress=saving_start,
                details={
                    "step": "unwind_batch",
                    "tables": ddl_stats['tables'],
                    "columns": ddl_stats['columns'],
                    "fks": ddl_stats['fks']
                }
            )
        
        all_nodes: dict = {}
        all_relationships: dict = {}
        
        # 1. 스키마 노드 생성
        if schemas_data:
            if emit_progress:
                yield emit_message(f"      📦 [1/6] 스키마 {len(schemas_data)}개 생성 중...")
            schema_query = """
            UNWIND $items AS item
            MERGE (__cy_s__:Schema {db: item.db, name: item.name})
            RETURN __cy_s__
            """
            async with self._cypher_lock:
                result = await client.run_batch_unwind(schema_query, schemas_data)
            for node in result.get("Nodes", []):
                all_nodes[node.get("Node ID")] = node
        
        # 2. 테이블 노드 생성
        if tables_data:
            if emit_progress:
                yield emit_message(f"      📦 [2/6] 테이블 {len(tables_data)}개 생성 중...")
            table_query = """
            UNWIND $items AS item
            MERGE (__cy_t__:Table {db: item.db, schema: item.schema, name: item.name})
            SET __cy_t__.description = item.description,
                __cy_t__.description_source = item.description_source,
                __cy_t__.table_type = item.table_type
            RETURN __cy_t__
            """
            async with self._cypher_lock:
                result = await client.run_batch_unwind(table_query, tables_data)
            for node in result.get("Nodes", []):
                all_nodes[node.get("Node ID")] = node
        
        # 3. 테이블-스키마 관계 생성
        if tables_data:
            if emit_progress:
                yield emit_message(f"      📦 [3/6] 테이블-스키마 관계 {len(tables_data)}개 생성 중...")
            belongs_query = """
            UNWIND $items AS item
            MATCH (__cy_t__:Table {db: item.db, schema: item.schema, name: item.name})
            MATCH (__cy_s__:Schema {db: item.db, name: item.schema})
            MERGE (__cy_t__)-[__cy_r__:BELONGS_TO]->(__cy_s__)
            RETURN __cy_t__, __cy_r__, __cy_s__
            """
            async with self._cypher_lock:
                result = await client.run_batch_unwind(belongs_query, tables_data)
            for node in result.get("Nodes", []):
                all_nodes[node.get("Node ID")] = node
            for rel in result.get("Relationships", []):
                all_relationships[rel.get("Relationship ID")] = rel
        
        # 4. 컬럼 노드 생성
        if columns_data:
            if emit_progress:
                yield emit_message(f"      📦 [4/6] 컬럼 {len(columns_data)}개 생성 중...")
            column_query = """
            UNWIND $items AS item
            MERGE (__cy_c__:Column {fqn: item.fqn})
            SET __cy_c__.name = item.name,
                __cy_c__.dtype = item.dtype,
                __cy_c__.description = item.description,
                __cy_c__.description_source = item.description_source,
                __cy_c__.nullable = item.nullable,
                __cy_c__.pk_constraint = CASE WHEN item.pk_constraint IS NOT NULL THEN item.pk_constraint ELSE __cy_c__.pk_constraint END
            RETURN __cy_c__
            """
            async with self._cypher_lock:
                result = await client.run_batch_unwind(column_query, columns_data)
            for node in result.get("Nodes", []):
                all_nodes[node.get("Node ID")] = node
        
        # 5. 테이블-컬럼 관계 생성
        if columns_data:
            if emit_progress:
                yield emit_message(f"      📦 [5/6] 테이블-컬럼 관계 {len(columns_data)}개 생성 중...")
            has_column_query = """
            UNWIND $items AS item
            MATCH (__cy_t__:Table {db: item.table_db, schema: item.table_schema, name: item.table_name})
            MATCH (__cy_c__:Column {fqn: item.fqn})
            MERGE (__cy_t__)-[__cy_r__:HAS_COLUMN]->(__cy_c__)
            RETURN __cy_t__, __cy_r__, __cy_c__
            """
            async with self._cypher_lock:
                result = await client.run_batch_unwind(has_column_query, columns_data)
            for node in result.get("Nodes", []):
                all_nodes[node.get("Node ID")] = node
            for rel in result.get("Relationships", []):
                all_relationships[rel.get("Relationship ID")] = rel
        
        # 6. FK 관계 생성 (참조 테이블 MERGE + FK 관계)
        if fks_data:
            if emit_progress:
                yield emit_message(f"      📦 [6/6] FK 관계 {len(fks_data)}개 생성 중...")
            # 먼저 참조 테이블이 없으면 생성
            ref_tables_query = """
            UNWIND $items AS item
            MERGE (__cy_rt__:Table {db: item.to_db, schema: item.to_schema, name: item.to_table})
            RETURN __cy_rt__
            """
            async with self._cypher_lock:
                result = await client.run_batch_unwind(ref_tables_query, fks_data)
            for node in result.get("Nodes", []):
                all_nodes[node.get("Node ID")] = node
            
            # FK 관계 생성
            fk_query = """
            UNWIND $items AS item
            MATCH (__cy_t__:Table {db: item.from_db, schema: item.from_schema, name: item.from_table})
            MATCH (__cy_rt__:Table {db: item.to_db, schema: item.to_schema, name: item.to_table})
            MERGE (__cy_t__)-[__cy_r__:FK_TO_TABLE {sourceColumn: item.from_column, targetColumn: item.to_column}]->(__cy_rt__)
            ON CREATE SET __cy_r__.type = 'many_to_one', __cy_r__.source = 'ddl'
            RETURN __cy_t__, __cy_r__, __cy_rt__
            """
            async with self._cypher_lock:
                result = await client.run_batch_unwind(fk_query, fks_data)
            for node in result.get("Nodes", []):
                all_nodes[node.get("Node ID")] = node
            for rel in result.get("Relationships", []):
                all_relationships[rel.get("Relationship ID")] = rel
        
        if emit_progress:
            yield emit_message(f"   ✅ UNWIND 배치 저장 완료: {len(all_nodes)}개 노드, {len(all_relationships)}개 관계")
            yield emit_phase_event(
                phase_num=0,
                phase_name="DDL 처리",
                status="in_progress",
                progress=saving_end,
                details={
                    "step": "unwind_completed",
                    "nodes_created": len(all_nodes),
                    "relationships_created": len(all_relationships)
                }
            )
        
        result = {
            "Nodes": list(all_nodes.values()),
            "Relationships": list(all_relationships.values())
        }
        
        if emit_progress:
            yield emit_message(f"   ✅ Neo4j 저장 완료: {len(result['Nodes'])}개 노드, {len(result['Relationships'])}개 관계 생성")
            yield emit_phase_event(
                phase_num=0,
                phase_name="DDL 처리",
                status="in_progress",
                progress=saving_end,
                details={
                    "step": "neo4j_saved",
                    "tables": ddl_stats['tables'],
                    "columns": ddl_stats['columns'],
                    "fks": ddl_stats['fks'],
                    "nodes_created": len(result['Nodes']),
                    "relationships_created": len(result['Relationships'])
                }
            )
        
        log_process("ANALYZE", "DDL", f"DDL 처리 완료: {file_name} (T:{ddl_stats['tables']}, C:{ddl_stats['columns']}, FK:{ddl_stats['fks']})")
        
        # 최종 결과를 특별한 형태로 yield (tuple)
        yield (result, ddl_stats)

    # =========================================================================
    # 스키마 결정
    # =========================================================================

    def _resolve_default_schema(self, directory: str, name_case: str = 'original') -> str:
        """파일 경로에서 기본 스키마를 결정합니다.
        
        우선순위:
        1. 경로의 폴더명 중 DDL 스키마와 일치하는 것 (깊은 폴더 우선)
        2. 매칭 실패 시 파일이 존재하는 디렉토리명 사용
        
        Args:
            directory: 파일이 위치한 디렉토리 경로
            name_case: 대소문자 변환 옵션 (original, uppercase, lowercase)
        """
        if not directory:
            return self._apply_name_case("public", name_case)
        
        # 경로를 폴더 목록으로 분리 (깊은 순서대로)
        parts = directory.replace("\\", "/").split("/")
        parts = [p for p in parts if p]  # 빈 문자열 제거
        
        if not parts:
            return self._apply_name_case("public", name_case)
        
        # DDL 스키마가 있으면 매칭 시도 (깊은 폴더부터)
        # 대소문자 무관 비교 후, DDL에 저장된 원본 대소문자 반환
        if self._ddl_schemas:
            ddl_schemas_lower_map = {s.lower(): s for s in self._ddl_schemas}
            for folder in reversed(parts):
                matched = ddl_schemas_lower_map.get(folder.lower())
                if matched:
                    return matched  # DDL에서 name_case 적용된 값 그대로 반환
        
        # 매칭 실패 시 파일이 존재하는 디렉토리명(가장 깊은 폴더)에 name_case 적용
        return self._apply_name_case(parts[-1], name_case)

    # =========================================================================
    # 파일 로드
    # =========================================================================

    async def _load_all_files(
        self,
        file_names: list[tuple[str, str]],
        orchestrator: Any,
    ) -> List[FileAnalysisContext]:
        """모든 파일의 AST와 소스코드를 병렬로 로드합니다."""
        
        async def load_single(directory: str, file_name: str) -> FileAnalysisContext:
            src_path = os.path.join(orchestrator.dirs["src"], directory, file_name)
            base_name = os.path.splitext(file_name)[0]
            ast_path = os.path.join(orchestrator.dirs["analysis"], directory, f"{base_name}.json")

            async with aiofiles.open(ast_path, "r", encoding="utf-8") as ast_file, \
                       aiofiles.open(src_path, "r", encoding="utf-8") as src_file:
                ast_content, source_lines = await asyncio.gather(
                    ast_file.read(),
                    src_file.readlines(),
                )
                return FileAnalysisContext(
                    directory=directory,
                    file_name=file_name,
                    ast_data=json.loads(ast_content),
                    source_lines=source_lines,
                )

        tasks = [load_single(d, f) for d, f in file_names]
        return await asyncio.gather(*tasks)

    # =========================================================================
    # Phase 1: AST 그래프 생성
    # =========================================================================

    async def _run_phase1(
        self,
        contexts: List[FileAnalysisContext],
        client: Neo4jClient,
        orchestrator: Any,
        stats: AnalysisStats,
    ) -> AsyncGenerator[bytes, None]:
        """Phase 1: 모든 파일의 AST 그래프를 병렬로 생성합니다."""
        
        completed = 0
        total = len(contexts)
        results_queue: asyncio.Queue = asyncio.Queue()

        async def process_file(ctx: FileAnalysisContext):
            async with self._file_semaphore:
                try:
                    # name_case 옵션 가져오기
                    name_case = getattr(orchestrator, 'name_case', 'original')
                    
                    # 파일 경로 기반 기본 스키마 결정 (name_case 적용)
                    default_schema = self._resolve_default_schema(ctx.directory, name_case)
                    
                    processor = DbmsAstProcessor(
                        antlr_data=ctx.ast_data,
                        file_content="".join(ctx.source_lines),
                        directory=ctx.directory,
                        file_name=ctx.file_name,
                        api_key=orchestrator.api_key,
                        locale=orchestrator.locale,
                        dbms=orchestrator.target,
                        last_line=len(ctx.source_lines),
                        default_schema=default_schema,
                        ddl_table_metadata=self._ddl_table_metadata,
                        name_case=name_case,
                    )
                    ctx.processor = processor
                    
                    # 정적 그래프 생성
                    queries = processor.build_static_graph_queries()
                    
                    if queries:
                        all_nodes = {}
                        all_relationships = {}
                        async with self._cypher_lock:
                            async for batch_result in client.run_graph_query(queries):
                                for node in batch_result.get("Nodes", []):
                                    all_nodes[node["Node ID"]] = node
                                for rel in batch_result.get("Relationships", []):
                                    all_relationships[rel["Relationship ID"]] = rel
                        
                        graph = {"Nodes": list(all_nodes.values()), "Relationships": list(all_relationships.values())}
                        node_count = len(graph.get("Nodes", []))
                        rel_count = len(graph.get("Relationships", []))
                        
                        ctx.status = FileStatus.PH1_OK
                        await results_queue.put({
                            "type": "success",
                            "file": ctx.file_name,
                            "graph": graph,
                            "node_count": node_count,
                            "rel_count": rel_count,
                        })
                    else:
                        ctx.status = FileStatus.PH1_OK
                        await results_queue.put({
                            "type": "success",
                            "file": ctx.file_name,
                            "graph": {"Nodes": [], "Relationships": []},
                            "node_count": 0,
                            "rel_count": 0,
                        })
                        
                except Exception as e:
                    log_process("ANALYZE", "ERROR", f"Phase 1 오류 ({ctx.file_name}): {e}", logging.ERROR, e)
                    ctx.status = FileStatus.PH1_FAIL
                    ctx.error_message = str(e)[:100]
                    await results_queue.put({
                        "type": "error",
                        "file": ctx.file_name,
                        "message": str(e),
                    })
                    raise  # 즉시 중단 - 부분 실패 허용 안함

        # 모든 파일 병렬 처리 시작
        tasks = [asyncio.create_task(process_file(ctx)) for ctx in contexts]

        # 결과 수신 및 스트리밍
        while completed < total:
            result = await asyncio.wait_for(results_queue.get(), timeout=300.0)
            result_type = result.get("type", "")
            
            completed += 1
            stats.files_completed = completed
            
            # Phase 1 진행률 계산 (0-50% 범위 사용)
            phase1_progress = int(completed / total * 50)
            
            if result_type == "error":
                yield emit_message(f"   ❌ [{completed}/{total}] {result['file']}: {result['message'][:50]}")
                stats.mark_file_failed(result['file'], "Phase1 실패")
                yield emit_phase_event(
                    phase_num=1,
                    phase_name="AST 구조 분석",
                    status="in_progress",
                    progress=phase1_progress,
                    details={"file": result['file'], "status": "failed", "completed": completed, "total": total}
                )
            else:
                stats.add_graph_result(result["graph"], is_static=True)
                
                graph = result["graph"]
                node_count = result.get("node_count", 0)
                rel_count = result.get("rel_count", 0)
                
                # 노드 타입별 상세 집계
                node_types = {}
                for node in graph.get("Nodes", []):
                    labels = node.get("Labels", [])
                    for label in labels:
                        node_types[label] = node_types.get(label, 0) + 1
                
                # 상세 메시지 생성
                yield emit_message(f"   ✓ [{completed}/{total}] {result['file']}")
                
                if node_types:
                    # 주요 노드 타입 표시
                    proc_count = node_types.get("PROCEDURE", 0) + node_types.get("FUNCTION", 0)
                    stmt_count = sum(v for k, v in node_types.items() if k in ["SELECT", "INSERT", "UPDATE", "DELETE", "MERGE"])
                    table_refs = node_types.get("Table", 0)
                    
                    detail_parts = []
                    if proc_count:
                        detail_parts.append(f"프로시저/함수 {proc_count}개")
                    if stmt_count:
                        detail_parts.append(f"SQL문 {stmt_count}개")
                    if table_refs:
                        detail_parts.append(f"테이블 참조 {table_refs}개")
                    
                    if detail_parts:
                        yield emit_message(f"      → {', '.join(detail_parts)}")
                    
                    # 관계 정보
                    if rel_count > 0:
                        yield emit_message(f"      → 관계 {rel_count}개 생성 (FROM, WRITES, CALLS 등)")
                
                yield emit_phase_event(
                    phase_num=1,
                    phase_name="AST 구조 분석",
                    status="in_progress",
                    progress=phase1_progress,
                    details={
                        "file": result['file'],
                        "nodes": node_count,
                        "relationships": rel_count,
                        "completed": completed,
                        "total": total,
                        "node_types": node_types
                    }
                )
                
                yield emit_data(
                    graph=graph,
                    line_number=0,
                    analysis_progress=int(completed / total * 50),
                    current_file=result["file"],
                )

        # 모든 작업 완료 대기
        await asyncio.gather(*tasks, return_exceptions=True)

    # =========================================================================
    # Phase 2: LLM 분석
    # =========================================================================

    async def _run_phase2(
        self,
        contexts: List[FileAnalysisContext],
        client: Neo4jClient,
        orchestrator: Any,
        stats: AnalysisStats,
    ) -> AsyncGenerator[bytes, None]:
        """Phase 2: Phase1 성공 파일의 LLM 분석을 병렬로 실행합니다."""
        
        if not contexts:
            yield emit_message("   ℹ️ 분석 대상 파일 없음")
            return
        
        completed = 0
        total = len(contexts)
        results_queue: asyncio.Queue = asyncio.Queue()

        async def analyze_file(ctx: FileAnalysisContext):
            async with self._file_semaphore:
                try:
                    if not ctx.processor:
                        raise AnalysisError(f"Phase 1에서 프로세서 초기화 실패: {ctx.file_name}")
                    
                    # LLM 분석 실행 (튜플 반환: queries, failed_batch_count, failed_details)
                    analysis_queries, failed_batch_count, failed_details = await ctx.processor.run_llm_analysis()
                    
                    if analysis_queries:
                        all_nodes = {}
                        all_relationships = {}
                        async with self._cypher_lock:
                            async for batch_result in client.run_graph_query(analysis_queries):
                                for node in batch_result.get("Nodes", []):
                                    all_nodes[node["Node ID"]] = node
                                for rel in batch_result.get("Relationships", []):
                                    all_relationships[rel["Relationship ID"]] = rel
                                # 배치 진행률 스트리밍 (그래프 데이터 포함)
                                await results_queue.put({
                                    "type": "batch_progress",
                                    "file": ctx.file_name,
                                    "batch": batch_result.get("batch", 0),
                                    "total_batches": batch_result.get("total_batches", 0),
                                    "graph": {
                                        "Nodes": batch_result.get("Nodes", []),
                                        "Relationships": batch_result.get("Relationships", []),
                                    },
                                })
                        
                        graph = {"Nodes": list(all_nodes.values()), "Relationships": list(all_relationships.values())}
                        ctx.status = FileStatus.PH2_OK
                        await results_queue.put({
                            "type": "success",
                            "file": ctx.file_name,
                            "graph": graph,
                            "query_count": len(analysis_queries),
                            "failed_batches": failed_batch_count,
                            "failed_details": failed_details,
                        })
                    else:
                        ctx.status = FileStatus.PH2_OK
                        await results_queue.put({
                            "type": "success",
                            "file": ctx.file_name,
                            "graph": {"Nodes": [], "Relationships": []},
                            "query_count": 0,
                            "failed_batches": failed_batch_count,
                        })
                    
                    # 배치 실패가 있으면 즉시 중단 - 부분 실패 허용 안함
                    if failed_batch_count > 0:
                        raise AnalysisError(f"{ctx.file_name}: {failed_batch_count}개 배치 실패")
                        
                except Exception as e:
                    log_process("ANALYZE", "ERROR", f"Phase 2 오류 ({ctx.file_name}): {e}", logging.ERROR, e)
                    ctx.status = FileStatus.PH2_FAIL
                    ctx.error_message = str(e)[:100]
                    await results_queue.put({
                        "type": "error",
                        "file": ctx.file_name,
                        "message": str(e),
                    })
                    raise  # 즉시 중단 - 부분 실패 허용 안함

        # 모든 파일 병렬 처리 시작
        tasks = [asyncio.create_task(analyze_file(ctx)) for ctx in contexts]

        # 결과 수신 및 스트리밍
        while completed < total:
            result = await asyncio.wait_for(results_queue.get(), timeout=600.0)
            result_type = result.get("type", "")
            
            # warning은 카운트하지 않음 (추가 정보일 뿐)
            if result_type == "warning":
                yield emit_message(f"   ⚠️ {result['file']}: {result['message']}")
                continue
            
            # 배치 진행률은 카운트하지 않음 (중간 진행 상태)
            if result_type == "batch_progress":
                batch = result.get("batch", 0)
                total_batches = result.get("total_batches", 0)
                graph = result.get("graph")
                yield emit_message(f"      📦 {result['file']}: 배치 {batch}/{total_batches} 저장 완료")
                # 배치별 그래프 데이터 즉시 전송
                if graph:
                    yield emit_data(graph=graph)
                continue
            
            completed += 1
            
            # Phase 2 진행률 계산 (50-100% 범위 사용)
            phase2_progress = 50 + int(completed / total * 50)
            
            if result_type == "error":
                yield emit_message(f"   ❌ [{completed}/{total}] {result['file']}: {result['message'][:50]}")
                stats.mark_file_failed(result['file'], "Phase2 실패")
                yield emit_phase_event(
                    phase_num=2,
                    phase_name="AI 분석",
                    status="in_progress",
                    progress=phase2_progress,
                    details={"file": result['file'], "status": "failed", "completed": completed, "total": total}
                )
            else:
                stats.llm_batches_executed += 1
                graph = result["graph"]
                stats.add_graph_result(graph, is_static=False)
                
                # 배치 실패 정보 표시
                failed_batches = result.get("failed_batches", 0)
                failed_details = result.get("failed_details", [])
                fail_info = f" (배치 {failed_batches}개 실패)" if failed_batches > 0 else ""
                
                # 분석 결과 상세 집계
                node_count = len(graph.get("Nodes", []))
                rel_count = len(graph.get("Relationships", []))
                
                # 업데이트된 노드 타입별 집계
                updated_types = {}
                for node in graph.get("Nodes", []):
                    labels = node.get("Labels", [])
                    for label in labels:
                        updated_types[label] = updated_types.get(label, 0) + 1
                
                yield emit_message(f"   ✓ [{completed}/{total}] {result['file']} (쿼리 {result['query_count']}개){fail_info}")
                
                # LLM 분석 결과 상세 표시
                if updated_types:
                    # 주요 업데이트 표시
                    summary_added = sum(1 for n in graph.get("Nodes", []) if n.get("Properties", {}).get("summary"))
                    table_desc_added = sum(1 for n in graph.get("Nodes", []) 
                                           if "Table" in (n.get("Labels") or []) 
                                           and n.get("Properties", {}).get("analyzed_description"))
                    
                    detail_parts = []
                    if summary_added:
                        detail_parts.append(f"요약 {summary_added}개 생성")
                    if table_desc_added:
                        detail_parts.append(f"테이블 설명 {table_desc_added}개 보강")
                    if rel_count:
                        detail_parts.append(f"관계 {rel_count}개 업데이트")
                    
                    if detail_parts:
                        yield emit_message(f"      → {', '.join(detail_parts)}")
                
                # 실패 상세 정보 출력 (최대 3개)
                if failed_details:
                    stats.llm_batches_failed += len(failed_details)
                    for detail in failed_details[:3]:
                        yield emit_message(f"      ⚠️ 배치 #{detail['batch_id']} ({detail['node_ranges']}): {detail['error'][:50]}")
                
                yield emit_phase_event(
                    phase_num=2,
                    phase_name="AI 분석",
                    status="in_progress",
                    progress=phase2_progress,
                    details={
                        "file": result['file'],
                        "queries": result['query_count'],
                        "nodes_updated": node_count,
                        "relationships_updated": rel_count,
                        "completed": completed,
                        "total": total
                    }
                )
                
                yield emit_data(
                    graph=graph,
                    line_number=0,
                    analysis_progress=phase2_progress,
                    current_file=result["file"],
                )

        # 모든 작업 완료 대기
        await asyncio.gather(*tasks, return_exceptions=True)

    # =========================================================================
    # Phase 4: 벡터라이징 (임베딩 생성)
    # =========================================================================
    
    async def _run_vectorize_phase(
        self,
        client: Neo4jClient,
        orchestrator: Any,
        stats: AnalysisStats,
    ) -> AsyncGenerator[bytes, None]:
        """Phase 4: 테이블/컬럼 벡터라이징 (배치 최적화)
        
        Neo4j에 저장된 테이블/컬럼의 description을 기반으로 임베딩 생성
        배치 처리로 성능 최적화
        """
        from openai import AsyncOpenAI
        
        # OpenAI 클라이언트 초기화
        api_key = orchestrator.api_key or settings.openai_api_key
        if not api_key:
            yield emit_message("   ⚠️ OpenAI API 키가 없어 벡터라이징을 건너뜁니다")
            return
        
        openai_client = AsyncOpenAI(api_key=api_key)
        embedding_client = EmbeddingClient(openai_client)
        
        # ===========================================
        # 테이블 벡터라이징 (배치 처리)
        # ===========================================
        yield emit_message("   📊 [Phase 4-1] 테이블 벡터라이징 시작...")
        yield emit_phase_event(
            phase_num=4,
            phase_name="벡터라이징",
            status="in_progress",
            progress=0,
            details={"step": "table_vectorizing"}
        )
        
        # description과 analyzed_description을 합쳐서 임베딩 생성 (검색 품질 향상)
        table_query = """
        MATCH (__cy_t__:Table)
        WHERE (__cy_t__.vector IS NULL OR size(__cy_t__.vector) = 0)
          AND (__cy_t__.description IS NOT NULL OR __cy_t__.analyzed_description IS NOT NULL)
        RETURN elementId(__cy_t__) AS tid, 
               __cy_t__.name AS name,
               __cy_t__.schema AS schema,
               trim(
                 coalesce(__cy_t__.description, '') + 
                 CASE WHEN __cy_t__.analyzed_description IS NOT NULL AND __cy_t__.analyzed_description <> '' 
                      THEN ' | AI 분석: ' + __cy_t__.analyzed_description 
                      ELSE '' 
                 END
               ) AS description
        ORDER BY __cy_t__.schema, __cy_t__.name
        """
        
        try:
            async with self._cypher_lock:
                result = await client.execute_queries([table_query])
            
            tables = result[0] if result and result[0] else []
            total_tables = len(tables)
            
            if total_tables == 0:
                yield emit_message("      ℹ️ 벡터화할 테이블이 없습니다")
            else:
                yield emit_message(f"      📋 벡터화 대상: {total_tables}개 테이블")
                
                # 테이블도 배치로 처리 (50개씩)
                batch_size = 50
                for batch_idx in range(0, total_tables, batch_size):
                    batch = tables[batch_idx:batch_idx + batch_size]
                    batch_num = batch_idx // batch_size + 1
                    total_batches = (total_tables + batch_size - 1) // batch_size
                    
                    # 유효한 테이블만 필터링
                    valid_items = []
                    texts = []
                    for item in batch:
                        description = item.get("description", "") or ""
                        if not description:
                            continue
                        text = embedding_client.format_table_text(
                            table_name=item.get("name", ""),
                            description=description
                        )
                        texts.append(text)
                        valid_items.append(item)
                    
                    if not texts:
                        continue
                    
                    # 배치 진행 상황 표시
                    batch_progress = int(batch_idx / total_tables * 25)  # 0-25% 범위
                    yield emit_message(f"      🔄 [{batch_num}/{total_batches}] 테이블 {len(valid_items)}개 임베딩 생성 중...")
                    yield emit_phase_event(
                        phase_num=4,
                        phase_name="벡터라이징",
                        status="in_progress",
                        progress=batch_progress,
                        details={"step": "table_embedding", "batch": batch_num, "total_batches": total_batches}
                    )
                    
                    # 배치 임베딩 API 호출
                    vectors = await embedding_client.embed_batch(texts)
                    
                    # UNWIND 배치 저장용 데이터 생성
                    vector_updates = []
                    for item, vector in zip(valid_items, vectors):
                        if vector:
                            vector_updates.append({
                                "tid": item['tid'],
                                "vector": vector
                            })
                            stats.tables_vectorized += 1
                    
                    # UNWIND로 한번에 저장
                    if vector_updates:
                        update_query = """
                        UNWIND $items AS item
                        MATCH (__cy_t__) WHERE elementId(__cy_t__) = item.tid
                        SET __cy_t__.vector = item.vector
                        RETURN __cy_t__
                        """
                        async with self._cypher_lock:
                            await client.execute_with_params(update_query, {"items": vector_updates})
                        
                        yield emit_message(f"      ✓ [{batch_num}/{total_batches}] {len(vector_updates)}개 테이블 벡터 저장 완료")
                
                yield emit_message(f"   ✅ 테이블 벡터라이징 완료: {stats.tables_vectorized}개 테이블")
            
        except Exception as e:
            yield emit_message(f"   ⚠️ 테이블 벡터라이징 실패: {str(e)[:100]}")
        
        # ===========================================
        # 컬럼 벡터라이징 (배치 처리)
        # ===========================================
        yield emit_message("   📊 [Phase 4-2] 컬럼 벡터라이징 시작...")
        yield emit_phase_event(
            phase_num=4,
            phase_name="벡터라이징",
            status="in_progress",
            progress=25,
            details={"step": "column_vectorizing"}
        )
        
        # description과 analyzed_description을 합쳐서 임베딩 생성 (검색 품질 향상)
        column_query = """
        MATCH (__cy_t__:Table)-[:HAS_COLUMN]->(__cy_c__:Column)
        WHERE (__cy_c__.vector IS NULL OR size(__cy_c__.vector) = 0)
          AND (__cy_c__.description IS NOT NULL OR __cy_c__.analyzed_description IS NOT NULL)
        RETURN elementId(__cy_c__) AS cid,
               __cy_c__.name AS column_name,
               __cy_t__.name AS table_name,
               coalesce(__cy_c__.dtype, '') AS dtype,
               trim(
                 coalesce(__cy_c__.description, '') + 
                 CASE WHEN __cy_c__.analyzed_description IS NOT NULL AND __cy_c__.analyzed_description <> '' 
                      THEN ' | AI 분석: ' + __cy_c__.analyzed_description 
                      ELSE '' 
                 END
               ) AS description
        ORDER BY __cy_t__.schema, __cy_t__.name, __cy_c__.name
        """
        
        try:
            async with self._cypher_lock:
                result = await client.execute_queries([column_query])
            
            columns = result[0] if result and result[0] else []
            total_columns = len(columns)
            
            if total_columns == 0:
                yield emit_message("      ℹ️ 벡터화할 컬럼이 없습니다")
            else:
                yield emit_message(f"      📋 벡터화 대상: {total_columns}개 컬럼")
            
                # 배치 처리 (50개씩)
                batch_size = 50
                for i in range(0, total_columns, batch_size):
                    batch = columns[i:i + batch_size]
                    batch_num = i // batch_size + 1
                    total_batches = (total_columns + batch_size - 1) // batch_size
                    texts = []
                    
                    for item in batch:
                        text = embedding_client.format_column_text(
                            column_name=item.get("column_name", ""),
                            table_name=item.get("table_name", ""),
                            dtype=item.get("dtype", ""),
                            description=item.get("description", "")
                        )
                        texts.append(text)
                    
                    # 배치 진행 상황 표시
                    batch_progress = 25 + int(i / total_columns * 75)  # 25-100% 범위
                    yield emit_message(f"      🔄 [{batch_num}/{total_batches}] 컬럼 {len(texts)}개 임베딩 생성 중...")
                    yield emit_phase_event(
                        phase_num=4,
                        phase_name="벡터라이징",
                        status="in_progress",
                        progress=batch_progress,
                        details={"step": "column_embedding", "batch": batch_num, "total_batches": total_batches, "done": i, "total": total_columns}
                    )
                    
                    vectors = await embedding_client.embed_batch(texts)
                    
                    # UNWIND 배치 저장용 데이터 생성
                    vector_updates = []
                    for item, vector in zip(batch, vectors):
                        if vector:
                            vector_updates.append({
                                "cid": item['cid'],
                                "vector": vector
                            })
                            stats.columns_vectorized += 1
                    
                    # UNWIND로 한번에 저장
                    if vector_updates:
                        update_query = """
                        UNWIND $items AS item
                        MATCH (__cy_c__) WHERE elementId(__cy_c__) = item.cid
                        SET __cy_c__.vector = item.vector
                        RETURN __cy_c__
                        """
                        async with self._cypher_lock:
                            await client.execute_with_params(update_query, {"items": vector_updates})
                        
                        yield emit_message(f"      ✓ [{batch_num}/{total_batches}] {len(vector_updates)}개 컬럼 벡터 저장 완료")
                
                yield emit_message(f"   ✅ 컬럼 벡터라이징 완료: {stats.columns_vectorized}개 컬럼")
                yield emit_phase_event(
                    phase_num=4,
                    phase_name="벡터라이징",
                    status="completed",
                    progress=100,
                    details={"tables_vectorized": stats.tables_vectorized, "columns_vectorized": stats.columns_vectorized}
                )
            
        except Exception as e:
            yield emit_message(f"   ⚠️ 컬럼 벡터라이징 실패: {str(e)[:100]}")

    # =========================================================================
    # 리니지 분석 (Phase 5)
    # =========================================================================

    async def _run_lineage_phase(
        self,
        client: Neo4jClient,
        orchestrator: Any,
        stats: AnalysisStats,
    ) -> AsyncGenerator[bytes, None]:
        """ETL 패턴 감지 및 데이터 리니지 관계 생성
        
        Stored Procedure가 ETL 역할을 하는지 분석하고,
        Source 테이블 → ETL → Target 테이블 간 데이터 흐름 관계를 생성합니다.
        """
        source_dir = orchestrator.dirs.get("source", "")
        
        if not source_dir or not os.path.exists(source_dir):
            yield emit_message("   ℹ️ SP 파일 없음 → 리니지 분석 건너뜀")
            return
        
        # SP 파일 목록 가져오기
        sql_files = []
        for root, _, files in os.walk(source_dir):
            for f in files:
                if f.endswith(".sql"):
                    sql_files.append(os.path.join(root, f))
        
        if not sql_files:
            yield emit_message("   ℹ️ SP 파일 없음 → 리니지 분석 건너뜀")
            return
        
        yield emit_message(f"   🔍 {len(sql_files)}개 SP 파일에서 ETL 패턴 분석...")
        
        # 리니지 분석기 생성
        lineage_analyzer = LineageAnalyzer(dbms="oracle")
        all_lineages: list[LineageInfo] = []
        
        # 각 SP 파일 분석
        for idx, sql_file in enumerate(sql_files, 1):
            file_name = os.path.basename(sql_file)
            
            try:
                async with aiofiles.open(sql_file, "r", encoding="utf-8", errors="ignore") as f:
                    sql_content = await f.read()
                
                # 리니지 분석
                lineages = lineage_analyzer.analyze_sql_content(sql_content, file_name)
                
                # ETL 패턴이 감지된 경우만 저장
                etl_lineages = [l for l in lineages if l.is_etl]
                if etl_lineages:
                    for l in etl_lineages:
                        l.file_name = file_name
                    all_lineages.extend(etl_lineages)
                    yield emit_message(
                        f"   ✅ {file_name}: ETL 패턴 {len(etl_lineages)}개 감지"
                    )
                
            except Exception as e:
                log_process("LINEAGE", "ERROR", f"{file_name} 분석 실패: {e}")
        
        # ETL 패턴이 감지된 경우 Neo4j에 저장
        if all_lineages:
            yield emit_message(f"\n   📊 총 {len(all_lineages)}개 ETL 패턴 → Neo4j 저장...")
            
            try:
                # name_case 옵션 가져오기
                name_case = getattr(orchestrator, "name_case", "original")
                
                result = await lineage_analyzer.save_lineage_to_neo4j(
                    client=client,
                    lineage_list=all_lineages,
                    file_name="",
                    name_case=name_case,
                )
                
                # 통계 업데이트
                if not hasattr(stats, 'etl_count'):
                    stats.etl_count = 0
                if not hasattr(stats, 'data_flows'):
                    stats.data_flows = 0
                
                stats.etl_count = result.get("etl_nodes", 0)
                stats.data_flows = result.get("data_flows", 0)
                
                yield emit_message(
                    f"   ✅ 리니지 저장 완료: "
                    f"ETL 프로시저 {result.get('etl_nodes', 0)}개, "
                    f"ETL_READS {result.get('etl_reads', 0)}개, "
                    f"ETL_WRITES {result.get('etl_writes', 0)}개, "
                    f"DATA_FLOWS_TO {result.get('data_flows', 0)}개"
                )
                
            except Exception as e:
                yield emit_message(f"   ⚠️ 리니지 저장 실패: {str(e)[:100]}")
                log_process("LINEAGE", "ERROR", f"Neo4j 저장 실패: {e}")
        else:
            yield emit_message("   ℹ️ ETL 패턴 없음 → 리니지 관계 미생성")
