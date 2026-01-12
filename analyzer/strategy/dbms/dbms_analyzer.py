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
            MATCH (n)
            WHERE (n:PROCEDURE OR n:FUNCTION OR n:TRIGGER)
              AND n.summary IS NOT NULL
            OPTIONAL MATCH (n)-[:HAS_USER_STORY]->(us:UserStory)
            OPTIONAL MATCH (us)-[:HAS_AC]->(ac:AcceptanceCriteria)
            WITH n, us, collect(DISTINCT {
                id: ac.id,
                title: ac.title,
                given: ac.given,
                when: ac.when,
                then: ac.then
            }) AS acceptance_criteria
            WITH n, collect(DISTINCT {
                id: us.id,
                role: us.role,
                goal: us.goal,
                benefit: us.benefit,
                acceptance_criteria: acceptance_criteria
            }) AS user_stories
            RETURN n.procedure_name AS name, 
                   n.summary AS summary,
                   user_stories AS user_stories, 
                   labels(n)[0] AS type
            ORDER BY n.file_name, n.startLine
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
            
            ddl_graph, ddl_stats = await self._process_ddl(
                ddl_path=os.path.join(ddl_dir, ddl_file),
                client=client,
                file_name=ddl_file,
                orchestrator=orchestrator,
            )
            
            if ddl_stats["tables"]:
                yield emit_message(f"   ✓ Table 노드: {ddl_stats['tables']}개")
            if ddl_stats["columns"]:
                yield emit_message(f"   ✓ Column 노드: {ddl_stats['columns']}개")
            if ddl_stats["fks"]:
                yield emit_message(f"   ✓ FK 관계: {ddl_stats['fks']}개")
            
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
    ) -> tuple[dict, dict]:
        """DDL 파일 처리 및 테이블/컬럼 노드 생성
        
        대용량 DDL 파일의 경우 CREATE TABLE 단위로 청크 분할하여 처리합니다.
        각 청크에는 CREATE TABLE, COMMENT ON, ALTER TABLE 구문이 함께 포함됩니다.
        """
        ddl_stats = {"tables": 0, "columns": 0, "fks": 0}
        
        async with aiofiles.open(ddl_path, "r", encoding="utf-8") as f:
            ddl_content = await f.read()
        
        # 대용량 DDL 청크 분할
        ddl_chunks = split_ddl_into_chunks(ddl_content)
        total_tokens = calculate_code_token(ddl_content)
        chunk_count = len(ddl_chunks)
        
        if chunk_count > 1:
            log_process("DDL", "CHUNK", f"📦 대용량 DDL 분할: {total_tokens:,} 토큰 → {chunk_count}개 청크")
        
        loader = RuleLoader(target_lang="dbms")
        
        # 청크별 LLM 호출 및 결과 병합
        all_parsed_results: List[Dict] = []
        for chunk_idx, chunk in enumerate(ddl_chunks, 1):
            chunk_tokens = calculate_code_token(chunk)
            if chunk_count > 1:
                log_process("DDL", "CHUNK", f"  청크 {chunk_idx}/{chunk_count} 처리 중 ({chunk_tokens:,} 토큰)")
            
            try:
                # LLM 호출을 비동기로 처리 (I/O 블로킹 방지)
                import asyncio
                chunk_parsed = await asyncio.to_thread(
                    loader.execute,
                    "ddl",
                    {"ddl_content": chunk, "locale": orchestrator.locale},
                    orchestrator.api_key,
                )
                tables_in_chunk = len(chunk_parsed.get("analysis", []))
                all_parsed_results.extend(chunk_parsed.get("analysis", []))
                
                if chunk_count > 1:
                    log_process("DDL", "CHUNK", f"  ✅ 청크 {chunk_idx} 완료: {tables_in_chunk}개 테이블 파싱")
            except Exception as e:
                log_process("DDL", "ERROR", f"  ❌ 청크 {chunk_idx} 실패: {str(e)[:100]}")
                raise AnalysisError(f"DDL 청크 {chunk_idx} 파싱 실패: {e}")
        
        # 병합된 결과를 parsed로 사용
        parsed = {"analysis": all_parsed_results}
        
        queries = []
        # db 속성은 DML 처리(ast_processor)와 일관성을 위해 소문자로 변환
        common = {
            "db": (orchestrator.target or 'postgres').lower(),
        }
        
        # 대소문자 변환 옵션
        name_case = getattr(orchestrator, 'name_case', 'original')

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
            
            # parse_table_identifier가 소문자로 변환하므로, 여기서 다시 대소문자 변환 적용
            schema = self._apply_name_case(parsed_schema if parsed_schema else "public", name_case)
            parsed_name = self._apply_name_case(parsed_name, name_case)
            
            # DDL에서 발견된 스키마 수집 (내부 비교용으로 소문자 저장)
            if schema and schema.lower() != 'public':
                self._ddl_schemas.add(schema.lower())

            # Table 노드 생성 (MERGE 키: db, schema, name 사용)
            # 같은 스키마/테이블명이면 같은 노드로 취급해야 함
            merge_key = {
                "db": common["db"],
                "schema": schema,
                "name": parsed_name
            }
            merge_str = ", ".join(f"`{k}`: '{v}'" for k, v in merge_key.items())
            
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
            
            set_props = {
                **common,
                "description": escape_for_cypher(comment),
                "description_source": "ddl" if comment else "",  # DDL에서 추출된 설명
                "table_type": table_type,
            }
            set_str = ", ".join(f"t.`{k}` = '{v}'" for k, v in set_props.items())
            
            # Schema 노드 생성 (스키마가 없으면 'public' 사용)
            # 대소문자 변환은 이미 schema 변수에 적용됨
            schema_name = schema if schema else self._apply_name_case('public', name_case)
            schema_merge = {
                "db": common["db"],
                "name": schema_name,  # 대소문자 변환이 이미 적용됨
            }
            schema_merge_str = ", ".join(f"`{k}`: '{v}'" for k, v in schema_merge.items())
            queries.append(f"MERGE (s:Schema {{{schema_merge_str}}}) RETURN s")
            
            # Table 노드 생성 및 Schema에 BELONGS_TO 관계 연결
            queries.append(f"MERGE (t:Table {{{merge_str}}}) SET {set_str} RETURN t")
            queries.append(
                f"MATCH (t:Table {{{merge_str}}})\n"
                f"MATCH (s:Schema {{{schema_merge_str}}})\n"
                f"MERGE (t)-[r:BELONGS_TO]->(s) RETURN t, r, s"
            )
            ddl_stats["tables"] += 1
            
            # DDL 메타데이터 캐시 저장 (메모리)
            # 키는 소문자로 저장하여 대소문자 무관하게 조회 가능
            # 원본 대소문자도 함께 저장하여 SP 분석에서 DDL과 동일한 대소문자 사용
            table_key = (schema.lower(), parsed_name.lower())
            self._ddl_table_metadata[table_key] = {
                "description": comment,
                "columns": column_metadata,
                "original_schema": schema,  # DDL에서 사용한 원본 스키마명
                "original_name": parsed_name,  # DDL에서 사용한 원본 테이블명
            }

            # Column 노드 생성
            for col in columns:
                col_name_raw = (col.get("name") or "").strip()
                if not col_name_raw:
                    continue
                
                # 대소문자 변환 적용
                col_name = self._apply_name_case(col_name_raw, name_case)
                
                col_type = (col.get("dtype") or col.get("type") or "").strip()
                col_nullable = col.get("nullable", True)
                col_comment = (col.get("comment") or "").strip()
                fqn = ".".join(filter(None, [schema, parsed_name, col_name])).lower()
                escaped_fqn = escape_for_cypher(fqn)

                col_merge = {"fqn": escaped_fqn}
                col_merge_str = ", ".join(f"`{k}`: '{v}'" for k, v in col_merge.items())
                col_set = {
                    "name": escape_for_cypher(col_name),
                    "dtype": escape_for_cypher(col_type),
                    "description": escape_for_cypher(col_comment),
                    "description_source": "ddl" if col_comment else "",  # DDL에서 추출된 설명
                    "nullable": "true" if col_nullable else "false",
                    "fqn": escaped_fqn,
                }
                if col_name_raw.upper() in primary_keys:  # PK 체크는 원본 대문자로
                    col_set["pk_constraint"] = f"{parsed_name}_pkey"
                
                col_set_str = ", ".join(f"c.`{k}` = '{v}'" for k, v in col_set.items())
                queries.append(f"MERGE (c:Column {{{col_merge_str}}}) SET {col_set_str} RETURN c")
                queries.append(
                    f"MATCH (t:Table {{{merge_str}}})\n"
                    f"MATCH (c:Column {{{col_merge_str}}})\n"
                    f"MERGE (t)-[r:HAS_COLUMN]->(c) RETURN t, r, c"
                )
                ddl_stats["columns"] += 1

            # FK 관계 생성 - 각 FK 매핑마다 별도의 FK_TO_TABLE 관계 생성
            # 속성: sourceColumn, targetColumn, type, source
            # source='ddl': DDL에서 추출 (실선 표시)
            for fk in foreign_keys:
                src_col_raw = (fk.get("column") or "").strip()
                ref = (fk.get("ref") or "").strip()
                if not src_col_raw or not ref or "." not in ref:
                    continue

                ref_table_part, ref_col_raw = ref.rsplit(".", 1)
                ref_schema_parsed, ref_table_raw, _ = parse_table_identifier(ref_table_part)
                ref_schema_final = self._apply_name_case(ref_schema_parsed or schema, name_case)
                ref_table = self._apply_name_case(ref_table_raw, name_case)
                
                # 컬럼명에도 대소문자 변환 적용
                src_col = self._apply_name_case(src_col_raw, name_case)
                ref_col = self._apply_name_case(ref_col_raw, name_case)

                # 참조 테이블 MERGE (스키마/이름으로만 매칭)
                ref_table_merge = {
                    "db": common["db"],
                    "schema": ref_schema_final or "",
                    "name": ref_table or ""
                }
                ref_merge_str = ", ".join(f"`{k}`: '{v}'" for k, v in ref_table_merge.items())
                queries.append(f"MERGE (rt:Table {{{ref_merge_str}}}) RETURN rt")
                
                escaped_src_col = escape_for_cypher(src_col)
                escaped_tgt_col = escape_for_cypher(ref_col)
                
                queries.append(
                    f"MATCH (t:Table {{{merge_str}}})\n"
                    f"MATCH (rt:Table {{{ref_merge_str}}})\n"
                    f"MERGE (t)-[r:FK_TO_TABLE {{sourceColumn: '{escaped_src_col}', targetColumn: '{escaped_tgt_col}'}}]->(rt)\n"
                    f"ON CREATE SET r.type = 'many_to_one', r.source = 'ddl'\n"
                    f"RETURN t, r, rt"
                )
                ddl_stats["fks"] += 1

        async with self._cypher_lock:
            result = await client.run_graph_query(queries)
        
        log_process("ANALYZE", "DDL", f"DDL 처리 완료: {file_name} (T:{ddl_stats['tables']}, C:{ddl_stats['columns']}, FK:{ddl_stats['fks']})")
        return result, ddl_stats

    # =========================================================================
    # 스키마 결정
    # =========================================================================

    def _resolve_default_schema(self, directory: str) -> str:
        """파일 경로에서 기본 스키마를 결정합니다.
        
        우선순위:
        1. 경로의 폴더명 중 DDL 스키마와 일치하는 것 (깊은 폴더 우선)
        2. 매칭 실패 시 'public'
        """
        if not directory or not self._ddl_schemas:
            return "public"
        
        # 경로를 폴더 목록으로 분리 (깊은 순서대로)
        parts = directory.replace("\\", "/").split("/")
        parts = [p.lower() for p in parts if p]
        
        # 깊은 폴더부터 매칭 (역순 순회)
        for folder in reversed(parts):
            if folder in self._ddl_schemas:
                return folder
        
        return "public"

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
                    # 파일 경로 기반 기본 스키마 결정
                    default_schema = self._resolve_default_schema(ctx.directory)
                    
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
                        name_case=getattr(orchestrator, 'name_case', 'original'),
                    )
                    ctx.processor = processor
                    
                    # 정적 그래프 생성
                    queries = processor.build_static_graph_queries()
                    
                    if queries:
                        async with self._cypher_lock:
                            graph = await client.run_graph_query(queries)
                        
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
            completed += 1
            stats.files_completed = completed
            
            if result["type"] == "error":
                yield emit_message(f"   ❌ [{completed}/{total}] {result['file']}: {result['message'][:50]}")
                stats.mark_file_failed(result['file'], "Phase1 실패")
            else:
                stats.add_graph_result(result["graph"], is_static=True)
                
                graph = result["graph"]
                graph_msg = format_graph_result(graph)
                
                yield emit_message(f"   ✓ [{completed}/{total}] {result['file']}")
                if graph_msg:
                    for line in graph_msg.split("\n")[:3]:
                        yield emit_message(f"      {line}")
                
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
                        async with self._cypher_lock:
                            graph = await client.run_graph_query(analysis_queries)
                        
                        ctx.status = FileStatus.PH2_OK
                        await results_queue.put({
                            "type": "success",
                            "file": ctx.file_name,
                            "graph": graph,
                            "query_count": len(analysis_queries),
                            "failed_batches": failed_batch_count,
                            "failed_details": failed_details,  # 상세 정보 추가
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
            
            completed += 1
            
            if result_type == "error":
                yield emit_message(f"   ❌ [{completed}/{total}] {result['file']}: {result['message'][:50]}")
                stats.mark_file_failed(result['file'], "Phase2 실패")
            else:
                stats.llm_batches_executed += 1
                graph = result["graph"]
                stats.add_graph_result(graph, is_static=False)
                
                # 배치 실패 정보 표시
                failed_batches = result.get("failed_batches", 0)
                failed_details = result.get("failed_details", [])
                fail_info = f" (배치 {failed_batches}개 실패)" if failed_batches > 0 else ""
                
                graph_msg = format_graph_result(graph)
                yield emit_message(f"   ✓ [{completed}/{total}] {result['file']} (쿼리 {result['query_count']}개){fail_info}")
                if graph_msg:
                    for line in graph_msg.split("\n")[:3]:
                        yield emit_message(f"      {line}")
                
                # 실패 상세 정보 출력 (최대 3개)
                if failed_details:
                    stats.llm_batches_failed += len(failed_details)
                    for detail in failed_details[:3]:
                        yield emit_message(f"      ⚠️ 배치 #{detail['batch_id']} ({detail['node_ranges']}): {detail['error'][:50]}")
                
                yield emit_data(
                    graph=graph,
                    line_number=0,
                    analysis_progress=50 + int(completed / total * 50),
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
        """Phase 4: 테이블/컬럼 벡터라이징
        
        Neo4j에 저장된 테이블/컬럼의 description을 기반으로 임베딩 생성
        """
        from openai import AsyncOpenAI
        
        # OpenAI 클라이언트 초기화
        api_key = orchestrator.api_key or settings.openai_api_key
        if not api_key:
            yield emit_message("   ⚠️ OpenAI API 키가 없어 벡터라이징을 건너뜁니다")
            return
        
        openai_client = AsyncOpenAI(api_key=api_key)
        embedding_client = EmbeddingClient(openai_client)
        
        # 테이블 벡터라이징
        yield emit_message("   📊 테이블 벡터라이징 중...")
        
        table_query = """
        MATCH (t:Table)
        WHERE (t.vector IS NULL OR size(t.vector) = 0)
          AND (t.description IS NOT NULL OR t.analyzed_description IS NOT NULL)
        RETURN elementId(t) AS tid, 
               t.name AS name,
               t.schema AS schema,
               coalesce(t.description, t.analyzed_description, '') AS description
        ORDER BY t.schema, t.name
        """
        
        try:
            async with self._cypher_lock:
                result = await client.execute_queries([table_query])
            
            tables = result[0] if result and result[0] else []
            
            for item in tables:
                description = item.get("description", "") or ""
                if not description:
                    continue
                
                text = embedding_client.format_table_text(
                    table_name=item.get("name", ""),
                    description=description
                )
                vector = await embedding_client.embed_text(text)
                
                if vector:
                    set_query = f"""
                    MATCH (t)
                    WHERE elementId(t) = '{item['tid']}'
                    SET t.vector = {vector}
                    """
                    async with self._cypher_lock:
                        await client.execute_queries([set_query])
                    stats.tables_vectorized += 1
            
            yield emit_message(f"   ✅ 테이블 {stats.tables_vectorized}개 벡터라이징 완료")
            
        except Exception as e:
            yield emit_message(f"   ⚠️ 테이블 벡터라이징 실패: {str(e)[:100]}")
        
        # 컬럼 벡터라이징
        yield emit_message("   📊 컬럼 벡터라이징 중...")
        
        column_query = """
        MATCH (t:Table)-[:HAS_COLUMN]->(c:Column)
        WHERE (c.vector IS NULL OR size(c.vector) = 0)
          AND c.description IS NOT NULL AND c.description <> ''
        RETURN elementId(c) AS cid,
               c.name AS column_name,
               t.name AS table_name,
               coalesce(c.dtype, '') AS dtype,
               c.description AS description
        ORDER BY t.schema, t.name, c.name
        """
        
        try:
            async with self._cypher_lock:
                result = await client.execute_queries([column_query])
            
            columns = result[0] if result and result[0] else []
            
            # 배치 처리
            batch_size = 50
            for i in range(0, len(columns), batch_size):
                batch = columns[i:i + batch_size]
                texts = []
                
                for item in batch:
                    text = embedding_client.format_column_text(
                        column_name=item.get("column_name", ""),
                        table_name=item.get("table_name", ""),
                        dtype=item.get("dtype", ""),
                        description=item.get("description", "")
                    )
                    texts.append(text)
                
                vectors = await embedding_client.embed_batch(texts)
                
                for item, vector in zip(batch, vectors):
                    if vector:
                        set_query = f"""
                        MATCH (c)
                        WHERE elementId(c) = '{item['cid']}'
                        SET c.vector = {vector}
                        """
                        async with self._cypher_lock:
                            await client.execute_queries([set_query])
                        stats.columns_vectorized += 1
                
                yield emit_message(f"   ... 컬럼 {min(i + batch_size, len(columns))}/{len(columns)} 처리 중")
            
            yield emit_message(f"   ✅ 컬럼 {stats.columns_vectorized}개 벡터라이징 완료")
            
        except Exception as e:
            yield emit_message(f"   ⚠️ 컬럼 벡터라이징 실패: {str(e)[:100]}")
