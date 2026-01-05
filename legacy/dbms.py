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
from dataclasses import dataclass, field
from enum import Enum
from typing import Any, AsyncGenerator, Optional, List, Dict, Tuple

import aiofiles

from analyzer.neo4j_client import Neo4jClient
from analyzer.strategy.base_analyzer import BaseStreamingAnalyzer, AnalysisStats
from analyzer.strategy.dbms.ast_processor import DbmsAstProcessor
from config.settings import settings
from util.exception import AnalysisError
from util.rule_loader import RuleLoader
from util.stream_utils import (
    emit_data,
    emit_message,
    format_graph_result,
)
from util.utility_tool import (
    escape_for_cypher,
    log_process,
    parse_table_identifier,
    generate_user_story_document,
)


class FileStatus(Enum):
    """파일 분석 상태"""
    PENDING = "PENDING"
    PH1_OK = "PH1_OK"
    PH1_FAIL = "PH1_FAIL"
    PH2_OK = "PH2_OK"
    PH2_FAIL = "PH2_FAIL"
    SKIPPED = "SKIPPED"


@dataclass
class FileAnalysisContext:
    """파일 분석 컨텍스트"""
    directory: str
    file_name: str
    ast_data: dict
    source_lines: List[str]
    processor: Optional[DbmsAstProcessor] = None
    status: FileStatus = field(default=FileStatus.PENDING)
    error_message: str = ""


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

        yield emit_message(f"⚡ 병렬 처리: 파일 {settings.concurrency.file_concurrency}개 동시")

        # ========== DDL 처리 ==========
        async for chunk in self._run_ddl_phase(client, orchestrator, stats):
            yield chunk

        # ========== 파일 로드 ==========
        yield emit_message("")
        yield self.emit_separator()
        yield self.emit_phase_header(1, "🏗️ AST 구조 그래프 생성", f"{total_files}개 파일 병렬")
        yield self.emit_separator()

        contexts = await self._load_all_files(file_names, orchestrator)
        yield emit_message(f"   ✓ {len(contexts)}개 파일 로드 완료")

        # ========== Phase 1: AST 그래프 생성 (병렬) ==========
        async for chunk in self._run_phase1(contexts, client, orchestrator, stats):
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

        async for chunk in self._run_phase2(ph2_targets, client, orchestrator, stats):
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
        """분석된 프로시저에서 User Story 문서 생성"""
        query = f"""
            MATCH (n)
            WHERE (n:PROCEDURE OR n:FUNCTION OR n:TRIGGER)
              AND n.user_id = '{escape_for_cypher(orchestrator.user_id)}'
              AND n.project_name = '{escape_for_cypher(orchestrator.project_name)}'
              AND n.summary IS NOT NULL
            OPTIONAL MATCH (n)-[:HAS_USER_STORY]->(us:UserStory)
            OPTIONAL MATCH (us)-[:HAS_AC]->(ac:AcceptanceCriteria)
            WITH n, us, collect(DISTINCT {{
                id: ac.id,
                title: ac.title,
                given: ac.given,
                when: ac.when,
                then: ac.then
            }}) AS acceptance_criteria
            WITH n, collect(DISTINCT {{
                id: us.id,
                role: us.role,
                goal: us.goal,
                benefit: us.benefit,
                acceptance_criteria: acceptance_criteria
            }}) AS user_stories
            RETURN n.procedure_name AS name, 
                   n.summary AS summary,
                   user_stories AS user_stories, 
                   labels(n)[0] AS type
            ORDER BY n.file_name, n.startLine
        """
        
        async with self._cypher_lock:
            results = await client.execute_queries([query])
        
        if not results or not results[0]:
            raise AnalysisError("User Story 생성을 위한 분석 결과가 없습니다")
        
        filtered = [
            r for r in results[0]
            if r.get("summary") or (r.get("user_stories") and len(r["user_stories"]) > 0)
        ]
        
        if not filtered:
            return None
        
        log_process("ANALYZE", "USER_STORY", f"User Story 생성 | 대상={len(filtered)}개 프로시저")
        return generate_user_story_document(
            results=filtered,
            source_name=orchestrator.project_name,
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
        """DDL 파일 목록 조회"""
        ddl_dir = orchestrator.dirs.get("ddl", "")
        if not ddl_dir:
            log_process("ANALYZE", "DDL", "DDL 디렉토리 설정 없음 - DDL 처리 생략")
            return []
        if not os.path.isdir(ddl_dir):
            raise AnalysisError(f"DDL 디렉토리가 존재하지 않습니다: {ddl_dir}")
        try:
            files = sorted(
                f for f in os.listdir(ddl_dir)
                if os.path.isfile(os.path.join(ddl_dir, f))
            )
            if not files:
                raise AnalysisError(f"DDL 디렉토리에 파일이 없습니다: {ddl_dir}")
            log_process("ANALYZE", "DDL", f"DDL 파일 발견: {len(files)}개")
            return files
        except OSError as e:
            raise AnalysisError(f"DDL 디렉토리 읽기 실패: {ddl_dir}") from e

    async def _process_ddl(
        self,
        ddl_path: str,
        client: Neo4jClient,
        file_name: str,
        orchestrator: Any,
    ) -> tuple[dict, dict]:
        """DDL 파일 처리 및 테이블/컬럼 노드 생성"""
        ddl_stats = {"tables": 0, "columns": 0, "fks": 0}
        
        async with aiofiles.open(ddl_path, "r", encoding="utf-8") as f:
            ddl_content = await f.read()
        
        loader = RuleLoader(target_lang="dbms")
        parsed = loader.execute(
            "ddl",
            {"ddl_content": ddl_content, "locale": orchestrator.locale},
            orchestrator.api_key,
        )
        
        queries = []
        common = {
            "user_id": orchestrator.user_id,
            "db": orchestrator.target,
            "project_name": orchestrator.project_name,
        }

        for table_info in parsed.get("analysis", []):
            table = table_info.get("table", {})
            columns = table_info.get("columns", [])
            foreign_keys = table_info.get("foreignKeys", [])
            primary_keys = [
                str(pk).strip().upper()
                for pk in (table_info.get("primaryKeys") or [])
                if pk
            ]

            schema_raw = (table.get("schema") or "").strip()
            table_name = (table.get("name") or "").strip()
            comment = (table.get("comment") or "").strip()
            table_type = (table.get("table_type") or "BASE TABLE").strip().upper()
            
            qualified = f"{schema_raw}.{table_name}" if schema_raw else table_name
            parsed_schema, parsed_name, _ = parse_table_identifier(qualified)
            schema = parsed_schema or ""
            
            # DDL에서 발견된 스키마 수집
            if schema:
                self._ddl_schemas.add(schema.lower())

            # Table 노드 생성
            merge_key = {**common, "schema": schema, "name": parsed_name}
            merge_str = ", ".join(f"`{k}`: '{v}'" for k, v in merge_key.items())
            
            column_metadata = {}
            for col in columns:
                col_name = (col.get("name") or "").strip()
                if not col_name:
                    continue
                col_comment = (col.get("comment") or "").strip()
                column_metadata[col_name] = {
                    "description": col_comment,
                    "dtype": (col.get("dtype") or col.get("type") or "").strip(),
                    "nullable": col.get("nullable", True),
                }
            
            set_props = {
                **common,
                "description": escape_for_cypher(comment),
                "table_type": table_type,
            }
            set_str = ", ".join(f"t.`{k}` = '{v}'" for k, v in set_props.items())
            queries.append(f"MERGE (t:Table {{{merge_str}}}) SET {set_str} RETURN t")
            ddl_stats["tables"] += 1
            
            # DDL 메타데이터 캐시 저장 (메모리)
            table_key = (schema.lower(), parsed_name.lower())
            self._ddl_table_metadata[table_key] = {
                "description": comment,
                "columns": column_metadata,
            }

            # Column 노드 생성
            for col in columns:
                col_name = (col.get("name") or "").strip()
                if not col_name:
                    continue
                
                col_type = (col.get("dtype") or col.get("type") or "").strip()
                col_nullable = col.get("nullable", True)
                col_comment = (col.get("comment") or "").strip()
                fqn = ".".join(filter(None, [schema, parsed_name, col_name])).lower()

                col_merge = {"user_id": orchestrator.user_id, "fqn": fqn, "project_name": orchestrator.project_name}
                col_merge_str = ", ".join(f"`{k}`: '{v}'" for k, v in col_merge.items())
                col_set = {
                    "name": escape_for_cypher(col_name),
                    "dtype": escape_for_cypher(col_type),
                    "description": escape_for_cypher(col_comment),
                    "nullable": "true" if col_nullable else "false",
                    "project_name": orchestrator.project_name,
                    "fqn": fqn,
                }
                if col_name.upper() in primary_keys:
                    col_set["pk_constraint"] = f"{parsed_name}_pkey"
                
                col_set_str = ", ".join(f"c.`{k}` = '{v}'" for k, v in col_set.items())
                queries.append(f"MERGE (c:Column {{{col_merge_str}}}) SET {col_set_str} RETURN c")
                queries.append(
                    f"MATCH (t:Table {{{merge_str}}})\n"
                    f"MATCH (c:Column {{{col_merge_str}}})\n"
                    f"MERGE (t)-[r:HAS_COLUMN]->(c) RETURN t, r, c"
                )
                ddl_stats["columns"] += 1

            # FK 관계 생성
            for fk in foreign_keys:
                src_col = (fk.get("column") or "").strip()
                ref = (fk.get("ref") or "").strip()
                if not src_col or not ref or "." not in ref:
                    continue

                ref_table_part, ref_col = ref.rsplit(".", 1)
                ref_schema, ref_table, _ = parse_table_identifier(ref_table_part)
                ref_schema = ref_schema or schema

                ref_table_merge = {**common, "schema": ref_schema or "", "name": ref_table or ""}
                ref_merge_str = ", ".join(f"`{k}`: '{v}'" for k, v in ref_table_merge.items())
                queries.append(f"MERGE (rt:Table {{{ref_merge_str}}}) RETURN rt")
                queries.append(
                    f"MATCH (t:Table {{{merge_str}}})\n"
                    f"MATCH (rt:Table {{{ref_merge_str}}})\n"
                    f"MERGE (t)-[r:FK_TO_TABLE]->(rt) RETURN t, r, rt"
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
                        user_id=orchestrator.user_id,
                        api_key=orchestrator.api_key,
                        locale=orchestrator.locale,
                        dbms=orchestrator.target,
                        project_name=orchestrator.project_name,
                        last_line=len(ctx.source_lines),
                        default_schema=default_schema,
                        ddl_table_metadata=self._ddl_table_metadata,
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
                    
                    # 배치 실패가 있으면 경고 표시
                    if failed_batch_count > 0:
                        await results_queue.put({
                            "type": "warning",
                            "file": ctx.file_name,
                            "message": f"{failed_batch_count}개 배치 실패 (부분 성공)",
                        })
                        
                except Exception as e:
                    log_process("ANALYZE", "ERROR", f"Phase 2 오류 ({ctx.file_name}): {e}", logging.ERROR, e)
                    ctx.status = FileStatus.PH2_FAIL
                    ctx.error_message = str(e)[:100]
                    await results_queue.put({
                        "type": "error",
                        "file": ctx.file_name,
                        "message": str(e),
                    })

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