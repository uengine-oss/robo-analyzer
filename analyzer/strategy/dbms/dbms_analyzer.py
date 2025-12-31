"""DBMS 코드 분석 전략 - PL/SQL, 프로시저, 함수 등

AST 기반 PL/SQL 코드 분석 → Neo4j 그래프 생성.

분석 흐름 (이중 병렬):
1. [Phase 0] DDL 처리 (테이블/컬럼 스키마)
2. [Phase 1] 파일별 병렬(5개)로 AST 그래프 생성
3. [Phase 2] 파일별 병렬(5개) + 청크별 병렬로 LLM 분석
4. [Phase 3] 프로시저 요약 및 User Story 생성
"""

import asyncio
import json
import logging
import os
from typing import Any, AsyncGenerator

import aiofiles

from analyzer.neo4j_client import Neo4jClient
from analyzer.parallel_executor import AnalysisTask, ParallelExecutor, ChunkBatcher
from analyzer.strategy.base_analyzer import AnalyzerStrategy
from analyzer.strategy.dbms.ast_processor import DbmsAstProcessor
from config.settings import settings
from util.rule_loader import RuleLoader
from util.stream_utils import (
    emit_data,
    emit_error,
    emit_message,
    format_graph_result,
)
from util.utility_tool import (
    escape_for_cypher,
    parse_table_identifier,
    generate_user_story_document,
)


class DbmsAnalyzer(AnalyzerStrategy):
    """DBMS 코드 분석 전략
    
    프로시저/함수 분석용 그래프 구축:
    - PROCEDURE, FUNCTION, TRIGGER 노드
    - Table, Column 노드
    - FROM, WRITES, CALL 관계
    - Variable 노드
    """

    async def analyze(
        self,
        file_names: list[tuple[str, str]],
        orchestrator: Any,
        **kwargs,
    ) -> AsyncGenerator[bytes, None]:
        """파일 목록을 분석하여 결과를 스트리밍합니다."""
        client = Neo4jClient()
        event_queue_from = asyncio.Queue()
        event_queue_to = asyncio.Queue()
        total_files = len(file_names)

        try:
            yield emit_message("🚀 DBMS 코드 분석을 시작합니다")
            yield emit_message(f"📦 프로젝트: {orchestrator.project_name}")
            yield emit_message(f"📊 분석 대상: {total_files}개 SQL 파일")
            
            await client.ensure_constraints()
            yield emit_message("🔌 Neo4j 데이터베이스 연결 완료")

            # 기존 분석 결과 확인
            if await client.check_nodes_exist(orchestrator.user_id, file_names):
                yield emit_message("🔄 이전 분석 결과 발견 → 증분 업데이트 모드")
            else:
                yield emit_message("🆕 새로운 분석 시작")

            # ========== DDL 처리 ==========
            ddl_files = self._list_ddl_files(orchestrator)
            if ddl_files:
                ddl_count = len(ddl_files)
                yield emit_message("")
                yield emit_message("━" * 42)
                yield emit_message(f"📋 [1단계] 테이블 스키마 수집 ({ddl_count}개 DDL)")
                yield emit_message("━" * 42)
                
                ddl_dir = orchestrator.dirs["ddl"]
                total_tables = 0
                total_columns = 0
                total_fks = 0
                
                for idx, ddl_file in enumerate(ddl_files, 1):
                    yield emit_message("")
                    yield emit_message(f"📄 [{idx}/{ddl_count}] {ddl_file}")
                    
                    ddl_graph, stats = await self._process_ddl(
                        ddl_path=os.path.join(ddl_dir, ddl_file),
                        client=client,
                        file_name=ddl_file,
                        orchestrator=orchestrator,
                    )
                    
                    if stats["tables"]:
                        yield emit_message(f"   ✓ Table 노드: {stats['tables']}개")
                        total_tables += stats["tables"]
                    if stats["columns"]:
                        yield emit_message(f"   ✓ Column 노드: {stats['columns']}개")
                        total_columns += stats["columns"]
                    if stats["fks"]:
                        yield emit_message(f"   ✓ FK 관계: {stats['fks']}개")
                        total_fks += stats["fks"]
                    
                    if ddl_graph and (ddl_graph.get("Nodes") or ddl_graph.get("Relationships")):
                        yield emit_data(
                            graph=ddl_graph,
                            line_number=0,
                            analysis_progress=0,
                            current_file=f"DDL-{ddl_file}",
                        )
                
                yield emit_message("")
                yield emit_message("📊 DDL 처리 완료:")
                yield emit_message(f"   • 테이블: {total_tables}개")
                yield emit_message(f"   • 컬럼: {total_columns}개")
                yield emit_message(f"   • FK: {total_fks}개")
            else:
                yield emit_message("ℹ️ DDL 파일 없음 → 스키마 처리 건너뜀")

            # ========== 소스 파일 분석 ==========
            yield emit_message("")
            yield emit_message("━" * 42)
            yield emit_message(f"🔍 [2단계] 프로시저/함수 분석 ({total_files}개 파일)")
            yield emit_message("━" * 42)

            for file_idx, (directory, file_name) in enumerate(file_names, 1):
                yield emit_message("")
                yield emit_message(f"📄 [{file_idx}/{total_files}] {file_name}")
                if directory:
                    yield emit_message(f"   📁 디렉토리: {directory}")
                
                async for chunk in self._analyze_file(
                    directory, file_name, file_names, client,
                    event_queue_from, event_queue_to, orchestrator,
                ):
                    yield chunk

            # ========== User Story 생성 ==========
            yield emit_message("")
            yield emit_message("━" * 42)
            yield emit_message("📝 [3단계] User Story 문서 생성")
            yield emit_message("━" * 42)
            
            user_story_doc = await self._create_user_story_doc(client, orchestrator)
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
                yield emit_message("   ℹ️ 추출할 User Story 없음")
            
            yield emit_message("")
            yield emit_message("━" * 42)
            yield emit_message("✅ 모든 분석이 완료되었습니다!")
            yield emit_message("━" * 42)
            
        finally:
            await client.close()

    def _get_rule_loader(self) -> RuleLoader:
        """DBMS 규칙 로더 반환"""
        return RuleLoader(target_lang="dbms")

    def _list_ddl_files(self, orchestrator: Any) -> list[str]:
        """DDL 파일 목록 조회"""
        ddl_dir = orchestrator.dirs.get("ddl", "")
        if not ddl_dir:
            logging.debug("[ANALYZE] DDL 디렉토리 설정 없음 - 건너뜀")
            return []
        if not os.path.isdir(ddl_dir):
            logging.debug("[ANALYZE] DDL 디렉토리 없음: %s - 건너뜀", ddl_dir)
            return []
        try:
            files = sorted(
                f for f in os.listdir(ddl_dir)
                if os.path.isfile(os.path.join(ddl_dir, f))
            )
            logging.info("[ANALYZE] DDL 파일 발견: %d개", len(files))
            return files
        except OSError as e:
            logging.warning("[ANALYZE] DDL 디렉토리 읽기 실패: %s | error=%s", ddl_dir, e)
            return []

    async def _load_file_assets(
        self,
        orchestrator: Any,
        directory: str,
        file_name: str,
    ) -> tuple[dict, list[str]]:
        """소스 파일과 AST JSON 로드"""
        src_path = os.path.join(orchestrator.dirs["src"], directory, file_name)
        base_name = os.path.splitext(file_name)[0]
        ast_path = os.path.join(orchestrator.dirs["analysis"], directory, f"{base_name}.json")

        async with aiofiles.open(ast_path, "r", encoding="utf-8") as ast_file, \
                   aiofiles.open(src_path, "r", encoding="utf-8") as src_file:
            ast_data, source_lines = await asyncio.gather(
                ast_file.read(),
                src_file.readlines(),
            )
            return json.loads(ast_data), source_lines

    async def _process_ddl(
        self,
        ddl_path: str,
        client: Neo4jClient,
        file_name: str,
        orchestrator: Any,
    ) -> tuple[dict, dict]:
        """DDL 파일 처리 및 테이블/컬럼 노드 생성"""
        stats = {"tables": 0, "columns": 0, "fks": 0}
        
        async with aiofiles.open(ddl_path, "r", encoding="utf-8") as f:
            ddl_content = await f.read()
        
        loader = self._get_rule_loader()
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

            # Table 노드 생성
            merge_key = {**common, "schema": schema, "name": parsed_name}
            merge_str = ", ".join(f"`{k}`: '{v}'" for k, v in merge_key.items())
            
            detail_lines = [f"설명: {comment}" if comment else "설명: ", "", "주요 컬럼:"]
            for col in columns:
                col_name = (col.get("name") or "").strip()
                if not col_name:
                    continue
                col_comment = (col.get("comment") or "").strip()
                detail_lines.append(f"   {col_name}: {col_comment}" if col_comment else f"   {col_name}: ")
            
            set_props = {
                **common,
                "description": escape_for_cypher(comment),
                "table_type": table_type,
                "detailDescription": escape_for_cypher("\n".join(detail_lines)),
            }
            set_str = ", ".join(f"t.`{k}` = '{v}'" for k, v in set_props.items())
            queries.append(f"MERGE (t:Table {{{merge_str}}}) SET {set_str} RETURN t")
            stats["tables"] += 1

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
                stats["columns"] += 1

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
                stats["fks"] += 1

        result = await client.run_graph_query(queries)
        logging.info("DDL 처리 완료: %s (T:%d, C:%d, FK:%d)", 
                    file_name, stats["tables"], stats["columns"], stats["fks"])
        return result, stats

    async def _analyze_file(
        self,
        directory: str,
        file_name: str,
        all_files: list[tuple[str, str]],
        client: Neo4jClient,
        event_queue_from: asyncio.Queue,
        event_queue_to: asyncio.Queue,
        orchestrator: Any,
    ) -> AsyncGenerator[bytes, None]:
        """단일 파일 분석"""
        current_file = f"{directory}/{file_name}" if directory else file_name

        ast_data, source_lines = await self._load_file_assets(
            orchestrator, directory, file_name
        )
        last_line = len(source_lines)
        source_raw = "".join(source_lines)

        analyzer = DbmsAstProcessor(
            antlr_data=ast_data,
            file_content=source_raw,
            send_queue=event_queue_from,
            receive_queue=event_queue_to,
            last_line=last_line,
            directory=directory,
            file_name=file_name,
            user_id=orchestrator.user_id,
            api_key=orchestrator.api_key,
            locale=orchestrator.locale,
            dbms=orchestrator.target,
            project_name=orchestrator.project_name,
        )
        analysis_task = asyncio.create_task(analyzer.run())

        analyzed_blocks = 0
        static_blocks = 0
        total_llm_batches = 0
        total_nodes = 0
        total_rels = 0

        while True:
            event = await event_queue_from.get()
            event_type = event.get("type")

            if event_type == "end_analysis":
                yield emit_message(f"   ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━")
                yield emit_message(f"   📊 파일 분석 완료: {file_name}")
                yield emit_message(f"      • 정적 블록: {static_blocks}개")
                yield emit_message(f"      • AI 분석 블록: {analyzed_blocks}개")
                yield emit_message(f"      • 생성된 노드: {total_nodes}개")
                yield emit_message(f"      • 생성된 관계: {total_rels}개")
                
                yield emit_data(
                    graph={"Nodes": [], "Relationships": []},
                    line_number=last_line,
                    analysis_progress=100,
                    current_file=current_file,
                )
                break

            if event_type == "error":
                error_msg = event.get("message", f"분석 실패: {file_name}")
                logging.error("분석 실패: %s - %s", file_name, error_msg)
                yield emit_message(f"   ❌ 오류 발생: {error_msg}")
                yield emit_error(error_msg)
                return

            next_line = event.get("line_number", 0)
            progress = self.calc_progress(next_line, last_line)

            if event_type == "static_graph":
                static_blocks += 1
                queries = event.get("query_data", [])
                graph = await client.run_graph_query(queries)
                
                total_nodes += len(graph.get("Nodes", []))
                total_rels += len(graph.get("Relationships", []))
                
                if static_blocks == 1:
                    yield emit_message("   🏗️ [Phase 1] 코드 구조 생성 중...")
                
                node_info = event.get("node_info", {})
                if node_info:
                    yield emit_message(
                        f"      → {node_info.get('type', 'Unknown')} 노드: "
                        f"{node_info.get('name', '')} (Line {node_info.get('start_line', 0)})"
                    )
                
                yield emit_data(
                    graph=graph,
                    line_number=next_line,
                    analysis_progress=progress,
                    current_file=current_file,
                )
                await event_queue_to.put({"type": "process_completed"})
                continue

            if event_type == "static_complete":
                yield emit_message(f"   ✓ Phase 1 완료: 구조 노드 {static_blocks}개 생성")
                await event_queue_to.put({"type": "process_completed"})
                continue

            if event_type == "llm_start":
                total_llm_batches = event.get("total_batches", 0)
                yield emit_message(f"   🤖 [Phase 2] AI 분석 시작 ({total_llm_batches}개 블록)")
                await event_queue_to.put({"type": "process_completed"})
                continue

            if event_type == "analysis_code":
                analyzed_blocks += 1
                queries = event.get("query_data", [])
                graph = await client.run_graph_query(queries)
                
                total_nodes += len(graph.get("Nodes", []))
                total_rels += len(graph.get("Relationships", []))
                
                # 결과 메시지화
                graph_msg = format_graph_result(graph)
                if graph_msg:
                    yield emit_message(f"      [{analyzed_blocks}/{total_llm_batches}] 분석 완료")
                    for line in graph_msg.split("\n"):
                        yield emit_message(f"      {line}")
                
                yield emit_data(
                    graph=graph,
                    line_number=next_line,
                    analysis_progress=progress,
                    current_file=current_file,
                )
                await event_queue_to.put({"type": "process_completed"})

        await analysis_task

    async def _create_user_story_doc(
        self,
        client: Neo4jClient,
        orchestrator: Any,
    ) -> str:
        """분석된 프로시저에서 User Story 문서 생성"""
        try:
            query = f"""
                MATCH (n)
                WHERE (n:PROCEDURE OR n:FUNCTION OR n:TRIGGER)
                  AND n.user_id = '{escape_for_cypher(orchestrator.user_id)}'
                  AND n.project_name = '{escape_for_cypher(orchestrator.project_name)}'
                  AND n.summary IS NOT NULL
                OPTIONAL MATCH (n)-[:HAS_USER_STORY]->(us:UserStory)
                OPTIONAL MATCH (us)-[:HAS_AC]->(ac:AcceptanceCriteria)
                WITH n, 
                     collect(DISTINCT {{
                         id: us.id,
                         role: us.role,
                         goal: us.goal,
                         benefit: us.benefit,
                         acceptance_criteria: collect(DISTINCT {{
                             id: ac.id,
                             title: ac.title,
                             given: ac.given,
                             when: ac.when,
                             then: ac.then
                         }})
                     }}) AS user_stories
                RETURN n.procedure_name AS name, 
                       n.summary AS summary,
                       user_stories AS user_stories, 
                       labels(n)[0] AS type
                ORDER BY n.file_name, n.startLine
            """
            
            results = await client.execute_queries([query])
            
            if not results or not results[0]:
                logging.info("[ANALYZE] User Story 생성 대상 없음 (쿼리 결과 없음)")
                return ""
            
            filtered = [
                r for r in results[0]
                if r.get("summary") or (r.get("user_stories") and len(r["user_stories"]) > 0)
            ]
            
            if not filtered:
                logging.info("[ANALYZE] User Story 생성 대상 없음 (요약 없는 프로시저만 존재)")
                return ""
            
            logging.info("[ANALYZE] User Story 생성 | 대상=%d개 프로시저", len(filtered))
            return generate_user_story_document(
                results=filtered,
                source_name=orchestrator.project_name,
                source_type="DBMS 프로시저/함수",
            )
            
        except Exception as exc:
            # User Story 생성 실패는 전체 분석을 중단하지 않음 (부분 실패 허용)
            logging.error("[ANALYZE] User Story 문서 생성 실패 | error=%s", exc, exc_info=True)
            return ""

