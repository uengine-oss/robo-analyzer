"""DBMS 코드 분석 전략 - PL/SQL, 프로시저, 함수 등"""

import asyncio
import json
import logging
import os
from typing import AsyncGenerator, Any

import aiofiles

from understand.neo4j_connection import Neo4jConnection
from understand.strategy.base_strategy import UnderstandStrategy
from understand.strategy.dbms.analysis import Analyzer
from util.rule_loader import RuleLoader
from util.utility_tool import (
    emit_message,
    emit_data,
    emit_error,
    escape_for_cypher,
    parse_table_identifier,
    parse_json_maybe,
    generate_user_story_document,
    aggregate_user_stories_from_results,
)


class DbmsUnderstandStrategy(UnderstandStrategy):
    """DBMS 이해 전략: DDL 처리 → Analyzer 실행 → 후처리."""

    @staticmethod
    def _calculate_progress(current_line: int, total_lines: int) -> int:
        """현재 진행률을 계산합니다 (0-99%)."""
        return min(int((current_line / total_lines) * 100), 99) if current_line > 0 else 0

    @staticmethod
    def _describe_graph_result(graph: dict) -> str:
        """그래프 결과를 사람이 읽기 쉬운 문자열로 변환합니다."""
        nodes = graph.get("Nodes", [])
        rels = graph.get("Relationships", [])
        
        if not nodes and not rels:
            return ""
        
        # 노드 타입별 집계
        node_types = {}
        for node in nodes:
            labels = node.get("labels", [])
            label = labels[0] if labels else "Unknown"
            node_types[label] = node_types.get(label, 0) + 1
        
        # 관계 타입별 집계
        rel_types = {}
        for rel in rels:
            rel_type = rel.get("type", "Unknown")
            rel_types[rel_type] = rel_types.get(rel_type, 0) + 1
        
        parts = []
        if node_types:
            node_desc = ", ".join(f"{t}({c})" for t, c in node_types.items())
            parts.append(f"노드: {node_desc}")
        if rel_types:
            rel_desc = ", ".join(f"{t}({c})" for t, c in rel_types.items())
            parts.append(f"관계: {rel_desc}")
        
        return " | ".join(parts)

    async def understand(self, file_names: list, orchestrator: Any, **kwargs) -> AsyncGenerator[bytes, None]:
        connection = Neo4jConnection()
        events_from_analyzer = asyncio.Queue()
        events_to_analyzer = asyncio.Queue()

        total_files = len(file_names)

        try:
            yield emit_message("🚀 DBMS 코드 분석을 시작합니다")
            yield emit_message(f"📦 프로젝트: {orchestrator.project_name}")
            yield emit_message(f"📊 분석 대상: {total_files}개 SQL 파일")
            
            await connection.ensure_constraints()
            yield emit_message("🔌 Neo4j 데이터베이스 연결 완료")

            # 기존 분석 결과 확인
            if await connection.node_exists(orchestrator.user_id, file_names):
                yield emit_message("🔄 이전 분석 결과 발견 → 증분 업데이트 모드")
            else:
                yield emit_message("🆕 새로운 분석 시작")

            # ========== DDL 처리 ==========
            ddl_files = self._list_ddl_files(orchestrator)
            if ddl_files:
                ddl_count = len(ddl_files)
                yield emit_message(f"")
                yield emit_message(f"━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━")
                yield emit_message(f"📋 [1단계] 테이블 스키마 수집 ({ddl_count}개 DDL)")
                yield emit_message(f"━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━")
                
                ddl_dir = orchestrator.dirs["ddl"]
                total_tables = 0
                total_columns = 0
                total_fks = 0
                
                for idx, ddl_file_name in enumerate(ddl_files, 1):
                    yield emit_message(f"")
                    yield emit_message(f"📄 [{idx}/{ddl_count}] {ddl_file_name}")
                    
                    ddl_graph, stats = await self._process_ddl_with_stats(
                        ddl_file_path=os.path.join(ddl_dir, ddl_file_name),
                        connection=connection,
                        file_name=ddl_file_name,
                        orchestrator=orchestrator,
                    )
                    
                    # 상세 통계 출력
                    if stats["tables"]:
                        yield emit_message(f"   ✓ Table 노드 생성/업데이트: {stats['tables']}개")
                        total_tables += stats["tables"]
                    if stats["columns"]:
                        yield emit_message(f"   ✓ Column 노드 생성/업데이트: {stats['columns']}개")
                        total_columns += stats["columns"]
                    if stats["fks"]:
                        yield emit_message(f"   ✓ FK 관계 생성: {stats['fks']}개")
                        total_fks += stats["fks"]
                    
                    if ddl_graph and (ddl_graph.get("Nodes") or ddl_graph.get("Relationships")):
                        yield emit_data(graph=ddl_graph, line_number=0, analysis_progress=0, current_file=f"DDL-{ddl_file_name}")
                
                yield emit_message(f"")
                yield emit_message(f"📊 DDL 처리 완료 요약:")
                yield emit_message(f"   • 테이블: {total_tables}개")
                yield emit_message(f"   • 컬럼: {total_columns}개")
                yield emit_message(f"   • FK 관계: {total_fks}개")
            else:
                yield emit_message("ℹ️ DDL 파일 없음 → 스키마 처리 건너뜀")

            # ========== 소스 파일 분석 ==========
            yield emit_message(f"")
            yield emit_message(f"━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━")
            yield emit_message(f"🔍 [2단계] 프로시저/함수 코드 분석 ({total_files}개 파일)")
            yield emit_message(f"━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━")

            for file_idx, (directory, file_name) in enumerate(file_names, 1):
                yield emit_message(f"")
                yield emit_message(f"📄 [{file_idx}/{total_files}] {file_name}")
                if directory:
                    yield emit_message(f"   📁 디렉토리: {directory}")
                
                async for chunk in self._analyze_file(
                    directory,
                    file_name,
                    file_names,
                    connection,
                    events_from_analyzer,
                    events_to_analyzer,
                    orchestrator,
                ):
                    yield chunk

            # ========== User Story 생성 ==========
            yield emit_message(f"")
            yield emit_message(f"━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━")
            yield emit_message(f"📝 [3단계] User Story 문서 생성")
            yield emit_message(f"━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━")
            
            user_story_doc = await self._generate_user_story_document(connection, orchestrator, file_names)
            if user_story_doc:
                yield emit_data(
                    graph={"Nodes": [], "Relationships": []},
                    line_number=0,
                    analysis_progress=100,
                    current_file="user_stories.md",
                    user_story_document=user_story_doc,
                    event_type="user_story_document"
                )
                yield emit_message("   ✓ User Story 문서 생성 완료")
            else:
                yield emit_message("   ℹ️ 추출할 User Story 없음")
            
            yield emit_message(f"")
            yield emit_message(f"━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━")
            yield emit_message("✅ 모든 분석이 완료되었습니다!")
            yield emit_message(f"━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━")
        finally:
            await connection.close()

    def _rule_loader(self) -> RuleLoader:
        return RuleLoader(target_lang="dbms", domain="understand")

    def _list_ddl_files(self, orchestrator) -> list:
        try:
            ddl_dir = orchestrator.dirs["ddl"]
            return [f for f in sorted(os.listdir(ddl_dir)) if os.path.isfile(os.path.join(ddl_dir, f))]
        except Exception:
            return []

    async def _load_assets(self, orchestrator, directory: str, file_name: str) -> tuple:
        plsql_file_path = os.path.join(orchestrator.dirs["src"], directory, file_name)
        base_name = os.path.splitext(file_name)[0]
        analysis_file_path = os.path.join(orchestrator.dirs["analysis"], directory, f"{base_name}.json")

        async with aiofiles.open(analysis_file_path, "r", encoding="utf-8") as antlr_file, aiofiles.open(
            plsql_file_path, "r", encoding="utf-8"
        ) as plsql_file:
            antlr_data, plsql_content = await asyncio.gather(antlr_file.read(), plsql_file.readlines())
            return json.loads(antlr_data), plsql_content

    async def _process_ddl_with_stats(
        self,
        ddl_file_path: str,
        connection: Neo4jConnection,
        file_name: str,
        orchestrator,
    ) -> tuple[dict, dict]:
        """DDL 처리 및 통계 반환"""
        stats = {"tables": 0, "columns": 0, "fks": 0}
        
        async with aiofiles.open(ddl_file_path, "r", encoding="utf-8") as ddl_file:
            ddl_content = await ddl_file.read()
            loader = self._rule_loader()
            parsed = loader.execute(
                "ddl",
                {"ddl_content": ddl_content, "locale": orchestrator.locale},
                orchestrator.api_key,
            )
            cypher_queries = []

            common_props = {"user_id": orchestrator.user_id, "db": orchestrator.target, "project_name": orchestrator.project_name}

            for table in parsed["analysis"]:
                table_info = table["table"]
                columns = table.get("columns", [])
                foreign_list = table.get("foreignKeys", [])
                primary_list = [s for pk in (table.get("primaryKeys") or []) if (s := str(pk).strip().upper())]

                orig_schema, orig_table, table_comment, table_type = (
                    (table_info.get("schema") or "").strip(),
                    (table_info.get("name") or "").strip(),
                    (table_info.get("comment") or "").strip(),
                    (table_info.get("table_type") or "BASE TABLE").strip().upper(),
                )
                qualified_table = f"{orig_schema}.{orig_table}" if orig_schema else orig_table
                parsed_schema, parsed_table, _ = parse_table_identifier(qualified_table)
                effective_schema = parsed_schema or ""

                t_merge_key = {**common_props, "schema": effective_schema, "name": parsed_table}
                t_merge_str = ", ".join(f"`{k}`: '{v}'" for k, v in t_merge_key.items())
                lines = []
                summary_line = f"설명: {table_comment}" if table_comment else "설명: "
                lines.append(summary_line)
                lines.append("")
                lines.append("주요  컬럼:")
                for col in columns:
                    col_name_i = (col.get("name") or "").strip()
                    if not col_name_i:
                        continue
                    role = (col.get("comment") or "").strip()
                    lines.append(f"   {col_name_i}: {role}" if role else f"   {col_name_i}: ")
                detail_desc_text = "\n".join(lines)

                t_set_props = {
                    **common_props,
                    "description": escape_for_cypher(table_comment),
                    "table_type": table_type,
                    "detailDescription": escape_for_cypher(detail_desc_text),
                }
                t_set_str = ", ".join(f"t.`{k}` = '{v}'" for k, v in t_set_props.items())
                cypher_queries.append(f"MERGE (t:Table {{{t_merge_str}}}) SET {t_set_str} RETURN t")
                stats["tables"] += 1

                for col in columns:
                    if not (col_name := (col.get("name") or "").strip()):
                        continue

                    col_type = (col.get("dtype") or col.get("type") or "").strip()
                    col_nullable = col.get("nullable", True)
                    col_comment = (col.get("comment") or "").strip()
                    fqn = ".".join(filter(None, [effective_schema, parsed_table, col_name])).lower()

                    c_merge_key = {"user_id": orchestrator.user_id, "fqn": fqn, "project_name": orchestrator.project_name}
                    c_merge_str = ", ".join(f"`{k}`: '{v}'" for k, v in c_merge_key.items())
                    c_set_props = {
                        "name": escape_for_cypher(col_name),
                        "dtype": escape_for_cypher(col_type),
                        "description": escape_for_cypher(col_comment),
                        "nullable": "true" if col_nullable else "false",
                        "project_name": orchestrator.project_name,
                        "fqn": fqn,
                    }
                    if col_name.upper() in primary_list:
                        c_set_props["pk_constraint"] = f"{parsed_table}_pkey"

                    c_set_str = ", ".join(f"c.`{k}` = '{v}'" for k, v in c_set_props.items())
                    cypher_queries.append(f"MERGE (c:Column {{{c_merge_str}}}) SET {c_set_str} RETURN c")
                    cypher_queries.append(
                        f"MATCH (t:Table {{{t_merge_str}}})\nMATCH (c:Column {{{c_merge_str}}})\nMERGE (t)-[r:HAS_COLUMN]->(c) RETURN t, r, c"
                    )
                    stats["columns"] += 1

                for fk in foreign_list:
                    src_col = (fk.get("column") or "").strip()
                    ref = (fk.get("ref") or "").strip()
                    if not src_col or not ref or "." not in ref:
                        continue

                    table_qualifier, ref_column = ref.rsplit(".", 1)
                    ref_schema, ref_table, _ = parse_table_identifier(table_qualifier)
                    ref_schema = ref_schema or effective_schema

                    ref_table_merge_key = {**common_props, "schema": ref_schema or "", "name": ref_table or ""}
                    ref_table_merge_str = ", ".join(f"`{k}`: '{v}'" for k, v in ref_table_merge_key.items())
                    cypher_queries.append(f"MERGE (rt:Table {{{ref_table_merge_str}}}) RETURN rt")
                    cypher_queries.append(
                        f"MATCH (t:Table {{{t_merge_str}}})\nMATCH (rt:Table {{{ref_table_merge_str}}})\nMERGE (t)-[r:FK_TO_TABLE]->(rt) RETURN t, r, rt"
                    )

                    src_fqn = ".".join(filter(None, [effective_schema, parsed_table, src_col])).lower()
                    ref_fqn = ".".join(filter(None, [ref_schema or effective_schema, ref_table, ref_column])).lower()

                    src_c_key = {
                        "user_id": orchestrator.user_id,
                        "name": src_col,
                        "fqn": src_fqn,
                        "project_name": orchestrator.project_name,
                    }
                    ref_c_key = {
                        "user_id": orchestrator.user_id,
                        "name": ref_column,
                        "fqn": ref_fqn,
                        "project_name": orchestrator.project_name,
                    }
                    src_c_str = ", ".join(f"`{k}`: '{v}'" for k, v in src_c_key.items())
                    ref_c_str = ", ".join(f"`{k}`: '{v}'" for k, v in ref_c_key.items())

                    cypher_queries.append(f"MERGE (sc:Column {{{src_c_str}}}) RETURN sc")
                    cypher_queries.append(f"MERGE (dc:Column {{{ref_c_str}}}) RETURN dc")
                    cypher_queries.append(
                        f"MATCH (sc:Column {{{src_c_str}}})\nMATCH (dc:Column {{{ref_c_str}}})\nMERGE (sc)-[r:FK_TO]->(dc) RETURN sc, r, dc"
                    )
                    stats["fks"] += 1

            result = await connection.execute_query_and_return_graph(cypher_queries)
            logging.info("DDL 파일 처리 완료: %s (테이블: %d, 컬럼: %d, FK: %d)", 
                        file_name, stats["tables"], stats["columns"], stats["fks"])
            return result, stats

    async def _analyze_file(
        self,
        directory: str,
        file_name: str,
        file_pairs: list,
        connection: Neo4jConnection,
        events_from_analyzer: asyncio.Queue,
        events_to_analyzer: asyncio.Queue,
        orchestrator: Any,
    ) -> AsyncGenerator[bytes, None]:
        current_file = f"{directory}/{file_name}" if directory else file_name

        antlr_data, plsql_content = await self._load_assets(orchestrator, directory, file_name)
        last_line = len(plsql_content)
        plsql_raw = "".join(plsql_content)
        analyzer = Analyzer(
            antlr_data=antlr_data,
            file_content=plsql_raw,
            send_queue=events_from_analyzer,
            receive_queue=events_to_analyzer,
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
        total_nodes_created = 0
        total_rels_created = 0

        while True:
            event = await events_from_analyzer.get()
            event_type = event.get("type")

            # 분석 완료
            if event_type == "end_analysis":
                yield emit_message("   🔧 변수 타입 해석 중 (테이블 메타데이터 기반)...")
                postprocess_graph = await self._postprocess_file(connection, directory, file_name, file_pairs, orchestrator)
                
                # 후처리 결과 통계
                post_nodes = len(postprocess_graph.get("Nodes", []))
                if post_nodes:
                    yield emit_message(f"   ✓ Variable 노드 타입 해석 완료: {post_nodes}개")
                
                yield emit_data(graph=postprocess_graph, line_number=last_line, analysis_progress=100, current_file=current_file)
                
                # 파일 분석 완료 요약
                yield emit_message(f"   ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━")
                yield emit_message(f"   📊 파일 분석 완료: {file_name}")
                yield emit_message(f"      • 정적 구조 블록: {static_blocks}개")
                yield emit_message(f"      • AI 분석 블록: {analyzed_blocks}개")
                yield emit_message(f"      • 생성된 노드: {total_nodes_created}개")
                yield emit_message(f"      • 생성된 관계: {total_rels_created}개")
                break

            # 오류 발생
            if event_type == "error":
                error_message = event.get("message", f"Understanding failed for {file_name}")
                logging.error("Understanding Failed for %s: %s", file_name, error_message)
                yield emit_message(f"   ❌ 오류 발생: {error_message}")
                yield emit_error(error_message)
                return

            next_line = event.get("line_number", 0)
            progress = self._calculate_progress(next_line, last_line)

            # 정적 그래프 생성
            if event_type == "static_graph":
                static_blocks += 1
                query_data = event.get("query_data", [])
                graph_result = await connection.execute_query_and_return_graph(query_data)
                
                # 노드/관계 집계
                nodes_count = len(graph_result.get("Nodes", []))
                rels_count = len(graph_result.get("Relationships", []))
                total_nodes_created += nodes_count
                total_rels_created += rels_count
                
                # 첫 번째 블록일 때 단계 시작 메시지
                if static_blocks == 1:
                    yield emit_message("   🏗️ [Phase 1] 코드 구조 그래프 생성 중...")
                
                # 노드 타입별 상세 정보
                node_info = event.get("node_info", {})
                if node_info:
                    node_type = node_info.get("type", "Unknown")
                    node_name = node_info.get("name", "")
                    start_line = node_info.get("start_line", 0)
                    yield emit_message(f"      → {node_type} 노드 생성: {node_name} (Line {start_line})")
                
                yield emit_data(graph=graph_result, line_number=next_line, analysis_progress=progress, current_file=current_file)
                await events_to_analyzer.put({"type": "process_completed"})
                continue

            # 정적 그래프 완료
            if event_type == "static_complete":
                yield emit_message(f"   ✓ Phase 1 완료: 구조 노드 {static_blocks}개 생성")
                await events_to_analyzer.put({"type": "process_completed"})
                continue

            # LLM 분석 시작
            if event_type == "llm_start":
                total_llm_batches = event.get("total_batches", 0)
                yield emit_message(f"   🤖 [Phase 2] AI 분석 시작 ({total_llm_batches}개 블록)")
                await events_to_analyzer.put({"type": "process_completed"})
                continue

            # LLM 분석 진행
            if event_type == "analysis_code":
                analyzed_blocks += 1
                query_data = event.get("query_data", [])
                graph_result = await connection.execute_query_and_return_graph(query_data)
                
                # 노드/관계 집계
                nodes_count = len(graph_result.get("Nodes", []))
                rels_count = len(graph_result.get("Relationships", []))
                total_nodes_created += nodes_count
                total_rels_created += rels_count
                
                # 분석 상세 정보
                analysis_info = event.get("analysis_info", {})
                if analysis_info:
                    node_type = analysis_info.get("type", "")
                    node_name = analysis_info.get("name", "")
                    summary_preview = analysis_info.get("summary", "")[:50]
                    if summary_preview:
                        yield emit_message(f"      → [{analyzed_blocks}/{total_llm_batches}] {node_type} 분석: {node_name}")
                        yield emit_message(f"         요약: {summary_preview}...")
                else:
                    yield emit_message(f"      → [{analyzed_blocks}/{total_llm_batches}] 블록 분석 완료")
                
                yield emit_data(graph=graph_result, line_number=next_line, analysis_progress=progress, current_file=current_file)
                await events_to_analyzer.put({"type": "process_completed"})

        await analysis_task

    async def _postprocess_file(
        self,
        connection: Neo4jConnection,
        directory: str,
        file_name: str,
        file_pairs: list,
        orchestrator: Any,
    ) -> dict:
        """변수 타입을 테이블 메타데이터 기반으로 해결하는 후처리 단계."""
        directory_normalized = directory.replace('\\', '/') if directory else ''
        directory_esc, file_esc = escape_for_cypher(directory_normalized), escape_for_cypher(file_name)

        var_rows = (
            (
                await connection.execute_queries(
                    [
                        f"""
            MATCH (v:Variable {{directory: '{directory_esc}', file_name: '{file_esc}', user_id: '{orchestrator.user_id}'}})
            WITH v,
                trim(replace(replace(coalesce(v.value, ''), 'Table: ', ''), 'Table:', '')) AS valueAfterPrefix,
                coalesce(v.type, '') AS vtype
            WITH v, trim(replace(CASE WHEN vtype <> '' THEN vtype ELSE valueAfterPrefix END, ' ', '')) AS raw
            WITH v,
                CASE WHEN raw CONTAINS '.' THEN split(raw, '.')[0] ELSE '' END AS schemaName,
                CASE WHEN raw CONTAINS '.' THEN split(raw, '.')[1] ELSE raw END AS tableName
            MATCH (t:Table {{user_id: '{orchestrator.user_id}', name: toUpper(tableName)}})
            WHERE coalesce(t.schema, '') = coalesce(toUpper(schemaName), '')
            OPTIONAL MATCH (t)-[:HAS_COLUMN]->(c:Column {{user_id: '{orchestrator.user_id}'}})
            WITH v, coalesce(toUpper(schemaName), '') AS schema, toUpper(tableName) AS table,
                collect(DISTINCT {{name: c.name, dtype: coalesce(c.dtype, ''), nullable: toBoolean(c.nullable), comment: coalesce(c.description, '')}}) AS columns
            RETURN v.name AS varName, v.type AS declaredType, schema, table, columns
        """
                    ]
                )
            )[0]
            if connection
            else []
        )

        if not var_rows:
            return {"Nodes": [], "Relationships": []}

        loader = self._rule_loader()
        type_results = await asyncio.gather(
            *[
                loader.execute(
                    "variable_type_resolve",
                    {
                        "var_name": row["varName"],
                        "declared_type": row.get("declaredType"),
                        "table_schema": row["schema"],
                        "table_name": row["table"],
                        "columns_json": parse_json_maybe(row.get("columns")),
                        "locale": orchestrator.locale,
                    },
                    orchestrator.api_key,
                )
                for row in var_rows
            ]
        )

        user_id_esc = escape_for_cypher(orchestrator.user_id)
        update_queries = [
            f"MATCH (v:Variable {{name: '{escape_for_cypher(row['varName'])}', directory: '{directory_esc}', file_name: '{file_esc}', user_id: '{user_id_esc}'}}) "
            f"SET v.type = '{escape_for_cypher((result or {}).get('resolvedType') or row.get('declaredType'))}', v.resolved = true RETURN v"
            for row, result in zip(var_rows, type_results)
        ]

        if update_queries:
            return await connection.execute_query_and_return_graph(update_queries)

        return {"Nodes": [], "Relationships": []}

    async def _generate_user_story_document(
        self,
        connection: Neo4jConnection,
        orchestrator: Any,
        file_names: list,
    ) -> str:
        """분석된 모든 프로시저/함수에서 Summary와 User Story를 수집하여 상세 문서를 생성합니다."""
        try:
            # summary와 user_stories를 모두 조회
            query = f"""
                MATCH (n)
                WHERE (n:PROCEDURE OR n:FUNCTION OR n:TRIGGER)
                  AND n.user_id = '{escape_for_cypher(orchestrator.user_id)}'
                  AND n.project_name = '{escape_for_cypher(orchestrator.project_name)}'
                  AND (n.summary IS NOT NULL OR n.user_stories IS NOT NULL)
                RETURN n.procedure_name AS name, 
                       n.summary AS summary,
                       n.user_stories AS user_stories, 
                       labels(n)[0] AS type
                ORDER BY n.file_name, n.startLine
            """
            
            results = await connection.execute_queries([query])
            
            if not results or not results[0]:
                return ""
            
            # summary가 있거나 user_stories가 있는 결과만 필터링
            filtered_results = [
                r for r in results[0] 
                if r.get("summary") or r.get("user_stories")
            ]
            
            if not filtered_results:
                return ""
            
            document = generate_user_story_document(
                results=filtered_results,
                source_name=orchestrator.project_name,
                source_type="DBMS 프로시저/함수"
            )
            
            return document
            
        except Exception as exc:
            logging.error("User Story 문서 생성 중 오류: %s", exc)
            return ""
