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
)

DDL_MAX_CONCURRENCY = int(os.getenv("DDL_MAX_CONCURRENCY", "5"))


class DbmsUnderstandStrategy(UnderstandStrategy):
    """DBMS 이해 전략: DDL 처리 → Analyzer 실행 → 후처리."""

    async def understand(self, file_names: list, orchestrator: Any, **kwargs) -> AsyncGenerator[bytes, None]:
        connection = Neo4jConnection()
        events_from_analyzer = asyncio.Queue()
        events_to_analyzer = asyncio.Queue()

        try:
            yield emit_message("Preparing Analysis Data")
            await connection.ensure_constraints()

            if await connection.node_exists(orchestrator.user_id, file_names):
                yield emit_message("ALREADY ANALYZED: RE-APPLYING UPDATES")

            ddl_files = self._list_ddl_files(orchestrator)
            if ddl_files:
                ddl_dir = orchestrator.dirs["ddl"]
                ddl_semaphore = asyncio.Semaphore(DDL_MAX_CONCURRENCY)
                ddl_tasks = []

                async def _run_single_ddl(file_name: str):
                    async with ddl_semaphore:
                        ddl_file_path = os.path.join(ddl_dir, file_name)
                        await self._process_ddl(ddl_file_path, connection, file_name, orchestrator)

                for ddl_file_name in ddl_files:
                    yield emit_message(f"START DDL PROCESSING: {ddl_file_name}")
                    logging.info("DDL 파일 처리 시작: %s", ddl_file_name)
                    ddl_tasks.append(asyncio.create_task(_run_single_ddl(ddl_file_name)))

                if ddl_tasks:
                    await asyncio.gather(*ddl_tasks)

            for system_name, file_name in file_names:
                await self._ensure_system_node(connection, system_name, orchestrator)
                async for chunk in self._analyze_file(
                    system_name,
                    file_name,
                    file_names,
                    connection,
                    events_from_analyzer,
                    events_to_analyzer,
                    orchestrator,
                ):
                    yield chunk

            yield emit_message("ALL_ANALYSIS_COMPLETED")
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

    async def _ensure_system_node(self, connection: Neo4jConnection, system_name: str, orchestrator) -> None:
        user_id_esc = escape_for_cypher(orchestrator.user_id)
        system_esc = escape_for_cypher(system_name)
        project_esc = escape_for_cypher(orchestrator.project_name)
        await connection.execute_queries(
            [
                f"MERGE (f:SYSTEM {{user_id: '{user_id_esc}', system_name: '{system_esc}', project_name: '{project_esc}', has_children: true}}) RETURN f"
            ]
        )

    async def _load_assets(self, orchestrator, system_name: str, file_name: str) -> tuple:
        system_dirs = orchestrator.get_system_dirs(system_name)
        plsql_file_path = os.path.join(system_dirs["src"], file_name)
        base_name = os.path.splitext(file_name)[0]
        analysis_file_path = os.path.join(system_dirs["analysis"], f"{base_name}.json")

        async with aiofiles.open(analysis_file_path, "r", encoding="utf-8") as antlr_file, aiofiles.open(
            plsql_file_path, "r", encoding="utf-8"
        ) as plsql_file:
            antlr_data, plsql_content = await asyncio.gather(antlr_file.read(), plsql_file.readlines())
            return json.loads(antlr_data), plsql_content

    async def _process_ddl(
        self,
        ddl_file_path: str,
        connection: Neo4jConnection,
        file_name: str,
        orchestrator,
    ) -> None:
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

            await connection.execute_queries(cypher_queries)
            logging.info("DDL 파일 처리 완료: %s", file_name)

    async def _analyze_file(
        self,
        system_name: str,
        file_name: str,
        file_pairs: list,
        connection: Neo4jConnection,
        events_from_analyzer: asyncio.Queue,
        events_to_analyzer: asyncio.Queue,
        orchestrator: Any,
    ) -> AsyncGenerator[bytes, None]:
        antlr_data, plsql_content = await self._load_assets(orchestrator, system_name, file_name)
        last_line = len(plsql_content)
        plsql_raw = "".join(plsql_content)

        analyzer = Analyzer(
            antlr_data=antlr_data,
            file_content=plsql_raw,
            send_queue=events_from_analyzer,
            receive_queue=events_to_analyzer,
            last_line=last_line,
            system_name=system_name,
            file_name=file_name,
            user_id=orchestrator.user_id,
            api_key=orchestrator.api_key,
            locale=orchestrator.locale,
            target=orchestrator.target,
            project_name=orchestrator.project_name,
        )
        analysis_task = asyncio.create_task(analyzer.run())

        current_file = f"{system_name}-{file_name}"
        while True:
            analysis_result = await events_from_analyzer.get()
            result_type = analysis_result.get("type")

            logging.info("Analysis Event: %s", current_file)

            if result_type == "end_analysis":
                logging.info("Understanding Completed for %s", current_file)
                postprocess_graph = await self._postprocess_file(connection, system_name, file_name, file_pairs, orchestrator)
                yield emit_data(
                    graph=postprocess_graph,
                    line_number=last_line,
                    analysis_progress=100,
                    current_file=current_file,
                )
                break

            if result_type == "error":
                error_message = analysis_result.get("message", f"Understanding failed for {file_name}")
                logging.error("Understanding Failed for %s: %s", file_name, error_message)
                yield emit_error(error_message)
                return

            next_analysis_line = analysis_result["line_number"]
            graph_result = await connection.execute_query_and_return_graph(analysis_result.get("query_data", []))
            yield emit_data(
                graph=graph_result,
                line_number=next_analysis_line,
                analysis_progress=int((next_analysis_line / last_line) * 100),
                current_file=current_file,
            )
            await events_to_analyzer.put({"type": "process_completed"})

        await analysis_task

    async def _postprocess_file(
        self,
        connection: Neo4jConnection,
        system_name: str,
        file_name: str,
        file_pairs: list,
        orchestrator: Any,
    ) -> dict:
        system_esc, file_esc = escape_for_cypher(system_name), escape_for_cypher(file_name)

        var_rows = (
            (
                await connection.execute_queries(
                    [
                        f"""
            MATCH (v:Variable {{system_name: '{system_esc}', file_name: '{file_esc}', user_id: '{orchestrator.user_id}'}})
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
            f"MATCH (v:Variable {{name: '{escape_for_cypher(row['varName'])}', system_name: '{system_esc}', file_name: '{file_esc}', user_id: '{user_id_esc}'}}) "
            f"SET v.type = '{escape_for_cypher((result or {}).get('resolvedType') or row.get('declaredType'))}', v.resolved = true RETURN v"
            for row, result in zip(var_rows, type_results)
        ]

        if update_queries:
            return await connection.execute_query_and_return_graph(update_queries)

        return {"Nodes": [], "Relationships": []}

