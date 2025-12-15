import asyncio
import json
import os
from typing import Any, AsyncGenerator

import aiofiles

from understand.neo4j_connection import Neo4jConnection
from understand.strategy.base_strategy import UnderstandStrategy
from understand.strategy.framework.analysis import FrameworkAnalyzer
from util.utility_tool import emit_data, emit_error, emit_message, escape_for_cypher, log_process


class FrameworkUnderstandStrategy(UnderstandStrategy):
    """Java/Framework 코드 기반 클래스 다이어그램 그래프 구축 전략략"""

    async def understand(self, file_names: list, orchestrator: Any, **kwargs) -> AsyncGenerator[bytes, None]:
        connection = Neo4jConnection()
        events_from_analyzer = asyncio.Queue()
        events_to_analyzer = asyncio.Queue()

        try:
            yield emit_message("Preparing Analysis Data")
            await connection.ensure_constraints()

            if await connection.node_exists(orchestrator.user_id, file_names):
                yield emit_message("ALREADY ANALYZED: RE-APPLYING UPDATES")

            for folder_name, file_name in file_names:
                await self._ensure_folder_node(connection, folder_name, orchestrator)
                async for chunk in self._analyze_file(
                    folder_name,
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

    async def _ensure_folder_node(self, connection: Neo4jConnection, folder_name: str, orchestrator) -> None:
        user_id_esc = escape_for_cypher(orchestrator.user_id)
        folder_esc = escape_for_cypher(folder_name)
        project_esc = escape_for_cypher(orchestrator.project_name)
        await connection.execute_queries(
            [
                f"MERGE (f:SYSTEM {{user_id: '{user_id_esc}', name: '{folder_esc}', project_name: '{project_esc}', has_children: true}}) RETURN f"
            ]
        )

    async def _load_assets(self, orchestrator, folder_name: str, file_name: str) -> tuple:
        system_dirs = orchestrator.get_system_dirs(folder_name)
        src_file_path = os.path.join(system_dirs["src"], file_name)
        base_name = os.path.splitext(file_name)[0]
        analysis_file_path = os.path.join(system_dirs["analysis"], f"{base_name}.json")

        async with aiofiles.open(analysis_file_path, "r", encoding="utf-8") as antlr_file, aiofiles.open(
            src_file_path, "r", encoding="utf-8"
        ) as source_file:
            antlr_data, source_content = await asyncio.gather(antlr_file.read(), source_file.readlines())
            return json.loads(antlr_data), source_content

    async def _analyze_file(
        self,
        folder_name: str,
        file_name: str,
        file_pairs: list,
        connection: Neo4jConnection,
        events_from_analyzer: asyncio.Queue,
        events_to_analyzer: asyncio.Queue,
        orchestrator: Any,
    ) -> AsyncGenerator[bytes, None]:
        antlr_data, source_content = await self._load_assets(orchestrator, folder_name, file_name)
        last_line = len(source_content)
        source_raw = "".join(source_content)
        current_file = f"{folder_name}-{file_name}"

        analyzer = FrameworkAnalyzer(
            antlr_data=antlr_data,
            file_content=source_raw,
            folder_name=folder_name,
            file_name=file_name,
            user_id=orchestrator.user_id,
            api_key=orchestrator.api_key,
            locale=orchestrator.locale,
            project_name=orchestrator.project_name,
            send_queue=events_from_analyzer,
            receive_queue=events_to_analyzer,
            last_line=last_line,
        )
        analysis_task = asyncio.create_task(analyzer.run())

        while True:
            analysis_result = await events_from_analyzer.get()
            result_type = analysis_result.get("type")

            if result_type == "end_analysis":
                graph_result = await connection.execute_query_and_return_graph(orchestrator.user_id, file_pairs)
                yield emit_data(
                    graph=graph_result,
                    line_number=last_line,
                    analysis_progress=100,
                    current_file=current_file,
                )
                break

            if result_type == "error":
                error_message = analysis_result.get("message", f"Understanding failed for {file_name}")
                yield emit_error(error_message)
                return

            if result_type == "analysis_code":
                next_line = analysis_result.get("line_number", last_line)
                await connection.execute_queries(analysis_result.get("query_data", []))
                graph_result = await connection.execute_query_and_return_graph(orchestrator.user_id, file_pairs)
                yield emit_data(
                    graph=graph_result,
                    line_number=next_line,
                    analysis_progress=int((next_line / last_line) * 100),
                    current_file=current_file,
                )
                await events_to_analyzer.put({"type": "process_completed"})

        await analysis_task


