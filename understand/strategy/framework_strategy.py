import asyncio
import json
import logging
import os
from typing import Any, AsyncGenerator

import aiofiles

from understand.neo4j_connection import Neo4jConnection
from understand.strategy.base_strategy import UnderstandStrategy
from understand.strategy.framework.analysis import FrameworkAnalyzer
from util.utility_tool import emit_data, emit_error, emit_message


class FrameworkUnderstandStrategy(UnderstandStrategy):
    """Java/Framework 코드 기반 클래스 다이어그램 그래프 구축 전략"""

    @staticmethod
    def _calculate_progress(current_line: int, total_lines: int) -> int:
        """현재 진행률을 계산합니다 (0-99%)."""
        return min(int((current_line / total_lines) * 100), 99) if current_line > 0 else 0

    async def understand(self, file_names: list, orchestrator: Any, **kwargs) -> AsyncGenerator[bytes, None]:
        connection = Neo4jConnection()
        events_from_analyzer = asyncio.Queue()
        events_to_analyzer = asyncio.Queue()
        total_files = len(file_names)

        try:
            yield emit_message("프레임워크 코드 분석을 시작합니다")
            yield emit_message(f"프로젝트 '{orchestrator.project_name}'의 {total_files}개 파일을 분석합니다")
            
            await connection.ensure_constraints()
            yield emit_message("데이터베이스 연결이 완료되었습니다")

            if await connection.node_exists(orchestrator.user_id, file_names):
                yield emit_message("이전 분석 결과가 발견되어 증분 업데이트 모드로 진행합니다")

            yield emit_message(f"클래스 및 인터페이스 구조 분석을 시작합니다 ({total_files}개 파일)")

            for file_idx, (directory, file_name) in enumerate(file_names, 1):
                yield emit_message(f"파일 분석 시작: {file_name} ({file_idx}/{total_files})")
                yield emit_message(f"경로: {directory}")
                
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
                
                yield emit_message(f"파일 분석 완료: {file_name} ({file_idx}/{total_files})")

            yield emit_message(f"프레임워크 코드 분석이 모두 완료되었습니다 (총 {total_files}개 파일 처리)")
            yield emit_message("ALL_ANALYSIS_COMPLETED")
        finally:
            await connection.close()

    async def _load_assets(self, orchestrator, directory: str, file_name: str) -> tuple:
        src_file_path = os.path.join(orchestrator.dirs["src"], directory, file_name)
        base_name = os.path.splitext(file_name)[0]
        analysis_file_path = os.path.join(orchestrator.dirs["analysis"], directory, f"{base_name}.json")

        async with aiofiles.open(analysis_file_path, "r", encoding="utf-8") as antlr_file, aiofiles.open(
            src_file_path, "r", encoding="utf-8"
        ) as source_file:
            antlr_data, source_content = await asyncio.gather(antlr_file.read(), source_file.readlines())
            return json.loads(antlr_data), source_content

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

        yield emit_message("소스 파일을 읽는 중입니다")
        antlr_data, source_content = await self._load_assets(orchestrator, directory, file_name)
        last_line = len(source_content)
        source_raw = "".join(source_content)
        yield emit_message("파일 로딩이 완료되었습니다")

        yield emit_message("구문 분석기를 준비하고 있습니다")
        analyzer = FrameworkAnalyzer(
            antlr_data=antlr_data,
            file_content=source_raw,
            directory=directory,
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

        analyzed_blocks = 0
        static_blocks = 0
        total_llm_batches = 0

        while True:
            event = await events_from_analyzer.get()
            event_type = event.get("type")
            logging.info("Analysis Event: %s, type: %s", current_file, event_type)

            # 분석 완료
            if event_type == "end_analysis":
                logging.info("Understanding Completed for %s", current_file)
                yield emit_message(f"파일별 코드 분석이 모두 끝났습니다 (구조 {static_blocks}개, AI 분석 {analyzed_blocks}개 블록 처리)")
                yield emit_data(graph={"Nodes": [], "Relationships": []}, line_number=last_line, analysis_progress=100, current_file=current_file)
                break

            # 오류 발생
            if event_type == "error":
                error_message = event.get("message", f"Understanding failed for {file_name}")
                logging.error("Understanding Failed for %s: %s", file_name, error_message)
                yield emit_message(f"분석 중 오류가 발생했습니다: {error_message}")
                yield emit_error(error_message)
                return

            next_line = event.get("line_number", 0)
            progress = self._calculate_progress(next_line, last_line)

            # 정적 그래프 생성
            if event_type == "static_graph":
                if static_blocks == 0:
                    yield emit_message("1단계: 클래스와 메서드 구조를 그래프로 구성하는 중입니다")
                static_blocks += 1
                if static_blocks % 2 == 0:
                    yield emit_message(f"  → 구조 생성 중... ({static_blocks}개 처리됨)")
                graph_result = await connection.execute_query_and_return_graph(event.get("query_data", []))
                yield emit_data(graph=graph_result, line_number=next_line, analysis_progress=progress, current_file=current_file)
                await events_to_analyzer.put({"type": "process_completed"})
                continue

            # 정적 그래프 완료
            if event_type == "static_complete":
                yield emit_message(f"1단계 완료: 클래스 구조 그래프가 생성되었습니다 (총 {static_blocks}개)")
                await events_to_analyzer.put({"type": "process_completed"})
                continue

            # LLM 분석 시작
            if event_type == "llm_start":
                total_llm_batches = event.get("total_batches", 0)
                yield emit_message(f"2단계: AI가 비즈니스 로직을 분석합니다 (총 {total_llm_batches}개 블록)")
                await events_to_analyzer.put({"type": "process_completed"})
                continue

            # LLM 분석 진행
            if event_type == "analysis_code":
                analyzed_blocks += 1
                msg = f"  → AI 분석 중... ({analyzed_blocks}/{total_llm_batches})" if total_llm_batches > 0 else f"  → AI 분석 중... ({analyzed_blocks}개 처리됨)"
                yield emit_message(msg)
                graph_result = await connection.execute_query_and_return_graph(event.get("query_data", []))
                yield emit_data(graph=graph_result, line_number=next_line, analysis_progress=progress, current_file=current_file)
                await events_to_analyzer.put({"type": "process_completed"})

        await analysis_task


