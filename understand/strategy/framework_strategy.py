import asyncio
import json
import logging
import os
from typing import Any, AsyncGenerator

import aiofiles

from understand.neo4j_connection import Neo4jConnection
from understand.strategy.base_strategy import UnderstandStrategy
from understand.strategy.framework.analysis import FrameworkAnalyzer
from util.utility_tool import (
    emit_data,
    emit_error,
    emit_message,
    escape_for_cypher,
    generate_user_story_document,
    aggregate_user_stories_from_results,
)


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
            yield emit_message("🚀 프레임워크 코드 분석을 시작합니다")
            yield emit_message(f"📦 프로젝트 '{orchestrator.project_name}'에서 {total_files}개 파일을 분석합니다")
            
            await connection.ensure_constraints()
            yield emit_message("🔌 데이터베이스에 연결되었습니다")

            if await connection.node_exists(orchestrator.user_id, file_names):
                yield emit_message("🔄 이전 분석 결과를 업데이트합니다")

            yield emit_message(f"🔍 클래스 및 인터페이스 구조를 분석합니다 ({total_files}개 파일)")

            for file_idx, (directory, file_name) in enumerate(file_names, 1):
                yield emit_message(f"📄 [{file_idx}/{total_files}] {file_name} 분석 중...")
                if directory:
                    yield emit_message(f"   📁 {directory}")
                
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
                
                yield emit_message(f"   ✓ {file_name} 완료")

            yield emit_message(f"🎉 코드 구조 분석이 완료되었습니다 ({total_files}개 파일)")
            
            # User Story 문서 생성
            yield emit_message("📝 비즈니스 요구사항을 정리하고 있습니다...")
            user_story_doc = await self._generate_user_story_document(connection, orchestrator)
            if user_story_doc:
                yield emit_data(
                    graph={"Nodes": [], "Relationships": []},
                    line_number=0,
                    analysis_progress=100,
                    current_file="user_stories.md",
                    user_story_document=user_story_doc,
                    event_type="user_story_document"
                )
                yield emit_message("📋 User Story 문서가 생성되었습니다")
            else:
                yield emit_message("ℹ️ 추출할 User Story가 없습니다")
            
            yield emit_message("✅ 모든 분석이 완료되었습니다!")
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

        antlr_data, source_content = await self._load_assets(orchestrator, directory, file_name)
        last_line = len(source_content)
        source_raw = "".join(source_content)
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
                yield emit_data(graph={"Nodes": [], "Relationships": []}, line_number=last_line, analysis_progress=100, current_file=current_file)
                break

            # 오류 발생
            if event_type == "error":
                error_message = event.get("message", f"Understanding failed for {file_name}")
                logging.error("Understanding Failed for %s: %s", file_name, error_message)
                yield emit_message(f"❌ 오류 발생: {error_message}")
                yield emit_error(error_message)
                return

            next_line = event.get("line_number", 0)
            progress = self._calculate_progress(next_line, last_line)

            # 정적 그래프 생성
            if event_type == "static_graph":
                if static_blocks == 0:
                    yield emit_message("   🏗️ 클래스/메서드 구조를 그래프로 구성 중...")
                static_blocks += 1
                graph_result = await connection.execute_query_and_return_graph(event.get("query_data", []))
                yield emit_data(graph=graph_result, line_number=next_line, analysis_progress=progress, current_file=current_file)
                await events_to_analyzer.put({"type": "process_completed"})
                continue

            # 정적 그래프 완료
            if event_type == "static_complete":
                yield emit_message(f"   ✓ 구조 그래프 생성 완료 ({static_blocks}개)")
                await events_to_analyzer.put({"type": "process_completed"})
                continue

            # LLM 분석 시작
            if event_type == "llm_start":
                total_llm_batches = event.get("total_batches", 0)
                yield emit_message(f"   🤖 AI가 비즈니스 로직을 분석합니다 ({total_llm_batches}개 블록)")
                await events_to_analyzer.put({"type": "process_completed"})
                continue

            # LLM 분석 진행
            if event_type == "analysis_code":
                analyzed_blocks += 1
                graph_result = await connection.execute_query_and_return_graph(event.get("query_data", []))
                yield emit_data(graph=graph_result, line_number=next_line, analysis_progress=progress, current_file=current_file)
                await events_to_analyzer.put({"type": "process_completed"})

        await analysis_task

    async def _generate_user_story_document(
        self,
        connection: Neo4jConnection,
        orchestrator,
    ) -> str:
        """분석된 모든 클래스에서 User Story를 수집하여 문서를 생성합니다."""
        try:
            # 모든 클래스/인터페이스의 user_stories 속성 조회
            query = f"""
                MATCH (n)
                WHERE (n:CLASS OR n:INTERFACE)
                  AND n.user_id = '{escape_for_cypher(orchestrator.user_id)}'
                  AND n.project_name = '{escape_for_cypher(orchestrator.project_name)}'
                  AND n.user_stories IS NOT NULL
                RETURN n.class_name AS name, n.user_stories AS user_stories, labels(n)[0] AS type
                ORDER BY n.file_name, n.startLine
            """
            
            results = await connection.execute_queries([query])
            
            if not results or not results[0]:
                return ""
            
            # 모든 User Story 집계
            all_user_stories = aggregate_user_stories_from_results(results[0])
            
            if not all_user_stories:
                return ""
            
            # 문서 생성
            document = generate_user_story_document(
                user_stories=all_user_stories,
                source_name=orchestrator.project_name,
                source_type="Java 클래스/인터페이스"
            )
            
            return document
            
        except Exception as exc:
            logging.error("User Story 문서 생성 중 오류: %s", exc)
            return ""

