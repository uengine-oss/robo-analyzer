"""Framework 코드 분석 전략 - Java, Kotlin 등

AST 기반 Java 코드 분석 → Neo4j 클래스 다이어그램 그래프 생성.

분석 흐름 (2단계 + 이중 병렬):
1. [Phase 1] 모든 파일 AST 그래프 생성 (병렬 5개)
   - 정적 노드 생성: CLASS, INTERFACE, METHOD, FIELD
   - 정적 관계 생성: HAS_METHOD, HAS_FIELD, CONTAINS
   
2. [Phase 2] 모든 파일 LLM 분석 (파일 병렬 5개 + 청크 병렬)
   - 코드 요약 및 분석
   - CALLS 관계 생성 (MATCH로 기존 노드 조회)
   - DEPENDENCY 관계 생성
   
3. [Phase 3] 클래스 요약 및 User Story 생성
"""

import asyncio
import json
import logging
import os
from dataclasses import dataclass
from typing import Any, AsyncGenerator, Optional

import aiofiles

from analyzer.neo4j_client import Neo4jClient
from analyzer.strategy.base_analyzer import AnalyzerStrategy
from analyzer.strategy.framework.ast_processor import FrameworkAstProcessor
from config.settings import settings
from util.exception import AnalysisError, CodeProcessError
from util.stream_utils import (
    emit_complete,
    emit_data,
    emit_error,
    emit_message,
    format_graph_result,
)
from util.utility_tool import (
    escape_for_cypher,
    generate_user_story_document,
    log_process,
)


@dataclass
class FileAnalysisContext:
    """파일 분석 컨텍스트"""
    directory: str
    file_name: str
    ast_data: dict
    source_lines: list[str]
    processor: Optional[FrameworkAstProcessor] = None


class FrameworkAnalyzer(AnalyzerStrategy):
    """Java/Framework 코드 분석 전략
    
    2단계 분석 + 이중 병렬 처리:
    - Phase 1: 모든 파일 AST 그래프 생성 (병렬)
    - Phase 2: 모든 파일 LLM 분석 (병렬)
    """

    def __init__(self):
        self._cypher_lock = asyncio.Lock()  # Cypher 쿼리 동시성 보호
        self._file_semaphore: Optional[asyncio.Semaphore] = None

    async def analyze(
        self,
        file_names: list[tuple[str, str]],
        orchestrator: Any,
        **kwargs,
    ) -> AsyncGenerator[bytes, None]:
        """파일 목록을 2단계로 분석하여 결과를 스트리밍합니다."""
        client = Neo4jClient()
        total_files = len(file_names)
        self._file_semaphore = asyncio.Semaphore(settings.concurrency.file_concurrency)
        
        # 전체 통계
        stats = {
            "total_nodes": 0,
            "total_rels": 0,
            "phase1_nodes": 0,
            "phase2_updates": 0,
        }

        try:
            # ========== 초기화 ==========
            yield emit_message("🚀 프레임워크 코드 분석을 시작합니다")
            yield emit_message(f"📦 프로젝트: {orchestrator.project_name}")
            yield emit_message(f"📊 분석 대상: {total_files}개 파일")
            yield emit_message(f"⚡ 병렬 처리: 파일 {settings.concurrency.file_concurrency}개 동시")
            
            await client.ensure_constraints()
            yield emit_message("🔌 Neo4j 데이터베이스 연결 완료")

            # 기존 분석 결과 확인
            if await client.check_nodes_exist(orchestrator.user_id, file_names):
                yield emit_message("🔄 이전 분석 결과 발견 → 증분 업데이트 모드")
            else:
                yield emit_message("🆕 새로운 분석 시작")

            # ========== Phase 1: AST 그래프 생성 (병렬) ==========
            yield emit_message("")
            yield emit_message("━" * 50)
            yield emit_message(f"🏗️ [Phase 1] AST 구조 그래프 생성 ({total_files}개 파일 병렬)")
            yield emit_message("━" * 50)

            # 파일 컨텍스트 로드 (병렬)
            contexts = await self._load_all_files(file_names, orchestrator)
            yield emit_message(f"   ✓ {len(contexts)}개 파일 로드 완료")

            # Phase 1: 정적 그래프 생성 (병렬)
            async for chunk in self._run_phase1(contexts, client, orchestrator, stats):
                yield chunk

            yield emit_message("")
            yield emit_message(f"   ✅ Phase 1 완료: {stats['phase1_nodes']}개 노드 생성")

            # ========== Phase 2: LLM 분석 (병렬) ==========
            yield emit_message("")
            yield emit_message("━" * 50)
            yield emit_message(f"🤖 [Phase 2] AI 분석 ({total_files}개 파일 병렬)")
            yield emit_message("━" * 50)

            async for chunk in self._run_phase2(contexts, client, orchestrator, stats):
                yield chunk

            yield emit_message("")
            yield emit_message(f"   ✅ Phase 2 완료: {stats['phase2_updates']}개 분석 완료")

            # ========== Phase 3: User Story 생성 ==========
            yield emit_message("")
            yield emit_message("━" * 50)
            yield emit_message("📝 [Phase 3] User Story 문서 생성")
            yield emit_message("━" * 50)
            
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
            
            # ========== 완료 ==========
            yield emit_message("")
            yield emit_message("━" * 50)
            yield emit_message("✅ 모든 분석이 완료되었습니다!")
            yield emit_message(f"   📊 총 노드: {stats['total_nodes']}개")
            yield emit_message(f"   🔗 총 관계: {stats['total_rels']}개")
            yield emit_message("━" * 50)
            yield emit_complete()
            
        except AnalysisError as e:
            log_process("ANALYZE", "ERROR", f"분석 오류: {e}", logging.ERROR, e)
            yield emit_error(str(e))
            raise
        except Exception as e:
            error_msg = f"예상치 못한 오류: {e}"
            log_process("ANALYZE", "ERROR", error_msg, logging.ERROR, e)
            yield emit_error(error_msg)
            raise CodeProcessError(error_msg) from e
        finally:
            await client.close()

    async def _load_all_files(
        self,
        file_names: list[tuple[str, str]],
        orchestrator: Any,
    ) -> list[FileAnalysisContext]:
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

    async def _run_phase1(
        self,
        contexts: list[FileAnalysisContext],
        client: Neo4jClient,
        orchestrator: Any,
        stats: dict,
    ) -> AsyncGenerator[bytes, None]:
        """Phase 1: 모든 파일의 AST 그래프를 병렬로 생성합니다."""
        
        completed = 0
        total = len(contexts)
        results_queue: asyncio.Queue = asyncio.Queue()

        async def process_file(ctx: FileAnalysisContext):
            async with self._file_semaphore:
                try:
                    processor = FrameworkAstProcessor(
                        antlr_data=ctx.ast_data,
                        file_content="".join(ctx.source_lines),
                        directory=ctx.directory,
                        file_name=ctx.file_name,
                        user_id=orchestrator.user_id,
                        api_key=orchestrator.api_key,
                        locale=orchestrator.locale,
                        project_name=orchestrator.project_name,
                        last_line=len(ctx.source_lines),
                    )
                    ctx.processor = processor
                    
                    # 정적 그래프 생성
                    queries = processor.build_static_graph_queries()
                    
                    if queries:
                        # Cypher 쿼리 실행 (락 사용)
                        async with self._cypher_lock:
                            graph = await client.run_graph_query(queries)
                        
                        node_count = len(graph.get("Nodes", []))
                        rel_count = len(graph.get("Relationships", []))
                        
                        await results_queue.put({
                            "type": "success",
                            "file": ctx.file_name,
                            "graph": graph,
                            "node_count": node_count,
                            "rel_count": rel_count,
                        })
                    else:
                        await results_queue.put({
                            "type": "success",
                            "file": ctx.file_name,
                            "graph": {"Nodes": [], "Relationships": []},
                            "node_count": 0,
                            "rel_count": 0,
                        })
                        
                except Exception as e:
                    log_process("ANALYZE", "ERROR", f"Phase 1 오류 ({ctx.file_name}): {e}", logging.ERROR, e)
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
            
            if result["type"] == "error":
                yield emit_message(f"   ❌ [{completed}/{total}] {result['file']}: {result['message']}")
            else:
                stats["phase1_nodes"] += result["node_count"]
                stats["total_nodes"] += result["node_count"]
                stats["total_rels"] += result["rel_count"]
                
                graph = result["graph"]
                graph_msg = format_graph_result(graph)
                
                yield emit_message(f"   ✓ [{completed}/{total}] {result['file']}")
                if graph_msg:
                    for line in graph_msg.split("\n")[:3]:  # 최대 3줄
                        yield emit_message(f"      {line}")
                
                yield emit_data(
                    graph=graph,
                    line_number=0,
                    analysis_progress=int(completed / total * 50),
                    current_file=result["file"],
                )

        # 모든 작업 완료 대기
        await asyncio.gather(*tasks, return_exceptions=True)

    async def _run_phase2(
        self,
        contexts: list[FileAnalysisContext],
        client: Neo4jClient,
        orchestrator: Any,
        stats: dict,
    ) -> AsyncGenerator[bytes, None]:
        """Phase 2: 모든 파일의 LLM 분석을 병렬로 실행합니다."""
        
        completed = 0
        total = len(contexts)
        results_queue: asyncio.Queue = asyncio.Queue()

        async def analyze_file(ctx: FileAnalysisContext):
            async with self._file_semaphore:
                try:
                    if not ctx.processor:
                        raise AnalysisError(f"Phase 1에서 프로세서 초기화 실패: {ctx.file_name}")
                    
                    # LLM 분석 실행
                    analysis_queries = await ctx.processor.run_llm_analysis()
                    
                    if analysis_queries:
                        # Cypher 쿼리 실행 (락 사용)
                        async with self._cypher_lock:
                            graph = await client.run_graph_query(analysis_queries)
                        
                        await results_queue.put({
                            "type": "success",
                            "file": ctx.file_name,
                            "graph": graph,
                            "query_count": len(analysis_queries),
                        })
                    else:
                        await results_queue.put({
                            "type": "success",
                            "file": ctx.file_name,
                            "graph": {"Nodes": [], "Relationships": []},
                            "query_count": 0,
                        })
                        
                except Exception as e:
                    log_process("ANALYZE", "ERROR", f"Phase 2 오류 ({ctx.file_name}): {e}", logging.ERROR, e)
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
            completed += 1
            
            if result["type"] == "error":
                yield emit_message(f"   ❌ [{completed}/{total}] {result['file']}: {result['message']}")
            else:
                stats["phase2_updates"] += 1
                graph = result["graph"]
                stats["total_nodes"] += len(graph.get("Nodes", []))
                stats["total_rels"] += len(graph.get("Relationships", []))
                
                graph_msg = format_graph_result(graph)
                yield emit_message(f"   ✓ [{completed}/{total}] {result['file']} (쿼리 {result['query_count']}개)")
                if graph_msg:
                    for line in graph_msg.split("\n")[:3]:
                        yield emit_message(f"      {line}")
                
                yield emit_data(
                    graph=graph,
                    line_number=0,
                    analysis_progress=50 + int(completed / total * 50),
                    current_file=result["file"],
                )

        # 모든 작업 완료 대기
        await asyncio.gather(*tasks, return_exceptions=True)

    async def _create_user_story_doc(
        self,
        client: Neo4jClient,
        orchestrator: Any,
    ) -> str:
        """분석된 클래스에서 User Story 문서 생성"""
        try:
            query = f"""
                MATCH (n)
                WHERE (n:CLASS OR n:INTERFACE)
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
                RETURN n.class_name AS name, 
                       n.summary AS summary,
                       user_stories AS user_stories, 
                       labels(n)[0] AS type
                ORDER BY n.file_name, n.startLine
            """
            
            async with self._cypher_lock:
                results = await client.execute_queries([query])
            
            if not results or not results[0]:
                log_process(
                    "ANALYZE", "USER_STORY",
                    f"⚠️ Neo4j 쿼리 결과가 비어있습니다. 클래스/인터페이스에 summary가 설정되었는지 확인하세요.",
                    logging.WARNING
                )
                raise AnalysisError("User Story 생성을 위한 분석 결과가 없습니다 (Neo4j에 summary가 있는 클래스/인터페이스가 없음)")
            
            filtered = [
                r for r in results[0]
                if r.get("summary") or (r.get("user_stories") and len(r["user_stories"]) > 0)
            ]
            
            if not filtered:
                raise AnalysisError("User Story 생성 대상이 없습니다 (요약된 클래스 없음)")
            
            log_process("ANALYZE", "USER_STORY", f"User Story 생성 | 대상={len(filtered)}개 클래스")
            return generate_user_story_document(
                results=filtered,
                source_name=orchestrator.project_name,
                source_type="Java 클래스/인터페이스",
            )
            
        except Exception as exc:
            log_process(
                "ANALYZE", "USER_STORY", 
                f"User Story 문서 생성 실패: {exc}",
                logging.ERROR, exc
            )
            raise AnalysisError(f"User Story 생성 실패: {exc}") from exc

