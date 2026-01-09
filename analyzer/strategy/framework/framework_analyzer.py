"""Framework 코드 분석 전략 - Java, Kotlin 등

AST 기반 Java 코드 분석 → Neo4j 클래스 다이어그램 그래프 생성.

분석 흐름 (2단계 + 이중 병렬):
1. [Phase 1] 모든 파일 AST 그래프 생성 (병렬)
   - 정적 노드 생성: CLASS, INTERFACE, METHOD, FIELD
   - 정적 관계 생성: HAS_METHOD, HAS_FIELD, CONTAINS
   
2. [Phase 2] 모든 파일 LLM 분석 (파일 병렬 + 청크 병렬)
   - 코드 요약 및 분석
   - CALLS 관계 생성 (MATCH로 기존 노드 조회)
   - DEPENDENCY 관계 생성
   
3. [Phase 3] User Story 문서 생성 (BaseStreamingAnalyzer 공통)

파일 상태 관리:
- Phase1 실패 파일은 Phase2 스킵 (토큰 절감)
- 파일별 SUCCESS/FAILED/SKIPPED 상태 추적
"""

import asyncio
import json
import logging
import os
from typing import Any, AsyncGenerator, Optional, List

import aiofiles

from analyzer.neo4j_client import Neo4jClient
from analyzer.strategy.base_analyzer import BaseStreamingAnalyzer, AnalysisStats
from analyzer.strategy.base.file_context import FileStatus, FileAnalysisContext
from analyzer.strategy.framework.ast_processor import FrameworkAstProcessor
from config.settings import settings
from util.exception import AnalysisError
from util.stream_utils import (
    emit_data,
    emit_message,
    format_graph_result,
)
from util.utility_tool import (
    escape_for_cypher,
    generate_user_story_document,
    log_process,
)


class FrameworkAnalyzer(BaseStreamingAnalyzer):
    """Java/Framework 코드 분석 전략
    
    2단계 분석 + 이중 병렬 처리:
    - Phase 1: 모든 파일 AST 그래프 생성 (병렬)
    - Phase 2: 모든 파일 LLM 분석 (병렬) - Phase1 실패 파일 제외
    - Phase 3: User Story 문서 생성 (부모 클래스 공통)
    
    파이프라인 특성:
    - 병렬 처리: 파일 단위로 동시 분석
    - 동시성 보호: Cypher 쿼리 락 사용
    - 프로세서 재사용: Phase 1에서 생성한 프로세서를 Phase 2에서 재사용
    - 토큰 절감: Phase1 실패 파일은 Phase2 스킵
    """

    # =========================================================================
    # 전략 메타데이터 (BaseStreamingAnalyzer 구현)
    # =========================================================================
    
    @property
    def strategy_name(self) -> str:
        return "프레임워크"
    
    @property
    def strategy_emoji(self) -> str:
        return "🚀"
    
    @property
    def file_type_description(self) -> str:
        return "Java/Kotlin 파일"

    def __init__(self):
        self._cypher_lock = asyncio.Lock()  # Cypher 쿼리 동시성 보호
        self._file_semaphore: Optional[asyncio.Semaphore] = None

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
        """Framework 분석 파이프라인 실행
        
        흐름:
        1. 파일 로드 (병렬)
        2. Phase 1: AST 그래프 생성 (병렬)
        3. Phase 2: LLM 분석 (병렬) - Phase1 실패 파일 제외 (토큰 절감)
        
        Note: User Story Phase는 부모 클래스에서 처리
        """
        total_files = len(file_names)
        self._file_semaphore = asyncio.Semaphore(settings.concurrency.file_concurrency)

        yield emit_message(f"⚡ 병렬 처리: 파일 {settings.concurrency.file_concurrency}개 동시")

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
        """분석된 클래스에서 User Story 문서 생성"""
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
                "User Story 생성 스킵: 분석된 클래스/인터페이스가 없습니다",
                logging.INFO
            )
            return None
        
        filtered = [
            r for r in results[0]
            if r.get("summary") or (r.get("user_stories") and len(r["user_stories"]) > 0)
        ]
        
        if not filtered:
            return None
        
        log_process("ANALYZE", "USER_STORY", f"User Story 생성 | 대상={len(filtered)}개 클래스")
        return generate_user_story_document(
            results=filtered,
            source_name=orchestrator.project_name,
            source_type="Java 클래스/인터페이스",
        )

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
        """Phase 1: 모든 파일의 AST 그래프를 병렬로 생성합니다.
        
        파일별 상태 기록:
        - 성공: PH1_OK → Phase 2 진행
        - 실패: PH1_FAIL → Phase 2 스킵 (토큰 절감)
        """
        
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
        """Phase 2: Phase1 성공 파일의 LLM 분석을 병렬로 실행합니다.
        
        Phase1 실패 파일은 이미 필터링되어 전달되지 않음 (토큰 절감).
        """
        
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
                        # Cypher 쿼리 실행 (락 사용)
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

