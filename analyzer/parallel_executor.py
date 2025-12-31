"""병렬 실행 관리자

파일 레벨 + 청크 레벨 이중 병렬 처리를 담당합니다.

주요 기능:
- 파일별 병렬 분석 (최대 5개 동시)
- 청크별 병렬 LLM 호출
- Cypher 쿼리 동시성 보호 (락 사용)
- 결과 스트리밍 통합
"""

import asyncio
import logging
from dataclasses import dataclass, field
from typing import Any, AsyncGenerator, Callable, Optional

from config.settings import settings


@dataclass
class AnalysisTask:
    """분석 작업 단위"""
    directory: str
    file_name: str
    index: int
    total: int


@dataclass
class ParallelConfig:
    """병렬 처리 설정"""
    file_concurrency: int = field(default_factory=lambda: settings.concurrency.file_concurrency)
    chunk_concurrency: int = field(default_factory=lambda: settings.concurrency.max_concurrency)


class ParallelExecutor:
    """이중 병렬 처리 실행기
    
    파일 레벨과 청크 레벨에서 동시에 병렬 처리를 수행합니다.
    Cypher 쿼리 실행은 락으로 보호하여 동시성 문제를 방지합니다.
    
    사용법:
        executor = ParallelExecutor()
        async for event in executor.run_parallel(
            tasks=tasks,
            processor=process_file,
        ):
            yield event
    """
    
    def __init__(self, config: Optional[ParallelConfig] = None):
        self.config = config or ParallelConfig()
        self._file_semaphore = asyncio.Semaphore(self.config.file_concurrency)
        self._cypher_lock = asyncio.Lock()
        self._event_queue: asyncio.Queue = asyncio.Queue()
        
    async def run_parallel(
        self,
        tasks: list[AnalysisTask],
        processor: Callable[[AnalysisTask, asyncio.Lock], AsyncGenerator[dict, None]],
    ) -> AsyncGenerator[dict, None]:
        """파일 목록을 병렬로 처리하고 이벤트 스트리밍
        
        Args:
            tasks: 분석 작업 리스트
            processor: 파일 처리 함수 (async generator)
                       processor(task, cypher_lock) -> AsyncGenerator[event, None]
        
        Yields:
            처리 이벤트 딕셔너리
        """
        if not tasks:
            return
        
        # 백그라운드 작업 시작
        worker_tasks = [
            asyncio.create_task(self._process_with_semaphore(task, processor))
            for task in tasks
        ]
        
        # 완료된 작업 수 추적
        completed = 0
        total = len(tasks)
        
        # 작업 완료 이벤트 대기 및 스트리밍
        while completed < total:
            try:
                event = await asyncio.wait_for(
                    self._event_queue.get(),
                    timeout=300.0,  # 5분 타임아웃
                )
                
                if event.get("type") == "file_complete":
                    completed += 1
                    logging.debug(f"파일 완료: {completed}/{total}")
                
                yield event
                
            except asyncio.TimeoutError:
                logging.warning("병렬 처리 타임아웃")
                break
        
        # 모든 작업 완료 대기
        await asyncio.gather(*worker_tasks, return_exceptions=True)
    
    async def _process_with_semaphore(
        self,
        task: AnalysisTask,
        processor: Callable,
    ) -> None:
        """세마포어로 동시 실행 수 제한"""
        async with self._file_semaphore:
            try:
                async for event in processor(task, self._cypher_lock):
                    await self._event_queue.put(event)
                
                await self._event_queue.put({
                    "type": "file_complete",
                    "file": task.file_name,
                    "index": task.index,
                })
            except Exception as e:
                logging.error(f"파일 처리 오류 ({task.file_name}): {e}")
                await self._event_queue.put({
                    "type": "error",
                    "file": task.file_name,
                    "message": str(e),
                })
    
    async def execute_cypher_safe(
        self,
        execute_fn: Callable[..., Any],
        *args,
        **kwargs,
    ) -> Any:
        """Cypher 쿼리를 락으로 보호하여 실행
        
        동시에 여러 파일에서 쿼리를 실행할 때 충돌을 방지합니다.
        
        Args:
            execute_fn: 쿼리 실행 함수
            *args, **kwargs: 함수 인자
            
        Returns:
            함수 실행 결과
        """
        async with self._cypher_lock:
            return await execute_fn(*args, **kwargs)


class ChunkBatcher:
    """청크 배치 처리기
    
    LLM 호출을 청크 단위로 배치하여 병렬 처리합니다.
    """
    
    def __init__(self, max_concurrency: Optional[int] = None):
        self.max_concurrency = max_concurrency or settings.concurrency.max_concurrency
    
    async def process_batches(
        self,
        batches: list[Any],
        processor: Callable[[Any], Any],
    ) -> list[Any]:
        """배치들을 병렬로 처리
        
        Args:
            batches: 처리할 배치 리스트
            processor: 배치 처리 함수 (async)
            
        Returns:
            처리 결과 리스트 (순서 유지)
        """
        semaphore = asyncio.Semaphore(self.max_concurrency)
        
        async def process_with_limit(batch):
            async with semaphore:
                return await processor(batch)
        
        return await asyncio.gather(*[
            process_with_limit(batch) for batch in batches
        ])

