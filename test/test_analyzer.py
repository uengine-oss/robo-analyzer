"""
분석 파이프라인 테스트

현재 아키텍처에 맞게 리팩토링된 테스트:
- Phase 1: AST 그래프 생성 (모든 파일)
- Phase 2: LLM 분석 (모든 파일)
- Phase 3: User Story 생성

Neo4j, LLM API 등의 외부 의존성이 필요합니다.

지원 전략:
- dbms: SQL 파일 기반 프로시저/함수 분석
- framework: Java 파일 기반 클래스 다이어그램 분석
"""

from __future__ import annotations

# ==================== Import ====================
import os
import sys
import time
from collections.abc import Awaitable, Callable
from pathlib import Path
from typing import Any, Dict, List, Tuple

import pytest
import pytest_asyncio

from api.orchestrator import AnalysisOrchestrator
from analyzer.neo4j_client import Neo4jClient

# ==================== 프로젝트 경로 설정 ====================
PROJECT_ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(PROJECT_ROOT))

# ==================== 환경변수 설정 ====================
# 한글 로그 인코딩
os.environ.setdefault("PYTHONIOENCODING", "utf-8")

# ==================== 환경변수 읽기 유틸리티 ====================
def _env(key: str, default: str) -> str:
    """환경변수 읽기 (공백 제거)"""
    value = os.getenv(key)
    return value.strip() if value and value.strip() else default


def _is_truthy_env(value: str | None) -> bool:
    """환경변수가 truthy 값인지 확인"""
    if value is None:
        return False
    return value.strip().lower() in {"1", "true", "yes", "on"}

# ==================== 테스트 설정 상수 ====================
# 환경변수에서 읽기
TEST_API_KEY = os.getenv("LLM_API_KEY")
TEST_DB_NAME = _env("TEST_DB_NAME", "test")
TEST_LOCALE = _env("TEST_LOCALE", "ko")
TEST_DBMS = _env("TEST_DBMS", "postgres")
TEST_ANALYSIS_STRATEGY = _env("TEST_ANALYSIS_STRATEGY", "dbms").lower()

# 테스트 데이터 디렉토리
DATA_DIR_ENV_KEY = "TEST_DATA_DIR"
DEFAULT_DATA_DIR = PROJECT_ROOT.parent / "data" / "test"
TEST_DATA_DIR = Path(os.getenv(DATA_DIR_ENV_KEY, str(DEFAULT_DATA_DIR))).expanduser()

# 전략별 설정
IS_FRAMEWORK = TEST_ANALYSIS_STRATEGY == "framework"
STRATEGY_LABEL = "FRAMEWORK" if IS_FRAMEWORK else "DBMS"

# MERGE 모드 설정
MERGE_MODE_ENABLED = _is_truthy_env(os.getenv("TEST_MERGE_MODE"))
_MERGE_MODE_NOTICE_PRINTED = False

# ==================== 타입 정의 ====================
SourceFile = Tuple[str, str]
SectionResult = Dict[str, float | int | str]
SectionRunner = Callable[[AnalysisOrchestrator, Neo4jClient, List[SourceFile] | None], Awaitable[SectionResult]]


def _ensure_api_key():
    """API 키가 설정되어 있는지 확인"""
    if not TEST_API_KEY:
        pytest.skip("LLM_API_KEY가 설정되지 않았습니다")


def _create_orchestrator() -> AnalysisOrchestrator:
    """테스트용 AnalysisOrchestrator 생성"""
    return AnalysisOrchestrator(
        api_key=TEST_API_KEY,
        locale=TEST_LOCALE,
        strategy=TEST_ANALYSIS_STRATEGY,
        target=TEST_DBMS if TEST_ANALYSIS_STRATEGY == "dbms" else "java",
    )


async def _clear_graph(connection: Neo4jClient):
    """Neo4j 데이터 삭제"""
    await connection.execute_queries([
        "MATCH (__cy_n__) DETACH DELETE __cy_n__"
    ])


async def _reset_graph_if_needed(connection: Neo4jClient):
    """MERGE 모드 여부에 따라 그래프 초기화"""
    global _MERGE_MODE_NOTICE_PRINTED
    if MERGE_MODE_ENABLED:
        if not _MERGE_MODE_NOTICE_PRINTED:
            print("[INFO] TEST_MERGE_MODE=1 → 그래프 초기화를 건너뜁니다(누적 적재)")
            _MERGE_MODE_NOTICE_PRINTED = True
        return
    await _clear_graph(connection)


@pytest_asyncio.fixture
async def real_neo4j() -> Neo4jClient:
    """테스트용 Neo4j 연결 생성 및 정리"""
    conn = Neo4jClient(database=TEST_DB_NAME)
    await _clear_graph(conn)

    try:
        yield conn
    finally:
        await conn.close()


# ==================== 분석 섹션 실행 ====================

async def _run_analysis_pipeline(
    orchestrator: AnalysisOrchestrator,
    connection: Neo4jClient,
    source_files: List[SourceFile] | None,
) -> SectionResult:
    """전체 분석 파이프라인 실행 (Phase 1 + Phase 2 + Phase 3)
    
    현재 아키텍처:
    - Phase 1: 모든 파일 AST 그래프 생성 (병렬)
    - Phase 2: 모든 파일 LLM 분석 (병렬)
    - Phase 3: User Story 생성
    """
    if not source_files:
        pytest.skip(f"분석할 소스 파일이 없습니다")

    start = time.perf_counter()
    event_count = 0
    
    # 분석 실행
    async for chunk in orchestrator.run_analysis(source_files):
        event_count += 1
    
    elapsed = time.perf_counter() - start

    assert event_count > 0, "분석 파이프라인에서 이벤트가 생성되지 않았습니다"

    # 결과 검증
    if IS_FRAMEWORK:
        # Framework: CLASS/INTERFACE 노드 확인
        result = await connection.execute_queries([
            "MATCH (__cy_c__) WHERE (__cy_c__:CLASS OR __cy_c__:INTERFACE) "
            "RETURN count(__cy_c__) AS c"
        ])
        node_count = int(result[0][0]["c"])
        assert node_count > 0, "CLASS/INTERFACE 노드가 생성되지 않았습니다"
        
        return {
            "elapsed_seconds": elapsed,
            "event_count": event_count,
            "files": len(source_files),
            "class_count": node_count,
        }
    else:
        # DBMS: PROCEDURE/FUNCTION 노드 확인
        result = await connection.execute_queries([
            "MATCH (__cy_p__) WHERE (__cy_p__:PROCEDURE OR __cy_p__:FUNCTION) "
            "RETURN count(__cy_p__) AS c"
        ])
        node_count = int(result[0][0]["c"])
        assert node_count > 0, "PROCEDURE/FUNCTION 노드가 생성되지 않았습니다"
        
        return {
            "elapsed_seconds": elapsed,
            "event_count": event_count,
            "files": len(source_files),
            "procedure_count": node_count,
        }


async def _run_ddl_only_section(
    orchestrator: AnalysisOrchestrator,
    connection: Neo4jClient,
    _source_files: List[SourceFile] | None,
) -> SectionResult:
    """DDL만 분석 (DBMS 전용)
    
    DDL 파일만 처리하여 Table/Column 노드를 생성합니다.
    source 파일은 빈 리스트로 전달하여 SP 분석을 스킵합니다.
    """
    if IS_FRAMEWORK:
        pytest.skip("Framework 전략에서는 DDL 단계가 없습니다")
    
    # DDL 디렉토리 확인
    ddl_dir = orchestrator.dirs.get("ddl", "")
    if not ddl_dir or not os.path.isdir(ddl_dir):
        pytest.skip(f"DDL 디렉토리가 없습니다: {ddl_dir}")
    
    # DDL 파일 목록 확인
    ddl_files = [f for f in os.listdir(ddl_dir) if os.path.isfile(os.path.join(ddl_dir, f))]
    if not ddl_files:
        pytest.skip("DDL 파일이 없습니다")
    
    start = time.perf_counter()
    event_count = 0
    
    # source 파일 없이 분석 실행 (DDL만 처리됨)
    async for chunk in orchestrator.run_analysis([]):
        event_count += 1
    
    elapsed = time.perf_counter() - start

    # Table 노드 확인
    result = await connection.execute_queries([
        "MATCH (__cy_t__:Table) "
        "RETURN count(__cy_t__) AS c"
    ])
    table_count = int(result[0][0]["c"])
    
    assert event_count > 0, "DDL 분석 이벤트가 생성되지 않았습니다"
    assert table_count > 0, "DDL 분석 후 Table 노드가 생성되지 않았습니다"
    
    return {
        "elapsed_seconds": elapsed,
        "event_count": event_count,
        "ddl_files": len(ddl_files),
        "table_count": table_count,
    }


async def _run_source_only_section(
    orchestrator: AnalysisOrchestrator,
    connection: Neo4jClient,
    source_files: List[SourceFile] | None,
) -> SectionResult:
    """소스 파일만 분석 (DDL 제외)
    
    DBMS: SP/FUNCTION만 분석
    Framework: Java 파일만 분석
    """
    if not source_files:
        pytest.skip("분석할 소스 파일이 없습니다")
    
    # DDL 처리를 스킵하기 위해 ddl 디렉토리를 임시로 비움
    original_ddl_dir = orchestrator.dirs.get("ddl", "")
    orchestrator.dirs["ddl"] = ""
    
    try:
        start = time.perf_counter()
        event_count = 0
        
        # 분석 실행 (DDL 없이)
        async for chunk in orchestrator.run_analysis(source_files):
            event_count += 1
        
        elapsed = time.perf_counter() - start
        
        assert event_count > 0, "소스 분석 이벤트가 생성되지 않았습니다"
        
        # 결과 검증
        if IS_FRAMEWORK:
            # Framework: CLASS/INTERFACE 노드 확인
            result = await connection.execute_queries([
                "MATCH (__cy_c__) WHERE (__cy_c__:CLASS OR __cy_c__:INTERFACE) "
                "RETURN count(__cy_c__) AS c"
            ])
            node_count = int(result[0][0]["c"])
            assert node_count > 0, "CLASS/INTERFACE 노드가 생성되지 않았습니다"
            
            return {
                "elapsed_seconds": elapsed,
                "event_count": event_count,
                "files": len(source_files),
                "class_count": node_count,
            }
        else:
            # DBMS: PROCEDURE/FUNCTION 노드 확인
            result = await connection.execute_queries([
                "MATCH (__cy_p__) WHERE (__cy_p__:PROCEDURE OR __cy_p__:FUNCTION) "
                "RETURN count(__cy_p__) AS c"
            ])
            node_count = int(result[0][0]["c"])
            assert node_count > 0, "PROCEDURE/FUNCTION 노드가 생성되지 않았습니다"
            
            return {
                "elapsed_seconds": elapsed,
                "event_count": event_count,
                "files": len(source_files),
                "procedure_count": node_count,
            }
    finally:
        # 원래 DDL 디렉토리 복원
        if original_ddl_dir:
            orchestrator.dirs["ddl"] = original_ddl_dir


# ==================== 결과 로깅 ====================

def _log_test_result(section: str, result: SectionResult):
    """테스트 결과를 로그로 출력"""
    lines = [f"\n[ANALYSIS TEST RESULT - {STRATEGY_LABEL}]"]
    line = f"  - section={section} elapsed={result['elapsed_seconds']:.2f}s"
    
    if "event_count" in result:
        line += f" events={result['event_count']}"
    if "files" in result:
        line += f" files={result['files']}"
    if "ddl_files" in result:
        line += f" ddl_files={result['ddl_files']}"
    if "table_count" in result:
        line += f" tables={result['table_count']}"
    if "class_count" in result:
        line += f" classes={result['class_count']}"
    if "procedure_count" in result:
        line += f" procedures={result['procedure_count']}"
    
    lines.append(line)
    print("\n".join(lines))


async def _execute_test(
    real_neo4j: Neo4jClient,
    section: str,
    runner: SectionRunner,
    source_files: List[SourceFile] | None = None,
):
    """테스트 섹션 실행"""
    orchestrator = _create_orchestrator()
    await _reset_graph_if_needed(real_neo4j)
    
    result = await runner(orchestrator, real_neo4j, source_files)
    _log_test_result(section, result)


# ==================== 테스트 케이스 ====================

@pytest.mark.asyncio
async def test_analysis_pipeline(real_neo4j: Neo4jClient):
    """전체 분석 파이프라인 테스트
    
    Phase 1 (AST 그래프 생성) + Phase 2 (LLM 분석) + Phase 3 (User Story)
    """
    _ensure_api_key()
    
    # orchestrator를 사용하여 소스 파일 탐색
    orchestrator = _create_orchestrator()
    try:
        source_files = orchestrator.discover_source_files()
    except Exception as e:
        pytest.skip(f"소스 파일 탐색 실패: {e}")
    
    if not source_files:
        pytest.skip("분석할 소스 파일이 없습니다")
    
    await _execute_test(
        real_neo4j,
        "pipeline",
        _run_analysis_pipeline,
        source_files=source_files,
    )


@pytest.mark.asyncio
async def test_ddl_only_section(real_neo4j: Neo4jClient):
    """DDL만 분석 테스트 (DBMS 전용)
    
    DDL 파일만 처리하여 Table/Column 노드를 생성합니다.
    """
    _ensure_api_key()
    
    if IS_FRAMEWORK:
        pytest.skip("Framework 전략에서는 DDL 단계가 없습니다")
    
    await _execute_test(
        real_neo4j,
        "ddl_only",
        _run_ddl_only_section,
    )


@pytest.mark.asyncio
async def test_source_only_section(real_neo4j: Neo4jClient):
    """소스 파일만 분석 테스트 (DDL 제외)
    
    DBMS: SP/FUNCTION만 분석
    Framework: Java 파일만 분석
    """
    _ensure_api_key()
    
    # orchestrator를 사용하여 소스 파일 탐색
    orchestrator = _create_orchestrator()
    try:
        source_files = orchestrator.discover_source_files()
    except Exception as e:
        pytest.skip(f"소스 파일 탐색 실패: {e}")
    
    if not source_files:
        pytest.skip("분석할 소스 파일이 없습니다")
    
    await _execute_test(
        real_neo4j,
        "source_only",
        _run_source_only_section,
        source_files=source_files,
    )
