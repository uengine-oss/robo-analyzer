"""
Understanding 파이프라인을 단일 테스트로 검증합니다.

섹션 단위로 전체/DDL-only/SP-only 흐름을 나눠 동일한 로직으로 실행하며,
실제 환경에서 사용하려면 Neo4j, LLM API 등의 외부 의존성을 동일하게 준비해야 합니다.
"""

from __future__ import annotations

import os
import sys
import time
from collections.abc import Awaitable, Callable
from pathlib import Path
from typing import Any, Dict, Iterable, List, Tuple

import pytest
import pytest_asyncio

from service.service import ServiceOrchestrator


PROJECT_ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(PROJECT_ROOT))

# 한글 로그가 깨지지 않도록 UTF-8 인코딩을 강제합니다.
os.environ.setdefault("PYTHONIOENCODING", "utf-8")


def _env(key: str, default: str) -> str:
    value = os.getenv(key)
    return value.strip() if value and value.strip() else default


DATA_DIR_ENV_KEY = "TEST_DATA_DIR"

TEST_USER_ID = _env("TEST_USER_ID", "TestSession")
TEST_PROJECT_NAME = _env("TEST_PROJECT_NAME", "test")
TEST_API_KEY = os.getenv("LLM_API_KEY")
TEST_DB_NAME = _env("TEST_DB_NAME", "test")
TEST_LOCALE = _env("TEST_LOCALE", "ko")
TEST_DBMS = _env("TEST_DBMS", "postgres")
TEST_ANALYSIS_STRATEGY = _env("TEST_ANALYSIS_STRATEGY", "dbms").lower()

DEFAULT_DATA_DIR = PROJECT_ROOT.parent / "data" / TEST_USER_ID / TEST_PROJECT_NAME
TEST_DATA_DIR = Path(os.getenv(DATA_DIR_ENV_KEY, str(DEFAULT_DATA_DIR))).expanduser()

SPFile = Tuple[str, str]
SectionResult = Dict[str, float | int | str]
SectionRunner = Callable[[ServiceOrchestrator, Any, List[SPFile] | None], Awaitable[SectionResult]]

SECTION_PIPELINE = "pipeline"
SECTION_DDL_ONLY = "ddl_only"
SECTION_SP_ONLY = "sp_only"


def _is_truthy_env(value: str | None) -> bool:
    if value is None:
        return False
    return value.strip().lower() in {"1", "true", "yes", "on"}


MERGE_MODE_ENABLED = _is_truthy_env(os.getenv("TEST_MERGE_MODE"))
_MERGE_MODE_NOTICE_PRINTED = False


def _ensure_api_key():
    if not TEST_API_KEY:
        pytest.skip("LLM_API_KEY가 설정되지 않았습니다")


def _create_orchestrator() -> ServiceOrchestrator:
    return ServiceOrchestrator(
        user_id=TEST_USER_ID,
        api_key=TEST_API_KEY,
        locale=TEST_LOCALE,
        project_name=TEST_PROJECT_NAME,
        dbms=TEST_DBMS,
        analysis_strategy=TEST_ANALYSIS_STRATEGY,
    )


async def _clear_graph(connection, user_id: str, project_name: str):
    """테스트 독립성을 확보하기 위해 지정 사용자/프로젝트 데이터를 삭제합니다."""
    await connection.execute_queries([
        f"MATCH (n {{user_id: '{user_id}', project_name: '{project_name}'}}) DETACH DELETE n"
    ])


async def _reset_graph_if_needed(connection, user_id: str, project_name: str):
    """MERGE 모드 여부에 따라 그래프 초기화를 결정합니다."""
    global _MERGE_MODE_NOTICE_PRINTED
    if MERGE_MODE_ENABLED:
        if not _MERGE_MODE_NOTICE_PRINTED:
            print("[INFO] TEST_MERGE_MODE=1 → 그래프 초기화를 건너뜁니다(누적 적재)")
            _MERGE_MODE_NOTICE_PRINTED = True
        return
    await _clear_graph(connection, user_id, project_name)


def _load_sp_files(data_dir: Path, *, skip_when_missing: bool = False) -> List[tuple[str, str]]:
    """analysis JSON 이 존재하는 SP 파일 목록을 폴더/파일 튜플 형태로 반환합니다."""
    src_dir = data_dir / "src"
    analysis_dir = data_dir / "analysis"

    def _fail(message: str):
        if skip_when_missing:
            pytest.skip(message)
        raise AssertionError(message)

    if not data_dir.exists():
        _fail(f"테스트 데이터 디렉토리가 없습니다: {data_dir}")
    if not src_dir.exists():
        _fail(f"src 디렉토리가 없습니다: {src_dir}")
    if not analysis_dir.exists():
        _fail(f"analysis 디렉토리가 없습니다: {analysis_dir}")

    def has_matching_json(system_name: str, sql_path: Path) -> bool:
        if not sql_path.is_file() or sql_path.suffix.lower() != ".sql":
            return False
        base = sql_path.stem
        return (analysis_dir / system_name / f"{base}.json").exists()

    sp_files: List[tuple[str, str]] = []
    seen: set[tuple[str, str]] = set()

    for folder in sorted(src_dir.iterdir()):
        if folder.is_dir():
            for sql_file in sorted(folder.iterdir()):
                if has_matching_json(folder.name, sql_file):
                    key = (folder.name, sql_file.name)
                    if key not in seen:
                        seen.add(key)
                        sp_files.append(key)

    for sql_file in sorted(src_dir.iterdir()):
        if sql_file.is_file() and has_matching_json("SYSTEM", sql_file):
            key = ("SYSTEM", sql_file.name)
            if key not in seen:
                seen.add(key)
                sp_files.append(key)

    if not sp_files:
        _fail(f"SP 파일이 없습니다: {src_dir}")
    return sp_files


@pytest_asyncio.fixture
async def real_neo4j():
    """테스트용 Neo4j 연결을 생성하고 종료 후 복구합니다."""
    from understand.neo4j_connection import Neo4jConnection

    original_db = Neo4jConnection.DATABASE_NAME
    Neo4jConnection.DATABASE_NAME = TEST_DB_NAME

    conn = Neo4jConnection()
    await _clear_graph(conn, TEST_USER_ID, TEST_PROJECT_NAME)

    try:
        yield conn
    finally:
        await conn.close()
        Neo4jConnection.DATABASE_NAME = original_db


async def _run_pipeline_section(orchestrator, _connection: Any, sp_files: List[SPFile] | None):
    if not sp_files:
        pytest.skip("SP 파일 혹은 분석 JSON이 없어 테스트를 건너뜁니다")

    start = time.perf_counter()
    event_count = 0
    async for _chunk in orchestrator.understand_project(list(sp_files)):
        event_count += 1
    elapsed = time.perf_counter() - start

    assert event_count > 0, "파이프라인 실행에서 이벤트가 생성되지 않았습니다"

    return {
        "elapsed_seconds": elapsed,
        "event_count": event_count,
        "files": len(sp_files),
    }


async def _run_ddl_only_section(orchestrator, connection, _sp_files: List[SPFile] | None = None):
    ddl_files = orchestrator._list_ddl_files()
    if not ddl_files:
        pytest.skip("DDL 파일이 없어 테스트를 건너뜁니다")

    ddl_dir = Path(orchestrator.dirs["ddl"])
    start = time.perf_counter()
    for file_name in ddl_files:
        await orchestrator._process_ddl(str(ddl_dir / file_name), connection, file_name)
    elapsed = time.perf_counter() - start

    count = (await connection.execute_queries([
        f"MATCH (t:Table {{user_id: '{TEST_USER_ID}', project_name: '{TEST_PROJECT_NAME}'}}) RETURN count(t) AS c"
    ]))[0][0]["c"]
    table_count = int(count)
    assert table_count > 0, "DDL 적재 후 Table 노드가 생성되지 않았습니다"

    return {
        "elapsed_seconds": elapsed,
        "ddl_files": len(ddl_files),
        "table_count": table_count,
    }


async def _run_sp_only_section(orchestrator, connection, sp_files: List[SPFile] | None):
    if not sp_files:
        pytest.skip("SP 파일 혹은 분석 JSON이 없어 테스트를 건너뜁니다")

    start = time.perf_counter()
    event_count = 0
    original_list_ddl = orchestrator._list_ddl_files
    orchestrator._list_ddl_files = lambda: []
    try:
        async for _chunk in orchestrator.understand_project(list(sp_files)):
            event_count += 1
    finally:
        orchestrator._list_ddl_files = original_list_ddl

    elapsed = time.perf_counter() - start

    sys_count = (await connection.execute_queries([
        f"MATCH (s:SYSTEM {{user_id: '{TEST_USER_ID}', project_name: '{TEST_PROJECT_NAME}'}}) RETURN count(s) AS c"
    ]))[0][0]["c"]
    system_nodes = int(sys_count)

    assert event_count > 0, "SP 분석 이벤트가 생성되지 않았습니다"
    assert system_nodes > 0, "SP 분석 후 SYSTEM 노드가 생성되지 않았습니다"

    return {
        "elapsed_seconds": elapsed,
        "event_count": event_count,
        "files": len(sp_files),
        "system_nodes": system_nodes,
    }


def _run_summary_log(results: Iterable[Dict[str, float | int | str]]):
    """섹션별 실행 결과를 읽기 좋은 로그 형식으로 출력합니다."""
    if not results:
        return

    lines = ["\n[UNDERSTANDING TEST RESULT]"]
    for item in results:
        line = f"  - section={item['section']} elapsed={item['elapsed_seconds']:.2f}s"
        if "event_count" in item:
            line += f" events={item['event_count']}"
        if "files" in item:
            line += f" files={item['files']}"
        if "ddl_files" in item:
            line += f" ddl_files={item['ddl_files']}"
        if "table_count" in item:
            line += f" tables={item['table_count']}"
        if "system_nodes" in item:
            line += f" system_nodes={item['system_nodes']}"
        lines.append(line)
    print("\n".join(lines))


async def _execute_section(
    real_neo4j,
    section: str,
    runner: SectionRunner,
    *,
    sp_files: List[SPFile] | None = None,
):
    orchestrator = _create_orchestrator()
    await _reset_graph_if_needed(real_neo4j, TEST_USER_ID, TEST_PROJECT_NAME)
    section_result = await runner(orchestrator, real_neo4j, sp_files)
    _run_summary_log([{"section": section, **section_result}])


@pytest.mark.asyncio
async def test_understanding_pipeline_section(real_neo4j):
    """DDL + SP 전체 파이프라인을 실행합니다."""
    _ensure_api_key()
    sp_files = _load_sp_files(TEST_DATA_DIR)
    await _execute_section(
        real_neo4j,
        SECTION_PIPELINE,
        _run_pipeline_section,
        sp_files=sp_files,
    )


@pytest.mark.asyncio
async def test_understanding_ddl_only_section(real_neo4j):
    """DDL 단계만 실행합니다."""
    _ensure_api_key()
    await _execute_section(
        real_neo4j,
        SECTION_DDL_ONLY,
        _run_ddl_only_section,
    )


@pytest.mark.asyncio
async def test_understanding_sp_only_section(real_neo4j):
    """SP 단계만 실행합니다."""
    _ensure_api_key()
    sp_files = _load_sp_files(TEST_DATA_DIR, skip_when_missing=True)
    await _execute_section(
        real_neo4j,
        SECTION_SP_ONLY,
        _run_sp_only_section,
        sp_files=sp_files,
    )

