"""
SP(PL/SQL) 단계만 별도로 Neo4j 적재를 검증하는 테스트.

전제:
- 환경 변수 TEST_USER_ID, TEST_PROJECT_NAME, TEST_DB_NAME, TEST_LOCALE, TEST_DBMS, LLM_API_KEY 사용
- 데이터 디렉터리 구조는 기존 파이프라인과 동일
- SP 테스트는 analysis/{SYSTEM}/{base}.json 이 존재하는 src/{SYSTEM}/{base}.sql 을 대상으로 함
- TEST_MERGE_MODE=1 일 때 그래프 초기화를 건너뜁니다(누적 적재)
"""

from __future__ import annotations

import os
from pathlib import Path
from typing import List, Tuple

import pytest
import pytest_asyncio

# 프로젝트 루트 추가
PROJECT_ROOT = Path(__file__).resolve().parents[1]
import sys
sys.path.insert(0, str(PROJECT_ROOT))

from service.service import ServiceOrchestrator  # noqa: E402
from understand.neo4j_connection import Neo4jConnection  # noqa: E402

# 환경 변수 로드
TEST_USER_ID = os.getenv("TEST_USER_ID", "KO_TestSession")
TEST_PROJECT_NAME = os.getenv("TEST_PROJECT_NAME", "HOSPITAL_MANAGEMENT")
TEST_API_KEY = os.getenv("LLM_API_KEY")
TEST_DB_NAME = os.getenv("TEST_DB_NAME", "test")
TEST_LOCALE = os.getenv("TEST_LOCALE", "ko")
TEST_DBMS = os.getenv("TEST_DBMS", "postgres")

TEST_DATA_DIR = PROJECT_ROOT.parent / "data" / TEST_USER_ID / TEST_PROJECT_NAME


def _is_merge_mode() -> bool:
    val = os.getenv("TEST_MERGE_MODE", "").strip().lower()
    return val not in ("", "0", "false", "no")


@pytest_asyncio.fixture
async def neo4j_conn():
    """테스트용 Neo4j 연결을 생성하고 종료 후 복구합니다."""
    original_db = Neo4jConnection.DATABASE_NAME
    Neo4jConnection.DATABASE_NAME = TEST_DB_NAME
    conn = Neo4jConnection()
    try:
        yield conn
    finally:
        await conn.close()
        Neo4jConnection.DATABASE_NAME = original_db


async def _clear_graph(conn: Neo4jConnection, user_id: str, project_name: str) -> None:
    await conn.execute_queries([
        f"MATCH (n {{user_id: '{user_id}', project_name: '{project_name}'}}) DETACH DELETE n"
    ])


def _load_sp_files(data_dir: Path) -> List[Tuple[str, str]]:
    """analysis JSON이 존재하는 SP 파일만 필터링하여 반환합니다.

    반환: [(system, file_name), ...]
    """
    src_dir = data_dir / "src"
    analysis_dir = data_dir / "analysis"
    if not data_dir.exists() or not src_dir.exists():
        return []

    def has_matching_json(system_name: str, file_path: Path) -> bool:
        if not file_path.is_file() or file_path.suffix.lower() == ".json":
            return False
        base = file_path.stem
        json_path = analysis_dir / system_name / f"{base}.json"
        return json_path.exists()

    sp_files: List[Tuple[str, str]] = []

    # src/{SYSTEM}/*
    for folder in sorted(src_dir.iterdir()):
        if folder.is_dir():
            for any_file in sorted(folder.iterdir()):
                if has_matching_json(folder.name, any_file):
                    sp_files.append((folder.name, any_file.name))
    # src/* → SYSTEM
    for any_file in sorted(src_dir.iterdir()):
        if any_file.is_file() and has_matching_json("SYSTEM", any_file):
            sp_files.append(("SYSTEM", any_file.name))

    return sp_files


@pytest.mark.asyncio
async def test_ingest_sp_only(neo4j_conn: Neo4jConnection, monkeypatch: pytest.MonkeyPatch):
    if not TEST_API_KEY:
        pytest.skip("LLM_API_KEY가 설정되지 않았습니다")

    if _is_merge_mode():
        print("[INFO] TEST_MERGE_MODE=1 → 그래프 초기화를 건너뜁니다(누적 적재)")
    else:
        await _clear_graph(neo4j_conn, TEST_USER_ID, TEST_PROJECT_NAME)

    orch = ServiceOrchestrator(
        user_id=TEST_USER_ID,
        api_key=TEST_API_KEY,
        locale=TEST_LOCALE,
        project_name=TEST_PROJECT_NAME,
        dbms=TEST_DBMS,
    )

    # SP 대상 파일 수집 (analysis JSON 있는 것만)
    sp_files = _load_sp_files(TEST_DATA_DIR)
    if not sp_files:
        pytest.skip("SP 파일 혹은 대응 JSON이 없어 테스트를 건너뜁니다")

    # DDL 단계는 건너뛰도록 monkeypatch
    monkeypatch.setattr(orch, "_list_ddl_files", lambda: [])

    # 이해 파이프라인 실행 (SP만)
    event_count = 0
    async for _ in orch.understand_project(sp_files):
        event_count += 1

    # 최소 검증: SYSTEM 노드 생성 및 이벤트 발생
    sys_count = (await neo4j_conn.execute_queries([
        f"MATCH (s:SYSTEM {{user_id: '{TEST_USER_ID}', project_name: '{TEST_PROJECT_NAME}'}}) RETURN count(s) AS c"
    ]))[0][0]["c"]
    assert int(sys_count) > 0, "SP 분석 후 SYSTEM 노드가 생성되지 않았습니다"
    assert event_count > 0, "SP 분석 이벤트가 생성되지 않았습니다"
