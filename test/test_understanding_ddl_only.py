"""
DDL 단계만 별도로 Neo4j 적재를 검증하는 테스트.

전제:
- 환경 변수 TEST_USER_ID, TEST_PROJECT_NAME, TEST_DB_NAME, TEST_LOCALE, TEST_DBMS, LLM_API_KEY 사용
- 데이터 디렉터리 구조는 기존 파이프라인과 동일
- DDL 테스트는 data/{user}/{project}/ddl 하위의 파일을 대상으로 함
- TEST_MERGE_MODE=1 일 때 그래프 초기화를 건너뛰어 누적 적재(병합 모드)로 동작
"""

from __future__ import annotations

import os
from pathlib import Path

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


@pytest.mark.asyncio
async def test_ingest_ddl_only(neo4j_conn: Neo4jConnection):
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

    ddl_files = orch._list_ddl_files()  # 내부 유틸 사용
    if not ddl_files:
        pytest.skip("DDL 파일이 없어 테스트를 건너뜁니다")

    ddl_dir = Path(orch.dirs["ddl"])  # type: ignore[index]
    for ddl in ddl_files:
        await orch._process_ddl(str(ddl_dir / ddl), neo4j_conn, ddl)

    # 최소한의 생성 검증: Table 노드 존재
    count = (await neo4j_conn.execute_queries([
        f"MATCH (t:Table {{user_id: '{TEST_USER_ID}', project_name: '{TEST_PROJECT_NAME}'}}) RETURN count(t) AS c"
    ]))[0][0]["c"]
    assert int(count) > 0, "DDL 적재 후 Table 노드가 생성되지 않았습니다"
