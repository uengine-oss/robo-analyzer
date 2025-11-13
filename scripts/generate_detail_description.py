import sys
from pathlib import Path

# 프로젝트 루트를 PYTHONPATH에 추가
_BASE_DIR = Path(__file__).resolve().parents[1]
if str(_BASE_DIR) not in sys.path:
    sys.path.insert(0, str(_BASE_DIR))
import os
import asyncio
from typing import List, Dict, Any

from understand.neo4j_connection import Neo4jConnection
from util.utility_tool import escape_for_cypher


def _dedupe_preserve_order(items: List[str]) -> List[str]:
    seen = set()
    ordered: List[str] = []
    for item in items or []:
        key = str(item)
        if key in seen:
            continue
        seen.add(key)
        ordered.append(key)
    return ordered


def _build_detail_text(table_description: str, columns: List[Dict[str, Any]]) -> str:
    # 설명 라인
    desc_line = f"설명: {table_description or ''}".rstrip()

    lines: List[str] = [desc_line, "주요  컬럼:"]

    # 컬럼 정렬(이름 기준)
    def _col_name(c: Dict[str, Any]) -> str:
        return (c.get("name") or "").lower()

    for col in sorted(columns or [], key=_col_name):
        role = (col.get("description") or "").strip()
        examples_raw = col.get("examples") or []
        examples = [str(v).strip() for v in examples_raw if v is not None and str(v).strip()]
        examples = _dedupe_preserve_order(examples)

        text = role
        if examples:
            text = f"{text} (예: {', '.join(examples)})" if text else f"(예: {', '.join(examples)})"
        lines.append(f"   {text}".rstrip())

    return "\n".join(lines)


async def generate_and_update_detail_descriptions(user_id: str, project_name: str | None = None) -> None:
    connection = Neo4jConnection()
    try:
        user_esc = escape_for_cypher(user_id)
        proj_cond = "true" if not project_name else f"t.project_name = '{escape_for_cypher(project_name)}'"

        # 테이블과 컬럼(핵심 속성 + examples 가능 시)을 한 번에 조회
        read_query = f"""
        MATCH (t:Table {{user_id: '{user_esc}'}})
        WHERE {proj_cond}
        OPTIONAL MATCH (t)-[:HAS_COLUMN]->(c:Column {{user_id: '{user_esc}'}})
        WITH t, collect(c) AS cols
        RETURN elementId(t) AS tid,
               coalesce(t.description,'') AS tdesc,
               [x IN cols | {{ name: x.name, description: coalesce(x.description,''), examples: x.examples }}] AS columns
        """

        read_results_wrapped = await connection.execute_queries([read_query])
        read_rows = read_results_wrapped[0] if read_results_wrapped else []

        update_queries: List[str] = []
        for row in read_rows:
            tid = row.get("tid")
            tdesc = row.get("tdesc") or ""
            columns = row.get("columns") or []

            detail_text = _build_detail_text(tdesc, columns)
            if not detail_text:
                continue

            detail_escaped = escape_for_cypher(detail_text)
            update_queries.append(
                f"MATCH (t) WHERE elementId(t) = '{tid}' SET t.detailDescription = '{detail_escaped}'"
            )

        if update_queries:
            await connection.execute_queries(update_queries)

    finally:
        await connection.close()

if __name__ == "__main__":
    uid = os.getenv("TEST_USER_ID") or os.getenv("UE_USER_ID") or ""
    project = os.getenv("TEST_PROJECT_NAME") or os.getenv("UE_PROJECT_NAME")

    if not uid:
        raise SystemExit("환경변수 USER_ID 가 필요합니다. 예) set USER_ID=myuser")

    asyncio.run(generate_and_update_detail_descriptions(uid, project))


