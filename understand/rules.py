import asyncio
import json
from typing import Any, Dict, List, Optional

from util.rule_loader import RuleLoader


# 공통 RuleLoader (understand/domain=dbms)
_loader = RuleLoader(domain="understand", target_lang="dbms")


def _normalize_analysis_structure(obj: dict) -> dict:
    """analysis 구조의 누락 필드를 기본값으로 보정합니다."""
    if not isinstance(obj, dict):
        return {"analysis": []}
    analysis = obj.get("analysis")
    if not isinstance(analysis, list):
        analysis = []
    normalized = []
    for item in analysis:
        if not isinstance(item, dict):
            continue
        item.setdefault("localTables", [])
        item.setdefault("calls", [])
        item.setdefault("variables", [])
        item.setdefault("fkRelations", [])
        item.setdefault("dbLinks", [])
        normalized.append(item)
    obj["analysis"] = normalized
    return obj


def understand_code(sp_code: str, context_ranges: List[Dict[str, Any]], context_range_count: int, api_key: str, locale: str) -> Dict[str, Any]:
    payload = {
        "code": sp_code,
        "ranges": json.dumps(context_ranges, ensure_ascii=False),
        "count": context_range_count,
        "locale": locale,
    }
    result = _loader.execute("analysis", payload, api_key=api_key)
    return _normalize_analysis_structure(result if isinstance(result, dict) else {})


def understand_dml_tables(code: str, ranges: List[Dict[str, Any]], api_key: str, locale: str) -> Dict[str, Any]:
    payload = {
        "code": code,
        "ranges": json.dumps(ranges, ensure_ascii=False),
        "locale": locale,
    }
    result = _loader.execute("dml", payload, api_key=api_key)
    if not isinstance(result, dict):
        return {"ranges": []}
    result.setdefault("ranges", [])
    return result


def understand_summary(summaries: List[Any], api_key: str, locale: str) -> Dict[str, Any]:
    payload = {
        "summaries": json.dumps(summaries, ensure_ascii=False),
        "locale": locale,
    }
    result = _loader.execute("procedure_summary", payload, api_key=api_key)
    return result if isinstance(result, dict) else {}


def summarize_table_metadata(
    table_name: str,
    table_sentences: List[str],
    column_sentences: Dict[str, List[str]],
    column_metadata: Dict[str, Dict[str, Any]],
    api_key: str,
    locale: str,
) -> Dict[str, Any]:
    payload = {
        "table_name": table_name,
        "table_sentences": "\n".join(table_sentences or []),
        "column_sentences": json.dumps(column_sentences or {}, ensure_ascii=False),
        "column_metadata": json.dumps(column_metadata or {}, ensure_ascii=False),
        "locale": locale,
    }
    result = _loader.execute("table_summary", payload, api_key=api_key)
    if not isinstance(result, dict):
        return {"tableDescription": "", "detailDescription": "", "columns": []}
    result.setdefault("tableDescription", "")
    result.setdefault("detailDescription", "")
    result.setdefault("columns", [])
    return result


def understand_variables(declaration_code: str, api_key: str, locale: str) -> Dict[str, Any]:
    payload = {"declaration_code": declaration_code, "locale": locale}
    result = _loader.execute("variables", payload, api_key=api_key)
    return result if isinstance(result, dict) else {}


async def resolve_table_variable_type(
    var_name: str,
    declared_type: str,
    table_schema: Optional[str],
    table_name: Optional[str],
    columns: Optional[List[Any]],
    api_key: str,
    locale: str,
) -> Dict[str, Any]:
    payload = {
        "var_name": var_name,
        "declared_type": declared_type,
        "table_schema": table_schema or "",
        "table_name": table_name or "",
        "columns_json": json.dumps(columns or [], ensure_ascii=False),
        "locale": locale,
    }
    # RuleLoader.execute는 동기 함수이므로 to_thread로 감싸 비동기 인터페이스 유지
    return await asyncio.to_thread(_loader.execute, "variable_type_resolve", payload, api_key)


async def understand_column_roles(columns: List[Any], dml_summaries: List[Any], api_key: str, locale: str) -> Dict[str, Any]:
    payload = {
        "columns_json": json.dumps(columns or [], ensure_ascii=False),
        "dml_summaries_json": json.dumps([s for s in (dml_summaries or []) if s], ensure_ascii=False),
        "locale": locale,
    }
    return await asyncio.to_thread(_loader.execute, "column", payload, api_key)


def understand_ddl(ddl_content: str, api_key: str, locale: str) -> Dict[str, Any]:
    payload = {"ddl_content": ddl_content, "locale": locale}
    result = _loader.execute("ddl", payload, api_key=api_key)
    return result if isinstance(result, dict) else {}

