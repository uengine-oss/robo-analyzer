"""LLM 호출 감사 로깅

LLM 호출을 기록하고 디버깅을 위한 로그를 생성합니다.
"""

import json
import os
import re
import threading
from datetime import datetime, timezone
from typing import Any, Optional

try:
    from langchain_community.callbacks import get_openai_callback
except ImportError:  # pragma: no cover
    from langchain.callbacks import get_openai_callback

from config.settings import settings

__all__ = [
    "reset_audit_log",
    "log_llm_interaction",
    "invoke_with_audit",
    "ainvoke_with_audit",
]

_AUDIT_DIR = settings.path.audit_dir
_PROMPT_LOG_DIR = settings.path.prompt_log_dir
_FILE_LOCK = threading.Lock()
_CLEARED_PROMPTS: set[str] = set()


def reset_audit_log() -> None:
    os.makedirs(_AUDIT_DIR, exist_ok=True)
    os.makedirs(_PROMPT_LOG_DIR, exist_ok=True)
    with _FILE_LOCK:
        _CLEARED_PROMPTS.clear()
        for filename in os.listdir(_PROMPT_LOG_DIR):
            if filename.endswith(".json"):
                try:
                    os.remove(os.path.join(_PROMPT_LOG_DIR, filename))
                except OSError:
                    pass


def _sanitize_prompt_name(prompt_name: str) -> str:
    safe = re.sub(r"[^A-Za-z0-9._-]+", "_", prompt_name)
    safe = safe.strip("_")
    return safe or "prompt"


def _prompt_log_path(prompt_name: str) -> str:
    os.makedirs(_PROMPT_LOG_DIR, exist_ok=True)
    safe_name = _sanitize_prompt_name(prompt_name)
    return os.path.join(_PROMPT_LOG_DIR, f"{safe_name}.json")


def _load_prompt_log(prompt_name: str) -> list[dict[str, Any]]:
    path = _prompt_log_path(prompt_name)
    try:
        with open(path, "r", encoding="utf-8") as fp:
            records = json.load(fp)
            return records if isinstance(records, list) else []
    except FileNotFoundError:
        return []
    except json.JSONDecodeError:
        return []


def _write_prompt_log(prompt_name: str, records: list[dict[str, Any]]) -> None:
    path = _prompt_log_path(prompt_name)
    with open(path, "w", encoding="utf-8") as fp:
        json.dump(records, fp, ensure_ascii=False, indent=2)


def _safe_serialize(value: Any) -> Any:
    try:
        json.dumps(value, ensure_ascii=False)
        return value
    except TypeError:
        if isinstance(value, dict):
            return {k: _safe_serialize(v) for k, v in value.items()}
        if isinstance(value, (list, tuple, set)):
            return [_safe_serialize(v) for v in value]
        return str(value)


def _extract_token_usage(callback) -> Optional[dict[str, int]]:
    prompt_tokens = getattr(callback, "prompt_tokens", 0)
    completion_tokens = getattr(callback, "completion_tokens", 0)
    total_tokens = getattr(callback, "total_tokens", 0)
    if any(value for value in (prompt_tokens, completion_tokens, total_tokens)):
        return {
            "prompt": int(prompt_tokens or 0),
            "completion": int(completion_tokens or 0),
            "total": int(total_tokens or 0),
        }
    return None


def _entry_sort_key(entry: dict[str, Any]) -> tuple[Any, str]:
    sort_candidate = entry.get("sortKey")
    if sort_candidate is None and isinstance(entry.get("metadata"), dict):
        sort_candidate = entry["metadata"].get("startLine")
    timestamp = entry.get("timestamp", "")
    return (sort_candidate if sort_candidate is not None else float("inf"), timestamp)


def log_llm_interaction(
    prompt_name: str,
    input_payload: Any,
    output_payload: Any,
    token_usage: Optional[dict[str, int]] = None,
    metadata: Optional[dict[str, Any]] = None,
    sort_key: Optional[Any] = None,
) -> None:
    entry = {
        "timestamp": datetime.now(timezone.utc).isoformat(),
        "prompt": prompt_name,
        "input": _safe_serialize(input_payload),
        "output": _safe_serialize(output_payload),
    }
    if token_usage:
        entry["tokenUsage"] = token_usage
    if metadata:
        entry["metadata"] = _safe_serialize(metadata)
    if sort_key is not None:
        entry["sortKey"] = sort_key

    with _FILE_LOCK:
        if prompt_name not in _CLEARED_PROMPTS:
            path = _prompt_log_path(prompt_name)
            if os.path.exists(path):
                try:
                    os.remove(path)
                except OSError:
                    pass
            _CLEARED_PROMPTS.add(prompt_name)

        records = _load_prompt_log(prompt_name)
        records.append(entry)
        records.sort(key=_entry_sort_key)
        _write_prompt_log(prompt_name, records)


def invoke_with_audit(
    chain,
    payload: dict[str, Any],
    prompt_name: str,
    *,
    input_payload: Optional[Any] = None,
    metadata: Optional[dict[str, Any]] = None,
    sort_key: Optional[Any] = None,
    config: Optional[Any] = None
):
    with get_openai_callback() as callback:
        result = chain.invoke(payload, config=config)
    token_usage = _extract_token_usage(callback)
    log_llm_interaction(
        prompt_name,
        input_payload if input_payload is not None else payload,
        result,
        token_usage=token_usage,
        metadata=metadata,
        sort_key=sort_key,
    )
    return result


async def ainvoke_with_audit(
    chain,
    payload: dict[str, Any],
    prompt_name: str,
    *,
    input_payload: Optional[Any] = None,
    metadata: Optional[dict[str, Any]] = None,
    sort_key: Optional[Any] = None,
    config: Optional[Any] = None
):
    with get_openai_callback() as callback:
        result = await chain.ainvoke(payload, config=config)
    token_usage = _extract_token_usage(callback)
    log_llm_interaction(
        prompt_name,
        input_payload if input_payload is not None else payload,
        result,
        token_usage=token_usage,
        metadata=metadata,
        sort_key=sort_key,
    )
    return result


