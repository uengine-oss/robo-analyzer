import json
import os
import threading
from datetime import datetime
from typing import Any, Optional

from langchain.callbacks import get_openai_callback

__all__ = [
    "reset_audit_log",
    "log_llm_interaction",
    "invoke_with_audit",
    "ainvoke_with_audit",
]


_BASE_DIR = os.getenv("DOCKER_COMPOSE_CONTEXT") or os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
_AUDIT_DIR = os.getenv("LLM_AUDIT_DIR") or os.path.join(_BASE_DIR, "logs")
_AUDIT_FILE = os.getenv("LLM_AUDIT_FILE") or os.path.join(_AUDIT_DIR, "llm_audit.json")
_FILE_LOCK = threading.Lock()


def reset_audit_log() -> None:
    os.makedirs(_AUDIT_DIR, exist_ok=True)
    with _FILE_LOCK:
        with open(_AUDIT_FILE, "w", encoding="utf-8") as fp:
            json.dump({"prompts": {}}, fp, ensure_ascii=False, indent=2)


def _load_audit_log() -> dict[str, Any]:
    try:
        with open(_AUDIT_FILE, "r", encoding="utf-8") as fp:
            data = json.load(fp)
            if not isinstance(data, dict):
                return {"prompts": {}}
            data.setdefault("prompts", {})
            return data
    except FileNotFoundError:
        return {"prompts": {}}
    except json.JSONDecodeError:
        return {"prompts": {}}


def _write_audit_log(data: dict[str, Any]) -> None:
    os.makedirs(_AUDIT_DIR, exist_ok=True)
    with open(_AUDIT_FILE, "w", encoding="utf-8") as fp:
        json.dump(data, fp, ensure_ascii=False, indent=2)


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
        "timestamp": datetime.utcnow().isoformat() + "Z",
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
        data = _load_audit_log()
        prompts = data.setdefault("prompts", {})
        records = prompts.setdefault(prompt_name, [])
        records.append(entry)
        records.sort(key=_entry_sort_key)
        prompts[prompt_name] = records
        _write_audit_log(data)


def invoke_with_audit(
    chain,
    payload: dict[str, Any],
    prompt_name: str,
    *,
    input_payload: Optional[Any] = None,
    metadata: Optional[dict[str, Any]] = None,
    sort_key: Optional[Any] = None,
):
    with get_openai_callback() as callback:
        result = chain.invoke(payload)
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
):
    with get_openai_callback() as callback:
        result = await chain.ainvoke(payload)
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


