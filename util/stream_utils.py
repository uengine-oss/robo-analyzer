"""스트리밍 유틸리티

NDJSON 스트리밍 이벤트 생성 및 처리 함수들.

이벤트 타입:
- message: 사용자 메시지
- data: 그래프 데이터
- error: 오류
- node_event: 노드 생성/수정 이벤트
- relationship_event: 관계 생성/수정 이벤트
- phase_event: 단계 진행 이벤트
- complete: 완료

메시지 설계 원칙 (pdb.md):
- 이 메시지는 개발자 로그가 아니라, 사용자에게 보여지는 진행 설명이다
- 조건 분기 결과가 명확히 드러날 것
- 실패/스킵/대체 상황이 숨겨지지 않을 것
- "무슨 일이 일어났는지"를 자연어로 설명할 것
"""

import json
import uuid
import logging
from collections import Counter
from typing import Any, Optional, AsyncGenerator


def emit_bytes(payload: dict) -> bytes:
    """NDJSON 형식으로 바이트 생성"""
    return json.dumps(payload, default=str, ensure_ascii=False).encode("utf-8") + b"\n"


def emit_message(content: str) -> bytes:
    """메시지 이벤트 전송
    
    사용자 친화적 메시지 작성 가이드:
    - 📄 파일 처리: "📄 Sample.java 분석 시작"
    - → 노드/관계 생성: " → CLASS 노드: OrderService"
    - ✓ 완료: "✓ 처리 완료"
    - ❌ 오류: "❌ 분석 실패"
    - ℹ️ 정보: "ℹ️ DDL 파일 없음"
    - ⚠️ 경고: "⚠️ 일부 파일 스킵"
    """
    return emit_bytes({"type": "message", "content": content})


def emit_error(
    content: str,
    error_type: Optional[str] = None,
    trace_id: Optional[str] = None,
) -> bytes:
    """에러 이벤트 전송"""
    payload = {"type": "error", "content": content}
    if error_type:
        payload["errorType"] = error_type
    if trace_id:
        payload["traceId"] = trace_id
    return emit_bytes(payload)


def emit_data(**fields) -> bytes:
    """데이터 이벤트 전송
    
    Args:
        **fields: 이벤트에 포함할 필드들
            - graph: {"Nodes": [...], "Relationships": [...]}
            - line_number: 현재 라인
            - analysis_progress: 진행률 (0-100)
            - current_file: 현재 파일명
    """
    payload = {"type": "data"}
    payload.update({k: v for k, v in fields.items() if v is not None})
    return emit_bytes(payload)


def emit_node_event(
    action: str,
    node_type: str,
    node_name: str,
    details: Optional[dict[str, Any]] = None,
) -> bytes:
    """노드 생성/수정 이벤트 전송
    
    Args:
        action: "created", "updated", "deleted"
        node_type: "CLASS", "METHOD", "PROCEDURE", "Table" 등
        node_name: 노드 이름
        details: 추가 상세 정보
    """
    payload = {
        "type": "node_event",
        "action": action,
        "nodeType": node_type,
        "nodeName": node_name,
    }
    if details:
        payload["details"] = details
    return emit_bytes(payload)


def emit_relationship_event(
    action: str,
    rel_type: str,
    source: str,
    target: str,
    details: Optional[dict[str, Any]] = None,
) -> bytes:
    """관계 생성/수정 이벤트 전송
    
    Args:
        action: "created", "updated", "deleted"
        rel_type: "CALLS", "PARENT_OF", "FROM", "WRITES" 등
        source: 소스 노드 이름
        target: 타겟 노드 이름
        details: 추가 상세 정보
    """
    payload = {
        "type": "relationship_event",
        "action": action,
        "relType": rel_type,
        "source": source,
        "target": target,
    }
    if details:
        payload["details"] = details
    return emit_bytes(payload)


def emit_complete(summary: Optional[str] = None) -> bytes:
    """완료 이벤트 전송"""
    payload = {"type": "complete"}
    if summary:
        payload["summary"] = summary
    return emit_bytes(payload)


def emit_canvas_update(
    update_type: str,
    table_name: str,
    schema: str = "public",
    field: Optional[str] = None,
    changes: Optional[dict[str, Any]] = None,
) -> bytes:
    """캔버스 실시간 업데이트 이벤트 전송
    
    캔버스에 표시된 테이블 관련 변경사항을 실시간으로 알림.
    
    Args:
        update_type: "table_description", "column_description", "relationship_added", 
                     "column_added", "table_added"
        table_name: 업데이트된 테이블 이름
        schema: 스키마 이름 (기본: public)
        field: 컬럼명 (컬럼 업데이트 시)
        changes: 변경 내용 {"description": "...", "analyzed_description": "..." 등}
    """
    payload = {
        "type": "canvas_update",
        "updateType": update_type,
        "tableName": table_name,
        "schema": schema,
    }
    if field:
        payload["field"] = field
    if changes:
        payload["changes"] = changes
    return emit_bytes(payload)


def emit_phase_event(
    phase_num: int,
    phase_name: str,
    status: str,
    progress: int = 0,
    details: Optional[dict[str, Any]] = None,
) -> bytes:
    """단계 진행 이벤트 전송
    
    Args:
        phase_num: 단계 번호 (1, 2, 3 ...)
        phase_name: 단계 이름 ("AST 구조 생성", "AI 분석" 등)
        status: "started", "in_progress", "completed", "skipped", "error"
        progress: 진행률 (0-100)
        details: 추가 상세 정보
    """
    payload = {
        "type": "phase_event",
        "phase": phase_num,
        "name": phase_name,
        "status": status,
        "progress": progress,
    }
    if details:
        payload["details"] = details
    return emit_bytes(payload)


def format_graph_result(graph: dict) -> str:
    """Neo4j 그래프 결과를 사용자 친화적 메시지로 변환
    
    Args:
        graph: {"Nodes": [...], "Relationships": [...]}
        
    Returns:
        포맷팅된 메시지 문자열
    """
    nodes = graph.get("Nodes", [])
    rels = graph.get("Relationships", [])
    
    if not nodes and not rels:
        return ""
    
    # 노드 타입별 집계
    node_types = Counter(
        (n.get("Labels") or ["Unknown"])[0] for n in nodes
    )
    
    # 관계 타입별 집계
    rel_types = Counter(
        r.get("Type", "Unknown") for r in rels
    )
    
    messages = []
    for label, count in node_types.items():
        messages.append(f"  → {label} 노드 {count}개 생성")
    for rel_type, count in rel_types.items():
        messages.append(f"  → {rel_type} 관계 {count}개 연결")
    
    return "\n".join(messages)


def build_error_body(
    exc: Exception,
    trace_id: Optional[str] = None,
    message: Optional[str] = None,
) -> dict:
    """비스트리밍 500 응답용 표준 에러 바디 생성"""
    return {
        "errorType": exc.__class__.__name__,
        "message": message or str(exc),
        "traceId": trace_id or f"req-{uuid.uuid4()}",
    }


async def stream_with_error_boundary(
    async_gen: AsyncGenerator[bytes, None],
) -> AsyncGenerator[bytes, None]:
    """스트리밍 처리 경계
    
    예외 발생 시 에러 이벤트 전송 후 안전하게 종료.
    
    Args:
        async_gen: 원본 비동기 제너레이터
        
    Yields:
        원본 데이터 및 완료/에러 이벤트
    """
    trace_id = f"stream-{uuid.uuid4().hex[:8]}"
    
    try:
        async for chunk in async_gen:
            yield chunk
        yield emit_complete()
    except GeneratorExit:
        logging.info(f"[{trace_id}] 클라이언트 연결 종료")
    except Exception as e:
        error_msg = f"{e.__class__.__name__}: {str(e)}"
        logging.error(f"[{trace_id}] 스트림 에러: {error_msg}", exc_info=True)
        yield emit_error(error_msg, error_type=e.__class__.__name__, trace_id=trace_id)

