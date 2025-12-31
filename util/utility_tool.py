"""
utility_tool.py - Robo Analyzer 유틸리티 모듈

소스 코드 분석을 위한 핵심 유틸리티 함수들을 제공합니다.
토큰 계산, 스트리밍 이벤트, 문자열 처리 등의 기능을 포함합니다.
"""

import os
import logging
import json
import uuid
import tiktoken
from typing import Optional, Dict, List, Any, Union

from util.exception import UtilProcessingError


def log_process(context: str, stage: str, message: str, level: int = logging.INFO, exc: Exception | None = None) -> None:
    """
    공통 파이프라인 로그 출력 헬퍼.
    - context: 'DBMS', 'FRAMEWORK' 등 분석 타입
    - stage: 논리적 단계 이름
    - message: 사용자 친화적 설명
    - level: logging 모듈 레벨
    - exc: 예외 객체 전달 시 스택 트레이스까지 출력
    """
    ctx = (context or "APP").upper()
    stage_text = (stage or "STAGE").upper()
    logging.log(level, f"[{ctx}:{stage_text}] {message}", exc_info=exc)


# tiktoken 인코더 초기화
ENCODER = tiktoken.get_encoding("cl100k_base")


#==============================================================================
# 스트리밍/이벤트 유틸리티 (NDJSON 표준)
#==============================================================================

def emit_bytes(payload: dict) -> bytes:
    """NDJSON 형식으로 스트림 전송용 바이트 생성."""
    return json.dumps(payload, default=str, ensure_ascii=False).encode('utf-8') + b'\n'


def emit_message(content: str) -> bytes:
    """message 이벤트 전송."""
    return emit_bytes({"type": "message", "content": content})


def emit_error(content: str, error_type: str = None, trace_id: str = None) -> bytes:
    """에러 이벤트 전송."""
    payload = {"type": "error", "content": content}
    if error_type:
        payload["errorType"] = error_type
    if trace_id:
        payload["traceId"] = trace_id
    return emit_bytes(payload)


def emit_data(**fields) -> bytes:
    """data 이벤트 전송. fields는 최상위 필드로 포함됨."""
    payload = {"type": "data"}
    payload.update({k: v for k, v in fields.items() if v is not None})
    return emit_bytes(payload)


def emit_node_event(
    action: str,
    node_type: str,
    node_name: str,
    details: Optional[Dict[str, Any]] = None
) -> bytes:
    """노드 생성/수정/업데이트 이벤트 전송.
    
    Args:
        action: 액션 타입 ("created", "updated", "deleted")
        node_type: 노드 타입 (예: "CLASS", "METHOD", "PROCEDURE", "Table")
        node_name: 노드 이름
        details: 추가 상세 정보 (선택)
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
    details: Optional[Dict[str, Any]] = None
) -> bytes:
    """관계 생성/수정/삭제 이벤트 전송.
    
    Args:
        action: 액션 타입 ("created", "updated", "deleted")
        rel_type: 관계 타입 (예: "CALLS", "PARENT_OF", "FROM", "WRITES")
        source: 소스 노드 이름
        target: 타겟 노드 이름
        details: 추가 상세 정보 (선택)
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


def emit_complete(summary: str = None) -> bytes:
    """스트림 완료 이벤트."""
    payload = {"type": "complete"}
    if summary:
        payload["summary"] = summary
    return emit_bytes(payload)


def build_error_body(exc: Exception, trace_id: str | None = None, message: str | None = None) -> dict:
    """비스트리밍 500 응답용 표준 에러 바디 생성."""
    return {
        "errorType": exc.__class__.__name__,
        "message": message or str(exc),
        "traceId": trace_id or f"req-{uuid.uuid4()}"
    }


async def stream_with_error_boundary(async_gen):
    """스트리밍 처리 경계. 예외 발생 시 에러 이벤트 전송 후 종료."""
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


#==============================================================================
# 문자열/JSON 유틸리티
#==============================================================================

def escape_for_cypher(text: str) -> str:
    """Cypher 쿼리용 문자열 이스케이프"""
    return str(text).replace("'", "\\'")


def parse_json_maybe(data):
    """JSON 문자열을 파싱하거나 리스트/딕셔너리는 그대로 반환"""
    if isinstance(data, str):
        return json.loads(data)
    return data or []


#==============================================================================
# 스키마/테이블 파싱 유틸리티
#==============================================================================

def parse_table_identifier(qualified_table_name: str) -> tuple[str, str, str | None]:
    """'SCHEMA.TABLE@DBLINK'에서 (schema, table, dblink) 추출"""
    if not qualified_table_name:
        return '', '', None
    
    text = qualified_table_name.strip()
    left, _, link = text.partition('@')
    s, _, t = left.partition('.')
    
    schema_raw = s.strip() if t else ''
    table_raw = t.strip() if t else left.strip()
    link_raw = link.strip() or None

    schema = (schema_raw or '').lower()
    table = (table_raw or '').lower()
    db_link = link_raw.lower() if link_raw else None

    return schema, table, db_link


#==============================================================================
# 코드 분석 유틸리티
#==============================================================================

def calculate_code_token(code: Union[str, Dict, List]) -> int:
    """코드 토큰 길이 계산"""
    try:
        if isinstance(code, str):
            text = code
        else:
            text = json.dumps(code, ensure_ascii=False)
        return len(ENCODER.encode(text))
    except Exception as e:
        err_msg = f"토큰 계산 도중 문제가 발생: {str(e)}"
        logging.error(err_msg)
        raise UtilProcessingError(err_msg)


#==============================================================================
# User Story 문서 생성 유틸리티
#==============================================================================

def generate_user_story_document(
    results: List[Dict[str, Any]],
    source_name: str = "",
    source_type: str = "프로시저"
) -> str:
    """Summary와 User Story를 포함한 상세한 마크다운 문서를 생성합니다.
    
    Args:
        results: Neo4j 쿼리 결과 리스트 (name, summary, user_stories, type 포함)
        source_name: 소스 이름 (프로젝트명 등)
        source_type: 소스 타입 ("DBMS 프로시저/함수", "Java 클래스/인터페이스" 등)
    
    Returns:
        마크다운 형식의 상세 문서 문자열
    """
    if not results:
        return ""
    
    lines = []
    
    # 헤더
    if source_name:
        lines.append(f"# {source_name} - 요구사항 분석 문서")
    else:
        lines.append("# 요구사항 분석 문서")
    lines.append("")
    lines.append(f"> {source_type}에서 도출된 상세 요약, 사용자 스토리 및 인수 조건")
    lines.append("")
    lines.append("---")
    lines.append("")
    lines.append("## 📋 목차")
    lines.append("")
    lines.append("1. [프로시저/클래스별 상세 요약](#프로시저클래스별-상세-요약)")
    lines.append("2. [User Stories & Acceptance Criteria](#user-stories--acceptance-criteria)")
    lines.append("")
    lines.append("---")
    lines.append("")
    
    # 1. 프로시저/클래스별 상세 요약
    lines.append("## 프로시저/클래스별 상세 요약")
    lines.append("")
    
    for result in results:
        name = result.get("name", "")
        summary_raw = result.get("summary", "")
        result_type = result.get("type", "")
        
        if not name:
            continue
        
        # Summary 파싱 (JSON 문자열일 수 있음)
        summary = ""
        if summary_raw:
            if isinstance(summary_raw, str):
                try:
                    summary_parsed = json.loads(summary_raw)
                    if isinstance(summary_parsed, str):
                        summary = summary_parsed
                    else:
                        summary = summary_raw
                except (json.JSONDecodeError, TypeError):
                    summary = summary_raw
            else:
                summary = str(summary_raw)
        
        if summary:
            lines.append(f"### {name} ({result_type})")
            lines.append("")
            # Summary를 문단별로 나누어 가독성 향상
            summary_paragraphs = summary.split('\n\n')
            for para in summary_paragraphs:
                para = para.strip()
                if para:
                    lines.append(para)
                    lines.append("")
            lines.append("---")
            lines.append("")
    
    # 2. User Stories & Acceptance Criteria
    lines.append("## User Stories & Acceptance Criteria")
    lines.append("")
    
    # 모든 User Story 집계
    all_user_stories = aggregate_user_stories_from_results(results)
    
    if not all_user_stories:
        lines.append("> User Story가 도출되지 않았습니다.")
        lines.append("")
        return "\n".join(lines)
    
    # 통계 정보
    total_stories = len(all_user_stories)
    total_ac = sum(len(us.get("acceptance_criteria", [])) for us in all_user_stories)
    lines.append(f"**총 {total_stories}개의 User Story, {total_ac}개의 Acceptance Criteria가 도출되었습니다.**")
    lines.append("")
    lines.append("---")
    lines.append("")
    
    # User Stories 상세 내용
    for us_idx, us in enumerate(all_user_stories, 1):
        us_id = us.get("id", f"US-{us_idx}")
        role = us.get("role", "")
        goal = us.get("goal", "")
        benefit = us.get("benefit", "")
        
        lines.append(f"## {us_id}")
        lines.append("")
        lines.append(f"**As a** {role}")
        lines.append("")
        lines.append(f"**I want** {goal}")
        lines.append("")
        lines.append(f"**So that** {benefit}")
        lines.append("")
        
        # Acceptance Criteria
        acs = us.get("acceptance_criteria", [])
        if acs:
            lines.append("### Acceptance Criteria")
            lines.append("")
            
            for ac in acs:
                ac_id = ac.get("id", "")
                ac_title = ac.get("title", "")
                given = ac.get("given", [])
                when = ac.get("when", [])
                then = ac.get("then", [])
                
                if ac_id or ac_title:
                    title_text = f"{ac_id}. {ac_title}" if (ac_id and ac_title) else (ac_id or ac_title)
                    lines.append(f"#### {title_text}")
                    lines.append("")
                
                if given:
                    lines.append("**Given**")
                    for g in given:
                        lines.append(f"- {g}")
                    lines.append("")
                
                if when:
                    lines.append("**When**")
                    for w in when:
                        lines.append(f"- {w}")
                    lines.append("")
                
                if then:
                    lines.append("**Then**")
                    for t in then:
                        lines.append(f"- {t}")
                    lines.append("")
        
        lines.append("---")
        lines.append("")
    
    # 푸터
    lines.append("")
    lines.append("---")
    lines.append("")
    lines.append(f"*이 문서는 {source_type} 코드 분석을 통해 자동으로 생성되었습니다.*")
    lines.append("")
    
    return "\n".join(lines)


def aggregate_user_stories_from_results(results: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    """여러 분석 결과에서 User Story를 집계합니다."""
    all_stories = []
    story_id_counter = 1
    
    for result in results:
        user_stories_raw = result.get("user_stories")
        if not user_stories_raw:
            continue
        
        if isinstance(user_stories_raw, str):
            try:
                user_stories = json.loads(user_stories_raw)
            except (json.JSONDecodeError, TypeError):
                continue
        else:
            user_stories = user_stories_raw
        
        if not isinstance(user_stories, list):
            continue
        
        for us in user_stories:
            # Neo4j 쿼리 결과에서 null 값 필터링
            if not us or not isinstance(us, dict) or not us.get("id"):
                continue
            
            us_copy = us.copy()
            # ID가 이미 있으면 유지, 없으면 새로 생성
            if not us_copy.get("id"):
                us_copy["id"] = f"US-{story_id_counter}"
            
            # Acceptance Criteria 처리 (Neo4j에서 collect로 묶인 배열)
            acs = us_copy.get("acceptance_criteria", [])
            if acs:
                # null 값 필터링
                acs = [ac for ac in acs if ac and isinstance(ac, dict) and ac.get("id")]
                us_copy["acceptance_criteria"] = acs
                
                # AC ID 재할당 (필요시)
                for ac_idx, ac in enumerate(acs, 1):
                    if isinstance(ac, dict) and not ac.get("id"):
                        ac["id"] = f"AC-{story_id_counter}-{ac_idx}"
            
            all_stories.append(us_copy)
            story_id_counter += 1
    
    return all_stories
