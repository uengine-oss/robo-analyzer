"""
util_core.py - 레거시 모더나이저 유틸리티 모듈

이 모듈은 Legacy-modernizer 프로젝트의 핵심 유틸리티 함수들을 제공합니다.
파일 처리, 문자열 변환, 토큰 계산, 코드 변환 등의 기능을 포함합니다.
"""

import os
import logging
import json
import aiofiles
import uuid
import tiktoken
from collections import defaultdict
from typing import Optional, Dict, List, Tuple, Any, Union

from util.exception import UtilProcessingError

# tiktoken 인코더 초기화
ENCODER = tiktoken.get_encoding("cl100k_base")

#==============================================================================
# 파일 처리 유틸리티
#==============================================================================

async def save_file(content: str, filename: str, base_path: Optional[str] = None) -> str:
    """파일을 비동기적으로 저장 (최적화: 경로 결합 최소화)"""
    try:
        os.makedirs(base_path, exist_ok=True)
        file_path = os.path.join(base_path, filename)
        
        async with aiofiles.open(file_path, 'w', encoding='utf-8') as file:
            await file.write(content)
        
        logging.info(f"파일 저장 성공: {file_path}")
        return file_path
        
    except Exception as e:
        err_msg = f"파일 저장 중 오류 발생: {str(e)}"
        logging.error(err_msg)
        raise UtilProcessingError(err_msg)


#==============================================================================
# 경로 유틸리티
#==============================================================================

# 모듈 레벨 캐싱 (반복 계산 방지)
_WORKSPACE_DIR = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

def build_rule_based_path(project_name: str, user_id: str, target_lang: str, role_name: str, **kwargs) -> str:
    """
    Rule 파일 기반 저장 경로 생성 (다중 언어 지원)
    
    Args:
        project_name: 프로젝트 이름
        user_id: 사용자 식별자
        target_lang: 타겟 언어 (java, python 등)
        role_name: Rule 파일명 (entity, service 등)
        **kwargs: 추가 변수 (dir_name 등)
    
    Returns:
        str: 저장 경로
    """
    from util.rule_loader import RuleLoader
    
    # Rule 파일에서 path 정보 로드
    rule_loader = RuleLoader(target_lang=target_lang)
    rule_info = rule_loader._load_role_file(role_name)
    relative_path = rule_info.get('path', '.')
    
    # 변수 치환 ({project_name}, {dir_name} 등)
    format_vars = {'project_name': project_name, **kwargs}
    relative_path = relative_path.format(**format_vars)
    
    # 전체 경로 생성
    docker_ctx = os.getenv('DOCKER_COMPOSE_CONTEXT')
    base_dir = docker_ctx if docker_ctx else _WORKSPACE_DIR
    base_path = os.path.join(base_dir, 'target', target_lang, user_id, project_name)
    
    return os.path.join(base_path, relative_path)


#==============================================================================
# 스트리밍/이벤트 유틸리티
#==============================================================================

# 서비스/전략 공통 스트림 구분자
STREAM_DELIMITER = b"send_stream"

def emit_bytes(payload: dict) -> bytes:
    """스트림 전송용 바이트 생성 (구분자 포함)"""
    return json.dumps(payload, default=str).encode('utf-8') + STREAM_DELIMITER

def emit_message(content) -> bytes:
    """message 이벤트 전송."""
    return emit_bytes({"type": "message", "content": content})

def emit_error(content) -> bytes:
    """에러 이벤트 전용 헬퍼.
    - {"type":"error", "content": <payload>} 형식으로 전송
    - content에는 에러 문자열 또는 에러 객체 요약을 전달
    """
    return emit_bytes({"type": "error", "content": content})

def emit_data(**fields) -> bytes:
    """data 이벤트 전송. fields는 최상위 필드로 포함됨."""
    payload = {"type": "data"}
    payload.update({k: v for k, v in fields.items() if v is not None})
    return emit_bytes(payload)

def emit_status(step: int, done: bool = False) -> bytes:
    """status 이벤트 전송. 단계 번호와 완료 여부를 전달."""
    return emit_bytes({"type": "status", "step": int(step), "done": bool(done)})


def build_error_body(exc: Exception, trace_id: str | None = None, message: str | None = None) -> dict:
    """비스트리밍 500 응답용 표준 에러 바디 생성.

    - errorType: 예외 클래스명
    - message: 사람이 읽을 수 있는 메시지
    - traceId: 로그 매칭용 식별자
    """
    return {
        "errorType": exc.__class__.__name__,
        "message": message or str(exc),
        "traceId": trace_id or f"req-{uuid.uuid4()}"
    }


async def stream_with_error_boundary(async_gen):
    """스트리밍 처리 경계. 내부 예외 발생 시 단일 에러 이벤트 전송 후 즉시 종료.

    모든 스트리밍 엔드포인트는 이 래퍼로 감싸 에러를 일관적으로 전파한다.
    """
    try:
        async for chunk in async_gen:
            yield chunk
    except Exception as e:
        # 실제 원인(예외 타입 + 메시지)만 전송하고 스트림 종료
        yield emit_error(f"{e.__class__.__name__}: {str(e)}")


#==============================================================================
# 문자열/JSON/경로 보조 유틸리티
#==============================================================================

def escape_for_cypher(text: str) -> str:
    """Cypher 쿼리용 문자열 이스케이프"""
    return str(text).replace("'", "\\'")

def parse_json_maybe(data):
    """JSON 문자열을 파싱하거나 리스트/딕셔너리는 그대로 반환"""
    if isinstance(data, str):
        return json.loads(data)
    return data or []

def safe_join(*parts: str) -> str:
    """안전한 경로 결합 (간단한 traversal 방지)"""
    p = os.path.normpath(os.path.join(*parts))
    if any(seg == '..' for seg in p.split(os.sep)):
        raise UtilProcessingError("Invalid path traversal")
    return p


#==============================================================================
# 문자열 변환 유틸리티
#==============================================================================

def convert_to_pascal_case(snake_str: str) -> str:
    """스네이크 케이스를 파스칼 케이스로 변환 (최적화: 조건 개선)"""
    try:
        if not snake_str:
            return ""
        if '_' not in snake_str:
            return snake_str[0].upper() + snake_str[1:]
        return ''.join(word.capitalize() for word in snake_str.split('_'))
    except Exception as e:
        err_msg = f"파스칼 케이스 변환 중 오류 발생: {str(e)}"
        logging.error(err_msg)
        raise UtilProcessingError("파스칼 케이스 변환 중 오류 발생")


def convert_to_camel_case(snake_str: str) -> str:
    """스네이크 케이스를 카멜 케이스로 변환 (최적화: 빈 체크)"""
    try:
        if not snake_str:
            return ""
        words = snake_str.split('_')
        return words[0].lower() + ''.join(word.capitalize() for word in words[1:])
    except Exception as e:
        err_msg = f"카멜 케이스 변환 중 오류 발생: {str(e)}"
        logging.error(err_msg)
        raise UtilProcessingError("카멜 케이스 변환 중 오류 발생")


def convert_to_upper_snake_case(camel_str: str) -> str:
    """파스칼/카멜 케이스를 대문자 스네이크 케이스로 변환 (최적화: 리스트 사용)"""
    try:
        if not camel_str:
            return ""
        
        result = [camel_str[0].upper()]
        for char in camel_str[1:]:
            if char.isupper():
                result.append('_')
                result.append(char)
            else:
                result.append(char.upper())
        
        return ''.join(result)
    except Exception as e:
        err_msg = f"대문자 스네이크 케이스 변환 중 오류 발생: {str(e)}"
        logging.error(err_msg)
        raise UtilProcessingError("대문자 스네이크 케이스 변환 중 오류 발생")


def add_line_numbers(plsql: List[str]) -> Tuple[str, List[str]]:
    """PL/SQL 코드에 라인 번호 추가 (최적화: enumerate 인덱스 조정)"""
    try:
        numbered_lines = [f"{i}: {line}" for i, line in enumerate(plsql, start=1)]
        return "".join(numbered_lines), numbered_lines
    except Exception as e:
        err_msg = f"코드에 라인번호를 추가하는 도중 문제가 발생했습니다: {str(e)}"
        logging.error(err_msg)
        raise UtilProcessingError(err_msg)


#==============================================================================
# 스키마/테이블 파싱 & 정규화 유틸리티
#==============================================================================
def parse_table_identifier(qualified_table_name: str) -> tuple[str, str, str | None]:
    """'SCHEMA.TABLE@DBLINK'에서 (schema, table, dblink) 추출 (최적화: 조건 개선)"""
    if not qualified_table_name:
        return '', '', None
    
    text = qualified_table_name.strip()
    left, _, link = text.partition('@')
    s, _, t = left.partition('.')
    
    return (s.strip() if t else ''), (t.strip() if t else left.strip()), (link.strip() or None)

#==============================================================================
# 코드 분석 및 변환 유틸리티
#==============================================================================

def calculate_code_token(code: Union[str, Dict, List]) -> int:
    """코드 토큰 길이 계산 (최적화: 중복 제거)"""
    try:
        text_json = json.dumps(code, ensure_ascii=False)
        return len(ENCODER.encode(text_json))
    except Exception as e:
        err_msg = f"토큰 계산 도중 문제가 발생: {str(e)}"
        logging.error(err_msg)
        raise UtilProcessingError(err_msg)


def build_variable_index(local_variable_nodes: List[Dict]) -> Dict:
    """변수 노드를 startLine 기준으로 인덱싱 (최적화: split 최소화)"""
    index = {}
    for variable_node in local_variable_nodes:
        node_data = variable_node.get('v', {})
        var_name = node_data.get('name')
        if not var_name:
            continue
        
        var_info = f"{node_data.get('type', 'Unknown')}: {var_name}"
        
        for key in node_data:
            if '_' in key:
                parts = key.split('_')
                if len(parts) == 2 and all(p.isdigit() for p in parts):
                    start_line = int(parts[0])
                    entry = index.setdefault(start_line, {'nodes': defaultdict(list), 'tokens': None})
                    entry['nodes'][f"{start_line}~{int(parts[1])}"].append(var_info)
    return index


async def extract_used_variable_nodes(startLine: int, local_variable_nodes: List[Dict]) -> Tuple[Dict, int]:
    """특정 라인에서 사용된 변수 추출 (최적화: 타입 체크 개선)"""
    try:
        # 인덱스면 그대로 사용, 리스트면 인덱스 생성
        var_index = (local_variable_nodes if isinstance(local_variable_nodes, dict) 
                     else build_variable_index(local_variable_nodes))
        
        if entry := var_index.get(startLine):
            var_nodes = entry['nodes']
            if entry['tokens'] is None:
                entry['tokens'] = calculate_code_token(var_nodes)
            return var_nodes, entry['tokens']
        return {}, 0
    
    except UtilProcessingError:
        raise
    except Exception as e:
        err_msg = f"사용된 변수 노드를 추출하는 도중 오류가 발생했습니다: {str(e)}"
        logging.error(err_msg)
        raise UtilProcessingError(err_msg)


async def collect_variables_in_range(local_variable_nodes: List[Dict], start_line: int, end_line: int) -> List[Dict]:
    """범위 내 변수 수집 (최적화: 딕셔너리 구조 개선)"""
    try:
        unique = {}
        for variable_node in local_variable_nodes:
            node_data = variable_node.get('v', {})
            var_name = node_data.get('name')
            if not var_name or var_name in unique:
                continue
            
            for range_key in node_data:
                if '_' in range_key:
                    parts = range_key.split('_')
                    if len(parts) == 2 and all(p.isdigit() for p in parts):
                        v_start, v_end = int(parts[0]), int(parts[1])
                        if start_line <= v_start and v_end <= end_line:
                            unique[var_name] = {'type': node_data.get('type', 'Unknown'), 'name': var_name}
                            break
        return list(unique.values())
    except Exception as e:
        err_msg = f"변수 범위 수집 중 오류가 발생했습니다: {str(e)}"
        logging.error(err_msg)
        raise UtilProcessingError(err_msg)

async def extract_used_query_methods(start_line: int, end_line: int, 
                                   jpa_method_list: Dict, 
                                   used_jpa_method_dict: Dict) -> Dict:
    """범위 내 JPA 메서드 수집 (최적화: 직접 업데이트)"""
    try:
        for range_key, method in jpa_method_list.items():
            method_start, method_end = map(int, range_key.split('~'))
            if start_line <= method_start and method_end <= end_line:
                used_jpa_method_dict[range_key] = method
        return used_jpa_method_dict
        
    except Exception as e:
        err_msg = f"JPA 쿼리 메서드를 추출하는 도중 오류가 발생했습니다: {str(e)}"
        logging.error(err_msg)
        raise UtilProcessingError(err_msg)