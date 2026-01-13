"""ROBO Analyzer 유틸리티 모듈

핵심 유틸리티 함수:
- 토큰 계산
- 문자열 처리 (Cypher 이스케이프)
- User Story 문서 생성
- 스트리밍 이벤트 (stream_utils에서 re-export)
"""

import logging
import json
import uuid
import tiktoken
from typing import Optional, Dict, List, Any, Union

from util.exception import RoboAnalyzerError

# 스트리밍 유틸리티 (stream_utils.py에서 import)
from util.stream_utils import (
    emit_bytes,
    emit_message,
    emit_error,
    emit_data,
    emit_node_event,
    emit_relationship_event,
    emit_complete,
    build_error_body,
    stream_with_error_boundary,
)


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
    """'SCHEMA.TABLE@DBLINK'에서 (schema, table, dblink) 추출
    
    따옴표(", ', `, [])를 자동으로 제거합니다.
    원본 대소문자를 유지합니다 (name_case 옵션에서 변환 처리).
    예: "RWIS"."TABLE" → (RWIS, TABLE, None)
    """
    if not qualified_table_name:
        return '', '', None
    
    def strip_quotes(s: str) -> str:
        """따옴표 제거: "name", 'name', `name`, [name] 형식 처리"""
        s = s.strip()
        if len(s) >= 2:
            if (s[0] == '"' and s[-1] == '"') or \
               (s[0] == "'" and s[-1] == "'") or \
               (s[0] == '`' and s[-1] == '`'):
                return s[1:-1]
            if s[0] == '[' and s[-1] == ']':
                return s[1:-1]
        return s
    
    text = qualified_table_name.strip()
    left, _, link = text.partition('@')
    s, _, t = left.partition('.')
    
    schema_raw = strip_quotes(s.strip()) if t else ''
    table_raw = strip_quotes(t.strip()) if t else strip_quotes(left.strip())
    link_raw = strip_quotes(link.strip()) if link.strip() else None

    # 원본 대소문자 유지 (name_case 옵션에서 변환 처리)
    schema = schema_raw or ''
    table = table_raw or ''
    db_link = link_raw if link_raw else None

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
        raise RoboAnalyzerError(err_msg)


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
                # summary가 JSON 문자열이면 파싱, 아니면 그대로 사용
                if summary_raw.startswith('{') or summary_raw.startswith('['):
                    try:
                        summary_parsed = json.loads(summary_raw)
                        if isinstance(summary_parsed, str):
                            summary = summary_parsed
                        else:
                            raise ValueError(f"Summary JSON이 문자열이 아닙니다: {type(summary_parsed)}")
                    except (json.JSONDecodeError, TypeError) as e:
                        raise ValueError(f"Summary JSON 파싱 실패: {summary_raw[:100]}...") from e
                else:
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
            except (json.JSONDecodeError, TypeError) as e:
                raise ValueError(f"User Story JSON 파싱 실패: {user_stories_raw[:100]}...") from e
        else:
            user_stories = user_stories_raw
        
        if not isinstance(user_stories, list):
            raise ValueError(f"User Story가 리스트 형식이 아닙니다: {type(user_stories)}")
        
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


#==============================================================================
# DDL 청크 분할 유틸리티
#==============================================================================

# DDL 청크 분할 시 최대 토큰 수
# LLM 출력 제한(max_tokens=32768) 고려: 테이블당 약 700토큰 출력
# 청크당 최대 20개 테이블 → 출력 14K 토큰 (충분한 안전 마진)
# 입력 5K 토큰 → 평균 15~25개 테이블 → 출력 10.5~17.5K 토큰
MAX_DDL_CHUNK_TOKENS = 5000


def split_ddl_into_chunks(ddl_content: str, max_tokens: int = MAX_DDL_CHUNK_TOKENS) -> List[str]:
    """대용량 DDL을 CREATE TABLE 단위로 분할하여 청크로 나눕니다.
    
    각 CREATE TABLE 블록과 관련 COMMENT ON 구문을 함께 그룹화합니다.
    ALTER TABLE (PK/FK 정의)도 해당 테이블 블록에 포함시킵니다.
    
    Args:
        ddl_content: 전체 DDL 문자열
        max_tokens: 청크당 최대 토큰 수
        
    Returns:
        DDL 청크 리스트 (각 청크는 여러 CREATE TABLE 블록 포함 가능)
    """
    import re
    
    # DDL이 작으면 분할하지 않음
    total_tokens = calculate_code_token(ddl_content)
    if total_tokens <= max_tokens:
        return [ddl_content]
    
    # 1. CREATE TABLE/VIEW 블록 추출 (정규식으로 분할)
    # CREATE TABLE ... ; 패턴 매칭
    create_pattern = re.compile(
        r'(CREATE\s+(?:TABLE|VIEW)\s+(?:IF\s+NOT\s+EXISTS\s+)?[\w\."]+\s*\([^;]+\);)',
        re.IGNORECASE | re.DOTALL
    )
    
    # 2. COMMENT ON 구문 추출 (여러 줄 코멘트 지원, 이스케이프된 작은따옴표 처리)
    comment_pattern = re.compile(
        r"(COMMENT\s+ON\s+(?:TABLE|COLUMN)\s+[\w\.\"]+(?:\.[\w\.\"]+)*\s+IS\s+'(?:[^']|'')*';)",
        re.IGNORECASE | re.DOTALL
    )
    
    # 3. ALTER TABLE 구문 추출 (PK, FK, CONSTRAINT)
    alter_pattern = re.compile(
        r'(ALTER\s+TABLE\s+[\w\."]+\s+ADD\s+(?:PRIMARY\s+KEY|CONSTRAINT|FOREIGN\s+KEY)[^;]+;)',
        re.IGNORECASE | re.DOTALL
    )
    
    # 테이블별로 DDL 블록 수집
    table_blocks: Dict[str, List[str]] = {}
    
    # CREATE TABLE 블록 수집
    for match in create_pattern.finditer(ddl_content):
        stmt = match.group(1).strip()
        # 테이블명 추출 (스키마.테이블 또는 테이블)
        table_name_match = re.search(
            r'CREATE\s+(?:TABLE|VIEW)\s+(?:IF\s+NOT\s+EXISTS\s+)?([\w\."]+)',
            stmt, re.IGNORECASE
        )
        if table_name_match:
            table_key = table_name_match.group(1).upper().replace('"', '').replace("'", '')
            if table_key not in table_blocks:
                table_blocks[table_key] = []
            table_blocks[table_key].append(stmt)
    
    # COMMENT ON 구문 매핑
    for match in comment_pattern.finditer(ddl_content):
        stmt = match.group(1).strip()
        # 테이블명 추출
        if 'COMMENT ON TABLE' in stmt.upper():
            # COMMENT ON TABLE SCHEMA."TABLE_NAME" IS '...';
            table_match = re.search(r'COMMENT\s+ON\s+TABLE\s+([\w\."]+)', stmt, re.IGNORECASE)
            if table_match:
                table_key = table_match.group(1).upper().replace('"', '').replace("'", '')
                if table_key in table_blocks:
                    table_blocks[table_key].append(stmt)
        else:  # COMMENT ON COLUMN
            # COMMENT ON COLUMN SCHEMA."TABLE_NAME"."COLUMN_NAME" IS '...';
            # 스키마.테이블.컬럼 또는 스키마.테이블.컬럼 형태에서 테이블명까지 추출
            col_match = re.search(r'COMMENT\s+ON\s+COLUMN\s+([\w\."]+)\.([\w\."]+)\s+IS', stmt, re.IGNORECASE)
            if col_match:
                # 첫 번째 그룹이 스키마.테이블 또는 테이블
                table_key = col_match.group(1).upper().replace('"', '').replace("'", '')
                if table_key in table_blocks:
                    table_blocks[table_key].append(stmt)
    
    # ALTER TABLE 구문 매핑
    for match in alter_pattern.finditer(ddl_content):
        stmt = match.group(1).strip()
        table_match = re.search(r'ALTER\s+TABLE\s+([\w\."]+)', stmt, re.IGNORECASE)
        if table_match:
            table_key = table_match.group(1).upper().replace('"', '').replace("'", '')
            if table_key in table_blocks:
                table_blocks[table_key].append(stmt)
    
    # 4. 테이블 블록들을 토큰 한도 내에서 청크로 묶음
    chunks: List[str] = []
    current_chunk_parts: List[str] = []
    current_tokens = 0
    
    for table_key, stmts in table_blocks.items():
        table_ddl = '\n'.join(stmts)
        table_tokens = calculate_code_token(table_ddl)
        
        # 단일 테이블이 너무 크면 그냥 하나의 청크로
        if table_tokens > max_tokens:
            if current_chunk_parts:
                chunks.append('\n\n'.join(current_chunk_parts))
                current_chunk_parts = []
                current_tokens = 0
            chunks.append(table_ddl)
            continue
        
        # 현재 청크에 추가 가능한지 확인
        if current_tokens + table_tokens > max_tokens:
            # 현재 청크 완료, 새 청크 시작
            if current_chunk_parts:
                chunks.append('\n\n'.join(current_chunk_parts))
            current_chunk_parts = [table_ddl]
            current_tokens = table_tokens
        else:
            # 현재 청크에 추가
            current_chunk_parts.append(table_ddl)
            current_tokens += table_tokens
    
    # 마지막 청크 처리
    if current_chunk_parts:
        chunks.append('\n\n'.join(current_chunk_parts))
    
    # 청크가 없으면 원본 반환 (분할 실패)
    if not chunks:
        return [ddl_content]
    
    log_process("DDL", "CHUNK", f"📦 DDL 분할 완료: {len(chunks)}개 청크 ({total_tokens:,} 토큰 → 각 청크 ~{max_tokens:,} 토큰)")
    
    return chunks
