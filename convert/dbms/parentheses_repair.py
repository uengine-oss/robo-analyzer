"""
괄호 검증 및 복구 모듈

DBMS 변환 시 LLM이 생성한 SQL의 괄호 불일치를 감지하고 복구합니다.
- 괄호 개수 검증 (30개 이상일 때만)
- 분해(decompose) → 조립(assemble) 방식으로 복구
- 로직 조립 우선, 실패 시 LLM 폴백
"""

import os
import re
import logging
from dataclasses import dataclass
from typing import Tuple, Optional, Dict

from util.rule_loader import RuleLoader

logger = logging.getLogger(__name__)

# ============================================================================
# 설정 상수
# ============================================================================
MIN_PARENTHESES_FOR_CHECK = int(os.getenv('PARENTHESES_MIN_CHECK', '30'))
MAX_REPAIR_RETRIES = int(os.getenv('PARENTHESES_MAX_RETRIES', '2'))
MAX_ASSEMBLE_RETRIES = int(os.getenv('PARENTHESES_MAX_ASSEMBLE_RETRIES', '3'))
LOG_DIR = os.getenv('PARENTHESES_LOG_DIR', './repair_logs/')


@dataclass
class RepairContext:
    """괄호 복구 컨텍스트"""
    work_id: int
    start_line: int
    end_line: int
    node_type: str = "UNKNOWN"
    parent_context: str = ""


# ============================================================================
# 괄호 검증 유틸리티
# ============================================================================

def remove_sql_comments(sql: str) -> str:
    """SQL에서 주석을 제거 (문자열 리터럴 보호)"""
    result = []
    i = 0
    in_string = False
    string_char = None
    
    while i < len(sql):
        # 문자열 리터럴 처리
        if not in_string:
            if sql[i] in ("'", '"'):
                in_string = True
                string_char = sql[i]
                result.append(sql[i])
                i += 1
                continue
        else:
            # 문자열 내부
            result.append(sql[i])
            if sql[i] == string_char:
                # 이스케이프 체크 ('')
                if i + 1 < len(sql) and sql[i + 1] == string_char:
                    result.append(sql[i + 1])
                    i += 2
                    continue
                else:
                    in_string = False
            i += 1
            continue
        
        # 문자열 외부에서 주석 처리
        # -- 라인 주석
        if sql[i:i+2] == '--':
            while i < len(sql) and sql[i] != '\n':
                i += 1
            if i < len(sql):
                result.append('\n')
                i += 1
            continue
        
        # /* */ 블록 주석
        if sql[i:i+2] == '/*':
            i += 2
            while i < len(sql) - 1:
                if sql[i:i+2] == '*/':
                    i += 2
                    break
                i += 1
            continue
        
        # 일반 문자
        result.append(sql[i])
        i += 1
    
    return ''.join(result)


def count_parentheses(sql: str) -> Tuple[int, int]:
    """SQL에서 여는 괄호와 닫는 괄호 개수 반환 (주석 제외)"""
    clean_sql = remove_sql_comments(sql)
    open_count = clean_sql.count('(')
    close_count = clean_sql.count(')')
    return open_count, close_count


def has_parentheses_mismatch(sql: str) -> bool:
    """괄호 불일치 여부 확인 (MIN_PARENTHESES_FOR_CHECK개 이상일 때만)"""
    open_count, close_count = count_parentheses(sql)
    total = open_count + close_count
    
    # 괄호가 임계값 미만이면 검사하지 않음
    if total < MIN_PARENTHESES_FOR_CHECK:
        return False
    
    return open_count != close_count


# ============================================================================
# 조립 로직 유틸리티
# ============================================================================

def parse_decomposed_sql(decomposed: str) -> Tuple[Optional[str], Dict[int, str]]:
    """
    분해된 SQL을 파싱하여 PARENT와 CHILD들로 분리
    
    Returns:
        (parent_sql, children_dict)
        parent_sql: PARENT 블록의 SQL
        children_dict: {1: CHILD_1의 SQL, 2: CHILD_2의 SQL, ...}
    """
    pattern = r'\[(PARENT|CHILD_\d+)\]\s*\n(.*?)(?=\n\[(?:PARENT|CHILD_\d+)\]|\Z)'
    matches = re.findall(pattern, decomposed, re.DOTALL)
    
    parent_sql = None
    children_dict = {}
    
    for block_name, content in matches:
        content = content.strip()
        
        if block_name == "PARENT":
            parent_sql = content
        elif block_name.startswith("CHILD_"):
            try:
                child_num = int(block_name.split("_")[1])
                children_dict[child_num] = content
            except (IndexError, ValueError):
                logger.warning(f"잘못된 CHILD 블록 이름: {block_name}")
    
    return parent_sql, children_dict


def assemble_sql_with_logic(parent_sql: str, children_dict: Dict[int, str]) -> Tuple[bool, str]:
    """
    로직으로 SQL 조립 시도 (재귀적 치환)
    
    Returns:
        (success, assembled_sql)
    """
    if not parent_sql:
        logger.warning("PARENT SQL이 없음 - 조립 실패")
        return False, ""
    
    placeholder_pattern = r'\{\{CHILD_(\d+)\}\}'
    assembled = parent_sql
    max_iterations = 10
    
    for iteration in range(max_iterations):
        placeholders = re.findall(placeholder_pattern, assembled)
        
        if not placeholders:
            logger.info(f"로직 조립 성공 ({iteration + 1}회 반복)")
            return True, assembled
        
        required_children = set(int(p) for p in placeholders)
        available_children = set(children_dict.keys())
        
        if not required_children.issubset(available_children):
            missing = required_children - available_children
            logger.warning(f"누락된 CHILD: {missing} - 조립 실패")
            return False, assembled
        
        changed = False
        for child_num in sorted(required_children, reverse=True):
            placeholder = f"{{{{CHILD_{child_num}}}}}"
            child_sql = children_dict[child_num]
            
            if placeholder in assembled:
                assembled = assembled.replace(placeholder, child_sql)
                changed = True
        
        if not changed:
            logger.warning(f"치환 불가능 - 조립 실패 (남은 플레이스홀더: {placeholders})")
            return False, assembled
    
    remaining = re.findall(placeholder_pattern, assembled)
    logger.warning(f"최대 반복 횟수 초과 - 조립 실패 (남은 플레이스홀더: {remaining})")
    return False, assembled


# ============================================================================
# 로그 저장 유틸리티
# ============================================================================

def _ensure_log_dir():
    """로그 디렉토리 생성"""
    if LOG_DIR and not os.path.exists(LOG_DIR):
        try:
            os.makedirs(LOG_DIR, exist_ok=True)
        except Exception as e:
            logger.warning(f"로그 디렉토리 생성 실패: {e}")


def _save_log_file(filename: str, content: str):
    """로그 파일 저장"""
    _ensure_log_dir()
    try:
        filepath = os.path.join(LOG_DIR, filename)
        with open(filepath, 'w', encoding='utf-8') as f:
            f.write(content)
        return filepath
    except Exception as e:
        logger.warning(f"로그 파일 저장 실패: {e}")
        return None


# ============================================================================
# LLM 복구 로직
# ============================================================================

def _call_llm_for_repair(
    rule_loader: RuleLoader,
    api_key: str,
    role_name: str,
    inputs: Dict
) -> str:
    """LLM 호출하여 복구 수행"""
    try:
        result = rule_loader.execute(
            role_name=role_name,
            inputs=inputs,
            api_key=api_key
        )
        code = (result.get('code') or '').strip()
        # 코드 블록 마커 제거
        code = code.replace('```sql', '').replace('```', '').strip()
        return code
    except Exception as e:
        logger.error(f"LLM 복구 호출 실패: {e}")
        return ""


def _assemble_with_llm_fallback(
    rule_loader: RuleLoader,
    api_key: str,
    locale: str,
    decomposed: str,
    context: RepairContext,
    attempt: int,
    logic_result: Optional[str] = None
) -> str:
    """LLM으로 조립 폴백 (로직 조립 실패 시)"""
    logger.info(f"작업 {context.work_id} LLM 조립 폴백 시작")
    
    best_repaired = logic_result
    best_diff = float('inf')
    if logic_result:
        open_logic, close_logic = count_parentheses(logic_result)
        best_diff = abs(open_logic - close_logic)
    
    for assemble_attempt in range(MAX_ASSEMBLE_RETRIES):
        logger.info(f"  - LLM 조립 시도 {assemble_attempt + 1}/{MAX_ASSEMBLE_RETRIES}")
        
        repaired = _call_llm_for_repair(
            rule_loader=rule_loader,
            api_key=api_key,
            role_name='parentheses_assemble',
            inputs={
                'decomposed_sql': decomposed,
                'locale': locale
            }
        )
        
        if not repaired:
            continue
        
        open_after, close_after = count_parentheses(repaired)
        diff = abs(open_after - close_after)
        
        logger.info(f"    여는 괄호: {open_after}, 닫는 괄호: {close_after}, 차이: {diff}")
        
        # 조립 결과 로그 저장
        log_content = (
            f"-- LLM 조립 시도 {assemble_attempt + 1} (decompose 시도 {attempt})\n"
            f"-- 여는 괄호: {open_after}, 닫는 괄호: {close_after}\n\n"
            f"{repaired}"
        )
        _save_log_file(
            f"work_{context.work_id:03d}_repair_attempt{attempt}_llm_assemble{assemble_attempt + 1}.sql",
            log_content
        )
        
        # 완벽하게 매칭되면 즉시 성공 반환
        if open_after == close_after:
            logger.info(f"작업 {context.work_id} 괄호 복구 성공 (LLM 조립)")
            
            success_content = (
                f"-- 복구 성공 (LLM 조립, decompose 시도 {attempt}, assemble 시도 {assemble_attempt + 1})\n"
                f"-- {open_after} 쌍 매칭\n\n{repaired}"
            )
            _save_log_file(
                f"work_{context.work_id:03d}_repair_attempt{attempt}_SUCCESS_llm.sql",
                success_content
            )
            
            return repaired
        
        if diff < best_diff:
            best_diff = diff
            best_repaired = repaired
    
    logger.warning(f"작업 {context.work_id} LLM 조립 {MAX_ASSEMBLE_RETRIES}번 시도 모두 실패")
    
    return best_repaired if best_repaired else ""


# ============================================================================
# 괄호 복구 메인 로직
# ============================================================================

def repair_parentheses_mismatch(
    rule_loader: RuleLoader,
    api_key: str,
    locale: str,
    broken_sql: str,
    context: RepairContext,
    attempt: int = 1
) -> str:
    """
    괄호 불일치 SQL을 분해 후 재조립하여 복구 (하이브리드 방식: 로직 우선, LLM 폴백)
    
    Args:
        rule_loader: RuleLoader 인스턴스
        api_key: LLM API 키
        locale: 로케일
        broken_sql: 괄호가 불일치한 SQL
        context: 복구 컨텍스트 정보
        attempt: 현재 시도 횟수
    
    Returns:
        str: 복구된 SQL (실패 시 원본 반환)
    """
    logger.warning(f"작업 {context.work_id} 괄호 불일치 감지 - 복구 시도 {attempt}")
    
    open_count, close_count = count_parentheses(broken_sql)
    logger.info(f"  - 여는 괄호: {open_count}, 닫는 괄호: {close_count}")
    
    # 1단계: 분해
    logger.info(f"작업 {context.work_id} 복구 1단계: SQL 분해 (시도 {attempt})")
    
    decomposed = _call_llm_for_repair(
        rule_loader=rule_loader,
        api_key=api_key,
        role_name='parentheses_decompose',
        inputs={
            'broken_sql': broken_sql,
            'open_count': open_count,
            'close_count': close_count,
            'locale': locale
        }
    )
    
    if not decomposed:
        logger.warning("분해 실패 - 원본 반환")
        return broken_sql
    
    # 분해 결과 로그 저장
    _save_log_file(
        f"work_{context.work_id:03d}_repair_attempt{attempt}_decomposed.txt",
        decomposed
    )
    
    # 분해 결과 괄호 검증
    decomposed_open, decomposed_close = count_parentheses(decomposed)
    logger.info(f"  - 분해 결과 괄호: 여는 괄호 {decomposed_open}, 닫는 괄호 {decomposed_close}")
    
    if decomposed_open != decomposed_close:
        logger.warning("분해 결과 자체에 괄호 불일치 - decompose 재시도 필요")
        return broken_sql
    
    # 2단계: 조립
    logger.info(f"작업 {context.work_id} 복구 2단계: SQL 조립 시작")
    
    # 2-1. 분해된 SQL 파싱
    parent_sql, children_dict = parse_decomposed_sql(decomposed)
    
    if not parent_sql:
        logger.warning("PARENT SQL 파싱 실패 - LLM 조립으로 폴백")
        return _assemble_with_llm_fallback(
            rule_loader, api_key, locale,
            decomposed, context, attempt, decomposed
        )
    
    logger.info(f"  - 파싱 성공: PARENT 1개, CHILD {len(children_dict)}개")
    
    # 2-2. 로직으로 조립 시도
    logger.info("  - 로직 조립 시도 중...")
    logic_success, assembled = assemble_sql_with_logic(parent_sql, children_dict)
    
    if logic_success:
        open_after, close_after = count_parentheses(assembled)
        logger.info(f"  - 로직 조립 완료: 여는 괄호 {open_after}, 닫는 괄호 {close_after}")
        
        # 로직 조립 결과 로그 저장
        log_content = (
            f"-- 로직 조립 결과 (decompose 시도 {attempt})\n"
            f"-- 여는 괄호: {open_after}, 닫는 괄호: {close_after}\n\n"
            f"{assembled}"
        )
        _save_log_file(
            f"work_{context.work_id:03d}_repair_attempt{attempt}_logic_assembled.sql",
            log_content
        )
        
        if open_after == close_after:
            logger.info(f"작업 {context.work_id} 괄호 복구 성공 (로직 조립)")
            
            success_content = (
                f"-- 복구 성공 (로직 조립, decompose 시도 {attempt})\n"
                f"-- {open_after} 쌍 매칭\n\n{assembled}"
            )
            _save_log_file(
                f"work_{context.work_id:03d}_repair_attempt{attempt}_SUCCESS_logic.sql",
                success_content
            )
            
            return assembled
        else:
            logger.warning("로직 조립 성공했으나 괄호 불일치 - LLM 조립으로 폴백")
    else:
        logger.warning("로직 조립 실패 - LLM 조립으로 폴백")
    
    # 2-3. LLM 조립 폴백
    return _assemble_with_llm_fallback(
        rule_loader, api_key, locale,
        decomposed, context, attempt,
        assembled if logic_success else None
    )


# ============================================================================
# 검증 및 복구 통합 함수
# ============================================================================

def validate_and_repair_sql(
    rule_loader: RuleLoader,
    api_key: str,
    locale: str,
    converted_sql: str,
    context: RepairContext
) -> str:
    """
    변환된 SQL의 괄호 검증 및 필요시 복구
    
    Args:
        rule_loader: RuleLoader 인스턴스
        api_key: LLM API 키
        locale: 로케일
        converted_sql: 변환된 SQL
        context: 복구 컨텍스트 정보
    
    Returns:
        str: 검증 완료된 SQL (복구됐거나 원본)
    """
    # 괄호 검증
    if not has_parentheses_mismatch(converted_sql):
        open_count, close_count = count_parentheses(converted_sql)
        total = open_count + close_count
        if total >= MIN_PARENTHESES_FOR_CHECK:
            logger.info(f"작업 {context.work_id} 괄호 검증 통과 ({open_count} 쌍)")
        return converted_sql
    
    open_count, close_count = count_parentheses(converted_sql)
    logger.warning(
        f"작업 {context.work_id} 괄호 불일치 감지: "
        f"여는 괄호 {open_count}, 닫는 괄호 {close_count}"
    )
    
    # 검증 실패 로그 저장
    failed_content = (
        f"-- 작업 {context.work_id} 검증 실패\n"
        f"-- 여는 괄호: {open_count}, 닫는 괄호: {close_count}\n"
        f"-- Lines: {context.start_line}~{context.end_line}\n"
        f"-- Type: {context.node_type}\n\n"
        f"{converted_sql}"
    )
    _save_log_file(
        f"work_{context.work_id:03d}_validation_failed.sql",
        failed_content
    )
    
    result = converted_sql
    
    # 복구 시도 (decompose 재시도)
    for decompose_attempt in range(MAX_REPAIR_RETRIES):
        try:
            repaired = repair_parentheses_mismatch(
                rule_loader=rule_loader,
                api_key=api_key,
                locale=locale,
                broken_sql=converted_sql,  # 원본 broken SQL을 계속 사용
                context=context,
                attempt=decompose_attempt + 1
            )
            
            # 복구 성공 여부 확인
            if not has_parentheses_mismatch(repaired):
                logger.info(
                    f"작업 {context.work_id} 복구 최종 성공 "
                    f"(decompose 시도 {decompose_attempt + 1})"
                )
                return repaired
            else:
                open_r, close_r = count_parentheses(repaired)
                logger.warning(
                    f"작업 {context.work_id} decompose 시도 {decompose_attempt + 1} 실패 "
                    f"(여는: {open_r}, 닫는: {close_r})"
                )
                result = repaired  # 최선의 결과 유지
                
        except Exception as e:
            logger.error(
                f"작업 {context.work_id} 복구 중 오류 "
                f"(decompose 시도 {decompose_attempt + 1}): {e}"
            )
            if decompose_attempt == MAX_REPAIR_RETRIES - 1:
                logger.error(f"작업 {context.work_id} 복구 최종 실패")
    
    return result

