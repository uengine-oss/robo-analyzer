"""
오류 코드 파서
- 컴파일 오류 메시지에서 오류 번호와 내용을 추출
"""

import re
import logging
from typing import Optional, Dict

logger = logging.getLogger(__name__)


def parse_error_message(error_message: str) -> Optional[Dict[str, any]]:
    """
    오류 메시지에서 오류 번호와 내용을 추출합니다.
    
    Args:
        error_message: 컴파일 오류 메시지 (예: "ORA-00942: table or view does not exist")
        
    Returns:
        {
            'error_number': int,  # 오류 번호 (예: 942)
            'error_code': str,    # 오류 코드 (예: "ORA-00942")
            'error_message': str, # 오류 내용
            'line_number': int | None  # 라인 번호 (있는 경우)
        } 또는 None
    """
    if not error_message or not error_message.strip():
        return None
    
    error_message = error_message.strip()
    
    # ORA-XXXXX 형식 (Oracle)
    ora_pattern = r'ORA-(\d{5}):\s*(.+?)(?:\s+at\s+line\s+(\d+))?'
    match = re.search(ora_pattern, error_message, re.IGNORECASE)
    if match:
        error_num = int(match.group(1))
        error_msg = match.group(2).strip()
        line_num = int(match.group(3)) if match.group(3) else None
        return {
            'error_number': error_num,
            'error_code': f'ORA-{error_num:05d}',
            'error_message': error_msg,
            'line_number': line_num
        }
    
    # SQL Server 형식 (예: "Msg 102, Level 15, State 1, Line 5")
    sql_server_pattern = r'Msg\s+(\d+).*?Line\s+(\d+)'
    match = re.search(sql_server_pattern, error_message, re.IGNORECASE)
    if match:
        error_num = int(match.group(1))
        line_num = int(match.group(2))
        return {
            'error_number': error_num,
            'error_code': f'SQL-{error_num}',
            'error_message': error_message,
            'line_number': line_num
        }
    
    # 일반적인 라인 번호 포함 패턴 (예: "Error at line 10: ...")
    line_pattern = r'(?:line|Line|라인)\s+(\d+)'
    line_match = re.search(line_pattern, error_message)
    line_num = int(line_match.group(1)) if line_match else None
    
    # 숫자로 시작하는 오류 코드 추출 시도
    num_pattern = r'(\d{4,5})'
    num_match = re.search(num_pattern, error_message)
    if num_match:
        error_num = int(num_match.group(1))
        return {
            'error_number': error_num,
            'error_code': f'ERR-{error_num}',
            'error_message': error_message,
            'line_number': line_num
        }
    
    # 라인 번호만 있는 경우
    if line_num:
        return {
            'error_number': None,
            'error_code': 'UNKNOWN',
            'error_message': error_message,
            'line_number': line_num
        }
    
    logger.warning(f"오류 메시지 파싱 실패: {error_message}")
    return None

