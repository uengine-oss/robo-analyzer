"""
util_core.py - 레거시 모더나이저 유틸리티 모듈

이 모듈은 Legacy-modernizer 프로젝트의 핵심 유틸리티 함수들을 제공합니다.
파일 처리, 문자열 변환, 토큰 계산, 코드 변환 등의 기능을 포함합니다.
"""

import os
import logging
import json
import aiofiles
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
    """
    파일을 비동기적으로 저장하는 함수
    
    Args:
        content: 저장할 파일 내용
        filename: 파일명 (확장자 포함)
        base_path: 기본 저장 경로
        
    Returns:
        저장된 파일의 전체 경로
        
    Raises:
        SaveFileError: 파일 저장 중 오류 발생 시
    """
    try:
        # 디렉토리 생성
        os.makedirs(base_path, exist_ok=True)
            
        # 파일 전체 경로
        file_path = os.path.join(base_path, filename)
        
        # 파일 저장
        async with aiofiles.open(file_path, 'w', encoding='utf-8') as file:
            await file.write(content)
            logging.info(f"파일 저장 성공: {file_path}")
            
        return file_path
        
    except Exception as e:
        err_msg = f"파일 저장 중 오류 발생: {str(e)}"
        logging.error(err_msg)
        raise UtilProcessingError(err_msg)


#==============================================================================
# 문자열 변환 유틸리티
#==============================================================================

def convert_to_pascal_case(snake_str: str) -> str:
    """
    스네이크 케이스를 파스칼 케이스로 변환
    
    Args:
        snake_str: 변환할 스네이크 케이스 문자열 (예: employee_payroll)
        
    Returns:
        파스칼 케이스로 변환된 문자열 (예: EmployeePayroll)
    """
    try:
        if '_' not in snake_str:
            # 이미 언더스코어가 없으면 첫 글자만 대문자로 변환
            return snake_str[0].upper() + snake_str[1:] if snake_str else ""
        
        return ''.join(word.capitalize() for word in snake_str.split('_'))
    except Exception as e:
        err_msg = f"파스칼 케이스 변환 중 오류 발생: {str(e)}"
        logging.error(err_msg)
        raise UtilProcessingError("파스칼 케이스 변환 중 오류 발생")


def convert_to_camel_case(snake_str: str) -> str:
    """
    스네이크 케이스를 카멜 케이스로 변환
    
    Args:
        snake_str: 변환할 스네이크 케이스 문자열 (예: user_profile_service)
        
    Returns:
        카멜 케이스로 변환된 문자열 (예: userProfileService)
    """
    try:
        words = snake_str.split('_')
        return words[0].lower() + ''.join(word.capitalize() for word in words[1:])
    except Exception as e:
        err_msg = f"카멜 케이스 변환 중 오류 발생: {str(e)}"
        logging.error(err_msg)
        raise UtilProcessingError("카멜 케이스 변환 중 오류 발생")


def convert_to_upper_snake_case(camel_str: str) -> str:
    """
    파스칼/카멜 케이스를 대문자 스네이크 케이스로 변환
    
    Args:
        camel_str: 변환할 파스칼/카멜 케이스 문자열 (예: UserProfileService)
        
    Returns:
        대문자 스네이크 케이스로 변환된 문자열 (예: USER_PROFILE_SERVICE)
    """
    try:
        if not camel_str:
            return ""
        
        # 첫 번째 대문자 앞에는 '_'를 추가하지 않음
        result = camel_str[0].upper()
        
        # 나머지 문자들을 순회하면서 대문자를 '_대문자'로 변환
        for char in camel_str[1:]:
            if char.isupper():
                result += '_' + char
            else:
                result += char.upper()
                
        return result
    except Exception as e:
        err_msg = f"대문자 스네이크 케이스 변환 중 오류 발생: {str(e)}"
        logging.error(err_msg)
        raise UtilProcessingError("대문자 스네이크 케이스 변환 중 오류 발생")


def add_line_numbers(plsql: List[str]) -> Tuple[str, List[str]]:
    """
    PL/SQL 코드의 각 라인에 번호를 추가
    
    Args:
        plsql: 원본 PL/SQL 코드 (라인 단위 리스트)
        
    Returns:
        (numbered_plsql, numbered_lines): 라인 번호가 추가된 코드와 라인 리스트
    """
    try: 
        # 각 라인에 번호를 추가
        numbered_lines = [f"{index + 1}: {line}" for index, line in enumerate(plsql)]
        numbered_plsql = "".join(numbered_lines)
        return numbered_plsql, numbered_lines
    except Exception as e:
        err_msg = f"코드에 라인번호를 추가하는 도중 문제가 발생했습니다: {str(e)}"
        logging.error(err_msg)
        raise UtilProcessingError(err_msg)


#==============================================================================
# 코드 분석 및 변환 유틸리티
#==============================================================================

def calculate_code_token(code: Union[str, Dict, List]) -> int:
    """
    전달된 코드의 토큰 길이를 계산
    
    Args:
        code: 토큰 수를 계산할 코드 (문자열, 딕셔너리, 리스트 등 다양한 타입 가능)
        
    Returns:
        코드의 토큰 수
    """
    try:
        text_json = json.dumps(code, ensure_ascii=False)
        return len(ENCODER.encode(text_json))
    except Exception as e:
        err_msg = f"토큰 계산 도중 문제가 발생: {str(e)}"
        logging.error(err_msg)
        raise UtilProcessingError(err_msg)


async def extract_used_variable_nodes(startLine: int, local_variable_nodes: List[Dict]) -> Tuple[Dict, int]:
    """
    특정 코드 라인에서 사용된 변수들의 정보를 추출
    
    Args:
        startLine: 분석할 코드의 시작 라인 번호
        local_variable_nodes: Neo4j에서 조회한 모든 변수 노드 정보
        
    Returns:
        (used_variables, variable_tokens): 사용된 변수 정보와 토큰 수
    """
    try:
        used_variables = defaultdict(list)
        
        for variable_node in local_variable_nodes:
            for used_range in variable_node['v']:
                # 라인 범위 형식 검증 (예: "1_5")
                if not ('_' in used_range and all(part.isdigit() for part in used_range.split('_'))):
                    continue
                    
                # 시작 라인이 일치하는 경우만 처리
                used_startLine, used_endLine = map(int, used_range.split('_'))
                if used_startLine != startLine:
                    continue
                    
                # 변수 정보 저장
                range_key = f'{used_startLine}~{used_endLine}'
                var_type = variable_node['v'].get('type', 'Unknown')
                var_name = variable_node['v']['name']
                used_variables[range_key].append(f"{var_type}: {var_name}")
                break

        return used_variables, calculate_code_token(used_variables)
    
    except UtilProcessingError:
        raise
    except Exception as e:
        err_msg = f"사용된 변수 노드를 추출하는 도중 오류가 발생했습니다: {str(e)}"
        logging.error(err_msg)
        raise UtilProcessingError(err_msg)


async def extract_used_query_methods(start_line: int, end_line: int, 
                                   jpa_method_list: Dict, 
                                   used_jpa_method_dict: Dict) -> Dict:
    """
    특정 노드 범위 내에서 사용된 JPA 쿼리 메서드들을 식별하고 수집
    
    Args:
        start_line: 노드의 시작 라인
        end_line: 노드의 끝 라인
        jpa_method_list: 노드 내에서 사용된 JPA 메서드 목록
        used_jpa_method_dict: 사용된 JPA 메서드를 저장할 딕셔너리
        
    Returns:
        사용된 JPA 메서드를 저장한 딕셔너리
    """
    try:
        for range_key, method in jpa_method_list.items():
            method_start, method_end = map(int, range_key.split('~'))
            
            # 현재 범위 내에 있는 JPA 메서드 추출
            if start_line <= method_start <= end_line and start_line <= method_end <= end_line:
                used_jpa_method_dict[range_key] = method
                break

        return used_jpa_method_dict
        
    except Exception as e:
        err_msg = f"JPA 쿼리 메서드를 추출하는 도중 오류가 발생했습니다: {str(e)}"
        logging.error(err_msg)
        raise UtilProcessingError(err_msg)