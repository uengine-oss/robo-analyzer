import logging
from util.exception import StringConversionError


# 역할: 스네이크 케이스 형식의 문자열을 자바 클래스명으로 사용할 수 있는 파스칼 케이스로 변환합니다.
# 
# 매개변수: 
#   - snake_case_input: 변환할 스네이크 케이스 문자열 (예: employee_payroll, user_profile_service)
# 
# 반환값: 
#   - 파스칼 케이스로 변환된 문자열 (예: snake_case_input이 'employee_payroll'인 경우 -> 'EmployeePayroll')
def convert_to_pascal_case(snake_str: str) -> str:
    try:
        if '_' not in snake_str:
            return snake_str
        
        return ''.join(word.capitalize() for word in snake_str.split('_'))
    except Exception as e:
        err_msg = f"파스칼 케이스 변환 중 오류 발생: {str(e)}"
        logging.error(err_msg)
        raise StringConversionError("파스칼 케이스 변환 중 오류 발생")


# 역할: 스네이크 케이스 형식의 문자열을 자바 클래스명으로 사용할 수 있는 카멜 케이스로 변환합니다.
#
# 매개변수: 
#   - snake_str: 변환할 스네이크 케이스 문자열 (예: user_profile_service)
#
# 반환값: 
#   - 카멜 케이스로 변환된 문자열 (예: userProfileService)
def convert_to_camel_case(snake_str: str) -> str:
    try:
        words = snake_str.split('_')
        return words[0].lower() + ''.join(word.capitalize() for word in words[1:])
    except Exception as e:
        err_msg = f"카멜 케이스 변환 중 오류 발생: {str(e)}"
        logging.error(err_msg)
        raise StringConversionError("카멜 케이스 변환 중 오류 발생")