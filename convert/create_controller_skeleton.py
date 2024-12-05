import logging
import textwrap
import tiktoken
from prompt.convert_controller_prompt import convert_controller_method_code
from util.exception import ConvertingError, ExtractCodeError, HandleResultError, LLMCallError, Neo4jError, ProcessResultError, SaveFileError, SkeletonCreationError, TraverseCodeError

encoder = tiktoken.get_encoding("cl100k_base")
JAVA_PATH = 'java/demo/src/main/java/com/example/demo'


# 역할: 스네이크 케이스 형식의 문자열을 자바 클래스명으로 사용할 수 있는 파스칼 케이스로 변환합니다.
#      예시) user_profile_service -> UserProfileService
# 매개변수: 
#   - snake_case_input: 변환할 스네이크 케이스 문자열
#                      (예: employee_payroll, user_profile_service)
# 반환값: 
#   - 파스칼 케이스로 변환된 문자열
#     (예: snake_case_input이 'employee_payroll'인 경우 -> 'EmployeePayroll')
def convert_to_pascal_case(snake_str: str) -> str:
    return ''.join(word.capitalize() for word in snake_str.split('_'))


# 역할: 스네이크 케이스 형식의 문자열을 자바 클래스명으로 사용할 수 있는 카멜 케이스로 변환합니다.
#      예시) user_profile_service -> userProfileService
# 매개변수: 
#   - snake_str: 변환할 스네이크 케이스 문자열
#                (예: user_profile_service)
# 반환값: 
#   - 카멜 케이스로 변환된 문자열
#     (예: userProfileService)
def convert_to_camel_case(snake_str: str) -> str:
    words = snake_str.split('_')
    return words[0].lower() + ''.join(word.capitalize() for word in words[1:])


# 역할: 컨트롤러 클래스의 기본 구조를 생성하는 함수입니다.
# 매개변수: 
#   - object_name: plsql 패키지 이름
# 반환값: 
#   - controller_class_template: 생성된 컨트롤러 클래스 템플릿
#   - controller_class_name: 생성된 컨트롤러 클래스 이름
async def create_controller_skeleton(object_name: str) -> str:
    try:
        # * 1. 컨트롤러 클래스명 생성 
        controller_class_name = convert_to_pascal_case(object_name) + "Controller"
        dir_name = convert_to_camel_case(object_name)


        # * 2. 컨트롤러 클래스 템플릿 생성
        controller_class_template = f"""package com.example.demo.controller;

import com.example.demo.command.{dir_name}.*;
import com.example.demo.service.{object_name}Service;
import org.springframework.web.bind.annotation.*;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.ResponseEntity;
import org.springframework.transaction.annotation.Transactional;
import java.util.*;

@RestController
@RequestMapping("/{dir_name}")
public class {controller_class_name} {{

    @Autowired
    private {convert_to_pascal_case(object_name)}Service {convert_to_camel_case(object_name)}Service;

CodePlaceHolder
}}"""

        return controller_class_template ,controller_class_name
    
    except (LLMCallError):
        raise
    except Exception:
        err_msg = "컨트롤러 클래스 골격을 생성하는 도중 문제가 발생했습니다."
        logging.error(err_msg, exc_info=False)
        raise ExtractCodeError(err_msg)


# 역할: 컨트롤러 클래스 기본 구조를 생성 프로세스를 시작하고 관리하는 함수입니다.
# 매개변수:
#   - object_name: plsql 패키지 이름
# 반환값:
#   - controller_skeleton: 생성된 컨트롤러 클래스의 기본 구조
#   - controller_class_name: 생성된 컨트롤러 클래스 이름
async def start_controller_skeleton_processing(object_name):

    logging.info(f"[{object_name}] 컨트롤러 틀 생성을 시작합니다.")

    try:
        # * 컨트롤러 클래스의 틀을 생성합니다.
        controller_skeleton, controller_class_name = await create_controller_skeleton(object_name)
        logging.info(f"[{object_name}] 컨트롤러 틀 생성 완료\n")
        return controller_skeleton, controller_class_name

    except (ConvertingError):
        raise
    except Exception:
        err_msg = "컨트롤러 골격 클래스를 생성하기 위해 데이터를 준비하는 도중 문제가 발생했습니다."
        logging.error(err_msg, exc_info=False)
        raise SkeletonCreationError(err_msg)
