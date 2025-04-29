import logging
import tiktoken

from util.exception import ConvertingError
from util.utility_tool import convert_to_camel_case, convert_to_pascal_case


encoder = tiktoken.get_encoding("cl100k_base")
# 프로젝트 이름은 함수 매개변수로 받음


# 역할: 컨트롤러 클래스의 기본 구조를 생성하는 함수입니다.
#
# 매개변수: 
#   - object_name: plsql 패키지 이름
#   - exist_command_class: 커맨드 클래스가 존재하는지 여부
#   - project_name: 프로젝트 이름
#
# 반환값: 
#   - controller_class_template: 생성된 컨트롤러 클래스 템플릿
#   - controller_class_name: 생성된 컨트롤러 클래스 이름
async def generate_controller_skeleton(object_name: str, exist_command_class: bool, project_name: str) -> str:
    try:
        # * 컨트롤러 클래스명 생성 
        pascal_name = convert_to_pascal_case(object_name)
        camel_name = convert_to_camel_case(object_name)
        controller_class_name = pascal_name + "Controller"


        # * 파라미터가 있는 경우 커맨드 패키지 임포트 추가
        command_import = (f"import com.example.{project_name}.command.{camel_name}.*;\n" 
                         if exist_command_class else "")
        
        
        # * 컨트롤러 클래스 템플릿 생성
        controller_class_template = f"""package com.example.{project_name}.controller;

{command_import}import com.example.{project_name}.service.{pascal_name}Service;
import org.springframework.web.bind.annotation.*;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.ResponseEntity;
import org.springframework.transaction.annotation.Transactional;
import java.util.*;

@RestController
@RequestMapping("/{camel_name}")
public class {controller_class_name} {{

    @Autowired
    private {pascal_name}Service {camel_name}Service;

CodePlaceHolder
}}"""

        return controller_class_template ,controller_class_name
    
    except ConvertingError:
        raise
    except Exception as e:
        err_msg = f"컨트롤러 클래스 골격을 생성하는 도중 문제가 발생: {str(e)}"
        logging.error(err_msg)
        raise ConvertingError(err_msg)


# 역할: 컨트롤러 클래스 기본 구조를 생성 프로세스를 시작하고 관리하는 함수입니다.
#
# 매개변수:
#   - object_name: plsql 패키지 이름
#   - exist_command_class: 커맨드 클래스가 존재하는지 여부
#   - api_key: Claude API 키
#   - project_name: 프로젝트 이름
#
# 반환값:
#   - controller_skeleton: 생성된 스프링부트 컨트롤러 클래스 코드 문자열
#   - controller_class_name: 생성된 컨트롤러 클래스명 (예: EmployeeManagementController)
async def start_controller_skeleton_processing(object_name: str, exist_command_class: bool, project_name: str) -> tuple[str, str]:
    try:
        controller_skeleton, controller_class_name = await generate_controller_skeleton(object_name, exist_command_class, project_name)
        logging.info(f"[{object_name}] 컨트롤러 클래스 골격이 생성되었습니다.\n")
        return controller_skeleton, controller_class_name
        
    except ConvertingError:
        raise
    except Exception as e:
        err_msg = f"컨트롤러 클래스 골격 생성 중 오류가 발생했습니다: {str(e)}"
        logging.error(err_msg)
        raise ConvertingError(err_msg)