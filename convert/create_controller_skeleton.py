import logging
import tiktoken
from util.exception import SkeletonCreationError, StringConversionError, TemplateGenerationError
from util.string_utils import convert_to_camel_case, convert_to_pascal_case

encoder = tiktoken.get_encoding("cl100k_base")
JAVA_PATH = 'java/demo/src/main/java/com/example/demo'


# 역할: 컨트롤러 클래스의 기본 구조를 생성하는 함수입니다.
#
# 매개변수: 
#   - object_name: plsql 패키지 이름
#   - exist_command_class: 커맨드 클래스가 존재하는지 여부
#
# 반환값: 
#   - controller_class_template: 생성된 컨트롤러 클래스 템플릿
#   - controller_class_name: 생성된 컨트롤러 클래스 이름
async def generate_controller_skeleton(object_name: str, exist_command_class: bool) -> str:
    try:
        # * 컨트롤러 클래스명 생성 
        pascal_name = convert_to_pascal_case(object_name)
        camel_name = convert_to_camel_case(object_name)
        controller_class_name = pascal_name + "Controller"


        # * 파라미터가 있는 경우 커맨드 패키지 임포트 추가
        command_import = (f"import com.example.demo.command.{camel_name}.*;\n" 
                         if exist_command_class else "")
        
        
        # * 컨트롤러 클래스 템플릿 생성
        controller_class_template = f"""package com.example.demo.controller;

{command_import}import com.example.demo.service.{pascal_name}Service;
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
    
    except StringConversionError:
        raise
    except Exception as e:
        err_msg = f"컨트롤러 클래스 골격을 생성하는 도중 문제가 발생: {str(e)}"
        logging.error(err_msg)
        raise TemplateGenerationError(err_msg)


# 역할: 컨트롤러 클래스 기본 구조를 생성 프로세스를 시작하고 관리하는 함수입니다.
#
# 매개변수:
#   - object_name: plsql 패키지 이름
#   - exist_command_class: 커맨드 클래스가 존재하는지 여부
#
# 반환값:
#   - controller_skeleton: 생성된 컨트롤러 클래스의 기본 구조
#   - controller_class_name: 생성된 컨트롤러 클래스 이름
async def start_controller_skeleton_processing(object_name: str, exist_command_class: bool) -> str:

    logging.info(f"[{object_name}] 컨트롤러 틀 생성을 시작합니다.")

    try:
        # * 컨트롤러 클래스의 틀을 생성합니다.
        controller_skeleton, controller_class_name = await generate_controller_skeleton(object_name, exist_command_class)
        logging.info(f"[{object_name}] 컨트롤러 틀 생성 완료\n")
        return controller_skeleton, controller_class_name

    except (StringConversionError, TemplateGenerationError):
        raise
    except Exception as e:
        err_msg = f"컨트롤러 골격 클래스를 생성하기 위해 데이터를 준비하는 도중 문제가 발생: {str(e)}"
        logging.error(err_msg)
        raise SkeletonCreationError(err_msg)
    



    PYTHON_PATH = 'demo/app'


# 역할: 컨트롤러 클래스의 기본 구조를 생성하는 함수입니다.
#
# 매개변수: 
#   - object_name: plsql 패키지 이름
#   - exist_command_class: 커맨드 클래스가 존재하는지 여부
#
# 반환값: 
#   - controller_class_template: 생성된 컨트롤러 클래스 템플릿
#   - controller_class_name: 생성된 컨트롤러 클래스 이름
async def generate_controller_skeleton_python(object_name: str, exist_command_class: bool) -> str:
    try:
        # * 컨트롤러 클래스명 생성 
        pascal_name = convert_to_pascal_case(object_name)
        camel_name = convert_to_camel_case(object_name)
        controller_class_name = camel_name + "_router.py"


        # * 파라미터가 있는 경우 스키마 임포트 추가
        command_import = (f"from app.command.{camel_name} import *\n" 
                         if exist_command_class else "")
        
        
        # * 컨트롤러 클래스 템플릿 생성
        controller_class_template = f"""from fastapi import APIRouter, Depends, HTTPException, status
from sqlalchemy.orm import Session
{command_import}from app.config import get_db
from app.service.{pascal_name}Service import {pascal_name}Service
from typing import List, Optional

router = APIRouter(prefix="/{camel_name}", tags=["{pascal_name}"])

# 서비스 의존성
def get_{camel_name}_service(db: Session = Depends(get_db)):
    return {pascal_name}Service(db)

CodePlaceHolder
"""

        return controller_class_template, controller_class_name
    
    except StringConversionError:
        raise
    except Exception as e:
        err_msg = f"컨트롤러 클래스 골격을 생성하는 도중 문제가 발생: {str(e)}"
        logging.error(err_msg)
        raise TemplateGenerationError(err_msg)


# 역할: 컨트롤러 클래스 기본 구조를 생성 프로세스를 시작하고 관리하는 함수입니다.
#
# 매개변수:
#   - object_name: plsql 패키지 이름
#   - exist_command_class: 커맨드 클래스가 존재하는지 여부
#
# 반환값:
#   - controller_skeleton: 생성된 컨트롤러 클래스의 기본 구조
#   - controller_class_name: 생성된 컨트롤러 클래스 이름
async def start_controller_skeleton_processing(object_name: str, exist_command_class: bool) -> str:

    logging.info(f"[{object_name}] 컨트롤러 틀 생성을 시작합니다.")

    try:
        # * 컨트롤러 클래스의 틀을 생성합니다.
        controller_skeleton, controller_class_name = await generate_controller_skeleton_python(object_name, exist_command_class)
        logging.info(f"[{object_name}] 컨트롤러 틀 생성 완료\n")
        return controller_skeleton, controller_class_name

    except (StringConversionError, TemplateGenerationError):
        raise
    except Exception as e:
        err_msg = f"컨트롤러 골격 클래스를 생성하기 위해 데이터를 준비하는 도중 문제가 발생: {str(e)}"
        logging.error(err_msg)
        raise SkeletonCreationError(err_msg)