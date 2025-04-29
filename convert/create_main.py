import os
import logging

from util.exception import ConvertingError, GenerateTargetError
from util.utility_tool import save_file

# 프로젝트 이름은 함수 매개변수로 받음

# 역할: Spring Boot 애플리케이션의 시작점이 되는 메인 클래스 파일을 생성합니다.
#
# 매개변수:
#   - user_id : 사용자 ID
#   - project_name : 프로젝트 이름
async def start_main_processing(user_id:str, project_name:str) -> str:
    logging.info("메인 클래스 생성을 시작합니다.")

    try:
        # 메인 클래스명과 경로 생성
        main_class_name = f"{project_name.capitalize()}Application.java"
        main_class_path = f'{project_name}/src/main/java/com/example/{project_name}'
        
        # JPA 메인 클래스 템플릿 생성
        main_template = f"""
package com.example.{project_name};

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;

@SpringBootApplication
public class {project_name.capitalize()}Application {{

    public static void main(String[] args) {{
        SpringApplication.run({project_name.capitalize()}Application.class, args);
    }}

}}
"""

        # * 저장 경로 설정
        if os.getenv('DOCKER_COMPOSE_CONTEXT'):
            save_path = os.path.join(os.getenv('DOCKER_COMPOSE_CONTEXT'), 'target', 'java', user_id, main_class_path)
        else:
            parent_workspace_dir = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
            save_path = os.path.join(parent_workspace_dir, 'target', 'java', user_id, main_class_path)


        # * 메인 클래스 파일 생성
        await save_file(
            content=main_template, 
            filename=main_class_name, 
            base_path=save_path
        )
        
        logging.info("메인 클래스가 생성되었습니다.\n")
        return main_template
    
    except ConvertingError:
        raise
    except Exception as e:
        err_msg = f"스프링부트의 메인 클래스를 생성하는 도중 오류가 발생했습니다: {str(e)}"
        logging.error(err_msg)
        raise GenerateTargetError(err_msg)