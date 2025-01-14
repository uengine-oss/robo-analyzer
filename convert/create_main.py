import os
import logging
from util.exception import MainCreationError, SaveFileError
from util.file_utils import save_file

MAIN_CLASS_NAME = "DemoApplication.java"
MAIN_CLASS_PATH = 'demo/src/main/java/com/example/demo'

# JPA용 메인 클래스 템플릿
JPA_MAIN_CLASS_TEMPLATE = """
package com.example.demo;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;

@SpringBootApplication
public class DemoApplication {

    public static void main(String[] args) {
        SpringApplication.run(DemoApplication.class, args);
    }

}
"""

# MyBatis용 메인 클래스 템플릿
MYBATIS_MAIN_CLASS_TEMPLATE = """
package com.example.demo;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.mybatis.spring.annotation.MapperScan;

@SpringBootApplication
@MapperScan(basePackages = "com.example.demo.repository")
public class DemoApplication {

    public static void main(String[] args) {
        SpringApplication.run(DemoApplication.class, args);
    }

}
"""

# 역할: Spring Boot 애플리케이션의 시작점이 되는 메인 클래스 파일을 생성합니다.
#
# 매개변수:
#   - orm_type : ORM 유형 (jpa, mybatis)
#   - user_id : 사용자 ID
async def start_main_processing(orm_type: str, user_id:str):
    logging.info("메인 클래스 생성을 시작합니다.")

    try:
        # * 템플릿 선택
        main_template = JPA_MAIN_CLASS_TEMPLATE if orm_type == 'jpa' else MYBATIS_MAIN_CLASS_TEMPLATE


        # * 저장 경로 설정
        if os.getenv('DOCKER_COMPOSE_CONTEXT'):
            save_path = os.path.join(os.getenv('DOCKER_COMPOSE_CONTEXT'), 'target', 'java', user_id, MAIN_CLASS_PATH)
        else:
            parent_workspace_dir = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
            save_path = os.path.join(parent_workspace_dir, 'target', 'java', user_id, MAIN_CLASS_PATH)


        # * 메인 클래스 파일 생성
        await save_file(
            content=main_template, 
            filename=MAIN_CLASS_NAME, 
            base_path=save_path
        )
        
        logging.info("메인 클래스가 생성되었습니다.\n")
    
    except SaveFileError:
        raise
    except Exception as e:
        err_msg = f"스프링부트의 메인 클래스를 생성하는 도중 오류가 발생했습니다: {str(e)}"
        logging.error(err_msg)
        raise MainCreationError(err_msg)