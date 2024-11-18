import os
import logging
import aiofiles
from util.exception import MainCreationError


# 역할: Spring Boot 애플리케이션의 메인 클래스 파일을 생성하는 함수입니다.
# 매개변수: 없음
# 반환값: 없음.
async def start_main_processing():
    
    logging.info("메인 클래스 생성을 시작합니다.")

    try:

        # * 메인 클래스 파일의 내용을 설정합니다.
        main_class_content = f"""
package com.example.demo;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;

@SpringBootApplication
public class DemoApplication {{

    public static void main(String[] args) {{
        SpringApplication.run(DemoApplication.class, args);
    }}

}}
        """


        # * 메인 클래스 파일을 저장할 디렉토리 경로를 설정합니다.
        base_directory = os.getenv('DOCKER_COMPOSE_CONTEXT')
        if base_directory:
            main_class_directory = os.path.join(base_directory, 'java', 'demo', 'src', 'main', 'java', 'com', 'example', 'demo')
        else:
            current_dir = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
            main_class_directory = os.path.join(current_dir, 'target', 'java', 'demo', 'src', 'main', 'java', 'com', 'example', 'demo')
        os.makedirs(main_class_directory, exist_ok=True)  


        # * 메인 클래스를 파일로 쓰기 작업을 수행합니다.
        main_class_path = os.path.join(main_class_directory, "DemoApplication.java")  
        async with aiofiles.open(main_class_path, 'w', encoding='utf-8') as file:  
            await file.write(main_class_content)  
            logging.info(f"메인 클래스가 생성되었습니다.\n")  
        
    except Exception:
        err_msg = "스프링부트의 메인 클래스를 생성하는 도중 오류가 발생했습니다."
        logging.exception(err_msg)
        raise MainCreationError(err_msg)