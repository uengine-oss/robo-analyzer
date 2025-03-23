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
    



MAIN_FILE_NAME = "main.py"
MAIN_FILE_PATH = 'demo/app'

# FastAPI 메인 템플릿
FASTAPI_MAIN_TEMPLATE = """import uvicorn
from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware

# 설정 파일에서 설정 가져오기
from app.config import engine, Base, APP_PORT

# 라우터 가져오기 (구현 필요)
# from app.routers import user_router, item_router

# 모델 가져오기 (구현 필요)
# from app.models import user, item

# FastAPI 앱 생성
app = FastAPI(
    title="Demo API",
    description="Demo API Service",
    version="0.1.0"
)

# CORS 설정
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],  # 실제 환경에서는 특정 도메인만 허용하세요
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# 라우터 등록
# app.include_router(user_router.router, prefix="/api", tags=["users"])
# app.include_router(item_router.router, prefix="/api", tags=["items"])

# 애플리케이션 시작 시 데이터베이스 테이블 생성
Base.metadata.create_all(bind=engine)

@app.get("/")
def read_root():
    return {"message": "Welcome to Demo API"}

# 개발 서버 실행
if __name__ == "__main__":
    uvicorn.run("app.main:app", host="0.0.0.0", port=APP_PORT, reload=True)
"""


# 역할: Python FastAPI 애플리케이션의 시작점이 되는 main.py 파일을 생성합니다.
#
# 매개변수:
#   - user_id : 사용자 ID
async def start_main_processing_python(user_id:str):
    logging.info("FastAPI 메인 파일 생성을 시작합니다.")

    try:
        # * 메인 템플릿 선택
        main_template = FASTAPI_MAIN_TEMPLATE

        # * 저장 경로 설정
        if os.getenv('DOCKER_COMPOSE_CONTEXT'):
            save_path = os.path.join(os.getenv('DOCKER_COMPOSE_CONTEXT'), 'target', 'python', user_id, MAIN_FILE_PATH)
        else:
            parent_workspace_dir = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
            save_path = os.path.join(parent_workspace_dir, 'target', 'python', user_id, MAIN_FILE_PATH)

        # * 메인 파일 생성
        await save_file(
            content=main_template, 
            filename=MAIN_FILE_NAME, 
            base_path=save_path
        )
        
        logging.info("FastAPI 메인 파일이 생성되었습니다.\n")
    
    except SaveFileError:
        raise
    except Exception as e:
        err_msg = f"FastAPI 메인 파일을 생성하는 도중 오류가 발생했습니다: {str(e)}"
        logging.error(err_msg)
        raise MainCreationError(err_msg)