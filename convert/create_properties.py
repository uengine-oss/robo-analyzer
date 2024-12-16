import os
import logging
import aiofiles
from util.exception import AplPropertiesCreationError

PROPERTIES_FILE_NAME = "application.properties"
PROPERTIES_PATH = 'java/demo/src/main/resources'
APPLICATION_PROPERTIES_TEMPLATE = """spring.application.name=demo
server.port=8082
spring.jpa.hibernate.naming.physical-strategy=org.hibernate.boot.model.naming.PhysicalNamingStrategyStandardImpl
spring.jpa.hibernate.ddl-auto=create
spring.jpa.properties.hibernate.show_sql=true
spring.jpa.properties.hibernate.format_sql=true
spring.jpa.properties.hibernate.dialect=org.hibernate.dialect.OracleDialect

spring.datasource.url=jdbc:oracle:thin:@localhost:1521/javadb
spring.datasource.username=c##debezium
spring.datasource.password=dbz
spring.datasource.driver-class-name=oracle.jdbc.OracleDriver"""


# 역할: Spring Boot 애플리케이션의 설정 파일인 application.properties를 생성합니다.
#      이 파일은 애플리케이션의 이름, 포트 번호 등 주요 설정을 관리합니다.
# 매개변수: 없음
# 반환값: 없음
async def start_APLproperties_processing():
    
    logging.info("application.properties 생성을 시작합니다.")

    try:
        # * properties 파일의 내용과 저장될 경로를 설정합니다.
        base_directory = os.getenv('DOCKER_COMPOSE_CONTEXT')
        if base_directory:
            application_properties_directory = os.path.join(base_directory, PROPERTIES_PATH)
        else:
            parent_workspace_dir = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
            application_properties_directory = os.path.join(parent_workspace_dir, 'target', PROPERTIES_PATH)
        os.makedirs(application_properties_directory, exist_ok=True)  


        # * application.properties 파일로 생성합니다.
        application_properties_file_path = os.path.join(application_properties_directory, PROPERTIES_FILE_NAME)  
        async with aiofiles.open(application_properties_file_path, 'w', encoding='utf-8') as file:  
            await file.write(APPLICATION_PROPERTIES_TEMPLATE)  
            logging.info(f"application.properties 파일이 생성되었습니다.\n")  

    except Exception:
        err_msg = "스프링부트의 application.properties 파일을 생성하는 도중 오류가 발생했습니다."
        logging.error(err_msg)
        raise AplPropertiesCreationError(err_msg)