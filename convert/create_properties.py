import os
import logging
from util.exception import AplPropertiesCreationError, SaveFileError
from util.file_utils import save_file

PROPERTIES_FILE_NAME = "application.properties"
PROPERTIES_PATH = 'demo/src/main/resources'

# JPA 템플릿
JPA_PROPERTIES_TEMPLATE = """spring.application.name=demo
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

# MyBatis 템플릿
MYBATIS_PROPERTIES_TEMPLATE = """spring.application.name=demo
server.port=8082

mybatis.type-aliases-package=com.example.demo.entity
mybatis.mapper-locations=classpath:mapper/**/*.xml
mybatis.configuration.map-underscore-to-camel-case=true

spring.datasource.url=jdbc:oracle:thin:@localhost:1521/javadb
spring.datasource.username=c##debezium
spring.datasource.password=dbz
spring.datasource.driver-class-name=oracle.jdbc.OracleDriver

logging.level.com.example.demo.mapper=TRACE"""


# 역할: Spring Boot 애플리케이션의 설정 파일인 application.properties를 생성합니다.
#     
# 매개변수:
#   - orm_type : 사용할 ORM 유형 (jpa, mybatis)
#   - user_id : 사용자 ID
async def start_APLproperties_processing(orm_type: str, user_id:str):
    logging.info("application.properties 생성을 시작합니다.")

    try:
        # * 템플릿 선택
        properties_template = JPA_PROPERTIES_TEMPLATE if orm_type == 'jpa' else MYBATIS_PROPERTIES_TEMPLATE


        # * 저장 경로 설정
        if os.getenv('DOCKER_COMPOSE_CONTEXT'):
            save_path = os.path.join(os.getenv('DOCKER_COMPOSE_CONTEXT'), 'target', 'java', user_id, PROPERTIES_PATH)
        else:
            parent_workspace_dir = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
            save_path = os.path.join(parent_workspace_dir, 'target', 'java', user_id, PROPERTIES_PATH)


        # * application.properties 파일 생성
        await save_file(
            content=properties_template,
            filename=PROPERTIES_FILE_NAME, 
            base_path=save_path
        )
        
        logging.info("application.properties 파일이 생성되었습니다.\n")

    except SaveFileError:
        raise
    except Exception as e:
        err_msg = f"스프링부트의 application.properties 파일을 생성하는 도중 오류가 발생했습니다: {str(e)}"
        logging.error(err_msg)
        raise AplPropertiesCreationError(err_msg)