import os
import logging
from util.exception import AplPropertiesCreationError, SaveFileError
from util.file_utils import save_file

PROPERTIES_FILE_NAME = "application.properties"
# 프로젝트 경로는 함수 매개변수로 받음

# 역할: Spring Boot 애플리케이션의 설정 정보를 담은 properties 파일을 생성합니다.
#
# 매개변수:
#   - user_id : 사용자 ID
#   - project_name : 프로젝트 이름
async def start_APLproperties_processing(user_id:str, project_name:str) -> str:
    logging.info("application.properties 생성을 시작합니다.")
    
    try:
        # application.properties 템플릿 생성
        properties_template = f"""spring.application.name={project_name}
spring.h2.console.enabled=true
spring.datasource.url=jdbc:h2:mem:testdb
spring.datasource.driverClassName=org.h2.Driver
spring.jpa.database-platform=org.hibernate.dialect.H2Dialect
spring.jpa.hibernate.ddl-auto=create-drop"""
        
        # 리소스 경로 설정
        properties_path = f'{project_name}/src/main/resources'

        # * 저장 경로 설정
        if os.getenv('DOCKER_COMPOSE_CONTEXT'):
            save_path = os.path.join(os.getenv('DOCKER_COMPOSE_CONTEXT'), 'target', 'java', user_id, properties_path)
        else:
            parent_workspace_dir = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
            save_path = os.path.join(parent_workspace_dir, 'target', 'java', user_id, properties_path)


        # * application.properties 파일 생성
        await save_file(
            content=properties_template, 
            filename=PROPERTIES_FILE_NAME, 
            base_path=save_path
        )
        
        logging.info("application.properties가 생성되었습니다.\n")
        return properties_template

    except SaveFileError:
        raise
    except Exception as e:
        err_msg = f"스프링부트의 application.properties 파일을 생성하는 도중 오류가 발생했습니다: {str(e)}"
        logging.error(err_msg)
        raise AplPropertiesCreationError(err_msg)