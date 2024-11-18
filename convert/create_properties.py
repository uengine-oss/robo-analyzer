import os
import logging
import aiofiles
from util.exception import AplPropertiesCreationError


# 역할: 스프링 부트의 application.properties 파일을 생성하는 함수입니다.
# 매개변수: 없음
# 반환값: 없음
async def start_APLproperties_processing():
    
    logging.info("application.properties 생성을 시작합니다.")

    try:
        # * properties 파일의 내용과 저장될 경로를 설정합니다.
        application_properties_content = f"spring.application.name=demo\nserver.port=8082"
        base_directory = os.getenv('DOCKER_COMPOSE_CONTEXT')
        if base_directory:
            application_properties_directory = os.path.join(base_directory, 'java', 'demo', 'src', 'main', 'resources')
        else:
            current_dir = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
            application_properties_directory = os.path.join(current_dir, 'target', 'java', 'demo', 'src', 'main', 'resources')
        os.makedirs(application_properties_directory, exist_ok=True)  


        # * application.properties 파일로 쓰기 작업을 수행합니다.
        application_properties_file_path = os.path.join(application_properties_directory, "application.properties")  
        async with aiofiles.open(application_properties_file_path, 'w', encoding='utf-8') as file:  
            await file.write(application_properties_content)  
            logging.info(f"application.properties 파일이 생성되었습니다.\n")  

    except Exception:
        err_msg = "스프링부트의 application.properties 파일을 생성하는 도중 오류가 발생했습니다."
        logging.exception(err_msg)
        raise AplPropertiesCreationError(err_msg)