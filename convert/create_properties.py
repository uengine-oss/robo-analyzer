import os
import logging
import aiofiles


# 역할: 스프링 부트의 application.properties 파일을 생성하는 함수입니다.
# 매개변수: 
#   - lower_case : 소문자 프로젝트 이름
# 반환값: 없음
async def start_APLproperties_processing(lower_case):

    try:
        # * properties 파일의 내용과 저장될 경로를 설정합니다.
        application_properties_content = f"spring.application.name={lower_case}\nserver.port=8082"
        base_directory = os.getenv('DOCKER_COMPOSE_CONTEXT', 'convert')
        application_properties_directory = os.path.join(base_directory, 'converting_result', f'{lower_case}', 'src', 'main', 'resources')
        os.makedirs(application_properties_directory, exist_ok=True)  


        # * application.properties 파일로 쓰기 작업을 수행합니다.
        application_properties_file_path = os.path.join(application_properties_directory, "application.properties")  
        async with aiofiles.open(application_properties_file_path, 'w', encoding='utf-8') as file:  
            await file.write(application_properties_content)  
            logging.info(f"\nSuccess Create Application Properties\n")  

    except Exception:
        logging.exception(f"Error occurred while create application.properties")
        raise