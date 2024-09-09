import unittest
import os
import logging
import aiofiles

# 로깅 설정
logging.basicConfig(level=logging.INFO)
logging.getLogger('asyncio').setLevel(logging.ERROR)


# 역할 : 전달받은 이름을 케밥 표기법을 전환하는 함수입니다,
# 매개변수 : 
#   - fileName : 스토어드 프로시저 파일의 이름
# 반환값 : 케밥 표기법으로 전환된 프로젝트 이름
def convert_to_kebab_case(fileName):
    components = fileName.split('_')
    return '-'.join(x.lower() for x in components)


# 역할: 스프링 부트의 application.properties 파일을 생성하는 함수입니다.
# 매개변수: 
#   - fileName : 스토어드 프로시저 파일 이름
# 반환값: 없음
async def start_APLproperties_processing(fileName):

    try:
        # * 프로젝트의 이름을 케밥 케이스로 변경(pom.xml에서 아티팩트 ID와 동일하게 함)
        kebabCaseFileName = convert_to_kebab_case(fileName)
        
        # * properties 파일의 내용과 저장될 경로를 설정합니다.
        application_properties_content = f"spring.application.name={kebabCaseFileName}\nserver.port=8082"
        application_properties_directory = os.path.join('test', 'test_converting', 'converting_result', 'properties')  
        os.makedirs(application_properties_directory, exist_ok=True)  


        # * application.properties 파일로 쓰기 작업을 수행합니다.
        application_properties_file_path = os.path.join(application_properties_directory, "application.properties")  
        async with aiofiles.open(application_properties_file_path, 'w', encoding='utf-8') as file:  
            await file.write(application_properties_content)  
            logging.info(f"\nSuccess Create Application Properties\n")  

    except Exception:
        logging.exception(f"Error occurred while create application.properties")
        raise


# application.properties 파일을 생성하는 테스트 모듈
class AsyncTestCase(unittest.IsolatedAsyncioTestCase):
    async def test_create_properties_file(self):
        await start_APLproperties_processing("P_B_CAC120_CALC_SUIP_STD")


if __name__ == '__main__':
    unittest.main()