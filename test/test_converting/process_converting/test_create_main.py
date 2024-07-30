import unittest
import os
import logging
import aiofiles

# 로깅 설정
logging.basicConfig(level=logging.INFO)
logging.getLogger('asyncio').setLevel(logging.ERROR)


# 역할: Spring Boot 애플리케이션의 메인 클래스 파일을 생성하는 함수입니다.
# 매개변수: 
#   - fileName : 스토어드 프로시저 파일의 이름.
# 반환값: 없음.
async def start_main_processing(fileName):
    
    try:
        # * 스토어드 파일 이름의 첫글자만 대문자로 변경합니다.
        upperFileName = fileName[0].upper() + fileName[1:]


        # * 메인 클래스 파일의 내용을 설정합니다.
        main_class_content = f"""
package com.example.{fileName};

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;

@SpringBootApplication
public class {upperFileName}Application {{

    public static void main(String[] args) {{
        SpringApplication.run({upperFileName}Application.class, args);
    }}

}}
        """


        # * 메인 클래스 파일을 저장할 디렉토리 경로를 설정합니다.
        main_class_directory = os.path.join('test', 'test_converting', 'converting_result', 'main')  
        os.makedirs(main_class_directory, exist_ok=True)  


        # * 메인 클래스를 파일로 쓰기 작업을 수행합니다.
        main_class_path = os.path.join(main_class_directory, f"{upperFileName}Application.java")  
        async with aiofiles.open(main_class_path, 'w', encoding='utf-8') as file:  
            await file.write(main_class_content)  
            logging.info(f"\nSuccess Create Main Class\n") 


    except Exception:
        logging.exception(f"Error occurred while create main class")
        raise


# Main 클래스를 생성하는 테스트 모듈
class AsyncTestCase(unittest.IsolatedAsyncioTestCase):
    async def test_create_main_file(self):
        await start_main_processing("P_B_CAC120_CALC_SUIP_STD")


if __name__ == '__main__':
    unittest.main()