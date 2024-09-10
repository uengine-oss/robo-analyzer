import unittest
import sys
import os
import logging
import unittest
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..', '..')))
from convert.create_main import start_main_processing


# * 로그 레벨을 INFO로 설정
logging.basicConfig(level=logging.INFO) 
logging.getLogger('asyncio').setLevel(logging.ERROR)


# 스프링부트 기반의 자바 메인 클래스를 생성하는 테스트
class TestMainGeneration(unittest.IsolatedAsyncioTestCase):
    async def test_create_Main(self):

        # * 테스트할 스토어드 프로시저 파일 이름을 설정 및 수정합니다. 
        sp_file_name = "P_B_CAC120_CALC_SUIP_STD"
        words = sp_file_name.split('_')
        pascal_file_name = ''.join(x.title() for x in words)
        lower_file_name = sp_file_name.replace('_', '').lower()


        try:
            # * Main 클래스 생성 테스트 시작
            await start_main_processing(lower_file_name, pascal_file_name)
            self.assertTrue(True, "Main 클래스 프로세스가 성공적으로 완료되었습니다.")
        except Exception:
            self.fail(f"Main 클래스 생성 테스트 중 예외 발생")

if __name__ == '__main__':
    unittest.main()
