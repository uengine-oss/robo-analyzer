import unittest
import sys
import os
import logging
import unittest
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..', '..')))
from convert.create_properties import start_APLproperties_processing


# * 로그 레벨을 INFO로 설정
logging.basicConfig(level=logging.INFO) 
logging.getLogger('asyncio').setLevel(logging.ERROR)


# 스프링부트 기반의 자바 application.properties를 생성하는 테스트
class TestAplPropertiesGeneration(unittest.IsolatedAsyncioTestCase):
    async def test_create_AplProperties(self):

        # * 테스트할 스토어드 프로시저 파일 이름을 설정 및 수정합니다. 
        sp_file_name = "P_B_CAC120_CALC_SUIP_STD"
        lower_file_name = sp_file_name.replace('_', '').lower()


        try:
            # * application.properties 생성 테스트 시작
            await start_APLproperties_processing(lower_file_name)
            self.assertTrue(True, "application.properties 프로세스가 성공적으로 완료되었습니다.")
        except Exception:
            self.fail(f"application.properties 생성 테스트 중 예외 발생")

if __name__ == '__main__':
    unittest.main()
