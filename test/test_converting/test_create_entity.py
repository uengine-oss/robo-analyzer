import unittest
import sys
import os
import logging
import unittest
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..', '..')))
from convert.create_entity import start_entity_processing


# * 로그 레벨을 INFO로 설정
logging.basicConfig(level=logging.INFO) 
logging.getLogger('asyncio').setLevel(logging.ERROR)


# 스프링부트 기반의 자바 엔티티 클래스를 생성하는 테스트)
class TestEntityGeneration(unittest.IsolatedAsyncioTestCase):
    async def test_create_entity(self):

        # * 테스트할 스토어드 프로시저 파일 이름을 설정 및 수정합니다. 
        sp_file_name = "P_B_CAC120_CALC_SUIP_STD"
        lower_file_name = sp_file_name.replace('_', '').lower()


        # * 엔티티 클래스 생성 테스트 시작
        try:
            isSuccess = await start_entity_processing(lower_file_name)
            if isinstance(isSuccess, Exception): raise isSuccess

            self.assertTrue(True, "엔티티 생성 프로세스가 성공적으로 완료되었습니다.")
        except Exception:
            self.fail(f"엔티티 생성 테스트 중 예외 발생")

if __name__ == '__main__':
    unittest.main()
