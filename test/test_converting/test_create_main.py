import unittest
import sys
import os
import logging
import unittest
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..', '..')))
from convert.create_main import start_main_processing_python


# * 로그 레벨 설정
logging.basicConfig(
    level=logging.INFO,
    format='%(levelname)s: %(message)s',
    force=True
)
logging.getLogger('asyncio').setLevel(logging.ERROR)
noisy_loggers = [
    'asyncio', 
    'anthropic', 
    'langchain', 
    'urllib3',
    'anthropic._base_client', 
    'anthropic._client',
    'langchain_core', 
    'langchain_anthropic',
    'uvicorn',
    'fastapi'
]

for logger_name in noisy_loggers:
    logging.getLogger(logger_name).setLevel(logging.CRITICAL)


# * 테스트할 세션 및 orm 타입 설정
session_uuid = "d654a0db-6038-40a8-bea5-5c6a1b183883"
orm_type = "jpa"


# 스프링부트 기반의 자바 메인 클래스를 생성하는 테스트
class TestMainGeneration(unittest.IsolatedAsyncioTestCase):
    async def test_create_main(self):

        try:
            # * Main 클래스 생성 테스트 시작
            # await start_main_processing(orm_type, session_uuid)
            await start_main_processing_python(session_uuid)
            self.assertTrue(True, "Main 클래스 프로세스가 성공적으로 완료되었습니다.")
        except Exception:
            self.fail(f"Main 클래스 생성 테스트 중 예외 발생")

if __name__ == '__main__':
    unittest.main()
