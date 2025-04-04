import unittest
import sys
import os
import logging
import unittest
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..', '..')))
from convert.create_pomxml import start_pomxml_processing


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
session_uuid = "e37f4668-8d1e-4650-bc95-2328a76cf594"

# 스프링부트 기반의 자바 pom.xml를 생성하는 테스트
class TestPomGeneration(unittest.IsolatedAsyncioTestCase):
    async def test_create_pomxml(self):

        try:
            # * pom.xml 생성 테스트 시작
            await start_pomxml_processing(session_uuid)
            self.assertTrue(True, "pom.xml 프로세스가 성공적으로 완료되었습니다.")
        except Exception:
            self.fail(f"pom.xml 생성 테스트 중 예외 발생")

if __name__ == '__main__':
    unittest.main()
