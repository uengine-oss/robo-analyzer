import unittest
import sys
import os
import logging
import unittest
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..', '..')))
from convert.create_properties import start_APLproperties_processing


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
session_uuid = "525f343f-006e-455d-9e52-9825170c2088"
orm_type = "jpa"


# 스프링부트 기반의 자바 application.properties를 생성하는 테스트
class TestAplPropertiesGeneration(unittest.IsolatedAsyncioTestCase):
    async def test_create_aplProperties(self):

        try:
            # * application.properties 생성 테스트 시작
            await start_APLproperties_processing(orm_type, session_uuid)
            self.assertTrue(True, "application.properties 프로세스가 성공적으로 완료되었습니다.")
        except Exception:
            self.fail(f"application.properties 생성 테스트 중 예외 발생")

if __name__ == '__main__':
    unittest.main()
