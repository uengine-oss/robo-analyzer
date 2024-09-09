import unittest
import sys
import os
import logging
import unittest
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))
from service.service import generate_and_execute_cypher

# * 로그 레벨을 INFO로 설정
logging.basicConfig(level=logging.INFO) 
logging.getLogger('asyncio').setLevel(logging.ERROR)


# cypher query를 생성 및 실행하여, neo4j에서 생성된 결과를 확인 할 수 있는 테스트
class TestCypherQueryGeneration(unittest.IsolatedAsyncioTestCase):
    async def test_generate_and_execute_cypher(self):
        
        # * 테스트할 파일 이름과 마지막 라인 번호 설정
        test_filename = "P_B_CAC120_CALC_SUIP_STD"
        last_line = 2008

        # * 검증 로직 없이 함수 실행만 확인
        async for _ in generate_and_execute_cypher(test_filename, last_line):
            pass  

if __name__ == '__main__':
    unittest.main()
