import unittest
import sys
import os
import logging

logging.basicConfig(level=logging.INFO)  # 로그 레벨을 INFO로 설정
logging.getLogger('asyncio').setLevel(logging.ERROR)

sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))
from service.service import generate_and_execute_cypher

import unittest

# 사이퍼쿼리를 생성하여, 노드와 관계를 생성하는 테스트 모듈(UnderStanding)
class AsyncTestCase(unittest.IsolatedAsyncioTestCase):
    async def test_generate_and_execute_cypher_simple_execution(self):
        
        # * 테스트할 파일 이름과 마지막 라인 번호 설정
        test_filename = "testjava"
        last_line = 67

        # * 검증 로직 없이 함수 실행만 확인
        async for _ in generate_and_execute_cypher(test_filename, last_line):
            pass  

if __name__ == '__main__':
    unittest.main()
