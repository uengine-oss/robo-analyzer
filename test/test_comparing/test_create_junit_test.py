import unittest
import sys
import os
import logging

sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..', '..')))

# 로그 레벨을 INFO로 설정
logging.basicConfig(level=logging.INFO, format='%(levelname)s: %(message)s')
logging.getLogger('asyncio').setLevel(logging.ERROR)

# * Junit 테스트 코드 생성 테스트
class TestGenerateJunitTest(unittest.IsolatedAsyncioTestCase):
    async def test_process_comparison_result(self):
        try:
            # TODO 작업 필요
            pass
            
        except Exception as e:
            self.fail(f"Junit 테스트 코드 생성 중 예외 발생: {str(e)}")

if __name__ == '__main__':
    unittest.main()