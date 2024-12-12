import unittest
import logging
import sys
import os
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..', '..')))
from service.service import process_comparison_result

# asyncio 경고 메시지 숨기기
logging.getLogger('asyncio').setLevel(logging.ERROR)
# 로그 설정
logging.basicConfig(level=logging.INFO)

class TestProcessComparisonResult(unittest.IsolatedAsyncioTestCase):
    async def test_process_comparison_result_success(self):
        """process_comparison_result 메서드가 정상적으로 실행되는지 테스트"""
        try:
            await process_comparison_result("TPX_MAIN.sql")
            # 예외가 발생하지 않으면 테스트 성공
        except Exception as e:
            self.fail(f"예상치 못한 예외 발생: {str(e)}")

if __name__ == '__main__':
    unittest.main()