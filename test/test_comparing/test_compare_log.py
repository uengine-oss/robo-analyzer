import unittest
import sys
import os
import logging
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..', '..')))
from compare.extract_log_info import compare_log_files

# 로그 레벨을 INFO로 설정
logging.basicConfig(level=logging.INFO, format='%(levelname)s: %(message)s')

class TestCompareLog(unittest.IsolatedAsyncioTestCase):
    async def test_compare_logs(self):
        try:
            # 로그 비교
            compare_result = await compare_log_files(1)
            self.assertIsNotNone(compare_result, "로그 비교가 실패했습니다")
            logging.info("로그 비교 완료")
            
        except Exception as e:
            self.fail(f"로그 비교 중 예외 발생: {str(e)}")

if __name__ == '__main__':
    unittest.main()