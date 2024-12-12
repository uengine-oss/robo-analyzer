import unittest
import sys
import os
import logging

sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..', '..')))
from service.service import process_comparison_result

# 로그 레벨을 INFO로 설정
logging.basicConfig(level=logging.INFO, format='%(levelname)s: %(message)s')
logging.getLogger('asyncio').setLevel(logging.ERROR)

# PLSQL 결과 로그와 자바 실행 로그와 비교하는 프로세스를 테스트
class TestGenerateJunitTest(unittest.IsolatedAsyncioTestCase):
    async def test_process_comparison_result(self):
        try:
            # * process_comparison_result 메서드 호출
            main_file_name = "TPX_MAIN.sql"  # 테스트할 파일 이름
            await process_comparison_result(main_file_name)
            logging.info("Junit 테스트 코드 생성 완료")
            
        except Exception as e:
            self.fail(f"Junit 테스트 코드 생성 중 예외 발생: {str(e)}")

if __name__ == '__main__':
    unittest.main()