import unittest
import sys
import os
import json
import logging

sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..', '..')))
from compare.create_junit_test import create_junit_test

logging.basicConfig(level=logging.INFO, format='%(levelname)s: %(message)s')
logging.getLogger('asyncio').setLevel(logging.ERROR)

class TestGenerateJunitTest(unittest.IsolatedAsyncioTestCase):
    async def test_create_junit_test(self):
        try:
            # logs 디렉토리는 tests와 같은 레벨에 있음
            procedure_name = "TPX_UPDATE_SALARY"
            table_names = ["TPJ_EMPLOYEE", "TPJ_SALARY", "TPJ_ATTENDANCE"]
            called_procedure_name = "TPX_UPDATE_SALARY"

            logs_dir = os.path.join(os.path.dirname(os.path.dirname(os.path.dirname(__file__))), 'logs')
            for file_name in os.listdir(logs_dir):
                if file_name.startswith('given_when_then_case') and file_name.endswith('.json'):
                    file_path = os.path.join(logs_dir, file_name)
                    with open(file_path, 'r', encoding='utf-8') as f:
                        test_case = json.load(f)
                        logging.info(f"테스트 케이스 실행: {file_name}")
                        await create_junit_test(test_case, table_names, procedure_name, called_procedure_name)
                        
        except Exception as e:
            self.fail(f"Junit 테스트 코드 생성 중 예외 발생: {str(e)}")

if __name__ == '__main__':
    unittest.main()