import unittest
import sys
import os
import logging

sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..', '..')))
from compare.create_init_sql import get_package_dependencies

logging.basicConfig(level=logging.INFO, format='%(levelname)s: %(message)s')

class TestPackageDependencies(unittest.IsolatedAsyncioTestCase):
    
    async def test_package_dependencies(self):
        try:
            result = await get_package_dependencies()
            print(result)
            self.assertTrue(result, "패키지 의존성 분석 성공")
            logging.info("패키지 의존성 분석 완료")
                
        except Exception as e:
            logging.error(f"패키지 의존성 분석 실패: {str(e)}")
            self.fail(f"패키지 의존성 분석 중 예외 발생: {str(e)}")

if __name__ == '__main__':
    unittest.main()