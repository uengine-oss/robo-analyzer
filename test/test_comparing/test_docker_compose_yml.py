import unittest
import sys
import os
import logging

sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..', '..')))
from compare.create_docker_compose_yml import process_docker_compose_yml
from compare.create_init_sql import generate_init_sql


# 로그 레벨을 INFO로 설정
logging.basicConfig(level=logging.INFO, format='%(levelname)s: %(message)s')

class TestDockerComposeYml(unittest.IsolatedAsyncioTestCase):
    async def test_generate_docker_compose_yml(self):
        try:
            # 테스트용 테이블 이름 리스트 및 패키지 이름 리스트
            table_names = ["TPJ_EMPLOYEE", "TPJ_SALARY", "TPJ_ATTENDANCE"]
            package_names = ["TPX_EMPLOYEE", "TPX_SALARY", "TPX_ATTENDANCE", "TPX_MAIN"]

            # ! 패키지간의 의존성 확인이 필요합니다.
            # result = await generate_init_sql(table_names, package_names)
            # self.assertTrue(result)
            
            result = await process_docker_compose_yml(table_names)
            self.assertTrue(result)
            
        except Exception as e:
            self.fail(f"docker-compose.yml 파일 생성 중 예외 발생: {str(e)}")

if __name__ == '__main__':
    unittest.main()