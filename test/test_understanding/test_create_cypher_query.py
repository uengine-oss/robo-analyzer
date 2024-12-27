import unittest
import sys
import os
import logging
import unittest
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..', '..')))
from service.service import generate_and_execute_cypherQuery

# * 로그 레벨을 INFO로 설정
logging.basicConfig(level=logging.INFO) 
logging.getLogger('asyncio').setLevel(logging.ERROR)


# cypher query를 생성 및 실행하여, neo4j에서 생성된 결과를 확인 할 수 있는 테스트
class TestCypherQueryGeneration(unittest.IsolatedAsyncioTestCase):
    async def test_generate_and_execute_cypher(self):
        
        # * 테스트할 파일 이름과 프로시저 이름을 설정
        file_names = [
            # ("TPX_UPDATE_SALARY.sql", "TPX_UPDATE_SALARY"),
            # ("TPX_EMPLOYEE.sql", "TPX_EMPLOYEE"),
            # ("TPX_SALARY.sql", "TPX_SALARY"),
            # ("TPX_ATTENDANCE.sql", "TPX_ATTENDANCE"),
            ("TPX_PROJECT.sql", "TPX_PROJECT"),
            # ("TPX_TMF_SYNC_JOB_STATUS.sql", "TPX_TMF_SYNC_JOB_STATUS"),
            # ("TPX_TMF_SYNC_JOB.sql", "TPX_TMF_SYNC_JOB"),
            # ("TPX_ALARM.sql", "TPX_ALARM"),
            # ("TPX_ALARM_CONTENT.sql", "TPX_ALARM_CONTENT"),
            # ("TPX_ALARM_FILE.sql", "TPX_ALARM_FILE"),
            # ("TPX_ALARM_RECIPIENT.sql", "TPX_ALARM_RECIPIENT"),
            # ("calculate_payroll.txt", "calculate_payroll"),
        ]

        # * 검증 로직 없이 함수 실행만 확인
        async for _ in generate_and_execute_cypherQuery(file_names):
            pass  

if __name__ == '__main__':
    unittest.main()
