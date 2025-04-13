import unittest
import sys
import os
import logging
import asyncio

sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..', '..')))
from internal.service import generate_spring_boot_project

# 로그 레벨을 INFO로 설정
logging.basicConfig(level=logging.INFO, format='%(levelname)s: %(message)s')
logging.getLogger('asyncio').setLevel(logging.ERROR)


class TestGenerateSpringBootProject(unittest.IsolatedAsyncioTestCase):
    async def test_generate_spring_boot_project(self):
        
        # * 테스트할 파일 이름 설정
        file_names = [
            ("TPX_UPDATE_SALARY.sql", "TPX_UPDATE_SALARY"),
            ("TPX_EMPLOYEE.sql", "TPX_EMPLOYEE"),
            ("TPX_SALARY.sql", "TPX_SALARY"),
            ("TPX_ATTENDANCE.sql", "TPX_ATTENDANCE"),
            # ("TPX_TMF_SYNC_JOB_STATUS.sql", "TPX_TMF_SYNC_JOB_STATUS"),
            # ("TPX_ALARM.sql", "TPX_ALARM"),
            # ("TPX_ALARM_CONTENT.sql", "TPX_ALARM_CONTENT"),
            # ("TPX_TMF_SYNC_JOB.sql", "TPX_TMF_SYNC_JOB"),
            # ("TPX_ALARM_FILE.sql", "TPX_ALARM_FILE"),
            # ("TPX_ALARM_RECIPIENT.sql", "TPX_ALARM_RECIPIENT"),
        ]

        orm_type = "jpa"
        user_id = "3c667f5b-6bde-4c1f-b3e9-bfb0a5396d52"

        try:
            # * generate_spring_boot_project 메서드 호출 및 결과 확인
            async for step_result in generate_spring_boot_project(file_names, orm_type, user_id):
                
                # * 각 단계의 결과를 로깅
                logging.info(f"Step result: {step_result}")
                if step_result == "convert-error":
                    raise Exception("변환 중 오류 발생")
        except Exception as e:
            self.fail(f"Spring Boot 프로젝트 생성 중 예외 발생: {str(e)}")

if __name__ == '__main__':
    unittest.main()