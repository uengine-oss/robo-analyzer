import unittest
import sys
import os
import logging
from datetime import date

sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..', '..')))
from compare.execute_plsql_sql import execute_plsql, execute_sql
from compare.extract_log_info import extract_given_log, extract_then_log, generate_given_when_then

# 로그 레벨을 INFO로 설정
logging.basicConfig(level=logging.INFO, format='%(levelname)s: %(message)s')

class TestPLSQLExecution(unittest.IsolatedAsyncioTestCase):
    async def test_execute_plsql_and_sql(self):
        try:
            # 테스트 케이스 정의
            test_cases = [
                # Case 1: 이정규 (정규직, 결근 4번)
                {
                    "params": {
                        "pEmpKey": "EMP001",
                        "pEmpName": "이정규",
                        "pDeptCode": "DEV001",
                        "pBaseAmount": 100000,
                        "pPayDate": date(2024, 3, 1),
                        "pRegularYn": "Y"
                    },
                    "sql": [
                        "INSERT INTO TPJ_EMPLOYEE (EMP_KEY, EMP_NAME, DEPT_CODE, REGULAR_YN) VALUES ('EMP001', '이정규', 'DEV001', 'Y')",
                        "INSERT INTO TPJ_SALARY (SAL_KEY, EMP_KEY, PAY_DATE, AMOUNT) VALUES ('SAL001', 'EMP001', TO_DATE('2024-03-01', 'YYYY-MM-DD'), 100000)",
                        "INSERT INTO TPJ_ATTENDANCE (ATT_KEY, EMP_KEY, WORK_DATE, STATUS) VALUES ('ATT001', 'EMP001', TO_DATE('2024-03-01', 'YYYY-MM-DD'), 'AB')",
                        "INSERT INTO TPJ_ATTENDANCE (ATT_KEY, EMP_KEY, WORK_DATE, STATUS) VALUES ('ATT002', 'EMP001', TO_DATE('2024-03-02', 'YYYY-MM-DD'), 'AB')",
                        "INSERT INTO TPJ_ATTENDANCE (ATT_KEY, EMP_KEY, WORK_DATE, STATUS) VALUES ('ATT003', 'EMP001', TO_DATE('2024-03-03', 'YYYY-MM-DD'), 'AB')",
                        "INSERT INTO TPJ_ATTENDANCE (ATT_KEY, EMP_KEY, WORK_DATE, STATUS) VALUES ('ATT004', 'EMP001', TO_DATE('2024-03-04', 'YYYY-MM-DD'), 'AB')"
                    ]
                },
                # Case 2: 김정규 (정규직, 결근 없음)
                {
                    "params": {
                        "pEmpKey": "EMP002",
                        "pEmpName": "김정규",
                        "pDeptCode": "DEV002",
                        "pBaseAmount": 200000,
                        "pPayDate": date(2024, 3, 1),
                        "pRegularYn": "Y"
                    },
                    "sql": [
                        "INSERT INTO TPJ_EMPLOYEE (EMP_KEY, EMP_NAME, DEPT_CODE, REGULAR_YN) VALUES ('EMP002', '김정규', 'DEV002', 'Y')",
                        "INSERT INTO TPJ_SALARY (SAL_KEY, EMP_KEY, PAY_DATE, AMOUNT) VALUES ('SAL002', 'EMP002', TO_DATE('2024-03-01', 'YYYY-MM-DD'), 200000)",
                        "INSERT INTO TPJ_ATTENDANCE (ATT_KEY, EMP_KEY, WORK_DATE, STATUS) VALUES ('ATT005', 'EMP002', TO_DATE('2024-03-01', 'YYYY-MM-DD'), 'NM')"
                    ]
                },
                # Case 3: 박계약 (비정규직, 결근 2번)
                {
                    "params": {
                        "pEmpKey": "EMP003",
                        "pEmpName": "박계약",
                        "pDeptCode": "DEV001",
                        "pBaseAmount": 150000,
                        "pPayDate": date(2024, 3, 1),
                        "pRegularYn": "N"
                    },
                    "sql": [
                        "INSERT INTO TPJ_EMPLOYEE (EMP_KEY, EMP_NAME, DEPT_CODE, REGULAR_YN) VALUES ('EMP003', '박계약', 'DEV001', 'N')",
                        "INSERT INTO TPJ_SALARY (SAL_KEY, EMP_KEY, PAY_DATE, AMOUNT) VALUES ('SAL003', 'EMP003', TO_DATE('2024-03-01', 'YYYY-MM-DD'), 150000)",
                        "INSERT INTO TPJ_ATTENDANCE (ATT_KEY, EMP_KEY, WORK_DATE, STATUS) VALUES ('ATT006', 'EMP003', TO_DATE('2024-03-01', 'YYYY-MM-DD'), 'AB')",
                        "INSERT INTO TPJ_ATTENDANCE (ATT_KEY, EMP_KEY, WORK_DATE, STATUS) VALUES ('ATT007', 'EMP003', TO_DATE('2024-03-02', 'YYYY-MM-DD'), 'AB')"
                    ]
                }
            ]

            # 테이블 데이터 초기화를 위한 DELETE 문
            clear_statements = [
                "DELETE FROM TPJ_ATTENDANCE",
                "DELETE FROM TPJ_SALARY", 
                "DELETE FROM TPJ_EMPLOYEE"
            ]

            # * 각 테스트 케이스 실행
            for i, test_case in enumerate(test_cases, 1):
                logging.info(f"테스트 케이스 {i} 실행 중...")
                
                # * Given 로그 추출
                given_result = await extract_given_log(i)
                self.assertIsInstance(given_result, dict)
                logging.info("Given 로그 추출 완료")


                # * THEN 로그 추출
                then_result = await extract_then_log(i)
                self.assertIsInstance(then_result, dict)
                logging.info("Then 로그 추출 완료")

                # * Given, When, Then 로그 생성
                gwt_result = await generate_given_when_then(i, 'TPX_UPDATE_SALARY', test_case["params"])
                self.assertIsInstance(gwt_result, dict)
                logging.info("Given, When, Then 로그 생성 완료")


            logging.info("모든 테스트 케이스 실행 완료")

            # * 테이블 데이터 삭제
            # logging.info("테스트 데이터 초기화 중...")
            # clear_result = await execute_sql(clear_statements)
            # self.assertTrue(clear_result, "테이블 데이터 초기화 실패")
            # logging.info("테이블 데이터 초기화 완료")

        except Exception as e:
            self.fail(f"테스트 실행 중 예외 발생: {str(e)}")

if __name__ == '__main__':
    unittest.main()