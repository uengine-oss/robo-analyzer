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
            test_case = {
                'id': 1,
                'procedure': {
                    'procedure_name': 'TPX_UPDATE_SALARY',
                    'object_name': 'TPX_UPDATE_SALARY',
                    'variables': {
                        'pEmpKey': {'type': 'VARCHAR2', 'value': 'EMP001'},
                        'pEmpName': {'type': 'VARCHAR2', 'value': '이정규'},
                        'pDeptCode': {'type': 'VARCHAR2', 'value': 'DEV001'},
                        'pBaseAmount': {'type': 'NUMBER', 'value': '1000000'},
                        'pPayDate': {'type': 'DATE', 'value': '2024-12-13'},
                        'pRegularYn': {'type': 'CHAR', 'value': 'Y'}
                    }
                },
                'tableFields': {
                    'TPJ_EMPLOYEE': {
                        'DEPT_CODE': {'type': 'VARCHAR2(10)', 'value': 'DEV001'},
                        'EMP_KEY': {'type': 'VARCHAR2(10)', 'value': 'EMP001'},
                        'EMP_NAME': {'type': 'VARCHAR2(50)', 'value': '이정규'},
                        'REGULAR_YN': {'type': 'CHAR(1)', 'value': 'Y'}
                    },
                    'TPJ_SALARY': {
                        'EMP_KEY': {'type': 'VARCHAR2(10)', 'value': 'EMP001'},
                        'AMOUNT': {'type': 'NUMBER(10)', 'value': '1000000'},
                        'PAY_DATE': {'type': 'DATE', 'value': '2024-12-13'},
                        'SAL_KEY': {'type': 'VARCHAR2(50)', 'value': 'SAL001'}
                    },
                    'TPJ_ATTENDANCE': {
                        'EMP_KEY': {'type': 'VARCHAR2(10)', 'value': 'EMP001'},
                        'ATT_KEY': {'type': 'VARCHAR2(50)', 'value': 'ATT001'},
                        'STATUS': {'type': 'VARCHAR2(2)', 'value': 'AB'},
                        'WORK_DATE': {'type': 'DATE', 'value': '2024-12-03'}
                    }
                }
            }

            test_cases = [test_case]
            
            # 단순히 결과 확인
            async for result in process_comparison_result(test_cases):
                print(f"결과: {result}")
                
        except Exception as e:
            self.fail(f"예상치 못한 예외 발생: {str(e)}")

if __name__ == '__main__':
    unittest.main()