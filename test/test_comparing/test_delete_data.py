import unittest
import sys
import os
import logging

sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..', '..')))
from compare.execute_plsql_sql import execute_sql

# 로그 레벨을 INFO로 설정
logging.basicConfig(level=logging.INFO, format='%(levelname)s: %(message)s')

class TestDeleteTableData(unittest.IsolatedAsyncioTestCase):
    
    async def test_delete_table_data(self):
        try:
            # 테스트용 테이블 이름 리스트
            table_names = ["TPJ_EMPLOYEE", "TPJ_SALARY", "TPJ_ATTENDANCE"]
            
            # DELETE 문 생성
            delete_statements = [f"DELETE FROM {table}" for table in table_names]
            
            logging.info("테이블 데이터 삭제 시작")
            
            # DELETE 문 실행 전 로깅
            for stmt in delete_statements:
                logging.info(f"실행할 SQL: {stmt}")
            
            # execute_sql 함수 실행
            result = await execute_sql(delete_statements)
            
            # 결과 확인
            self.assertTrue(result, "테이블 데이터 삭제 실패")
            logging.info("테이블 데이터 삭제 완료")
                
        except Exception as e:
            logging.error(f"상세 오류 내용: {str(e)}")
            logging.error(f"오류 타입: {type(e)}")
            self.fail(f"테이블 데이터 삭제 중 예외 발생: {str(e)}")

if __name__ == '__main__':
    unittest.main()