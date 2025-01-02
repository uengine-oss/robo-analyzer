import unittest
import oracledb

class TestOracleConnection(unittest.TestCase):
    
    def test_oracle_client_initialization(self):
        try:
            oracledb.init_oracle_client()
            print("Oracle Client 연결 성공!")
            
        except Exception as e:
            self.fail(f"Oracle Client 연결 실패: {str(e)}")

if __name__ == '__main__':
    unittest.main()