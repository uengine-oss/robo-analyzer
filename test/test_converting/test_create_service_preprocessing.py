import json
import unittest
import sys
import os
import logging
import unittest
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..', '..')))
from convert.create_service_preprocessing import start_service_Preprocessing


# * 로그 레벨을 INFO로 설정
logging.basicConfig(level=logging.INFO) 
logging.getLogger('asyncio').setLevel(logging.ERROR)


# 스프링부트 기반의 자바 서비스(전처리) 생성하는 테스트
class TestPreServiceGeneration(unittest.IsolatedAsyncioTestCase):
    async def test_create_preService(self):

        try:
            # * 파일이 존재하면 기존 데이터를 읽고, 없다면 새로 생성합니다.
            result_file_path = os.path.join('test', 'test_converting', 'test_results.json')
            if os.path.exists(result_file_path):
                with open(result_file_path, 'r', encoding='utf-8') as f:
                    test_data = json.load(f)
            else:
                test_data = {}              

                
            # * 결과 파일에서 테스트에 필요한 데이터를 가지고 옵니다.
            summarzied_service_skeleton = test_data.get('summarzied_service_skeleton', [])
            jpa_method_list = test_data.get('jpa_method_list', [])
            command_class_variable = test_data.get('command_class_variable', [])


            # * Service 전처리 테스트 시작
            await start_service_Preprocessing(summarzied_service_skeleton, jpa_method_list, command_class_variable)
            
            self.assertTrue(True, "전처리 Service 프로세스가 성공적으로 완료되었습니다.")
        except Exception:
            self.fail(f"Service 전처리 테스트 중 예외 발생")

if __name__ == '__main__':
    unittest.main()
