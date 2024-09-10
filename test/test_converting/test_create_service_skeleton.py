import json
import unittest
import sys
import os
import logging
import unittest
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..', '..')))
from convert.create_service_skeleton import start_service_skeleton_processing


# * 로그 레벨을 INFO로 설정
logging.basicConfig(level=logging.INFO) 
logging.getLogger('asyncio').setLevel(logging.ERROR)


# 스프링부트 기반의 자바 서비스 틀을 생성하는 테스트
class TestSkeletonGeneration(unittest.IsolatedAsyncioTestCase):
    async def test_create_Skeleton(self):

        # * 테스트할 스토어드 프로시저 파일 이름을 설정 및 수정합니다. 
        sp_file_name = "P_B_CAC120_CALC_SUIP_STD"
        lower_file_name = sp_file_name.replace('_', '').lower()


        try:
            # * 파일이 존재하면 기존 데이터를 읽고, 없다면 새로 생성합니다.
            result_file_path = os.path.join('test', 'test_converting', 'test_results.json')
            if os.path.exists(result_file_path):
                with open(result_file_path, 'r', encoding='utf-8') as f:
                    test_data = json.load(f)
            else:
                test_data = {}              


            # * 결과 파일에서 테스트에 필요한 데이터를 가지고 옵니다.
            repository_interface_names = test_data.get('repository_interface_names', [])
            entity_name_list = test_data.get('entity_name_list', [])


            # * Service Skeleton 생성 테스트 시작
            (service_skeleton, command_class_variable, service_skeleton_name, summarzied_service_skeleton) = await start_service_skeleton_processing(
                lower_file_name, entity_name_list, repository_interface_names)
            
            
            # * 결과를 결과 파일에 저장합니다.
            test_data["service_skeleton"] = service_skeleton
            test_data["command_class_variable"] = command_class_variable
            test_data["service_skeleton_name"] = service_skeleton_name
            test_data["summarzied_service_skeleton"] = summarzied_service_skeleton
            with open(result_file_path, 'w', encoding='utf-8') as f:
                json.dump(test_data, f, ensure_ascii=False, indent=2)

            self.assertTrue(True, "Service Skeleton 프로세스가 성공적으로 완료되었습니다.")
        except Exception:
            self.fail(f"Service Skeleton 생성 테스트 중 예외 발생")

if __name__ == '__main__':
    unittest.main()
