import json
import unittest
import sys
import os
import logging
import unittest
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..', '..')))
from convert.create_service_skeleton import start_service_skeleton_processing


# * 로그 레벨 설정
logging.basicConfig(
    level=logging.INFO,
    format='%(levelname)s: %(message)s',
    force=True
)
logging.getLogger('asyncio').setLevel(logging.ERROR)
noisy_loggers = [
    'asyncio', 
    'anthropic', 
    'langchain', 
    'urllib3',
    'anthropic._base_client', 
    'anthropic._client',
    'langchain_core', 
    'langchain_anthropic',
    'uvicorn',
    'fastapi'
]

for logger_name in noisy_loggers:
    logging.getLogger(logger_name).setLevel(logging.CRITICAL)


# 스프링부트 기반의 자바 서비스 틀을 생성하는 테스트
class TestSkeletonGeneration(unittest.IsolatedAsyncioTestCase):
    async def test_create_Skeleton(self):
        
        # * 테스트할 객체 이름들을 설정
        object_names = [
            "TPX_PROJECT",
            "TPX_TMF_SYNC_JOB_STATUS",
            # "TPX_ALARM",
            # "TPX_ALARM_CONTENT",
            # "TPX_TMF_SYNC_JOB",
            # "TPX_ALARM_FILE",
            # "TPX_ALARM_RECIPIENT"
        ]

        try:
            # * 파일이 존재하면 기존 데이터를 읽고, 없다면 새로 생성합니다.
            result_file_path = os.path.join('test', 'test_converting', 'test_results.json')
            if os.path.exists(result_file_path):
                with open(result_file_path, 'r', encoding='utf-8') as f:
                    test_data = json.load(f)
            else:
                test_data = {}              

            # * Service Skeleton 생성 테스트 시작
            skeleton_results = {}
            for object_name in object_names:

                # * 결과 파일에서 해당 객체의 데이터를 가져옵니다
                entity_name_list = test_data.get('entity_name_list', {}).get(object_name, [])
                global_variables = test_data.get('global_variables', {}).get(object_name, [])
                
                # * Service Skeleton 생성
                service_skeleton_list, service_skeleton, service_class_name = await start_service_skeleton_processing(entity_name_list, object_name, global_variables)
                
                # * 객체별 결과 저장
                skeleton_results[object_name] = {
                    "service_skeleton_list": service_skeleton_list,
                    "service_skeleton": service_skeleton,
                    "service_class_name": service_class_name
                }
            
            # * 결과를 결과 파일에 저장합니다.
            test_data.update({
                "service_skeleton_list": {name: results["service_skeleton_list"] for name, results in skeleton_results.items()},
                "service_skeleton": {name: results["service_skeleton"] for name, results in skeleton_results.items()},
                "service_class_name": {name: results["service_class_name"] for name, results in skeleton_results.items()}
            })
            
            with open(result_file_path, 'w', encoding='utf-8') as f:
                json.dump(test_data, f, ensure_ascii=False, indent=2)

            self.assertTrue(True, "Service Skeleton 프로세스가 성공적으로 완료되었습니다.")
        except Exception:
            self.fail(f"Service Skeleton 생성 테스트 중 예외 발생")

if __name__ == '__main__':
    unittest.main()
