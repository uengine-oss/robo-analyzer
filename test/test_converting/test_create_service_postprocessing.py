import json
import unittest
import sys
import os
import logging
import unittest
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..', '..')))
from convert.create_service_postprocessing import generate_service_class, start_service_postprocessing


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


# 스프링부트 기반의 자바 서비스(후처리)를 생성하는 테스트
class TestPostServiceGeneration(unittest.IsolatedAsyncioTestCase):
    async def test_create_postService(self):
        
        # * 테스트할 스토어드 프로시저 파일 이름을 설정 및 수정합니다. 
        object_names = [
            # "TPX_BOOK",
            # "TPX_LIBRARY_OPERATION",
            # "TPX_UPDATE_SALARY",
            # "TPX_EMPLOYEE",
            # "TPX_SALARY",
            # "TPX_ATTENDANCE",
            # "TPX_PROJECT",
            # "TPX_TMF_SYNC_JOB_STATUS",
            # "TPX_TMF_SYNC_JOB",
            # "TPX_ALARM",
            # "TPX_ALARM_CONTENT",
            # "TPX_ALARM_FILE",
            # "TPX_ALARM_RECIPIENT"
        ]


        session_uuid = "TestSession"




        try:
            # * 파일이 존재하면 기존 데이터를 읽고, 없다면 새로 생성합니다.
            result_file_path = os.path.join('test', 'test_converting', 'test_results.json')
            if os.path.exists(result_file_path):
                with open(result_file_path, 'r', encoding='utf-8') as f:
                    test_data = json.load(f)
            else:
                test_data = {}   


            # * Service 후처리 테스트 시작
            for object_name in object_names:
                    
                    # * 결과 파일에서 해당 객체의 데이터를 가져옵니다
                    service_skeleton_list = test_data.get('service_skeleton_list', {}).get(object_name, [])
                    service_skeleton = test_data.get('service_skeleton', {}).get(object_name, '')
                    service_class_name = test_data.get('service_class_name', {}).get(object_name, '')
                    merge_method_code = ""

                    # * 각 스켈레톤 데이터에 대해 후처리 수행
                    for skeleton_data in service_skeleton_list:
                        merge_method_code = await start_service_postprocessing(
                            skeleton_data['method_skeleton_code'],
                            skeleton_data['procedure_name'],
                            object_name,
                            merge_method_code,
                            session_uuid,
                        )

                    await generate_service_class(service_skeleton, service_class_name, merge_method_code, session_uuid)            
            
            self.assertTrue(True, "후처리 Service 프로세스가 성공적으로 완료되었습니다.")
        except Exception:
            self.fail(f"Service 후처리 테스트 중 예외 발생")

if __name__ == '__main__':
    unittest.main()
