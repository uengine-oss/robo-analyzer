import json
import unittest
import sys
import os
import logging
import unittest
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..', '..')))
from convert.create_service_preprocessing import start_service_preprocessing


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


# 스프링부트 기반의 자바 서비스(전처리) 생성하는 테스트
class TestPreServiceGeneration(unittest.IsolatedAsyncioTestCase):
    async def test_create_preService(self):
        
        # * 테스트할 객체 이름들을 설정
        object_names = [
            "TPX_LIBRARY_OPERATION",
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
        api_key = "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"




        try:
            # * 파일이 존재하면 기존 데이터를 읽고, 없다면 새로 생성합니다.
            result_file_path = os.path.join('test', 'test_converting', 'test_results.json')
            if os.path.exists(result_file_path):
                with open(result_file_path, 'r', encoding='utf-8') as f:
                    test_data = json.load(f)
            else:
                test_data = {}              

            # * 전처리 결과를 저장할 사전
            preprocessing_results = {}

            # * Service 전처리 테스트 시작
            for object_name in object_names:

                # * 결과 파일에서 해당 객체의 데이터를 가져옵니다
                service_skeleton_list = test_data.get('service_skeleton_list', {}).get(object_name, [])
                repository_methods = test_data.get('repository_methods', {}).get(object_name, [])
                sequence_methods = test_data.get('sequence_methods', {}).get(object_name, [])

                # * 각 스켈레톤 데이터에 대해 전처리 수행
                procedure_results = {}
                for skeleton_data in service_skeleton_list:
                    procedure_name = skeleton_data['procedure_name']
                    variable_nodes = await start_service_preprocessing(
                        skeleton_data['service_method_skeleton'],
                        skeleton_data['command_class_variable'],
                        procedure_name,
                        repository_methods,
                        object_name,
                        sequence_methods,
                        session_uuid,
                        api_key
                    )

                    # * 각 프로시저 결과를 저장합니다.
                    procedure_results[procedure_name] = variable_nodes
                
                # * 각 패키지의 전처리 결과를 저장합니다.
                preprocessing_results[object_name] = procedure_results
            
            # * 테스트 결과를 저장합니다.
            test_data['variable_nodes'] = preprocessing_results
            with open(result_file_path, 'w', encoding='utf-8') as f:
                json.dump(test_data, f, ensure_ascii=False, indent=2)

            self.assertTrue(True, "전처리 Service 프로세스가 성공적으로 완료되었습니다.")
        except Exception:
            self.fail(f"Service 전처리 테스트 중 예외 발생")

if __name__ == '__main__':
    unittest.main()
