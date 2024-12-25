import json
import unittest
import sys
import os
import logging
import unittest
from convert.validate_service_preprocessing import start_validate_service_preprocessing
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..', '..')))
from convert.create_service_preprocessing import start_validate_service_preprocessing


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


# 스프링부트 기반의 자바 서비스 검증(전처리) 테스트
class TestValidateServicePreprocessing(unittest.IsolatedAsyncioTestCase):
    async def test_validate_service_preprocessing(self):
        
        object_names = [
            "TPX_PROJECT",
            "TPX_TMF_SYNC_JOB_STATUS",
            "TPX_TMF_SYNC_JOB",
        ]

        try:
            # * 결과 파일 경로 설정
            result_file_path = os.path.join('test', 'test_converting', 'test_results.json')
            if os.path.exists(result_file_path):
                with open(result_file_path, 'r', encoding='utf-8') as f:
                    test_data = json.load(f)
            else:
                test_data = {}

            # * 검증 프로세스 시작
            for object_name in object_names:

                # * 필요한 데이터 가져오기
                service_skeleton_list = test_data.get('service_skeleton_list', {}).get(object_name, [])
                jpa_method_list = test_data.get('jpa_method_list', {}).get(object_name, [])
                variable_nodes = test_data.get('variable_nodes', {}).get(object_name, {})

                # * 각 스켈레톤 데이터에 대해 검증 수행
                for skeleton_data in service_skeleton_list:
                    procedure_name = skeleton_data['procedure_name']
                    procedure_variables = variable_nodes.get(procedure_name, [])

                    # * 검증 프로세스 실행
                    await start_validate_service_preprocessing(
                        procedure_variables,
                        skeleton_data['service_method_skeleton'],
                        skeleton_data['command_class_variable'],
                        procedure_name,
                        jpa_method_list,
                        object_name
                    )

            self.assertTrue(True, "서비스 검증 프로세스가 성공적으로 완료되었습니다.")
        
        except Exception as e:
            self.fail(f"서비스 검증 테스트 중 예외 발생: {str(e)}")

if __name__ == '__main__':
    unittest.main()