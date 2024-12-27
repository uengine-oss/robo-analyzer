import json
import unittest
import sys
import os
import logging
import unittest

from util.file_utils import read_sequence_file
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..', '..')))
from convert.create_repository import start_repository_processing

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

    
# 스프링부트 기반의 자바 Repository Interface를 생성하는 테스트
class TestRepositoryGeneration(unittest.IsolatedAsyncioTestCase):
    async def test_create_repository(self):
        # * 테스트할 객체 이름들을 설정
        object_names = [
            # "TPX_MAIN",
            # "TPX_EMPLOYEE",
            # "TPX_SALARY",
            # "TPX_ATTENDANCE",
            "TPX_PROJECT",
            "TPX_TMF_SYNC_JOB_STATUS",
            "TPX_TMF_SYNC_JOB",
            # "TPX_ALARM",
            # "TPX_ALARM_CONTENT",
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
            
            # * Repository Interface 생성 테스트 시작
            used_methods_dict = {} 
            global_variables = {}
            all_methods_dict = {}
            orm_type = "jpa"  # ? 원하는 모델로 수정

            for object_name in object_names:
                seq_data = await read_sequence_file(object_name)
                used_methods, global_variable_nodes, all_methods = await start_repository_processing(object_name, seq_data, orm_type)
                
                used_methods_dict[object_name] = used_methods
                global_variables[object_name] = global_variable_nodes
                all_methods_dict[object_name] = all_methods
            
            test_data.update({
                "repository_methods": used_methods_dict,
                "global_variables": global_variables,
                "all_query_methods": all_methods_dict,
            })
            
            # * 결과를 결과 파일에 저장합니다.
            with open(result_file_path, 'w', encoding='utf-8') as f:
                json.dump(test_data, f, ensure_ascii=False, indent=2)

            self.assertTrue(True, "Repository Interface 프로세스가 성공적으로 완료되었습니다.")
        except Exception:
            self.fail(f"Repository Interface 생성 테스트 중 예외 발생")


if __name__ == '__main__':
    unittest.main()