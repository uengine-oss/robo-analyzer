import json
import unittest
import sys
import os
import logging
import unittest
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..', '..')))
from convert.create_entity import start_entity_processing


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



# 스프링부트 기반의 자바 엔티티 클래스를 생성하는 테스트
class TestEntityGeneration(unittest.IsolatedAsyncioTestCase):
    async def test_create_entity(self):

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
            # * 파일이 존재하면 기존 데이터를 읽고, 없다면 새로운 딕셔너리 생성
            result_file_path = os.path.join('test', 'test_converting', 'test_results.json')
            if os.path.exists(result_file_path):
                with open(result_file_path, 'r', encoding='utf-8') as f:
                    test_data = json.load(f)
            else:
                test_data = {}


            # * 엔티티 클래스 생성 테스트 시작
            entity_results = {}
            for object_name in object_names:
                entity_names = await start_entity_processing(object_name)
                entity_results[object_name] = entity_names


            # * 결과를 외부 파일에 저장
            test_data["entity_name_list"] = entity_results
            with open(result_file_path, 'w', encoding='utf-8') as f:
                json.dump(test_data, f, ensure_ascii=False, indent=2)
                
            self.assertTrue(True, "모든 엔티티 생성 프로세스가 성공적으로 완료되었습니다.")
        except Exception:
            self.fail(f"엔티티 생성 테스트 중 예외 발생")


if __name__ == '__main__':
    unittest.main()
