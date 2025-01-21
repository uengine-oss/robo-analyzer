import json
import unittest
import sys
import os
import logging

sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..', '..')))
from util.file_utils import read_sequence_file
from convert.create_support_files import start_mybatis_mapper_processing

# * 로그 레벨 설정
logging.basicConfig(
    level=logging.INFO,
    format='%(levelname)s: %(message)s',
    force=True
)

# * 불필요한 로그 제거
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

# MyBatis Mapper XML 파일 생성을 테스트하는 클래스
class TestMyBatisMapperGeneration(unittest.IsolatedAsyncioTestCase):
    async def test_create_mybatis_mapper(self):
        # * 테스트할 객체 이름들을 설정
        object_names = [
            "TPX_PROJECT",
            "TPX_TMF_SYNC_JOB_STATUS",
            "TPX_TMF_SYNC_JOB"
        ]


        # * 테스트할 세션 및 orm 타입 설정
        session_uuid = "525f343f-006e-455d-9e52-9825170c2088"


        try:
            # * 테스트 결과 파일 경로 설정
            result_file_path = os.path.join('test', 'test_converting', 'test_results.json')
            
            # * 파일이 존재하면 기존 데이터를 읽고, 없다면 새로 생성
            if os.path.exists(result_file_path):
                with open(result_file_path, 'r', encoding='utf-8') as f:
                    test_data = json.load(f)
            else:
                test_data = {}

            # * MyBatis Mapper 생성 테스트 시작
            for object_name in object_names:
                
                # * 결과 파일에서 필요한 데이터 추출
                entity_infos = test_data.get('entity_codes', {})
                all_query_methods = test_data.get('all_query_methods', {})

                # * MyBatis Mapper 생성
                await start_mybatis_mapper_processing(
                    entity_infos=entity_infos[object_name],
                    all_query_methods=all_query_methods[object_name],
                    session_uuid=session_uuid,
                )

            self.assertTrue(True, "MyBatis Mapper 생성 프로세스가 성공적으로 완료되었습니다.")
        except Exception as e:
            self.fail(f"MyBatis Mapper 생성 테스트 중 예외 발생: {str(e)}")

if __name__ == '__main__':
    unittest.main()