import json
import unittest
import sys
import os
import logging
import unittest
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..', '..')))
from convert.create_controller import generate_controller_class, start_controller_processing


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


# 스프링부트 기반의 자바 컨트롤러를 생성하는 테스트
class TestControllerGeneration(unittest.IsolatedAsyncioTestCase):
    async def test_create_controller(self):
        
        # * 테스트할 객체 이름들을 설정
        object_names = [
            "TPX_UPDATE_SALARY",
            "TPX_EMPLOYEE",
            "TPX_SALARY",
            "TPX_ATTENDANCE",
            # "TPX_PROJECT",
            # "TPX_TMF_SYNC_JOB_STATUS",
            # "TPX_TMF_SYNC_JOB",
            # "TPX_ALARM",
            # "TPX_ALARM_CONTENT",
            # "TPX_ALARM_FILE",
            # "TPX_ALARM_RECIPIENT"
        ]


        # * 테스트할 세션 및 orm 타입 설정
        session_uuid = "test-session-123"


        try:
            # * 파일이 존재하면 기존 데이터를 읽고, 없다면 새로 생성합니다.
            result_file_path = os.path.join('test', 'test_converting', 'test_results.json')
            if os.path.exists(result_file_path):
                with open(result_file_path, 'r', encoding='utf-8') as f:
                    test_data = json.load(f)
            else:
                test_data = {}              

            # * Controller 생성 테스트 시작
            controller_results = {}
            for object_name in object_names:
                service_creation_info = test_data.get('service_skeleton_list', {}).get(object_name, [])
                controller_skeleton = test_data.get("controller_skeleton", {}).get(object_name, "")
                controller_class_name = test_data.get("controller_class_name", {}).get(object_name, "")
                merge_controller_method_code = ""
                
                # * 각 스켈레톤 데이터에 대해 컨트롤러 메서드 생성 수행
                for service_data in service_creation_info:
                    merge_controller_method_code = await start_controller_processing(
                        service_data['method_signature'],
                        service_data['procedure_name'],
                        service_data['command_class_variable'],
                        service_data['command_class_name'],
                        service_data['node_type'],
                        merge_controller_method_code,
                        controller_skeleton,
                        object_name
                    )

                # * 컨트롤러 클래스 파일 생성   
                await generate_controller_class(controller_skeleton, controller_class_name, merge_controller_method_code, session_uuid)


            # * 결과를 결과 파일에 저장합니다.
            test_data.update({
                "controller_skeleton": {name: results["controller_skeleton"] for name, results in controller_results.items()},
                "controller_class_name": {name: results["controller_class_name"] for name, results in controller_results.items()}
            })
            
            with open(result_file_path, 'w', encoding='utf-8') as f:
                json.dump(test_data, f, ensure_ascii=False, indent=2)

            self.assertTrue(True, "컨트롤러 생성 테스트가 성공적으로 완료되었습니다.")
        except Exception:
            self.fail(f"컨트롤러 생성 테스트 중 예외 발생")

if __name__ == '__main__':
    unittest.main()
