import logging
import os
from prompt.generate_junit_test_prompt import generate_test_code
from util.exception import CompareResultError

TEST_PATH = 'target/java/demo/src/test/java/com/example/demo'


async def create_junit_test(given_when_then_log: dict, table_names: list, package_name: str, procedure_name: str):
    try:
        test_result = generate_test_code(
            table_names,
            package_name,
            procedure_name,
            procedure_info=given_when_then_log.get("when"),
            given_log=given_when_then_log.get("given"),
            then_log=given_when_then_log.get("then")
        )
        

        # * 생성된 테스트 코드를 파일로 저장
        parent_workspace_dir = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
        test_file_path = os.path.join(parent_workspace_dir, TEST_PATH, f"{test_result['className']}.java")
        
        os.makedirs(os.path.dirname(test_file_path), exist_ok=True)
        with open(test_file_path, "w", encoding="utf-8") as f:
            f.write(test_result['testCode'])
        
        return test_result['className']
    
    except Exception:
        err_msg = "Junit 테스트 코드 작성 중 오류가 발생했습니다."
        logging.error(err_msg, exc_info=False)
        raise CompareResultError(err_msg)
    