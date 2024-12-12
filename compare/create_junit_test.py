import logging
import os
from util.exception import CompareResultError

TEST_PATH = 'target/java/demo/src/test/java/com/example/demo'


async def create_junit_test(parameters: list, camel_object_name: str, procedure_name: str):
    try:
        
        
        # * 생성된 테스트 코드를 파일로 저장
        parent_workspace_dir = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
        test_file_path = os.path.join(parent_workspace_dir, TEST_PATH, "ComparisonTest.java")
        os.makedirs(os.path.dirname(test_file_path), exist_ok=True)
        with open(test_file_path, "w", encoding="utf-8") as f:
            f.write(test_template)
        
    except Exception:
        err_msg = "Junit 테스트 코드 작성 중 오류가 발생했습니다."
        logging.error(err_msg, exc_info=False)
        raise CompareResultError(err_msg)