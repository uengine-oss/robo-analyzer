import logging
import os
from util.exception import CompareResultError

TEST_PATH = 'target/java/demo/src/test/java/com/example/demo'


async def create_junit_test(parameters: list, procedure_name: str):
    try:
        // 전달된 값으로 데이터를 적절히 가공해서 llm에게 전달해야함
        procedure_name : 프로시저 이름(스네이크 케이스임, 예 : TPX_MAIN)
        parameters : 프로시저에 전달될 파라미터 정보
        위 에 두 가지 정보를 하나로 합쳐서 프로시저 호출 정보를 생성

        given과 then 로그 파일을 읽어와서 가져와야함 

        given, then, 프로시저 호출 정보를 llm에 넘김 

        파일 경로는 다 설정되어있음   
        
        # * 생성된 테스트 코드를 파일로 저장
        parent_workspace_dir = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
        test_file_path = os.path.join(parent_workspace_dir, TEST_PATH, "실제 이름")

        os.makedirs(os.path.dirname(test_file_path), exist_ok=True)
        with open(test_file_path, "w", encoding="utf-8") as f:
            f.write(실제 코드)
        
    except Exception:
        err_msg = "Junit 테스트 코드 작성 중 오류가 발생했습니다."
        logging.error(err_msg, exc_info=False)
        raise CompareResultError(err_msg)