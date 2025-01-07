import logging
import os
import re
from prompt.generate_junit_test_prompt import generate_test_code
from util.exception import GenerateJunitError, QueryMethodError, ReadFileError, StringConversionError
from util.file_utils import read_target_file
from util.string_utils import convert_to_pascal_case

TEST_PATH = 'target/java/demo/src/test/java/com/example/demo'

# 역할 : JUnit 테스트 코드를 생성하는 함수
#
# 매개변수 : 
#   - given_when_then_log : 주어진 로그 데이터
#   - table_names : 테이블 이름 리스트
#   - package_name : 패키지 이름
#   - procedure_name : 프로시저 이름
#
# 반환값 : 
#   - str : 생성된 테스트 코드 파일 이름
async def create_junit_test(given_when_then_log: dict, table_names: list, package_name: str, procedure_name: str):
    try:

        repository_codes = await get_repository_codes(given_when_then_log)

        test_result = generate_test_code(
            table_names,
            package_name,
            procedure_name,
            repository_codes,
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
    
    except Exception as e:
        err_msg = f"Junit 테스트 코드 작성 중 오류가 발생했습니다: {str(e)}"
        logging.error(err_msg)
        raise GenerateJunitError(err_msg)


# 역할 : 주어진 로그 데이터에서 테이블 이름을 추출하고, 해당 테이블의 Repository.java 파일을 읽어서 SQL 메서드를 추출하는 함수
#
# 매개변수 :
#   - given_when_then_log: 주어진 로그 데이터
#
# 반환값 :
#   - dict: 테이블 이름을 키로 하고, 해당 테이블의 SQL 메서드를 값으로 하는 딕셔너리
async def get_repository_codes(given_when_then_log: dict):
    try:
        # * then 섹션에서 테이블 이름 추출
        then_tables = [entry['table'] for entry in given_when_then_log.get('then', [])]
        
        # * 중복 제거
        unique_tables = list(set(then_tables))
        
        # * 테이블 이름을 파스칼 케이스로 변환하고 Repository.java 추가
        repository_files = {}  # 빈 딕셔너리로 초기화
        for table in unique_tables:
            pascal_name = convert_to_pascal_case(table.lower())
            repository_name = f"{pascal_name}Repository"
            
            # * repository 파일 읽기
            repository_code = read_target_file(repository_name, "repository")
            
            # * JPA 쿼리 메서드 추출
            pattern = r'(@Query|@Select|@Insert|@Update|@Delete)\([^)]*\)\s*[\w\s<>,@()]*;'
            query_methods = re.findall(pattern, repository_code, re.DOTALL)
            repository_files[pascal_name] = query_methods
            
        # * 추가 작업을 위해 repository_files 반환
        return repository_files

    except (ReadFileError, StringConversionError) as e:
        raise
    except Exception as e:
        err_msg = f"Repository 코드 추출 중 오류 발생: {str(e)}"
        logging.error(err_msg)
        raise QueryMethodError(err_msg)