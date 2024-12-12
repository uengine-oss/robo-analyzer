import os
import logging
import tiktoken
import subprocess
import json
from openai import OpenAI

encoder = tiktoken.get_encoding("cl100k_base")
JAVA_PATH = 'target/java/demo/src/main/java/com/example/demo'
JAVA_TEST_PATH = 'target/java/demo/src/test/java/com/example/demo'
base_directory = os.path.abspath(os.path.join(os.path.dirname(__file__), '../../'))

def read_files_in_directory(directory_path):
    logging.info(f"디렉토리 '{directory_path}'의 파일 읽기 시작")
    files_data = []
    try:
        for root, _, files in os.walk(directory_path):
            for file in files:
                file_path = os.path.join(root, file)
                try:
                    with open(file_path, 'r', encoding='utf-8') as f:
                        content = f.read()
                except UnicodeDecodeError:
                    # If UTF-8 fails, try reading with a different encoding
                    with open(file_path, 'r', encoding='latin-1') as f:
                        content = f.read()
                files_data.append({
                    "name": file,
                    "content": content
                })
        logging.info(f"디렉토리 '{directory_path}'에서 {len(files_data)}개 파일을 성공적으로 읽음")
        return files_data
    except Exception as e:
        logging.error(f"디렉토리 '{directory_path}' 파일 읽기 중 오류 발생: {str(e)}")
        raise

def get_all_java_files_in_directory(directory):
    logging.info(f"디렉토리 '{directory}'에서 Java 파일 검색 시작")
    code_list = []
    try:
        for root, _, files in os.walk(directory):
            for file in files:
                if file.endswith('.java'):  # Java 파일만 선택
                    file_path = os.path.join(root, file)
                    try:
                        with open(file_path, 'r', encoding='utf-8') as f:
                            content = f.read()
                    except UnicodeDecodeError:
                        # If UTF-8 fails, try reading with a different encoding
                        with open(file_path, 'r', encoding='latin-1') as f:
                            content = f.read()
                    code_list.append({
                        "filePath": file_path,
                        "content": content
                    })
        logging.info(f"디렉토리 '{directory}'에서 {len(code_list)}개의 Java 파일을 성공적으로 읽음")
        return code_list
    except Exception as e:
        logging.error(f"Java 파일 읽기 중 오류 발생: {str(e)}")
        raise

def read_log_files_in_directory(directory_path, prefix):
    logging.info(f"'{directory_path}' 디렉토리에서 '{prefix}' 접두사를 가진 로그 파일 읽기 시작")
    files_data = []
    try:
        for root, _, files in os.walk(directory_path):
            for file in files:
                if file.startswith(prefix):
                    file_path = os.path.join(root, file)
                    try:
                        with open(file_path, 'r', encoding='utf-8') as f:
                            content = f.read()
                    except UnicodeDecodeError:
                        with open(file_path, 'r', encoding='latin-1') as f:
                            content = f.read()
                    files_data.append({
                        "name": file,
                        "content": content
                    })
        logging.info(f"'{prefix}' 접두사를 가진 {len(files_data)}개의 로그 파일을 성공적으로 읽음")
        return files_data
    except Exception as e:
        logging.error(f"로그 파일 읽기 중 오류 발생: {str(e)}")
        raise

async def execute_maven_commands(pom_directory: str) -> None:
    logging.info(f"Maven 테스트 실행 시작 (디렉토리: {pom_directory})")
    try:
        # base_directory = os.getenv('DOCKER_COMPOSE_CONTEXT')
        #  if base_directory:
        #     # Docker 환경인 경우의 경로
        #     search_path = os.path.join(base_directory, 'target')
        # else:
        #     # 로컬 환경인 경우의 경로
        #     parent_workspace_dir = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
        #     search_path = os.path.join(parent_workspace_dir, 'target')

        java_test_directory = os.path.join(base_directory, JAVA_TEST_PATH)
        
        test_result = subprocess.run(
            ['mvn', 'test', '-Dtest=com.example.demo.ComparisonTest'],
            cwd=java_test_directory,
            capture_output=False,
            text=True
        )

        if test_result.returncode != 0:
            logging.error(f"테스트 실행 결과: 로그 불일치 (종료 코드: {test_result.returncode})")
            # 테스트가 실패한 경우(로그가 다른 경우)에만 추가 분석 수행
            plsql_directory_path = 'data/plsql'
            plsql_files = read_files_in_directory(plsql_directory_path)

            java_files = get_all_java_files_in_directory(os.path.join(base_directory, 'target'))
            
            logs_directory = 'logs'
            compare_result_files = read_log_files_in_directory(logs_directory, 'compare_result')
            extracted_plsql_files = read_log_files_in_directory(logs_directory, 'extracted_plsql')
            extracted_java_files = read_log_files_in_directory(logs_directory, 'extracted_java')

            # 코드 분석 실행
            analyze_code(plsql_files, java_files, extracted_plsql_files, extracted_java_files, compare_result_files)
        else:
            logging.info("테스트 실행 결과: 로그 일치")

    except Exception as e:
        logging.error(f"Maven 명령 실행 중 오류 발생: {str(e)}")
        raise

def generate_prompt(plsql_files, java_files, extracted_plsql_files, extracted_java_files, compare_result_files):
    return f"""
    PL/SQL 파일과 변환된 Java 파일의 실행 결과가 다릅니다.
    모든 PL/SQL 파일 내용:
    {plsql_files}

    변환된 Java 파일 내용:
    {java_files}

    PL/SQL 실행 로그:
    {extracted_plsql_files}

    Java 실행 로그:
    {extracted_java_files}

    PL/SQL, Java 실행 로그 비교 결과:
    {compare_result_files}

    Java 파일을 어떻게 수정해야 PL/SQL 파일과 동일한 실행 결과를 얻을 수 있을지 제안해 주세요.
    답변은 항상 아래의 JSON 형식으로 출력해주세요.
    JSON 형식: [
        {{
            "filePath": "수정된 Java 파일 경로", 
            "code": "수정된 Java 코드",
            "reason": "수정 이유"
        }}
    ]
    
    """

def analyze_code(plsql_files, java_files, extracted_plsql_files, extracted_java_files, compare_result_files):
    logging.info("코드 업데이트 시작")
    try:
        # 프롬프트 생성
        prompt = generate_prompt(plsql_files, java_files, extracted_plsql_files, extracted_java_files, compare_result_files)

        client = OpenAI(
            api_key=os.environ.get("sk-"),
        )

        chat_completion = client.chat.completions.create(
            messages=[
                {
                    "role": "user",
                    "content": prompt,
                }
            ],
            model="gpt-4o",
        )

        # 결과 출력
        response_content = chat_completion.choices[0].message.content
    
        updates = json.loads(response_content.strip('```json\n').strip('```'))
        logging.info(f"GPT 응답 수신 완료: {len(updates)}개의 파일 업데이트 제안")
        
        for update in updates:
            file_path = update['filePath']
            logging.info(f"파일 업데이트 중: {file_path}")
            new_code = update['code'].replace("\\n", "\n")  # Replace escaped newlines with actual newlines

            # Write the new code to the file
            with open(file_path, 'w', encoding='utf-8') as file:
                file.write(new_code)

            print(f"Updated file: {file_path}")
            logging.info(f"파일 업데이트 완료: {file_path}")

        logging.info("모든 코드 업데이트가 완료되었습니다. 테스트를 재실행합니다.")
        execute_maven_commands(pom_directory='/path/to/your/pom/directory')
        
    except json.JSONDecodeError as e:
        logging.error(f"GPT 응답 JSON 파싱 중 오류 발생: {str(e)}")
        raise
    except Exception as e:
        logging.error(f"코드 분석 중 오류 발생: {str(e)}")
        raise

