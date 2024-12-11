import os
import logging
import tiktoken
import subprocess
import json
import time
from openai import OpenAI

encoder = tiktoken.get_encoding("cl100k_base")
JAVA_PATH = 'java/demo/src/main/java/com/example/demo'
base_directory = os.path.abspath(os.path.join(os.path.dirname(__file__), '../../'))
    
def find_java_directory(base_path):
    for root, dirs, files in os.walk(base_path):
        if 'pom.xml' in files:
            return root
    return None

def kill_process_on_port(port):
    try:
        # Find the process using the port
        result = subprocess.run(
            ['lsof', '-t', f'-i:{port}'],
            capture_output=True,
            text=True
        )
        
        if result.stdout:
            # Kill the process
            pid = result.stdout.strip()
            subprocess.run(['kill', '-9', pid])
            logging.info(f"Port {port} was in use. Process {pid} has been killed.")
        else:
            logging.info(f"No process is using port {port}.")
    except Exception as e:
        logging.error(f"Failed to kill process on port {port}: {e}")

async def execute_maven_commands(pom_directory: str) -> None:
    try:
        # base_directory = os.getenv('DOCKER_COMPOSE_CONTEXT')
        #  if base_directory:
        #     # Docker 환경인 경우의 경로
        #     search_path = os.path.join(base_directory, 'target')
        # else:
        #     # 로컬 환경인 경우의 경로
        #     parent_workspace_dir = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
        #     search_path = os.path.join(parent_workspace_dir, 'target')

        target_directory = os.path.join(base_directory, 'target')
        # target 디렉토리 내의 첫 번째 하위 디렉토리를 찾습니다.
        subdirectories = [d for d in os.listdir(target_directory) if os.path.isdir(os.path.join(target_directory, d))]
        if not subdirectories:
            logging.error("target 디렉토리에 하위 디렉토리가 없습니다.")
            raise Exception("target 디렉토리에 하위 디렉토리가 없습니다.")
        # 첫 번째 하위 디렉토리를 사용하여 search_path를 설정합니다.
        search_path = os.path.join(target_directory, subdirectories[0])

        # Find the directory containing the pom.xml
        java_directory = find_java_directory(search_path)
        if not java_directory:
            logging.error("pom.xml 파일을 찾을 수 없습니다.")
            raise Exception("pom.xml 파일을 찾을 수 없습니다.")

        kill_process_on_port(8082)

        # Maven command execution
        # Maven command execution in the background
        process = subprocess.Popen(['mvn', 'spring-boot:run'], cwd=java_directory, stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True)

        # Wait for a few seconds to ensure the application starts
        time.sleep(10)

        # Check if the process is still running
        if process.poll() is None:
            logging.info("Maven 명령이 백그라운드에서 실행 중입니다.")
        else:
            stdout, stderr = process.communicate()
            if process.returncode != 0:
                logging.error(f"Maven 명령 실패: {stderr}")
                raise Exception("Maven 명령 실행 실패")
            else:
                logging.info(f"Maven 명령 출력: {stdout}")

        # * 5. Java 테스트 메서드 순차 실행
        # Maven을 사용하여 테스트 실행
        test_methods = [
            "com.uengine.result_comparator.OracleDBManagerTest#testDatabaseConnection",
            "com.uengine.result_comparator.OracleDBManagerTest#testInitializeDatabaseSchema",
            "com.uengine.result_comparator.OracleDBManagerTest#testRefreshData",
            "com.uengine.result_comparator.OracleDBManagerTest#testRegisterStoredProcedure",
            "com.uengine.result_comparator.OracleDBManagerTest#testExecutePayrollCalculation",
            "com.uengine.result_comparator.OracleDBManagerTest#testCalculatePayrollViaJavaService",
            "com.uengine.result_comparator.OracleDBManagerTest#testLogComparison"
        ]

        for test_method in test_methods:
            test_result = subprocess.run(
                ['mvn', 'test', f'-Dtest={test_method}'],
                cwd='resultComparator',
                capture_output=False,
                text=True
            )

            if test_result.returncode != 0:
                logging.error(f"Java 테스트 실행 실패: {test_method}")
            else:
                logging.info(f"Java 테스트 실행 성공: {test_method}")

        comparison_result = ""

        result_file_path = 'LogComparisonResult.json'
        if os.path.exists(result_file_path):
            with open(result_file_path, 'r') as file:
                log_comparison_result = json.load(file)
                comparison_result = log_comparison_result
                logging.info(f"Log comparison result: {log_comparison_result}")

        plsql_directory_path = 'data/plsql'
        plsql_log_path = 'logs/plsql_logs.jsonl'
        java_log_path = 'logs/java_logs.jsonl'

        # 파일 내용 읽기
        plsql_files = read_files_in_directory(plsql_directory_path)
        java_files = get_all_java_files_in_directory(os.path.join(base_directory, 'target'))
        plsql_log = read_file(plsql_log_path)
        java_log = read_file(java_log_path)

        # 코드 분석 실행
        analyze_code(plsql_files, java_files, plsql_log, java_log, comparison_result)

    except Exception as e:
        logging.exception("Maven 명령 실행 중 오류 발생")
        raise e

def read_files_in_directory(directory_path):
    files_data = []
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
    return files_data

def get_all_java_files_in_directory(directory):
    code_list = []
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
    return code_list

def read_file(file_path):
    with open(file_path, 'r', encoding='utf-8') as file:
        return file.read()

def generate_prompt(plsql_files, java_files, plsql_log, java_log, comparison_result):
    return f"""
    PL/SQL 파일과 변환된 Java 파일의 실행 결과가 다릅니다.
    모든 PL/SQL 파일 내용:
    {plsql_files}

    변환된 Java 파일 내용:
    {java_files}

    PL/SQL 실행 로그:
    {plsql_log}

    Java 실행 로그:
    {java_log}

    PL/SQL, Java 실행 로그 비교 결과:
    {comparison_result}

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

def analyze_code(plsql_files, java_files, plsql_log, java_log, comparison_result):
    try:
        # 프롬프트 생성
        prompt = generate_prompt(plsql_files, java_files, plsql_log, java_log, comparison_result)

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

        # Iterate over each update and write the new code to the specified file
        for update in updates:
            file_path = update['filePath']
            new_code = update['code'].replace("\\n", "\n")  # Replace escaped newlines with actual newlines

            # Write the new code to the file
            with open(file_path, 'w', encoding='utf-8') as file:
                file.write(new_code)

            print(f"Updated file: {file_path}")

        logging.info("코드 분석이 완료되었습니다. Maven 명령을 다시 실행합니다.")
        execute_maven_commands(pom_directory='/path/to/your/pom/directory')

    except Exception as e:
        logging.exception("코드 분석 중 오류 발생")
        raise e

