import os
import logging
import tiktoken
import subprocess
import json
from openai import OpenAI
import difflib

from compare.extract_log_info import clear_log_file, compare_then_results, extract_java_given_when_then
from semantic.vectorizer import vectorize_text
from understand.neo4j_connection import Neo4jConnection

encoder = tiktoken.get_encoding("cl100k_base")
JAVA_PATH = 'target/java/demo/src/main/java/com/example/demo'
JAVA_TEST_PATH = 'target/java/demo/src/test/java/com/example/demo'
base_directory = os.path.abspath(os.path.join(os.path.dirname(__file__), '../../'))
modification_history = []

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
    
def get_all_files_in_directory(directory):
    logging.info(f"디렉토리 '{directory}'에서 모든 파일 검색 시작")
    code_list = []
    try:
        for root, _, files in os.walk(directory):
            for file in files:
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
        logging.info(f"디렉토리 '{directory}'에서 {len(code_list)}개의 파일을 성공적으로 읽음")
        return code_list
    except Exception as e:
        logging.error(f"파일 읽기 중 오류 발생: {str(e)}")
        raise

def read_single_log_file(directory_path, file_name):
    logging.info(f"'{directory_path}' 디렉토리에서 '{file_name}' 파일 읽기 시작")
    try:
        file_path = os.path.join(directory_path, file_name)
        if not os.path.exists(file_path):
            logging.error(f"파일을 찾을 수 없음: {file_path}")
            return None
            
        try:
            with open(file_path, 'r', encoding='utf-8') as f:
                content = f.read()
        except UnicodeDecodeError:
            with open(file_path, 'r', encoding='latin-1') as f:
                content = f.read()
                
        logging.info(f"'{file_name}' 파일을 성공적으로 읽음")
        return {
            "name": file_name,
            "content": content
        }
    except Exception as e:
        logging.error(f"파일 읽기 중 오류 발생: {str(e)}")
        raise

async def execute_maven_commands(test_class_names: list):
    logging.info(f"Maven 테스트 실행 시작")
    test_failed = False
    failed_test_index = []
    maven_project_root = os.path.join(base_directory, 'target', 'java', 'demo')
    logging.info(f"Maven 프로젝트 경로: {maven_project_root}")
    conn = Neo4jConnection()
        
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

        for i, test_class_name in enumerate(test_class_names, start=1):
            logging.info(f"{test_class_name} 테스트 클래스 실행 시작")
            command = f"mvn test -Dtest=com.example.demo.{test_class_name}"
            logging.info(f"실행 명령어: {command}")
            
            yield json.dumps({
                "type": "status",
                "message": f"Junit 테스트 케이스 {i} 실행 중"
            }, ensure_ascii=False).encode('utf-8') + b"send_stream"


            test_result = subprocess.run(
                command,
                cwd=maven_project_root,
                capture_output=True,
                text=True,
                shell=True
            )

            if test_result.returncode != 0:
                error_output = ""

                if "COMPILATION ERROR" in error_output or "Exception" in error_output:
                    error_output = test_result.stderr
                    logging.error(f"오류 내용: {error_output}")
                    java_files = get_all_files_in_directory(os.path.join(base_directory, 'target'))
                    
                    async for update_result in update_code(java_files, error_output):
                        yield update_result
                    return

                else:
                    test_failed = True
                    failed_test_index.append(i)
            
            logging.info(f"{test_class_name} 테스트 클래스 실행 완료")
            given_when_then_log = await extract_java_given_when_then(i)
            compare_log, difference_text = await compare_then_results(i)
            await clear_log_file('java')

            yield json.dumps({
                "type": "java",
                "log": given_when_then_log
            }, ensure_ascii=False).encode('utf-8') + b"send_stream"
        
        yield json.dumps({
            "type": "compare_complete",
        }, ensure_ascii=False).encode('utf-8') + b"send_stream"

        if test_failed:
            java_files = get_all_java_files_in_directory(os.path.join(base_directory, 'target'))
            logs_directory = 'logs'
            plsql_log_files = []
            java_log_files = []
            compare_result_files = []
            case_ids_str = ", ".join(map(str, failed_test_index))

            yield json.dumps({
                "type": "status",
                "message": f"실패한 테스트 케이스 {case_ids_str} 처리 중"
            }, ensure_ascii=False).encode('utf-8') + b"send_stream"

            # 차이점 벡터화 
            plsql_java_pairs = []
            vector_log = vectorize_text(difference_text)
            similar_node = await conn.search_similar_nodes(vector_log)
            
            matching_file = next(
                (file for file in java_files if os.path.basename(file['filePath']) == node['java_file']), 
                None
            )
            file_path = matching_file['filePath'] if matching_file else node['java_file']
            for node in similar_node:
                plsql_java_pairs.append({
                    'plsql': node['node_code'],
                    'java': node['java_code'],
                    'filePath': file_path,
                    'java_range': node['java_range']
                })

            for index in failed_test_index:
                plsql_log_files.extend(read_single_log_file(logs_directory, f'result_plsql_given_when_then_case{index}.json'))
                java_log_files.extend(read_single_log_file(logs_directory, f'result_java_given_when_then_case{index}.json'))
                compare_result_files.extend(read_single_log_file(logs_directory, f'compare_result_case{index}.json'))

            # await update_code(error_output, plsql_log_files, java_log_files, compare_result_files, test_class_names, plsql_java_pairs)
            async for update_result in update_code(java_files, error_output, 
                                         plsql_log_files, java_log_files, 
                                         compare_result_files, test_class_names, 
                                         plsql_java_pairs):
                yield update_result
        else:
            logging.info("테스트 실행 결과: 성공")

    except Exception as e:
        logging.error(f"Maven 명령 실행 중 오류 발생: {str(e)}")
        raise
    finally:
        await conn.close()

def generate_prompt(plsql_log_files, java_log_files, compare_result_files, plsql_java_pairs):
    
    global modification_history
    # summarized_plsql = prepare_code_for_prompt(plsql_files, plsql_log_files)
    
    return f"""
    PL/SQL 파일과 해당 PL/SQL 파일이 Java 변환된 파일의 실행 결과가 다릅니다.

    실행 결과 여러 case 가 있으며, 각 실행 결과 given, when, then 으로 구성되어 있습니다.
    given 의 내용은 초기 조건을 설정하는 부분이고,
    when 은 실행된 procedure 내용이고,
    then 은 실행 결과 내용입니다.

    각 case 별 when 에서 사용된 procedure 내용을 추출하여 프롬프트에 사용합니다.

    테스트 실패에 가장 원인이 되는 java 코드 목록들과 해당 자바 코드의 원본 plsql 코드 목록들:
    {plsql_java_pairs}

    여러 case 별 PL/SQL 실행 결과:
    {plsql_log_files}

    여러 case 별 Java 실행 결과:
    {java_log_files}

    PL/SQL, Java 실행 결과 비교:
    {compare_result_files}


    Java 파일을 어떻게 수정해야 PL/SQL 파일과 동일한 실행 결과를 얻을 수 있을지 제안해 주세요.
    이전 수정 내용들을 참고하여 수정 내용을 제안해 주세요. 동일한 수정 내용은 제안하지 않아도 됩니다.
    또한 동일한 파일에 대한 수정 내용은 여러번 나눠서 제안하는것이 아니라 한번에 합쳐서 제안해 주세요. 이런 경우에는 수정 이유가 여러개가 될 수 있습니다.
    수정 이유는 보다 상세하고 명확하게 작성해 주세요.

    테스트 파일에 대한 수정은 절대 이루워져서는 안됩니다.

    답변은 항상 아래의 JSON 형식으로 출력해주세요.
    JSON 형식: [
        {{
            "filePath": "수정된 Java 파일 경로", 
            "code": "수정된 Java 코드",
            "reason": "수정 이유(보다 상세하고 명확하게 작성)",
            "java_range": "수정된 자바 코드의 범위(기존 제공된 범위)"
        }}
    ]
    
    """

def generate_error_fix_prompt(java_files, error):
    
    return f"""
    Java 파일 실행을 시도하였는데 오류가 발생하였습니다.

    오류 내용:
    {error}

    실행을 시도한 Java 파일 내용:
    {java_files}

    Java 파일을 어떻게 수정해야 오류 없이 컴파일 시킬 수 있는지 개선안을 자세히 제안해 주세요.
    답변은 항상 아래의 JSON 형식으로 출력해주세요.
    JSON 형식: [
        {{
            "filePath": "수정된 Java 파일 경로", 
            "code": "수정된 Java 코드",
            "reason": "수정 이유"
        }}
    ]
    
    """
def merge_code(original_code, updated_code, java_range):
    start_line, end_line = map(int, java_range.split('-'))
    original_lines = original_code.splitlines()
    updated_lines = updated_code.splitlines()
    original_lines[start_line-1:end_line] = updated_lines

    return '\n'.join(original_lines)

async def update_code(java_files=None, error=None, plsql_log_files=None, java_log_files=None, compare_result_files=None, test_class_names=None, plsql_java_pairs=None):
    global modification_history
    logging.info("코드 업데이트 시작")
    try:
        if error:
            # 컴파일 또는 런타임 오류가 발생한 경우
            prompt = generate_error_fix_prompt(java_files, error)
        else:
            prompt = generate_prompt(plsql_log_files, java_log_files, compare_result_files, plsql_java_pairs)

        client = OpenAI(
            api_key=os.environ.get("OPENAI_API_KEY")
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

        response_content = chat_completion.choices[0].message.content
    
        updates = json.loads(response_content.strip('```json\n').strip('```'))
        logging.info(f"GPT 응답 수신 완료: {len(updates)}개의 파일 업데이트 제안")
        
        for update in updates:
            file_path = update['filePath']
            originalCode = next((file['content'] for file in java_files if file['filePath'] == file_path), None)
            logging.info(f"파일 업데이트 중: {file_path}")

            java_range = next((pair['java_range'] for pair in plsql_java_pairs if pair['filePath'] == file_path), None)
            
            if not java_range:
                java_range = update['java_range']
            
            modifiedCode = merge_code(originalCode, update['code'], java_range)

            reason = update['reason']

            diff = difflib.unified_diff(
                originalCode.splitlines(),
                modifiedCode.splitlines(),
                lineterm=''
            )
            diff_text = '\n'.join(diff)

            if len(modification_history) >= 5:
                modification_history.pop(0)

            modification_history.append({
                "filePath": file_path,
                "diff": diff_text,
                "reason": reason
            })

            yield json.dumps({
                "filePath": file_path, 
                "originalCode": originalCode, 
                "modifiedCode": modifiedCode, 
                "reason": reason, 
                "type": "java_file_update"
            }).encode('utf-8') + b"send_stream"

            with open(file_path, 'w', encoding='utf-8') as file:
                file.write(modifiedCode)

            print(f"Updated file: {file_path}")
            logging.info(f"파일 업데이트 완료: {file_path}")

        logging.info("모든 코드 업데이트가 완료되었습니다. 테스트를 재실행합니다.")
        yield json.dumps({"type": "java_file_update_finished"}).encode('utf-8') + b"send_stream"
        execute_maven_commands(test_class_names)
        
    except json.JSONDecodeError as e:
        logging.error(f"GPT 응답 JSON 파싱 중 오류 발생: {str(e)}")
        raise
    except Exception as e:
        logging.error(f"코드 분석 중 오류 발생: {str(e)}")
        raise

