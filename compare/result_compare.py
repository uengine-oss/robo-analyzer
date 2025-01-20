import os
import logging
import tiktoken
import subprocess
import json
from openai import OpenAI
import difflib

from compare.extract_log_info import clear_log_files, compare_then_results, extract_java_given_when_then
from prompt.generate_compare_text_prompt import generate_compare_text
from prompt.generate_error_log_prompt import generate_error_log
from semantic.vectorizer import vectorize_text
from understand.neo4j_connection import Neo4jConnection

encoder = tiktoken.get_encoding("cl100k_base")
JAVA_PATH = 'target/java/demo/src/main/java/com/example/demo'
JAVA_TEST_PATH = 'target/java/demo/src/test/java/com/example/demo'
base_directory = os.path.abspath(os.path.join(os.path.dirname(__file__), '../../'))
modification_history = []
global_test_class_names = []
global_plsql_gwt_log = []
stop_execution_flag = False

def stop_execution():
    global stop_execution_flag
    stop_execution_flag = True

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
                # Skip .DS_Store and .class files
                if os.path.basename(file_path) == '.DS_Store' or file_path.endswith('.class'):
                    continue
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

async def execute_maven_commands(test_class_names: list, plsql_gwt_log: list, user_id: str):
    global global_test_class_names, global_plsql_gwt_log, stop_execution_flag
    
    stop_execution_flag = False
    
    # Store the received parameters in global variables
    global_test_class_names = test_class_names
    global_plsql_gwt_log = plsql_gwt_log
    
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

                if "COMPILATION ERROR" in test_result.stdout or "Exception" in test_result.stdout:
                    yield json.dumps({"type": "update_feedbackLoop_status", "status": "java_compile_error"}).encode('utf-8') + b"send_stream"
                    error_output = test_result.stdout
                    logging.error(f"오류 내용: {error_output}")
                    java_files = get_all_files_in_directory(os.path.join(base_directory, 'target'))
                    plsql_java_pairs = []
                    error_result = generate_error_log(error_output)
                    error_text = error_result["error_text"]
                    explanation = error_text
                    vector_log = vectorize_text(error_text)
                    similar_node = await conn.search_similar_nodes(vector_log)
                    
                    for node in similar_node:
                        matching_file = next(
                            (file for file in java_files if os.path.basename(file['filePath']) == node['java_file']), 
                            None
                        )
                        file_path = matching_file['filePath'] if matching_file else node['java_file']
                        plsql_java_pairs.append({
                            'plsql_code': node['node_code'],
                            'java_code': node['java_code'],
                            'filePath': file_path,
                            # 'java_range': node['java_range'] if 'java_range' in node else None
                        })
                    
                    async for update_result in update_code(explanation, java_files, error_output, plsql_java_pairs):
                        yield update_result
                    return

                else:
                    test_failed = True
                    failed_test_index.append(i)
            
            logging.info(f"{test_class_name} 테스트 클래스 실행 완료")
            java_gwt_log = await extract_java_given_when_then(i)
            compare_log = await compare_then_results(i)
            await clear_log_files('java', 'plsql')

            yield json.dumps({
                "type": "java",
                "log": java_gwt_log
            }, ensure_ascii=False).encode('utf-8') + b"send_stream"
        
        yield json.dumps({
            "type": "update_feedbackLoop_status", 
            "status": "compare_complete"
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
            compare_result = generate_compare_text(java_gwt_log, plsql_gwt_log, compare_log)
            compare_text = compare_result["compare_text"]
            plsql_java_pairs = []
            vector_log = vectorize_text(compare_text)
            explanation = compare_text
            similar_node = await conn.search_similar_nodes(vector_log)
            
            for node in similar_node:
                matching_file = next(
                    (file for file in java_files if os.path.basename(file['filePath']) == node['java_file']), 
                    None
                )
                file_path = matching_file['filePath'] if matching_file else node['java_file']
                plsql_java_pairs.append({
                    'plsql_code': node['node_code'],
                    'java_code': node['java_code'],
                    'filePath': file_path,
                    # 'java_range': node['java_range'] if 'java_range' in node else None
                })

            for index in failed_test_index:
                plsql_log_files.extend(read_single_log_file(logs_directory, f'result_plsql_given_when_then_case{index}.json'))
                java_log_files.extend(read_single_log_file(logs_directory, f'result_java_given_when_then_case{index}.json'))
                compare_result_files.extend(read_single_log_file(logs_directory, f'compare_result_case{index}.json'))

            async for update_result in update_code(explanation, java_files, error_output, plsql_java_pairs, 
                                         plsql_log_files, java_log_files, 
                                         compare_result_files, test_class_names):
                yield update_result
        else:
            logging.info("테스트 실행 결과: 성공")

    except Exception as e:
        logging.error(f"Maven 명령 실행 중 오류 발생: {str(e)}")
        raise
    finally:
        await conn.close()

def generate_prompt(plsql_log_files, java_log_files, compare_result_files, plsql_java_pairs, explanation):
    
    global modification_history
    # summarized_plsql = prepare_code_for_prompt(plsql_files, plsql_log_files)
    
    return f"""
    PL/SQL 파일과 해당 PL/SQL 파일이 Java 변환된 파일의 실행 결과가 다릅니다.

    실행 결과 여러 case 가 있으며, 각 실행 결과 given, when, then 으로 구성되어 있습니다.
    given 의 내용은 초기 조건을 설정하는 부분이고,
    when 은 실행된 procedure 내용이고,
    then 은 실행 결과 내용입니다.

    각 case 별 when 에서 사용된 procedure 내용을 추출하여 프롬프트에 사용합니다.

    노드 정보(테스트 실패에 가장 원인이 되는 Java 코드 목록들과 해당 자바 코드의 원본 plsql 코드 목록들):
    {plsql_java_pairs}

    여러 case 별 PL/SQL 실행 결과:
    {plsql_log_files}

    여러 case 별 Java 실행 결과:
    {java_log_files}

    PL/SQL, Java 실행 결과 비교:
    {compare_result_files}
    
    두 파일의 차이점 설명:
    {explanation}
    
    이전 수정 목록:
    {modification_history}
    이전 수정 목록을 참고하여 수정 내용을 제안해 주세요. 동일한 수정이 반복되지 않도록 해주세요.

    Java 파일을 어떻게 수정해야 PL/SQL 파일과 동일한 실행 결과를 얻을 수 있을지 제안해 주세요.
    이전 수정 내용들을 참고하여 수정 내용을 제안해 주세요. 동일한 수정 내용은 제안하지 않아도 됩니다.
    또한 동일한 파일에 대한 수정 내용은 여러번 나눠서 제안하는것이 아니라 한번에 합쳐서 제안해 주세요. 이런 경우에는 수정 이유가 여러개가 될 수 있습니다.
    수정 이유는 보다 상세하고 명확하게 작성해 주세요.

    테스트 파일에 대한 수정은 절대 이루워져서는 안됩니다.
    
    생성할 값 중 "original_java_code" 는 제공받은 노드 정보에 있는 'java_code' 값을 그대로 제공해야합니다. 
    "original_java_code" 를 사용해서 전체 코드중 "modified_code" 로 수정할 위치를 찾아야하기 때문에 
    제공받은 목록에 있는 내용중에서만 제공해야하며 그 어떠한 임의의 설명이나 수정 내용 없이 제공받은 노드 정보에 있는 'java_code' 값을 그대로 제공해야합니다.
    
    동일한 파일에 대한 수정은 한번만 제공해야합니다.

    답변은 항상 아래의 JSON 형식으로 출력해주세요.
    JSON 형식: [
        {{
            "type": "java_file_update",
            "filePath": "수정된 Java 파일 경로", 
            "reason": "original_java_code 를 수정하는 이유(보다 상세하고 명확하게 한글로 작성)",
            "original_java_code": "수정되기 전 원본 자바 코드(어떠한 설명이나 수정 내용 없이 제공받은 노드 정보에 있는 'java_code' 값을 그대로 제공해야합니다. 제공받은 목록에 있는 내용중에서만 제공해야합니다.)"
            "modified_code": "original_java_code 내용을 오류 해결을 위해 수정한 Java 코드(original_java_code 내용과 동일해서는 안됩니다. 수정이유에 근거하여 코드를 수정하여야합니다.)",
        }}
    ]
    
    """

def generate_error_fix_prompt(pom_files, test_java_files, error, plsql_java_pairs, explanation):
    global modification_history
    
    return f"""
    Java 파일 실행을 시도하였는데 오류가 발생하였습니다.

    오류 내용:
    {error}
    
    오류 내용 설명:
    {explanation}

    컴파일을 시도한 Java 폴더 내 pom.xml:
    {pom_files}
    
    컴파일을 시도한 Java 폴더 내 테스트 파일 목록:
    {test_java_files}
    
    컴파일 오류의 원인에 가장 가까운 Java 코드 목록들과 해당 자바 코드의 원본 plsql 코드 목록들:
    {plsql_java_pairs}
    
    테스트 파일에 대한 수정은 왠만하면 이루어지지 않아야합니다. 정말 필요한 경우에만 수정해야합니다.
    테스트 파일에서 오류가 발생한 경우 제공된 테스트 파일 내용을 보고 오류를 해결하면 됩니다. 테스트 파일을 수정할 때에는 오류에 관한 수정만 이루어져야하며, 기존 테스트 내용을 변경하거나 코드를 제거하는등의 방법으로 오류를 회피해서는 절대 안됩니다.
    테스트 파일에서 오류가 발생하지 않은 경우 제공된 컴파일 오류의 원인에 가장 가까운 Java 코드 목록들과 해당 자바 코드의 원본 plsql 코드 목록들 내용을 보고 오류를 해결하면 됩니다.
    
    이전 수정 목록:
    {modification_history}
    이전 수정 목록을 참고하여 수정 내용을 제안해 주세요. 동일한 수정이 반복되지 않도록 해주세요.
    
    Java 파일을 어떻게 수정해야 오류 없이 컴파일 시킬 수 있는지 개선안을 자세히 제안해 주세요.
    답변은 항상 아래의 JSON 형식으로 출력해주세요.
    
    생성할 값 중 "original_java_code" 는 제공받은 노드 정보에 있는 'java_code' 값을 그대로 제공해야합니다. 
    "original_java_code" 를 사용해서 전체 코드중 "modified_code" 로 수정할 위치를 찾아야하기 때문에 
    제공받은 목록에 있는 내용중에서만 제공해야하며 그 어떠한 임의의 설명이나 수정 내용 없이 제공받은 노드 정보에 있는 'java_code' 값을 그대로 제공해야합니다.
    
    동일한 파일에 대한 수정은 한번만 제공해야합니다.
    
    테스트 파일에서 오류가 발생한 경우에는 아래의 JSON 형식을 사용합니다.
    JSON 형식: [
        {{
            "type": "test_file_update",
            "filePath": "수정된 Java 파일 경로", 
            "modified_code": "수정된 테스트 파일의 전체 Java 코드(반드시 전체 파일 내용을 제공해야합니다.)",
            "reason": "수정 이유(보다 상세하고 명확하게 한글로 작성)",
        }}
    ]
    
    그렇지 않은 경우에는 아래의 JSON 형식을 사용합니다.
    JSON 형식: [
        {{
            "type": "java_file_update",
            "filePath": "수정된 Java 파일 경로", 
            "reason": "original_java_code 를 수정하는 이유(보다 상세하고 명확하게 한글로 작성)",
            "original_java_code": "수정되기 전 원본 자바 코드(어떠한 설명이나 수정 내용 없이 제공받은 노드 정보에 있는 'java_code' 값을 그대로 제공해야합니다. 제공받은 목록에 있는 내용중에서만 제공해야합니다.)"
            "modified_code": "original_java_code 내용을 오류 해결을 위해 수정한 Java 코드(original_java_code 내용과 동일해서는 안됩니다. 수정이유에 근거하여 코드를 수정하여야합니다.)",
        }}
    ]
    
    어떠한 경우에도 JSON 형식 이외의 다른 추가 설명이나 내용은 제공하지 않아야합니다.
    
    """

def normalize_code(code):
    # Remove all whitespace and newline characters
    return ''.join(code.split())

def replace_code(original_code, java_code, updated_code):
    # Normalize both the original and the java_code for comparison
    normalized_original = normalize_code(original_code)
    normalized_java_code = normalize_code(java_code)

    # Find the start index of the normalized java_code in the normalized original code
    start_index = normalized_original.find(normalized_java_code)
    
    if start_index == -1:
        raise ValueError("java_code not found in original_code")
    
    # Calculate the end index
    end_index = start_index + len(normalized_java_code)
    
    # Replace the code in the original with the updated code
    modified_code = original_code[:start_index] + updated_code + original_code[end_index:]
    
    return modified_code
    
def merge_code(original_code, updated_code, java_range):
    start_line, end_line = map(int, java_range.split('-'))
    original_lines = original_code.splitlines()
    updated_lines = updated_code.splitlines()
    original_lines[start_line-1:end_line] = updated_lines

    return '\n'.join(original_lines)

async def update_code(explanation=None, java_files=None, error=None, plsql_java_pairs=None, plsql_log_files=None, java_log_files=None, compare_result_files=None, test_class_names=None):
    global modification_history, global_test_class_names, global_plsql_gwt_log, stop_execution_flag
    conn = Neo4jConnection()
    logging.info("코드 업데이트 시작")
    try:
        if error:
            # 컴파일 또는 런타임 오류가 발생한 경우
            # Extract pom.xml files from java_files
            pom_files = [file for file in java_files if file['filePath'].endswith('pom.xml')]
            test_java_files = [
                file for file in java_files 
                if any(file['filePath'].endswith(f"{test_class_name}.java") for test_class_name in global_test_class_names)
            ]
            prompt = generate_error_fix_prompt(pom_files, test_java_files, error, plsql_java_pairs, explanation)
        else:
            prompt = generate_prompt(plsql_log_files, java_log_files, compare_result_files, plsql_java_pairs, explanation)

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
            
            # java_range = next((pair['java_range'] for pair in plsql_java_pairs if pair['filePath'] == file_path), None)
            
            # if not java_range:
            #     java_range = update.get('java_range', None)
            
            # if not java_range:
            #     modifiedCode = update['modified_code']
            # else:
            #     modifiedCode = merge_code(originalCode, update['modified_code'], java_range)

            if update['type'] == "test_file_update":
                modifiedCode = update['modified_code']
            else:
                java_code = next((pair['java_code'] for pair in plsql_java_pairs 
                    if pair['filePath'] == file_path and pair['java_code'].strip() == update['original_java_code'].strip()), None)
                modifiedCode = replace_code(originalCode, java_code, update['modified_code'])
                
                try:
                    await conn.update_node_code(java_code, update['modified_code'], file_path)
                    logging.info(f"노드 업데이트 완료")
                    
                    with open(file_path, 'w', encoding='utf-8') as file:
                        file.write(modifiedCode)
                    
                    print(f"Updated file: {file_path}")
                    logging.info(f"파일 업데이트 완료: {file_path}")

                except Exception as e:
                    logging.error(f"업데이트 중 오류 발생: {str(e)}")
                    raise
                
            reason = update['reason']

            diff = difflib.unified_diff(
                originalCode.splitlines(),
                modifiedCode.splitlines(),
                lineterm=''
            )
            diff_text = '\n'.join(diff)

            if len(modification_history) >= 10:
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

        await conn.close()
        logging.info("모든 코드 업데이트가 완료되었습니다. 테스트를 재실행합니다.")
        yield json.dumps({"type": "update_feedbackLoop_status", "status": "java_file_update_finished"}).encode('utf-8') + b"send_stream"
        
        if stop_execution_flag:
            logging.info("Execution stop requested. Exiting...")
            return
        
        async for result in execute_maven_commands(global_test_class_names, global_plsql_gwt_log):
            yield result
        
    except json.JSONDecodeError as e:
        logging.error(f"GPT 응답 JSON 파싱 중 오류 발생: {str(e)}")
        raise
    except Exception as e:
        logging.error(f"코드 분석 중 오류 발생: {str(e)}")
        raise