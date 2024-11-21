import json
import os
import logging
import aiofiles
import tiktoken
from prompt.service_skeleton_prompt import convert_service_skeleton_code
from prompt.command_prompt import convert_command_code
from understand.neo4j_connection import Neo4jConnection
from util.exception import ConvertingError, ExtractCodeError, HandleResultError, LLMCallError, Neo4jError, ProcessResultError, SkeletonCreationError, TraverseCodeError

encoder = tiktoken.get_encoding("cl100k_base")



# 역할: 프로시저 노드의 토큰 개수를 체크하여, 처리하는 함수입니다.
# 매개변수: 
#   - input_data : 프로시저 노드 데이터
#   - variable_data: 선언 노드 데이터
#   - entity_name_list : 엔티티 이름 리스트
#   - object_name : 패키지 및 프로시저 이름
# 반환값: 
#   - service_skeleton_code : 서비스 스켈레톤 클래스 코드 
#   - command_class_variable : Command 클래스에 선언된 변수 목록
#   - service_skeleton_name: 서비스 클래스의 이름
#   - summarzied_service_skeleton : 요약된 서비스 골격 클래스
async def calculate_tokens_and_process(input_data, variable_data, entity_name_list, object_name):

    try:
        service_skeleton_code, command_class_variable, service_skeleton_name, summarzied_service_skeleton = await create_service_skeleton(input_data, variable_data, entity_name_list, object_name)
        return service_skeleton_code, command_class_variable, service_skeleton_name, summarzied_service_skeleton 
    
    except (LLMCallError, HandleResultError, ProcessResultError):
        raise
    except Exception:
        err_msg = "서비스 골격 클래스를 생성하는 과정에서 노드를 순회하는 도중 문제가 발생했습니다."
        logging.error(err_msg, exc_info=False)
        raise TraverseCodeError(err_msg)


# 역할: LLM의 분석 결과를 이용하여, 서비스 골격 클래스에 import문을 추가합니다.
# 매개변수: 
#   - service_skeleton_code : 서비스 골격 클래스 코드
#   - entity_name_list : 엔티티 이름 리스트
#   - service_skeleton_name :
#   - command_class_name : 
# 반환값: 
#   - service : 서비스 스켈레톤 클래스 코드
#   - command_class_variable : 프로시저의 입력 매개변수(Command 클래스에 선언된 변수 목록)
async def modify_service_skeleton(service_skeleton_code, entity_name_list, service_skeleton_name, command_class_name):
    
    repository_injection_code = ""
    
    try:
        # * package 선언 다음에 줄바꿈을 추가하고 entity import 문을 삽입합니다.
        package_line_end = service_skeleton_code.index(';') + 1
        modified_service_skeleton = service_skeleton_code[:package_line_end] + '\n\n'
        
        
        # * entity class에 대한 import 문을 추가합니다.
        for entity_name in entity_name_list:
            modified_service_skeleton += f"import com.example.demo.entity.{entity_name};\n"


        # * repository interface의 대한 import 문을 추가합니다.
        for entity_name in entity_name_list:
            modified_service_skeleton += f"import com.example.demo.repository.{entity_name}Repository;\n"


        # * 나머지 코드를 추가합니다.
        modified_service_skeleton += service_skeleton_code[package_line_end:]


        # * 리포지토리 주입 코드를 생성합니다.
        repository_injection_code = ""
        for entity_name in entity_name_list:
            camel_case_name = entity_name[0].lower() + entity_name[1:]
            repository_injection_code += f"    @Autowired\n    private {entity_name}Repository {camel_case_name}Repository;\n\n"


        # * 생성된 리포지토리 주입 코드를 서비스 클래스에 삽입합니다.
        if "CodePlaceHolder1" in modified_service_skeleton:
            modified_service_skeleton = modified_service_skeleton.replace("CodePlaceHolder1", repository_injection_code.rstrip())

        service_skeleton_code = modified_service_skeleton


        # * 서비스 클래스의 기본 틀
        summarzied_service_skeleton = f"""
@RestController
@Transactional
public class {service_skeleton_name} {{

    @PostMapping(path="/Endpoint")
    public ResponseEntity<String> methodName(@RequestBody {command_class_name} {command_class_name}Dto) {{
        //Here is business logic

        return ResponseEntity.ok("Operation completed successfully");
    }}
}}
"""    
        return summarzied_service_skeleton, service_skeleton_code
    
    except Exception:
        err_msg = "서비스 골격 클래스를 생성하는 과정에서 결과를 이용하여, 추가적인 정보를 설정하는 도중 문제가 발생했습니다."
        logging.error(err_msg, exc_info=False)
        raise HandleResultError(err_msg)
    


# 역할: 서비스 및 커맨드 파일로 저장합니다.
# 매개변수: 
#   - input_data : 프로시저 노드 데이터
#   - variable_data : 선언 노드 데이터
#   - entity_name_list : 엔티티 이름 리스트
#   - object_name : 패키지 및 프로시저 이름
# 반환값: 
#   - command_class_variable : Command 클래스에 선언된 변수 목록
#   - service_skeleton_code: 서비스 골격 클래스 완성본
#   - service_skeleton_name: 서비스 클래스 이름
#   - summarzied_service_skeleton: 요약된 서비스 골격 클래스
async def create_service_skeleton(input_data, variable_data, entity_name_list, object_name):
    
    try:
        # * LLM을 사용하여 Command 클래스 생성에 필요한 정보를 받습니다.
        analysis_command = convert_command_code(input_data, object_name)  
        command_class_name = analysis_command['commandName']
        command_class_code = analysis_command['command']
        command_class_variable = analysis_command['command_class_variable']

        # * LLM을 사용하여 서비스 골격 클래스 생성에 필요한 정보를 받습니다.
        analysis_service_skeleton = convert_service_skeleton_code(variable_data, command_class_name)  
        service_skeleton_name = analysis_service_skeleton['serviceName']
        service_skeleton_code = analysis_service_skeleton['service']
        

        # * 서비스 골격 클래스에 추가적인 정보를 추가하는 작업을 진행합니다.
        summarzied_service_skeleton, modified_service_skeleton = await modify_service_skeleton(service_skeleton_code, entity_name_list, service_skeleton_name, command_class_name)


        # * command 클래스 파일을 저장할 디렉토리를 설정하고, 없으면 생성합니다.
        base_directory = os.getenv('DOCKER_COMPOSE_CONTEXT')
        if base_directory:
            command_class_directory = os.path.join(base_directory, 'java', 'demo', 'src', 'main', 'java', 'com', 'example', 'demo','command')
        else:
            current_dir = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
            command_class_directory = os.path.join(current_dir, 'target', 'java', 'demo', 'src', 'main', 'java', 'com', 'example', 'demo', 'command')        
        os.makedirs(command_class_directory, exist_ok=True) 


        # * 커맨드 클래스를 파일로 저장합니다.
        command_class_path = os.path.join(command_class_directory, f"{command_class_name}.java")  
        async with aiofiles.open(command_class_path, 'w', encoding='utf-8') as file:  
            await file.write(command_class_code)  

        return modified_service_skeleton, command_class_variable, service_skeleton_name, summarzied_service_skeleton

    except (LLMCallError, HandleResultError):
        raise
    except Exception:
        err_msg = "서비스 골격 클래스를 생성하는 과정에서 결과 처리 준비 처리를 하는 도중 문제가 발생했습니다."
        logging.error(err_msg, exc_info=False)
        raise ProcessResultError(err_msg)


# 역할: Neo4j에서 프로시저, Declare 노드를 가져와서, 서비스 골격 클래스 생성을 준비하는 함수
# 매개변수: 
#      - object_name : 프로시저, 패키지 이름
#      - entity_name_list : 엔티티 이름 리스트
# 반환값: 
#   - command_class_variable : Command 클래스에 선언된 변수 목록
#   - service_skeleton_code: 서비스 골격 클래스 완성본
#   - service_skeleton_name: 서비스 클래스 이름
#   - summarzied_service_skeleton: 요약된 서비스 골격 클래스
async def start_service_skeleton_processing(entity_name_list, object_name):
    
    connection = Neo4jConnection()  
    logging.info(f"[{object_name}] 서비스 틀 생성을 시작합니다.")

    try:

        # * Neo4j 데이터베이스에서 프로시저, 패키지 입력 매개변수와 단순 지역 변수 노드를 검색하는 쿼리를 실행합니다.
        query = [
            f"MATCH (v:Variable {{object_name: '{object_name}'}}) WHERE NOT EXISTS ((v)-[:SCOPE]-(:DECLARE)) RETURN DISTINCT v",
            f"MATCH (d:DECLARE {{object_name: '{object_name}'}})-[:SCOPE]-(v:Variable {{object_name: '{object_name}'}}) RETURN DISTINCT v"
        ]  
        procedure_declare_nodes = await connection.execute_queries(query)  
        transformed_variable_data = [] 
        transformed_input_data = [] 
        

        # * Neo4j로 부터 전달받은 프로시저 노드의 데이터의 구조를 사용하기 쉽게 변경합니다.
        for item in procedure_declare_nodes[0]:
            transformed_node = {
                'type': item['v']['type'],
                'name': item['v']['name'],
                'object_name': item['v']['object_name']
            }
            transformed_input_data.append(transformed_node)


        # * Neo4j로 부터 전달받은 Declare노드의 데이터의 구조를 사용하기 쉽게 변경합니다.
        for item in procedure_declare_nodes[1]:
            transformed_node = {
                'type': item['v']['type'],
                'name': item['v']['name'],
                'object_name': item['v']['object_name']
            }
            transformed_variable_data.append(transformed_node) 
                

        # * 변환된 데이터를 사용하여 토큰 계산 및 서비스 스켈레톤 생성을 수행합니다.
        service_skeleton, command_class_variable, service_skeleton_name, summarzied_service_skeleton = await calculate_tokens_and_process(transformed_input_data, transformed_variable_data, entity_name_list, object_name)  
        logging.info(f"[{object_name}] 커맨드 클래스 및 서비스 골격을 생성했습니다.\n") 
        return service_skeleton, command_class_variable, service_skeleton_name, summarzied_service_skeleton
    
    except (ConvertingError, Neo4jError):
        raise
    except Exception:
        err_msg = "서비스 골격 클래스를 생성하기 위해 데이터를 준비하는 도중 문제가 발생했습니다."
        logging.error(err_msg, exc_info=False)
        raise SkeletonCreationError(err_msg)
    finally:
        await connection.close()
