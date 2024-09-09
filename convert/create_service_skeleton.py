import json
import os
import logging
import aiofiles
import tiktoken
from prompt.service_skeleton_prompt import convert_service_skeleton_code
from prompt.command_prompt import convert_command_code
from understand.neo4j_connection import Neo4jConnection


# * 인코더 설정 및 파일 이름 초기화
encoder = tiktoken.get_encoding("cl100k_base")


# 역할: 프로시저 노드 코드에서 입력 매개변수 부분만 추출하는 메서드입니다.
# 매개변수: 
#      - procedure_code : 프로시저 노드 부분의 스토어드 프로시저 코드.
# 반환값: 프로시저 노드 코드에서 변수 선언 부분만 필터링된 코드.
def extract_procedure_variable_code(procedure_code):
    try:

        # * AS 키워드를 찾아서 AS 이후 모든 라인을 제거합니다.
        as_index = procedure_code.find(' AS')
        if as_index != -1:
            newline_after_as = procedure_code.find('\n', as_index)
            procedure_code = procedure_code[:newline_after_as]

        return procedure_code

    except Exception:
        logging.exception("Error during code placeholder removal(understanding)")
        raise


# 역할: 프로시저 노드의 토큰 개수를 체크하여, 처리하는 함수입니다.
# 매개변수: 
#      - procedure_data : 프로시저 노드 데이터
#      - lower_name : 소문자 프로젝트 이름
#      - entity_name_list : 엔티티 이름 리스트
#      - repository_interface_names : 리포지토리 인터페이스 이름 리스트
# 반환값: 
#      - service_skeleton_code : 서비스 스켈레톤 클래스 코드 
#      - command_class_variable : 프로시저의 입력 매개변수(Command 클래스에 선언된 변수 목록)
async def calculate_tokens_and_process(procedure_data, declare_data, lower_name, entity_name_list, repository_interface_names):

    try:
        service_skeleton_code, command_class_variable, service_class_name, service_skeleton_summarzied = await create_service_skeleton(procedure_data, declare_data, lower_name, entity_name_list, repository_interface_names)
        return service_skeleton_code, command_class_variable, service_class_name, service_skeleton_summarzied 
     
    except Exception:
        logging.exception(f"Error occurred while procedure node token check")
        raise


# 역할: LLM을 사용하여 주어진 프로시저 데이터 그룹을 분석하고, 결과를 서비스 및 커맨드 파일로 저장합니다.
# 매개변수: 
#      - procedure_data : 분석할 프로시저 데이터 그룹
#      - lower_name : 소문자 프로젝트 이름
#      - entity_name_list : 엔티티 이름 리스트
#      - repository_interface_names : 리포지토리 인터페이스 이름 리스트
# 반환값: 
#      - service : 서비스 스켈레톤 클래스 코드
#      - command_class_variable : 프로시저의 입력 매개변수(Command 클래스에 선언된 변수 목록)
async def create_service_skeleton(procedure_data, declare_data, lower_name, entity_name_list, repository_interface_names):
    
    try:
        # * LLM을 사용하여 주어진 데이터를 분석하고 받은 결과에서 정보를 추출합니다
        analysis_command = convert_command_code(procedure_data, lower_name)  
        command_class_name = analysis_command['commandName']
        command_class_code = analysis_command['command']
        command_class_variable = analysis_command['command_class_variable']

        analysis_service_skeleton = convert_service_skeleton_code(declare_data, lower_name, command_class_name)  
        service_class_name = analysis_service_skeleton['serviceName']
        service_class_code = analysis_service_skeleton['service']
        repository_injection_code = ""
        

        # * package 선언 다음에 줄바꿈을 추가하고 entity import 문을 삽입합니다.
        package_line_end = service_class_code.index(';') + 1
        modified_service_code = service_class_code[:package_line_end] + '\n\n'


        # * entity_name_list의 각 항목에 대해 import 문을 추가합니다.
        for entity_name in entity_name_list:
            modified_service_code += f"import com.example.{lower_name}.entity.{entity_name};\n"


        # * repository_interface_names의 각 항목에 대해 import 문을 추가합니다.
        for repo_interface in repository_interface_names.keys():
            modified_service_code += f"import com.example.{lower_name}.repository.{repo_interface}Repository;\n"


        # * import 문과 클래스 선언 사이에 빈 줄 추가
        modified_service_code += '\n'  


        # * 나머지 코드를 추가합니다.
        modified_service_code += service_class_code[package_line_end:]


        # * 리포지토리 주입 코드를 생성합니다.
        repository_injection_code = ""
        for pascal_name, camel_name in repository_interface_names.items():
            repository_injection_code += f"    @Autowired\n    private {pascal_name}Repository {camel_name}Repository;\n\n"


        # * 생성된 리포지토리 주입 코드를 서비스 클래스에 삽입합니다.
        if "CodePlaceHolder1" in modified_service_code:
            modified_service_code = modified_service_code.replace("CodePlaceHolder1", repository_injection_code.rstrip())


        service_class_code = modified_service_code


        service_skeleton_summarzied = f"""
@RestController
@Transactional
public class {service_class_name} {{

    @PostMapping(path="/Endpoint")
    public ResponseEntity<String> methodName(@RequestBody {command_class_name} {command_class_name}Dto) {{
        //Here is business logic

        return ResponseEntity.ok("Operation completed successfully");
    }}
}}
"""


        # * command 클래스 파일을 저장할 디렉토리를 설정하고, 없으면 생성합니다.
        logging.info("\nSuccess RqRs LLM\n")  
        base_directory = os.getenv('DOCKER_COMPOSE_CONTEXT', 'convert')
        command_class_directory = os.path.join(base_directory, 'converting_result', f'{lower_name}', 'src', 'main', 'java', 'com', 'example', f'{lower_name}','command')
        os.makedirs(command_class_directory, exist_ok=True) 


        # * 커맨드 코드를 파일로 저장합니다.
        command_class_path = os.path.join(command_class_directory, f"{command_class_name}.java")  
        async with aiofiles.open(command_class_path, 'w', encoding='utf-8') as file:  
            await file.write(command_class_code)  
            logging.info(f"\nSuccess Create {command_class_name} Java File\n") 

        return service_class_code, command_class_variable, service_class_name, service_skeleton_summarzied

    except Exception:
        logging.exception(f"Error occurred while create service skeleton and command")
        raise


# 역할: Neo4j 데이터베이스에서 프로시저, Declare 노드를 가져와서 JSON의 구조를 단순하게 변경하는 함수입니다.
# 매개변수: 
#      - lower_name : 소문자 프로젝트 이름
#      - entity_name_list : 엔티티 이름 리스트
#      - repository_interface_names : 리포지토리 인터페이스 이름 리스트
# 반환값: 
#      - service : 서비스 스켈레톤 클래스 코드
#      - command_class_variable : 프로시저의 입력 매개변수(Command 클래스에 선언된 변수 목록)
async def start_service_skeleton_processing(lower_name, entity_name_list, repository_interface_names):
    
    try:
        connection = Neo4jConnection()  

        # * Neo4j 데이터베이스에서 프로시저, Declare 노드를 검색하는 쿼리를 실행합니다.
        query = ['MATCH (n:CREATE_PROCEDURE_BODY) RETURN n', 'MATCH (n:DECLARE) RETURN n']  
        procedure_declare_nodes = await connection.execute_queries(query)  
        logging.info("\nSuccess Received Procedure, Declare Nodes from Neo4J\n")  
        transformed_declare_data = [] 
        transformed_procedure_data = [] 
        

        # * Neo4j로 부터 전달받은 프로시저 노드의 데이터의 구조를 사용하기 쉽게 변경합니다.
        for item in procedure_declare_nodes[0]:
            transformed_node = {
                'type': 'procedure',
                'code': extract_procedure_variable_code(item['n']['summarized_code'])
            }
            transformed_procedure_data.append(transformed_node)  


        # * Neo4j로 부터 전달받은 Declare노드의 데이터의 구조를 사용하기 쉽게 변경합니다.
        for item in procedure_declare_nodes[1]:
            transformed_node = {
                'type': 'declare',
                'code': item['n']['node_code']
            }
            transformed_declare_data.append(transformed_node)   
                

        # * 변환된 데이터를 사용하여 토큰 계산 및 서비스 스켈레톤 생성을 수행합니다.
        logging.info("\nSuccess Transformed Procedure, Declare Nodes Data\n")  
        service_skeleton, command_class_variable, service_class_name, service_skeleton_summarzied = await calculate_tokens_and_process(transformed_procedure_data, transformed_declare_data, lower_name, entity_name_list, repository_interface_names)  
        return service_skeleton, command_class_variable, service_class_name, service_skeleton_summarzied
    
    except Exception:
        logging.exception(f"Error occurred while bring procedure node from neo4j")
        raise
    finally:
        await connection.close()  