import os
import logging
import aiofiles
import tiktoken
from prompt.service_skeleton_prompt import convert_function_code, convert_method_skeleton_code
from prompt.command_prompt import convert_command_code
from understand.neo4j_connection import Neo4jConnection
from util.exception import ConvertingError, ExtractCodeError, HandleResultError, LLMCallError, Neo4jError, ProcessResultError, SaveFileError, SkeletonCreationError, TraverseCodeError

encoder = tiktoken.get_encoding("cl100k_base")


# 역할: 스네이크 케이스를 파스칼 케이스로 변환합니다.
# 매개변수: 
#   - snake_str: 변환할 스네이크 케이스 문자열 (예: employee_payroll)
# 반환값: 
#   - 파스칼 케이스로 변환된 문자열 (예: EmployeePayroll)
def convert_to_pascal_case(snake_str: str) -> str:
    return ''.join(word.capitalize() for word in snake_str.split('_'))


# 역할: 스프링부트 서비스 클래스의 기본 골격을 생성합니다.
# 매개변수: 
#   - object_name: 서비스 클래스명의 기반이 될 객체 이름 (스네이크 케이스)
#   - entity_name_list: 서비스에서 사용할 엔티티 이름 목록
# 반환값: 
#   - 생성된 서비스 클래스 코드 문자열
async def create_service_skeleton(object_name: str, entity_name_list: list) -> str:
    try:
        # * 1. 파스칼 케이스로 변환하여 서비스 클래스명 생성 
        service_name = convert_to_pascal_case(object_name) + "Service"
        
        # * 2. 서비스 클래스 템플릿 생성
        template = f"""package com.example.testjava.service;

{chr(10).join(f'import com.example.testjava.entity.{entity};' for entity in entity_name_list)}
{chr(10).join(f'import com.example.testjava.repository.{entity}Repository;' for entity in entity_name_list)}
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RestController;
import org.springframework.beans.factory.annotation.Autowired;
import java.time.LocalDate;
import java.util.List;
import org.springframework.http.ResponseEntity;
import org.springframework.transaction.annotation.Transactional;

@RestController
@Transactional
public class {service_name} {{

{chr(10).join(f'    @Autowired\n    private {entity}Repository {entity[0].lower()}{entity[1:]}Repository;' for entity in entity_name_list)}

CodePlaceHolder1
}}"""

        return template

    except Exception:
        err_msg = "서비스 클래스 골격을 생성하는 도중 문제가 발생했습니다."
        logging.error(err_msg, exc_info=False)
        raise ExtractCodeError(err_msg)


# 역할: 커맨드 파일로 저장하고, 메서드(일반 or 컨트롤러) 틀 생성을 진행합니다.
# 매개변수: 
#   - method_skeleton_data : 메서드 틀 생성에 필요한 데이터
#   - parameter_data : 입력 매개변수 데이터
#   - entity_name_list : 엔티티 이름 리스트
#   - object_name : 패키지 및 프로시저 이름
#   - node_type : 노드 타입 (create_procedure_body, procedure, function)
# 반환값: 일단 없음
async def create_method_and_command(method_skeleton_data, parameter_data, object_name, node_type):
    
    try:
        if node_type != 'FUNCTION':
            # * 커맨드 클래스 생성에 필요한 정보를 받습니다.
            analysis_command = convert_command_code(parameter_data, object_name)  
            command_class_name = analysis_command['commandName']
            command_class_code = analysis_command['command']
            command_class_variable = analysis_command['command_class_variable']              

            # * 메서드 틀 생성에 필요한 정보를 받습니다.
            analysis_method_skeleton = convert_method_skeleton_code(method_skeleton_data, parameter_data, command_class_name, object_name)  
            method_skeleton_name = analysis_method_skeleton['methodName']
            method_skeleton_code = analysis_method_skeleton['method']
        else:
            analysis_function = convert_function_code(parameter_data, object_name)  
            method_skeleton_name = analysis_function['commandName']
            method_skeleton_code = analysis_function['command']


        # * command 클래스 파일을 저장할 디렉토리를 설정하고, 없으면 생성합니다.
        await save_java_file(command_class_name, command_class_code)

        return command_class_variable, method_skeleton_name, method_skeleton_code

    except (LLMCallError, HandleResultError):
        raise
    except Exception:
        err_msg = "메서드 틀을 생성하는 과정에서 결과 처리 준비 처리를 하는 도중 문제가 발생했습니다."
        logging.error(err_msg, exc_info=False)
        raise HandleResultError(err_msg)


# 역할: 생성된 Java 코드를 파일로 저장합니다.
# 매개변수: 
#   - file_name: 저장할 파일명 (확장자 제외)
#   - code: 저장할 Java 코드 내용
#   - sub_directory: 저장할 하위 디렉토리 이름 (기본값: 'command')
# 반환값: 없음
async def save_java_file(file_name: str, code: str, sub_directory: str = 'command') -> None:

    try:
        # * 1. 환경변수에 따른 기본 디렉토리 경로 설정
        base_directory = os.getenv('DOCKER_COMPOSE_CONTEXT')
        if base_directory:
            # * Docker 환경인 경우의 경로
            java_directory = os.path.join(base_directory, 'java', 'demo', 'src', 'main', 'java', 'com', 'example', 'demo', sub_directory)
        else:
            # * 로컬 환경인 경우의 경로
            current_dir = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
            java_directory = os.path.join(current_dir, 'target', 'java', 'demo', 'src', 'main', 'java', 'com', 'example', 'demo', sub_directory)
        
        # * 2. 저장 디렉토리가 없으면 생성
        os.makedirs(java_directory, exist_ok=True)
        
        # * 3. Java 파일 생성 및 코드 작성
        file_path = os.path.join(java_directory, f"{file_name}.java")
        async with aiofiles.open(file_path, 'w', encoding='utf-8') as file:
            await file.write(code)
            
    except Exception:
        err_msg = "커맨드 클래스 파일을 저장하는 도중 오류가 발생했습니다."
        logging.error(err_msg, exc_info=False)
        raise ProcessResultError(err_msg)


# 역할: 서비스 틀, 커맨드 클래스, 함수 틀(컨틀로러 메서드, 일반 메서드)를 생성하는 함수
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

        # TODO 프로시저 스펙에 있는 전역변수는 서비스 클래스의 필드로 되어야함
        # TODO 리턴 노드면 어떤 타입을 리턴하는지 정보가 필요함 현재는 node_code로 임시 대체 
        query = [
            # Command 생성과 Service 클래스 생성을 위한 노드를 가져옵니다.
            f"""
            MATCH (v:Variable)-[:SCOPE]-(p)
            WHERE (p:PROCEDURE OR p:CREATE_PROCEDURE_BODY OR p:FUNCTION)
            AND v.object_name = '{object_name}'
            AND p.object_name = '{object_name}'
            OPTIONAL MATCH (p)-[:PARENT_OF]->(d:DECLARE)
            WHERE d.object_name = '{object_name}'
            OPTIONAL MATCH (p)-[:PARENT_OF]->(r:RETURN)
            WHERE r.object_name = '{object_name}'
            RETURN v, p, d, r
            """
        ]

        nodes = await connection.execute_queries(query)  
        
        # * 프로시저별로 데이터 구조화
        procedure_groups = {}
        for item in nodes[0]:
            proc_name = item['p'].get('procedure_name', '')
            
            # * 새로운 프로시저 그룹 초기화
            if proc_name not in procedure_groups:
                procedure_groups[proc_name] = {
                    'parameters': [],  # 프로시저 입력 매개변수
                    'declaration': item['p'].get('declaration', ''),  # 프로시저 선언부
                    'local_variables': item['d'].get('node_code', '') if item['d'] else '',  # 지역변수
                    'return_code': item['r'].get('node_code', '') if item['r'] else '',  # return 노드 코드 추가
                    'node_type': list(item['p'].labels)[0]  # 노드 레이블 추가
                }
            
            # * 변수 정보 추가
            variable = {
                'type': item['v']['type'],
                'name': item['v']['name']
            }
            procedure_groups[proc_name]['parameters'].append(variable)


        # * 서비스 클래스의 틀을 생성합니다.
        service_skeleton = await create_service_skeleton(object_name, entity_name_list)
    

        # * 커맨드 클래스 및 메서드 틀 생성을 위한 데이터를 구성합니다.
        service_skeleton_list = []
        for proc_name, proc_data in procedure_groups.items():
            
            # * 메서드 틀 생성을 위한 데이터 구성
            service_skeleton_data = {
                'procedure_name': proc_name,
                'local_variables': proc_data['local_variables'],
                'return_code': proc_data['return_code'],
                'node_type': proc_data['node_type']
            }

            # * 커맨드 클래스 생성을 위한 데이터 구성
            parameter_data = {
                'parameters': proc_data['parameters'],
                'procedure_name': proc_name
            }

            # * 각 프로시저별 커맨드 클래스, 메서드 틀 생성을 진행합니다.
            service_skeleton, command_class_variable, method_skeleton_name, method_skeleton_code = await create_method_and_command(
                service_skeleton_data, 
                parameter_data, 
                entity_name_list, 
                object_name,
                proc_data['node_type']
            )

            # * 결과를 딕셔너리로 구성하여 리스트에 추가
            service_skeleton_list.append({
                'service_skeleton': service_skeleton,
                'command_class_variable': command_class_variable,
                'method_skeleton_name': method_skeleton_name,
                'method_skeleton_code': method_skeleton_code,
                'procedure_name': proc_name
            })

        logging.info(f"[{object_name}] {len(service_skeleton_list)}개의 커맨드 클래스 및 서비스 골격을 생성했습니다.\n")
        return service_skeleton_list, service_skeleton

    except (ConvertingError, Neo4jError, SaveFileError):
        raise
    except Exception:
        err_msg = "서비스 골격 클래스를 생성하기 위해 데이터를 준비하는 도중 문제가 발생했습니다."
        logging.error(err_msg, exc_info=False)
        raise SkeletonCreationError(err_msg)
    finally:
        await connection.close()
