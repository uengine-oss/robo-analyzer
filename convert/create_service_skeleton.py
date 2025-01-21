import os
import logging
import textwrap
import tiktoken
from prompt.convert_variable_prompt import convert_variables
from prompt.convert_service_skeleton_prompt import convert_method_code
from prompt.convert_command_prompt import convert_command_code
from semantic.vectorizer import vectorize_text
from understand.neo4j_connection import Neo4jConnection
from util.exception import ConvertingError, ExtractNodeInfoError, FilePathError, Neo4jError, ProcessResultError, SaveFileError, SkeletonCreationError, StringConversionError, TemplateGenerationError
from util.file_utils import save_file
from util.string_utils import convert_to_camel_case, convert_to_pascal_case

encoder = tiktoken.get_encoding("cl100k_base")
JAVA_PATH = 'demo/src/main/java/com/example/demo'


# 역할: 스프링부트 서비스 클래스의 기본 골격을 생성합니다.
#
# 매개변수: 
#   - object_name: 서비스 클래스명의 기반이 될 패키지 이름 (스네이크 케이스)
#   - entity_name_list: 서비스에서 사용할 엔티티 클래스명 목록
#   - converted_global_variables: 클래스 필드로 변환된 전역 변수 목록
#   - dir_name: 서비스 클래스가 저장될 디렉토리 이름
#   - external_call_package_names: 외부 호출된 패키지 이름 목록
#   - exist_command_class: 커맨드 클래스가 존재하는지 여부
#
# 반환값: 
#   - service_class_template: 생성된 서비스 클래스 코드 문자열
#   - service_class_name: 생성된 서비스 클래스명 (예: EmployeeManagementService)
async def generate_service_skeleton(object_name: str, entity_name_list: list, converted_global_variables: list, dir_name: str, external_call_package_names: list, exist_command_class: bool) -> str:
    try:
        global_fields = []
        sections = []

        # * 서비스 클래스명 생성 및 전역 변수를 클래스 필드로 전환 
        service_class_name = convert_to_pascal_case(object_name) + "Service"


        # * 글로벌 변수를 클래스 필드로 변환
        if converted_global_variables:
            for var in converted_global_variables["variables"]:
                field = f"    private {var['javaType']} {var['javaName']} = {var['value']};"
                global_fields.append(field)


        # * Autowired 주입 로직 및 필드 생성
        if global_fields:
            sections.append('\n'.join(global_fields))
        if entity_name_list:
            sections.append('\n'.join(f'    @Autowired\n    private {entity}Repository {entity[0].lower()}{entity[1:]}Repository;' for entity in entity_name_list))
        if external_call_package_names:
            sections.append('\n'.join(f'    @Autowired\n    private {convert_to_pascal_case(package_name)}Service {convert_to_camel_case(package_name)}Service;' for package_name in external_call_package_names))


        # * 섹션들을 하나의 줄바꿈으로 연결하고, strip()으로 양쪽 공백 제거
        class_content = "\n\n".join(sections).strip()
        

        # * 파라미터가 있는 경우 커맨드 패키지 임포트 추가
        command_import = f"import com.example.demo.command.{dir_name}.*;\n" if exist_command_class else ""


        # * 서비스 클래스 템플릿 생성
        service_class_template = f"""package com.example.demo.service;

{chr(10).join(f'import com.example.demo.entity.{entity};' for entity in entity_name_list)}
{chr(10).join(f'import com.example.demo.repository.{entity}Repository;' for entity in entity_name_list)}
{command_import}import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RestController;
import org.springframework.beans.factory.annotation.Autowired;
import jakarta.persistence.EntityNotFoundException;
import org.springframework.http.ResponseEntity;
import org.springframework.transaction.annotation.Transactional;
import org.springframework.beans.BeanUtils;
import java.time.format.DateTimeFormatter;
import org.springframework.stereotype.Service;
import java.time.temporal.TemporalAdjusters;
import java.time.*;
import java.util.*;

@Transactional
@Service
public class {service_class_name} {{
    {class_content}

CodePlaceHolder
}}"""

        return service_class_template, service_class_name
    
    except StringConversionError:
        raise
    except Exception as e:
        err_msg = f"서비스 클래스 골격을 생성하는 도중 문제가 발생했습니다: {str(e)}"
        logging.error(err_msg)
        raise TemplateGenerationError(err_msg)


# 역할: 프로시저/함수에 대한 커맨드 클래스와 서비스 메서드의 구현 코드를 생성합니다.
#
# 매개변수: 
#   - method_skeleton_data: 메서드 생성에 필요한 상세 정보
#   - parameter_data: 입력 매개변수 관련 정보
#   - node_type: 대상 노드의 유형
#   - dir_name: 커맨드 클래스가 저장될 디렉토리 이름
#   - user_id : 사용자 ID
#   - connection: Neo4j 연결 객체
#   - object_name: 프로시저 노드의 객체 이름
#
# 반환값: 
#   - command_class_variable: 커맨드 클래스에 정의된 필드 정보 (함수인 경우 None)
#   - command_class_name: 생성된 커맨드 클래스명 (함수인 경우 None)
#   - method_skeleton_name: 생성된 서비스 메서드명
#   - method_skeleton_code: 생성된 메서드 구현 코드
#   - method_signature: 생성된 메서드 시그니처
async def process_method_and_command_code(method_skeleton_data: dict, parameter_data: dict, node_type: str, dir_name: str, user_id: str, connection: Neo4jConnection, object_name: str) -> tuple:
    command_class_variable = None
    command_class_name = None
    
    try:
        # * 파라미터가 있는 프로시저인 경우 커맨드 클래스 생성
        if node_type != 'FUNCTION' and parameter_data['parameters']:


            # * 커맨드 클래스를 생성하는 프롬프트 호출
            analysis_command = convert_command_code(parameter_data, dir_name)  
            command_class_name = analysis_command['commandName']
            command_class_code = analysis_command['command']
            command_class_variable = analysis_command['command_class_variable']     
            command_summary = analysis_command['summary']
            command_summary_vector = vectorize_text(command_summary)
            
            
            # * 커맨드 클래스 정보를 노드에 저장
            command_query = [
                f"""
                MATCH (p)
                WHERE (p:PROCEDURE OR p:CREATE_PROCEDURE_BODY)
                AND p.procedure_name = '{method_skeleton_data['procedure_name']}'
                AND p.user_id = '{user_id}'
                AND p.object_name = '{object_name}'
                MERGE (cmd:COMMAND {{
                    name: '{command_class_name}',
                    user_id: '{user_id}',
                    object_name: '{object_name}',
                    procedure_name: '{method_skeleton_data['procedure_name']}'
                }})
                SET cmd.java_code = '{command_class_code}',
                    cmd.summary = '{command_summary}'
                MERGE (p)-[:CONVERT]->(cmd)
                RETURN cmd
                """,
                
                f"""
                MATCH (cmd:COMMAND {{
                    name: '{command_class_name}',
                    user_id: '{user_id}',
                    object_name: '{object_name}',
                    procedure_name: '{method_skeleton_data['procedure_name']}'
                }})
                SET cmd.summary_vector = {command_summary_vector.tolist()}
                """
            ]
            await connection.execute_queries(command_query)
            

            # * 커맨드 클래스 파일 생성
            await generate_command_class(command_class_name, command_class_code, dir_name, user_id)


        # * 메서드 틀 생성에 필요한 정보를 받습니다.
        analysis_method = convert_method_code(method_skeleton_data, parameter_data)  
        method_skeleton_name = analysis_method['methodName']
        method_skeleton_code = analysis_method['method']
        method_signature = analysis_method['methodSignature']


        return (
            command_class_variable,
            command_class_name,
            method_skeleton_name,
            method_skeleton_code,
            method_signature
        )
    
    except ConvertingError:
        raise
    except Exception as e:
        err_msg = f"메서드 틀을 생성하는 과정에서 결과 처리 준비 처리를 하는 도중 문제가 발생했습니다: {str(e)}"
        logging.error(err_msg)
        raise ProcessResultError(err_msg)


# 역할: 생성된 자바 소스 코드를 지정된 디렉토리에 파일로 저장합니다.
#
# 매개변수: 
#   - class_name: 저장할 자바 클래스명 (확장자 제외)
#   - source_code: 저장할 자바 소스 코드 내용
#   - dir_name: 커맨드 클래스가 저장될 디렉토리 이름
#   - user_id : 사용자 ID
async def generate_command_class(class_name: str, source_code: str, dir_name: str, user_id: str) -> None:

    try:
        # * 저장 경로 설정
        if os.getenv('DOCKER_COMPOSE_CONTEXT'):
            base_path = os.path.join(os.getenv('DOCKER_COMPOSE_CONTEXT'), 'target', 'java', user_id, JAVA_PATH, 'command', dir_name)
        else:
            parent_workspace_dir = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
            base_path = os.path.join(parent_workspace_dir, 'target', 'java', user_id, JAVA_PATH, 'command', dir_name)


        # * 커맨드 클래스 파일로 생성합니다.
        await save_file(content=source_code, filename=f"{class_name}.java", base_path=base_path)

    except SaveFileError:
        raise
    except Exception as e:
        err_msg = f"커맨드 클래스 [{class_name}] 파일 저장을 위한 경로 설정중 오류가 발생했습니다: {str(e)}"
        logging.error(err_msg)
        raise FilePathError(err_msg)
    

# 역할: 프로시저 노드와 외부 호출 노드를 조회하여 프로시저 그룹을 구성합니다.
#
# 매개변수: 
#   - connection: Neo4j 연결 객체
#   - object_name: 프로시저 노드의 객체 이름
#
# 반환값: 
#   - procedure_groups: 프로시저 그룹 데이터
#   - external_packages: 외부 호출된 패키지 이름 목록
async def get_procedure_groups(connection: Neo4jConnection, object_name: str) -> tuple[dict, list]:
    try:
        query = [
            # * 프로시저 노드 쿼리
            f"""MATCH (p)
            WHERE (p:PROCEDURE OR p:CREATE_PROCEDURE_BODY OR p:FUNCTION)
            AND p.object_name = '{object_name}'
            OPTIONAL MATCH (p)-[:PARENT_OF]->(d:DECLARE)
            WHERE d.object_name = '{object_name}'
            OPTIONAL MATCH (d)-[:SCOPE]-(dv:Variable)
            WHERE dv.object_name = '{object_name}'
            OPTIONAL MATCH (p)-[:PARENT_OF]->(s:SPEC)
            WHERE s.object_name = '{object_name}'
            OPTIONAL MATCH (s)-[:SCOPE]-(sv:Variable)
            WHERE sv.object_name = '{object_name}'
            WITH p, d, dv, s, sv, CASE 
                WHEN p:FUNCTION THEN 'FUNCTION'
                WHEN p:PROCEDURE THEN 'PROCEDURE'
                WHEN p:CREATE_PROCEDURE_BODY THEN 'CREATE_PROCEDURE_BODY'
            END as node_type
            RETURN p, d, dv, s, sv, node_type
            ORDER BY p.startLine""",
            # * 외부 호출 노드 쿼리
            f"""MATCH (p)-[:EXT_CALL]->(ext)
            WHERE p.object_name = '{object_name}'
            WITH DISTINCT ext.object_name as obj_name, COLLECT(ext)[0] as ext
            RETURN ext"""
        ]
        

        # * 쿼리 실행 및 결과 할당
        procedure_nodes, external_call_nodes = await connection.execute_queries(query)
        
        
        # * 프로시저 데이터 구조화
        procedure_groups = {}
        for item in procedure_nodes:
            proc_name = item['p'].get('procedure_name', '')
            
            # * 새 프로시저 초기화
            if proc_name not in procedure_groups:
                procedure_groups[proc_name] = {
                    'parameters': [],
                    'local_variables': [],
                    'declaration': item['s'].get('node_code', ''),
                    'node_type': item['node_type']
                }
            
            # * 파라미터 추가
            if item['sv']:
                new_param = {
                    'type': item['sv']['type'],
                    'name': item['sv']['name'],
                    'parameter_type': item['sv'].get('parameter_type', '')  # parameter_type 추가
                }
                if new_param not in procedure_groups[proc_name]['parameters']:
                    procedure_groups[proc_name]['parameters'].append(new_param)
            
            # * 로컬 변수 추가
            if item['dv']:
                new_var = {
                    'type': item['dv']['type'],
                    'name': item['dv']['name'],
                    'value': item['dv']['value']
                }
                if new_var not in procedure_groups[proc_name]['local_variables']:
                    procedure_groups[proc_name]['local_variables'].append(new_var)
        
        # * 외부 패키지 목록 추출
        external_packages = [node['ext']['object_name'] for node in external_call_nodes]
        
        return procedure_groups, external_packages
    
    except Neo4jError:
        raise
    except Exception as e:
        err_msg = f"[{object_name}] 프로시저 데이터 조회 중 오류 발생: {str(e)}"
        logging.error(err_msg)
        raise ExtractNodeInfoError(err_msg)


# 역할: Neo4j 데이터베이스에서 프로시저 정보를 조회하여 서비스 클래스와 관련 코드를 생성합니다.
#
# 매개변수: 
#   - required_entities: 서비스에서 사용할 엔티티 클래스명 목록
#   - service_base_name: 서비스 클래스명의 기반이 될 객체 이름
#   - global_variables: 전역 변수 목록
#   - user_id : 사용자 ID
#
# 반환값: 
#   - service_components: 생성된 서비스 구성요소 목록 (커맨드 클래스, 메서드 등)
#   - service_template: 서비스 클래스의 기본 템플릿 코드
#   - service_class_name: 생성된 서비스 클래스명
#   - exist_command_class: 커맨드 클래스가 존재하는지 여부
async def start_service_skeleton_processing(entity_name_list: list, object_name: str, global_variables: list, user_id: str) -> tuple[list, str, str, bool]:

    connection = Neo4jConnection()
    dir_name = convert_to_camel_case(object_name)
    method_info_list = []
    
    logging.info(f"[{object_name}] 서비스 틀 생성을 시작합니다.")

    try:
        # * 프로시저 데이터 조회 및 구조화
        procedure_groups, external_packages = await get_procedure_groups(connection, object_name)
        exist_command_class = any(group['parameters'] for group in procedure_groups.values())
        

        # * 전역 변수 변환
        convert_global_variables = convert_variables(global_variables)
        

        # * 서비스 스켈레톤 생성
        service_skeleton, service_class_name = await generate_service_skeleton(
            object_name,
            entity_name_list,
            convert_global_variables,
            dir_name,
            external_packages,
            exist_command_class,
        )

        # * 각 프로시저별 메서드 생성
        for proc_name, proc_data in procedure_groups.items():
            logging.info(f"[{object_name}] {proc_name} 메서드 생성 시작")
            

            # * 메서드 생성 데이터 준비
            method_skeleton_data = {
                'procedure_name': proc_name,
                'local_variables': proc_data['local_variables'],
                'declaration': proc_data['declaration']
            }
            
            # * 파라미터 데이터 준비
            parameter_data = {
                'parameters': proc_data['parameters'],
                'procedure_name': proc_name
            }

            # * 메서드와 커맨드 클래스 생성
            command_class_variable, command_class_name, method_skeleton_name, method_skeleton_code, method_signature = await process_method_and_command_code(
                method_skeleton_data,
                parameter_data,
                proc_data['node_type'],
                dir_name,
                user_id,
                connection,
                object_name
            )


            # * 메서드 코드 포맷팅
            method_skeleton_code = textwrap.indent(method_skeleton_code, '    ')
            service_method_skeleton = service_skeleton.replace("CodePlaceHolder", method_skeleton_code)
            

            # * 메서드 정보 저장
            method_info_list.append({
                'command_class_variable': command_class_variable,
                'command_class_name': command_class_name,
                'method_skeleton_name': method_skeleton_name,
                'method_skeleton_code': method_skeleton_code,
                'method_signature': method_signature,
                'service_method_skeleton': service_method_skeleton,
                'node_type': proc_data['node_type'],
                'procedure_name': proc_name
            })
            logging.info(f"[{object_name}] {proc_name} 메서드 생성 완료")

        logging.info(f"[{object_name}] 메서드 생성 완료\n")
        return method_info_list, service_skeleton, service_class_name, exist_command_class


    except ConvertingError:
        raise
    except Exception as e:
        err_msg = f"[{object_name}] 서비스 클래스 생성 중 오류 발생: {str(e)}"
        logging.error(err_msg)
        raise SkeletonCreationError(err_msg)
    finally:
        await connection.close()