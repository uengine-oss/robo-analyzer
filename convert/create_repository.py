import json
import os
import logging
import aiofiles
import tiktoken
from prompt.repository_prompt import convert_repository_code
from understand.neo4j_connection import Neo4jConnection
from util.exception import ConvertingError, LLMCallError, Neo4jError, ProcessResultError, RepositoryCreationError, TokenCountError, TraverseCodeError, VariableNodeError

encoder = tiktoken.get_encoding("cl100k_base")


# 역할: 전달된 스토어드 프로시저 코드의 토큰 길이를 계산합니다.
# 매개변수: 
#   - spCode : 스토어드 프로시저 코드
# 반환값: 
#   - len(spCode) : 스토어드 프로시저 코드의 토큰 수
def calculate_code_token(spCode):
    
    try:
        # * 데이터를 JSON 형식으로 인코딩하고, 인코딩된 데이터의 길이를 반환합니다.
        spCode_json = json.dumps(spCode, ensure_ascii=False)
        return len(encoder.encode(spCode_json))

    except Exception:
        err_msg = "리포지토리 인터페이스 생성 과정에서 노드 토큰 계산 도중 문제가 발생"
        logging.exception(err_msg)
        raise TokenCountError(err_msg)


# 역할: 여러 JPA 쿼리 메서드를 통합하여, 하나의 리포지토리 인터페이스로 생성합니다.
# 매개변수:
#   repository_code_list - 각 리포지토리 인터페이스의 코드가 담긴 리스트.
#   repository_pascal_name - 생성될 리포지토리 인터페이스의 이름 (PascalCase 형식).
#   repository_camel_name - 생성될 리포지토리 인터페이스의 이름 (camelCase 형식).
#   lower_file_name - 소문자로 구성된 프로젝트 이름
# 반환값: 없음
async def merge_jpa_query_method(repository_code_list, repository_pascal_name, repository_camel_name, lower_file_name):
    
    try:        
        # * 전달된 JPA 쿼리 메서드들의 들여쓰기 조정하여 하나의 문자열로 생성
        adjusted_methods = ['    ' + method.strip() for method in repository_code_list]
        merged_methods = '\n\n'.join(adjusted_methods)


        # * 리포지토리 인터페이스를 생성합니다.
        repository_interface = f"""package com.example.{lower_file_name}.repository;
import java.util.List;
import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.data.jpa.repository.Query;
import org.springframework.data.repository.query.Param;
import org.springframework.data.rest.core.annotation.RepositoryRestResource;
import com.example.{lower_file_name}.entity.{repository_pascal_name};
import java.time.LocalDate;

@RepositoryRestResource(collectionResourceRel = "{repository_camel_name}s", path = "{repository_camel_name}s")
public interface {repository_pascal_name}Repository extends JpaRepository<{repository_pascal_name}, Long> {{
{merged_methods}
}}
    """

        # * 리포지토리 인터페이스를 저장할 경로를 설정합니다
        base_directory = os.getenv('DOCKER_COMPOSE_CONTEXT', 'data')
        entity_directory = os.path.join(base_directory, 'java', f'{lower_file_name}', 'src', 'main', 'java', 'com', 'example', f'{lower_file_name}', 'repository')
        os.makedirs(entity_directory, exist_ok=True)


        # * 설정된 경로에 리포지토리 인터페이스를 생성합니다
        file_path = os.path.join(entity_directory, f"{repository_pascal_name}Repository.java")
        async with aiofiles.open(file_path, 'w', encoding='utf-8') as file:
            await file.write(repository_interface)
            logging.info("Success Create Java Repository Interface")

    except Exception:
        err_msg = "리포지토리 인터페이스 파일 쓰기 및 생성 도중 오류가 발생"
        logging.exception(err_msg)
        raise OSError(err_msg)


# 역할: 현재 노드에서 사용된 변수 노드를 neo4j에서 가져오는 함수 
# 매개변수: 
#   - node_id : 현재 노드 id
# 반환값: 
#   - variable_node_list : 변수에 대한 정보가 담긴 리스트
#   - variable_tokens : 변수 정보 리스트의 토큰 길이.
async def process_variable_nodes(node_id, connection):
    try:

        # * Neo4j 데이터베이스에서 모든 변수 노드를 가져옵니다
        query = ["MATCH (v:Variable) RETURN v"]
        all_variable_nodes = await connection.execute_queries(query) 


        # * node_id가 속성명의 범위 내에 있는 노드를 필터링합니다
        filtered_variable_info = []
        for node in all_variable_nodes[0]:
            for key, value in node['v'].items():
                if '_' in key:
                    start, end = map(int, key.split('_'))
                    if start <= node_id <= end:
                        filtered_variable_info.append({
                            'name': node['v']['name'],
                            'type': node['v'].get('type', 'Unknown'),
                            'role': f"{key} : {value}",
                        })                        
                        break


        # * 필터링된 노드 정보의 토큰을 계산합니다.
        variable_tokens = calculate_code_token(filtered_variable_info)
        return filtered_variable_info, variable_tokens
    
    except (TokenCountError, Neo4jError):
        raise
    except Exception:
        err_msg = "사용된 변수 노드를 추출하는 도중 오류가 발생했습니다."
        logging.exception(err_msg)
        raise VariableNodeError(err_msg)


# 역할: 테이블과 직접 연결된 노드의 정보의 토큰의 개수를 체크하고, 처리하는 함수
# 매개변수: 
#   - table_link_node : 테이블과 직접적으로 연결된 노드 리스트
#   - lower_file_name : 소문자로 구성된 프로젝트 이름
#   - connection : neo4j 연결 객체
# 반환값: 
#   - jpa_method_list : JPA 쿼리 메서드 리스트
#   -  repository_interface_names : JPA 쿼리 메서드 이름 리스트
async def check_tokens_and_process(table_link_node, connection, lower_file_name):
    total_tokens = 0                    # 전체 토큰 수
    variable_tokens = 0                 # llm에게 전달할 변수 정보의 토큰 수
    table_link_node_chunk = []          # llm에게 전달할 노드 데이터 모음
    variable_nodes_context = {}         # llm에게 전달할 변수 데이터 모음
    repository_code_list = []           # 리포지토리 코드를 모아두는 리스트 
    jpa_method_list = []                # JPA 쿼리 메서드 리스트
    repository_interface_names = {}     # JPA 쿼리 메서드의 이름 리스트
    current_table_name = None           # 현재 처리 중인 테이블의 이름을 추적
    pascal_name = None                  # 파스칼 표기법이 적용된 엔티티의 이름
    camel_name = None                   # 카멜 표기법이 적용된 엔티티의 이름
    process_append_flag = True          # 데이터 추가를 제어하는 플래그

    try:
        # * 테이블와 직접 연결된 노드를 순회하면서 토큰 수를 체크합니다
        for node in table_link_node:
            process_append_flag = True                    
            node_tokens = calculate_code_token(node)   
            

            # * 현재 처리중인 테이블 이름이 없는 경우, 새로 할당합니다
            if current_table_name is None:
                current_table_name = node['name']


            # * 테이블 이름이 변경되었는지 확인합니다(같은 테이블에 대한 처리가 끝났는지)
            if node['name'] != current_table_name:


                # * 현재까지 쌓아둔 데이터 그룹을 llm에게 전달해서 처리하기 위한 함수를 호출합니다
                repository_code, pascal_name, camel_name, method_list = await create_repository_interface(table_link_node_chunk, variable_nodes_context)
                repository_code_list.append(repository_code)
                jpa_method_list.append(method_list)
                repository_interface_names[pascal_name] = camel_name


                # * 한 테이블에 대한 처리가 끝났으니 최종적으로 하나의 리포지토리 인터페이스를 생성합니다 
                await merge_jpa_query_method(repository_code_list, pascal_name, camel_name, lower_file_name)


                # * 다음 사이클을 위해 각 변수를 초기화합니다
                repository_code_list.clear()
                current_table_name = node['name']  
                table_link_node_chunk = [] 
                variable_nodes_context.clear()
                total_tokens = 0  
                variable_tokens = 0


            # * 총 토큰 수가 1500을 넘었는지 확인합니다
            if total_tokens + node_tokens + variable_tokens >= 1300:
                
                # * table_link_node_chunk가 비어있으면서, 첫 시작 노드의 크기가 클 경우, 처리합니다
                if not table_link_node_chunk:     
                    table_link_node_chunk.append(node)
                    variable_node_list, variable_tokens = await process_variable_nodes(node['startLine'], connection) 
                    variable_nodes_context[f'variables_{node["startLine"]}'] = variable_node_list
                    process_append_flag = False  # 추가 데이터 처리 방지
                repository_code, pascal_name, camel_name, method_list = await create_repository_interface(table_link_node_chunk, variable_nodes_context)
                repository_code_list.append(repository_code)
                jpa_method_list.append(method_list)
                repository_interface_names[pascal_name] = camel_name


                # * 다음 사이클을 위해 각 변수를 초기화합니다
                table_link_node_chunk = []
                variable_nodes_context.clear()  
                total_tokens = 0  
                variable_tokens = 0
                
            
            # * 데이터 추가 플래그가 True인 경우, 현재 노드를 데이터 청크에 추가합니다
            if process_append_flag:    
                table_link_node_chunk.append(node)
                variable_node_list, variable_tokens = await process_variable_nodes(node['startLine'], connection)
                variable_nodes_context[f'variables_{node["startLine"]}'] = variable_node_list
                total_tokens += node_tokens + variable_tokens


        # * 마지막 데이터 그룹을 처리합니다
        if table_link_node_chunk:
            repository_code, pascal_name, camel_name, method_list = await create_repository_interface(table_link_node_chunk, variable_nodes_context)
            repository_code_list.append(repository_code)
            jpa_method_list.append(method_list)
            repository_interface_names[pascal_name] = camel_name
            await merge_jpa_query_method(repository_code_list, pascal_name, camel_name, lower_file_name)
        
        return jpa_method_list, repository_interface_names
    
    except (ConvertingError, OSError, Neo4jError):
        raise
    except Exception:
        err_msg = "Converting 과정에서 리포지토리 인터페이스 관련 정보를 순회하는 도중 문제가 발생했습니다."
        logging.exception(err_msg)
        raise TraverseCodeError(err_msg)


# 역할: LLM의 분석 결과를 바탕으로 리포지토리 인터페이스 정보를 얻는 함수
# 매개변수:
#   - node_data: 분석할 노드 데이터의 리스트
#   - variable_nodes_context: 변수 노드의 컨텍스트 정보를 포함하는 딕셔너리
# 반환값: 
#   - repository_code: 생성된 Java 리포지토리 인터페이스 코드.
#   - repository_pascal_name: 리포지토리의 인터페이스의 이름 (PascalCase 형식).
#   - repository_camel_name: 리포지토리 인터페이스의 이름 (camelCase 형식).
#   - method_list: 리포지토리 인터페이스에 선언된 메서드 리스트.
async def create_repository_interface(node_data, variable_nodes_context):
    
    try:
        # * LLM을 사용하여 주어진 노드 데이터를 분석하고, 나온 결과에서 필요한 정보를 추출합니다
        analysis_data = convert_repository_code(node_data, variable_nodes_context)    
        repository_code = analysis_data['jpaQueryMethod']
        repository_pascal_name = analysis_data['pascalName']
        repository_camel_name = analysis_data['camelName']
        method_list = analysis_data['methodList']
        return repository_code, repository_pascal_name, repository_camel_name, method_list
    
    except LLMCallError:
        raise
    except Exception:
        err_msg = "리포지토리 인터페이스 생성을 위한 LLM 결과 처리 도중 문제가 발생했습니다."
        logging.exception(err_msg)
        raise ProcessResultError(err_msg)


# 역할: 주어진 테이블 노드 리스트를 기반으로 1단계 깊이의 연결된 노드를 가져와서 Repository 생성을 준비합니다
# 매개변수: 
#   - table_node_list : 테이블 노드의 리스트
#   - lower_file_name : 소문자 프로젝트 이름
# 반환값: 
#   - jpa_method_list : JPA 쿼리 메서드 리스트
#   - repository_interface_names : 리포지토리 인터페이스 이름 리스트
async def start_repository_processing(table_node_list, lower_file_name):
    
    try:
        connection = Neo4jConnection()
        filtered_table_list = []


        # * 테이블 정보를 순회하면서 해당 테이블과 직접적으로 연결된 노드만 가져옵니다
        for node in table_node_list:
            node_name = node['name']       
            key_type = node['keyType']
            one_depth_query = [f"MATCH (n:Table {{name: '{node_name}'}})--(m) WHERE NOT m:Table AND NOT m:EXECUTE_IMMDDIATE RETURN m"]
            one_depth_nodes = await connection.execute_queries(one_depth_query)


            # * 만약 해당 테이블과 직접적으로 연결된 노드가 없을 경우 넘어갑니다 
            if not one_depth_nodes[0]:  
                logging.info(f"No connected nodes found for {node_name}, skipping.")
                continue 
            

            # * 복잡한 데이터의 구조를 좀 더 편리하게 재구성합니다 
            filtered_table_info = [{'name': node_name, 'startLine': m['m']['startLine'], 'endLine': m['m']['endLine'], 'PrimaryKeyType': key_type, 'code': m['m']['node_code']} for m in one_depth_nodes[0] if 'node_code' in m['m']]
            filtered_table_list.extend(filtered_table_info)


        # * 재구성된 노드 정보들을 담아서 처리 및 토큰 계산하는 함수를 호출합니다
        jpa_method_list, repository_interface_names = await check_tokens_and_process(filtered_table_list, connection, lower_file_name)
        logging.info("Success Create Repository\n")
        return jpa_method_list, repository_interface_names
    
    except (ConvertingError, OSError, Neo4jError):
        raise
    except Exception:
        err_msg = "리포지토리 인터페이스를 생성하는 도중 오류가 발생했습니다."
        logging.exception(err_msg)
        raise RepositoryCreationError(err_msg)
    finally:
        await connection.close() 
