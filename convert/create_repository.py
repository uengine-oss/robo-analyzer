from collections import defaultdict
import json
import os
import logging
import aiofiles
import tiktoken
from prompt.repository_prompt import convert_repository_code
from understand.neo4j_connection import Neo4jConnection
from util.exception import ConvertingError, LLMCallError, Neo4jError, ProcessResultError, RepositoryCreationError, TokenCountError, TraverseCodeError, VariableNodeError

MAX_TOKENS = 1000
REPOSITORY_PATH = 'java/demo/src/main/java/com/example/demo/repository'
encoder = tiktoken.get_encoding("cl100k_base")


# 역할: 전달된 코드의 토큰 길이를 계산하는 유틸리티 함수입니다.
# 매개변수: 
#   - code : 토큰 수를 계산할 코드 문자열 (dict, list 등 다양한 타입 가능)
# 반환값: 
#   - len(text_json) : JSON으로 변환된 코드의 토큰 수
def calculate_code_token(code):
    
    try:
        text_json = json.dumps(code, ensure_ascii=False)
        return len(encoder.encode(text_json))

    except Exception:
        err_msg = "리포지토리 인터페이스 생성 과정에서 토큰 계산 도중 문제가 발생"
        logging.error(err_msg, exc_info=False)
        raise TokenCountError(err_msg)



# 역할: Spring Data JPA 리포지토리 인터페이스 파일을 생성합니다.
# 매개변수:
#   - repository_interface : {테이블명: [JPA 메서드 정보]} 형식의 딕셔너리
async def create_repository_interface(repository_interface):
    
    try:
        # * 리포지토리 인터페이스를 저장할 경로를 설정합니다.
        base_directory = os.getenv('DOCKER_COMPOSE_CONTEXT')
        if base_directory:
            repository_directory = os.path.join(base_directory, REPOSITORY_PATH)
        else:
            parent_workspace_dir = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
            repository_directory = os.path.join(parent_workspace_dir, 'target', REPOSITORY_PATH)
        os.makedirs(repository_directory, exist_ok=True)


        # * 엔티티 클래스의 이름을 찾아내서 해당 엔티티를 위한 리포지토리 인터페이스 이름을 결정
        for table_name, jpa_query_method in repository_interface.items():
            entity_pascal_name = ''.join(word.capitalize() for word in table_name.split('_'))
            entity_camel_name = entity_pascal_name[0].lower() + entity_pascal_name[1:]
        

            # * 전달된 JPA 쿼리 메서드들의 들여쓰기 조정하여 하나의 문자열로 생성
            indented_method_list = []
            for method in jpa_query_method:
                method_lines = method['method'].strip().split('\n')
                indented_lines = ['    ' + line.strip() for line in method_lines]
                indented_method_list.append('\n'.join(indented_lines))
            merged_methods = '\n\n'.join(indented_method_list)


            # * 리포지토리 인터페이스 템플릿을 생성합니다.
            repository_interface_template = f"""package com.example.demo.repository;
import java.util.List;
import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.data.jpa.repository.Query;
import org.springframework.data.repository.query.Param;
import org.springframework.data.rest.core.annotation.RepositoryRestResource;
import com.example.demo.entity.{entity_pascal_name};
import java.time.*;

@RepositoryRestResource(collectionResourceRel = "{entity_camel_name}s", path = "{entity_camel_name}s")
public interface {entity_pascal_name}Repository extends JpaRepository<{entity_pascal_name}, Long> {{
{merged_methods}
}}
    """
            # * 설정된 경로에 리포지토리 인터페이스 파일을 생성합니다
            file_path = os.path.join(repository_directory, f"{entity_pascal_name}Repository.java")
            async with aiofiles.open(file_path, 'w', encoding='utf-8') as file:
                await file.write(repository_interface_template)

    except Exception:
        err_msg = "리포지토리 인터페이스 파일 쓰기 및 생성 도중 오류가 발생"
        logging.error(err_msg, exc_info=False)
        raise OSError(err_msg)


# 역할: 특정 코드 라인에서 사용된 변수들의 정보를 추출합니다.
# 매개변수: 
#   - startLine : 분석할 코드의 시작 라인 번호
#   - local_variable_nodes : Neo4j에서 조회한 모든 변수 노드 정보
# 반환값: 
#   - used_variables : {'라인범위': ['변수타입: 변수명']} 형식의 딕셔너리
#   - variable_tokens : 추출된 변수 정보의 토큰 수
async def extract_used_variable_nodes(startLine, local_variable_nodes):
    try:
        # * 현재 노드에서 사용된 변수를 구합니다.
        used_variables = defaultdict(list)
        for variable_node in local_variable_nodes:
            for used_range in variable_node['v']:
                if '_' in used_range and all(part.isdigit() for part in used_range.split('_')):
                    used_startLine, used_endLine = map(int, used_range.split('_'))
                    if used_startLine == startLine:
                        range_key = f'{used_startLine}~{used_endLine}'
                        variable_info = f"{variable_node['v'].get('type', 'Unknown')}: {variable_node['v']['name']}"
                        used_variables[range_key].append(variable_info)
                        break


        # * 변수 정보의 토큰을 계산합니다.
        variable_tokens = calculate_code_token(used_variables)
        return used_variables, variable_tokens
    
    except (TokenCountError):
        raise
    except Exception:
        err_msg = "사용된 변수 노드를 추출하는 도중 오류가 발생했습니다."
        logging.error(err_msg, exc_info=False)
        raise VariableNodeError(err_msg)



# 역할: 테이블 연관 노드들을 토큰 제한에 맞춰 처리하는 메인 로직입니다.
# 매개변수: 
#   - repository_nodes : 테이블과 직접 연결된 Neo4j 노드 리스트
#   - local_variable_nodes : 모든 지역 변수 노드 정보 리스트
#   - global_variable_nodes : 모든 전역 변수 노드 정보 리스트
# 반환값: 
#   - used_jpa_methods_list : 생성된 JPA 메서드들의 정보 리스트
async def process_repository_by_token_limit(repository_nodes, local_variable_nodes, global_variable_nodes):
    current_tokens = 0                        # 현재 토큰 수
    repository_data_chunk = []                # 리포지토리 인터페이스 데이터 청크
    used_variable_nodes = defaultdict(list)   # 현재 노드에서 사용된 변수 노드 정보
    repository_interface = defaultdict(list)  # 테이블(엔티티)를 기준으로 리포지토리 인터페이스 코드 저장할 딕셔너리
    used_jpa_methods_list = []                # 특정 라인 범위에서 사용된 JPA 쿼리 메서드 리스트

    try:
        # * 테이블와 직접 연결된 노드를 순회하면서 토큰 수를 체크합니다
        for node in repository_nodes:
            node_tokens = node['m']['token']  
            node_sp_code = node['m'].get('summarized_code', node['m']['node_code'])
            node_startLine = node['m']['startLine']

            # * 현재 노드에서 사용된 변수를 구합니다.
            variable_node_dict, variable_tokens = await extract_used_variable_nodes(node_startLine, local_variable_nodes) 

            # * 총 토큰 수가 1500을 넘었는지 확인합니다
            if repository_data_chunk and current_tokens + node_tokens + variable_tokens >= MAX_TOKENS:

                # * 리포지토리 인터페이스 코드 생성을 위한 함수 호출 및 결과 처리
                convert_data_count = len(repository_data_chunk)
                jpa_query_methods, used_jpa_methods = await process_convert_with_llm(repository_data_chunk, used_variable_nodes, convert_data_count, global_variable_nodes)
                [repository_interface[key].extend(value) for key, value in jpa_query_methods.items()]
                used_jpa_methods_list.extend([{key: value} for key, value in used_jpa_methods.items()])

                # * 다음 사이클을 위해 각 변수를 초기화합니다
                repository_data_chunk = []
                used_variable_nodes.clear()  
                current_tokens = 0  
                
            
            # * 현재 노드를 데이터 청크에 추가합니다
            repository_data_chunk.append(node_sp_code)
            [used_variable_nodes[key].extend(value) for key, value in variable_node_dict.items()]
            current_tokens += node_tokens + variable_tokens


        # * 마지막 데이터 그룹을 처리합니다
        if repository_data_chunk:
            convert_data_count = len(repository_data_chunk)
            jpa_query_methods, used_jpa_methods = await process_convert_with_llm(repository_data_chunk, used_variable_nodes, convert_data_count, global_variable_nodes)
            {repository_interface[key].extend(value) for key, value in jpa_query_methods.items()}
            used_jpa_methods_list.extend([{key: value} for key, value in used_jpa_methods.items()])
        

        # * 리포지토리 인터페이스.java를 생성합니다.
        await create_repository_interface(repository_interface)

        return used_jpa_methods_list
    
    except (ConvertingError, OSError, Neo4jError):
        raise
    except Exception:
        err_msg = "Converting 과정에서 리포지토리 인터페이스 관련 정보를 순회하는 도중 문제가 발생했습니다."
        logging.error(err_msg, exc_info=False)
        raise TraverseCodeError(err_msg)


# 역할: LLM 분석 결과를 처리하여 리포지토리 인터페이스 정보를 생성합니다.
# 매개변수:
#   - repository_data: LLM에 전달할 코드 데이터 리스트
#   - used_variable_nodes: {'라인범위': ['변수정보']} 형식의 변수 컨텍스트
#   - convert_data_count : 처리할 데이터 청크의 크기
#   - global_variable_nodes : 모든 전역 변수 노드 정보 리스트
# 반환값: 
#   - jpa_query_methods: {'테이블명': [JPA메서드정보]} 형식의 딕셔너리
#   - used_jpa_method : {'라인범위': 'JPA메서드'} 형식의 딕셔너리
async def process_convert_with_llm(repository_data, used_variable_nodes, convert_data_count, global_variable_nodes):
    
    jpa_query_methods = {}
    used_jpa_method = {}

    try:
        # * LLM을 통해 리포지토리 인터페이스 코드를 받고, 그 중 필요한 정보를 추출합니다
        analysis_data = convert_repository_code(repository_data, used_variable_nodes, convert_data_count, global_variable_nodes)            
        for method_info in analysis_data['analysis']:
            table_name = method_info['tableName'].split('.')[-1]
            if table_name not in jpa_query_methods:
                jpa_query_methods[table_name] = []
            jpa_query_methods[table_name].append({
                'startLine': method_info['startLine'],
                'endLine': method_info['endLine'],
                'method': method_info['method']
            })


            # * @Query 어노테이션을 제거하고, 특정 라인에서 사용된 JPA 쿼리 메서드 목록들을 생성합니다.
            method_body = method_info['method'].split('\n')[1].strip()
            used_jpa_method[f"{method_info['startLine']}~{method_info['endLine']}"] = method_body

        return jpa_query_methods, used_jpa_method
    
    except LLMCallError:
        raise
    except Exception:
        err_msg = "리포지토리 인터페이스 생성을 위한 LLM 결과 처리 도중 문제가 발생했습니다."
        logging.error(err_msg, exc_info=False)
        raise ProcessResultError(err_msg)


# 역할: 리포지토리 인터페이스 생성 프로세스의 시작점입니다.
# 매개변수: 
#   - object_name : 처리할 프로시저/패키지의 이름
#                   이 이름으로 Neo4j에서 관련 노드들을 검색
# 반환값: 
#   - jpa_method_list : 생성된 모든 JPA 메서드 정보 리스트
#   - global_variables : 전역 변수 목록
async def start_repository_processing(object_name):
    
    logging.info(f"[{object_name}] Repository Interface 생성을 시작합니다.")

    try:
        connection = Neo4jConnection()


        # * 테이블 노드와 직접적으로 연결된 노드와 모든 변수 노드들을 가지고오는 사이퍼쿼리를 준비하고 실행합니다.
        queries = [
            f"MATCH (n:Table {{object_name: '{object_name}'}})--(m {{object_name: '{object_name}'}}) WHERE NOT m:Table AND NOT m:EXECUTE_IMMEDIATE AND NOT m:INSERT RETURN m ORDER BY m.startLine",
            f"MATCH (v:Variable {{object_name: '{object_name}', scope: 'Local'}}) RETURN v",
            f"MATCH (v:Variable {{object_name: '{object_name}', scope: 'Global'}}) RETURN v"
        ]

        results = await connection.execute_queries(queries)
        one_depth_nodes = results[0]
        local_variables_nodes = results[1]
        global_variables = results[2]

        # * 전역 변수 정보 가공
        global_variable_nodes = [{
            'name': var['v']['name'],
            'type': var['v'].get('type', 'Unknown'),
            'role': var['v'].get('role', ''),
            'scope': var['v'].get('scope', 'Global')
        } for var in global_variables]


        # * 리포지토리 인터페이스 생성을 시작합니다.
        jpa_method_list = await process_repository_by_token_limit(one_depth_nodes, local_variables_nodes, global_variable_nodes)
        logging.info(f"[{object_name}] Repository Interface를 생성했습니다.\n")
        return jpa_method_list, global_variable_nodes
    
    except (ConvertingError, OSError, Neo4jError):
        raise
    except Exception:
        err_msg = f"[{object_name}] Repository Interface를 생성하는 도중 오류가 발생했습니다."
        logging.error(err_msg, exc_info=False)
        raise RepositoryCreationError(err_msg)
    finally:
        await connection.close() 

