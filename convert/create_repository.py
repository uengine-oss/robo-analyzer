from collections import defaultdict
import json
import os
import logging
import aiofiles
import tiktoken
from prompt.repository_prompt import convert_repository_code
from understand.neo4j_connection import Neo4jConnection
from util.exception import ConvertingError, LLMCallError, Neo4jError, ProcessResultError, RepositoryCreationError, TokenCountError, TraverseCodeError, VariableNodeError

encoder = tiktoken.get_encoding("cl100k_base")


# 역할: 전달된 문자열의 토큰 길이를 계산합니다.
# 매개변수: 
#   - text : 전달된 문자열
# 반환값: 
#   - len(text_json) : 전달된 문자열의 토큰 수
def calculate_code_token(text):
    
    try:
        # * 데이터를 JSON 형식으로 인코딩하고, 인코딩된 데이터의 길이를 반환합니다.
        text_json = json.dumps(text, ensure_ascii=False)
        return len(encoder.encode(text_json))

    except Exception:
        err_msg = "리포지토리 인터페이스 생성 과정에서 토큰 계산 도중 문제가 발생"
        logging.exception(err_msg)
        raise TokenCountError(err_msg)



# 역할: 리포지토리 인터페이스로 생성합니다. (파일로 생성하는 최종 단계)
# 매개변수:
#   - repository_codes : 리포지토리 인터페이스 생성을 위한 모든 JPA 쿼리 메서드 코드들
#   - lower_file_name : 소문자로 구성된 프로젝트 이름
# 반환값: 없음
async def create_repository_interface(repository_codes, lower_file_name):
    
    
    try:
        # * 리포지토리 인터페이스를 저장할 경로를 설정합니다
        base_directory = os.getenv('DOCKER_COMPOSE_CONTEXT', 'data')
        entity_directory = os.path.join(base_directory, 'java', f'{lower_file_name}', 'src', 'main', 'java', 'com', 'example', f'{lower_file_name}', 'repository')
        os.makedirs(entity_directory, exist_ok=True)


        # * 엔티티 클래스의 이름을 찾아내서 해당 엔티티를 위한 리포지토리 인터페이스 이름을 결정
        for table_name, methods in repository_codes.items():
            enttiy_pascal_name = ''.join(word.capitalize() for word in table_name.split('_'))
            entity_camel_name = enttiy_pascal_name[0].lower() + enttiy_pascal_name[1:]
        

            # * 전달된 JPA 쿼리 메서드들의 들여쓰기 조정하여 하나의 문자열로 생성
            adjusted_methods = []
            for method in methods:
                lines = method['method'].strip().split('\n')
                indented_lines = ['    ' + line.strip() for line in lines]
                adjusted_methods.append('\n'.join(indented_lines))


            # * 메서드들 사이에 빈 줄을 추가하여 합칩니다.
            merged_methods = '\n\n'.join(adjusted_methods)


            # * 리포지토리 인터페이스를 생성합니다.
            repository_interface = f"""package com.example.{lower_file_name}.repository;
import java.util.List;
import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.data.jpa.repository.Query;
import org.springframework.data.repository.query.Param;
import org.springframework.data.rest.core.annotation.RepositoryRestResource;
import com.example.{lower_file_name}.entity.{enttiy_pascal_name};
import java.time.LocalDate;

@RepositoryRestResource(collectionResourceRel = "{entity_camel_name}s", path = "{entity_camel_name}s")
public interface {enttiy_pascal_name}Repository extends JpaRepository<{enttiy_pascal_name}, Long> {{
{merged_methods}
}}
    """

            # * 설정된 경로에 리포지토리 인터페이스를 생성합니다
            file_path = os.path.join(entity_directory, f"{enttiy_pascal_name}Repository.java")
            async with aiofiles.open(file_path, 'w', encoding='utf-8') as file:
                await file.write(repository_interface)

    except Exception:
        err_msg = "리포지토리 인터페이스 파일 쓰기 및 생성 도중 오류가 발생"
        logging.exception(err_msg)
        raise OSError(err_msg)


# 역할: 현재 노드에서 사용된 변수 노드를 neo4j에서 가져오는 함수 
# 매개변수: 
#   - startLine : 현재 노드 시작라인
#   - variable_nodes : 모든 변순 노드 리스트
# 반환값: 
#   - filtered_variable_info : 사용된 변수에 대한 정보가 담긴 사전
#   - variable_tokens : 변수 정보 리스트의 토큰 길이.
async def process_variable_nodes(startLine, variable_nodes):
    try:

        # * 현재 노드에서 사용된 변수를 구합니다.
        filtered_variable_info = defaultdict(list)
        for node in variable_nodes:
            for key in node['v']:
                if '_' in key:
                    var_start, var_end = map(int, key.split('_'))
                    if var_start == startLine:
                        range_key = f'{var_start}~{var_end}'
                        variable_info = f"{node['v'].get('type', 'Unknown')}: {node['v']['name']}"
                        filtered_variable_info[range_key].append(variable_info)
                        break


        # * 필터링된 변수 정보의 토큰을 계산합니다.
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
#   - one_depth_nodes : 테이블과 직접적으로 연결된 노드 리스트
#   - variable_nodes : 모든 변수 노드 리스트
#   - lower_file_name : 소문자로 구성된 프로젝트 이름
# 반환값: 
#   - used_jpa_methods_list : JPA 쿼리 메서드 리스트
async def check_tokens_and_process(one_depth_nodes, variable_nodes, lower_file_name):
    total_tokens = 0                        # 전체 토큰 수
    variable_tokens = 0                     # llm에게 전달할 변수 정보의 토큰 수
    table_link_node_chunk = []              # llm에게 전달할 노드 데이터 모음
    used_variable_nodes = defaultdict(list) # llm에게 전달할 변수 데이터 모음
    repository_codes = defaultdict(list)    # JPA 쿼리 메서드 리스트(현재 단계에서 리포지토리 인터페이스 생성에 사용) 
    used_jpa_methods_list = []              # 사용된 JPA 쿼리 메서드 리스트(반환값 다음 단계에 사용)
    process_append_flag = True              # 데이터 추가를 제어하는 플래그

    try:
        # * 테이블와 직접 연결된 노드를 순회하면서 토큰 수를 체크합니다
        for node in one_depth_nodes:
            process_append_flag = True                    
            node_tokens = node['m']['token']  
            node_sp_code = node['m'].get('summarized_code', node['m']['node_code'])
            node_startLine = node['m']['startLine']

            # * 총 토큰 수가 1500을 넘었는지 확인합니다
            if total_tokens + node_tokens + variable_tokens >= 1000:
                

                # * table_link_node_chunk가 비어있으면서, 첫 시작 노드의 크기가 클 경우, 처리합니다
                if not table_link_node_chunk:     
                    table_link_node_chunk.append(node_sp_code)
                    variable_node_dict, variable_tokens = await process_variable_nodes(node_startLine, variable_nodes) 
                    [used_variable_nodes[k].extend(v) for k, v in variable_node_dict.items()]
                    process_append_flag = False  # 추가 데이터 처리 방지
                

                # * 리포지토리 인터페이스 생성을 위해 데이터를 LLM에게 전달합니다.
                convert_data_count = len(table_link_node_chunk)
                grouped_query_methods, used_jpa_methods = await process_llm_repository_interface(table_link_node_chunk, used_variable_nodes, convert_data_count)
                [repository_codes[key].extend(value) for key, value in grouped_query_methods.items()]
                used_jpa_methods_list.extend([{key: value} for key, value in used_jpa_methods.items()])


                # * 다음 사이클을 위해 각 변수를 초기화합니다
                table_link_node_chunk = []
                used_variable_nodes.clear()  
                total_tokens = 0  
                variable_tokens = 0
                
            
            # * 데이터 추가 플래그가 True인 경우, 현재 노드를 데이터 청크에 추가합니다
            if process_append_flag:    
                table_link_node_chunk.append(node_sp_code)
                variable_node_dict, variable_tokens = await process_variable_nodes(node_startLine, variable_nodes)
                [used_variable_nodes[k].extend(v) for k, v in variable_node_dict.items()]
                total_tokens += node_tokens + variable_tokens


        # * 마지막 데이터 그룹을 처리합니다
        if table_link_node_chunk:
            convert_data_count = len(table_link_node_chunk)
            grouped_query_methods, used_jpa_methods = await process_llm_repository_interface(table_link_node_chunk, used_variable_nodes, convert_data_count)
            {repository_codes[key].extend(value) for key, value in grouped_query_methods.items()}
            used_jpa_methods_list.extend([{key: value} for key, value in used_jpa_methods.items()])
        

        # * 최종적으로 리포지토리 인터페이스를 생성합니다.
        await create_repository_interface(repository_codes, lower_file_name)

        return used_jpa_methods_list
    
    except (ConvertingError, OSError, Neo4jError):
        raise
    except Exception:
        err_msg = "Converting 과정에서 리포지토리 인터페이스 관련 정보를 순회하는 도중 문제가 발생했습니다."
        logging.exception(err_msg)
        raise TraverseCodeError(err_msg)


# 역할: LLM의 분석 결과를 바탕으로 리포지토리 인터페이스 정보를 얻는 함수
# 매개변수:
#   - node_data: 분석할 노드 데이터의 리스트
#   - used_variable_nodes: 변수 노드의 컨텍스트 정보를 포함하는 딕셔너리
#   - convert_data_count : 분석할 데이터의 개수
# 반환값: 
#   - grouped_query_methods: 각 테이블의 JPA 쿼리 메서드 리스트.
#   - used_jpa_method : 사용된 JPA 쿼리 메서드 목록으로 다음 단계(서비스 생성)에서 사용됨
async def process_llm_repository_interface(node_data, used_variable_nodes, convert_data_count):
    
    grouped_query_methods = {}
    used_jpa_method = {}

    try:
        # * LLM을 사용하여 주어진 노드 데이터를 분석하고, 나온 결과에서 필요한 정보를 추출합니다
        analysis_data = convert_repository_code(node_data, used_variable_nodes, convert_data_count)            
        for method_info in analysis_data['analysis']:
            table_name = method_info['tableName'].split('.')[-1]
            if table_name not in grouped_query_methods:
                grouped_query_methods[table_name] = []
            grouped_query_methods[table_name].append({
                'startLine': method_info['startLine'],
                'endLine': method_info['endLine'],
                'method': method_info['method']
            })


            # * @query 어노테이션 부분을 제거하고 최대한 축약해서 사용된 JPA 쿼리 메서드 목록들을 생성합니다.
            method_body = method_info['method'].split('\n')[1].strip()
            used_jpa_method[f"{method_info['startLine']}~{method_info['endLine']}"] = method_body

        return grouped_query_methods, used_jpa_method
    
    except LLMCallError:
        raise
    except Exception:
        err_msg = "리포지토리 인터페이스 생성을 위한 LLM 결과 처리 도중 문제가 발생했습니다."
        logging.exception(err_msg)
        raise ProcessResultError(err_msg)


# 역할: 테이블 노드와 1단계 깊이의 수준으로 연결된 노드를 가져와서 Repository 생성을 준비합니다
# 매개변수: 
#   - table_node_list : 테이블 노드의 리스트
#   - lower_file_name : 소문자 프로젝트 이름
# 반환값: 
#   - jpa_method_list : JPA 쿼리 메서드 리스트
async def start_repository_processing(lower_file_name):
    
    logging.info("repository interface 생성을 시작합니다.")

    try:
        connection = Neo4jConnection()


        # * 테이블 노드와 직접적으로 연결된 노드와 모든 변수 노드들을 가지고오는 사이퍼쿼리를 준비하고 실행합니다.
        queries = [
            "MATCH (n:Table)--(m) WHERE NOT m:Table AND NOT m:EXECUTE_IMMDDIATE AND NOT (m:INSERT AND m.summarized_code IS NOT NULL) RETURN m ORDER BY m.startLine",
            "MATCH (v:Variable) RETURN v"
        ]
        results = await connection.execute_queries(queries)
        one_depth_nodes = results[0]
        variable_nodes = results[1]


        # * 토큰 수에 따라서 리포지토리 인터페이스 관련 작업을 처리하는 함수를 호출합니다.
        jpa_method_list = await check_tokens_and_process(one_depth_nodes, variable_nodes, lower_file_name)
        logging.info("리포지토리 인터페이스를 생성했습니다.\n")
        return jpa_method_list
    
    except (ConvertingError, OSError, Neo4jError):
        raise
    except Exception:
        err_msg = "리포지토리 인터페이스를 생성하는 도중 오류가 발생했습니다."
        logging.exception(err_msg)
        raise RepositoryCreationError(err_msg)
    finally:
        await connection.close() 
