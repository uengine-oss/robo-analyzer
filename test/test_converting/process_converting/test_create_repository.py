import json
import re
import unittest
import sys
import os
import logging
import aiofiles
import tiktoken
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..', '..')))
from test_converting.converting_prompt.repository_prompt import convert_repository_code
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..', '..', '..')))
from cypher.neo4j_connection import Neo4jConnection

# 로깅 설정
logging.basicConfig(level=logging.INFO)
logging.getLogger('asyncio').setLevel(logging.ERROR)


# * 인코더 설정 및 파일 이름 초기화
encoder = tiktoken.get_encoding("cl100k_base")
fileName = None
jpa_method_dict = {}

# 역할 : 전달받은 이름을 전부 소문자로 전환하는 함수입니다,
# 매개변수 : 
#   - fileName : 스토어드 프로시저 파일의 이름
# 반환값 : 전부 소문자로 전환된 프로젝트 이름
def convert_to_lower_case_no_underscores(fileName):
    return fileName.replace('_', '').lower()


# 역할: 주어진 노드 id를 기반으로 현재 노드에서 사용된 변수 노드를 가져옵니다.
# 매개변수: 
#      - node_id : 노드 id.
# 반환값: 현재 노드에서 사용된 변수 노드 리스트
async def fetch_variable_nodes(node_id):

    try:
        # * 변수 노드를 가지고 오는 사이퍼쿼리를 준비하고, Neo4j 데이터베이스에 쿼리를 실행하여 변수 노드를 가져옵니다.
        query = [f"MATCH (v:Variable) WHERE COALESCE(v.role_{node_id}, NULL) IS NOT NULL RETURN v"]
        connection = Neo4jConnection()  
        variable_nodes = await connection.execute_queries(query) 
        logging.info("\nSuccess received Variable Nodes from Neo4J\n")
        await connection.close()  
        return variable_nodes

    except Exception:
        logging.exception(f"Error occurred while bring variable node from neo4j")
        raise


# 역할: 전달된 스토어드 프로시저 코드에서 불필요한 정보(주석 등)를 제거합니다.
# 매개변수: 
#      - spCode : 스토어드 프로시저 코드
# 반환값: 주석이 제거된 스토어드 프로시저 코드
def remove_code_placeholders(spCode):
    
    try:
        if spCode == "": return spCode

        # * 다중 라인 주석과 단일 라인 주석을 제거합니다.
        spCode = re.sub(r'/\*.*?\*/', '', spCode, flags=re.DOTALL)
        spCode = re.sub(r'--.*$', '', spCode, flags=re.MULTILINE)
        return spCode

    except Exception:
        logging.exception(f"Error occurred while remove placeholders")
        raise


# 역할: 전달된 스토어드 프로시저 코드의 토큰 길이를 계산합니다.
# 매개변수: 
#      - spCode : 스토어드 프로시저 코드
# 반환값: 스토어드 프로시저 코드의 토큰 길이
def calculate_spCode_length(spCode):
    
    try:
        # * 데이터를 JSON 형식으로 인코딩하고, 인코딩된 데이터의 길이를 반환합니다.
        spCode_json = json.dumps(spCode, ensure_ascii=False)
        return len(encoder.encode(spCode_json))

    except Exception:
        logging.exception(f"Error occurred while calculate spcode token")
        raise


# 역할: 여러 JPA 쿼리 메서드를 하나의 리포지토리 인터페이스로 통합합니다.
# 매개변수:
#   repository_code_list - 각 리포지토리 인터페이스의 코드가 담긴 리스트.
#   repository_pascal_name - 생성될 리포지토리 인터페이스의 이름 (PascalCase 형식).
#   repository_camel_name - 생성될 리포지토리 인터페이스의 이름 (camelCase 형식).
#   primary_key_type - 리포지토리의 주 키 타입.
#   entity_name - 사용된 엔티티 이름
#   spFile_name - 스토어드 프로시저 파일 이름
# 반환값: 없음
async def merge_jpa_query_method(repository_code_list, repository_pascal_name, repository_camel_name, primary_key_type):
    
    try:        
        # * 각 리포지토리 인터페이스 코드에서 @Query 어노테이션과 해당 메서드를 추출하고, 과도한 공백을 정리합니다.
        all_methods = set()
        for interface_content in repository_code_list:
            methods = re.findall(r'(@Query\(".*?"\))\s+(.*?;)', interface_content, re.DOTALL)
            methods = [re.sub(r'\s+', ' ', query) + '\n    ' + re.sub(r'\s+', ' ', method) for query, method in methods]
            all_methods.update(methods) 


        # * 추출된 메서드들을 들여쓰기를 조정하여 하나의 문자열로 결합합니다.
        adjusted_methods = ['    ' + method for method in all_methods]
        merged_methods = '\n\n'.join(adjusted_methods) 

        # * 통합된 리포지토리 인터페이스를 생성합니다.
        repository_interface = f"""package com.example.{fileName}.repository;
import java.util.List;
import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.data.jpa.repository.Query;
import org.springframework.data.repository.query.Param;
import org.springframework.data.rest.core.annotation.RepositoryRestResource;
import com.example.{fileName}.entity.{repository_pascal_name};
import java.time.LocalDate;

@RepositoryRestResource(collectionResourceRel = "{repository_camel_name}s", path = "{repository_camel_name}s")
public interface {repository_pascal_name}Repository extends JpaRepository<{repository_pascal_name}, {primary_key_type}> {{
{merged_methods}
}}
    """
    
        # * 완성된 리포지토리 인터페이스를 저장할 경로를 설정합니다
        repository_interface_directory = os.path.join('test', 'test_converting', 'converting_result', 'repository')
        os.makedirs(repository_interface_directory, exist_ok=True)
        file_path = os.path.join(repository_interface_directory, f"{repository_pascal_name}Repository.txt")
        

        # * 설정된 경로에 리포지토리 인터페이스를 생성합니다
        async with aiofiles.open(file_path, 'w', encoding='utf-8') as file:
            await file.write(repository_interface)
            logging.info("\nSuccess Create Java Repository Interface\n")

    except Exception:
        logging.exception(f"Error occurred while merge jpa query method")
        raise


# 역할: 현재 노드에서 사용된 변수 노드를 neo4j에서 가져오고, 변수의 이름이 담긴 리스트와 토큰 길이를 반환합니다.
# 매개변수: 
#   - node_id : 현재 노드 id
# 반환값: 
#   - variable_names_list : 변수 이름으로 구성된 리스트.
#   - variable_tokens : 변수 이름 리스트의 토큰 길이.
async def process_variable_nodes(node_id):
    
    try:
        # * Neo4j 데이터베이스에서 node_id에 해당하는 변수 노드를 가져와서 필요한 정보를 추출합니다
        variable_nodes = await fetch_variable_nodes(node_id)
        variable_names_list = [node['v']['name'] for node in variable_nodes[0]]
        variable_tokens = calculate_spCode_length(variable_names_list)
        return variable_names_list, variable_tokens

    except Exception:
        logging.exception(f"Error occurred while merge jpa query method")
        raise


# 역할: 테이블과 직접 연결된 노드의 정보의 토큰의 개수를 체크하여, 처리하는 함수입니다.
# 매개변수: 
#      - table_link_node : 테이블과 직접적으로 연결된 노드 리스트
# 반환값: JPA 메서드 리스트.
async def check_tokens_and_process(table_link_node):
    total_tokens = 0                # 전체 데이터의 토큰 수
    variable_tokens = 0             # llm에게 전달할 변수 정보의 토큰 수
    table_link_node_chunk = []            # llm에게 전달할 노드 데이터 모음
    variable_nodes_context = {}     # llm에게 전달할 변수 데이터 모음
    repository_code_list = []       # 리포지토리 코드를 모아두는 리스트 
    jpa_method_list = []            # JPA 메서드 리스트
    current_name = None             # 현재 처리 중인 테이블의 이름을 추적
    pascal_name = None              # 파스칼 표기법이 적용된 엔티티의 이름
    camel_name = None               # 카멜 표기법이 적용된 엔티티의 이름
    process_append_flag = True      # 데이터 추가를 제어하는 플래그


    try:
        # * 테이블와 직접 연결된 노드를 순회하면서 토큰 수를 체크합니다
        for item in table_link_node:
            
            process_append_flag = True                     # 기본적으로 데이터 추가를 허용
            item_tokens = calculate_spCode_length(item)    # 현재 노드의 정보 토큰 수를 계산
        
            # * 현재 처리중인 테이블 이름이 없는 경우, 새로 할당합니다
            if current_name is None:
                current_name = item['name']

            # * 테이블 이름이 변경되었는지 확인합니다(같은 테이블에 대한 처리가 끝났는지)
            if item['name'] != current_name:

                # * 현재까지 쌓아둔 데이터 그룹을 llm에게 전달해서 처리하기 위한 함수를 호출합니다
                repository_code, pascal_name, camel_name, primary_key_type, method_list = await create_repository_interface(table_link_node_chunk, variable_nodes_context)
                repository_code_list.append(repository_code)
                jpa_method_list.append(method_list)


                # * 한 테이블에 대한 처리가 끝났으니 최종적으로 하나의 리포지토리 인터페이스를 생성합니다 
                await merge_jpa_query_method(repository_code_list, pascal_name, camel_name, primary_key_type)

                # * 다음 사이클을 위해 각 변수를 초기화합니다
                repository_code_list.clear()
                current_name = item['name']  
                table_link_node_chunk = [] 
                variable_nodes_context.clear()
                total_tokens = 0  
                variable_tokens = 0
                logging.info("\nSuccess RQRS from LLM (Name Changed)\n")

            # * 총 토큰 수가 1500을 넘었는지 확인합니다
            if total_tokens + item_tokens + variable_tokens >= 1500:
                
                # * table_link_node_chunk가 비어있으면서, 첫 시작 노드의 크기가 클 경우, 처리합니다
                if not table_link_node_chunk:     
                    table_link_node_chunk.append(item)
                    variable_names_list, variable_tokens = await process_variable_nodes(item['startLine']) # 사용된 변수 노드 정보를 가져옵니다
                    variable_nodes_context[f'variables_{item["startLine"]}'] = variable_names_list
                    process_append_flag = False  # 추가 데이터 처리 방지
                repository_code, pascal_name, camel_name, primary_key_type, method_list = await create_repository_interface(table_link_node_chunk, variable_nodes_context)
                repository_code_list.append(repository_code)
                jpa_method_list.append(method_list)
                
                # * 다음 사이클을 위해 각 변수를 초기화합니다
                table_link_node_chunk = []
                variable_nodes_context.clear()  
                total_tokens = 0  
                variable_tokens = 0
                logging.info("\nSuccess RQRS from LLM (Token Limit)\n")
            

            # * 데이터 추가 플래그가 True인 경우, 현재 노드를 데이터 청크에 추가합니다
            if process_append_flag:    
                table_link_node_chunk.append(item)
                variable_names_list, variable_tokens = await process_variable_nodes(item['startLine'])  # 사용된 변수 노드 정보를 가져옵니다
                variable_nodes_context[f'variables_{item["startLine"]}'] = variable_names_list
                total_tokens += item_tokens


        # * 마지막 데이터 그룹을 처리합니다
        if table_link_node_chunk:
            repository_code, pascal_name, camel_name, primary_key_type, method_list = await create_repository_interface(table_link_node_chunk, variable_nodes_context)
            repository_code_list.append(repository_code)
            jpa_method_list.append(method_list)
            await merge_jpa_query_method(repository_code_list, pascal_name, camel_name, primary_key_type)
        
        return jpa_method_list

    except Exception:
        logging.exception(f"Error occurred while table link node traverse")
        raise


# 역할: LLM을 사용하여 주어진 노드 데이터를 분석하고, 분석 결과를 바탕으로 리포지토리 인터페이스 코드를 생성합니다.
# 매개변수:
#   node_data: 분석할 노드 데이터의 리스트. 각 노드는 테이블의 구조적 정보를 포함합니다.
#   variable_nodes_context: 변수 노드의 컨텍스트 정보를 포함하는 딕셔너리. 각 변수 노드는 특정 테이블과 연관된 추가 정보를 제공합니다.
# 반환값: 
#   repository_code: 생성된 Java 리포지토리 인터페이스 코드.
#   repository_pascal_name: 리포지토리의 이름 (PascalCase 형식).
#   repository_camel_name: 리포지토리의 이름 (camelCase 형식).
#   primary_key_type: 리포지토리의 주 키 타입.
#   method_list: 리포지토리 인터페이스에 포함될 메서드 리스트.
#   entity_name: 사용된 엔티티 이름
async def create_repository_interface(node_data, variable_nodes_context):
    
    try:
        # * LLM을 사용하여 주어진 노드 데이터를 분석하고, 나온 결과에서 필요한 정보를 추출합니다
        analysis_data = convert_repository_code(node_data, variable_nodes_context)    
        repository_code = analysis_data['code']
        repository_pascal_name = analysis_data['pascalName']
        repository_camel_name = analysis_data['camelName']
        primary_key_type = analysis_data['primaryKeyType']
        method_list = analysis_data['methodList']
        jpa_method_dict.update(method_list)
        logging.info("\nSuccess RqRs LLM\n")
        return repository_code, repository_pascal_name, repository_camel_name, primary_key_type, method_list
    except Exception:
        logging.exception(f"Error occurred while create jpa query method")
        raise


# 역할: 주어진 테이블 노드 리스트를 기반으로 1단계 깊이의 연결된 노드를 가져와서 Repository 생성을 준비합니다
# 매개변수: 
#      - table_node_list : 테이블 노드의 리스트
#      - sp_fileName : 스토어드 프로시저 파일 이름
# 반환값: JPA 메서드 리스트
async def start_repository_processing(sp_fileName):

    try:
        global fileName
        connection = Neo4jConnection()
        node_sources = []
        fileName = convert_to_lower_case_no_underscores(sp_fileName)


        # * 테이블 노드를 가져옵니다. 
        query = ['MATCH (n:Table) RETURN n']
        table_nodes = await connection.execute_queries(query)
        logging.info("\nSuccess received Table Nodes from Neo4J\n")
        table_node_list = table_nodes[0]   # 쿼리 결과 사용

        
        # * 테이블 정보를 순회하면서 해당 테이블과 직접적으로 연결된 노드만 가져옵니다
        for node in table_node_list:
            node_name = node['n']['name'] 
            key_type = node['n'].get('keyType', 'Long') 
            one_depth_nodes = [f"MATCH (n:Table {{name: '{node_name}'}})--(m) WHERE NOT m:Table AND NOT m:OPERATION RETURN m"]
            one_depth_nodes = await connection.execute_queries(one_depth_nodes)
            

            # * 만약 해당 테이블과 직접적으로 연결된 노드가 없을 경우 넘어갑니다 
            if not one_depth_nodes[0]:  
                logging.info(f"No connected nodes found for {node_name}, skipping.")
                continue 
            logging.info("\nSuccess Recevied 1 Depth Nodes\n")
            

            # * 복잡한 데이터의 구조를 좀 더 편리하게 재구성합니다 
            sources = [{'name': node_name, 'startLine': m['m']['id'], 'endLine': m['m']['endLine'], 'PrimaryKeyType': key_type, 'code': remove_code_placeholders(m['m']['source'])} for m in one_depth_nodes[0] if 'source' in m['m']]
            node_sources.extend(sources)


        # * 재구성된 노드 정보들을 담아서 처리 및 토큰 계산하는 함수를 호출합니다
        jpa_method_list = await check_tokens_and_process(node_sources)
        logging.info("\nSuccess processed All Nodes\n")
        return jpa_method_list
    
    except Exception:
        logging.exception(f"Error occurred while bring 1 Depth Nodes from neo4j")
        raise
    finally:
        await connection.close() 


# 리포지토리 인터페이스 생성하는 테스트 모듈
class AsyncTestCase(unittest.IsolatedAsyncioTestCase):
    async def test_create_repository_interface(self):
        await start_repository_processing("P_B_CAC120_CALC_SUIP_STD")
        print("jpa_method_list 결과 : ")
        print(jpa_method_dict)

if __name__ == '__main__':
    unittest.main()