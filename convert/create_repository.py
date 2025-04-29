from collections import defaultdict
import os
import logging
import textwrap
from prompt.convert_repository_prompt import convert_repository_code
from understand.neo4j_connection import Neo4jConnection
from util.exception import ConvertingError, GenerateTargetError, GenerateTargetError
from util.utility_tool import convert_to_camel_case, convert_to_pascal_case, extract_used_variable_nodes, save_file


MAX_TOKENS = 1000
# 프로젝트 이름을 파라미터로 받도록 수정
# JPA_TEMPLATE는 함수 내에서 동적으로 생성


# 역할: Spring Data 리포지토리 인터페이스 파일을 생성합니다.
#
# 매개변수:
#   - all_query_methods : {테이블명: [ 쿼리 메서드 정보]} 형식의 딕셔너리
#   - sequence_methods : 사용 가능한 시퀀스 메서드 목록
#   - user_id : 사용자 ID
#   - api_key : Claude API 키
#   - project_name : 프로젝트 이름
async def generate_repository_interface(all_query_methods: dict, sequence_methods: list, user_id: str, api_key: str, project_name: str) -> list:
    repository_list = []
    try:
        # 리포지토리 경로 생성
        repository_path = f'{project_name}/src/main/java/com/example/{project_name}/repository'
        
        # * JPA 템플릿 동적 생성
        jpa_template = """package com.example.{project_name}.repository;
import java.util.List;
import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.data.jpa.repository.Query;
import org.springframework.data.repository.query.Param;
import org.springframework.data.rest.core.annotation.RepositoryRestResource;
import com.example.{project_name}.entity.{entity_pascal_name};
import java.time.*;

@RepositoryRestResource(collectionResourceRel = "{entity_camel_name}s", path = "{entity_camel_name}s")
public interface {entity_pascal_name}Repository extends JpaRepository<{entity_pascal_name}, Long> {{
{merged_methods}
}}"""

        # * 저장 경로 설정
        if os.getenv('DOCKER_COMPOSE_CONTEXT'):
            save_path = os.path.join(os.getenv('DOCKER_COMPOSE_CONTEXT'), 'target', 'java', user_id, repository_path)
        else:
            parent_workspace_dir = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
            save_path = os.path.join(parent_workspace_dir, 'target', 'java', user_id, repository_path)


        # * 시퀀스 메서드 들여쓰기 처리
        formatted_sequence_methods = '\n\n'.join(
            textwrap.indent(method['method'].strip().replace('\n\n', '\n'),  '    ')
            for method in (sequence_methods or [])
        ) if sequence_methods else ''
        

        # * 각 테이블별 리포지토리 인터페이스 생성
        for entity_pascal_name, query_method in all_query_methods.items():
            
            # * 엔티티 이름 변환
            entity_camel_name = convert_to_camel_case(entity_pascal_name)
            
            # * 쿼리 메서드 들여쓰기 처리
            merged_methods = '\n\n'.join(
                textwrap.indent(method.strip().replace('\n\n', '\n'),  '    ')
                for method in query_method
            )

            # * 데이터 삽입
            repository_interface_template = jpa_template.format(
                project_name=project_name,
                entity_pascal_name=entity_pascal_name,
                entity_camel_name=entity_camel_name,
                merged_methods=merged_methods
            )

            # * 파일 저장
            filename = f"{entity_pascal_name}Repository.java"
            await save_file(
                content=repository_interface_template, 
                filename=filename, 
                base_path=save_path
            )
            
            # * 생성된 리포지토리 정보 저장
            repository_list.append({
                "repositoryName": f"{entity_pascal_name}Repository",
                "code": repository_interface_template
            })

        return repository_list

    except ConvertingError:
        raise
    except Exception as e:
        err_msg = f"리포지토리 인터페이스 파일 저장 중 오류가 발생: {str(e)}"
        logging.error(err_msg)
        raise GenerateTargetError(err_msg)


# 역할: 테이블 연관 노드들을 토큰 제한에 맞춰 처리하는 메인 로직입니다.
#
# 매개변수: 
#   - repository_nodes : 테이블과 직접 연결된 Neo4j 노드 리스트
#   - local_variable_nodes : 모든 지역 변수 노드 정보 리스트
#   - global_variable_nodes : 모든 전역 변수 노드 정보 리스트
#   - user_id : 사용자 ID
#   - api_key : Claude API 키
#   - project_name : 프로젝트 이름
#
# 반환값: 
#   - used_repository_methodss_list : 생성된 쿼리 메서드들의 정보 리스트
async def process_repository_by_token_limit(repository_nodes: list, local_variable_nodes: list, global_variable_nodes: list, user_id: str, api_key: str, project_name: str) -> list:
    
    try:
        current_tokens = 0
        repository_data_chunk = []
        used_variable_nodes = defaultdict(list)
        all_query_methods = {}
        used_query_methods_list = {}
        sequence_methods = []


        # 역할: LLM 분석 결과를 처리하여 리포지토리 인터페이스 정보를 생성합니다.
        async def prcoess_repository_interface_code() -> None:
            nonlocal all_query_methods, current_tokens, repository_data_chunk, used_variable_nodes, current_tokens, sequence_methods    

            try:
                # * LLM을 통한 코드 변환
                analysis_data = convert_repository_code(
                    repository_data_chunk, 
                    used_variable_nodes, 
                    len(repository_data_chunk),
                    global_variable_nodes,
                    api_key
                )


                # * 분석 결과 처리
                for method in analysis_data['analysis']:
                    table_name = method['tableName'].split('.')[-1]
                    entity_name = convert_to_pascal_case(table_name)

                    # * 테이블별 메서드 정보 저장
                    methods = all_query_methods.setdefault(entity_name, [])
                    methods.append(method['method'])
                    
                    # * 사용된 메서드 범위 저장
                    for range in method['range']:
                        range_str = f"{range['startLine']}~{range['endLine']}"
                        used_query_methods_list[range_str] = method['method']
                    

                # * 시퀀스 메서드 저장
                if analysis_data.get('seq_method'):
                    sequence_methods.extend(analysis_data['seq_method'])


                # * 다음 사이클을 위한 상태 초기화
                repository_data_chunk = []
                used_variable_nodes.clear()
                current_tokens = 0
            
            except ConvertingError:
                raise
            except Exception as e:
                err_msg = f"리포지토리 인터페이스 생성을 위한 LLM 결과 처리 도중 문제가 발생했습니다: {str(e)}"
                logging.error(err_msg)
                raise ConvertingError(err_msg)


        # * 리포지토리 노드 처리
        for node in repository_nodes:
            # * 노드 정보 추출
            try:
                node_tokens = node['token']
                node_code = node.get('summarized_code', node['node_code'])
                node_start_line = node['startLine']
            except KeyError as e:
                logging.warning(f"리포지토리 노드에 필요한 속성이 없습니다: {e}")
                continue
            
            # * 변수 노드 처리
            var_nodes, var_tokens = await extract_used_variable_nodes(node_start_line, local_variable_nodes)
            total_tokens = current_tokens + node_tokens + var_tokens

            # * 토큰 제한 초과시 처리
            if repository_data_chunk and total_tokens >= MAX_TOKENS:
                await prcoess_repository_interface_code()
                
            # * 현재 노드 데이터 추가
            repository_data_chunk.append(node_code)
            [used_variable_nodes[key].extend(value) for key, value in var_nodes.items()]
            current_tokens = total_tokens


        # * 남은 데이터 처리
        if repository_data_chunk:
            await prcoess_repository_interface_code()


        # * 리포지토리 인터페이스 및 노드 생성
        await generate_repository_interface(all_query_methods, sequence_methods, user_id, api_key, project_name)
        return used_query_methods_list, all_query_methods, sequence_methods

    except ConvertingError:
        raise
    except Exception as e:
        err_msg = f"리포지토리 인터페이스 처리 중 오류가 발생했습니다: {str(e)}"
        logging.error(err_msg)
        raise ConvertingError(err_msg)


# 역할: 리포지토리 인터페이스 생성 프로세스의 시작점입니다.
#
# 매개변수: 
#   - file_names : 처리할 파일 이름과 객체 이름 튜플의 리스트 [(file_name, object_name), ...]
#   - user_id : 사용자 ID
#   - api_key : Claude API 키
#   - project_name : 프로젝트 이름
#
# 반환값: 
#   - query_method_list : 생성된 모든 쿼리 메서드 정보 리스트
#   - global_variables : 전역 변수 목록
async def start_repository_processing(file_names: list, user_id: str, api_key: str, project_name: str):
    
    logging.info("Repository Interface 생성을 시작합니다.")
    connection = Neo4jConnection()
    repository_list = []
    all_used_query_methods = {}
    all_global_variables = []
    all_sequence_methods = []

    try:
        # file_names에서 object_name 추출
        object_names = [obj_name for _, obj_name in file_names]
        object_names_str = "', '".join(object_names)
        
        # 테이블과 DML 노드를 한 번에 가져오는 쿼리
        table_dml_query = f"""
        MATCH (t:Table {{user_id: '{user_id}'}})--(m) 
        WHERE m.object_name IN ['{object_names_str}'] AND m.user_id = '{user_id}' 
        AND (m:SELECT OR m:UPDATE OR m:DELETE)
        RETURN t, COLLECT(m) as dml_nodes
        """
        
        # 전역 변수와 지역 변수 가져오기
        vars_query = f"""
        MATCH (v:Variable) 
        WHERE v.user_id = '{user_id}' AND v.object_name IN ['{object_names_str}']
        RETURN v, v.scope as scope
        """
        
        # 쿼리 실행
        table_dml_results, var_results = await connection.execute_queries([table_dml_query, vars_query])
        
        print(table_dml_results)

        # 변수 결과 처리
        local_vars = []
        global_variable_nodes = []
        
        for var in var_results:
            var_node = var['v']
            if var['scope'] == 'Global':
                global_variable_nodes.append({
                    'name': var_node['name'],
                    'type': var_node.get('type', 'Unknown'),
                    'role': var_node.get('role', ''),
                    'scope': 'Global',
                    'value': var_node.get('value', '')
                })
            else:
                local_vars.append(var)
        
        all_global_variables.extend(global_variable_nodes)
        
        # 각 테이블에 대해 처리
        for result in table_dml_results:
            table_node = result['t']
            dml_nodes = result['dml_nodes']
            table_name = table_node['name']
            
            logging.info(f"{table_name} 테이블의 Repository Interface 생성 중...")
            
            if dml_nodes:
                # 이 테이블에 대한 리포지토리 인터페이스 생성
                used_query_methods, table_query_methods, sequence_methods = await process_repository_by_token_limit(
                    dml_nodes, 
                    local_vars, 
                    global_variable_nodes,
                    user_id,
                    api_key,
                    project_name
                )
                
                all_used_query_methods.update(used_query_methods)
                all_sequence_methods.extend(sequence_methods)
                
                # 이 테이블에 대한 리포지토리 생성
                table_repositories = await generate_repository_interface(
                    table_query_methods, 
                    sequence_methods, 
                    user_id, 
                    api_key, 
                    project_name
                )
                
                repository_list.extend(table_repositories)
                logging.info(f"{table_name} 테이블의 Repository Interface 생성 완료")
            else:
                logging.info(f"{table_name} 테이블에 연결된 DML 노드가 없습니다.")

        logging.info("모든 Repository Interface 생성이 완료되었습니다.\n")
        return all_used_query_methods, all_global_variables, all_sequence_methods, repository_list
    
    except ConvertingError:
        raise
    except Exception as e:
        err_msg = f"Repository Interface를 생성하는 도중 오류가 발생했습니다: {str(e)}"
        logging.error(err_msg)
        raise ConvertingError(err_msg)
    finally:
        await connection.close() 

