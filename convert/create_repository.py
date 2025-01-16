from collections import defaultdict
import os
import logging
import textwrap
from prompt.convert_repository_prompt import convert_repository_code, convert_save_method
from understand.neo4j_connection import Neo4jConnection
from util.converting_utlis import extract_used_variable_nodes
from util.exception import ConvertingError, LLMCallError, ProcessResultError, RepositoryCreationError, SaveFileError, StringConversionError, TemplateGenerationError, TokenCountError, TraverseCodeError, VariableNodeError
from util.file_utils import save_file
from util.string_utils import convert_to_camel_case, convert_to_pascal_case


MAX_TOKENS = 1000
REPOSITORY_PATH = 'demo/src/main/java/com/example/demo/repository'

MYBATIS_TEMPLATE = """package com.example.demo.repository;
import java.util.List;
import org.apache.ibatis.annotations.*;
import com.example.demo.entity.{entity_pascal_name};
import java.time.*;

@Mapper
public interface {entity_pascal_name}Repository {{
{merged_methods}

{sequence_methods}
}}"""

JPA_TEMPLATE = """package com.example.demo.repository;
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
}}"""


# 역할: Spring Data 리포지토리 인터페이스 파일을 생성합니다.
#
# 매개변수:
#   - all_query_methods : {테이블명: [ 쿼리 메서드 정보]} 형식의 딕셔너리
#   - orm_type : 사용할 ORM 유형
#   - sequence_methods : 사용 가능한 시퀀스 메서드 목록
#   - user_id : 사용자 ID
async def generate_repository_interface(all_query_methods: dict, orm_type: str, sequence_methods: list, user_id: str) -> None:
    try:
        # * 저장 경로 설정
        if os.getenv('DOCKER_COMPOSE_CONTEXT'):
            save_path = os.path.join(os.getenv('DOCKER_COMPOSE_CONTEXT'), 'target', 'java', user_id, REPOSITORY_PATH)
        else:
            parent_workspace_dir = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
            save_path = os.path.join(parent_workspace_dir, 'target', 'java', user_id, REPOSITORY_PATH)


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

            # * ORM 타입에 따른 템플릿 선택 및 데이터 삽입
            template = JPA_TEMPLATE if orm_type == 'jpa' else MYBATIS_TEMPLATE
            repository_interface_template = template.format(
                entity_pascal_name=entity_pascal_name,
                entity_camel_name=entity_camel_name,
                merged_methods=merged_methods,
                sequence_methods=formatted_sequence_methods
            )

            # * 파일 저장
            await save_file(
                content=repository_interface_template, 
                filename=f"{entity_pascal_name}Repository.java", 
                base_path=save_path
            )
    
    except (SaveFileError, StringConversionError):
        raise
    except Exception as e:
        err_msg = f"리포지토리 인터페이스 파일 저장 중 오류가 발생: {str(e)}"
        logging.error(err_msg)
        raise TemplateGenerationError(err_msg)


# 역할: 테이블 연관 노드들을 토큰 제한에 맞춰 처리하는 메인 로직입니다.
#
# 매개변수: 
#   - repository_nodes : 테이블과 직접 연결된 Neo4j 노드 리스트
#   - local_variable_nodes : 모든 지역 변수 노드 정보 리스트
#   - global_variable_nodes : 모든 전역 변수 노드 정보 리스트
#   - sequence_data : 시퀀스 정보
#   - orm_type : 사용할 ORM 유형
#   - user_id : 사용자 ID
#   - table_entity_info : 테이블 및 엔티티 정보
#
# 반환값: 
#   - used_repository_methodss_list : 생성된 쿼리 메서드들의 정보 리스트
async def process_repository_by_token_limit(repository_nodes: list, local_variable_nodes: list, global_variable_nodes: list, sequence_data: str, orm_type: str, user_id: str, table_entity_info: dict) -> list:
    
    try:
        current_tokens = 0
        repository_data_chunk = []
        used_variable_nodes = defaultdict(list)
        all_query_methods = {}
        used_query_methods_list = {}
        sequence_methods = []


        # 역할 : mybatis로 Converting 할시 -> save 쿼리 메서드 저장
        async def save_query_methods(table_entity_info: dict) -> None:
            try:
                # * save 메서드 생성
                save_result = await convert_save_method(table_entity_info)
                
                # * 각 엔티티별로 처리
                for method_info in save_result['analysis']:
                    
                    # * 테이블 이름을 파스칼 케이스로 변환
                    table_name = method_info['tableName'].split('.')[-1]
                    entity_name = convert_to_pascal_case(table_name)

                    # * 기존 메서드 리스트 가져오기 (없으면 빈 리스트 생성)
                    existing_methods = all_query_methods.setdefault(entity_name, [])
                    
                    # * save 메서드 추가
                    existing_methods.append(method_info['method'])


            except (LLMCallError, StringConversionError):
                raise
            except Exception as e:
                err_msg = f"save 쿼리 메서드 저장 중 오류가 발생했습니다: {str(e)}"
                logging.error(err_msg)
                raise ProcessResultError(err_msg)


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
                    sequence_data,
                    orm_type
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
            
            except (LLMCallError, StringConversionError):
                raise
            except Exception as e:
                err_msg = f"리포지토리 인터페이스 생성을 위한 LLM 결과 처리 도중 문제가 발생했습니다: {str(e)}"
                logging.error(err_msg)
                raise ProcessResultError(err_msg)



        # * 만약 mybatis 일 경우 save 메서드 생성
        if orm_type == 'mybatis':
            await save_query_methods(table_entity_info)


        # * 리포지토리 노드 처리
        for node in repository_nodes:

            # * 노드 정보 추출
            node_tokens = node['m']['token']
            node_code = node['m'].get('summarized_code', node['m']['node_code'])
            node_start_line = node['m']['startLine']
            
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
        



        # * 리포지토리 인터페이스 생성
        await generate_repository_interface(all_query_methods, orm_type, sequence_methods, user_id)
        return used_query_methods_list, all_query_methods, sequence_methods

    except ConvertingError:
        raise
    except Exception as e:
        err_msg = f"리포지토리 인터페이스 처리 중 오류가 발생했습니다: {str(e)}"
        logging.error(err_msg)
        raise TraverseCodeError(err_msg)


# 역할: 리포지토리 인터페이스 생성 프로세스의 시작점입니다.
#
# 매개변수: 
#   - object_name : 처리할 프로시저/패키지의 이름
#   - sequence_data : 시퀀스 정보
#   - orm_type : 사용할 ORM 유형
#   - user_id : 사용자 ID
#   - table_entity_info : 엔티티 클래스 정보
#
# 반환값: 
#   - query_method_list : 생성된 모든 쿼리 메서드 정보 리스트
#   - global_variables : 전역 변수 목록
async def start_repository_processing(object_name: str, sequence_data: str, orm_type: str, user_id: str, table_entity_info: dict):
    
    logging.info(f"[{object_name}] Repository Interface 생성을 시작합니다.")
    connection = Neo4jConnection()

    try:
        # * 테이블 노드와 직접적으로 연결된 노드와 모든 변수 노드들을 가지고오는 사이퍼쿼리를 준비하고 실행합니다.
        jpa_queries = [
            f"MATCH (n:Table {{object_name: '{object_name}', user_id: '{user_id}'}})--(m {{object_name: '{object_name}', user_id: '{user_id}'}}) WHERE NOT m:Table AND NOT m:EXECUTE_IMMEDIATE AND NOT m:INSERT RETURN m ORDER BY m.startLine",
            f"MATCH (v:Variable {{object_name: '{object_name}', user_id: '{user_id}', scope: 'Local'}}) RETURN v",
            f"MATCH (v:Variable {{object_name: '{object_name}', user_id: '{user_id}', scope: 'Global'}}) RETURN v"
        ]

        mybatis_queries = [
            f"MATCH (n:Table {{object_name: '{object_name}', user_id: '{user_id}'}})--(m {{object_name: '{object_name}', user_id: '{user_id}'}}) WHERE NOT m:Table AND NOT m:EXECUTE_IMMEDIATE RETURN m ORDER BY m.startLine",
            f"MATCH (v:Variable {{object_name: '{object_name}', user_id: '{user_id}', scope: 'Local'}}) RETURN v",
            f"MATCH (v:Variable {{object_name: '{object_name}', user_id: '{user_id}', scope: 'Global'}}) RETURN v"
        ]


        # * 사용할 쿼리 선택
        queries = jpa_queries if orm_type == 'jpa' else mybatis_queries


        # * 쿼리 실행 및 결과 할당
        dml_nodes, local_variables_nodes, global_variables = await connection.execute_queries(queries)


        # * 전역 변수 정보 가공
        global_variable_nodes = [{
            'name': var['v']['name'],
            'type': var['v'].get('type', 'Unknown'),
            'role': var['v'].get('role', ''),
            'scope': var['v'].get('scope', 'Global'),
            'value': var['v'].get('value', '')
        } for var in global_variables]


        # * 리포지토리 인터페이스 생성을 시작합니다.
        used_query_methods, all_query_methods, sequence_methods = await process_repository_by_token_limit(
            dml_nodes, 
            local_variables_nodes, 
            global_variable_nodes,
            sequence_data,
            orm_type,
            user_id,
            table_entity_info
        )

        logging.info(f"[{object_name}] Repository Interface를 생성했습니다.\n")
        return used_query_methods, global_variable_nodes, all_query_methods, sequence_methods
    
    except ConvertingError:
        raise
    except Exception as e:
        err_msg = f"[{object_name}] Repository Interface를 생성하는 도중 오류가 발생했습니다: {str(e)}"
        logging.error(err_msg)
        raise RepositoryCreationError(err_msg)
    finally:
        await connection.close() 

