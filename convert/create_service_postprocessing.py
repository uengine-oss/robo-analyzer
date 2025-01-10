import logging
import os
import textwrap
import tiktoken
from understand.neo4j_connection import Neo4jConnection
from util.exception import ConvertingError, FilePathError, ProcessResultError, SaveFileError, ServiceCreationError, TraverseCodeError
from util.file_utils import save_file

encoder = tiktoken.get_encoding("cl100k_base")
SERVICE_PATH = 'demo/src/main/java/com/example/demo/service'


# 역할: 토큰 수가 1700개 이상인 큰 부모 노드의 요약된 코드를 실제 자식 노드들의 코드로 대체하는 함수입니다.
#
# 매개변수: 
#   - node_startLine : 처리할 부모 노드의 시작 라인 번호
#   - summarized_java_code : 자식 노드들이 "...code..."로 요약된 부모 노드의 Java 코드
#   - connection : Neo4j 데이터베이스 연결 객체
#   - object_name : 현재 처리 중인 패키지/프로시저의 식별자
#   - user_id : 사용자 ID
#
# 반환값: 
#   - summarized_java_code : 모든 자식 노드의 실제 코드로 대체된 완성된 Java 코드
# TODO 프로시저 별 처리 필요 및 엄청 큰 TRY 노드 처리 필요(프롬포트 수정)
async def process_big_size_node(node_startLine:int, summarized_java_code:str, connection:Neo4jConnection, object_name:str, user_id:str) -> str:
    try:
        # * 자식 노드 조회 쿼리 생성
        query = [
            f"MATCH (n)-[r:PARENT_OF]->(m) "
            f"WHERE n.startLine = {node_startLine} "
            f"AND n.object_name = '{object_name}' "
            f"AND m.object_name = '{object_name}' "
            f"AND n.user_id = '{user_id}' "
            f"AND m.user_id = '{user_id}' "
            f"RETURN m"
        ]
        

        # * 자식 노드 조회하는 쿼리 실행
        child_node_list = await connection.execute_queries(query)


        # * 각 자식 노드의 코드를 부모 코드에 병합
        for node in child_node_list[0]:
            child = node['m']
            
            # * 자식 노드가 큰 경우 재귀적으로 처리
            if child['token'] > 1700:
                java_code = await process_big_size_node(child['startLine'], child['java_code'], connection, object_name, user_id)
            else:
                java_code = child['java_code']
            
            # * 부모 코드에 자식 코드 병합
            placeholder = f"{child['startLine']}: ...code..."
            indented_code = textwrap.indent(java_code, '    ')
            summarized_java_code = summarized_java_code.replace(placeholder, f"\n{indented_code}")
        
        return summarized_java_code
    
    except ConvertingError:
        raise
    except Exception as e:
        err_msg = f"Service 클래스 생성과정에서 크기가 매우 큰 노드에 대한 처리를 하는 도중 문제가 발생했습니다: {str(e)}"
        logging.error(err_msg)
        raise ProcessResultError(err_msg)


# 역할: Neo4j에서 가져온 노드들을 순회하면서 Java 코드를 생성하는 함수입니다.
#
# 매개변수: 
#   - node_list : Neo4j에서 조회한 노드, 관계, 다음 노드 정보를 담은 리스트
#   - connection : Neo4j 데이터베이스 연결 객체
#   - object_name : 현재 처리 중인 패키지/프로시저의 식별자
#   - user_id : 사용자 ID
#
# 반환값: 
#   - all_java_code : 모든 노드의 Java 코드가 순서대로 결합된 최종 코드
async def traverse_node_for_merging_service(node_list:list, connection:Neo4jConnection, object_name:str, user_id:str) -> str:

    try:
        previous_node_endLine = 0
        all_java_code = ""
        try_catch_code = ""


        # * 노드의 순회 시작
        for node in node_list:
            
            # * 노드 정보 추출
            start_node = node['n']
            relationship = node['r'][1] if node['r'] else "NEXT"
            end_node = node['m']
            start_node_type = node['nType'] 
            token = start_node['token']
            java_code = start_node['java_code']

            # * 노드 처리 여부 확인
            is_duplicate = previous_node_endLine > start_node['startLine'] and previous_node_endLine
            is_unnecessary = "EXECUTE_IMMEDIATE" in start_node_type
            is_try_node = "TRY" in start_node_type
            is_exception_node = "EXCEPTION" in start_node_type
            
            # * 노드 정보 출력
            print("-" * 40) 
            print(f"시작 노드 : [ 시작 라인 : {start_node['startLine']}, 이름 : ({start_node['name']}), 끝라인: {start_node['endLine']}, 토큰 : {start_node['token']}")
            print(f"관계: {relationship}")
            if end_node: print(f"종료 노드 : [ 시작 라인 : {end_node['startLine']}, 이름 : ({end_node['name']}), 끝라인: {end_node['endLine']}, 토큰 : {end_node['token']}\n")


            # * 중복(이미 처리된 자식노드) 또는 불필요한 노드 건너뛰기
            if is_duplicate or is_unnecessary:
                print("현재 노드에 대한 처리가 필요하지 않습니다.") 
                continue
            

            # * TRY 노드 처리
            if is_try_node:
                try_catch_code += java_code
                print("TRY 노드 처리 중입니다.") 
                continue


            # * EXCEPTION 노드 처리
            if is_exception_node:
                try_catch_code = try_catch_code.replace("        CodePlaceHolder", "CodePlaceHolder")
                indented_code = textwrap.indent(try_catch_code, '    ')
                java_code = java_code.replace("CodePlaceHolder", indented_code)
                all_java_code += java_code + "\n"
                try_catch_code = ""
                print("EXCEPTION 노드 처리 중입니다.") 
                continue
            

            # * 노드의 토큰 크기에 따라 처리 방식 결정
            # TODO 엄청 큰 TRY 노드 처리 필요(프롬포트 수정)
            if token > 1700:
                logging.info("크기가 매우 큰 노드 처리를 시작합니다.")
                java_code = await process_big_size_node(start_node['startLine'], java_code, connection, object_name, user_id)


            # * 변수 값 할당 및 Java 코드 추가
            all_java_code += java_code + "\n"
            previous_node_endLine = start_node['endLine']

        return all_java_code
    
    except ConvertingError:
        raise
    except Exception as e:
        err_msg = f"(후처리) Service 클래스 생성을 위해 노드를 순회하는 도중 문제가 발생했습니다: {str(e)}"
        logging.error(err_msg)
        raise TraverseCodeError(err_msg)


# 역할: 생성된 서비스 코드를 지정된 경로에 Java 파일로 저장하는 함수입니다.
#
# 매개변수:
#   - service_skeleton : 전체 서비스 클래스의 기본 구조 템플릿
#   - service_class_name : 생성할 서비스 클래스의 이름
#   - merge_method_code : 서비스 클래스에 추가될 메서드 코드
#   - user_id : 사용자 ID
async def generate_service_class(service_skeleton: str, service_class_name: str, merge_method_code: str, user_id:str) -> None:
    try:
        # * 병합된 메서드 코드를 들여쓰기 처리
        service_skeleton = service_skeleton.replace("CodePlaceHolder", merge_method_code)


        # * 저장 경로 설정
        if os.getenv('DOCKER_COMPOSE_CONTEXT'):
            save_path = os.path.join(os.getenv('DOCKER_COMPOSE_CONTEXT'), 'target', 'java', user_id, SERVICE_PATH)
        else:
            current_dir = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
            save_path = os.path.join(current_dir, 'target', 'java', user_id, SERVICE_PATH)


        # * 서비스 클래스 파일 생성
        await save_file(
            content=service_skeleton,
            filename=f"{service_class_name}.java",
            base_path=save_path
        )

        logging.info(f"[{service_class_name}] Success Create Service Java File")

    except SaveFileError:
        raise
    except Exception as e:
        err_msg = f"서비스 클래스 파일 생성 중 오류가 발생했습니다: {str(e)}"
        logging.error(err_msg)
        raise FilePathError(err_msg)


# 역할: 서비스 클래스 생성을 위한 후처리 작업의 시작점입니다.
#
# 매개변수: 
#   - method_skeleton_code : 생성될 메서드의 기본 구조 템플릿
#   - procedure_name : 처리할 프로시저의 이름
#   - object_name : 현재 처리 중인 패키지/프로시저의 식별자
#   - merge_method_code : 기존에 생성된 메서드 코드 (새로운 메서드가 추가될 기반 코드)
#   - user_id : 사용자 ID   
#
# 반환값:
#   - merge_method_code : 새로운 메서드가 추가된 최종 Java 코드
async def start_service_postprocessing(method_skeleton_code: str, procedure_name: str, object_name: str, merge_method_code: str, user_id:str) -> str:
    
    connection = Neo4jConnection() 
    logging.info(f"[{object_name}] {procedure_name} 프로시저의 서비스 코드 병합을 시작합니다.")
    
    try:
        # * 코드 병합에 필요한 노드와 관계를 가져오는 쿼리 
        query = [
            f"MATCH (p) "
            f"WHERE p.object_name = '{object_name}' "
            f"AND p.procedure_name = '{procedure_name}' "
            f"AND p.user_id = '{user_id}' "
            f"AND (p:FUNCTION OR p:PROCEDURE OR p:CREATE_PROCEDURE_BODY) "
            f"MATCH (p)-[:PARENT_OF]->(n) "
            f"WHERE NOT (n:ROOT OR n:Variable OR n:DECLARE OR n:Table "
            f"OR n:PACKAGE_BODY OR n:PACKAGE_SPEC OR n:PROCEDURE_SPEC OR n:SPEC) "
            f"OPTIONAL MATCH (n)-[r:NEXT]->(m) "
            f"WHERE m.object_name = '{object_name}' "
            f"AND m.user_id = '{user_id}' "
            f"AND NOT (m:ROOT OR m:Variable OR m:DECLARE OR m:Table "
            f"OR m:PACKAGE_BODY OR m:PACKAGE_SPEC OR m:PROCEDURE_SPEC OR m:SPEC) "
            f"RETURN n, labels(n) as nType, r, m, labels(m) as mType "
            f"ORDER BY n.startLine"
        ]
        

        # * 노드 조회 및 Java 코드 병합
        results = await connection.execute_queries(query)
        all_java_code = await traverse_node_for_merging_service(results[0], connection, object_name, user_id)


        # * 메서드 코드 들여쓰기 및 완성
        method_skeleton_code = method_skeleton_code.replace("        CodePlaceHolder", "CodePlaceHolder")
        indented_java_code = textwrap.indent(all_java_code.strip(), '        ')
        completed_service_code = method_skeleton_code.replace("CodePlaceHolder", indented_java_code)
        

        # * 최종 병합된 메서드 코드를 생성
        merge_method_code = f"{merge_method_code}\n\n{completed_service_code}"


        logging.info(f"[{object_name}] {procedure_name} 프로시저의 메서드 코드 병합이 완료되었습니다.\n")
        return merge_method_code

    except ConvertingError:
        raise
    except Exception as e:
        err_msg = f"(후처리) 서비스 클래스를 생성을 위한 준비 및 시작 도중 문제가 발생했습니다: {str(e)}"
        logging.error(err_msg)
        raise ServiceCreationError(err_msg)
    finally:
        await connection.close() 


