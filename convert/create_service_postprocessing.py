import logging
import os
import textwrap
import aiofiles
import tiktoken
from understand.neo4j_connection import Neo4jConnection
from util.exception import Neo4jError, ProcessResultError, ServiceCreationError, TraverseCodeError

encoder = tiktoken.get_encoding("cl100k_base")


# 역할: 요약 처리된 자바 코드에 실제 자식 자바 코드로 채워넣기 위한 함수
# 매개변수: 
#   - node_startLine : 부모 노드의 시작라인
#   - summarized_java_code: 자식 자바 코드들이 모두 요약 처리된 부모의 자바 코드
#   - connection : NEO4J 연결 객체
#   - object_name : 패키지 및 프로시저 이름
# 반환값 : 
#   - summarized_java_code : 실제 코드로 대체되어 완성된 부모의 자바 코드
async def process_big_size_node(node_startLine, summarized_java_code, connection, object_name):
    
    try:
        # * 부모 노드의 시작 라인을 기준으로 자식 노드를 찾는 쿼리
        query = [f"""
        MATCH (n)-[r:PARENT_OF]->(m)
        WHERE n.startLine = {node_startLine}
        AND n.object_name = '{object_name}'
        AND m.object_name = '{object_name}'
        RETURN m
        """]
        
        # * 자식 노드 리스트를 비동기적으로 그래프 데이터베이스에서 가져옴
        child_node_list = await connection.execute_queries(query)
        

        # * 자식 노드 리스트를 순회하면서 각 노드 처리
        for node in child_node_list[0]:
            child = node['m']
            token = child['token']
            logging.info("크기가 매우 큰 노드 처리중")
            
            # * 자식 노드의 토큰 크기에 따라 재귀적으로 처리하거나 기존 코드 사용하여 요약된 부모 코드에서 실제 자식 코드로 교체
            java_code = await process_big_size_node(child['startLine'], child['java_code'], connection) if token > 1700 else child['java_code']
            placeholder = f"{child['startLine']}: ...code..."
            indented_code = textwrap.indent(java_code, '    ')
            summarized_java_code = summarized_java_code.replace(placeholder, f"\n{indented_code}")
        
        return summarized_java_code
    
    except Neo4jError:
        raise
    except Exception:
        err_msg = "Service 클래스 생성과정에서 크기가 매우 큰 노드에 대한 처리를 하는 도중 문제가 발생했습니다"
        logging.error(err_msg, exc_info=False)
        raise ProcessResultError(err_msg)


# 역할: 서비스 클래스를 생성하기 위해 노드를 순회하는 함수
# 매개변수: 
#   - node_list : 노드 리스트
#   - connection : Neo4J 연결 객체
#   - object_name : 패키지 및 프로시저 이름
# 반환값: 
#   - all_java_code : 생성된 전체 자바 코드
async def process_service_class(node_list, connection, object_name):

    try:
        previous_node_endLine = 0
        all_java_code = ""


        # * service class를 생성하기 위한 노드의 순회 시작
        for node in node_list:
            start_node = node['n']
            relationship = node['r'][1] if node['r'] else "NEXT"
            end_node = node['m']
            token = start_node['token']
            node_name = start_node['name']
            print("\n"+"-" * 40) 
            print(f"시작 노드 : [ 시작 라인 : {start_node['startLine']}, 이름 : ({start_node['name']}), 끝라인: {start_node['endLine']}, 토큰 : {start_node['token']}")
            print(f"관계: {relationship}")
            if end_node: print(f"종료 노드 : [ 시작 라인 : {end_node['startLine']}, 이름 : ({end_node['name']}), 끝라인: {end_node['endLine']}, 토큰 : {end_node['token']}")
            is_duplicate_or_unnecessary = (previous_node_endLine > start_node['startLine'] and previous_node_endLine) or ("EXECUTE_IMMDDIATE" in node_name)


            # * 중복(이미 처리된 자식노드) 또는 불필요한 노드 건너뛰기
            if is_duplicate_or_unnecessary:
                print("자식노드 및 필요없는 노드로 넘어갑니다")
                continue

            
            # * 노드의 토큰 크기에 따라 처리 방식 결정
            if token > 1700:
                java_code = await process_big_size_node(start_node['startLine'], start_node['java_code'], connection, object_name)
            else:
                java_code = start_node['java_code']


            # * 변수 값 할당 및 Java 코드 추가
            all_java_code += java_code + "\n\n"
            previous_node_endLine = start_node['endLine']

        return all_java_code
    
    except (Neo4jError, ProcessResultError):
        raise
    except Exception:
        err_msg = "(후처리) Service 클래스 생성을 위해 노드를 순회하는 도중 문제가 발생했습니다."
        logging.error(err_msg, exc_info=False)
        raise TraverseCodeError(err_msg)


# 역할: 각 노드에 Java_code 이름의 속성을 이용하여,  최종적으로  서비스 클래스 파일을 생성하는 시작 함수. 
# 매개변수: 
#   - service_skeleton : 서비스 스켈레톤
#   - service_class_name : 서비스 클래스 이름
#   - object_name : 패키지 및 프로시저 이름
# 반환값: 없음
async def start_service_postprocessing(service_skeleton, service_class_name, object_name):
    
    connection = Neo4jConnection() 
    logging.info(f"[{object_name}] (후처리) 서비스 생성을 시작합니다.")
    
    try:
        # * 노드와 관계를 가져오는 쿼리 
        node_query = [
            f"""
            MATCH (n)
            WHERE (NOT (n:ROOT OR n:Variable OR n:DECLARE OR n:Table OR n:CREATE_PROCEDURE_BODY 
                      OR n:PACKAGE_BODY OR n:PACKAGE_SPEC OR n:PROCEDURE_SPEC)
                  AND n.object_name = '{object_name}')
            OPTIONAL MATCH (n)-[r:NEXT]->(m)
            WHERE (NOT (m:ROOT OR m:Variable OR m:DECLARE OR m:Table OR m:CREATE_PROCEDURE_BODY
                      OR m:PACKAGE_BODY OR m:PACKAGE_SPEC OR m:PROCEDURE_SPEC)
                  AND m.object_name = '{object_name}')
            RETURN n, r, m
            ORDER BY n.startLine
            """
        ]

        
        # * 쿼리 실행
        results = await connection.execute_queries(node_query)
        

        # * 서비스 클래스 생성을 시작하는 함수를 호출
        all_java_code = await process_service_class(results[0], connection, object_name)


        # * 결과를 바탕으로 서비스 클래스 생성 (바디 채우기)
        all_java_code = all_java_code.strip()
        indented_java_code = textwrap.indent(all_java_code, '        ')
        completed_service_code = service_skeleton.replace("CodePlaceHolder2", indented_java_code)


        # * 서비스 클래스 생성을 위한 경로를 설정합니다.
        base_directory = os.getenv('DOCKER_COMPOSE_CONTEXT')
        if base_directory:
            service_directory = os.path.join(base_directory, 'java', 'demo', 'src', 'main', 'java', 'com', 'example', 'demo', 'service')
        else:
            current_dir = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
            service_directory = os.path.join(current_dir, 'target', 'java', 'demo', 'src', 'main', 'java', 'com', 'example', 'demo', 'service')
        os.makedirs(service_directory, exist_ok=True) 
        service_file_path = os.path.join(service_directory, f"{service_class_name}.java")


        # * 서비스 클래스를 파일로 생성합니다.
        async with aiofiles.open(service_file_path, 'w', encoding='utf-8') as file:  
            await file.write(completed_service_code)  
            logging.info(f"[{object_name}] Success Create Service Java File\n")  

    except (Neo4jError, ProcessResultError, TraverseCodeError):
        raise
    except Exception:
        err_msg = "(후처리) 서비스 클래스를 생성을 위한 준비 및 시작 도중 문제가 발생했습니다."
        logging.error(err_msg, exc_info=False)
        raise ServiceCreationError(err_msg)
    finally:
        await connection.close() 