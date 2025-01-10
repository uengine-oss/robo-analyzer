from understand.neo4j_connection import Neo4jConnection
from util.exception import ExtractJavaLineError, Neo4jError, ReadFileError
from util.file_utils import read_target_file
import logging


# 역할 : 서비스 파일에서 Java 코드 블록의 범위를 찾아 Neo4j에 업데이트하는 함수
#
# 매개변수 :
#   - object_name: 패키지 이름
#   - service_class_name: 서비스 클래스 이름
#   - user_id: 사용자 ID
async def find_service_line_ranges(object_name: str, service_class_name: str, user_id: str) -> list[str]:
    try:
        # * 서비스 클래스 파일 내용 읽기
        file_content = read_target_file(service_class_name, "service", user_id)
        file_lines = file_content.splitlines()
        connection = Neo4jConnection()
        

        # * Neo4j에서 Java 코드가 존재하는 노드 조회
        java_nodes = await get_java_node(object_name, connection, user_id)
        

        # * 라인 범위를 업데이트하는 Cypher 쿼리를 저장할 리스트
        update_queries = []
        

        # * 각 Java 코드 블록에 대해 라인 범위를 찾아 업데이트하는 Cypher 쿼리 생성
        for node in java_nodes:
            node_data = node['n'] 
            java_code = node_data['java_code']
            start_line = node_data['startLine']
            end_line = node_data['endLine']
            
            if not java_code:
                continue
                
            # * 자바 코드 라인 정규화
            java_lines = [line.strip() for line in java_code.splitlines() if line.strip()]
            java_first_line = java_lines[0]
            
            # * 서비스 클래스 파일에서 자바 코드 블록 찾기
            for i, line in enumerate(file_lines, 1):
                if line.strip() != java_first_line:
                    continue
                    
                # * 자바 코드 블록 전체 매칭 확인
                is_match = True
                current_line = i
                
                for java_line in java_lines[1:]:

                    # * 빈 줄 건너뛰기
                    while current_line < len(file_lines) and not file_lines[current_line].strip():
                        current_line += 1
                        
                    # * 자바 코드 라인 매칭 실패 시 중단
                    if current_line >= len(file_lines) or file_lines[current_line].strip() != java_line:
                        is_match = False
                        break
                        
                    current_line += 1
                
                # * 자바 코드 블록 전체 매칭 성공 시 라인 범위를 속성으로 추가하는 사이퍼 쿼리 생성
                if is_match:
                    query = f"""
                        MATCH (n)
                        WHERE n.object_name = '{object_name}'
                        AND n.user_id = '{user_id}'
                        AND n.startLine = {start_line}
                        AND n.endLine = {end_line}
                        SET n.java_range = '{i}~{current_line - 1}'
                    """
                    update_queries.append(query)
                    break
        

        # * 사이퍼 쿼리 실행
        if update_queries:
            await connection.execute_queries(update_queries)
            logging.info(f"{len(update_queries)}개의 노드가 업데이트되었습니다.")
        else:
            logging.info(f"일치하는 Java 코드를 찾을 수 없습니다: {service_class_name}")
    
    
    except (Neo4jError, ReadFileError):
        raise
    except Exception as e:
        error_msg = f"일치하는 Java 코드를 찾을 수 없습니다: {service_class_name}, 에러: {e}"
        logging.error(error_msg)
        raise ExtractJavaLineError(error_msg)
    finally:
        await connection.close()


# 역할 : Neo4j에서 Java 코드가 존재하는 노드를 조회하는 함수
#
# 매개변수 :
#   - object_name: 패키지 이름
#   - connection: Neo4j 연결 객체
#   - user_id: 사용자 ID
#
# 반환값 :
#   - list[dict]: Java 코드가 존재하는 노드 리스트
async def get_java_node(object_name: str, connection: Neo4jConnection, user_id: str) -> list[dict]:
    try:
        query = [f"""
            MATCH (n)
            WHERE n.object_name = '{object_name}'
            AND n.user_id = '{user_id}'
            AND n.java_code IS NOT NULL
            AND NOT n:EXCEPTION
            RETURN n
            ORDER BY n.startLine
        """]
        nodes = await connection.execute_queries(query)
        return nodes[0]
    
    except Exception as e:
        error_msg = f"Neo4j에서 Java 코드가 존재하는 노드를 조회하는 도중 오류가 발생했습니다: {str(e)}"
        logging.error(error_msg)
        raise Neo4jError(error_msg)
