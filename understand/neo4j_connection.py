import logging
from neo4j import AsyncGraphDatabase
import os

import numpy as np
from util.exception import Neo4jError

class Neo4jConnection:

    database_name = "note"

    # 역할 : Neo4j 데이터베이스와의 연결을 초기화합니다. 환경변수를 통해 연결 정보를 설정하며, 설정되지 않은 경우 기본값을 사용합니다.
    #
    # 매개변수:
    #   - uri: 데이터베이스 URI (기본값: "bolt://localhost:실제 포트 번호")
    #   - user: 데이터베이스 사용자 이름 (기본값: "neo4j")
    #   - password: 데이터베이스 비밀번호 (기본값: "neo4j")
    def __init__(self):
        uri = os.getenv("NEO4J_URI", "bolt://localhost:7691")
        user = os.getenv("NEO4J_USER", "neo4j")
        password = os.getenv("NEO4J_PASSWORD", "an1021402")
        self.__driver = AsyncGraphDatabase.driver(uri, auth=(user, password))


    # 역할: 데이터베이스 연결을 안전하게 종료하고 리소스를 정리합니다.
    async def close(self):
        await self.__driver.close()



    # 역할: 여러 개의 사이퍼 쿼리를 순차적으로 실행하고 결과를 수집합니다.
    #
    # 매개변수: 
    #   - queries: 실행할 사이퍼 쿼리 문자열의 리스트
    #
    # 반환값: 
    #   - results: 각 쿼리의 실행 결과를 담은 리스트
    async def execute_queries(self, queries: list) -> list:
        try:
            results = [] 
            async with self.__driver.session(database=self.database_name) as session:
                for query in queries:
                    query_result = await session.run(query)
                    query_data = await query_result.data()
                    results.append(query_data)
            return results
        except Exception as e:
            error_msg = f"Cypher Query를 실행하여, 노드 및 관계를 생성하는 도중 오류가 발생: {str(e)}"
            logging.exception(error_msg)
            raise Neo4jError(error_msg)
    
    
    # 역할: 그래프 데이터베이스의 노드와 관계를 시각화 가능한 형태로 조회합니다.
    #
    # 매개변수: 
    #   - package_names: 패키지 이름 목록
    #   - user_id: 사용자 ID
    #   - custom_query: 사용자가 정의한 조회 쿼리 (선택적)
    #
    # 반환값: 
    #   - graph_data: 노드와 관계 정보를 포함하는 그래프 데이터 딕셔너리
    async def execute_query_and_return_graph(self, user_id: str, package_names: list, custom_query=None) -> dict:
        try:
            # * 패키지 별 노드를 가져오기 위한 정보를 추출
            default_query = custom_query or f"""
            MATCH (n)-[r]->(m) 
            WHERE NOT n:Variable AND NOT n:PACKAGE_SPEC AND NOT n:FUNCTION_SPEC AND NOT n:PROCEDURE_SPEC AND NOT n:PACKAGE_VARIABLE
            AND NOT m:Variable AND NOT m:PACKAGE_SPEC AND NOT m:FUNCTION_SPEC AND NOT m:PROCEDURE_SPEC AND NOT m:PACKAGE_VARIABLE
            AND n.object_name IN $package_names
            AND m.object_name IN $package_names
            AND n.userId = $user_id
            AND m.userId = $user_id
            RETURN n, r, m
            """

            params = {
                "package_names": package_names,
                "user_id": user_id
            }

            async with self.__driver.session(database=self.database_name) as session:
                result = await session.run(default_query, params)
                graph = await result.graph()

                nodes_data = [
                    {
                        "Node ID": node.element_id,
                        "Labels": list(node.labels),
                        "Properties": dict(node),
                    }
                    for node in graph.nodes
                ]

                relationships_data = [
                    {
                        "Relationship ID": relationship.element_id,
                        "Type": relationship.type,
                        "Properties": dict(relationship),
                        "Start Node ID": relationship.start_node.element_id,
                        "End Node ID": relationship.end_node.element_id,
                    }
                    for relationship in graph.relationships
                ]

                logging.info("Queries executed successfully")
                return {"Nodes": nodes_data, "Relationships": relationships_data}
            
        except Exception as e:
            error_msg = f"Neo4J에서 그래프 객체 형태로 결과를 반환하는 도중 문제가 발생: {str(e)}"
            logging.exception(error_msg)
            raise Neo4jError(error_msg)
        


    # 역할: 텍스트 유사도 기반으로 노드를 검색합니다.
    #
    # 매개변수: 
    #   - search_text: 검색할 텍스트
    #   - similarity_threshold: 유사도 임계값 (기본값: 0.5)
    #   - limit: 반환할 최대 결과 수 (기본값: 5)
    #
    # 반환값:
    #   - 유사도가 높은 순으로 정렬된 노드 목록
    async def search_similar_nodes(self, search_vector: np.ndarray, similarity_threshold: float = 0.3, limit: int = 15) -> list:
        try:
            query = """
            MATCH (n)
            WHERE n.summary_vector IS NOT NULL AND n.java_code IS NOT NULL AND n.java_range IS NOT NULL AND NOT n:EXCEPTION
            WITH n, gds.similarity.cosine(n.summary_vector, $search_vector) AS similarity
            WHERE similarity >= $threshold
            RETURN n.node_code as node_code, n.java_code as java_code, n.summary as summary, n.name as name, n.java_file as java_file, similarity
            ORDER BY similarity DESC
            LIMIT $limit
            """
            
            async with self.__driver.session(database=self.database_name) as session:
                result = await session.run(query, 
                                            search_vector=search_vector.tolist(),
                                            threshold=similarity_threshold,
                                            limit=limit)
                nodes = await result.data()
                
                # 결과가 없는 경우 임계값을 낮춰서 다시 검색
                if not nodes:
                    query = """
                    MATCH (n)
                    WHERE n.summary_vector IS NOT NULL AND n.java_code IS NOT NULL AND n.java_range IS NOT NULL AND NOT n:EXCEPTION
                    WITH n, gds.similarity.cosine(n.summary_vector, $search_vector) AS similarity
                    RETURN n.node_code as node_code, n.java_code as java_code, n.summary as summary, n.name as name, n.java_file as java_file, similarity
                    ORDER BY similarity DESC
                    LIMIT $limit
                    """
                    
                    result = await session.run(query,
                                             search_vector=search_vector.tolist(),
                                             limit=limit)
                    nodes = await result.data()

                return nodes

        except Exception as e:
            error_msg = f"유사도 검색 중 오류 발생: {str(e)}"
            logging.exception(error_msg)
            raise Neo4jError(error_msg)
        

    # 역할: 이전에 사용자가 생성한 노드 존재 여부를 확인합니다.
    #
    # 매개변수:
    #   - user_id: 사용자 ID
    #   - object_names: 객체 이름
    #
    # 반환값:
    #   - node_exists: 노드 존재 여부 (True 또는 False)
    async def node_exists(self, user_id: str, package_names: list) -> bool:
        try:
            query = """
            MATCH (n:Node)
            WHERE n.objectName IN $package_names
            AND n.userId = $user_id
            RETURN COUNT(n) > 0 AS exists
            """

            params = {
                "package_names": package_names,
                "user_id": user_id
            }

            async with self.__driver.session(database=self.database_name) as session:
                result = await session.run(query, params)
                record = await result.single()
                return record["exists"]
        except Exception as e:
            error_msg = f"노드 존재 여부 확인 중 오류 발생: {str(e)}"
            logging.exception(error_msg)
            raise Neo4jError(error_msg)