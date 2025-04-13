import logging
from neo4j import AsyncGraphDatabase
import os
from util.exception import Neo4jError

class Neo4jConnection:
    """Neo4j 데이터베이스와의 비동기 연결을 관리하는 클래스"""

    database_name = "neo4j"

    def __init__(self):
        """Neo4j 데이터베이스 연결 초기화

        환경변수를 통해 연결 정보를 설정하며, 설정되지 않은 경우 기본값을 사용합니다.
        """
        uri = os.getenv("NEO4J_URI", "bolt://localhost:7687")
        user = os.getenv("NEO4J_USER", "neo4j")
        password = os.getenv("NEO4J_PASSWORD", "an1021402")
        self._driver = AsyncGraphDatabase.driver(uri, auth=(user, password))


    async def close_connection(self):
        """데이터베이스 연결을 안전하게 종료하고 리소스를 정리합니다."""
        await self._driver.close()


    async def execute_cypher_queries(self, cypher_queries: list) -> list:
        """여러 개의 사이퍼 쿼리를 순차적으로 실행하고 결과를 수집합니다.

        Args:
            cypher_queries (list): 실행할 사이퍼 쿼리 문자열의 리스트

        Returns:
            list: 각 쿼리의 실행 결과를 담은 리스트
        """
        try:
            results = []
            async with self._driver.session(database=self.database_name) as session:
                for query in cypher_queries:
                    query_result = await session.run(query)
                    query_data = await query_result.data()
                    results.append(query_data)
            return results
        except Exception as e:
            error_message = f"사이퍼 쿼리 실행 중 오류 발생: {str(e)}"
            logging.exception(error_message)
            raise Neo4jError(error_message)


    async def fetch_graph_data(self, user_id: str, package_names: list) -> dict:
        """그래프 데이터베이스의 노드와 관계를 그래프 형태로 조회합니다.

        Args:
            user_id (str): 사용자 ID
            package_names (list): 패키지 이름 목록

        Returns:
            dict: 노드와 관계 정보를 포함하는 그래프 데이터 딕셔너리
        """
        try:
            # 기본 쿼리 설정
            default_query = f"""
            MATCH (n)-[r]->(m) 
            WHERE NOT n:Variable AND NOT n:PACKAGE_VARIABLE
            AND NOT m:Variable AND NOT m:PACKAGE_VARIABLE
            AND n.object_name IN $package_names
            AND m.object_name IN $package_names
            AND n.user_id = $user_id
            AND m.user_id = $user_id
            RETURN n, r, m
            """

            # 파라미터 설정
            params = {
                "package_names": package_names,
                "user_id": user_id
            }

            # 쿼리 실행
            async with self._driver.session(database=self.database_name) as session:
                result = await session.run(default_query, params)
                graph = await result.graph()

                # 노드 데이터 추출
                nodes_data = [
                    {
                        "Node ID": node.element_id,
                        "Labels": list(node.labels),
                        "Properties": dict(node),
                    }
                    for node in graph.nodes
                ]

                # 관계 데이터 추출
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

                # 쿼리 실행 결과 반환
                logging.info("쿼리 실행 성공")
                return {"Nodes": nodes_data, "Relationships": relationships_data}

        except Exception as e:
            error_message = f"그래프 데이터 조회 중 오류 발생: {str(e)}"
            logging.exception(error_message)
            raise Neo4jError(error_message)


    async def check_node_existence(self, user_id: str, package_names: list) -> bool:
        """이전에 사용자가 생성한 노드 존재 여부를 확인합니다.

        Args:
            user_id (str): 사용자 ID
            package_names (list): 패키지 이름 목록

        Returns:
            bool: 노드 존재 여부 (True 또는 False)
        """
        try:
            # 노드 존재 여부 확인 쿼리
            query = """
            MATCH (n)
            WHERE n.object_name IN $package_names
            AND n.user_id = $user_id
            RETURN COUNT(n) > 0 AS exists
            """

            # 파라미터 설정
            params = {
                "package_names": package_names,
                "user_id": user_id
            }

            # 쿼리 실행
            async with self._driver.session(database=self.database_name) as session:
                result = await session.run(query, params)
                record = await result.single()
                return record["exists"]

        except Exception as e:
            error_message = f"노드 존재 여부 확인 중 오류 발생: {str(e)}"
            logging.exception(error_message)
            raise Neo4jError(error_message)