import logging
import os
from neo4j import AsyncGraphDatabase
from util.exception import Neo4jError

class Neo4jConnection:
    """Neo4j 비동기 연결을 관리하고 쿼리 실행/그래프 조회를 제공합니다."""

    database_name = "neo4j"

    def __init__(self):
        """환경변수에서 연결 정보를 읽어 드라이버를 초기화합니다.

        환경변수:
        - NEO4J_URI (기본: bolt://localhost:7687)
        - NEO4J_USER (기본: neo4j)
        - NEO4J_PASSWORD (기본: an1021402)
        """
        uri = os.getenv("NEO4J_URI", "bolt://localhost:7687")
        user = os.getenv("NEO4J_USER", "neo4j")
        password = os.getenv("NEO4J_PASSWORD", "an1021402")
        self.__driver = AsyncGraphDatabase.driver(uri, auth=(user, password))


    async def close(self):
        """데이터베이스 연결을 안전하게 종료합니다."""
        await self.__driver.close()



    async def execute_queries(self, queries: list) -> list:
        """여러 사이퍼 쿼리를 순차 실행하고 결과를 리스트로 반환합니다.

        매개변수:
        - queries: 사이퍼 쿼리 문자열 리스트

        반환값:
        - list: 각 쿼리 결과(data()) 리스트
        """
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
    
    
    async def execute_query_and_return_graph(self, user_id: str, file_names: list, custom_query: str | None = None) -> dict:
        """노드/관계를 조회하여 그래프 형태의 딕셔너리로 반환합니다.

        매개변수:
        - user_id: 사용자 ID
        - file_names: (folder_name, file_name) 튜플 리스트
        - custom_query: 사용자 정의 조회 쿼리(선택)

        반환값:
        - dict: { Nodes: [...], Relationships: [...] }
        """
        try:
            pairs = [{"folder_name": f, "file_name": s} for f, s in file_names]
            query = custom_query or (
                """
                UNWIND $pairs as target
                MATCH (n)-[r]->(m)
                WHERE NOT n:Variable AND NOT n:PACKAGE_VARIABLE
                  AND NOT m:Variable AND NOT m:PACKAGE_VARIABLE
                  AND n.user_id = $user_id AND m.user_id = $user_id
                  AND ((n:Table OR (n.folder_name = target.folder_name AND n.file_name = target.file_name))
                       AND (m:Table OR (m.folder_name = target.folder_name AND m.file_name = target.file_name)))
                RETURN DISTINCT n, r, m
                """
            )
            params = {"user_id": user_id, "pairs": pairs}


            async with self.__driver.session(database=self.database_name) as session:
                result = await session.run(query, params)
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
        

    async def node_exists(self, user_id: str, file_names: list) -> bool:
        """사용자/파일 쌍 기준으로 그래프 내 노드 존재 여부를 반환합니다.

        매개변수:
        - user_id: 사용자 ID
        - file_names: (folder_name, file_name) 튜플 리스트

        반환값:
        - bool: 노드 존재 여부
        """
        try:
            pairs = [{"folder_name": f, "file_name": s} for f, s in file_names]
            query = """
            UNWIND $pairs as target
            MATCH (n)
            WHERE n.user_id = $user_id
              AND n.folder_name = target.folder_name
              AND n.file_name = target.file_name
            RETURN COUNT(n) > 0 AS exists
            """
            params = {"pairs": pairs, "user_id": user_id}

            async with self.__driver.session(database=self.database_name) as session:
                result = await session.run(query, params)
                record = await result.single()
                return record["exists"]
            
        except Exception as e:
            error_msg = f"노드 존재 여부 확인 중 오류 발생: {str(e)}"
            logging.exception(error_msg)
            raise Neo4jError(error_msg)