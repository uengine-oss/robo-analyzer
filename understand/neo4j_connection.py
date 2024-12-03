import logging
from neo4j import AsyncGraphDatabase
import os
from util.exception import Neo4jError

class Neo4jConnection:

    database_name = "note"

    # 역할 : Neo4j 데이터베이스와의 연결을 초기화합니다. 환경변수를 통해 연결 정보를 설정하며, 설정되지 않은 경우 기본값을 사용합니다.
    # 매개변수:
    #   - uri: 데이터베이스 URI (기본값: "bolt://localhost:실제 포트 번호")
    #   - user: 데이터베이스 사용자 이름 (기본값: "neo4j")
    #   - password: 데이터베이스 비밀번호 (기본값: "neo4j")
    def __init__(self):
        uri = os.getenv("NEO4J_URI", "bolt://localhost:7689")
        user = os.getenv("NEO4J_USER", "neo4j")
        password = os.getenv("NEO4J_PASSWORD", "an1021402!@")
        self.__driver = AsyncGraphDatabase.driver(uri, auth=(user, password))


    # 역할: 데이터베이스 연결을 안전하게 종료하고 리소스를 정리합니다.
    async def close(self):
        await self.__driver.close()



    # 역할: 여러 개의 사이퍼 쿼리를 순차적으로 실행하고 결과를 수집합니다.
    # 매개변수: 
    #   - queries: 실행할 사이퍼 쿼리 문자열의 리스트
    # 반환값: 
    #   - results: 각 쿼리의 실행 결과를 담은 리스트
    async def execute_queries(self, queries):
        try:
            results = [] 
            async with self.__driver.session(database=self.database_name) as session:
                for query in queries:
                    query_result = await session.run(query)
                    query_data = await query_result.data()
                    results.append(query_data)
            return results
        except Exception:
            error_msg = "Cypher Query를 실행하여, 노드 및 관계를 생성하는 도중 오류가 발생"
            logging.exception(error_msg)
            raise Neo4jError(error_msg)
    
    
    # 역할: 그래프 데이터베이스의 노드와 관계를 시각화 가능한 형태로 조회합니다.
    #      Variable 라벨을 가진 노드는 제외하고 조회합니다.
    # 매개변수: 
    #   - custom_query: 사용자가 정의한 조회 쿼리 (선택적)
    # 반환값: 
    #   - graph_data: 노드와 관계 정보를 포함하는 그래프 데이터 딕셔너리
    async def execute_query_and_return_graph(self, custom_query=None):
        try:
            default_query = custom_query or "MATCH (n)-[r]->(m) WHERE NOT 'Variable' IN labels(n) AND NOT 'Variable' IN labels(m) RETURN n, r, m"
            async with self.__driver.session(database=self.database_name) as session:
                result = await session.run(default_query)
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
            
        except Exception:
            error_msg = "Neo4J에서 그래프 객체 형태로 결과를 반환하는 도중 문제가 발생"
            logging.exception(error_msg)
            raise Neo4jError(error_msg)