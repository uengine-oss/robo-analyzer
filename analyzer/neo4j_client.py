"""Neo4j 비동기 클라이언트

Cypher 쿼리 실행 및 그래프 데이터 관리를 담당합니다.

주요 기능:
- 비동기 쿼리 실행
- 그래프 결과 반환 (노드/관계)
- 제약조건 관리
- 배치 쿼리 실행 (동시성 안전)
"""

import asyncio
import logging
import warnings
from typing import Any, Optional

from neo4j import AsyncGraphDatabase

from config.settings import settings
from util.exception import Neo4jError, QueryExecutionError, Neo4jConnectionError

# Neo4j notification warnings 무시
warnings.filterwarnings("ignore", category=DeprecationWarning, module="neo4j")
warnings.filterwarnings("ignore", message=".*Received notification from DBMS server.*")
logging.getLogger("neo4j.notifications").setLevel(logging.ERROR)


class Neo4jClient:
    """Neo4j 비동기 연결 관리 및 쿼리 실행
    
    사용법:
        client = Neo4jClient()
        try:
            result = await client.execute_queries([query1, query2])
        finally:
            await client.close()
    
    또는 async with 구문 사용:
        async with Neo4jClient() as client:
            result = await client.execute_queries([query])
    """

    __slots__ = ("_driver", "_lock", "_config", "_database")

    # 유니크 제약조건 쿼리
    _CONSTRAINT_QUERIES = [
        "CREATE CONSTRAINT table_unique IF NOT EXISTS FOR (t:Table) "
        "REQUIRE (t.db, t.schema, t.name) IS UNIQUE",
        "CREATE CONSTRAINT column_unique IF NOT EXISTS FOR (c:Column) "
        "REQUIRE (c.fqn) IS UNIQUE",
    ]

    def __init__(self, database: Optional[str] = None):
        """환경변수에서 연결 정보를 읽어 드라이버 초기화
        
        Args:
            database: 사용할 데이터베이스 이름. None이면 settings.neo4j.database 사용
        """
        self._config = settings.neo4j
        self._database = database if database is not None else self._config.database
        self._driver = AsyncGraphDatabase.driver(
            self._config.uri,
            auth=(self._config.user, self._config.password),
        )
        self._lock = asyncio.Lock()

    async def __aenter__(self):
        return self

    async def __aexit__(self, exc_type, exc_val, exc_tb):
        await self.close()

    async def close(self):
        """데이터베이스 연결 종료"""
        if self._driver:
            await self._driver.close()

    # =========================================================================
    # 쿼리 실행
    # =========================================================================

    async def execute_queries(self, queries: list[str]) -> list[Any]:
        """Cypher 쿼리 순차 실행 및 결과 반환
        
        Args:
            queries: 실행할 Cypher 쿼리 리스트
            
        Returns:
            각 쿼리의 결과 데이터 리스트
            
        Raises:
            QueryExecutionError: 쿼리 실행 실패 시
        """
        if not queries:
            return []
        
        try:
            results = []
            async with self._driver.session(database=self._database) as session:
                for query in queries:
                    query_result = await session.run(query)
                    results.append(await query_result.data())
            return results
        except Exception as e:
            raise QueryExecutionError(
                "Cypher 쿼리 실행 중 오류 발생",
                query_count=len(queries),
                cause=e,
            )

    async def execute_queries_batch(self, queries: list[str]) -> list[Any]:
        """배치 쿼리 실행 (락으로 동시성 보호)
        
        여러 파일에서 동시에 쿼리를 실행할 때 사용합니다.
        """
        async with self._lock:
            return await self.execute_queries(queries)

    async def run_graph_query(self, queries: list[str]) -> dict[str, list]:
        """쿼리 실행 후 영향받은 노드/관계를 그래프 형태로 반환
        
        Args:
            queries: 실행할 Cypher 쿼리 리스트
            
        Returns:
            {"Nodes": [...], "Relationships": [...]} 형태의 딕셔너리
        """
        if not queries:
            return {"Nodes": [], "Relationships": []}
        
        try:
            nodes: dict[str, dict] = {}
            relationships: dict[str, dict] = {}

            async with self._driver.session(database=self._database) as session:
                for query in queries:
                    graph = await (await session.run(query)).graph()

                    for node in graph.nodes:
                        self._collect_node(node, nodes)

                    for rel in graph.relationships:
                        relationships[rel.element_id] = {
                            "Relationship ID": rel.element_id,
                            "Type": rel.type,
                            "Properties": dict(rel),
                            "Start Node ID": rel.start_node.element_id,
                            "End Node ID": rel.end_node.element_id,
                        }
                        self._collect_node(rel.start_node, nodes)
                        self._collect_node(rel.end_node, nodes)

            return {
                "Nodes": list(nodes.values()),
                "Relationships": list(relationships.values()),
            }
        except Exception as e:
            raise QueryExecutionError(
                "그래프 쿼리 실행 중 오류 발생",
                query_count=len(queries),
                cause=e,
            )

    @staticmethod
    def _collect_node(node, nodes_dict: dict) -> None:
        """노드를 결과 딕셔너리에 추가 (중복/빈 노드 제외)"""
        if node.element_id in nodes_dict:
            return
        labels, props = list(node.labels), dict(node)
        if not labels and not props:
            return
        nodes_dict[node.element_id] = {
            "Node ID": node.element_id,
            "Labels": labels,
            "Properties": props,
        }

    # =========================================================================
    # 유틸리티
    # =========================================================================

    async def ensure_constraints(self) -> None:
        """MERGE 시 중복/충돌 방지를 위한 유니크 제약조건 생성"""
        try:
            async with self._driver.session(database=self._database) as session:
                for query in self._CONSTRAINT_QUERIES:
                    try:
                        await session.run(query)
                    except Exception:
                        logging.debug(f"제약조건 생성 스킵 (이미 존재): {query[:50]}...")
        except Exception as e:
            logging.warning(f"제약조건 보장 중 경고: {e}")

    async def check_nodes_exist(
        self,
        file_names: list[tuple[str, str]],
    ) -> bool:
        """지정된 파일에 해당하는 노드 존재 여부 확인
        
        Args:
            file_names: [(directory, file_name), ...] 튜플 리스트
            
        Returns:
            하나라도 존재하면 True
        """
        if not file_names:
            return False
        
        query = """
            UNWIND $pairs as target
            MATCH (n)
            WHERE n.directory = target.directory
              AND n.file_name = target.file_name
            RETURN COUNT(n) > 0 AS exists
        """
        
        try:
            pairs = [{"directory": d, "file_name": f} for d, f in file_names]
            params = {"pairs": pairs}

            async with self._driver.session(database=self._database) as session:
                result = await session.run(query, params)
                record = await result.single()
                return record["exists"] if record else False
        except Exception as e:
            raise QueryExecutionError(
                "노드 존재 여부 확인 중 오류",
                cause=e,
            )

