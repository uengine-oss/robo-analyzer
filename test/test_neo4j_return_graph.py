"""
Neo4j execute_query_and_return_graph 메서드 테스트

임의의 Cypher 쿼리를 실행하고 반환되는 데이터 구조를 로그로 출력합니다.
"""
import asyncio
import json
import os
import sys
from pathlib import Path

# 프로젝트 루트를 경로에 추가
PROJECT_ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(PROJECT_ROOT))

from understand.neo4j_connection import Neo4jConnection

# 한글 로그가 깨지지 않도록 UTF-8 인코딩
os.environ.setdefault("PYTHONIOENCODING", "utf-8")


class TestNeo4jConnection(Neo4jConnection):
    """테스트용 Neo4j 연결 - 'test' 데이터베이스 사용"""
    DATABASE_NAME = "test"


def print_json(data, indent=2):
    """JSON 데이터를 보기 좋게 출력"""
    print(json.dumps(data, ensure_ascii=False, indent=indent))


async def main():
    """메인 테스트 실행"""
    connection = TestNeo4jConnection()
    
    try:
        # 1. 초기 노드 생성
        queries1 = [
            "MERGE (a:Person {id: 'person1', name: 'Alice', age: 30}) RETURN a",
            "MERGE (b:Person {id: 'person2', name: 'Bob', age: 25}) RETURN b",
        ]
        
        print("\n" + "="*80)
        print("쿼리 실행:")
        for q in queries1:
            print(f"  {q}")
        print("\n결과:")
        result1 = await connection.execute_query_and_return_graph(queries1)
        print_json(result1)
        
        # 2. 노드 업데이트
        queries2 = [
            "MATCH (a:Person {id: 'person1'}) SET a.age = 35 RETURN a",
        ]
        
        print("\n" + "="*80)
        print("쿼리 실행:")
        for q in queries2:
            print(f"  {q}")
        print("\n결과:")
        result2 = await connection.execute_query_and_return_graph(queries2)
        print_json(result2)
        
        # 3. 새 노드 추가
        queries3 = [
            "MERGE (c:Person {id: 'person3', name: 'Charlie', age: 35}) RETURN c",
        ]
        
        print("\n" + "="*80)
        print("쿼리 실행:")
        for q in queries3:
            print(f"  {q}")
        print("\n결과:")
        result3 = await connection.execute_query_and_return_graph(queries3)
        print_json(result3)
        
        # 4. 관계 생성
        queries4 = [
            "MATCH (a:Person {id: 'person1'}) "
            "MATCH (b:Person {id: 'person2'}) "
            "MERGE (a)-[r:KNOWS]->(b) RETURN a, r, b",
            "MATCH (b:Person {id: 'person2'}) "
            "MATCH (c:Person {id: 'person3'}) "
            "MERGE (b)-[r:KNOWS]->(c) RETURN b, r, c",
        ]
        
        print("\n" + "="*80)
        print("쿼리 실행:")
        for q in queries4:
            print(f"  {q}")
        print("\n결과:")
        result4 = await connection.execute_query_and_return_graph(queries4)
        print_json(result4)
        
        # 5. 관계 속성 업데이트
        queries5 = [
            "MATCH (a:Person {id: 'person1'})-[r:KNOWS]->(b:Person {id: 'person2'}) "
            "SET r.since = 2023 RETURN a, r, b",
        ]
        
        print("\n" + "="*80)
        print("쿼리 실행:")
        for q in queries5:
            print(f"  {q}")
        print("\n결과:")
        result5 = await connection.execute_query_and_return_graph(queries5)
        print_json(result5)
        
        # 6. 노드 속성 업데이트 + 관계 연결
        queries6 = [
            "MATCH (a:Person {id: 'person1'}) SET a.city = 'Seoul' RETURN a",
            "MATCH (b:Person {id: 'person2'}) SET b.city = 'Busan' RETURN b",
            "MATCH (a:Person {id: 'person1'}) RETURN a",
            "MATCH (a:Person {id: 'person1'}) "
            "MATCH (c:Person {id: 'person3'}) "
            "MERGE (a)-[r:KNOWS]->(c) RETURN a, r, c",
        ]
        
        print("\n" + "="*80)
        print("쿼리 실행:")
        for q in queries6:
            print(f"  {q}")
        print("\n결과:")
        result6 = await connection.execute_query_and_return_graph(queries6)
        print_json(result6)
        
        # 7. 여러 관계 생성
        queries7 = [
            "MATCH (a:Person {id: 'person1'}) "
            "MATCH (b:Person {id: 'person2'}) "
            "MERGE (a)-[r1:FOLLOWS]->(b) RETURN a, r1, b",
            "MATCH (b:Person {id: 'person2'}) "
            "MATCH (c:Person {id: 'person3'}) "
            "MERGE (b)-[r2:FOLLOWS]->(c) RETURN b, r2, c",
            "MATCH (c:Person {id: 'person3'}) "
            "MATCH (a:Person {id: 'person1'}) "
            "MERGE (c)-[r3:FOLLOWS]->(a) RETURN c, r3, a",
        ]
        
        print("\n" + "="*80)
        print("쿼리 실행:")
        for q in queries7:
            print(f"  {q}")
        print("\n결과:")
        result7 = await connection.execute_query_and_return_graph(queries7)
        print_json(result7)
        
        # 정리
        cleanup_queries = [
            "MATCH (n:Person) DETACH DELETE n",
        ]
        await connection.execute_queries(cleanup_queries)
        
    except Exception as e:
        print(f"\n❌ 테스트 실패: {e}")
        import traceback
        traceback.print_exc()
    finally:
        await connection.close()


if __name__ == "__main__":
    asyncio.run(main())
