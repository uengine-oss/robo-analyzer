import unittest
import sys
import os
import logging
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..', '..')))
from understand.neo4j_connection import Neo4jConnection
from semantic.vectorizer import vectorize_text

logging.basicConfig(level=logging.INFO)

class TestVectorSimilarity(unittest.IsolatedAsyncioTestCase):

    async def test_find_similar_nodes(self):
        
        conn = Neo4jConnection()

        # 테스트할 검색 텍스트
        search_text = "TPJ_PROJECT 테이블에서 일치하는 PROJ_KEY 찾기"
        
        # 검색 텍스트를 벡터화
        search_vector = vectorize_text(search_text)
        
        # Neo4j 쿼리 실행
        nodes = await conn.search_similar_nodes(
            search_vector=search_vector,
            similarity_threshold=0.5,
            limit=5
        )

        # 결과 출력 및 검증
        print("\n=== 유사도 검색 결과 ===")
        print(f"검색어: {search_text}\n")
        
        for node in nodes:
            print(f"유사도: {node['similarity']:.4f}")
            print(f"노드 이름: {node['name']}")
            print(f"요약: {node['summary']}\n")

        self.assertTrue(len(nodes) > 0, "검색 결과가 없습니다")

if __name__ == '__main__':
    unittest.main()