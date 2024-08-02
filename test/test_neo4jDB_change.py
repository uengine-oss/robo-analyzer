import os
import sys
import unittest
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))
from cypher.neo4j_connection import Neo4jConnection

class TestNeo4jConnection(unittest.IsolatedAsyncioTestCase):
    
    async def test_execute_queries(self):
        connection = Neo4jConnection()
        queries = ["CREATE (n:HUMAN {name: 'James Lis', age: 20})"]
        await connection.execute_queries(queries)
        await connection.close()

if __name__ == '__main__':
    unittest.main()