"""
Neo4j execute_query_and_return_graph 메서드 테스트

실제 DBMS 로직에서 사용하는 모든 Cypher 쿼리 패턴을 테스트합니다.
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
    """메인 테스트 실행 - 실제 DBMS 로직의 모든 쿼리 패턴 테스트"""
    connection = TestNeo4jConnection()
    
    # 테스트용 상수 (실제 로직과 동일한 형식)
    test_user_id = "test_user"
    test_system_name = "test_system"
    test_file_name = "test_file.sql"
    test_project_name = "test_project"
    test_dbms = "postgres"
    
    test_node_base_props = f"system_name: '{test_system_name}', file_name: '{test_file_name}', user_id: '{test_user_id}', project_name: '{test_project_name}'"
    test_system_props = f"user_id: '{test_user_id}', system_name: '{test_system_name}', project_name: '{test_project_name}'"
    test_table_base_props = f"user_id: '{test_user_id}'"
    
    try:
        # ========== 1. SYSTEM 노드 생성 (실제 로직: _ensure_system_node) ==========
        queries1 = [
            f"MERGE (f:SYSTEM {{user_id: '{test_user_id}', system_name: '{test_system_name}', project_name: '{test_project_name}', has_children: true}}) RETURN f"
        ]
        
        print("\n" + "="*80)
        print("1. SYSTEM 노드 생성 (실제 로직: _ensure_system_node)")
        print("쿼리 실행:")
        for q in queries1:
            print(f"  {q}")
        print("\n결과:")
        result1 = await connection.execute_query_and_return_graph(queries1)
        print_json(result1)
        
        # ========== 2. FILE 노드 생성 (실제 로직: _build_static_node_queries - FILE) ==========
        queries2 = [
            f"MERGE (n:FILE {{startLine: 1, {test_node_base_props}}})\n"
            f"SET n.endLine = 1, n.name = '{test_file_name}', n.summary = '파일 노드',\n"
            f"    n.has_children = true\n"
            f"WITH n\n"
            f"MERGE (system:SYSTEM {{{test_system_props}}})\n"
            f"MERGE (system)-[r:CONTAINS]->(n)\n"
            f"RETURN n, r"
        ]
        
        print("\n" + "="*80)
        print("2. FILE 노드 생성 (실제 로직: _build_static_node_queries - FILE)")
        print("쿼리 실행:")
        for q in queries2:
            print(f"  {q}")
        print("\n결과:")
        result2 = await connection.execute_query_and_return_graph(queries2)
        print_json(result2)
        
        # ========== 3. 리프 노드 생성 (실제 로직: _build_static_node_queries - 리프) ==========
        queries3 = [
            f"MERGE (n:SELECT {{startLine: 10, {test_node_base_props}}})\n"
            f"SET n.endLine = 20, n.name = 'SELECT[10]', n.node_code = 'SELECT * FROM users',\n"
            f"    n.token = 50, n.procedure_name = '', n.has_children = false\n"
            f"WITH n\n"
            f"MERGE (system:SYSTEM {{{test_system_props}}})\n"
            f"MERGE (system)-[r:CONTAINS]->(n)\n"
            f"RETURN n, r",
            
            f"MERGE (n:INSERT {{startLine: 25, {test_node_base_props}}})\n"
            f"SET n.endLine = 30, n.name = 'INSERT[25]', n.node_code = 'INSERT INTO users VALUES (1)',\n"
            f"    n.token = 60, n.procedure_name = '', n.has_children = false\n"
            f"WITH n\n"
            f"MERGE (system:SYSTEM {{{test_system_props}}})\n"
            f"MERGE (system)-[r:CONTAINS]->(n)\n"
            f"RETURN n, r"
        ]
        
        print("\n" + "="*80)
        print("3. 리프 노드 생성 (실제 로직: _build_static_node_queries - 리프)")
        print("쿼리 실행:")
        for q in queries3:
            print(f"  {q}")
        print("\n결과:")
        result3 = await connection.execute_query_and_return_graph(queries3)
        print_json(result3)
        
        # ========== 4. 부모 노드 생성 (실제 로직: _build_static_node_queries - 부모) ==========
        queries4 = [
            f"MERGE (n:IF {{startLine: 5, {test_node_base_props}}})\n"
            f"SET n.endLine = 35, n.name = 'IF[5]', n.summarized_code = 'IF ...code...',\n"
            f"    n.node_code = 'IF condition THEN ... END IF', n.token = 100, n.procedure_name = '', n.has_children = true\n"
            f"WITH n\n"
            f"MERGE (system:SYSTEM {{{test_system_props}}})\n"
            f"MERGE (system)-[r:CONTAINS]->(n)\n"
            f"RETURN n, r"
        ]
        
        print("\n" + "="*80)
        print("4. 부모 노드 생성 (실제 로직: _build_static_node_queries - 부모)")
        print("쿼리 실행:")
        for q in queries4:
            print(f"  {q}")
        print("\n결과:")
        result4 = await connection.execute_query_and_return_graph(queries4)
        print_json(result4)
        
        # ========== 5. 노드 업데이트 (실제 로직: _build_node_queries - summary 포함) ==========
        queries5 = [
            f"MERGE (n:SELECT {{startLine: 10, {test_node_base_props}}})\n"
            f"SET n.endLine = 20, n.name = 'SELECT[10]', n.summary = '사용자 조회',\n"
            f"    n.node_code = 'SELECT * FROM users', n.token = 50, n.procedure_name = '', n.has_children = false\n"
            f"WITH n\n"
            f"MERGE (system:SYSTEM {{{test_system_props}}})\n"
            f"MERGE (system)-[r:CONTAINS]->(n)\n"
            f"RETURN n, r"
        ]
        
        print("\n" + "="*80)
        print("5. 노드 속성 업데이트 (실제 로직: _build_node_queries - summary 포함)")
        print("쿼리 실행:")
        for q in queries5:
            print(f"  {q}")
        print("\n결과:")
        result5 = await connection.execute_query_and_return_graph(queries5)
        print_json(result5)
        
        # ========== 6. Variable 노드 마킹 (실제 로직: _build_node_queries - variables) ==========
        # 실제 로직에서는 Variable 노드가 먼저 생성된 후 마킹되므로, 테스트도 동일한 순서로 진행
        queries6_prep = [
            f"MERGE (v:Variable {{name: 'user_id', {test_node_base_props}, type: 'VARCHAR', parameter_type: '', value: ''}})\n"
            f"WITH v\n"
            f"MERGE (system:SYSTEM {{{test_system_props}}})\n"
            f"MERGE (system)-[r:CONTAINS]->(v)\n"
            f"RETURN v, r"
        ]
        await connection.execute_queries(queries6_prep)
        
        queries6 = [
            f"MATCH (v:Variable {{name: 'user_id', {test_node_base_props}}})\n"
            f"SET v.`10_20` = 'Used'\n"
            f"RETURN v"
        ]
        
        print("\n" + "="*80)
        print("6. Variable 노드 마킹 (실제 로직: _build_node_queries - variables)")
        print("쿼리 실행:")
        for q in queries6:
            print(f"  {q}")
        print("\n결과:")
        result6 = await connection.execute_query_and_return_graph(queries6)
        print_json(result6)
        
        # ========== 7. 내부 CALL 관계 (실제 로직: _build_node_queries - 내부 호출) ==========
        queries7_prep = [
            f"MERGE (p:PROCEDURE {{startLine: 1, {test_node_base_props}, procedure_name: 'test_proc'}})\n"
            f"SET p.endLine = 100, p.name = 'PROCEDURE[1]', p.has_children = true\n"
            f"WITH p\n"
            f"MERGE (system:SYSTEM {{{test_system_props}}})\n"
            f"MERGE (system)-[r:CONTAINS]->(p)\n"
            f"RETURN p, r"
        ]
        await connection.execute_queries(queries7_prep)
        
        queries7 = [
            f"MATCH (c:SELECT {{startLine: 10, {test_node_base_props}}})\n"
            f"WITH c\n"
            f"MATCH (p {{procedure_name: 'test_proc', {test_node_base_props}}})\n"
            f"WHERE p:PROCEDURE OR p:FUNCTION\n"
            f"MERGE (c)-[r:CALL {{scope: 'internal'}}]->(p)\n"
            f"RETURN r"
        ]
        
        print("\n" + "="*80)
        print("7. 내부 CALL 관계 (실제 로직: _build_node_queries - 내부 호출)")
        print("쿼리 실행:")
        for q in queries7:
            print(f"  {q}")
        print("\n결과:")
        result7 = await connection.execute_query_and_return_graph(queries7)
        print_json(result7)
        
        # ========== 8. 외부 CALL 관계 (실제 로직: _build_node_queries - 외부 호출) ==========
        queries8 = [
            f"MATCH (c:SELECT {{startLine: 10, {test_node_base_props}}})\n"
            f"OPTIONAL MATCH (p)\n"
            f"WHERE (p:PROCEDURE OR p:FUNCTION)\n"
            f"  AND p.system_name = 'external_package'\n"
            f"  AND p.procedure_name = 'external_proc'\n"
            f"  AND p.user_id = '{test_user_id}'\n"
            f"WITH c, p\n"
            f"MERGE (target:PROCEDURE:FUNCTION {{system_name: 'external_package', procedure_name: 'external_proc', user_id: '{test_user_id}', project_name: '{test_project_name}'}})\n"
            f"MERGE (c)-[r:CALL {{scope: 'external'}}]->(target)\n"
            f"RETURN r"
        ]
        
        print("\n" + "="*80)
        print("8. 외부 CALL 관계 (실제 로직: _build_node_queries - 외부 호출)")
        print("쿼리 실행:")
        for q in queries8:
            print(f"  {q}")
        print("\n결과:")
        result8 = await connection.execute_query_and_return_graph(queries8)
        print_json(result8)
        
        # ========== 9. PARENT_OF 관계 (실제 로직: _build_parent_relationship_query) ==========
        queries9 = [
            f"MATCH (parent:IF {{startLine: 5, {test_node_base_props}}})\n"
            f"MATCH (child:SELECT {{startLine: 10, {test_node_base_props}}})\n"
            f"MERGE (parent)-[r:PARENT_OF]->(child)\n"
            f"RETURN r",
            
            f"MATCH (parent:IF {{startLine: 5, {test_node_base_props}}})\n"
            f"MATCH (child:INSERT {{startLine: 25, {test_node_base_props}}})\n"
            f"MERGE (parent)-[r:PARENT_OF]->(child)\n"
            f"RETURN r"
        ]
        
        print("\n" + "="*80)
        print("9. PARENT_OF 관계 (실제 로직: _build_parent_relationship_query)")
        print("쿼리 실행:")
        for q in queries9:
            print(f"  {q}")
        print("\n결과:")
        result9 = await connection.execute_query_and_return_graph(queries9)
        print_json(result9)
        
        # ========== 10. NEXT 관계 (실제 로직: _build_next_relationship_query) ==========
        queries10 = [
            f"MATCH (prev:SELECT {{startLine: 10, {test_node_base_props}}})\n"
            f"MATCH (current:INSERT {{startLine: 25, {test_node_base_props}}})\n"
            f"MERGE (prev)-[r:NEXT]->(current)\n"
            f"RETURN r"
        ]
        
        print("\n" + "="*80)
        print("10. NEXT 관계 (실제 로직: _build_next_relationship_query)")
        print("쿼리 실행:")
        for q in queries10:
            print(f"  {q}")
        print("\n결과:")
        result10 = await connection.execute_query_and_return_graph(queries10)
        print_json(result10)
        
        # ========== 11. 테이블 노드 및 FROM/WRITES 관계 (실제 로직: _build_table_queries) ==========
        queries11 = [
            f"MERGE (n:SELECT {{startLine: 10, {test_node_base_props}}})\n"
            f"WITH n\n"
            f"MERGE (t:Table {{{test_table_base_props}, name: 'users', schema: 'public', db: '{test_dbms}', project_name: '{test_project_name}'}})\n"
            f"WITH n, t\n"
            f"MERGE (system:SYSTEM {{{test_system_props}}})\n"
            f"MERGE (system)-[r_system:CONTAINS]->(t)\n"
            f"SET t.db = coalesce(t.db, '{test_dbms}')\n"
            f"MERGE (n)-[r0:FROM]->(t)\n"
            f"RETURN n, t, r_system, r0",
            
            f"MERGE (n:INSERT {{startLine: 25, {test_node_base_props}}})\n"
            f"WITH n\n"
            f"MERGE (t:Table {{{test_table_base_props}, name: 'users', schema: 'public', db: '{test_dbms}', project_name: '{test_project_name}'}})\n"
            f"WITH n, t\n"
            f"MERGE (system:SYSTEM {{{test_system_props}}})\n"
            f"MERGE (system)-[r_system:CONTAINS]->(t)\n"
            f"SET t.db = coalesce(t.db, '{test_dbms}')\n"
            f"MERGE (n)-[r0:WRITES]->(t)\n"
            f"RETURN n, t, r_system, r0"
        ]
        
        print("\n" + "="*80)
        print("11. 테이블 노드 및 FROM/WRITES 관계 (실제 로직: _build_table_queries)")
        print("쿼리 실행:")
        for q in queries11:
            print(f"  {q}")
        print("\n결과:")
        result11 = await connection.execute_query_and_return_graph(queries11)
        print_json(result11)
        
        # ========== 12. CREATE_TEMP_TABLE (실제 로직: _build_table_queries - CREATE_TEMP_TABLE) ==========
        queries12 = [
            f"MERGE (n:CREATE_TEMP_TABLE {{startLine: 40, {test_node_base_props}}})\n"
            f"SET n:Table, n.name = 'temp_users', n.schema = '', n.db = '{test_dbms}'\n"
            f"RETURN n"
        ]
        
        print("\n" + "="*80)
        print("12. CREATE_TEMP_TABLE (실제 로직: _build_table_queries - CREATE_TEMP_TABLE)")
        print("쿼리 실행:")
        for q in queries12:
            print(f"  {q}")
        print("\n결과:")
        result12 = await connection.execute_query_and_return_graph(queries12)
        print_json(result12)
        
        # ========== 13. 컬럼 노드 및 HAS_COLUMN 관계 - 스키마 있음 (실제 로직: _build_table_queries) ==========
        queries13 = [
            f"MERGE (t:Table {{{test_table_base_props}, name: 'users', schema: 'public', db: '{test_dbms}', project_name: '{test_project_name}'}})\n"
            f"WITH t\n"
            f"MERGE (c:Column {{`user_id`: '{test_user_id}', `fqn`: 'public.users.id', `project_name`: '{test_project_name}'}})\n"
            f"SET c.`name` = 'id', c.`dtype` = 'INTEGER', c.`description` = '사용자 ID', c.`nullable` = 'false', c.`fqn` = 'public.users.id'\n"
            f"WITH t, c\n"
            f"MERGE (t)-[r:HAS_COLUMN]->(c)\n"
            f"RETURN r"
        ]
        
        print("\n" + "="*80)
        print("13. 컬럼 노드 및 HAS_COLUMN 관계 - 스키마 있음 (실제 로직: _build_table_queries)")
        print("쿼리 실행:")
        for q in queries13:
            print(f"  {q}")
        print("\n결과:")
        result13 = await connection.execute_query_and_return_graph(queries13)
        print_json(result13)
        
        # ========== 14. 컬럼 노드 및 HAS_COLUMN 관계 - 스키마 없음 (실제 로직: _build_table_queries) ==========
        queries14 = [
            f"MERGE (t:Table {{{test_table_base_props}, name: 'products', schema: '', db: '{test_dbms}', project_name: '{test_project_name}'}})\n"
            f"WITH t\n"
            f"OPTIONAL MATCH (existing_col:Column)-[:HAS_COLUMN]-(t)\n"
            f"WHERE existing_col.`name` = 'product_id' AND existing_col.`user_id` = '{test_user_id}' AND existing_col.`project_name` = '{test_project_name}'\n"
            f"WITH t, existing_col\n"
            f"WHERE existing_col IS NULL\n"
            f"WITH t, lower(case when t.schema <> '' and t.schema IS NOT NULL then t.schema + '.' + 'products' + '.' + 'product_id' else 'products' + '.' + 'product_id' end) as fqn\n"
            f"CREATE (c:Column {{`user_id`: '{test_user_id}', `fqn`: fqn, `project_name`: '{test_project_name}', "
            f"`name`: 'product_id', `dtype`: 'VARCHAR', `description`: '제품 ID', `nullable`: 'true'}})\n"
            f"WITH t, c\n"
            f"MERGE (t)-[r:HAS_COLUMN]->(c)\n"
            f"RETURN r"
        ]
        
        print("\n" + "="*80)
        print("14. 컬럼 노드 및 HAS_COLUMN 관계 - 스키마 없음 (실제 로직: _build_table_queries)")
        print("쿼리 실행:")
        for q in queries14:
            print(f"  {q}")
        print("\n결과:")
        result14 = await connection.execute_query_and_return_graph(queries14)
        print_json(result14)
        
        # ========== 15. DB_LINK 관계 (실제 로직: _build_table_queries - dbLinks) ==========
        queries15 = [
            f"MERGE (t:Table {{{test_table_base_props}, name: 'remote_table', schema: 'remote_schema', db: '{test_dbms}', project_name: '{test_project_name}'}})\n"
            f"SET t.db_link = 'DB_LINK_NAME'\n"
            f"WITH t\n"
            f"MERGE (l:DBLink {{user_id: '{test_user_id}', name: 'DB_LINK_NAME', project_name: '{test_project_name}'}})\n"
            f"MERGE (l)-[r1:CONTAINS]->(t)\n"
            f"WITH t, l, r1\n"
            f"MERGE (n:SELECT {{startLine: 10, {test_node_base_props}}})\n"
            f"MERGE (n)-[r2:DB_LINK {{mode: 'r'}}]->(t)\n"
            f"RETURN r1, r2"
        ]
        
        print("\n" + "="*80)
        print("15. DB_LINK 관계 (실제 로직: _build_table_queries - dbLinks)")
        print("쿼리 실행:")
        for q in queries15:
            print(f"  {q}")
        print("\n결과:")
        result15 = await connection.execute_query_and_return_graph(queries15)
        print_json(result15)
        
        # ========== 16. FK_TO_TABLE 관계 (실제 로직: _build_table_queries - fkRelations) ==========
        queries16_prep = [
            f"MERGE (t:Table {{{test_table_base_props}, name: 'orders', schema: 'public', db: '{test_dbms}', project_name: '{test_project_name}'}})\n"
            f"WITH t\n"
            f"MERGE (system:SYSTEM {{{test_system_props}}})\n"
            f"MERGE (system)-[r:CONTAINS]->(t)\n"
            f"RETURN t, r"
        ]
        await connection.execute_queries(queries16_prep)
        
        queries16 = [
            f"MATCH (st:Table {{{test_table_base_props}, schema: 'public', name: 'orders', db: '{test_dbms}', project_name: '{test_project_name}'}})\n"
            f"MATCH (tt:Table {{{test_table_base_props}, schema: 'public', name: 'users', db: '{test_dbms}', project_name: '{test_project_name}'}})\n"
            f"MERGE (st)-[r:FK_TO_TABLE]->(tt)\n"
            f"RETURN r"
        ]
        
        print("\n" + "="*80)
        print("16. FK_TO_TABLE 관계 (실제 로직: _build_table_queries - fkRelations)")
        print("쿼리 실행:")
        for q in queries16:
            print(f"  {q}")
        print("\n결과:")
        result16 = await connection.execute_query_and_return_graph(queries16)
        print_json(result16)
        
        # ========== 17. FK_TO 관계 - 컬럼 간 (실제 로직: _build_table_queries - fkRelations) ==========
        queries17_prep = [
            f"MERGE (sc:Column {{user_id: '{test_user_id}', name: 'order_id', fqn: 'public.orders.order_id', project_name: '{test_project_name}'}})\n"
            f"RETURN sc",
            f"MERGE (dc:Column {{user_id: '{test_user_id}', name: 'id', fqn: 'public.users.id', project_name: '{test_project_name}'}})\n"
            f"RETURN dc"
        ]
        await connection.execute_queries(queries17_prep)
        
        queries17 = [
            f"MATCH (sc:Column {{user_id: '{test_user_id}', name: 'order_id', fqn: 'public.orders.order_id'}})\n"
            f"MATCH (dc:Column {{user_id: '{test_user_id}', name: 'id', fqn: 'public.users.id'}})\n"
            f"MERGE (sc)-[r:FK_TO]->(dc)\n"
            f"RETURN r"
        ]
        
        print("\n" + "="*80)
        print("17. FK_TO 관계 - 컬럼 간 (실제 로직: _build_table_queries - fkRelations)")
        print("쿼리 실행:")
        for q in queries17:
            print(f"  {q}")
        print("\n결과:")
        result17 = await connection.execute_query_and_return_graph(queries17)
        print_json(result17)
        
        # ========== 18. Variable 노드 및 SCOPE 관계 (실제 로직: _build_variable_queries) ==========
        queries18_prep = [
            f"MERGE (p:DECLARE {{startLine: 15, {test_node_base_props}, procedure_name: 'test_proc'}})\n"
            f"SET p.summary = '변수 선언'\n"
            f"RETURN p"
        ]
        await connection.execute_queries(queries18_prep)
        
        queries18 = [
            f"MERGE (v:Variable {{name: 'user_id', {test_node_base_props}, type: 'VARCHAR', parameter_type: '', value: ''}})\n"
            f"WITH v\n"
            f"MATCH (p:DECLARE {{startLine: 15, {test_node_base_props}, procedure_name: 'test_proc'}})\n"
            f"MERGE (p)-[r1:SCOPE]->(v)\n"
            f"WITH v, p, r1\n"
            f"MERGE (system:SYSTEM {{{test_system_props}}})\n"
            f"MERGE (system)-[r2:CONTAINS]->(v)\n"
            f"RETURN v, p, r1, r2"
        ]
        
        print("\n" + "="*80)
        print("18. Variable 노드 및 SCOPE 관계 (실제 로직: _build_variable_queries)")
        print("쿼리 실행:")
        for q in queries18:
            print(f"  {q}")
        print("\n결과:")
        result18 = await connection.execute_query_and_return_graph(queries18)
        print_json(result18)
        
        # ========== 19. 프로시저 요약 업데이트 (실제 로직: _finalize_procedure_summary) ==========
        queries19 = [
            f"MATCH (n:PROCEDURE {{procedure_name: 'test_proc', {test_node_base_props}}})\n"
            f"SET n.summary = '테스트 프로시저입니다'\n"
            f"RETURN n"
        ]
        
        print("\n" + "="*80)
        print("19. 프로시저 요약 업데이트 (실제 로직: _finalize_procedure_summary)")
        print("쿼리 실행:")
        for q in queries19:
            print(f"  {q}")
        print("\n결과:")
        result19 = await connection.execute_query_and_return_graph(queries19)
        print_json(result19)
        
        # ========== 20. 테이블 설명 업데이트 (실제 로직: _summarize_table) ==========
        queries20 = [
            f"MATCH (t:Table {{{test_table_base_props}, schema: 'public', name: 'users', db: '{test_dbms}', project_name: '{test_project_name}'}})\n"
            f"SET t.description = '사용자 테이블'\n"
            f"RETURN t"
        ]
        
        print("\n" + "="*80)
        print("20. 테이블 설명 업데이트 (실제 로직: _summarize_table)")
        print("쿼리 실행:")
        for q in queries20:
            print(f"  {q}")
        print("\n결과:")
        result20 = await connection.execute_query_and_return_graph(queries20)
        print_json(result20)
        
        # ========== 21. 컬럼 설명 업데이트 (실제 로직: _summarize_table) ==========
        queries21 = [
            f"MATCH (c:Column {{{test_table_base_props}, name: 'id', fqn: 'public.users.id', project_name: '{test_project_name}'}})\n"
            f"SET c.description = '사용자 고유 식별자'\n"
            f"RETURN c"
        ]
        
        print("\n" + "="*80)
        print("21. 컬럼 설명 업데이트 (실제 로직: _summarize_table)")
        print("쿼리 실행:")
        for q in queries21:
            print(f"  {q}")
        print("\n결과:")
        result21 = await connection.execute_query_and_return_graph(queries21)
        print_json(result21)
        
        # 정리
        cleanup_queries = [
            f"MATCH (n) WHERE n.user_id = '{test_user_id}' DETACH DELETE n",
            f"MATCH (s:SYSTEM) WHERE s.user_id = '{test_user_id}' DETACH DELETE s"
        ]
        await connection.execute_queries(cleanup_queries)
        print("\n" + "="*80)
        print("✅ 모든 테스트 완료 및 정리 완료")
        
    except Exception as e:
        print(f"\n❌ 테스트 실패: {e}")
        import traceback
        traceback.print_exc()
    finally:
        await connection.close()


if __name__ == "__main__":
    asyncio.run(main())
