// 데이터 추출 및 그래프 시각화용 쿼리
// - 데이터 추출: UNION ALL로 각 타입별로 명확히 분리, 프로시저별 구분 가능
// - 그래프 시각화: 모든 노드와 관계를 반환하여 Neo4j 브라우저에서 연결된 그래프로 표시
//
// 참고: IF/WHILE/ASSIGNMENT 노드는 포함하지 않음
// 이유: PARENT_OF* 패턴으로 모든 하위 노드를 찾을 수 있으므로
//       리포트에 필요한 정보(DML, Variable, CALL)는 모두 하위 노드에서 추출 가능

// 1. PROCEDURE 노드 (각 프로시저별로 반환)
MATCH (p:PROCEDURE {
  folder_name: 'HOSPITAL_RECEPTION',
  file_name: 'SP_HOSPITAL_RECEPTION.sql',
  user_id: 'KO_TestSession',
  project_name: 'HOSPITAL_MANAGEMENT'
})
RETURN p AS procedure_node,
       p AS node1,
       NULL AS node2,
       NULL AS relationship,
       p.procedure_name AS procedure_name,
       p.startLine AS procedure_start_line,
       'PROCEDURE' AS node_type

UNION ALL

// 2. DML 노드와 일반 테이블 (WRITES/FROM 관계)
MATCH (p:PROCEDURE {
  folder_name: 'HOSPITAL_RECEPTION',
  file_name: 'SP_HOSPITAL_RECEPTION.sql',
  user_id: 'KO_TestSession',
  project_name: 'HOSPITAL_MANAGEMENT'
})
MATCH path = (p)-[:PARENT_OF*]->(dml)-[r:WRITES|FROM]->(t:Table {
  folder_name: 'HOSPITAL_RECEPTION',
  user_id: 'KO_TestSession',
  project_name: 'HOSPITAL_MANAGEMENT'
})
WHERE ANY(label IN labels(dml) WHERE label IN ['SELECT', 'INSERT', 'UPDATE', 'DELETE', 'MERGE', 'EXECUTE_IMMEDIATE', 'FETCH', 'CTE', 'OPEN_CURSOR'])
RETURN p AS procedure_node,
       dml AS node1,
       t AS node2,
       r AS relationship,
       p.procedure_name AS procedure_name,
       p.startLine AS procedure_start_line,
       labels(dml)[0] AS node_type

UNION ALL

// 3. CREATE_TEMP_TABLE 노드 (DML이면서 동시에 Table, 관계 없음)
MATCH (p:PROCEDURE {
  folder_name: 'HOSPITAL_RECEPTION',
  file_name: 'SP_HOSPITAL_RECEPTION.sql',
  user_id: 'KO_TestSession',
  project_name: 'HOSPITAL_MANAGEMENT'
})
MATCH path = (p)-[:PARENT_OF*]->(temp_table:CREATE_TEMP_TABLE:Table {
  folder_name: 'HOSPITAL_RECEPTION',
  user_id: 'KO_TestSession',
  project_name: 'HOSPITAL_MANAGEMENT'
})
RETURN p AS procedure_node,
       temp_table AS node1,
       NULL AS node2,
       NULL AS relationship,
       p.procedure_name AS procedure_name,
       p.startLine AS procedure_start_line,
       'CREATE_TEMP_TABLE' AS node_type

UNION ALL

// 4. Variable 노드 (DECLARE/SPEC과 SCOPE 관계)
MATCH (p:PROCEDURE {
  folder_name: 'HOSPITAL_RECEPTION',
  file_name: 'SP_HOSPITAL_RECEPTION.sql',
  user_id: 'KO_TestSession',
  project_name: 'HOSPITAL_MANAGEMENT'
})
MATCH path = (p)-[:PARENT_OF*]->(decl)-[r:SCOPE]->(v:Variable {
  folder_name: 'HOSPITAL_RECEPTION',
  user_id: 'KO_TestSession',
  project_name: 'HOSPITAL_MANAGEMENT'
})
WHERE ANY(label IN labels(decl) WHERE label IN ['DECLARE', 'SPEC', 'PACKAGE_VARIABLE'])
RETURN p AS procedure_node,
       decl AS node1,
       v AS node2,
       r AS relationship,
       p.procedure_name AS procedure_name,
       p.startLine AS procedure_start_line,
       labels(decl)[0] AS node_type

UNION ALL

// 5. CALL 관계 (프로시저 호출)
MATCH (p:PROCEDURE {
  folder_name: 'HOSPITAL_RECEPTION',
  file_name: 'SP_HOSPITAL_RECEPTION.sql',
  user_id: 'KO_TestSession',
  project_name: 'HOSPITAL_MANAGEMENT'
})
MATCH path = (p)-[:PARENT_OF*]->(caller)-[r:CALL]->(callee)
WHERE (callee:PROCEDURE OR callee:FUNCTION)
  AND callee.user_id = 'KO_TestSession'
RETURN p AS procedure_node,
       caller AS node1,
       callee AS node2,
       r AS relationship,
       p.procedure_name AS procedure_name,
       p.startLine AS procedure_start_line,
       labels(caller)[0] AS node_type

ORDER BY procedure_name, procedure_start_line, node_type

