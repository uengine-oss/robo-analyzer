def generate_sibling_order_cypher(queries):
    # Split the input into individual CREATE statements
    create_statements = queries.split('\n')

    # Dictionary to hold parent-child relationships
    parent_child = {}

    # Extract relationships and entities
    for statement in create_statements:
        if statement.startswith('CREATE ('):
            entity, rest = statement.split(' ', 1)
            entity_type, entity_id = entity[7:-1].split(':')
            if ')-[:PARENTOF]->(' in rest:
                parent, child = rest.split(')-[:PARENTOF]->(')
                child = child.split(')')[0]
                if parent not in parent_child:
                    parent_child[parent] = []
                parent_child[parent].append((entity_type, entity_id))

    # Generate MATCH and CREATE statements for sibling order
    sibling_order_cypher = []
    for parent, children in parent_child.items():
        for i in range(len(children) - 1):
            prev_type, prev_id = children[i]
            next_type, next_id = children[i + 1]
            match_create = f"MATCH (prev: {prev_type} {{id: {prev_id}}}), (next: {next_type} {{id: {next_id}}}) CREATE (prev)-[:NEXT]->(next);"
            sibling_order_cypher.append(match_create)

    return '\n'.join(sibling_order_cypher)


if __name__ == "__main__":
    cypher_queries = """
CREATE (proc1:PROCEDURE {id: 1, name: "calculate_payroll 프로시저", source: "CREATE OR REPLACE PROCEDURE calculate_payroll AS ...child code...", closed: true, endLine: 67})
CREATE (for1:FOR {id: 3, name: "직원 정보 루프", source: "FOR rec IN (SELECT e.employee_id, e.base_salary, e.employee_type, e.contract_tax_rate FROM employees e) LOOP ...child code... END LOOP", closed: true, endLine: 63})
CREATE (proc1)-[:PARENTOF]->(for1)
CREATE (declare1:DECLARE {id: 6, name: "야근 수당 계산", source: "DECLARE ...child code... BEGIN ...child code... END", closed: true, endLine: 22})
CREATE (for1)-[:PARENTOF]->(declare1)
CREATE (select1:SELECT {id: 11, name: "야근 시간 계산", source: "SELECT SUM(over_hours) INTO overtime_hours FROM work_logs WHERE employee_id = rec.employee_id AND work_date BETWEEN trunc(sysdate, 'MM') AND last_day(sysdate)", closed: true, endLine: 15})
CREATE (declare1)-[:PARENTOF]->(select1)
CREATE (if1:IF {id: 17, name: "야근 시간 null 체크", condition: "overtime_hours IS NULL", source: "IF overtime_hours IS NULL THEN ...child code... END IF", closed: true, endLine: 19})
CREATE (declare1)-[:PARENTOF]->(if1)
CREATE (declare2:DECLARE {id: 25, name: "무급 휴가 공제 계산", source: "DECLARE ...child code... BEGIN ...child code... END", closed: true, endLine: 41})
CREATE (for1)-[:PARENTOF]->(declare2)
CREATE (select2:SELECT {id: 29, name: "무급 휴가 일수 계산", source: "SELECT SUM(leave_days) INTO unpaid_leave_days FROM leave_records WHERE employee_id = rec.employee_id AND leave_type = 'Unpaid' AND leave_date BETWEEN trunc(sysdate, 'MM') AND last_day(sysdate)", closed: true, endLine: 34})
CREATE (declare2)-[:PARENTOF]->(select2)
CREATE (if2:IF {id: 36, name: "무급 휴가 일수 null 체크", condition: "unpaid_leave_days IS NULL", source: "IF unpaid_leave_days IS NULL THEN ...child code... END IF", closed: true, endLine: 38})
CREATE (declare2)-[:PARENTOF]->(if2)
CREATE (declare3:DECLARE {id: 44, name: "세금 공제 계산", source: "DECLARE ...child code... BEGIN ...child code... END", closed: true, endLine: 57})
CREATE (for1)-[:PARENTOF]->(declare3)
CREATE (if3:IF {id: 50, name: "직원 유형 체크", condition: "rec.employee_type = 'Contract'", source: "IF rec.employee_type = 'Contract' THEN ...child code... END IF", closed: true, endLine: 54})
CREATE (declare3)-[:PARENTOF]->(if3)
CREATE (update1:UPDATE {id: 60, name: "최종 급여 업데이트", source: "UPDATE employees SET final_salary = rec.base_salary + overtime_pay - unpaid_deduction - tax_deduction WHERE employee_id = rec.employee_id", closed: true, endLine: 62})
CREATE (for1)-[:PARENTOF]->(update1)
CREATE (select3:SELECT {id: 3, name: "직원 정보 조회", source: "SELECT e.employee_id, e.base_salary, e.employee_type, e.contract_tax_rate FROM employees e"})
CREATE (for1)-[:PARENTOF]->(select3)
    """
    print("Generated Cypher Queries for Sibling Order:")
    print(generate_sibling_order_cypher(cypher_queries))
