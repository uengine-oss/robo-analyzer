def convert_cypher_to_context(cypher_query):
    # Extract all CREATE statements with closed: false
    open_if_statements = []
    for line in cypher_query.split('\n'):
        if 'CREATE' in line and 'IF' in line and 'closed: false' in line:
            # Extract the IF id
            start = line.find('{id: ') + 5
            end = line.find(',', start)
            if_id = line[start:end]
            open_if_statements.append(if_id)

    # Convert the list of open IF ids to a context stack
    context_stack = ""
    indent = ""
    for if_id in open_if_statements:
        context_stack += f"{indent}IF{{id:{if_id}}}\n"
        indent += "  "  # Increase indent for nested IFs

    return context_stack.strip()

# Example usage
cypher_query = """
CREATE (if1:IF {id: 10, name: "조건3에 따른 IF문", condition: "condition_3", source: "IF (condition_3) THEN", closed: false, endLine: -1})
CREATE (select1:SELECT {id: 11, source: "SELECT column_name INTO variable_name FROM table_name WHERE condition;"})
CREATE (if1)-[:PARENT_OF]->(select1)
MATCH (table:Table {id: 'table_name'}) 
CREATE (select1)-[:FROM]->(table)

CREATE (if2:IF {id: 13, name: "조건4에 따른 IF문", condition: "condition_4", source: "IF (condition_4) THEN", closed: false, endLine: -1})
CREATE (select2:SELECT {id: 14, source: "SELECT column_name INTO variable_name FROM table_name WHERE condition;"})
CREATE (if2)-[:PARENT_OF]->(select2)
MATCH (table:Table {id: 'table_name'}) 
CREATE (select2)-[:FROM]->(table)
CREATE (if1)-[:PARENT_OF]->(if2)
"""

context_stack = convert_cypher_to_context(cypher_query)
print(context_stack)