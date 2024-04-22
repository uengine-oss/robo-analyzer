#file_path = '/Users/uengine/Documents/payroll/sp/pg_pay_pay.pck'
file_path = 'payroll_by_gpt.txt'


import json
import tiktoken

def count_tokens_in_range(start_line, end_line, file_path):
    # tiktoken 인코더 초기화
    encoder = tiktoken.get_encoding("cl100k_base")  # 예시로 'cl100k_base' 인코딩 사용

    with open(file_path, 'r') as file:
        lines = file.readlines()
        start_line = max(start_line - 1, 0)
        end_line = min(end_line, len(lines))
        text = "".join(lines[start_line:end_line])
        # tiktoken을 사용하여 텍스트를 토큰화하고 토큰 수를 카운트
        tokens = encoder.encode(text)
        return len(tokens)



# structure.json 파일을 읽고 파싱하는 코드
with open('structure.json', 'r') as file:
    data = json.load(file)


import json

def extract_and_summarize_code(file_path, node):
    with open(file_path, 'r') as file:
        lines = file.readlines()

    def summarize_code(start_line, end_line, children):
        code_lines = lines[start_line-1:end_line]
        summarized_code = ""
        last_end_line = start_line - 1

        for child in children:
            # Add the code before the child
            before_child_code = code_lines[last_end_line-start_line+1:child['startLine']-start_line]
            summarized_code += ''.join([f"{i+last_end_line+1}: {line}" for i, line in enumerate(before_child_code)])
            # Add the summary for the child
            summarized_code += f"{child['startLine']}: ... code ...\n"
            last_end_line = child['endLine']

        # Add any remaining code after the last child
        after_last_child_code = code_lines[last_end_line-start_line+1:]
        summarized_code += ''.join([f"{i+last_end_line+1}: {line}" for i, line in enumerate(after_last_child_code)])
        
        return summarized_code

    if not node.get('children'):
        with open(file_path, 'r') as file:
            lines = file.readlines()
        code_lines = lines[node['startLine']-1:node['endLine']]
        return ''.join([f"{i+node['startLine']}: {line}" for i, line in enumerate(code_lines)])
    else:
        return summarize_code(node['startLine'], node['endLine'], node.get('children', []))

from understand import understand_code

def create_focused_code(current_schedule, schedule_stack):
    # 현재 노드의 코드와 시작 라인 번호를 가져옴
    focused_code = current_schedule["code"]
    current_start_line = current_schedule["startLine"]

    # 스케줄 스택을 역순으로 순회하면서 상위 코드에 현재 코드를 삽입
    for schedule in reversed(schedule_stack):

        # 현재 노드의 시작 라인 번호를 기준으로 ... code ... 위치를 식별
        placeholder = f"{current_start_line}: ... code ..."
        if placeholder in schedule["code"]:
            # 해당 위치의 ... code ...를 현재 노드의 코드로 대체
            focused_code = schedule["code"].replace(placeholder, focused_code, 1)

        current_start_line = schedule["startLine"]

    return focused_code

def analysis(data, file_path):
    schedule_stack = []
    cypher_create = []

    def traverse(node, schedule_stack, parent_alias=None):
        
        code = extract_and_summarize_code(file_path, node)
        current_schedule = {
            "startLine": node['startLine'],
            "endLine": node['endLine'],
            "code": code
        }

        understanding = None


        if not node.get('children'):  #leaf 면 우선 요약한다.
            focused_code = create_focused_code(current_schedule, schedule_stack)
            understanding = understand_code(focused_code, current_schedule)

        schedule_stack.append(current_schedule)

        node_alias = f"n{node['startLine']}"
#        create_node_query = f'CREATE (:STATEMENT{{id: {node["startLine"]}, endLine: {node["endLine"]}}})'
        create_node_query = f'CREATE ({node_alias}:STATEMENT{{id: {node["startLine"]}, endLine: {node["endLine"]}}})'
        cypher_create.append(create_node_query)
        if parent_alias:
#            create_parent_rel_query = f'MATCH (parent:{parent_node.parent_type}{{id:{parent_node.id}}}),(node:{{id: {node["startLine"]}) CREATE (parent)-[:PARENT_OF]->(node)'
            create_parent_rel_query = f'CREATE ({parent_alias})-[:PARENT_OF]->({node_alias})'
            cypher_create.append(create_parent_rel_query)

        # Traverse into children if they exist
        prev_alias = None
        for child in node.get('children', []):
            summary = traverse(child, schedule_stack, node_alias)
            placeholder_summary = f"{child['startLine']}: ... code ..."
            if placeholder_summary in current_schedule["code"]:
                current_schedule["code"] = current_schedule["code"].replace(placeholder_summary, f"{child['startLine']}~{child['endLine']}: {summary}", 1)

            if prev_alias:
                create_parent_rel_query = f'CREATE ({prev_alias})-[:NEXT]->(n{child['startLine']})'
                cypher_create.append(create_parent_rel_query)
            prev_alias = f"n{child['startLine']}"
        
        if(node.get('children')):  #child가 있었다면 상위로 보내기 전에 다시 한번 요약을 한다.
            print(current_schedule["code"])
            focused_code = create_focused_code(current_schedule, schedule_stack)
            understanding = understand_code(focused_code, current_schedule)

        #code update
        for i, query in enumerate(cypher_create):
            if f'id: {node["startLine"]},' in query:
                summary = understanding["summary"].replace('"', '\\"')  # 코드 내 따옴표 이스케이프
                cypher_create[i] = f'CREATE ({node_alias}:{understanding["statementType"]}{{id: {node["startLine"]}, summary: "{summary}", source: "{code.replace('"', '\\"').replace('\n', '\\n')}", endLine: {node["endLine"]}}})'
                break




        #print(current_schedule["code"])
        # After traversing children, pop the current node as we return to the parent
        schedule_stack.pop()

        return understanding["summary"]

    traverse(data, schedule_stack)  # Start the traversal from the root node
    return cypher_create

# Assuming 'data' is already loaded from 'structure.json'
cypher_create = analysis(data, file_path)

# for query in cypher_create:
#     print(query)

with open('cypher_queries.txt', 'w') as file:
    for query in cypher_create:
        file.write(query + '\n')
print("Cypher queries have been saved to cypher_queries.txt")
