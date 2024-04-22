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

def add_token_count_to_node(node, file_path):
    total_child_tokens = 0  # 자식 노드들의 토큰 수 합계를 저장할 변수

    # 자식 노드가 있을 경우, 먼저 자식 노드들의 토큰 수를 계산
    if 'children' in node:
        for child in node['children']:
            child_tokens = add_token_count_to_node(child, file_path)
            total_child_tokens += child_tokens

    # 현재 노드의 토큰 수를 계산
    node_tokens = count_tokens_in_range(node['startLine'], node['endLine'], file_path)

    # 자식 노드들의 토큰 수 합계를 현재 노드의 토큰 수에서 제외
    node['tokens'] = max(0, node_tokens - total_child_tokens)

    # 현재 노드의 최종 토큰 수를 반환 (부모 노드의 계산을 위해)
    return node['tokens']

# structure.json 파일을 읽고 파싱하는 코드
with open('structure.json', 'r') as file:
    data = json.load(file)

# 각 노드에 토큰 수 추가
add_token_count_to_node(data, 'payroll_by_gpt.txt')



# def reorder_children_last(node):
#     if 'children' in node:
#         children = node.pop('children')
#         node['children'] = children
#         for child in node['children']:
#             reorder_children_last(child)

# reorder_children_last(data)


# 결과 출력 및 파일에 저장
print(json.dumps(data, indent=4))
with open('structure_with_tokens.json', 'w') as file:
    json.dump(data, file, indent=4)

def dfs_print_endline_within_limit(node, limit=100, current_sum=0):
    # 현재 노드의 토큰 수를 누적 합계에 추가
    current_sum += node.get('tokens', 0)

    # 누적 합계가 limit을 초과하는 경우, 현재 노드의 endLine을 출력하고, 누적 합계를 현재 노드의 토큰 수로 재설정
    if current_sum > limit:
        print(node['endLine'])  # 현재 노드의 endLine 출력
        current_sum = node.get('tokens', 0)  # 누적 합계를 현재 노드의 토큰 수로 재설정

    # 자식 노드가 있는 경우, 각 자식 노드에 대해 재귀적으로 함수 호출
    if 'children' in node:
        for child in node['children']:
            current_sum = dfs_print_endline_within_limit(child, limit, current_sum)

    return current_sum

# data 값에 대해 깊이우선 탐색을 시작하여 조건에 맞는 endLine 출력
dfs_print_endline_within_limit(data)

