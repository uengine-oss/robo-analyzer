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
    if 'children' in node:
        for child in node['children']:
            add_token_count_to_node(child, file_path)
    node['tokens'] = count_tokens_in_range(node['startLine'], node['endLine'], file_path)

# structure.json 파일을 읽고 파싱하는 코드
with open('structure.json', 'r') as file:
    data = json.load(file)

# 각 노드에 토큰 수 추가
add_token_count_to_node(data, 'payroll_by_gpt.txt')



# 결과 출력 및 파일에 저장
print(json.dumps(data, indent=4))
with open('structure_with_tokens.json', 'w') as file:
    json.dump(data, file, indent=4)