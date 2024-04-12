def add_line_numbers(file_path):
    """주어진 파일의 각 라인 앞에 라인 번호를 추가하여 같은 파일에 저장하는 함수"""
    with open(file_path, 'r', encoding='utf-8') as file:
        lines = file.readlines()

    with open(file_path, 'w', encoding='utf-8') as file:
        for index, line in enumerate(lines, start=1):
            file.write(f"{index}: {line}")

# 사용 예시
file_path = '/Users/uengine/Documents/modernizer/payroll_by_gpt.txt'  # 변환하고자 하는 소스 파일 경로
add_line_numbers(file_path)