#!/usr/bin/env python3
"""
특정 경로의 파일들에서 특정 문자열이 존재하는지 체크하는 스크립트

사용법:
    python scripts/check_table_usage.py
"""

import os
from pathlib import Path


# ==================== 설정 (하드코딩) ====================
# 검색할 경로
SEARCH_PATH = r"D:\다운로드\output\real-scheme\sp\RWIS"

# 검색할 문자열 (대소문자 구분 없이 검색)
SEARCH_STRINGS = [
    "tmp_up_tran2fa_rditag_tb"
    # 추가 문자열을 여기에 추가할 수 있습니다
]


def check_string_in_file(file_path: Path, search_string: str) -> list:
    """파일에서 문자열을 검색하고 매칭된 라인 번호와 내용을 반환"""
    matches = []
    
    try:
        with open(file_path, 'r', encoding='utf-8', errors='ignore') as f:
            for line_num, line in enumerate(f, start=1):
                if search_string.lower() in line.lower():
                    matches.append((line_num, line.strip()))
    except Exception as e:
        print(f"⚠️  파일 읽기 오류 ({file_path}): {e}")
    
    return matches


def main():
    """메인 함수"""
    print("=" * 80)
    print("🔍 문자열 검색 스크립트")
    print("=" * 80)
    print(f"📂 검색 경로: {SEARCH_PATH}")
    print(f"🔍 검색 문자열: {', '.join(SEARCH_STRINGS)}")
    print("=" * 80)
    
    if not os.path.exists(SEARCH_PATH):
        print(f"❌ 경로가 존재하지 않습니다: {SEARCH_PATH}")
        return
    
    if not os.path.isdir(SEARCH_PATH):
        print(f"❌ 디렉토리가 아닙니다: {SEARCH_PATH}")
        return
    
    path_obj = Path(SEARCH_PATH)
    found_any = False
    
    # 각 검색 문자열별로 검색
    for search_string in SEARCH_STRINGS:
        print(f"\n🔍 검색 중: '{search_string}'")
        print("-" * 80)
        
        file_matches = {}
        
        # 모든 파일 순회
        for file_path in path_obj.rglob("*"):
            if not file_path.is_file():
                continue
            
            # __pycache__ 등 제외
            if "__pycache__" in file_path.parts:
                continue
            
            matches = check_string_in_file(file_path, search_string)
            
            if matches:
                relative_path = file_path.relative_to(path_obj)
                file_matches[str(relative_path)] = matches
                found_any = True
        
        # 결과 출력
        if file_matches:
            print(f"✅ '{search_string}' 발견: {len(file_matches)}개 파일")
            for file_path, matches in sorted(file_matches.items()):
                print(f"\n  📄 {file_path}")
                for line_num, line_content in matches:
                    print(f"     L{line_num:4d}: {line_content[:70]}")
        else:
            print(f"❌ '{search_string}' 발견되지 않음")
    
    print("\n" + "=" * 80)
    if not found_any:
        print("⚠️  어떤 문자열도 찾지 못했습니다.")


if __name__ == "__main__":
    main()

