import os
from typing import Iterable, Tuple


# ===== 사용자가 여기 두 값만 수정하면 됩니다 =====
BASE_DIR = r"D:\다운로드\HDAPS 프로시저 모음"  # 검색 시작 폴더 경로
QUERY = "LK_HDAPS"              # 검색할 단어 (부분 일치, 대소문자 구분)
# ================================================


def iter_files(root_dir: str) -> Iterable[str]:
    for current_dir, _subdirs, files in os.walk(root_dir):
        for file_name in files:
            yield os.path.join(current_dir, file_name)


def open_with_fallbacks(file_path: str):
    encodings_to_try = ["utf-8", "cp949", "euc-kr", "iso-8859-1"]
    last_error = None
    for enc in encodings_to_try:
        try:
            return open(file_path, "r", encoding=enc, errors="replace")
        except Exception as exc:  # pragma: no cover
            last_error = exc
            continue
    try:
        return open(file_path, "r", encoding="utf-8", errors="ignore")
    except Exception:
        if last_error is not None:
            raise last_error
        raise


def search_in_file(file_path: str, needle: str) -> Iterable[Tuple[int, str]]:
    with open_with_fallbacks(file_path) as f:
        for idx, line in enumerate(f, start=1):
            if needle not in line:
                continue
            start = 0
            matched = False
            while True:
                pos = line.find(needle, start)
                if pos == -1:
                    break
                next_pos = pos + len(needle)
                if next_pos >= len(line) or line[next_pos] != '@':
                    matched = True
                    break
                start = pos + 1
            if matched:
                yield idx, line.rstrip("\n\r")


def main() -> int:
    base_dir = os.path.abspath(BASE_DIR)
    if not os.path.isdir(base_dir):
        print(f"[오류] 폴더가 아닙니다: {base_dir}")
        return 2

    print(f"[검색 시작] dir='{base_dir}', query='{QUERY}'")

    total = 0
    scanned = 0
    for txt_path in iter_files(base_dir):
        scanned += 1
        try:
            for line_no, text in search_in_file(txt_path, QUERY):
                print(f"{txt_path}:{line_no}: {text}")
                total += 1
        except Exception as exc:  # pragma: no cover
            print(f"[경고] 파일을 읽지 못했습니다: {txt_path} ({exc})")
            continue

    print(f"[요약] {scanned}개 파일 검사, {total}개 결과")
    return 0 if total > 0 else 1


if __name__ == "__main__":
    main()


