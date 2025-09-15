import asyncio
import os
import sys
import logging
from pathlib import Path

# 로깅 설정 - 콘솔에 출력
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.StreamHandler(sys.stdout)
    ]
)

# Neo4j 경고 메시지 숨기기
logging.getLogger('neo4j.notifications').setLevel(logging.ERROR)
logging.getLogger('neo4j').setLevel(logging.ERROR)
sys.path.append(str(Path(__file__).resolve().parents[1]))
from service.service import generate_and_execute_cypherQuery, get_user_directories


async def main() -> None:
    # * src 전체를 스캔하여 테스트 데이터 자동 구성
    test_data = []  # [(folder_name, file_name_with_ext)]

    # * 검증 로직 없이 함수 실행만 확인 (제너레이터 안전 종료 + 한 틱 양보)
    session_uuid = os.getenv("TEST_SESSION_UUID", "TestSession")
    api_key = os.getenv("LLM_API_KEY", "your-api-key")
    locale = os.getenv("TEST_LOCALE", "ko")

    # * 사용자 디렉터리에서 src 구조를 스캔
    dirs = get_user_directories(session_uuid)
    src_root = dirs['plsql']
    allowed_ext = {'.pkb', '.pks', '.sql', '.pls', '.prc', '.txt'}

    try:
        for folder_name in os.listdir(src_root):
            folder_path = os.path.join(src_root, folder_name)
            if not os.path.isdir(folder_path):
                continue
            for file_name in os.listdir(folder_path):
                _, ext = os.path.splitext(file_name)
                if ext.lower() in allowed_ext:
                    test_data.append((folder_name, file_name))
    except Exception as e:
        logging.error(f"src 스캔 중 오류: {str(e)}")

    # 스캔 결과 로그 출력
    if not test_data:
        logging.warning("src 폴더에서 처리 대상 파일을 찾지 못했습니다.")
    else:
        total = len(test_data)
        logging.info(f"스캔된 파일 수: {total} (root: {src_root})")
        for idx, (folder_name, file_name) in enumerate(test_data, start=1):
            logging.info(f"{idx:02d}/{total} - {folder_name}/{file_name}")

    agen = generate_and_execute_cypherQuery(test_data, session_uuid, api_key, locale)
    async for _ in agen:
        pass


if __name__ == "__main__":
    asyncio.run(main())
