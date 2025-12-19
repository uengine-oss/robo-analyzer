"""
클래스 다이어그램 생성 테스트

실제 Neo4j 데이터를 사용하여 클래스 다이어그램을 생성하고 결과를 파일로 저장합니다.
환경 변수(.env)에서 Neo4j 연결 정보를 읽어옵니다.

실행 방법:
    python test/test_create_class_diagram.py
"""
import asyncio
import os
import json
from pathlib import Path
from typing import List, Tuple
from datetime import datetime

import sys
sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from convert.architecture.create_class_diagram import start_class_diagram_generation
from understand.neo4j_connection import Neo4jConnection

# 한글 로그가 깨지지 않도록 UTF-8 인코딩
os.environ.setdefault("PYTHONIOENCODING", "utf-8")


# ==================== 설정 ====================

def _env(key: str, default: str) -> str:
    """환경 변수 읽기 (공백 제거)"""
    value = os.getenv(key)
    return value.strip() if value and value.strip() else default


TEST_USER_ID = _env("TEST_USER_ID", "TestSession_4")
TEST_PROJECT_NAME = _env("TEST_PROJECT_NAME", "test")
TEST_API_KEY = os.getenv("LLM_API_KEY", "test-api-key")
TEST_DB_NAME = _env("TEST_DB_NAME", "neo4j")
TEST_LOCALE = _env("TEST_LOCALE", "ko")

# 결과 저장 디렉토리
OUTPUT_DIR = Path(__file__).parent / "data" / "class_diagrams"
OUTPUT_DIR.mkdir(parents=True, exist_ok=True)


# ==================== Helper Functions ====================

async def get_available_classes(conn: Neo4jConnection) -> List[Tuple[str, str]]:
    """Neo4j에서 사용 가능한 클래스 목록 조회"""
    query = f"""
    MATCH (c)
    WHERE (c:CLASS OR c:INTERFACE)
      AND c.project_name = '{TEST_PROJECT_NAME}'
      AND c.user_id = '{TEST_USER_ID}'
    RETURN DISTINCT c.directory AS directory, c.class_name AS class_name
    ORDER BY c.directory, c.class_name
    """
    
    results = await conn.execute_queries([query])
    if not results or not results[0]:
        return []
    
    return [(row.get("directory", ""), row.get("class_name", "")) 
            for row in results[0] if row.get("class_name")]


def save_diagram_result(result: dict, class_names: List[Tuple[str, str]] = None):
    """다이어그램 결과를 파일로 저장"""
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    
    # 파일명 생성
    if class_names and len(class_names) == 1:
        class_name = class_names[0][1].replace(".", "_").replace("/", "_")
        filename_base = f"class_diagram_{class_name}_{timestamp}"
    elif class_names:
        filename_base = f"class_diagram_selected_{len(class_names)}classes_{timestamp}"
    else:
        filename_base = f"class_diagram_all_{timestamp}"
    
    # Mermaid 다이어그램 저장 (.md 파일)
    mermaid_file = OUTPUT_DIR / f"{filename_base}.md"
    with open(mermaid_file, 'w', encoding='utf-8') as f:
        f.write(f"# 클래스 다이어그램\n\n")
        f.write(f"**생성 시간**: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}\n\n")
        f.write(f"**프로젝트**: {TEST_PROJECT_NAME}\n\n")
        f.write(f"**사용자 ID**: {TEST_USER_ID}\n\n")
        if class_names:
            f.write(f"**포함된 클래스**: {', '.join([f'{s}.{c}' for s, c in class_names])}\n\n")
        else:
            f.write(f"**포함된 클래스**: 전체\n\n")
        f.write(f"**클래스 수**: {result.get('class_count', 0)}\n\n")
        f.write(f"**관계 수**: {result.get('relationship_count', 0)}\n\n")
        f.write("---\n\n")
        f.write(result.get('diagram', ''))
    
    # JSON 결과 저장 (전체 데이터 포함)
    json_file = OUTPUT_DIR / f"{filename_base}.json"
    json_data = {
        "timestamp": datetime.now().isoformat(),
        "project_name": TEST_PROJECT_NAME,
        "user_id": TEST_USER_ID,
        "class_names": [{"directory": s, "class_name": c} for s, c in (class_names or [])],
        "result": {
            "diagram": result.get('diagram', ''),
            "class_count": result.get('class_count', 0),
            "relationship_count": result.get('relationship_count', 0),
            "classes": result.get('classes', []),
            "relationships": result.get('relationships', [])
        }
    }
    with open(json_file, 'w', encoding='utf-8') as f:
        json.dump(json_data, f, ensure_ascii=False, indent=2)
    
    return {
        "mermaid_file": str(mermaid_file),
        "json_file": str(json_file),
        "filename_base": filename_base
    }


# ==================== 테스트 함수 ====================

async def test_class_diagram_generation(conn: Neo4jConnection):
    """클래스 다이어그램 생성 테스트"""
    print("=" * 80)
    print("클래스 다이어그램 생성 테스트")
    print("=" * 80)
    print(f"프로젝트: {TEST_PROJECT_NAME}")
    print(f"사용자 ID: {TEST_USER_ID}")
    print(f"데이터베이스: {TEST_DB_NAME}")
    print()
    
    # 사용 가능한 클래스 조회
    print("클래스 조회 중...")
    available_classes = await get_available_classes(conn)
    
    if not available_classes:
        print(f"❌ 클래스를 찾을 수 없습니다. (project_name: {TEST_PROJECT_NAME}, user_id: {TEST_USER_ID})")
        return None
    
    print(f"✅ 조회된 클래스: {len(available_classes)}개")
    if len(available_classes) <= 10:
        for system, cls in available_classes:
            print(f"   - {system}.{cls}")
    else:
        for system, cls in available_classes[:10]:
            print(f"   - {system}.{cls}")
        print(f"   ... 외 {len(available_classes) - 10}개")
    print()
    
    # 다이어그램 생성 (전체 클래스)
    print("다이어그램 생성 중...")
    result = await start_class_diagram_generation(
        class_names=[],  # 빈 리스트 = 전체 클래스
        project_name=TEST_PROJECT_NAME,
        user_id=TEST_USER_ID,
        api_key=TEST_API_KEY,
        locale=TEST_LOCALE
    )
    
    # 결과 검증
    assert "diagram" in result
    assert "class_count" in result
    assert "relationship_count" in result
    assert "classes" in result
    assert "relationships" in result
    
    print()
    print("=" * 80)
    print("생성 완료")
    print("=" * 80)
    print(f"클래스 수: {result['class_count']}")
    print(f"관계 수: {result['relationship_count']}")
    print()
    
    # 파일 저장
    saved_files = save_diagram_result(result, None)
    print("✅ 다이어그램 저장 완료:")
    print(f"   - Mermaid 파일: {saved_files['mermaid_file']}")
    print(f"   - JSON 파일: {saved_files['json_file']}")
    print()
    
    return result


# ==================== 메인 실행 함수 ====================

async def main():
    """메인 테스트 실행"""
    # Neo4j 연결 설정
    original_db = Neo4jConnection.DATABASE_NAME
    Neo4jConnection.DATABASE_NAME = TEST_DB_NAME
    
    conn = Neo4jConnection()
    
    try:
        # 클래스 다이어그램 생성 테스트
        result = await test_class_diagram_generation(conn)
        
        if result:
            print("=" * 80)
            print("✅ 테스트 성공")
            print("=" * 80)
        else:
            print("=" * 80)
            print("⚠️  테스트 완료 (클래스 없음)")
            print("=" * 80)
        
    except Exception as e:
        print(f"\n❌ 테스트 실패: {e}")
        import traceback
        traceback.print_exc()
    finally:
        await conn.close()
        Neo4jConnection.DATABASE_NAME = original_db


if __name__ == "__main__":
    asyncio.run(main())
