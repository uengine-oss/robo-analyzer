import pytest
import pytest_asyncio
import asyncio
import os
from pathlib import Path

import sys
sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from service.service import ServiceOrchestrator
from understand.neo4j_connection import Neo4jConnection


# ==================== 설정 ====================

TEST_USER_ID = "TestSession"
TEST_PROJECT_NAME = "HOSPITAL_PROJECT"
TEST_API_KEY = os.getenv("LLM_API_KEY")
TEST_DB_NAME = "test"

# 테스트 데이터 경로 (상위 디렉토리의 data 폴더)
TEST_DATA_DIR = Path(__file__).resolve().parents[2] / "data" / TEST_USER_ID / TEST_PROJECT_NAME


# ==================== Fixtures ====================

@pytest.fixture(scope="module")
def test_data_exists():
    """테스트 데이터 존재 확인"""
    assert TEST_DATA_DIR.exists(), f"테스트 데이터 디렉토리가 없습니다: {TEST_DATA_DIR}"
    assert (TEST_DATA_DIR / "src" / "HOSPITAL_RECEPTION" / "SP_HOSPITAL_RECEPTION.sql").exists()
    assert (TEST_DATA_DIR / "ddl" / "DDL_HOSPITAL_RECEPTION.sql").exists()
    assert (TEST_DATA_DIR / "analysis" / "HOSPITAL_RECEPTION" / "SP_HOSPITAL_RECEPTION.json").exists()
    return TEST_DATA_DIR


@pytest_asyncio.fixture
async def real_neo4j():
    """실제 Neo4j 연결 (test DB 사용)"""
    # DATABASE_NAME을 test로 변경
    original_db = Neo4jConnection.DATABASE_NAME
    Neo4jConnection.DATABASE_NAME = TEST_DB_NAME
    
    conn = Neo4jConnection()
    
    # 테스트 시작 전 기존 데이터 삭제
    await conn.execute_queries([
        f"MATCH (n {{user_id: '{TEST_USER_ID}', project_name: '{TEST_PROJECT_NAME}'}}) DETACH DELETE n"
    ])
    
    yield conn
    
    await conn.close()
    # 원래대로 복구
    Neo4jConnection.DATABASE_NAME = original_db


# ==================== 실제 Understanding 테스트 ====================

class TestRealUnderstanding:
    """실제 Understanding 로직 테스트 (Mock 없음)"""
    
    @pytest.mark.asyncio
    async def test_complete_understanding_pipeline(self, test_data_exists, real_neo4j):
        """완전한 Understanding 파이프라인 실행 (실제 LLM 호출 포함)"""
        if not TEST_API_KEY:
            pytest.skip("LLM_API_KEY가 설정되지 않았습니다")
        
        print(f"\n{'='*60}")
        print(f"🚀 Understanding 파이프라인 시작")
        print(f"📁 데이터 경로: {TEST_DATA_DIR}")
        print(f"👤 User ID: {TEST_USER_ID}")
        print(f"📊 Project: {TEST_PROJECT_NAME}")
        print(f"🗄️  Neo4j DB: {TEST_DB_NAME}")
        print(f"{'='*60}\n")
        
        # ServiceOrchestrator 생성
        orchestrator = ServiceOrchestrator(
            user_id=TEST_USER_ID,
            api_key=TEST_API_KEY,
            locale="ko",
            project_name=TEST_PROJECT_NAME,
            dbms="postgres"
        )
        
        # 분석할 파일
        file_names = [("HOSPITAL_RECEPTION", "SP_HOSPITAL_RECEPTION.sql")]
        
        # Understanding 실행
        events = []
        alarm_messages = []
        errors = []
        
        try:
            print("📝 Understanding 실행 중...\n")
            
            async for chunk in orchestrator.understand_project(file_names):
                events.append(chunk)
                
                # 이벤트 파싱
                try:
                    import json
                    decoded = chunk.decode('utf-8').replace('send_stream', '')
                    if decoded.strip():
                        event_data = json.loads(decoded)
                        
                        if event_data.get('type') == 'ALARM':
                            msg = event_data.get('MESSAGE', '')
                            alarm_messages.append(msg)
                            print(f"🔔 {msg}")
                        
                        elif event_data.get('type') == 'ERROR':
                            error_msg = event_data.get('MESSAGE', '')
                            errors.append(error_msg)
                            print(f"❌ ERROR: {error_msg}")
                except:
                    pass
            
            print(f"\n✅ Understanding 완료! (총 {len(events)}개 이벤트)")
            
        except Exception as e:
            pytest.fail(f"Understanding 실행 중 예외 발생: {str(e)}")
        
        # 기본 검증
        assert len(errors) == 0, f"에러 발생: {errors}"
        assert len(events) > 0, "이벤트가 전혀 발생하지 않았습니다"
        
        print(f"\n{'='*60}")
        print("🔍 Neo4j 데이터 검증 시작")
        print(f"{'='*60}\n")
        
        # 실제 Neo4j 데이터 검증
        # 1. PROCEDURE 노드 확인
        print("1️⃣  PROCEDURE 노드 확인...")
        proc_result = await real_neo4j.execute_query_and_return_graph(
            TEST_USER_ID,
            [("HOSPITAL_RECEPTION", "SP_HOSPITAL_RECEPTION.sql")],
            f"MATCH (p:PROCEDURE {{user_id: '{TEST_USER_ID}', project_name: '{TEST_PROJECT_NAME}'}}) RETURN p"
        )
        proc_count = len(proc_result.get("Nodes", []))
        assert proc_count > 0, "PROCEDURE 노드가 없습니다"
        print(f"   ✅ PROCEDURE 노드: {proc_count}개")
        
        # 간단한 쿼리로 직접 실행
        file_pair = [("HOSPITAL_RECEPTION", "SP_HOSPITAL_RECEPTION.sql")]
        
        # 2. Variable 노드 확인
        print("2️⃣  Variable 노드 확인...")
        var_result = await real_neo4j.execute_queries([
            f"MATCH (v:Variable {{user_id: '{TEST_USER_ID}', project_name: '{TEST_PROJECT_NAME}'}}) RETURN v"
        ])
        var_count = len(var_result[0])
        print(f"   ✅ Variable 노드: {var_count}개")
        
        # 3. Table 노드 확인
        print("3️⃣  Table 노드 확인...")
        table_result = await real_neo4j.execute_queries([
            f"MATCH (t:Table {{user_id: '{TEST_USER_ID}', project_name: '{TEST_PROJECT_NAME}'}}) RETURN t"
        ])
        table_count = len(table_result[0])
        assert table_count >= 2, f"Table 노드 부족: {table_count}/2"
        print(f"   ✅ Table 노드: {table_count}개")
        
        # 4. DML 노드 확인
        print("4️⃣  DML 노드 확인...")
        dml_result = await real_neo4j.execute_queries([
            f"MATCH (d:DML {{user_id: '{TEST_USER_ID}', project_name: '{TEST_PROJECT_NAME}'}}) RETURN d"
        ])
        dml_count = len(dml_result[0])
        print(f"   ✅ DML 노드: {dml_count}개")
        
        # 5. FK 관계 확인
        print("5️⃣  FK 관계 확인...")
        fk_result = await real_neo4j.execute_queries([
            f"MATCH (t1:Table {{user_id: '{TEST_USER_ID}', project_name: '{TEST_PROJECT_NAME}'}})-[r:FK_TO_TABLE]->(t2:Table) RETURN r"
        ])
        fk_count = len(fk_result[0])
        print(f"   ✅ FK 관계: {fk_count}개")
        
        print(f"\n{'='*60}")
        print("🎉 배포 준비 완료!")
        print(f"{'='*60}")
        print(f"✅ 총 이벤트: {len(events)}개")
        print(f"✅ PROCEDURE: {proc_count}개")
        print(f"✅ Variable: {var_count}개")
        print(f"✅ Table: {table_count}개")
        print(f"✅ DML: {dml_count}개")
        print(f"✅ FK 관계: {fk_count}개")
        print(f"{'='*60}\n")


# ==================== 실행 ====================

if __name__ == "__main__":
    pytest.main([
        __file__, 
        "-v", 
        "-s", 
        "--tb=short",
        "--color=yes"
    ])
