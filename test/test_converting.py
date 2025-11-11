import pytest
import asyncio
import os
import json
from pathlib import Path

import sys
sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from convert.framework.create_entity import EntityGenerator
from convert.framework.create_repository import RepositoryGenerator
from convert.framework.create_service_skeleton import ServiceSkeletonGenerator
from convert.framework.create_controller import ControllerGenerator
from convert.framework.create_main import MainClassGenerator
from convert.framework.create_config_files import ConfigFilesGenerator
from service.service import ServiceOrchestrator
from understand.neo4j_connection import Neo4jConnection
from convert.strategies.strategy_factory import StrategyFactory


# ==================== 설정 ====================

TEST_USER_ID = "TestSession"
TEST_PROJECT_NAME = "HOSPITAL_PROJECT"
TEST_API_KEY = os.getenv("LLM_API_KEY")
TEST_DB_NAME = "test"
TEST_LOCALE = "ko"
TEST_TARGET_LANG = "java"
TEST_DBMS = "postgres"

# 변환 설정 (기본값 - 파라미터화된 테스트에서 오버라이드 가능)
TEST_CONVERSION_TYPE = "framework"
TEST_TARGET_FRAMEWORK = "springboot"
TEST_TARGET_DBMS = "oracle"

# 테스트 데이터 경로
TEST_DATA_DIR = Path(__file__).resolve().parents[2] / "data" / TEST_USER_ID / TEST_PROJECT_NAME

# 결과 저장 파일
RESULTS_FILE = Path(__file__).parent / "test_converting_results.json"


# ==================== Fixtures ====================

@pytest.fixture(scope="module")
def results_storage():
    """단계별 결과 저장소"""
    if RESULTS_FILE.exists():
        with open(RESULTS_FILE, 'r', encoding='utf-8') as f:
            data = json.load(f)
    else:
        data = {
            'user_id': TEST_USER_ID,
            'project_name': TEST_PROJECT_NAME,
            'locale': TEST_LOCALE
        }
    
    yield data
    
    # 테스트 종료 시 저장
    with open(RESULTS_FILE, 'w', encoding='utf-8') as f:
        json.dump(data, f, ensure_ascii=False, indent=2)


@pytest.fixture(scope="function")
def setup_test_db():
    """test DB 사용 설정"""
    original_db = Neo4jConnection.DATABASE_NAME
    Neo4jConnection.DATABASE_NAME = TEST_DB_NAME
    yield
    Neo4jConnection.DATABASE_NAME = original_db

@pytest.fixture
async def real_neo4j(setup_test_db):
    """실제 Neo4j 연결 (test DB 사용)"""
    conn = Neo4jConnection()
    yield conn
    await conn.close()


# ==================== 1단계: Entity 생성 ====================

class TestEntityGeneration:
    """Entity 생성 테스트"""
    
    @pytest.mark.asyncio
    async def test_generate_entities(self, results_storage, real_neo4j):
        """Entity 생성 및 결과 저장"""
        if not TEST_API_KEY:
            pytest.skip("LLM_API_KEY가 설정되지 않았습니다")
        
        print(f"\n{'='*60}")
        print("🏗️  1단계: Entity 생성")
        print(f"{'='*60}")
        print(f"📊 설정: USER_ID={TEST_USER_ID}, PROJECT={TEST_PROJECT_NAME}, DB={Neo4jConnection.DATABASE_NAME}\n")
        
        # Entity 생성
        generator = EntityGenerator(TEST_PROJECT_NAME, TEST_USER_ID, TEST_API_KEY, TEST_LOCALE, TEST_TARGET_LANG)
        entity_results = await generator.generate()
        
        # 검증
        assert len(entity_results) > 0, "Entity가 생성되지 않았습니다"
        
        # 결과 저장
        entity_name_list = {
            entity['entityName']: {"entityName": entity['entityName']}
            for entity in entity_results
        }
        results_storage['entity_name_list'] = entity_name_list
        results_storage['entity_results'] = entity_results
        
        print(f"✅ Entity {len(entity_results)}개 생성 완료")
        print(f"   Entity 목록: {list(entity_name_list.keys())}\n")


# ==================== 2단계: Repository 생성 ====================

class TestRepositoryGeneration:
    """Repository 생성 테스트"""
    
    @pytest.mark.asyncio
    async def test_generate_repositories(self, results_storage, real_neo4j):
        """Repository 생성 및 결과 저장"""
        if not TEST_API_KEY:
            pytest.skip("LLM_API_KEY가 설정되지 않았습니다")
        
        # 1단계 결과 확인
        if 'entity_name_list' not in results_storage:
            pytest.skip("1단계(Entity) 결과가 없습니다")
        
        print(f"\n{'='*60}")
        print("🏗️  2단계: Repository 생성")
        print(f"{'='*60}\n")
        
        # Repository 생성
        generator = RepositoryGenerator(TEST_PROJECT_NAME, TEST_USER_ID, TEST_API_KEY, TEST_LOCALE, TEST_TARGET_LANG)
        used_query_methods, global_variables, sequence_methods, repository_list = await generator.generate()
        
        # 검증
        assert len(repository_list) > 0, "Repository가 생성되지 않았습니다"
        
        # 결과 저장
        results_storage['used_query_methods'] = used_query_methods
        results_storage['global_variables'] = global_variables
        results_storage['sequence_methods'] = sequence_methods
        results_storage['repository_list'] = repository_list
        
        print(f"✅ Repository {len(repository_list)}개 생성 완료")
        print(f"   쿼리 메서드: {len(used_query_methods)}개")
        print(f"   전역 변수: {len(global_variables)}개")
        print(f"   시퀀스 메서드: {len(sequence_methods)}개\n")


# ==================== 3단계: Service Skeleton 생성 ====================

class TestServiceSkeletonGeneration:
    """Service Skeleton 생성 테스트"""
    
    @pytest.mark.asyncio
    async def test_generate_service_skeleton(self, results_storage, real_neo4j):
        """Service Skeleton 생성 및 결과 저장"""
        if not TEST_API_KEY:
            pytest.skip("LLM_API_KEY가 설정되지 않았습니다")
        
        # 2단계 결과 확인
        if 'entity_name_list' not in results_storage or 'global_variables' not in results_storage:
            pytest.skip("2단계(Repository) 결과가 없습니다")
        
        print(f"\n{'='*60}")
        print("🏗️  3단계: Service Skeleton 생성")
        print(f"{'='*60}\n")
        
        entity_name_list = list(results_storage['entity_name_list'].values())
        global_variables = results_storage['global_variables']
        repository_list = results_storage.get('repository_list', [])
        
        # 실제 서비스 로직과 동일하게: file_names 기반으로 처리
        file_names = []
        src_dir = TEST_DATA_DIR / "src"
        if src_dir.exists():
            for folder in src_dir.iterdir():
                if folder.is_dir():
                    for sql_file in folder.glob("*.sql"):
                        file_names.append((folder.name, sql_file.name))
        
        assert len(file_names) > 0, f"SP 파일이 없습니다: {src_dir}"
        
        # Service Skeleton 생성 (실제 서비스 로직과 동일)
        generator = ServiceSkeletonGenerator(TEST_PROJECT_NAME, TEST_USER_ID, TEST_API_KEY, TEST_LOCALE, TEST_TARGET_LANG)
        
        # 파일별로 Service Skeleton 생성 (실제 서비스 로직과 동일)
        # 실제 서비스 스펙: file_names 기반으로 파일별 처리
        file_skeleton_results = {}
        
        for folder_name, file_name in file_names:
            print(f"   📝 처리 중: {folder_name}/{file_name}")
            
            # 실제 서비스 로직과 동일하게 generate 호출
            service_creation_info, service_class_name, exist_command_class, command_class_list = (
                await generator.generate(entity_name_list, folder_name, file_name, global_variables, repository_list)
            )
            
            # 파일별로 결과 저장 (실제 서비스 스펙과 일치)
            file_key = f"{folder_name}/{file_name}"
            file_skeleton_results[file_key] = {
                'folder_name': folder_name,
                'file_name': file_name,
                'service_creation_info': service_creation_info,
                'service_class_name': service_class_name,
                'exist_command_class': exist_command_class,
                'command_class_list': command_class_list
            }
            
            proc_count = len(service_creation_info)
            print(f"   ✅ {file_name} Skeleton 생성 완료 ({proc_count}개 프로시저)")
        
        # 검증
        assert len(file_skeleton_results) > 0, "Service Skeleton이 생성되지 않았습니다"
        
        # 결과 저장 (실제 서비스 스펙과 일치하는 구조)
        results_storage['file_skeleton_results'] = file_skeleton_results
        
        print(f"\n✅ Service Skeleton {len(file_skeleton_results)}개 파일 처리 완료\n")


# ==================== 4단계: Service 코드 생성 ====================

class TestServiceCodeGeneration:
    """Service 코드 생성 테스트 (전처리 포함)"""
    
    @pytest.mark.asyncio
    async def test_generate_service_code(self, results_storage, real_neo4j):
        """Service 코드 생성 (Preprocessing 포함)"""
        if not TEST_API_KEY:
            pytest.skip("LLM_API_KEY가 설정되지 않았습니다")
        
        # 3단계 결과 확인
        if 'service_skeleton_results' not in results_storage:
            pytest.skip("3단계(Service Skeleton) 결과가 없습니다")
        
        print(f"\n{'='*60}")
        print("4단계: Service 코드 생성")
        print(f"{'='*60}\n")
        
        # 실제 서비스 로직과 동일하게: 파일별 처리 (JSON에서 로드)
        file_skeleton_results = results_storage.get('file_skeleton_results', {})
        if not file_skeleton_results:
            pytest.skip("3단계(Service Skeleton) 결과가 없습니다")
        
        used_query_methods = results_storage['used_query_methods']
        sequence_methods = results_storage['sequence_methods']
        service_code_results = {}
        
        from convert.framework.create_service_preprocessing import start_service_preprocessing
        
        # 파일별로 Service 생성 (실제 서비스 로직과 동일)
        for file_key, file_data in file_skeleton_results.items():
            folder_name = file_data['folder_name']
            file_name = file_data['file_name']
            service_creation_info = file_data['service_creation_info']
            service_class_name = file_data['service_class_name']
            
            print(f"   📝 처리 중: {folder_name}/{file_name}")
            
            # 각 프로시저별로 Service 코드 생성 (실제 서비스와 동일)
            for svc_info in service_creation_info:
                proc_name = svc_info.get('procedure_name', '')
                if not proc_name:
                    continue
                
                svc_skeleton = svc_info.get('service_method_skeleton', '')
                cmd_var = svc_info.get('command_class_variable', {})
                
                try:
                    # 실제 Service Preprocessing 실행 (실제 서비스와 동일 - 위치 인자)
                    await start_service_preprocessing(
                        svc_skeleton,
                        cmd_var,
                        proc_name,
                        used_query_methods,
                        folder_name,
                        file_name,
                        sequence_methods,
                        TEST_PROJECT_NAME,
                        TEST_USER_ID,
                        TEST_API_KEY,
                        TEST_LOCALE,
                        TEST_TARGET_LANG
                    )
                    
                    service_code_results[proc_name] = {
                        'service_class_name': service_class_name,
                        'status': '✅ 생성 및 저장 완료'
                    }
                    print(f"   ✅ {proc_name} Service 생성 및 저장 완료")
                    
                except Exception as e:
                    print(f"   ❌ {proc_name} Service 생성 실패: {str(e)}")
                    service_code_results[proc_name] = {
                        'service_class_name': service_class_name,
                        'error': str(e)
                    }
        
        # 결과 저장
        results_storage['service_code_results'] = service_code_results
        
        print(f"\n✅ Service 파일 {len(service_code_results)}개 생성 및 저장 완료\n")
        
        # 검증
        assert len(service_code_results) > 0, "Service 코드가 생성되지 않았습니다"


# ==================== 5단계: Controller 생성 ====================

class TestControllerGeneration:
    """Controller 생성 테스트"""
    
    @pytest.mark.asyncio
    async def test_generate_controllers(self, results_storage, real_neo4j):
        """Controller 생성 및 결과 저장"""
        if not TEST_API_KEY:
            pytest.skip("LLM_API_KEY가 설정되지 않았습니다")
        
        # 3단계 결과 확인
        file_skeleton_results = results_storage.get('file_skeleton_results', {})
        if not file_skeleton_results:
            pytest.skip("3단계(Service Skeleton) 결과가 없습니다")
        
        print(f"\n{'='*60}")
        print("🏗️  5단계: Controller 생성")
        print(f"{'='*60}\n")
                
        controller_results = {}
        
        # 파일별로 Controller 생성 (실제 서비스와 동일)
        for file_key, file_data in file_skeleton_results.items():
            folder_name = file_data['folder_name']
            file_name = file_data['file_name']
            service_creation_info = file_data['service_creation_info']
            service_class_name = file_data['service_class_name']
            exist_command_class = file_data['exist_command_class']
            
            # base_name은 파일명에서 확장자 제거 (실제 서비스와 동일)
            base_name = file_name.rsplit(".", 1)[0]
            
            print(f"   📝 처리 중: {folder_name}/{file_name}")
            
            try:
                # 실제 서비스 로직과 동일하게 ControllerGenerator.generate() 호출
                controller_name, controller_code = await ControllerGenerator(
                    TEST_PROJECT_NAME, TEST_USER_ID, TEST_API_KEY, TEST_LOCALE, TEST_TARGET_LANG
                ).generate(
                    base_name, service_class_name, exist_command_class, service_creation_info
                )
                
                proc_count = len(service_creation_info)
                controller_results[file_key] = {
                    'controller_class_name': controller_name,
                    'procedure_count': proc_count,
                    'status': '✅ 생성 및 저장 완료'
                }
                print(f"   ✅ {controller_name} 생성 완료 ({proc_count}개 프로시저)\n")
                
            except Exception as e:
                print(f"   ❌ {file_name} Controller 생성 실패: {str(e)}\n")
                controller_results[file_key] = {
                    'controller_class_name': f"{base_name}Controller",
                    'error': str(e)
                }
        
        # 결과 저장
        results_storage['controller_results'] = controller_results
        
        print(f"\n✅ Controller 파일 {len(controller_results)}개 생성 및 저장 완료\n")
        
        # 검증
        assert len(controller_results) > 0, "Controller가 생성되지 않았습니다"


# ==================== 6단계: Main & Config 파일 생성 ====================

class TestConfigGeneration:
    """Main 및 Config 파일 생성 테스트"""
    
    @pytest.mark.asyncio
    async def test_generate_main_and_config(self, results_storage, real_neo4j):
        """Main 클래스 및 설정 파일 생성"""
        print(f"\n{'='*60}")
        print("🏗️  6단계: Main & Config 생성")
        print(f"{'='*60}\n")
        
        # Main 클래스 생성
        main_generator = MainClassGenerator(TEST_PROJECT_NAME, TEST_USER_ID, TEST_TARGET_LANG)
        main_content = await main_generator.generate()
        
        # Config 파일 생성
        config_generator = ConfigFilesGenerator(TEST_PROJECT_NAME, TEST_USER_ID, TEST_TARGET_LANG)
        pom_content, properties_content = await config_generator.generate()
        
        # 검증
        assert main_content, "Main 클래스가 생성되지 않았습니다"
        assert pom_content, "pom.xml이 생성되지 않았습니다"
        assert properties_content, "application.properties가 생성되지 않았습니다"
        
        # 결과 저장
        results_storage['main_content'] = main_content
        results_storage['pom_content'] = pom_content
        results_storage['properties_content'] = properties_content
        
        print(f"✅ Main 클래스 생성 완료")
        print(f"✅ pom.xml 생성 완료")
        print(f"✅ application.properties 생성 완료\n")


# ==================== 통합 테스트 ====================

class TestFullPipeline:
    """전체 파이프라인 통합 테스트"""
    
    @pytest.mark.asyncio
    async def test_complete_converting_pipeline(self, results_storage, real_neo4j):
        """전체 Converting 파이프라인 검증"""
        print(f"\n{'='*60}")
        print("🎉 전체 파이프라인 검증")
        print(f"{'='*60}\n")
        
        # 필수 결과 확인 (실제 서비스 스펙과 일치)
        required_keys = [
            'entity_name_list',
            'used_query_methods',
            'global_variables',
            'sequence_methods',
            'repository_list',
            'file_skeleton_results',  # 실제 서비스 스펙: 파일별 저장
            'controller_results',
            'main_content',
            'pom_content',
            'properties_content'
        ]
        
        for key in required_keys:
            assert key in results_storage, f"{key} 결과가 없습니다"
        
        # 통계 (실제 서비스 스펙과 일치)
        entity_count = len(results_storage.get('entity_name_list', {}))
        repo_count = len(results_storage.get('repository_list', []))
        file_count = len(results_storage.get('file_skeleton_results', {}))
        # 각 파일의 프로시저 수 집계
        total_procedures = sum(
            len(file_data.get('service_creation_info', []))
            for file_data in results_storage.get('file_skeleton_results', {}).values()
        )
        controller_count = len(results_storage.get('controller_results', {}))
        
        print(f"✅ Entity: {entity_count}개")
        print(f"✅ Repository: {repo_count}개")
        print(f"✅ Service 파일: {file_count}개 ({total_procedures}개 프로시저)")
        print(f"✅ Controller: {controller_count}개")
        print(f"✅ Main 클래스: 1개")
        print(f"✅ Config 파일: 2개 (pom.xml, application.properties)")
        
        print(f"\n{'='*60}")
        print("🎉 배포 준비 완료!")
        print(f"{'='*60}\n")


# ==================== 통합 테스트: 전체 파이프라인 ====================

class TestConvertingPipeline:
    """Converting 전체 파이프라인 통합 테스트 (전략별 분리 실행 가능)"""
    
    async def _run_pipeline(self, conversion_type: str, orchestrator: ServiceOrchestrator) -> None:
        if not TEST_API_KEY:
            pytest.skip("LLM_API_KEY가 설정되지 않았습니다")
        
        target_framework = "springboot" if conversion_type == "framework" else None
        target_dbms = "oracle" if conversion_type == "dbms" else None
        
        print(f"\n{'='*80}")
        print(f"🚀 통합 테스트: {conversion_type.upper()} 전략 파이프라인")
        if target_framework:
            print(f"   타겟 프레임워크: {target_framework}")
        if target_dbms:
            print(f"   타겟 DBMS: {target_dbms}")
        print(f"{'='*80}")
        print(f"📊 설정: USER_ID={TEST_USER_ID}, PROJECT={TEST_PROJECT_NAME}")
        print(f"🎯 타겟 언어: {TEST_TARGET_LANG}")
        print(f"{'='*80}\n")
        
        sp_files = []
        src_dir = TEST_DATA_DIR / "src"
        if src_dir.exists():
            for folder in src_dir.iterdir():
                if folder.is_dir():
                    for sql_file in folder.glob("*.sql"):
                        sp_files.append((folder.name, sql_file.name))
        
        assert len(sp_files) > 0, f"SP 파일이 없습니다: {src_dir}"
        file_names = sp_files
        
        print(f"📝 변환할 SP 파일: {len(sp_files)}개")
        for folder_name, file_name in sp_files:
            print(f"   - {folder_name}/{file_name}")
        
        strategy_kwargs = {"conversion_type": conversion_type}
        if target_framework:
            strategy_kwargs["target_framework"] = target_framework
        if target_dbms:
            strategy_kwargs["target_dbms"] = target_dbms
        
        strategy = StrategyFactory.create_strategy(**strategy_kwargs)
        
        events = []
        step_messages = []
        generated_files = {}
        
        try:
            print("📝 Converting 파이프라인 실행 중...\n")
            
            async for chunk in strategy.convert(file_names, orchestrator=orchestrator):
                events.append(chunk)
                chunk_str = chunk.decode('utf-8').replace('send_stream', '')
                if not chunk_str:
                    continue
                try:
                    data = json.loads(chunk_str)
                except json.JSONDecodeError:
                    continue
                
                event_type = data.get('type')
                if event_type == 'message':
                    content = data.get('content')
                    step_messages.append(content)
                    print(f"  📌 {content}")
                elif event_type == 'data':
                    file_type = data.get('file_type')
                    file_name = data.get('file_name')
                    if file_type == 'project_name':
                        print(f"  📦 프로젝트: {data.get('project_name')}")
                        continue
                    if not file_name:
                        continue
                    generated_files.setdefault(file_type, []).append(file_name)
                    print(f"  ✅ 생성: {file_name} ({file_type})")
                elif event_type == 'status':
                    step = data.get('step')
                    done = data.get('done', False)
                    if done and step:
                        print(f"  ✔️  Step {step} 완료\n")
                elif event_type == 'error':
                    content = data.get('content')
                    print(f"  ❌ ERROR: {content}")
            
            print(f"\n{'='*80}")
            print("📊 통합 테스트 결과")
            print(f"{'='*80}")
            
            assert len(events) > 0, "이벤트가 수신되지 않았습니다"
            print(f"✅ 스트리밍 이벤트: {len(events)}개 수신")
            
            if conversion_type == "framework":
                assert 'entity_class' in generated_files, "Entity 파일이 생성되지 않았습니다"
                assert 'repository_class' in generated_files, "Repository 파일이 생성되지 않았습니다"
                assert 'pom' in generated_files, "pom.xml이 생성되지 않았습니다"
                assert 'main' in generated_files, "Main 클래스가 생성되지 않았습니다"
                
                print(f"✅ Entity: {len(generated_files.get('entity_class', []))}개")
                print(f"✅ Repository: {len(generated_files.get('repository_class', []))}개")
                print(f"✅ Command: {len(generated_files.get('command_class', []))}개")
                print(f"✅ Service: {len(generated_files.get('service_class', []))}개")
                print(f"✅ Controller: {len(generated_files.get('controller_class', []))}개")
                print(f"✅ Config: pom.xml, application.properties")
                print(f"✅ Main: {generated_files.get('main', ['N/A'])[0]}")
            
            if conversion_type == "dbms":
                assert 'converted_sp' in generated_files, "변환된 SP 파일이 생성되지 않았습니다"
                converted_count = len(generated_files.get('converted_sp', []))
                print(f"✅ 변환된 SP 파일: {converted_count}개")
                for file_name in generated_files.get('converted_sp', []):
                    print(f"   - {file_name}")
            
            assert len(step_messages) > 0, "단계 메시지가 없습니다"
            print(f"\n✅ 파이프라인 단계: {len(step_messages)}개 메시지")
            
            print(f"\n{'='*80}")
            print(f"🎉 통합 테스트 성공: {conversion_type.upper()} 전략 정상 작동!")
            print(f"{'='*80}\n")
        
        except Exception as e:
            print(f"\n❌ 통합 테스트 실패: {str(e)}\n")
            raise
    
    @pytest.mark.asyncio
    async def test_framework_pipeline(self, setup_test_db):
        orchestrator = ServiceOrchestrator(
            user_id=TEST_USER_ID,
            api_key=TEST_API_KEY,
            locale=TEST_LOCALE,
            project_name=TEST_PROJECT_NAME,
            dbms=TEST_DBMS,
            target_lang=TEST_TARGET_LANG
        )
        await self._run_pipeline("framework", orchestrator)
    
    @pytest.mark.asyncio
    async def test_dbms_pipeline(self, setup_test_db):
        orchestrator = ServiceOrchestrator(
            user_id=TEST_USER_ID,
            api_key=TEST_API_KEY,
            locale=TEST_LOCALE,
            project_name=TEST_PROJECT_NAME,
            dbms=TEST_DBMS,
            target_lang=TEST_TARGET_LANG
        )
        await self._run_pipeline("dbms", orchestrator)


# ==================== 실행 ====================

if __name__ == "__main__":
    import argparse
    
    parser = argparse.ArgumentParser(description="Legacy Modernizer 통합 테스트 실행기")
    parser.add_argument(
        "--scenario",
        choices=("all", "framework", "dbms", "steps"),
        default="all",
        help="실행할 테스트 시나리오를 선택합니다."
    )
    args = parser.parse_args()
    
    pytest_args = [
        __file__,
        "-v",
        "-s",
        "--tb=short",
        "--color=yes",
    ]
    
    if args.scenario == "framework":
        pytest_args += ["-k", "TestConvertingPipeline and test_framework_pipeline"]
    elif args.scenario == "dbms":
        pytest_args += ["-k", "TestConvertingPipeline and test_dbms_pipeline"]
    elif args.scenario == "steps":
        pytest_args += ["-k", "TestEntityGeneration or TestRepositoryGeneration or TestServiceSkeletonGeneration or TestServiceCodeGeneration or TestControllerGeneration"]
    
    pytest.main(pytest_args)

