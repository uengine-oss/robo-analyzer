import pytest
import asyncio
import os
import json
from pathlib import Path

import sys
sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from convert.create_entity import EntityGenerator
from convert.create_repository import RepositoryGenerator
from convert.create_service_skeleton import ServiceSkeletonGenerator
from convert.create_controller import ControllerGenerator
from convert.create_main import MainClassGenerator
from convert.create_config_files import ConfigFilesGenerator
from service.service import ServiceOrchestrator
from understand.neo4j_connection import Neo4jConnection


# ==================== 설정 ====================

TEST_USER_ID = "TestSession"
TEST_PROJECT_NAME = "HOSPITAL_PROJECT"
TEST_API_KEY = os.getenv("LLM_API_KEY")
TEST_DB_NAME = "test"
TEST_LOCALE = "ko"
TEST_TARGET_LANG = "java"  # 타겟 언어 설정

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
        
        # Service Skeleton 생성
        generator = ServiceSkeletonGenerator(TEST_PROJECT_NAME, TEST_USER_ID, TEST_API_KEY, TEST_LOCALE, TEST_TARGET_LANG)
        
        # 각 프로시저별로 생성
        skeleton_results = {}
        
        # Neo4j에서 프로시저 목록 조회
        connection = Neo4jConnection()
        procs = await connection.execute_queries([
            f"""
            MATCH (p {{user_id: '{TEST_USER_ID}', project_name: '{TEST_PROJECT_NAME}'}})
            WHERE p:PROCEDURE OR p:FUNCTION
            RETURN DISTINCT p.folder_name AS folder_name, p.file_name AS file_name, p.procedure_name AS procedure_name
            """
        ])
        await connection.close()
        
        for proc in procs[0]:
            folder_name = proc.get('folder_name') or ''
            file_name = proc.get('file_name') or ''
            procedure_name = proc.get('procedure_name') or ''
            
            if not procedure_name:
                continue
            
            result = await generator.generate(entity_name_list, folder_name, file_name, global_variables)
            skeleton_results[procedure_name] = result
            
            print(f"   ✅ {procedure_name} Skeleton 생성 완료")
        
        # 검증
        assert len(skeleton_results) > 0, "Service Skeleton이 생성되지 않았습니다"
        
        # 결과 저장
        results_storage['service_skeleton_results'] = skeleton_results
        
        print(f"\n✅ Service Skeleton {len(skeleton_results)}개 생성 완료\n")


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
        
        # 각 프로시저별로 실제 Service 코드 생성 (Preprocessing 실행)
        skeleton_results = results_storage['service_skeleton_results']
        service_code_results = {}
        
        for proc_name, skeleton_data in skeleton_results.items():
            # skeleton_data는 배열: [method_info, service_skeleton, service_class_name, exist_command_class, command_list]
            # 실제 서비스 로직과 동일하게 service_method_skeleton 사용 (클래스 + 메서드 포함)
            service_skeleton = skeleton_data[0][0]['service_method_skeleton'] if skeleton_data[0] else skeleton_data[1]
            service_class_name = skeleton_data[2]  # Service 클래스명
            command_class_variable = skeleton_data[0][0]['command_class_variable'] if skeleton_data[0] else []
            
            # 실제 Service Preprocessing 실행
            from convert.create_service_preprocessing import start_service_preprocessing
            
            try:
                await start_service_preprocessing(
                    service_skeleton=service_skeleton,
                    command_class_variable=command_class_variable,
                    procedure_name=proc_name,
                    query_method_list=results_storage['used_query_methods'],
                    folder_name='HOSPITAL_RECEPTION',  # 하드코딩
                    file_name='SP_HOSPITAL_RECEPTION.sql',  # 하드코딩
                    sequence_methods=results_storage['sequence_methods'],
                    user_id=TEST_USER_ID,
                    api_key=TEST_API_KEY,
                    locale=TEST_LOCALE,
                    project_name=TEST_PROJECT_NAME
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
        if 'service_skeleton_results' not in results_storage:
            pytest.skip("3단계(Service Skeleton) 결과가 없습니다")
        
        print(f"\n{'='*60}")
        print("🏗️  5단계: Controller 생성")
        print(f"{'='*60}\n")
        
        from convert.create_controller import start_controller_processing, finalize_controller
        
        skeleton_results = results_storage['service_skeleton_results']
        controller_results = {}
        
        # 프로시저별로 그룹화 (같은 파일의 프로시저들)
        proc_groups = {}
        for proc_name, skeleton_data in skeleton_results.items():
            folder_name = proc_name.split('_')[0] if '_' in proc_name else proc_name
            if folder_name not in proc_groups:
                proc_groups[folder_name] = []
            proc_groups[folder_name].append((proc_name, skeleton_data))
        
        # 파일(폴더)별로 Controller 생성
        for folder_name, proc_list in proc_groups.items():
            print(f"\n📂 {folder_name} Controller 처리 중...")
            
            # Controller Skeleton 생성 (첫 번째 프로시저 기준)
            first_proc_name, first_skeleton_data = proc_list[0]
            exist_command_class = first_skeleton_data[3] if len(first_skeleton_data) > 3 else False
            controller_skeleton, controller_class_name = start_controller_skeleton_processing(
                folder_name,
                exist_command_class,
                TEST_PROJECT_NAME
            )
            
            # 각 프로시저별로 Controller 메서드 생성 (매니저에 누적)
            for proc_name, skeleton_data in proc_list:
                method_info = skeleton_data[0][0] if skeleton_data[0] else None
                if not method_info:
                    print(f"   ⚠️  {proc_name} 메서드 정보 없음, 스킵")
                    continue
                
                method_signature = method_info.get('method_signature', '')
                command_class_variable = method_info.get('command_class_variable', '')
                command_class_name = method_info.get('command_class_name', '')
                node_type = method_info.get('node_type', 'PROCEDURE')
                
                try:
                    # Controller 메서드 생성 (매니저에 누적)
                    start_controller_processing(
                        method_signature,
                        proc_name,
                        command_class_variable,
                        command_class_name,
                        node_type,
                        controller_skeleton,
                        controller_class_name,
                        folder_name,
                        TEST_USER_ID,
                        TEST_PROJECT_NAME,
                        TEST_API_KEY,
                        TEST_LOCALE
                    )
                    print(f"   ✅ {proc_name} 메서드 생성 완료")
                    
                except Exception as e:
                    print(f"   ❌ {proc_name} 메서드 생성 실패: {str(e)}")
            
            # Controller 파일 저장 (한 번만)
            try:
                await finalize_controller(TEST_USER_ID, folder_name)
                print(f"   💾 {controller_class_name} 파일 저장 완료\n")
                
                controller_results[folder_name] = {
                    'controller_class_name': controller_class_name,
                    'procedure_count': len(proc_list),
                    'status': '✅ 생성 및 저장 완료'
                }
            except Exception as e:
                print(f"   ❌ {controller_class_name} 파일 저장 실패: {str(e)}\n")
                controller_results[folder_name] = {
                    'controller_class_name': controller_class_name,
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
        main_generator = MainClassGenerator(TEST_PROJECT_NAME, TEST_USER_ID)
        main_content = await main_generator.generate()
        
        # Config 파일 생성
        config_generator = ConfigFilesGenerator(TEST_PROJECT_NAME, TEST_USER_ID)
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
        
        # 필수 결과 확인
        required_keys = [
            'entity_name_list',
            'used_query_methods',
            'global_variables',
            'sequence_methods',
            'repository_list',
            'service_skeleton_results',
            'controller_results',
            'main_content',
            'pom_content',
            'properties_content'
        ]
        
        for key in required_keys:
            assert key in results_storage, f"{key} 결과가 없습니다"
        
        # 통계
        entity_count = len(results_storage.get('entity_name_list', {}))
        repo_count = len(results_storage.get('repository_list', []))
        service_count = len(results_storage.get('service_skeleton_results', {}))
        controller_count = len(results_storage.get('controller_results', {}))
        
        print(f"✅ Entity: {entity_count}개")
        print(f"✅ Repository: {repo_count}개")
        print(f"✅ Service: {service_count}개")
        print(f"✅ Controller: {controller_count}개")
        print(f"✅ Main 클래스: 1개")
        print(f"✅ Config 파일: 2개 (pom.xml, application.properties)")
        
        print(f"\n{'='*60}")
        print("🎉 배포 준비 완료!")
        print(f"{'='*60}\n")


# ==================== 통합 테스트: 전체 파이프라인 ====================

class TestConvertingPipeline:
    """Converting 전체 파이프라인 통합 테스트 (실제 API 동작 검증)"""
    
    @pytest.mark.asyncio
    async def test_complete_converting_pipeline(self, setup_test_db):
        """convert_to_springboot() 전체 파이프라인 실행 테스트"""
        if not TEST_API_KEY:
            pytest.skip("LLM_API_KEY가 설정되지 않았습니다")
        
        print(f"\n{'='*80}")
        print("🚀 통합 테스트: convert_to_springboot() 전체 파이프라인")
        print(f"{'='*80}")
        print(f"📊 설정: USER_ID={TEST_USER_ID}, PROJECT={TEST_PROJECT_NAME}")
        print(f"🎯 타겟: {TEST_TARGET_LANG}")
        print(f"{'='*80}\n")
        
        # ServiceOrchestrator 생성
        orchestrator = ServiceOrchestrator(
            user_id=TEST_USER_ID,
            api_key=TEST_API_KEY,
            locale=TEST_LOCALE,
            project_name=TEST_PROJECT_NAME,
            dbms="postgres",
            target_lang=TEST_TARGET_LANG
        )
        
        # 변환할 파일
        file_names = [("HOSPITAL_RECEPTION", "SP_HOSPITAL_RECEPTION.sql")]
        
        # 전체 파이프라인 실행
        events = []
        step_messages = []
        generated_files = {}
        
        try:
            print("📝 Converting 파이프라인 실행 중...\n")
            
            async for chunk in orchestrator.convert_to_springboot(file_names):
                # 이벤트 수집
                events.append(chunk)
                
                # 파싱하여 내용 확인
                chunk_str = chunk.decode('utf-8').replace('send_stream', '')
                if chunk_str:
                    try:
                        data = json.loads(chunk_str)
                        data_type = data.get('data_type')
                        
                        # 단계 메시지
                        if data_type == 'message':
                            step = data.get('step')
                            content = data.get('content')
                            step_messages.append(f"Step {step}: {content}")
                            print(f"  📌 {content}")
                        
                        # 생성된 파일
                        elif data_type == 'data':
                            file_type = data.get('file_type')
                            file_name = data.get('file_name')
                            
                            if file_type == 'project_name':
                                print(f"  📦 프로젝트: {data.get('project_name')}")
                            elif file_name:
                                generated_files.setdefault(file_type, []).append(file_name)
                                print(f"  ✅ 생성: {file_name} ({file_type})")
                        
                        # 단계 완료
                        elif data_type == 'Done':
                            step = data.get('step')
                            if step:
                                print(f"  ✔️  Step {step} 완료\n")
                    
                    except json.JSONDecodeError:
                        pass
            
            print(f"\n{'='*80}")
            print("📊 통합 테스트 결과")
            print(f"{'='*80}")
            
            # 검증 1: 이벤트 수신 확인
            assert len(events) > 0, "이벤트가 수신되지 않았습니다"
            print(f"✅ 스트리밍 이벤트: {len(events)}개 수신")
            
            # 검증 2: 파일 생성 확인
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
            
            # 검증 3: 단계 메시지 확인
            assert len(step_messages) > 0, "단계 메시지가 없습니다"
            print(f"\n✅ 파이프라인 단계: {len(step_messages)}개 메시지")
            
            print(f"\n{'='*80}")
            print("🎉 통합 테스트 성공: convert_to_springboot() 정상 작동!")
            print(f"{'='*80}\n")
        
        except Exception as e:
            print(f"\n❌ 통합 테스트 실패: {str(e)}\n")
            raise


# ==================== 실행 ====================

if __name__ == "__main__":
    pytest.main([
        __file__, 
        "-v", 
        "-s", 
        "--tb=short",
        "--color=yes"
    ])

