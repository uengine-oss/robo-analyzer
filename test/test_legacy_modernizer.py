import unittest
import os
import sys
import json
import logging
import asyncio
import dotenv
from unittest import IsolatedAsyncioTestCase

# 프로젝트 루트 경로 추가
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..', '..')))

# 필요한 모듈 임포트
from service.service import generate_and_execute_cypherQuery, generate_spring_boot_project
from convert.create_entity import start_entity_processing
from convert.create_repository import start_repository_processing
from convert.create_service_skeleton import start_service_skeleton_processing
from convert.create_service_preprocessing import start_service_preprocessing
from convert.create_service_postprocessing import start_service_postprocessing, generate_service_class
from convert.create_controller_skeleton import start_controller_skeleton_processing
from convert.create_controller import start_controller_processing, generate_controller_class
from convert.create_pomxml import start_pomxml_processing
from convert.create_properties import start_APLproperties_processing
from convert.create_main import start_main_processing

# 환경변수 로드
dotenv.load_dotenv()

# 로그 설정 간소화
logging.basicConfig(level=logging.INFO, format='%(levelname)s: %(message)s')
logging.getLogger('asyncio').setLevel(logging.ERROR)
for logger in ['anthropic', 'langchain', 'urllib3', 'uvicorn', 'fastapi']:
    logging.getLogger(logger).setLevel(logging.CRITICAL)


class LegacyModernizerTestRunner(IsolatedAsyncioTestCase):
    """레거시 코드 현대화 통합 테스트 클래스"""
    
    def setUp(self):
        """테스트 설정"""
        # 테스트 파일 설정
        self.file_names = [
            ("SP_HOSPITAL_RECEPTION.sql", "TPX_HOSPITAL_RECEPTION"),
            # 필요시 추가 파일 활성화
        ]
        
        # 환경변수에서 설정값 로드
        self.session_uuid = os.getenv("TEST_SESSION_UUID", "TestSession")
        self.api_key = os.getenv("TEST_API_KEY", "your-api-key")
        self.project_name = os.getenv("TEST_PROJECT_NAME", "hospital")
        
        # 결과 저장 디렉토리
        self.result_dir = os.path.join('test', 'results')
        os.makedirs(self.result_dir, exist_ok=True)
        
        # 테스트 데이터 저장소
        self.test_data = {}

    def _save_result(self, name, data):
        """결과 저장 헬퍼 함수"""
        path = os.path.join(self.result_dir, f'{name}.json')
        with open(path, 'w', encoding='utf-8') as f:
            json.dump(data, f, ensure_ascii=False, indent=2)
    
    #==========================================================================
    # UNDERSTANDING 테스트 섹션
    #==========================================================================
    
    async def test_a01_understand_create_cypher(self):
        """코드를 분석하여 Cypher 쿼리를 생성하고 Neo4j에 그래프 생성"""
        logging.info("===== 코드 분석 및 Neo4j 그래프 생성 테스트 =====")
        
        results = []
        async for result in generate_and_execute_cypherQuery(
            self.file_names, self.session_uuid, self.api_key
        ):
            results.append(result)
        
        self._save_result('understanding_results', {'results': results})
        self.assertTrue(len(results) > 0)

    #==========================================================================
    # CONVERTING 단계별 테스트 섹션
    #==========================================================================
    
    async def test_b01_create_entity(self):
        """1단계: 엔티티 클래스 생성 테스트"""
        logging.info("===== 1단계: 엔티티 클래스 생성 테스트 =====")
        
        entity_results = await start_entity_processing(
            self.file_names, self.session_uuid, self.api_key, self.project_name
        )
        
        # 결과 저장
        entity_dict = {entity['entityName']: entity['entityCode'] for entity in entity_results}
        self._save_result('01_entity_results', entity_dict)
        self.test_data['entity_results'] = entity_results
        
        self.assertTrue(len(entity_results) > 0)
        return entity_results

    async def test_b02_create_repository(self):
        """2단계: 리포지토리 인터페이스 생성 테스트"""
        logging.info("===== 2단계: 리포지토리 인터페이스 생성 테스트 =====")
        
        query_methods, global_vars, seq_methods, repo_results = await start_repository_processing(
            self.file_names, self.session_uuid, self.api_key, self.project_name
        )
        
        # 결과 저장
        repo_dict = {
            'query_methods': query_methods,
            'global_variables': global_vars,
            'sequence_methods': seq_methods,
            'repositories': {repo['repositoryName']: repo['code'] for repo in repo_results}
        }
        self._save_result('02_repository_results', repo_dict)
        
        # 다음 테스트에서 사용할 데이터 저장
        self.test_data.update({
            'query_methods': query_methods,
            'global_variables': global_vars,
            'sequence_methods': seq_methods,
            'repository_results': repo_results
        })
        
        self.assertTrue(len(repo_results) > 0)
        return repo_results

    async def test_b03_create_service_skeleton(self):
        """3단계: 서비스 스켈레톤 생성 테스트"""
        logging.info("===== 3단계: 서비스 스켈레톤 생성 테스트 =====")
        
        if 'entity_results' not in self.test_data:
            await self.test_b01_create_entity()
        
        global_vars = self.test_data.get('global_variables', [])
        all_skeletons = {}
        
        for _, object_name in self.file_names:
            # 엔티티 리스트 준비
            entity_list = [{"entityName": entity["entityName"]} for entity in self.test_data['entity_results']]
            
            # 서비스 스켈레톤 생성
            service_info, skeleton, class_name, exist_command, command_classes = (
                await start_service_skeleton_processing(
                    entity_list, object_name, global_vars,
                    self.session_uuid, self.api_key, self.project_name
                )
            )
            
            all_skeletons[object_name] = {
                'service_info': service_info,
                'skeleton': skeleton,
                'class_name': class_name,
                'exist_command': exist_command,
                'command_classes': command_classes
            }
        
        # 결과 저장
        self._save_result('03_service_skeleton_results', all_skeletons)
        self.test_data['service_skeletons'] = all_skeletons
        
        self.assertTrue(len(all_skeletons) > 0)
        return all_skeletons

    async def test_b04_create_service_controller(self):
        """4단계: 서비스 및 컨트롤러 생성 테스트"""
        logging.info("===== 4단계: 서비스 및 컨트롤러 생성 테스트 =====")
        
        # 필요한 데이터 로드
        if 'service_skeletons' not in self.test_data:
            await self.test_b03_create_service_skeleton()
        
        if 'query_methods' not in self.test_data:
            await self.test_b02_create_repository()
        
        result = {}
        
        for _, object_name in self.file_names:
            service_data = self.test_data['service_skeletons'][object_name]
            service_info = service_data['service_info']
            service_skeleton = service_data['skeleton']
            service_class_name = service_data['class_name']
            exist_command = service_data['exist_command']
            
            # 컨트롤러 스켈레톤 생성
            controller_skeleton, controller_class_name = await start_controller_skeleton_processing(
                object_name, exist_command, self.project_name
            )
            
            # 서비스 및 컨트롤러 메서드 처리
            merge_method_code = ""
            merge_controller_method_code = ""
            
            for data in service_info:
                # 서비스 처리 로직
                merge_method_code = await self._process_service_method(
                    data, object_name, merge_method_code
                )
                
                # 컨트롤러 처리
                merge_controller_method_code = await start_controller_processing(
                    data['method_signature'],
                    data['procedure_name'],
                    data['command_class_variable'],
                    data['command_class_name'],
                    data['node_type'],
                    merge_controller_method_code,
                    controller_skeleton,
                    object_name,
                    self.session_uuid,
                    self.api_key,
                    self.project_name
                )
            
            # 최종 클래스 생성
            service_code = await generate_service_class(
                service_skeleton, service_class_name, merge_method_code,
                self.session_uuid, self.project_name
            )
            
            controller_code = await generate_controller_class(
                controller_skeleton, controller_class_name, merge_controller_method_code,
                self.session_uuid, self.project_name
            )
            
            result[object_name] = {
                'service_code': service_code,
                'controller_code': controller_code,
                'service_class_name': service_class_name,
                'controller_class_name': controller_class_name
            }
        
        # 결과 저장
        self._save_result('04_service_controller_results', result)
        self.test_data['service_controller_results'] = result
        
        self.assertTrue(len(result) > 0)
        return result

    async def _process_service_method(self, data, object_name, merge_code):
        """서비스 메서드 처리 헬퍼 함수"""
        # 서비스 전처리
        variable_nodes = await start_service_preprocessing(
            data['service_method_skeleton'],
            data['command_class_variable'],
            data['procedure_name'],
            self.test_data.get('query_methods', {}),
            object_name,
            self.test_data.get('sequence_methods', []),
            self.session_uuid,
            self.api_key
        )
        
        # 서비스 후처리 (검증 단계 생략)
        return await start_service_postprocessing(
            data['method_skeleton_code'],
            data['procedure_name'],
            object_name,
            merge_code,
            self.session_uuid
        )

    async def test_b05_create_project_files(self):
        """5단계: 프로젝트 파일 생성 테스트"""
        logging.info("===== 5단계: 프로젝트 파일 생성 테스트 =====")
        
        # pom.xml 생성
        pom_xml = await start_pomxml_processing(self.session_uuid, self.project_name)
        
        # application.properties 생성
        properties = await start_APLproperties_processing(self.session_uuid, self.project_name)
        
        # 메인 어플리케이션 클래스 생성
        main_app = await start_main_processing(self.session_uuid, self.project_name)
        
        result = {
            'pom_xml': pom_xml,
            'application_properties': properties,
            'main_application': main_app
        }
        
        # 결과 저장
        self._save_result('05_project_files_results', result)
        self.test_data['project_files'] = result
        
        self.assertTrue(all([pom_xml, properties, main_app]))
        return result

    async def test_b06_full_project_generation(self):
        """전체 스프링부트 프로젝트 생성 테스트"""
        logging.info("===== 전체 스프링부트 프로젝트 생성 테스트 =====")
        
        results = []
        async for result in generate_spring_boot_project(
            self.file_names, self.session_uuid, self.api_key
        ):
            # 바이트 문자열 처리
            if isinstance(result, bytes):
                result = result.decode('utf-8').replace('send_stream', '')
                try:
                    result = json.loads(result)
                except:
                    pass
            
            # 결과 수집
            results.append(result)
        
        # 파일 타입별 결과 분류
        categorized = {}
        for result in results:
            if isinstance(result, dict) and result.get('data_type') == 'data':
                file_type = result.get('file_type')
                if file_type not in categorized:
                    categorized[file_type] = []
                categorized[file_type].append(result)
        
        # 결과 저장
        self._save_result('06_full_project_generation', categorized)
        
        self.assertTrue(len(results) > 0)
        return results


# 테스트 실행기
if __name__ == '__main__':
    import argparse
    
    parser = argparse.ArgumentParser(description='레거시 코드 현대화 테스트')
    parser.add_argument('--mode', choices=['understand', 'convert', 'all'], 
                        default='all', help='테스트 모드 (understand, convert, all)')
    parser.add_argument('--step', type=int, default=0, 
                        help='컨버팅 단계 (1:엔티티, 2:리포지토리, 3:서비스스켈레톤, 4:서비스컨트롤러, 5:프로젝트파일, 6:전체)')
    args = parser.parse_args()
    
    # 테스트 스위트 생성
    suite = unittest.TestSuite()
    runner = unittest.TextTestRunner()
    
    # 테스트 모드에 따른 테스트 추가
    if args.mode in ['understand', 'all']:
        suite.addTest(LegacyModernizerTestRunner('test_a01_understand_create_cypher'))
    
    if args.mode in ['convert', 'all']:
        if args.step == 0 or args.step == 1:
            suite.addTest(LegacyModernizerTestRunner('test_b01_create_entity'))
        if args.step == 0 or args.step == 2:
            suite.addTest(LegacyModernizerTestRunner('test_b02_create_repository'))
        if args.step == 0 or args.step == 3:
            suite.addTest(LegacyModernizerTestRunner('test_b03_create_service_skeleton'))
        if args.step == 0 or args.step == 4:
            suite.addTest(LegacyModernizerTestRunner('test_b04_create_service_controller'))
        if args.step == 0 or args.step == 5:
            suite.addTest(LegacyModernizerTestRunner('test_b05_create_project_files'))
        if args.step == 0 or args.step == 6:
            suite.addTest(LegacyModernizerTestRunner('test_b06_full_project_generation'))
    
    # 테스트 실행
    runner.run(suite)