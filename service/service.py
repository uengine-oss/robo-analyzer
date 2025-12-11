"""Understanding/Converting 파이프라인을 오케스트레이션하는 서비스 레이어."""

import logging
import shutil
import zipfile
import os
from typing import AsyncGenerator
from fastapi import HTTPException

from understand.strategy.strategy_factory import UnderstandStrategyFactory
from understand.neo4j_connection import Neo4jConnection
from util.exception import FileProcessingError
from util.llm_client import get_llm


# ----- 상수 -----
BASE_DIR = os.getenv('DOCKER_COMPOSE_CONTEXT') or os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
TEST_SESSIONS = ("EN_TestSession", "KO_TestSession")


# ----- 서비스 오케스트레이터 클래스 -----
class ServiceOrchestrator:
    """
    Understanding과 Converting 전체 프로세스를 관리하는 오케스트레이터 클래스
    """

    def __init__(
        self,
        user_id: str,
        api_key: str,
        locale: str,
        project_name: str,
        dbms: str,
        target_lang: str = 'java',
        update_mode: str = 'merge',
        analysis_strategy: str = 'dbms',
    ):
        """
        ServiceOrchestrator 초기화
        
        Args:
            user_id: 사용자 식별자
            api_key: LLM API 키
            locale: 언어 설정
            project_name: 프로젝트 이름
            dbms: 데이터베이스 종류
            target_lang: 타겟 언어 (기본값: 'java')
        """
        self.user_id = user_id
        self.api_key = api_key
        self.locale = locale
        self.project_name = project_name
        self.dbms = dbms
        self.target_lang = target_lang
        self.update_mode = update_mode if update_mode in ('skip', 'merge') else 'merge'
        self.analysis_strategy = (analysis_strategy or 'dbms').lower()
        self.project_name_cap = project_name.capitalize() if project_name else ''
        
        # 디렉토리 경로 설정
        if project_name:
            user_base = os.path.join(BASE_DIR, 'data', user_id, project_name)
            self.dirs = {
                'plsql': os.path.join(user_base, "src"),
                'analysis': os.path.join(user_base, "analysis"),
                'ddl': os.path.join(user_base, "ddl"),
            }

    # ----- API 키 검증 -----

    async def validate_api_key(self) -> None:
        """API 키가 유효한지 확인합니다.

        테스트 세션(EN/KO_TestSession)은 외부 호출 없이 통과시키고, 그 외에는
        간단한 ping 호출로 OpenAI 호환 API가 응답하는지 검증합니다.
        실패 시 HTTP 401 오류를 발생시켜 프론트엔드에서 바로 감지할 수 있도록 합니다.
        """
        if self.user_id in TEST_SESSIONS:
            return
        
        try:
            llm = get_llm(api_key=self.api_key)
            if not llm.invoke("ping"):
                raise HTTPException(status_code=401, detail="API 키 검증 실패: ping 실패")
        except Exception as e:
            logging.error(f"API 키 검증 실패: {str(e)}")
            raise HTTPException(status_code=401, detail=f"API 키 검증 실패: {e.__class__.__name__}: {str(e)}")

    # ----- Understanding 프로세스 -----

    async def understand_project(self, file_names: list) -> AsyncGenerator[bytes, None]:
        """PL/SQL 파일 묶음을 분석하고 Neo4j 그래프 이벤트를 스트리밍합니다.

        Args:
            file_names: `(folder_name, file_name)` 형식의 튜플 리스트

        Yields:
            bytes: 프론트엔드로 전송하는 스트리밍 이벤트(JSON 직렬화 결과)
        """
        strategy = UnderstandStrategyFactory.create_strategy(self.analysis_strategy)
        async for chunk in strategy.understand(file_names=file_names, orchestrator=self):
            yield chunk


    # ----- Converting 프로세스 -----

    async def convert_project(
        self,
        file_names: list,
        conversion_type: str = 'framework',
        target_framework: str = 'springboot',
        target_dbms: str = 'oracle'
    ) -> AsyncGenerator[bytes, None]:
        """변환 타입에 따라 적절한 전략을 선택하여 변환을 수행합니다.
        
        Args:
            file_names: 변환할 파일 목록 [(folder_name, file_name), ...]
            conversion_type: 변환 타입 ('framework' 또는 'dbms')
            target_framework: 타겟 프레임워크 (기본값: 'springboot')
            target_dbms: 타겟 DBMS (기본값: 'oracle')
            
        Yields:
            bytes: 스트리밍 응답 데이터
        """
        from convert.strategies.strategy_factory import StrategyFactory
        
        logging.info("Convert: type=%s, project=%s, files=%d, target=%s",
                    conversion_type, self.project_name, len(file_names),
                    target_framework if conversion_type == 'framework' else f"{self.dbms}→{target_dbms}")

        # 전략 생성
        strategy = StrategyFactory.create_strategy(
            conversion_type,
            target_dbms=target_dbms,
            target_framework=target_framework
        )

        # 전략 실행
        async for chunk in strategy.convert(file_names, orchestrator=self):
            yield chunk

    # ----- 파일 작업 -----

    async def zip_project(self, source_directory: str, output_zip_path: str) -> None:
        """프로젝트 디렉토리를 ZIP으로 압축"""
        try:
            os.makedirs(os.path.dirname(output_zip_path), exist_ok=True)
            logging.info(f"Zipping {source_directory} to {output_zip_path}")
            
            with zipfile.ZipFile(output_zip_path, 'w', zipfile.ZIP_DEFLATED) as zipf:
                for root, _, files in os.walk(source_directory):
                    for file in files:
                        file_path = os.path.join(root, file)
                        zipf.write(file_path, os.path.relpath(file_path, source_directory))
            
            logging.info("Zipping completed successfully.")
        except Exception as e:
            logging.error(f"Zip 압축 중 오류: {str(e)}")
            raise FileProcessingError(f"Zip 압축 중 오류: {str(e)}")

    async def cleanup_all_data(self) -> None:
        """사용자 데이터 전체 삭제 (파일 + Neo4j)"""
        connection = Neo4jConnection()
        
        try:
            # 파일 삭제
            user_dirs = [
                os.path.join(BASE_DIR, 'data', self.user_id),
                os.path.join(BASE_DIR, 'target', 'java', self.user_id)
            ]
            
            for dir_path in user_dirs:
                if os.path.exists(dir_path):
                    shutil.rmtree(dir_path)
                    os.makedirs(dir_path)
                    logging.info(f"디렉토리 재생성 완료: {dir_path}")
            
            # Neo4j 데이터 삭제
            await connection.execute_queries([f"MATCH (n {{user_id: '{self.user_id}'}}) DETACH DELETE n"])
            logging.info(f"Neo4J 데이터 초기화 완료 - User ID: {self.user_id}")
        
        except Exception as e:
            logging.error(f"데이터 삭제 중 오류: {str(e)}")
            raise FileProcessingError(f"데이터 삭제 중 오류: {str(e)}")
        finally:
            await connection.close()