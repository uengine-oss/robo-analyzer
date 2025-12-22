"""Understanding/Converting 파이프라인을 오케스트레이션하는 서비스 레이어."""

import logging
import os
import shutil
import zipfile
from typing import AsyncGenerator, List, Optional

from fastapi import HTTPException, Request

from understand.neo4j_connection import Neo4jConnection
from understand.strategy.strategy_factory import UnderstandStrategyFactory
from util.exception import FileProcessingError
from util.llm_client import get_llm


# =============================================================================
# 상수
# =============================================================================

BASE_DIR = os.getenv('DOCKER_COMPOSE_CONTEXT') or os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
TEST_SESSIONS = frozenset({"EN_TestSession", "KO_TestSession"})

# 전략별 소스 파일 확장자
STRATEGY_EXTENSIONS = {
    'framework': {'.java', '.kt', '.scala'},
    'dbms': {'.sql', '.pls', '.pck', '.pkb', '.pks', '.trg', '.fnc', '.prc'},
}
DEFAULT_EXTENSIONS = {'.java', '.sql'}


# =============================================================================
# 요청 파싱 헬퍼
# =============================================================================

def get_user_id(request: Request) -> str:
    """Session-UUID 헤더에서 사용자 ID 추출"""
    user_id = request.headers.get('Session-UUID')
    if not user_id:
        raise HTTPException(400, "요청 헤더 누락: Session-UUID")
    return user_id


def get_api_key(request: Request, user_id: str, missing_status: int = 401) -> str:
    """API 키 추출 (테스트 세션은 환경 변수 사용)"""
    if user_id in TEST_SESSIONS:
        api_key = os.getenv("LLM_API_KEY") or os.getenv("API_KEY")
        if not api_key:
            raise HTTPException(missing_status, "환경 변수에 API 키가 설정되어 있지 않습니다.")
        return api_key

    api_key = request.headers.get('OpenAI-Api-Key') or request.headers.get('Anthropic-Api-Key')
    if not api_key:
        raise HTTPException(401, "요청 헤더 누락: OpenAI-Api-Key 또는 Anthropic-Api-Key")
    return api_key


def get_locale(request: Request) -> str:
    """Accept-Language 헤더에서 로케일 추출"""
    return request.headers.get('Accept-Language', 'ko')


def parse_class_names(raw_list: list[str]) -> list[tuple[str, str]]:
    """classNames 문자열 리스트를 (directory, className) 튜플로 변환
    
    예: ["game/Player", "game/Enemy"] → [("game", "Player"), ("game", "Enemy")]
    """
    result = []
    for item in raw_list:
        if '/' not in item:
            raise HTTPException(400, f"잘못된 classNames 형식: '{item}'. 'directory/className' 형식이어야 합니다.")
        directory, class_name = item.split('/', 1)
        if not directory or not class_name:
            raise HTTPException(400, f"잘못된 classNames 형식: '{item}'. directory와 className이 모두 필요합니다.")
        result.append((directory.strip(), class_name.strip()))
    return result


async def create_orchestrator(request: Request, body: dict, api_key_missing_status: int = 401) -> 'ServiceOrchestrator':
    """요청에서 ServiceOrchestrator 생성 및 API 키 검증"""
    user_id = get_user_id(request)
    api_key = get_api_key(request, user_id, api_key_missing_status)
    
    project_name = body.get('projectName')
    if not project_name:
        raise HTTPException(400, "projectName이 없습니다.")
    
    orchestrator = ServiceOrchestrator(
        user_id=user_id,
        api_key=api_key,
        locale=get_locale(request),
        project_name=project_name,
        strategy=(body.get('strategy') or 'framework').strip().lower(),
        target=(body.get('target') or 'java').strip().lower(),
    )
    await orchestrator.validate_api_key()
    return orchestrator


# =============================================================================
# 서비스 오케스트레이터
# =============================================================================

class ServiceOrchestrator:
    """Understanding과 Converting 전체 프로세스를 관리하는 오케스트레이터"""

    __slots__ = ('user_id', 'api_key', 'locale', 'project_name', 'strategy', 
                 'target', 'project_name_cap', '_user_base', 'dirs')

    def __init__(
        self,
        user_id: str,
        api_key: str,
        locale: str,
        project_name: str,
        strategy: str = 'framework',
        target: str = 'java',
    ):
        self.user_id = user_id
        self.api_key = api_key
        self.locale = locale
        self.project_name = project_name
        self.strategy = (strategy or 'framework').lower()
        self.target = (target or 'java').lower()
        self.project_name_cap = project_name.capitalize() if project_name else ''
        
        # 디렉토리 경로 초기화
        self._user_base = ''
        self.dirs = {}
        if project_name:
            self._user_base = os.path.join(BASE_DIR, 'data', user_id, project_name)
            self.dirs = {
                'ddl': os.path.join(self._user_base, 'ddl'),
                'src': os.path.join(self._user_base, 'source'),
                'analysis': os.path.join(self._user_base, 'analysis'),
            }

    # -------------------------------------------------------------------------
    # 디렉토리 경로
    # -------------------------------------------------------------------------

    def get_directory_dirs(self, directory: str) -> dict[str, str]:
        """디렉토리별 source/analysis 경로 반환"""
        return {
            'src': os.path.join(self.dirs['src'], directory),
            'analysis': os.path.join(self.dirs['analysis'], directory),
        }

    # -------------------------------------------------------------------------
    # 파일 탐색
    # -------------------------------------------------------------------------

    def discover_source_files(self) -> list[tuple[str, str]]:
        """source/ 디렉토리에서 분석 가능한 소스 파일 목록을 자동 탐색
        
        analysis/ 디렉토리에 해당 JSON 파일이 있는 소스 파일만 반환.
        
        Returns:
            [(directory, file_name), ...] 형태의 튜플 리스트
        """
        source_dir = self.dirs.get('src', '')
        analysis_dir = self.dirs.get('analysis', '')
        
        if not source_dir or not os.path.isdir(source_dir):
            logging.warning(f"source 디렉토리 없음: {source_dir}")
            return []
        
        if not analysis_dir or not os.path.isdir(analysis_dir):
            logging.warning(f"analysis 디렉토리 없음: {analysis_dir}")
            return []
        
        extensions = STRATEGY_EXTENSIONS.get(self.strategy, DEFAULT_EXTENSIONS)
        result = []
        
        for root, _, files in os.walk(source_dir):
            rel_dir = os.path.relpath(root, source_dir)
            directory = '' if rel_dir == '.' else rel_dir
            
            for file_name in files:
                ext = os.path.splitext(file_name)[1].lower()
                if ext not in extensions:
                    continue
                
                # analysis JSON 파일 존재 확인
                base_name = os.path.splitext(file_name)[0]
                analysis_path = os.path.join(analysis_dir, directory, f"{base_name}.json") if directory else os.path.join(analysis_dir, f"{base_name}.json")
                
                if os.path.isfile(analysis_path):
                    result.append((directory, file_name))
        
        result.sort()
        logging.info(f"분석 가능한 소스 파일: {len(result)}개")
        return result

    # -------------------------------------------------------------------------
    # API 키 검증
    # -------------------------------------------------------------------------

    async def validate_api_key(self) -> None:
        """API 키 유효성 검증 (테스트 세션은 스킵)"""
        if self.user_id in TEST_SESSIONS:
            return
        
        try:
            llm = get_llm(api_key=self.api_key)
            if not llm.invoke("ping"):
                raise HTTPException(401, "API 키 검증 실패: ping 실패")
        except HTTPException:
            raise
        except Exception as e:
            logging.error(f"API 키 검증 실패: {e}")
            raise HTTPException(401, f"API 키 검증 실패: {type(e).__name__}: {e}")

    # -------------------------------------------------------------------------
    # Understanding 프로세스
    # -------------------------------------------------------------------------

    async def understand_project(self, file_names: list[tuple[str, str]]) -> AsyncGenerator[bytes, None]:
        """소스 파일 분석 → Neo4j 그래프 이벤트 스트리밍
        
        Args:
            file_names: [(directory, file_name), ...] 튜플 리스트
        """
        strategy = UnderstandStrategyFactory.create_strategy(self.strategy)
        async for chunk in strategy.understand(file_names=file_names, orchestrator=self):
            yield chunk

    # -------------------------------------------------------------------------
    # Converting 프로세스
    # -------------------------------------------------------------------------

    async def convert_project(
        self,
        file_names: list[tuple[str, str]],
        directories: Optional[List[str]] = None
    ) -> AsyncGenerator[bytes, None]:
        """전략에 따른 코드 변환 실행
        
        Args:
            file_names: [(directory, file_name), ...] 튜플 리스트
            directories: ["dir/file.java", ...] (architecture 전략용, 파일 경로 리스트)
        """
        from convert.strategies.strategy_factory import StrategyFactory
        
        logging.info(f"Convert: strategy={self.strategy}, target={self.target}, "
                     f"files={len(file_names)}, directories={len(directories or [])}")

        strategy = StrategyFactory.create_strategy(self.strategy, target=self.target)
        async for chunk in strategy.convert(file_names, orchestrator=self, directories=directories):
            yield chunk

    # -------------------------------------------------------------------------
    # 파일 작업
    # -------------------------------------------------------------------------

    async def zip_project(self, source_dir: str, output_path: str) -> None:
        """프로젝트 디렉토리를 ZIP으로 압축"""
        try:
            os.makedirs(os.path.dirname(output_path), exist_ok=True)
            logging.info(f"ZIP 생성: {source_dir} → {output_path}")
            
            with zipfile.ZipFile(output_path, 'w', zipfile.ZIP_DEFLATED) as zf:
                for root, _, files in os.walk(source_dir):
                    for file in files:
                        file_path = os.path.join(root, file)
                        zf.write(file_path, os.path.relpath(file_path, source_dir))
            
            logging.info("ZIP 생성 완료")
        except Exception as e:
            logging.error(f"ZIP 압축 오류: {e}")
            raise FileProcessingError(f"ZIP 압축 오류: {e}")

    async def cleanup_all_data(self) -> None:
        """사용자 데이터 전체 삭제 (파일 + Neo4j)"""
        connection = Neo4jConnection()
        
        try:
            # 파일 시스템 정리
            for subdir in ['data', os.path.join('target', 'java')]:
                dir_path = os.path.join(BASE_DIR, subdir, self.user_id)
                if os.path.exists(dir_path):
                    shutil.rmtree(dir_path)
                    os.makedirs(dir_path)
                    logging.info(f"디렉토리 초기화: {dir_path}")
            
            # Neo4j 데이터 삭제
            await connection.execute_queries([
                f"MATCH (n {{user_id: '{self.user_id}'}}) DETACH DELETE n"
            ])
            logging.info(f"Neo4j 데이터 삭제 완료: {self.user_id}")
        
        except Exception as e:
            logging.error(f"데이터 삭제 오류: {e}")
            raise FileProcessingError(f"데이터 삭제 오류: {e}")
        finally:
            await connection.close()
