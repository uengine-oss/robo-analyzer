"""분석 오케스트레이터

소스 코드 분석 파이프라인을 관리하는 서비스 레이어.

주요 기능:
- 요청 파싱 및 검증
- 분석 전략 선택 및 실행
- 데이터 정리
"""

import logging
import os
import shutil
from typing import AsyncGenerator

from fastapi import HTTPException, Request

from analyzer.neo4j_client import Neo4jClient
from analyzer.strategy import AnalyzerFactory
from config.settings import settings
from util.exception import FileProcessError
from util.llm_client import get_llm


# =============================================================================
# 상수
# =============================================================================

# 전략별 소스 파일 확장자
STRATEGY_EXTENSIONS = {
    "framework": {".java", ".kt", ".scala"},
    "dbms": {".sql", ".pls", ".pck", ".pkb", ".pks", ".trg", ".fnc", ".prc"},
}
DEFAULT_EXTENSIONS = {".java", ".sql"}


# =============================================================================
# 요청 파싱 헬퍼
# =============================================================================

def extract_api_key(request: Request, missing_status: int = 401) -> str:
    """API 키 추출 (헤더 → 환경 변수 순서로 폴백)"""
    # 1. 헤더에서 API 키 추출
    api_key = (
        request.headers.get("OpenAI-Api-Key") or
        request.headers.get("Anthropic-Api-Key")
    )
    
    # 2. 헤더에 없으면 환경 변수에서 폴백
    if not api_key:
        api_key = settings.llm.api_key
    
    if not api_key:
        raise HTTPException(missing_status, "요청 헤더 누락: OpenAI-Api-Key 또는 Anthropic-Api-Key (환경변수 LLM_API_KEY도 설정되지 않음)")
    return api_key


def extract_locale(request: Request) -> str:
    """Accept-Language 헤더에서 로케일 추출"""
    return request.headers.get("Accept-Language", "ko")


async def create_orchestrator(
    request: Request,
    body: dict,
    api_key_missing_status: int = 401,
) -> "AnalysisOrchestrator":
    """요청에서 AnalysisOrchestrator 생성 및 API 키 검증"""
    api_key = extract_api_key(request, api_key_missing_status)
    
    orchestrator = AnalysisOrchestrator(
        api_key=api_key,
        locale=extract_locale(request),
        strategy=(body.get("strategy") or "framework").strip().lower(),
        target=(body.get("target") or "java").strip().lower(),
        name_case=(body.get("nameCase") or "original").strip().lower(),
    )
    await orchestrator.validate_api_key()
    return orchestrator


# =============================================================================
# 분석 오케스트레이터
# =============================================================================

class AnalysisOrchestrator:
    """소스 코드 분석 프로세스를 관리하는 오케스트레이터
    
    Attributes:
        api_key: LLM API 키
        locale: 출력 언어 (ko, en)
        strategy: 분석 전략 (framework, dbms)
        target: 타겟 언어 (java, oracle 등)
        name_case: 메타데이터 대소문자 변환 옵션 (original, uppercase, lowercase)
    """

    __slots__ = (
        "api_key", "locale", "strategy", "target", "name_case", "dirs"
    )

    def __init__(
        self,
        api_key: str,
        locale: str,
        strategy: str = "framework",
        target: str = "java",
        name_case: str = "original",
    ):
        self.api_key = api_key
        self.locale = locale
        self.strategy = (strategy or "framework").lower()
        self.target = (target or "java").lower()
        self.name_case = (name_case or "original").lower()  # original, uppercase, lowercase
        
        # 디렉토리 경로 초기화 - 단순하게 data/ 직접 하위에서 찾음
        self.dirs = {
            "ddl": os.path.join(settings.path.data_dir, "ddl"),
            "src": os.path.join(settings.path.data_dir, "source"),
            "analysis": os.path.join(settings.path.data_dir, "analysis"),
        }

    # -------------------------------------------------------------------------
    # 디렉토리 경로
    # -------------------------------------------------------------------------

    def get_directory_dirs(self, directory: str) -> dict[str, str]:
        """디렉토리별 source/analysis 경로 반환"""
        return {
            "src": os.path.join(self.dirs["src"], directory),
            "analysis": os.path.join(self.dirs["analysis"], directory),
        }

    def has_ddl_files(self) -> bool:
        """DDL 디렉토리에 파일이 있는지 확인"""
        ddl_dir = self.dirs.get("ddl", "")
        if not ddl_dir or not os.path.isdir(ddl_dir):
            return False
        
        # DDL 디렉토리에 파일이 있는지 확인
        for _, _, files in os.walk(ddl_dir):
            if files:
                return True
        return False

    # -------------------------------------------------------------------------
    # 파일 탐색
    # -------------------------------------------------------------------------

    def discover_source_files(self) -> list[tuple[str, str]]:
        """source/ 디렉토리에서 분석 가능한 소스 파일 목록 탐색
        
        analysis/ 디렉토리에 해당 JSON 파일이 있는 소스 파일만 반환.
        DDL만 있는 경우(source 디렉토리가 없는 경우) 빈 리스트 반환.
        
        Returns:
            [(directory, file_name), ...] 형태의 튜플 리스트
        """
        source_dir = self.dirs.get("src", "")
        analysis_dir = self.dirs.get("analysis", "")
        
        # DDL만 있는 경우 source/analysis 디렉토리가 없을 수 있음 - 빈 리스트 반환
        if not source_dir or not os.path.isdir(source_dir):
            logging.info("source 디렉토리가 없습니다 (DDL만 있는 경우일 수 있음): %s", source_dir)
            return []
        
        if not analysis_dir or not os.path.isdir(analysis_dir):
            logging.info("analysis 디렉토리가 없습니다 (DDL만 있는 경우일 수 있음): %s", analysis_dir)
            return []
        
        extensions = STRATEGY_EXTENSIONS.get(self.strategy, DEFAULT_EXTENSIONS)
        result = []
        
        for root, _, files in os.walk(source_dir):
            rel_dir = os.path.relpath(root, source_dir)
            directory = "" if rel_dir == "." else rel_dir
            
            for file_name in files:
                ext = os.path.splitext(file_name)[1].lower()
                if ext not in extensions:
                    continue
                
                # analysis JSON 파일 존재 확인
                base_name = os.path.splitext(file_name)[0]
                analysis_path = (
                    os.path.join(analysis_dir, directory, f"{base_name}.json")
                    if directory
                    else os.path.join(analysis_dir, f"{base_name}.json")
                )
                
                if os.path.isfile(analysis_path):
                    result.append((directory, file_name))
        
        result.sort()
        logging.info("분석 가능 소스 파일: %d개", len(result))
        return result

    # -------------------------------------------------------------------------
    # API 키 검증
    # -------------------------------------------------------------------------

    async def validate_api_key(self) -> None:
        """API 키 유효성 검증"""
        try:
            llm = get_llm(api_key=self.api_key)
            if not llm.invoke("ping"):
                raise HTTPException(401, "API 키 검증 실패: ping 실패")
        except HTTPException:
            raise
        except Exception as e:
            logging.error("API 키 검증 실패: %s", e)
            raise HTTPException(401, f"API 키 검증 실패: {type(e).__name__}: {e}")

    # -------------------------------------------------------------------------
    # 분석 프로세스
    # -------------------------------------------------------------------------

    async def run_analysis(
        self,
        file_names: list[tuple[str, str]],
    ) -> AsyncGenerator[bytes, None]:
        """소스 파일 분석 → Neo4j 그래프 이벤트 스트리밍
        
        Args:
            file_names: [(directory, file_name), ...] 튜플 리스트
        """
        analyzer = AnalyzerFactory.create(self.strategy)
        async for chunk in analyzer.analyze(file_names=file_names, orchestrator=self):
            yield chunk

    # -------------------------------------------------------------------------
    # 데이터 정리
    # -------------------------------------------------------------------------

    async def cleanup_neo4j_data(self) -> None:
        """Neo4j 그래프 데이터 삭제 (파일 시스템 유지)"""
        client = Neo4jClient()
        
        try:
            await client.execute_queries([
                "MATCH (__cy_n__) DETACH DELETE __cy_n__"
            ])
            logging.info("Neo4j 데이터 삭제 완료")
        
        except Exception as e:
            logging.error("Neo4j 데이터 삭제 오류: %s", e)
            raise FileProcessError(f"Neo4j 데이터 삭제 오류: {e}", cause=e)
        finally:
            await client.close()

    async def cleanup_all_data(self, include_files: bool = True) -> None:
        """데이터 전체 삭제
        
        Args:
            include_files: True면 파일 시스템도 함께 삭제, False면 Neo4j만 삭제
        """
        client = Neo4jClient()
        
        try:
            # 파일 시스템 정리 (옵션)
            if include_files:
                dir_path = settings.path.data_dir
                if os.path.exists(dir_path):
                    shutil.rmtree(dir_path)
                    os.makedirs(dir_path)
                    logging.info("디렉토리 초기화: %s", dir_path)
            
            # Neo4j 데이터 삭제
            await client.execute_queries([
                "MATCH (__cy_n__) DETACH DELETE __cy_n__"
            ])
            logging.info("Neo4j 데이터 삭제 완료")
        
        except Exception as e:
            logging.error("데이터 삭제 오류: %s", e)
            raise FileProcessError(f"데이터 삭제 오류: {e}", cause=e)
        finally:
            await client.close()

