"""ROBO Analyzer 환경변수 중앙 관리

모든 환경변수를 한 곳에서 관리하여 일관성과 유지보수성을 높입니다.
"""

import os
from dataclasses import dataclass, field
from functools import lru_cache
from pathlib import Path
from typing import Optional


def _get_base_dir() -> str:
    """프로젝트 루트 디렉토리 경로 반환"""
    return os.getenv("DOCKER_COMPOSE_CONTEXT") or str(
        Path(__file__).resolve().parents[1]
    )


@dataclass(frozen=True)
class Neo4jConfig:
    """Neo4j 연결 설정"""
    uri: str = field(default_factory=lambda: os.getenv("NEO4J_URI", "bolt://127.0.0.1:7687"))
    user: str = field(default_factory=lambda: os.getenv("NEO4J_USER", "neo4j"))
    password: str = field(default_factory=lambda: os.getenv("NEO4J_PASSWORD", "neo4j"))
    database: str = "neo4j"


@dataclass(frozen=True)
class LLMConfig:
    """LLM API 설정"""
    api_base: str = field(default_factory=lambda: os.getenv("LLM_API_BASE", "https://api.openai.com/v1"))
    api_key: str = field(default_factory=lambda: os.getenv("LLM_API_KEY", ""))
    model: str = field(default_factory=lambda: os.getenv("LLM_MODEL", "gpt-4.1"))
    max_tokens: int = field(default_factory=lambda: int(os.getenv("LLM_MAX_TOKENS", "32768")))
    reasoning_effort: str = field(default_factory=lambda: os.getenv("LLM_REASONING_EFFORT", "medium"))
    is_custom: bool = field(default_factory=lambda: bool(os.getenv("IS_CUSTOM_LLM")))
    company_name: Optional[str] = field(default_factory=lambda: os.getenv("COMPANY_NAME"))


@dataclass(frozen=True)
class ConcurrencyConfig:
    """병렬 처리 설정"""
    # 파일 단위 병렬 처리 (새로 추가)
    file_concurrency: int = field(default_factory=lambda: int(os.getenv("FILE_CONCURRENCY", "5")))
    
    # 청크/배치 단위 병렬 처리
    max_concurrency: int = field(default_factory=lambda: int(os.getenv("MAX_CONCURRENCY", "5")))
    
    # Framework 분석 전용
    framework_max_concurrency: int = field(default_factory=lambda: int(os.getenv("FRAMEWORK_MAX_CONCURRENCY", "5")))
    inheritance_concurrency: int = field(default_factory=lambda: int(os.getenv("INHERITANCE_CONCURRENCY", "5")))
    field_concurrency: int = field(default_factory=lambda: int(os.getenv("FIELD_CONCURRENCY", "5")))
    method_concurrency: int = field(default_factory=lambda: int(os.getenv("METHOD_CONCURRENCY", "5")))
    
    # DBMS 분석 전용
    variable_concurrency: int = field(default_factory=lambda: int(os.getenv("VARIABLE_CONCURRENCY", "5")))


@dataclass(frozen=True)
class BatchConfig:
    """배치 처리 설정"""
    max_batch_token: int = field(default_factory=lambda: int(os.getenv("MAX_BATCH_TOKEN", "1000")))
    framework_max_batch_token: int = field(default_factory=lambda: int(os.getenv("FRAMEWORK_MAX_BATCH_TOKEN", "1000")))
    max_summary_chunk_token: int = field(default_factory=lambda: int(os.getenv("MAX_SUMMARY_CHUNK_TOKEN", "5000")))
    static_query_batch_size: int = field(default_factory=lambda: int(os.getenv("STATIC_QUERY_BATCH_SIZE", "40")))


@dataclass(frozen=True)
class PathConfig:
    """경로 설정"""
    base_dir: str = field(default_factory=_get_base_dir)
    audit_dir: str = field(default_factory=lambda: os.getenv("LLM_AUDIT_DIR") or os.path.join(_get_base_dir(), "logs"))
    
    @property
    def data_dir(self) -> str:
        return os.path.join(self.base_dir, "data")
    
    @property
    def prompt_log_dir(self) -> str:
        return os.path.join(self.audit_dir, "llm_prompts")


@dataclass(frozen=True)
class TestConfig:
    """테스트 설정"""
    test_sessions: frozenset = field(default_factory=lambda: frozenset({"EN_TestSession", "KO_TestSession"}))
    test_user_id: str = field(default_factory=lambda: os.getenv("TEST_USER_ID", "EN_TestSession"))
    test_project_name: str = field(default_factory=lambda: os.getenv("TEST_PROJECT_NAME", "test"))


@dataclass(frozen=True)
class AnalyzerConfig:
    """ROBO Analyzer 통합 설정"""
    neo4j: Neo4jConfig = field(default_factory=Neo4jConfig)
    llm: LLMConfig = field(default_factory=LLMConfig)
    concurrency: ConcurrencyConfig = field(default_factory=ConcurrencyConfig)
    batch: BatchConfig = field(default_factory=BatchConfig)
    path: PathConfig = field(default_factory=PathConfig)
    test: TestConfig = field(default_factory=TestConfig)
    
    # API 설정
    api_prefix: str = "/robo"
    
    # 서버 설정
    host: str = field(default_factory=lambda: os.getenv("HOST", "0.0.0.0"))
    port: int = field(default_factory=lambda: int(os.getenv("PORT", "5502")))


@lru_cache(maxsize=1)
def get_settings() -> AnalyzerConfig:
    """싱글톤 설정 인스턴스 반환 (캐싱)"""
    return AnalyzerConfig()


# 전역 설정 인스턴스
settings = get_settings()

