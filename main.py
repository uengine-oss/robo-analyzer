"""ROBO Analyzer 메인 애플리케이션

FastAPI 기반 소스 코드 분석 서버.

시작 방법:
    uvicorn main:app --host 0.0.0.0 --port 5502 --reload
"""

from fastapi import FastAPI, Request
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import JSONResponse

from api.router import router
from api.glossary_router import router as glossary_router
from config.settings import settings
from util.logger import setup_logging, get_logger
from util.exception import (
    RoboAnalyzerError,
    AnalysisError,
    LLMCallError,
    FileProcessError,
    Neo4jError,
    ConfigError,
)


# 로깅 초기화
setup_logging()
logger = get_logger(__name__)


# =============================================================================
# FastAPI 앱 설정
# =============================================================================

app = FastAPI(
    title="ROBO Analyzer",
    description="소스 코드 분석 및 Neo4j 그래프 변환 서비스",
    version="2.0.0",
    docs_url="/docs",
    redoc_url="/redoc",
)

# CORS 미들웨어
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# 라우터 등록
app.include_router(router)
app.include_router(glossary_router)


# =============================================================================
# 예외 핸들러 - 정확한 오류 메시지 전달
# =============================================================================

@app.exception_handler(LLMCallError)
async def llm_call_error_handler(request: Request, exc: LLMCallError):
    """LLM 호출 오류 처리 (OpenAI API 오류 등)"""
    return JSONResponse(
        status_code=503,  # Service Unavailable
        content={
            "detail": exc.message,
            "error_type": "LLMCallError",
            "context": exc.context,
        }
    )


@app.exception_handler(FileProcessError)
async def file_process_error_handler(request: Request, exc: FileProcessError):
    """파일 처리 오류"""
    return JSONResponse(
        status_code=400,  # Bad Request
        content={
            "detail": exc.message,
            "error_type": "FileProcessError",
            "context": exc.context,
        }
    )


@app.exception_handler(Neo4jError)
async def neo4j_error_handler(request: Request, exc: Neo4jError):
    """Neo4j 오류"""
    return JSONResponse(
        status_code=503,  # Service Unavailable
        content={
            "detail": exc.message,
            "error_type": "Neo4jError",
            "context": exc.context,
        }
    )


@app.exception_handler(ConfigError)
async def config_error_handler(request: Request, exc: ConfigError):
    """설정 오류"""
    return JSONResponse(
        status_code=500,  # Internal Server Error
        content={
            "detail": exc.message,
            "error_type": "ConfigError",
            "context": exc.context,
        }
    )


@app.exception_handler(AnalysisError)
async def analysis_error_handler(request: Request, exc: AnalysisError):
    """분석 오류"""
    return JSONResponse(
        status_code=500,
        content={
            "detail": exc.message,
            "error_type": "AnalysisError",
            "context": exc.context,
        }
    )


@app.exception_handler(RoboAnalyzerError)
async def robo_analyzer_error_handler(request: Request, exc: RoboAnalyzerError):
    """ROBO Analyzer 기본 예외 처리"""
    return JSONResponse(
        status_code=500,
        content={
            "detail": exc.message,
            "error_type": exc.__class__.__name__,
            "context": exc.context,
        }
    )


# =============================================================================
# 헬스체크 및 유틸리티 엔드포인트
# =============================================================================

@app.get("/")
async def health_check():
    """헬스체크 엔드포인트"""
    return {"status": "ok", "service": "robo-analyzer", "version": "2.0.0"}


@app.get("/health")
async def health():
    """상세 헬스체크"""
    return {
        "status": "healthy",
        "service": "robo-analyzer",
        "version": "2.0.0",
        "config": {
            "file_concurrency": settings.concurrency.file_concurrency,
            "max_concurrency": settings.concurrency.max_concurrency,
        },
    }


# =============================================================================
# 서버 시작
# =============================================================================

if __name__ == "__main__":
    import uvicorn
    
    logger.info("ROBO Analyzer starting on %s:%d", settings.host, settings.port)
    uvicorn.run(
        "main:app",
        host=settings.host,
        port=settings.port,
        reload=True,
    )
