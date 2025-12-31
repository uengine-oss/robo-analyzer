"""ROBO Analyzer 메인 애플리케이션

FastAPI 기반 소스 코드 분석 서버.

시작 방법:
    uvicorn main:app --host 0.0.0.0 --port 5502 --reload
"""

from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware

from api.router import router
from config.settings import settings
from util.logger import setup_logging, get_logger


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
