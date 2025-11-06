import logging
from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware
import uvicorn
from service.router import router  # service.router.py 파일에서 정의한 라우터 가져오기
from util.llm_audit import reset_audit_log

# API 엔드포인트를 정의하고 요청을 처리하기 위해 FastAPI 애플리케이션을 생성
reset_audit_log()
app = FastAPI()

# CORS 미들웨어 추가: 다른 도메인에서의 요청을 허용하기 위한 설정
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],  # 모든 도메인에서의 요청을 허용
    allow_credentials=True,
    allow_methods=["*"],  # 모든 HTTP 메소드를 허용
    allow_headers=["*"],  # 모든 헤더를 허용
)

# 라우터를 FastAPI 애플리케이션 인스턴스에 등록
app.include_router(router)


# 로깅 설정
logging.basicConfig(
    level=logging.INFO,
    format='%(levelname)s: %(message)s',
    force=True  # 기존 로깅 설정을 덮어쓰기
)


# 헬스 체크 엔드포인트 추가 (루트 경로)
@app.get("/")
async def health_check():
    return {"status": "ok"}


# 애플리케이션 실행: 개발 시 uvicorn을 사용하여 로컬 서버를 실행
if __name__ == "__main__":
    uvicorn.run(app, host="0.0.0.0", port=5502)
