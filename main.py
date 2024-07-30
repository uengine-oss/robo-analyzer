import logging
from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware
import uvicorn
from service.router import router

# API 엔드포인트를 정의하고 요청을 처리하기 위해 FastAPI 애플리케이션을 생성
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

# 로그 레벨을 info로 설정
logging.basicConfig(level=logging.INFO)

# 애플리케이션 실행: 개발 시 uvicorn을 사용하여 로컬 서버를 실행
if __name__ == "__main__":
    uvicorn.run(app, host="0.0.0.0", port=5502)