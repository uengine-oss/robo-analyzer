import logging
from fastapi import APIRouter, HTTPException, Request
from fastapi.responses import StreamingResponse

# 서비스 모듈에서 필요한 함수들을 임포트합니다.
from internal.service import (
    delete_all_temp_data, 
    generate_cypher_query_and_stream_results,
    process_ddl_and_sequence_files,
)
# 선택된 DBMS에 따른 이해 전략 생성 함수
from understand.understading_strategy import create_understanding_strategy

# API 라우터와 로거 객체를 생성합니다.
router = APIRouter()
logger = logging.getLogger(__name__)


@router.post("/understanding/")
async def understanding_spCode(request: Request) -> StreamingResponse:
    """
    SP 코드 분석 및 Neo4j 그래프 데이터 생성을 위한 API 엔드포인트

    요청 데이터 예시:
    {
      "uploadedFileInfos": [{"fileName": "example.ddl", "objectName": "example_object"}, ...],
      "selectedDbms": "Oracle"
    }

    헤더:
      - Session-UUID: 사용자 세션 ID
      - Anthropic-Api-Key: Anthropic API 키

    반환:
      - Neo4j 사이퍼 쿼리 결과 스트림을 포함하는 StreamingResponse
    """
    try:
        # 요청 헤더 및 본문에서 필수 정보를 추출합니다.
        user_id = request.headers.get("Session-UUID")
        request_data = await request.json()
        file_infos = request_data.get("uploadedFileInfos", [])
        selected_dbms = request_data.get("selectedDbms")
        api_key = request.headers.get("Anthropic-Api-Key")

        logger.info(
            "그래프 생성 요청 - 사용자: %s, 파일 수: %d, 선택된 DBMS: %s",
            user_id, len(file_infos), selected_dbms or "기본값"
        )

        # 선택된 DBMS에 따른 이해 전략을 생성합니다.
        strategy = create_understanding_strategy(selected_dbms)
        logger.debug("생성된 이해 전략: %s", strategy)

        # DDL 및 시퀀스 파일 처리를 수행합니다.
        await process_ddl_and_sequence_files(file_infos, user_id, strategy, api_key)
        logger.info("DDL 및 시퀀스 파일 처리 완료 - 사용자: %s", user_id)

        # 사이퍼 쿼리 스트림 생성 후 StreamingResponse로 반환합니다.
        cypher_stream = generate_cypher_query_and_stream_results(file_infos, user_id, strategy, api_key)
        logger.info("Neo4j 사이퍼 쿼리 스트림 생성 시작 - 사용자: %s", user_id)
        return StreamingResponse(cypher_stream)

    except Exception as ex:
        error_message = f"그래프 데이터 생성 중 오류 발생: {ex}"
        logger.exception(error_message)
        raise HTTPException(status_code=500, detail=error_message)


@router.delete("/deleteAll/")
async def delete_temp_project_data(request: Request) -> dict:
    """
    역할:
      생성된 임시 파일 및 디렉토리를 삭제하는 API 엔드포인트.
    
    매개변수:
      - request: 클라이언트 요청 객체 (FastAPI Request)
          * 헤더에 'Session-UUID'가 포함되어 있어야 하며, 이 값은 사용자 식별에 사용됩니다.
    
    반환:
      - dict: 임시 데이터 삭제 완료 메시지를 담은 딕셔너리
              (예: {"message": "모든 임시 파일이 삭제되었습니다."})
    """
    try:
        # 1. 사용자 식별: 헤더에서 Session-UUID 추출 (클라이언트 식별)
        user_id = request.headers.get("Session-UUID")
        if not user_id:
            logger.error("삭제 요청: 사용자 ID가 헤더에 존재하지 않음")
            raise HTTPException(status_code=400, detail="사용자 ID가 없습니다.")

        logger.info("임시 데이터 삭제 요청 수신 - 사용자: %s", user_id)

        # 2. 임시 데이터 삭제: 비동기 함수를 호출하여 사용자의 모든 임시 데이터를 삭제함
        await delete_all_temp_data(user_id)
        logger.info("임시 데이터 삭제 완료 - 사용자: %s", user_id)

        # 3. 성공 메시지 반환: 삭제 완료 후 클라이언트에 메시지 전달
        return {"message": "모든 임시 파일이 삭제되었습니다."}

    except Exception as ex:
        # 4. 예외 처리: 오류 발생 시, 예외 메시지를 로깅하고 HTTP 500 예외를 발생시킴
        error_message = f"임시 파일 삭제 중 오류 발생: {ex}"
        logger.exception(error_message)
        raise HTTPException(status_code=500, detail=error_message)
