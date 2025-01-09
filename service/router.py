import logging
import os
from fastapi import APIRouter, HTTPException, Request
from fastapi.responses import FileResponse, StreamingResponse
from service.service import delete_all_temp_data, get_node_info_from_neo4j, process_comparison_result, process_project_zipping
from service.service import generate_and_execute_cypherQuery
from service.service import generate_two_depth_match
from service.service import generate_simple_java_code
from service.service import generate_spring_boot_project


router = APIRouter()


# 역할: 전달받은 파일들을 분석하여 Neo4j 사이퍼 쿼리를 생성하고 실행합니다
#
# 매개변수:
#   - request: 분석할 파일 정보가 담긴 요청 객체 (fileInfos: [{fileName, objectName}, ...])
#
# 반환값: 
#   - StreamingResponse: Neo4j 그래프 데이터 스트림
@router.post("/cypherQuery/")
async def understand_data(request: Request):    
    try:
        user_id = request.headers.get('User-ID')
        if not user_id:
            raise HTTPException(status_code=400, detail="사용자 ID가 없습니다.")

        file_data = await request.json()
        if not file_data:
            raise HTTPException(status_code=400, detail="파일 정보가 없습니다.")

        file_names = [(item['fileName'], item['objectName']) for item in file_data['fileInfos']]
        logging.info("User ID: %s, File Infos: %s", user_id, file_names)
        
        return StreamingResponse(generate_and_execute_cypherQuery(file_names, user_id))
    
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Understanding에 실패했습니다: {str(e)}")




# 역할: 선택된 테이블 노드를 중심으로 2단계 깊이까지의 연관 노드들을 조회하여 자바 코드로 변환합니다
#
# 매개변수: 
#   - request: 선택된 테이블 노드 정보가 담긴 요청 객체
#
# 반환값: 
#   - StreamingResponse: 생성된 자바 코드 스트림
# TODO FRONT에서 값이 전달이 제대로 안되고 있습니다.
@router.post("/java/")
async def convert_simple_java(request: Request):

    try:
        node_info = await request.json()
        logging.info("Received Node Info for Java: %s", node_info)  
        cypher_query_for_java = await generate_two_depth_match(node_info)
        return StreamingResponse(generate_simple_java_code(cypher_query_for_java))

    except Exception as e:
        raise HTTPException(status_code=500, detail=f"테이블 노드 기준으로 자바 코드 생성에 실패했습니다: {str(e)}")

    


# 역할: 사용자의 요구사항과 이전 대화 내역을 기반으로 자바 코드를 생성합니다
# 매개변수: 
#   - request: 채팅 정보가 담긴 요청 객체 (userInput: 사용자 입력, prevHistory: 이전 대화 내역)
# 반환값: 
#   - StreamingResponse: 생성된 자바 코드 스트림
# TODO FRONT에서 값이 전달이 안되고 있습니다.
@router.post("/chat/")
async def receive_chat(request: Request):

    try:
        chat_info = await request.json()
        logging.info("Received chat Info for Java:", chat_info)

        userInput = chat_info['userInput']
        prevHistory = chat_info['prevHistory']

    except Exception as e:
        raise HTTPException(status_code=500, detail=f"자바 코드 생성을 위해 전달된 데이터가 잘못되었습니다: {str(e)}")

    return StreamingResponse(generate_simple_java_code(None, prevHistory, userInput))



# 역할: 스토어드 프로시저를 스프링 부트 프로젝트로 변환합니다
# 매개변수: 
#   - request: 변환할 파일 정보가 담긴 요청 객체 (fileInfos: [{fileName, objectName}, ...])
# 반환값: 
#   - StreamingResponse: 변환 진행 상태 메시지 스트림
@router.post("/springBoot/")
async def covnert_spring_project(request: Request):

    try:

        file_data = await request.json()
        logging.info("Received File Info for Convert Spring Boot: %s", file_data)
        file_names = [(item['fileName'], item['objectName']) for item in file_data['fileInfos']]
        orm_type = file_data.get('selectedORM', 'jpa').lower()

        if not file_names:
            raise HTTPException(status_code=400, detail="파일 정보가 없습니다.")

    except Exception as e:
        raise HTTPException(status_code=500, detail=f"스프링 부트 프로젝트 생성을 위해 전달된 데이터가 잘못되었습니다: {str(e)}")

    return StreamingResponse(generate_spring_boot_project(file_names, orm_type), media_type="text/plain")


 
# 역할: 생성된 스프링 부트 프로젝트를 ZIP 파일로 압축하여 다운로드를 제공합니다
# 매개변수: 없음
# 반환값: 
#   - FileResponse: 압축된 프로젝트 파일
@router.post("/downloadJava/")
async def download_spring_project():
    try:
        # * 환경에 따라 저장 경로 설정
        if os.getenv('DOCKER_COMPOSE_CONTEXT'):
            base_dir = os.getenv('DOCKER_COMPOSE_CONTEXT')
            target_path = os.path.join(base_dir, 'target')
            zipfile_dir = os.path.join(base_dir, 'data', 'zipfile')
        else:
            parent_dir = os.path.dirname(os.getcwd())
            target_path = os.path.join(parent_dir, 'target')
            zipfile_dir = os.path.join(parent_dir, 'data', 'zipfile')
        
        # * 디렉토리 존재 여부 확인 및 생성
        if not os.path.exists(zipfile_dir):
            os.makedirs(zipfile_dir)

        # * 압축 파일 경로
        output_zip_path = os.path.join(zipfile_dir, 'project.zip')
        
        # * 프로젝트 압축
        await process_project_zipping(target_path, output_zip_path)
        
        return FileResponse(
            path=output_zip_path, 
            filename="project.zip", 
            media_type='application/octet-stream'
        )
    
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"스프링 부트 프로젝트를 Zip 파일로 압축하는데 실패했습니다: {str(e)}")
    


# 역할: 생성된 모든 임시 파일과 디렉토리를 정리합니다
# 매개변수: 없음
# 반환값: 
#   - dict: 삭제 완료 메시지가 포함된 딕셔너리
@router.delete("/deleteAll/")
async def delete_all_data():
    try:
        docker_context = os.getenv('DOCKER_COMPOSE_CONTEXT')
        
        # * 환경에 따라 저장 경로 설정
        if docker_context:
            delete_paths = {
                'target_dir': os.path.join(docker_context, 'target'),
                'zip_dir': os.path.join(docker_context, 'data', 'zipfile')
            }
        else:
            parent_dir = os.path.dirname(os.getcwd())
            delete_paths = {
                'target_dir': os.path.join(parent_dir, 'target'),
                'zip_dir': os.path.join(parent_dir, 'data', 'zipfile')
            }

        # * 임시 파일 삭제
        await delete_all_temp_data(delete_paths)
        return {"message": "모든 임시 파일이 삭제되었습니다."}
        
    except Exception as e:
        logging.error(f"파일 삭제 중 오류 발생: {str(e)}")
        raise HTTPException(status_code=500, detail="임시 파일 삭제 중 오류가 발생했습니다.")


# 역할: 비교 결과를 반환하는 엔드포인트
# 매개변수: 
#   - request: 비교할 파일 정보가 담긴 요청 객체 (fileNames: string[])
# 반환값: 비교 결과 데이터
@router.post("/compare/")
async def get_compare_result(request: Request):
    try:
        compare_data = await request.json()
        logging.info("Received Compare Data")
        
        test_cases = compare_data.get('cases', [])
        
        # 테스트 케이스 정보 검증
        if not test_cases:
            raise HTTPException(status_code=400, detail="테스트 케이스 정보가 없습니다.")
            
        return StreamingResponse(process_comparison_result(test_cases))
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"비교 결과를 가져오는데 실패했습니다: {str(e)}")


# 역할: Neo4j에서 모든 노드 정보를 조회하여 반환합니다
# 매개변수: 없음
# 반환값: 노드 정보 데이터
@router.get("/nodeInfo/")
async def get_node_info():
    try:
        # Neo4j에서 모든 노드 정보를 조회하는 로직 구현 필요
        result = await get_node_info_from_neo4j()
        return result
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"노드 정보를 가져오는데 실패했습니다: {str(e)}")