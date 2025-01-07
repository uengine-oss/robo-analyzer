import os
import logging
import aiofiles
from typing import Optional
from util.exception import ReadFileError, SaveFileError

# 역할 : 파일을 비동기적으로 저장하는 유틸리티 함수
#
# 매개변수 :
#   - content : 저장할 파일 내용
#   - filename : 파일명 (확장자 포함)
#   - base_path : 기본 저장 경로 (기본값: 현재 프로젝트 루트)
#   - sub_path : 하위 경로 (선택사항)
#
# 반환값 : 
#   - str : 저장된 파일의 전체 경로
async def save_file(content: str, filename: str, base_path: Optional[str] = None) -> str:

    try:
        # * 디렉토리 생성
        os.makedirs(base_path, exist_ok=True)
            
        # * 파일 전체 경로
        file_path = os.path.join(base_path, filename)
        
        # * 파일 저장
        async with aiofiles.open(file_path, 'w', encoding='utf-8') as file:
            await file.write(content)
            logging.info(f"파일 저장 성공: {file_path}")
            
        return file_path
        
    except Exception as e:
        err_msg = f"파일 저장 중 오류 발생: {str(e)}"
        logging.error(err_msg)
        raise SaveFileError(err_msg)
    

# 역할: 시퀀스 파일을 읽어 시퀀스 목록을 반환하는 함수입니다.
#
# 매개변수:
#   - object_name : 패키지 또는 프로시저 이름
#
# 반환값:
#   - 시퀀스 목록
async def read_sequence_file(object_name: str) -> str:
    try:
        seq_file_name = object_name.replace('TPX_', 'SEQ_')

        # * 환경에 따라 저장 경로  설정
        if os.getenv('DOCKER_COMPOSE_CONTEXT'):
            base_dir = os.getenv('DOCKER_COMPOSE_CONTEXT')
        else:
            base_dir = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
        
        # * 시퀀스 파일 경로
        seq_file_path = os.path.join(base_dir, 'data', 'sequence', f'{seq_file_name}.sql')
        logging.info(f"현재 디렉토리: {base_dir}")
        logging.info(f"시퀀스 파일 경로: {seq_file_path}")
        
        # * 시퀀스 파일 존재 여부 확인
        if os.path.exists(seq_file_path):
            logging.info(f"시퀀스 파일명: {seq_file_name} 시퀀스 파일 읽기 성공")
            async with aiofiles.open(seq_file_path, 'r', encoding='utf-8') as f:
                return await f.read()
            
        return ''
    
    except Exception as e:
        err_msg = f"시퀀스 파일을 읽는 도중 오류가 발생했습니다: {str(e)}"
        logging.error(err_msg)
        raise ReadFileError(err_msg)
    

# 역할 : target 디렉토리 내의 파일을 읽는 유틸리티 함수
#
# 매개변수 :
#   - class_name: 읽을 파일의 클래스 이름 (예: "UserService", "UserEntity")
#   - component_type: 컴포넌트 타입 경로 (예: "service", "entity", "repository")
#
# 반환값 :
#   - str: 파일의 내용
def read_target_file(class_name: str, component_type: str) -> str:

    try:
        # * 환경에 따라 저장 경로 설정
        if os.getenv('DOCKER_COMPOSE_CONTEXT'):
            base_dir = os.getenv('DOCKER_COMPOSE_CONTEXT')
        else:
            base_dir = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
            
        # * 파일 경로
        base_path = 'java/demo/src/main/java/com/example/demo'
        file_path = os.path.join(base_dir, 'target', base_path, component_type, f'{class_name}.java')
        
        # * 파일 읽기
        with open(file_path, 'r', encoding='utf-8') as file:
            return file.read()
        
    except Exception as e:
        err_msg = f"파일 읽기 중 오류 발생: {str(e)}"
        logging.error(err_msg)
        raise ReadFileError(err_msg)