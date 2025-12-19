"""
DBMS 변환 전략 (PostgreSQL → Oracle 등)
"""

import logging
from typing import AsyncGenerator, Any

from .base_strategy import ConversionStrategy
from convert.dbms.create_dbms_conversion import start_dbms_conversion_steps
from util.utility_tool import emit_message, emit_data, emit_error, emit_status, get_procedures_from_file


logger = logging.getLogger(__name__)


# 단계 정의
STEP_SKELETON = 1  # 스켈레톤 생성
STEP_BODY = 2      # 본문 변환


class DbmsConversionStrategy(ConversionStrategy):
    """DBMS 간 변환 전략 (PostgreSQL → Oracle 등)"""
    
    def __init__(self, target: str):
        self.target = target.lower()
    
    async def convert(self, file_names: list, orchestrator: Any, **kwargs) -> AsyncGenerator[bytes, None]:
        """
        DBMS 간 변환을 수행합니다.
        
        Args:
            file_names: 변환할 파일 목록
            orchestrator: ServiceOrchestrator 인스턴스
            **kwargs: 추가 매개변수
        """
        try:
            user_id = orchestrator.user_id
            project_name = orchestrator.project_name
            api_key = orchestrator.api_key
            locale = orchestrator.locale
            
            total_files = len(file_names)
            target_name = self.target.upper()

            yield emit_message(f"DBMS 변환을 시작합니다 (대상: {target_name})")
            yield emit_message(f"프로젝트 '{project_name}'의 {total_files}개 파일을 변환합니다")

            total_procedures_converted = 0

            for file_idx, (directory, file_name) in enumerate(file_names, 1):
                try:
                    yield emit_message(f"파일 변환 시작: {file_name} ({file_idx}/{total_files})")
                    yield emit_message(f"경로: {directory}")
                    
                    yield emit_message("프로시저 정보를 조회하고 있습니다")
                    procedure_names = await get_procedures_from_file(
                        directory=directory,
                        file_name=file_name,
                        user_id=user_id,
                        project_name=project_name
                    )
                    
                    if not procedure_names:
                        procedure_names = [file_name.rsplit(".", 1)[0]]
                        logger.warning(f"Neo4j에서 프로시저를 찾지 못함, 파일명 기반 사용: {procedure_names[0]}")
                        yield emit_message("프로시저 정보가 없어 파일명 기반으로 처리합니다")
                    
                    proc_count = len(procedure_names)
                    yield emit_message(f"발견된 프로시저: {proc_count}개")
                    
                    for proc_idx, procedure_name in enumerate(procedure_names, 1):
                        yield emit_message(f"프로시저 변환 시작: {procedure_name} ({proc_idx}/{proc_count})")
                        
                        yield emit_message("프로시저 구조를 분석하고 있습니다")
                        yield emit_status(STEP_SKELETON, done=False)
                        
                        result = await start_dbms_conversion_steps(
                            directory=directory,
                            file_name=file_name,
                            procedure_name=procedure_name,
                            project_name=project_name,
                            user_id=user_id,
                            api_key=api_key,
                            locale=locale,
                            target=self.target
                        )
                        
                        yield emit_status(STEP_SKELETON, done=True)
                        yield emit_message("프로시저 구조 분석이 완료되었습니다")
                        
                        yield emit_message(f"{target_name} 코드로 변환하고 있습니다")
                        yield emit_status(STEP_BODY, done=True)
                        yield emit_message("코드 변환이 완료되었습니다")
                        
                        yield emit_data(
                            file_type="converted_sp",
                            file_name=file_name,
                            directory=directory,
                            procedure_name=procedure_name,
                            code=result["converted_code"],
                            summary=f"{target_name}로 변환됨",
                        )
                        
                        total_procedures_converted += 1
                        yield emit_message(f"프로시저 변환 완료: {procedure_name} ({proc_idx}/{proc_count})")
                    
                    yield emit_message(f"파일 변환 완료: {file_name} ({file_idx}/{total_files}, 프로시저 {proc_count}개)")
                    
                except Exception as file_error:
                    logger.error(f"Conversion failed for {directory}/{file_name}: {str(file_error)}")
                    yield emit_message(f"변환 중 오류가 발생했습니다: {str(file_error)}")
                    yield emit_error(f"{directory}/{file_name} 변환 실패: {str(file_error)}")
                    return

            yield emit_message(f"DBMS 변환이 모두 완료되었습니다 (대상: {target_name})")
            yield emit_message(f"결과: {total_files}개 파일, {total_procedures_converted}개 프로시저 변환")
            
        except Exception as e:
            logger.error(f"Conversion error: {str(e)}")
            yield emit_error(f"변환 오류: {str(e)}")
