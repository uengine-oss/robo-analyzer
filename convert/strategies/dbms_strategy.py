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
            yield emit_message(f"DBMS conversion started → {self.target.upper()}")

            user_id = orchestrator.user_id
            project_name = orchestrator.project_name
            api_key = orchestrator.api_key
            locale = orchestrator.locale
            
            total_files = len(file_names)

            # 파일별 변환
            for file_idx, (system_name, file_name) in enumerate(file_names, 1):
                try:
                    # Neo4j에서 파일의 모든 프로시저 조회
                    procedure_names = await get_procedures_from_file(
                        system_name=system_name,
                        file_name=file_name,
                        user_id=user_id,
                        project_name=project_name
                    )
                    
                    # 프로시저가 없으면 파일명 기반으로 폴백
                    if not procedure_names:
                        procedure_names = [file_name.rsplit(".", 1)[0]]
                        logger.warning(f"Neo4j에서 프로시저를 찾지 못함, 파일명 기반 사용: {procedure_names[0]}")
                    
                    yield emit_message(f"[{file_idx}/{total_files}] Converting {system_name}/{file_name} ({len(procedure_names)} procedure(s))")
                    
                    # 각 프로시저별로 변환 수행
                    for proc_idx, procedure_name in enumerate(procedure_names, 1):
                        
                        # Step 1: 스켈레톤 생성 시작
                        yield emit_message(f"  [{proc_idx}/{len(procedure_names)}] {procedure_name} - Step 1: Skeleton generation")
                        yield emit_status(STEP_SKELETON, done=False)
                        
                        # 단계별 변환 수행
                        result = await start_dbms_conversion_steps(
                            system_name=system_name,
                            file_name=file_name,
                            procedure_name=procedure_name,
                            project_name=project_name,
                            user_id=user_id,
                            api_key=api_key,
                            locale=locale,
                            target=self.target
                        )
                        
                        # Step 1 완료
                        yield emit_status(STEP_SKELETON, done=True)
                        yield emit_message(f"  [{proc_idx}/{len(procedure_names)}] {procedure_name} - Step 1: Skeleton completed")
                        
                        # Step 2 완료 (body 변환은 start_dbms_conversion_steps 내부에서 수행됨)
                        yield emit_status(STEP_BODY, done=True)
                        yield emit_message(f"  [{proc_idx}/{len(procedure_names)}] {procedure_name} - Step 2: Body conversion completed")
                        
                        # 스트리밍으로 결과 전송
                        yield emit_data(
                            file_type="converted_sp",
                            file_name=file_name,
                            system_name=system_name,
                            procedure_name=procedure_name,
                            code=result["converted_code"],
                            summary=f"Converted to {self.target.upper()}",
                        )
                    
                    yield emit_message(f"[{file_idx}/{total_files}] Completed: {system_name}/{file_name}")
                    
                except Exception as file_error:
                    logger.error(f"Conversion failed for {system_name}/{file_name}: {str(file_error)}")
                    yield emit_error(f"Conversion failed for {system_name}/{file_name}: {str(file_error)}")
                    return
            
            yield emit_message(f"DBMS conversion completed → {self.target.upper()}")
            
        except Exception as e:
            logger.error(f"Conversion error: {str(e)}")
            yield emit_error(f"Conversion error: {str(e)}")
