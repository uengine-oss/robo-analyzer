import logging
from typing import AsyncGenerator, Any
from .base_strategy import ConversionStrategy
from convert.dbms.create_dbms_conversion import start_dbms_conversion
from util.utility_tool import emit_message, emit_data, emit_error, get_procedures_from_file


logger = logging.getLogger(__name__)


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
        async for chunk in self._convert_to_target(file_names, orchestrator, **kwargs):
            yield chunk
    
    async def _convert_to_target(self, file_names: list, orchestrator: Any, **kwargs) -> AsyncGenerator[bytes, None]:
        """PostgreSQL → Target DBMS 변환 (Graph 기반)"""
        try:
            yield emit_message(f"DBMS conversion started → {self.target.upper()}")

            user_id = orchestrator.user_id
            project_name = orchestrator.project_name
            api_key = orchestrator.api_key
            locale = orchestrator.locale

            # 프로시저 목록 가져오기 (Neo4j에서)
            for folder_name, file_name in file_names:
                try:
                    # Neo4j에서 파일의 모든 프로시저 조회
                    procedure_names = await get_procedures_from_file(
                        folder_name=folder_name,
                        file_name=file_name,
                        user_id=user_id,
                        project_name=project_name
                    )
                    
                    # 프로시저가 없으면 파일명 기반으로 폴백
                    if not procedure_names:
                        procedure_names = [file_name.rsplit(".", 1)[0]]
                        logger.warning(f"Neo4j에서 프로시저를 찾지 못함, 파일명 기반 사용: {procedure_names[0]}")
                    
                    yield emit_message(f"Converting {folder_name}/{file_name} ({len(procedure_names)} procedure(s))")
                    
                    # 각 프로시저별로 변환 수행
                    for procedure_name in procedure_names:
                        # Graph 기반 변환
                        converted_code = await start_dbms_conversion(
                            folder_name=folder_name,
                            file_name=file_name,
                            procedure_name=procedure_name,
                            project_name=project_name,
                            user_id=user_id,
                            api_key=api_key,
                            locale=locale,
                            target=self.target
                        )
                        
                        # 스트리밍으로 결과 전송
                        yield emit_data(
                            file_type="converted_sp",
                            file_name=file_name,
                            folder_name=folder_name,
                            code=converted_code,
                            summary=f"Converted to {self.target.upper()} for {procedure_name}",
                        )
                    
                    yield emit_message(f"Conversion completed for {folder_name}/{file_name}")
                    
                except Exception as file_error:
                    logger.error(f"Conversion failed for {folder_name}/{file_name}: {str(file_error)}")
                    yield emit_error(f"Conversion failed for {folder_name}/{file_name}: {str(file_error)}")
                    return
            
            yield emit_message(f"DBMS conversion completed → {self.target.upper()}")
            
        except Exception as e:
            logger.error(f"Conversion error: {str(e)}")
            yield emit_error(f"Conversion error: {str(e)}")
