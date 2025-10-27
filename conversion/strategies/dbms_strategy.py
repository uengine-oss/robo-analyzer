import json
import logging
import os
import aiofiles
from typing import AsyncGenerator, Any
from .base_strategy import ConversionStrategy
from prompt.convert_dbms_prompt import convert_postgres_to_oracle
from service.service import BASE_DIR


logger = logging.getLogger(__name__)


class DbmsConversionStrategy(ConversionStrategy):
    """DBMS 간 변환 전략 (PostgreSQL → Oracle 등)"""
    
    def __init__(self, source_dbms: str, target_dbms: str):
        self.source_dbms = source_dbms.lower()
        self.target_dbms = target_dbms.lower()
    
    async def convert(self, file_names: list, orchestrator: Any, **kwargs) -> AsyncGenerator[bytes, None]:
        """
        DBMS 간 변환을 수행합니다.
        
        Args:
            file_names: 변환할 파일 목록
            orchestrator: ServiceOrchestrator 인스턴스
            **kwargs: 추가 매개변수
        """
        logger.info(f"DBMS 변환 시작: {self.source_dbms} → {self.target_dbms}")
        
        # 변환 타입에 따라 적절한 메서드 호출
        if self.source_dbms == "postgres" and self.target_dbms == "oracle":
            async for chunk in self._postgres_to_oracle(file_names, orchestrator, **kwargs):
                yield chunk
        else:
            error_msg = f"Unsupported DBMS conversion: {self.source_dbms} → {self.target_dbms}"
            yield f'{{"error": "{error_msg}"}}'.encode('utf-8')
    
    async def _postgres_to_oracle(self, file_names: list, orchestrator: Any, **kwargs) -> AsyncGenerator[bytes, None]:
        """PostgreSQL → Oracle 변환"""
        try:
            yield json.dumps({"type": "ALARM", "MESSAGE": "PostgreSQL to Oracle conversion started"}).encode('utf-8')
            
            user_id = orchestrator.user_id
            project_name = orchestrator.project_name
            api_key = orchestrator.api_key
            locale = orchestrator.locale
            
            # 오케스트레이터의 dirs 속성 사용 (존재하는 경우)
            if hasattr(orchestrator, 'dirs') and orchestrator.dirs:
                plsql_dir = orchestrator.dirs.get('plsql')
                analysis_dir = orchestrator.dirs.get('analysis')
            else:
                # dirs가 없으면 직접 구성
                user_base = os.path.join(BASE_DIR, 'data', user_id, project_name)
                plsql_dir = os.path.join(user_base, "src")
                analysis_dir = os.path.join(user_base, "analysis")
            
            # 출력 디렉토리 설정
            output_dir = os.path.join(BASE_DIR, 'data', user_id, project_name, "oracle_converted")
            
            logger.info(f"plsql_dir: {plsql_dir}")
            logger.info(f"analysis_dir: {analysis_dir}")
            logger.info(f"output_dir: {output_dir}")
            
            # 출력 디렉토리 생성
            os.makedirs(output_dir, exist_ok=True)
            
            for folder_name, file_name in file_names:
                yield json.dumps({"type": "ALARM", "MESSAGE": f"Converting {folder_name}/{file_name}"}).encode('utf-8')
                
                try:
                    # 1. 원본 SQL 파일 로드
                    sql_file_path = os.path.join(plsql_dir, folder_name, file_name)
                    async with aiofiles.open(sql_file_path, 'r', encoding='utf-8') as f:
                        source_code = await f.read()
                    
                    # 2. ANTLR 분석 결과 로드
                    base_name = os.path.splitext(file_name)[0]
                    analysis_file_path = os.path.join(analysis_dir, folder_name, f"{base_name}.json")
                    
                    antlr_data = "{}"
                    if os.path.exists(analysis_file_path):
                        async with aiofiles.open(analysis_file_path, 'r', encoding='utf-8') as f:
                            antlr_data = await f.read()
                    
                    # 3. LLM을 통한 변환
                    logger.info(f"Converting {folder_name}/{file_name} using LLM")
                    result = convert_postgres_to_oracle(source_code, antlr_data, api_key, locale)
                    
                    converted_code = result.get('converted_code', '')
                    summary = result.get('summary', '')
                    
                    # 4. 변환된 파일 저장
                    output_folder = os.path.join(output_dir, folder_name)
                    os.makedirs(output_folder, exist_ok=True)
                    
                    output_file_path = os.path.join(output_folder, file_name)
                    async with aiofiles.open(output_file_path, 'w', encoding='utf-8') as f:
                        await f.write(converted_code)
                    
                    logger.info(f"Converted file saved: {output_file_path}")
                    
                    # 5. 스트리밍으로 결과 전송
                    yield json.dumps({
                        "type": "DATA", 
                        "file_type": "converted_sp", 
                        "file_name": file_name,
                        "folder_name": folder_name,
                        "code": converted_code,
                        "summary": summary
                    }).encode('utf-8')
                    
                except Exception as file_error:
                    logger.exception(f"Error converting file {folder_name}/{file_name}: {str(file_error)}")
                    yield json.dumps({
                        "type": "ALARM", 
                        "MESSAGE": f"Error converting {folder_name}/{file_name}: {str(file_error)}"
                    }).encode('utf-8')
                    continue
            
            yield json.dumps({"type": "ALARM", "MESSAGE": "PostgreSQL to Oracle conversion completed"}).encode('utf-8')
            
        except Exception as e:
            logger.exception(f"PostgreSQL to Oracle 변환 중 오류: {str(e)}")
            yield json.dumps({"error": f"Conversion error: {str(e)}"}).encode('utf-8')
