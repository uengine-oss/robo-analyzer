"""
Architecture 변환 전략
- Framework understanding 결과를 기반으로 Mermaid 클래스 다이어그램 생성
"""

import logging
from typing import AsyncGenerator, Any, List, Tuple

from .base_strategy import ConversionStrategy
from convert.architecture.create_class_diagram import start_class_diagram_generation
from util.utility_tool import emit_message, emit_data, emit_error


logger = logging.getLogger(__name__)


class ArchitectureConversionStrategy(ConversionStrategy):
    """Architecture 변환 전략 (Mermaid 클래스 다이어그램 생성)"""
    
    def __init__(self, target: str = "mermaid"):
        self.target = target.lower()
    
    async def convert(
        self,
        file_names: list,
        orchestrator: Any,
        directories: List[str] = None,
        **kwargs
    ) -> AsyncGenerator[bytes, None]:
        """
        클래스 다이어그램 변환 수행
        
        Args:
            file_names: 사용 안함 (architecture는 directories 기반)
            orchestrator: ServiceOrchestrator
            directories: [("dir/file.java", "ClassName"), ...] (directory, class_name) 튜플 리스트
        """
        if not directories:
            yield emit_error("directories가 필요합니다. 예: [(\"sample/com/example/Player.java\", \"Player\")]")
            return
        
        file_count = len(directories)
        
        try:
            yield emit_message("클래스 다이어그램 생성을 시작합니다")
            yield emit_message(f"프로젝트 '{orchestrator.project_name}'의 {file_count}개 클래스를 분석합니다")
            
            yield emit_message("클래스 정보를 수집하고 있습니다")
            
            for idx, (directory_path, class_name) in enumerate(directories, 1):
                yield emit_message(f"대상 클래스: {class_name} ({directory_path}) ({idx}/{file_count})")
            
            yield emit_message("클래스 구조 및 관계를 분석하고 있습니다")
            
            yield emit_message("Mermaid 다이어그램 코드를 생성하고 있습니다")
            
            result = await start_class_diagram_generation(
                directories=directories,
                project_name=orchestrator.project_name,
                user_id=orchestrator.user_id
            )
            
            result_class_count = result['class_count']
            result_rel_count = result['relationship_count']
            
            yield emit_message(f"분석이 완료되었습니다 (클래스 {result_class_count}개, 관계 {result_rel_count}개)")
            
            yield emit_data(
                file_type="mermaid_diagram",
                diagram=result["diagram"],
                class_count=result_class_count,
                relationship_count=result_rel_count
            )
            
            yield emit_message("클래스 다이어그램 생성이 완료되었습니다")
            yield emit_message(f"결과: 클래스 {result_class_count}개, 관계 {result_rel_count}개 (Mermaid 형식)")
            
        except ValueError as e:
            yield emit_message(f"검증 중 오류가 발생했습니다: {str(e)}")
            yield emit_error(str(e))
        except Exception as e:
            logger.error(f"Architecture 변환 오류: {e}")
            yield emit_message(f"다이어그램 생성 중 오류가 발생했습니다: {str(e)}")
            yield emit_error(f"다이어그램 생성 실패: {str(e)}")
