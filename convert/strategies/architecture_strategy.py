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
        class_names: List[Tuple[str, str]] = None,
        **kwargs
    ) -> AsyncGenerator[bytes, None]:
        """
        클래스 다이어그램 변환 수행
        
        Args:
            file_names: 사용 안함 (architecture는 class_names 기반)
            orchestrator: ServiceOrchestrator
            class_names: [(systemName, className), ...] 튜플 리스트
        """
        if not class_names:
            yield emit_error("class_names가 필요합니다. 형식: [(systemName, className), ...]")
            return
        
        try:
            yield emit_message(f"클래스 다이어그램 생성 시작: {len(class_names)}개 클래스")
            
            # 클래스 다이어그램 생성
            result = await start_class_diagram_generation(
                class_names=class_names,
                project_name=orchestrator.project_name,
                user_id=orchestrator.user_id,
                api_key=orchestrator.api_key,
                locale=orchestrator.locale
            )
            
            yield emit_message(f"조회 완료: {result['class_count']}개 클래스, {result['relationship_count']}개 관계")
            
            # 결과 전송
            yield emit_data(
                file_type="mermaid_diagram",
                diagram=result["diagram"],
                class_count=result["class_count"],
                relationship_count=result["relationship_count"]
            )
            
            yield emit_message("클래스 다이어그램 생성 완료")
            
        except ValueError as e:
            yield emit_error(str(e))
        except Exception as e:
            logger.error(f"Architecture 변환 오류: {e}")
            yield emit_error(f"다이어그램 생성 실패: {str(e)}")
