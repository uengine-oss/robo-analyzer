from .base_strategy import ConversionStrategy
from .framework_strategy import FrameworkConversionStrategy
from .dbms_strategy import DbmsConversionStrategy
from .architecture_strategy import ArchitectureConversionStrategy


class StrategyFactory:
    """전략 패턴 팩토리 클래스"""
    
    @staticmethod
    def create_strategy(strategy: str, target: str = 'java') -> ConversionStrategy:
        """전략과 타겟에 따라 변환 전략을 생성합니다.
        
        Args:
            strategy: 전략 타입 ('framework', 'dbms', 'architecture')
            target: 타겟 언어/DBMS (java, python, oracle, postgresql, mermaid)
        """
        strategy = (strategy or 'framework').lower()
        target = (target or 'java').lower()

        creators = {
            "framework": lambda: FrameworkConversionStrategy(target),
            "dbms": lambda: DbmsConversionStrategy(target),
            "architecture": lambda: ArchitectureConversionStrategy(target),
        }

        try:
            return creators[strategy]()
        except KeyError as e:
            raise ValueError(f"Unsupported strategy: {strategy}") from e
    
    @staticmethod
    def get_supported_options() -> dict:
        """지원하는 전략 및 타겟 옵션을 반환합니다."""
        return {
            "framework": ["java", "python"],
            "dbms": ["oracle", "postgresql"],
            "architecture": ["mermaid"]
        }
