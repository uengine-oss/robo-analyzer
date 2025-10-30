from .base_strategy import ConversionStrategy
from .framework_strategy import FrameworkConversionStrategy
from .dbms_strategy import DbmsConversionStrategy


class StrategyFactory:
    """전략 패턴 팩토리 클래스"""
    
    @staticmethod
    def create_strategy(conversion_type: str, **kwargs) -> ConversionStrategy:
        """변환 타입에 따라 전략을 생성 (매핑 기반, 확장 용이)."""
        conversion_type = (conversion_type or '').lower()

        creators = {
            "framework": lambda: FrameworkConversionStrategy(
                kwargs.get('target_framework', 'springboot')
            ),
            "dbms": lambda: DbmsConversionStrategy(
                kwargs.get('target_dbms', 'oracle')
            ),
        }

        try:
            return creators[conversion_type]()
        except KeyError as e:
            raise ValueError(f"Unsupported conversion type: {conversion_type}") from e
    
    @staticmethod
    def get_supported_conversion_types() -> dict:
        """
        지원하는 변환 타입 목록을 반환합니다.
        
        Returns:
            dict: 지원하는 변환 타입과 옵션들
        """
        return {
            "framework": {
                "springboot": "Java Spring Boot",
                "fastapi": "Python FastAPI (TODO)"
            },
            "dbms": {
                "postgres_to_oracle": "PostgreSQL → Oracle",
                "oracle_to_postgres": "Oracle → PostgreSQL (TODO)"
            }
        }
