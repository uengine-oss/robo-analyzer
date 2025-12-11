from .base_strategy import UnderstandStrategy
from .dbms_strategy import DbmsUnderstandStrategy
from .framework_strategy import FrameworkUnderstandStrategy


class UnderstandStrategyFactory:
    """분석 타입에 따라 적절한 Understanding 전략을 생성합니다."""

    @staticmethod
    def create_strategy(strategy_type: str) -> UnderstandStrategy:
        strategy_type = (strategy_type or "dbms").lower()
        creators = {
            "dbms": DbmsUnderstandStrategy,
            "framework": FrameworkUnderstandStrategy,
        }
        try:
            return creators[strategy_type]()
        except KeyError as exc:  # pragma: no cover - 방어적 처리
            raise ValueError(f"Unsupported analysis strategy: {strategy_type}") from exc

