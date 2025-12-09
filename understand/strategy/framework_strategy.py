from __future__ import annotations

from typing import Any, Dict

from understand.strategy.base_strategy import UnderstandingStrategy

class FrameworkUnderstandingStrategy:
    """
    프레임워크/Java 코드 분석용 전략 스켈레톤.
    - 클래스/메서드/호출/상속/구현/의존 관계 추출 전용으로 확장 예정.
    """

    @property
    def name(self) -> str:
        return "framework"

    def statement_rules(self) -> Dict[str, Any]:
        raise NotImplementedError("FrameworkUnderstandingStrategy is not yet implemented.")

    def prepare_context(
        self,
        *,
        node_base_props: str,
        folder_props: str,
        table_base_props: str,
        user_id: str,
        project_name: str,
        folder_name: str,
        file_name: str,
        dbms: str,
        api_key: str,
        locale: str,
        procedures: Dict[str, Any],
        send_queue,
        receive_queue,
        file_last_line: int,
    ) -> None:
        raise NotImplementedError("FrameworkUnderstandingStrategy is not yet implemented.")

    async def process_variables(self, analyzer, nodes):
        raise NotImplementedError("FrameworkUnderstandingStrategy is not yet implemented.")

    async def invoke_batch(self, batch):
        raise NotImplementedError("FrameworkUnderstandingStrategy is not yet implemented.")

    async def apply_batch(self, batch, general, table):
        raise NotImplementedError("FrameworkUnderstandingStrategy is not yet implemented.")

    async def finalize(self):
        raise NotImplementedError("FrameworkUnderstandingStrategy is not yet implemented.")

    def build_call_queries(
        self,
        node,
        analysis: Dict[str, Any],
    ):
        raise NotImplementedError("FrameworkUnderstandingStrategy is not yet implemented.")


