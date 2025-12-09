from __future__ import annotations

from typing import Any, Dict, Protocol, TYPE_CHECKING, Tuple, List

if TYPE_CHECKING:
    from asyncio import Queue
    from understand.analysis import AnalysisBatch, ProcedureInfo
    from understand.analysis import Analyzer, StatementNode


class UnderstandingStrategy(Protocol):
    """
    Understanding 파이프라인에서 필요한 모든 훅을 한 곳에 모은 단일 인터페이스.

    구현체는 아래 메서드만 충실히 구현하면 되며, 추가 호환 레이어를 두지 않습니다.
    이름은 의도를 직관적으로 표현하도록 유지합니다.
    """

    @property
    def name(self) -> str:
        ...

    def statement_rules(self) -> Dict[str, Any]:
        """
        AST 수집 시 사용할 구문 정의 세트를 반환합니다.
        반환 키:
            - procedure_types: Tuple[str, ...]
            - non_analysis_types: frozenset[str]
            - non_next_recursive_types: frozenset[str]
            - dml_statement_types: frozenset[str]
            - variable_role_map: Dict[str, str]
            - variable_declaration_types: frozenset[str]
        """
        ...

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
        procedures: Dict[str, "ProcedureInfo"],
        send_queue: "Queue",
        receive_queue: "Queue",
        file_last_line: int,
    ) -> None:
        """파일 단위 실행 컨텍스트를 한 번만 설정합니다."""
        ...

    async def process_variables(self, analyzer: "Analyzer", nodes: List["StatementNode"]):
        """변수 선언 처리 훅 (필요 시 전략에서 구현)."""
        ...

    async def invoke_batch(
        self, batch: "AnalysisBatch"
    ) -> Tuple[Dict[str, Any] | None, Dict[str, Any] | None]:
        """배치 단위로 LLM을 호출하고 결과를 반환합니다."""
        ...

    async def apply_batch(
        self,
        batch: "AnalysisBatch",
        general: Dict[str, Any] | None,
        table: Dict[str, Any] | None,
    ):
        """LLM 결과를 Neo4j 쿼리로 변환하고 전송합니다."""
        ...

    async def finalize(self):
        """남은 누적 작업(프로시저 요약/테이블 요약 등)을 마무리합니다."""
        ...

    def build_call_queries(
        self,
        node: "StatementNode",
        analysis: Dict[str, Any],
    ) -> List[str]:
        """요약 결과를 기반으로 CALL 관계 Cypher를 생성합니다."""
        ...

