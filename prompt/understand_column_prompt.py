import json
import os
from langchain_core.globals import set_llm_cache
from langchain_community.cache import SQLiteCache
from langchain_core.prompts import PromptTemplate
from langchain_core.runnables import RunnablePassthrough, RunnableConfig
from util.llm_client import get_llm
from util.llm_audit import ainvoke_with_audit
from util.exception import LLMCallError

db_path = os.path.join(os.path.dirname(__file__), 'langchain.db')
set_llm_cache(SQLiteCache(database_path=db_path))


prompt = PromptTemplate.from_template(
    """
[ROLE]
당신은 컬럼 역할 분석 전문가입니다. 주어진 테이블 컬럼 메타와 DML 요약(요약된 의도/흐름)을 바탕으로, 각 컬럼의 역할 라벨을 추론해 명시하세요.

[LANGUAGE]
응답 언어: {locale}

[INPUT]
- 테이블 컬럼(JSON): {columns_json}
- DML 요약 목록(JSON): {dml_summaries_json}

[SECTION_TABLE_DESCRIPTION]
- DML 요약의 흐름과 주요 조작(조회/집계/정렬/갱신/삽입/삭제) 대상/관계를 근거로, 테이블이 수행하는 핵심 기능과 데이터 흐름상 역할을 구체적으로 기술합니다.
- DDL 코멘트의 톤/형식을 준용하되, DML에서 드러난 실제 사용 의도를 반영합니다.
- 추정은 피하고, 요약에 근거가 없는 내용은 포함하지 않습니다.
- 다른 테이블과의 간접적인 관계 및 사소한 정보들 또한 다 포함합니다.

[SECTION_COLUMN_ROLES]
- summary에 언급된 컬럼/흐름과 DML 사용 맥락(WHERE/JOIN/GROUP BY/ORDER BY/INSERT/UPDATE/VALUES)을 근거로, 지나치게 일반적이지 않은 '구체 라벨'을 지정합니다. (예: 수량 → 주문수량/출고수량, 금액 → 결제금액/청구금액/할인금액)
- 가능한 한 2~4 토큰의 짧은 명사구로 표기하며, 테이블/엔티티 의미(ORDER, SHIPMENT, INVOICE 등)나 주변 키 컬럼(ORDER_ID, USER_ID 등) 맥락을 활용해 구체화합니다. (예: 상태 → 주문상태, 일시 → 주문일시)
- 컬럼명 단서(id/no/code/status/date/count/amt/flag 등)는 참고하되, DML의 실제 용도(조회/조인/집계/정렬/갱신 대상)를 우선 반영합니다.
- 동일 컬럼이 복수 용도로 쓰이면 가장 우세한 주 역할을 선택합니다. 모호하면 범용 라벨(참고값/페이로드)을 사용하되, 가능하면 도메인 접두를 붙여 구체화합니다. (예: 참고값 → 고객참고값)

[OUTPUT]
 주석 없이 아래 JSON만 출력합니다.
 ```json
 {{
   "tableDescription": "DML 요약을 근거로 테이블의 기능/흐름/관계를 디테일하게 설명(1~2문장)",
   "roles": [
     {{ "name": "컬럼명", "role": "역할라벨" }}
   ]
 }}
 ```"""
)


async def understand_column_roles(columns: list, dml_summaries: list, api_key: str, locale: str):
    try:
        llm = get_llm(api_key=api_key)
        chain = (
            RunnablePassthrough()
            | prompt
            | llm
        )
        columns_json = json.dumps(columns or [], ensure_ascii=False)
        dml_json = json.dumps([s for s in (dml_summaries or []) if s], ensure_ascii=False)
        payload = {
            "columns_json": columns_json,
            "dml_summaries_json": dml_json,
            "locale": locale,
        }
        result = await ainvoke_with_audit(
            chain,
            payload,
            prompt_name="prompt/understand_column_prompt.py",
            input_payload={
                "columns": columns,
                "dml_summaries": dml_summaries,
                "locale": locale,
            },
            metadata={"type": "column_role_analysis"},
            config=RunnableConfig(
                prompt_type="understand_column_roles"
            )
        )
        content = getattr(result, "content", str(result))
        return json.loads(content)
    except Exception as e:
        raise LLMCallError(f"컬럼 역할 분석 LLM 호출 중 오류: {str(e)}")



