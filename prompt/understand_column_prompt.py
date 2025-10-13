import json
import os
from langchain.globals import set_llm_cache
from langchain_community.cache import SQLiteCache
from langchain.prompts import PromptTemplate
from langchain.schema.runnable import RunnablePassthrough
from util.llm_client import get_llm
from util.exception import LLMCallError

db_path = os.path.join(os.path.dirname(__file__), 'langchain.db')
set_llm_cache(SQLiteCache(database_path=db_path))


prompt = PromptTemplate.from_template(
    """
[ROLE]
당신은 컬럼 역할 분석 전문가입니다. 주어진 테이블 컬럼 메타와 DML 요약을 바탕으로, 각 컬럼의 역할을 한 문장으로 명확히 설명하세요.

[LANGUAGE]
응답 언어: {locale}

[INPUT]
- 테이블 컬럼(JSON): {columns_json}
- DML 요약 목록(JSON): {dml_summaries_json}

[GUIDELINES]
각 컬럼당 1문장으로, 실제 용도를 구체적으로 설명합니다. (예: "주문 상세를 식별하는 기본키로 사용")
 DML 요약에서 해당 컬럼이 WHERE, JOIN, GROUP BY, ORDER BY, INSERT/UPDATE SET, VALUES 등 어디에서 어떻게 쓰였는지 근거를 반영합니다.
 컬럼명이 id, no, code, status, date, count, amt, flag 등을 포함해도 라벨 나열이 아닌 문장형 설명으로 작성합니다.
 모호하면 가장 우세한 용도를 기준으로 간결한 문장을 작성합니다.

[OUTPUT]
주석 없이 아래 JSON만 출력합니다.
```json
{{
  "roles": [
    {{ "name": "컬럼명", "description": "해당 컬럼의 역할을 설명하는 한 줄" }}
  ]
}}
```
"""
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
        result = await chain.ainvoke({
            "columns_json": columns_json,
            "dml_summaries_json": dml_json,
            "locale": locale,
        })
        content = getattr(result, "content", str(result))
        return json.loads(content)
    except Exception as e:
        raise LLMCallError(f"컬럼 역할 분석 LLM 호출 중 오류: {str(e)}")


