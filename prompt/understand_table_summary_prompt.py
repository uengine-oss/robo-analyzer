import json
import logging
import os

from langchain_core.globals import set_llm_cache
from langchain_community.cache import SQLiteCache
from langchain_core.prompts import PromptTemplate
from langchain_core.output_parsers import JsonOutputParser
from langchain_core.runnables import RunnablePassthrough

from util.exception import LLMCallError
from util.llm_client import get_llm
from util.llm_audit import invoke_with_audit


_db_path = os.path.join(os.path.dirname(__file__), "langchain.db")
set_llm_cache(SQLiteCache(database_path=_db_path))


_prompt = PromptTemplate.from_template(
    """
당신은 데이터베이스 문서를 작성하는 기술 전문가입니다.

테이블 이름: {table_name}
사용자 언어: {locale}

테이블에 대한 참고 문장들:
{table_sentences}

컬럼에 대한 참고 문장들(JSON):
{column_sentences}

요구사항:
1. 제공된 문장을 활용하여 테이블의 핵심 목적과 DML 사용 패턴을 2~3문장으로 요약합니다.
2. 각 컬럼별로 역할을 한 문장으로 간결하게 정리합니다. (입력 문장이 여러 개일 경우 필요한 정보만 결합)
3. 중복 표현을 피하고, 추측하지 말고 주어진 문장에 기반해 설명합니다.
4. 결과는 다음 JSON 형식으로 출력합니다.

```json
{{
  "tableDescription": "...",
  "columns": [
    {{"name": "컬럼명", "description": "컬럼 역할 요약"}}
  ]
}}
```

JSON 외 다른 텍스트는 출력하지 마세요.
"""
)


def summarize_table_metadata(
    table_name: str,
    table_sentences: list[str],
    column_sentences: dict[str, list[str]],
    api_key: str,
    locale: str,
) -> dict:
    try:
        llm = get_llm(api_key=api_key)

        table_text = "\n".join(table_sentences) if table_sentences else ""
        columns_json = json.dumps(column_sentences, ensure_ascii=False)

        chain = (
            RunnablePassthrough()
            | _prompt
            | llm
            | JsonOutputParser()
        )

        payload = {
            "table_name": table_name,
            "table_sentences": table_text,
            "column_sentences": columns_json,
            "locale": locale,
        }
        result = invoke_with_audit(
            chain,
            payload,
            prompt_name="prompt/understand_table_summary_prompt.py",
            input_payload={
                "table_name": table_name,
                "table_sentences": table_sentences,
                "column_sentences": column_sentences,
                "locale": locale,
            },
            metadata={"type": "table_summary"},
        )
        if not isinstance(result, dict):
            return {"tableDescription": "", "columns": []}
        result.setdefault("tableDescription", "")
        result.setdefault("columns", [])
        return result
    except Exception as exc:
        logging.error("테이블 설명 요약 중 오류: %s", exc)
        raise LLMCallError(str(exc))
