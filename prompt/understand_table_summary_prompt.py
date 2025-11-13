import json
import logging
import os

from langchain_core.globals import set_llm_cache
from langchain_community.cache import SQLiteCache
from langchain_core.prompts import PromptTemplate
from langchain_core.output_parsers import JsonOutputParser
from langchain_core.runnables import RunnablePassthrough, RunnableConfig

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

컬럼 메타데이터와 예시 값(JSON):
{column_metadata}

요구사항:
1. 제공된 문장을 활용하여 테이블의 핵심 목적과 DML 사용 패턴을 2~3문장으로 요약합니다.
2. 각 컬럼별로 역할을 한 문장으로 간결하게 정리합니다. (입력 문장이 여러 개일 경우 필요한 정보만 결합)
3. 중복 표현을 피하고, 추측하지 말고 주어진 문장에 기반해 설명합니다.
4. detailDescription는 JSON이 아닌 '사람이 읽을 수 있는 텍스트' 형식으로 작성합니다.
   - 첫 줄은 반드시 "설명: "으로 시작하고, 테이블 목적을 1문장으로 요약합니다.
   - 다음 줄에 "주요  컬럼:"(공백 2개)을 넣고, 그 아래에 각 컬럼을 한 줄씩 기재합니다.
   - 각 컬럼 줄은 "   역할 요약" 형식으로 작성합니다(앞에 공백 3칸). 컬럼의 name은 출력하지 않습니다.
   - 예시 값(examples)이 있는 경우 " (예: v1, v2, ...)"를 컬럼 줄 끝에 추가하며, 가능한 모든 코드값을 모두 포함합니다(최대 20개).
5. 결과는 다음 JSON 형식으로 출력합니다.

```json
{{
  "tableDescription": "...",
  "detailDescription": "설명: ...\n주요  컬럼:\n   ... (예: ...)\n   ...",
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
    column_metadata: dict[str, dict],
    api_key: str,
    locale: str,
) -> dict:
    try:
        llm = get_llm(api_key=api_key)

        table_text = "\n".join(table_sentences) if table_sentences else ""
        columns_json = json.dumps(column_sentences, ensure_ascii=False)
        columns_meta_json = json.dumps(column_metadata or {}, ensure_ascii=False)

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
            "column_metadata": columns_meta_json,
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
                "column_metadata": column_metadata,
                "locale": locale,
            },
            metadata={"type": "table_summary"},
            config=RunnableConfig(
                prompt_type="summarize_table_metadata"
            )
        )
        if not isinstance(result, dict):
            return {"tableDescription": "", "detailDescription": "", "columns": []}
        result.setdefault("tableDescription", "")
        result.setdefault("detailDescription", "")
        result.setdefault("columns", [])
        return result
    except Exception as exc:
        logging.error("테이블 설명 요약 중 오류: %s", exc)
        raise LLMCallError(str(exc))
