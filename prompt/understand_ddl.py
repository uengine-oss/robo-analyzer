import json
import logging
import os
from langchain.globals import set_llm_cache
from langchain_community.cache import SQLiteCache
from util.llm_client import get_llm
from langchain_core.output_parsers import JsonOutputParser
from langchain.prompts import PromptTemplate
from langchain.schema.runnable import RunnablePassthrough
from util.exception import LLMCallError

db_path = os.path.join(os.path.dirname(__file__), 'langchain.db')
set_llm_cache(SQLiteCache(database_path=db_path))


prompt = PromptTemplate.from_template(
"""
당신은 DDL을 분석하여 테이블 구조를 파악하는 전문가입니다. 주어진 DDL에서 테이블 정보와 관계를 추출합니다.

사용자 언어 설정: {locale}

DDL:
{ddl_content}

[규칙]
- 컬럼명은 DDL 표기를 그대로 사용 (대소문자/언더스코어 보존)
- 존재하지 않는 컬럼/키를 추정하여 추가하지 않음
- nullable은 불리언(boolean)으로 표기
- 외래키 참조는 "SCHEMA.TABLE.COLUMN" 형식의 단일 문자열로 평탄화(스키마 없으면 "TABLE.COLUMN")

[JSON 출력]
주석 없이 아래 형식으로만 출력:
```json
{{
  "analysis": [
    {{
      "table": {{"schema": "스키마 또는 null", "name": "테이블명"}},
      "columns": [
        {{"name": "컬럼명", "type": "데이터타입", "nullable": true}}
      ],
      "primaryKeys": ["PK_COL1", "PK_COL2"],
      "foreignKeys": [
        {{"column": "FK_COL", "ref": "SCHEMA.TABLE.COLUMN"}}
      ]
    }}
  ]
}}
```
"""
)


# 역할: DDL 문을 분석하여 테이블 구조와 관계 정보를 추출하는 함수입니다.
#      LLM을 통해 테이블의 정보(이름, 설명), 컬럼 정보(이름, 타입, 설명),
#      키 정보(기본키, 외래키)를 분석하고 정형화된 JSON 형식으로 변환합니다.
# 매개변수:
#   - ddl_content : 분석할 DDL 문자열
#                  (CREATE TABLE 문, 테이블/컬럼 코멘트, 제약조건 포함)
# 반환값:
#   - result : 테이블 구조 분석 결과가 담긴 JSON 형식의 딕셔너리
#             (테이블 정보, 컬럼 목록, 키 정보 포함)
def understand_ddl(ddl_content, api_key, locale):
    """DDL을 분석하여 테이블·컬럼·키 정보를 단순 JSON 스키마로 반환합니다.

    매개변수:
    - ddl_content: 분석할 DDL 문자열
    - api_key: OpenAI 호환 API 키
    - locale: 로케일 문자열

    반환값:
    - dict: { analysis: [ { table, columns[], primaryKeys[], foreignKeys[] } ] }
    """

    try:
        llm = get_llm(api_key=api_key)

        chain = (
            RunnablePassthrough()
            | prompt
            | llm
            | JsonOutputParser()
        )
        result = chain.invoke({"ddl_content": ddl_content, "locale": locale})
        return result
    except Exception as e:
        err_msg = f"Understanding 과정에서 DDL 관련 LLM 호출하는 도중 오류가 발생했습니다: {str(e)}"
        logging.error(err_msg)
        raise LLMCallError(err_msg)