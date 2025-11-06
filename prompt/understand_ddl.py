import json
import logging
import os
from langchain_core.globals import set_llm_cache
from langchain_community.cache import SQLiteCache
from util.llm_client import get_llm
from util.llm_audit import invoke_with_audit
from langchain_core.output_parsers import JsonOutputParser
from langchain_core.prompts import PromptTemplate
from langchain_core.runnables import RunnablePassthrough
from util.exception import LLMCallError

db_path = os.path.join(os.path.dirname(__file__), 'langchain.db')
set_llm_cache(SQLiteCache(database_path=db_path))


prompt = PromptTemplate.from_template(
"""
당신은 DDL을 분석하여 테이블과 컬럼, 그리고 외래키 관계를 구조화해 반환합니다.

사용자 언어 설정: {locale}

DDL:
{ddl_content}

[규칙]
- 추정 금지: DDL에 없는 정보는 추가하지 않음
- 컬럼명/타입 표기는 DDL 그대로 유지 (대소문자/언더스코어 보존)
- nullable은 불리언(boolean) true/false
- 테이블 설명은 DDL 내 주석/COMMENT 구문으로 파악해 간단 명료하게 1문장
- table_type: CREATE 구문으로부터 BASE TABLE/VIEW 등으로 식별
- 외래키는 소스 컬럼과 대상 "SCHEMA.TABLE.COLUMN" 형태로만 표현
 - 외래키는 DDL 내 제약조건(FOREIGN KEY ... REFERENCES ...)에 명시된 경우에만 추출하며, 그 외 추정/암시는 금지
 - 외래키 표기 시 스키마가 없는 경우 빈 문자열로 처리하며 문자열 'NULL'/'None' 같은 텍스트 표기는 절대 사용하지 않음
- 테이블 의미 파악이 불가하거나, 테이블/컬럼명이 모호해 추정이 필요한 경우 table.comment는 빈 문자열("")로 둠(추정 금지)
- 존재하지 않는 정보는 빈 값으로 표현

[JSON 출력]
- 주석 없이, 형식을 엄격히 준수하여 출력
```json
{{
  "analysis": [
    {{
      "table": {{
        "schema": "스키마 또는 null",
        "name": "테이블명",
        "table_type": "BASE TABLE 또는 VIEW",
        "comment": "테이블 한줄 설명"
      }},
      "columns": [
        {{"name": "컬럼명", "dtype": "데이터타입", "nullable": true, "comment": "컬럼 설명 또는 빈 문자열"}}
      ],
      "primaryKeys": ["PK_COL1", "PK_COL2"],
      "foreignKeys": [
        {{"column": "소스FK컬럼명", "ref": "SCHEMA.TABLE.COLUMN"}}
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
        payload = {"ddl_content": ddl_content, "locale": locale}
        result = invoke_with_audit(
            chain,
            payload,
            prompt_name="prompt/understand_ddl.py",
            input_payload=payload,
            metadata={"type": "ddl_analysis"},
        )
        return result
    except Exception as e:
        err_msg = f"Understanding 과정에서 DDL 관련 LLM 호출하는 도중 오류가 발생했습니다: {str(e)}"
        logging.error(err_msg)
        raise LLMCallError(err_msg)