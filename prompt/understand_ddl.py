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
import openai

db_path = os.path.join(os.path.dirname(__file__), 'langchain.db')
set_llm_cache(SQLiteCache(database_path=db_path))


prompt = PromptTemplate.from_template(
"""
당신은 DDL을 분석하여 테이블 구조를 파악하는 전문가입니다. 주어진 DDL에서 테이블 정보와 관계를 추출합니다.


사용자 언어 설정 : {locale}, 입니다. 이를 반영하여 결과를 생성해주세요.


DDL 내용입니다:
{ddl_content}

[분석 규칙]
===============================================
1. 전달된 모든 테이블 정보 추출
   - 스키마와 테이블 이름을 구분하여 추출
     * 예: SGIOS.T_RAW_TABLE -> schema: "SGIOS", name: "T_RAW_TABLE"
     * 스키마가 명시되지 않으면 schema: null, name: "테이블명"
   
2. 전달된 모든 테이블의 컬럼 정보 추출
   - 컬럼명
   - 데이터 타입
   - null 허용 여부

3. 전달된 모든 테이블의 키 정보 추출
   - Primary Key 컬럼
   - Foreign Key 관계 (참조하는 스키마, 테이블, 컬럼)

4. 컬럼명/생성 규칙 (중요)
   - 컬럼명은 DDL에 기재된 이름을 그대로 사용하세요. 대소문자 및 언더스코어 등 표기 변경 금지
   - DDL에 명시된 컬럼만 생성하세요. 추정으로 새로운 컬럼을 추가하거나 이름을 바꾸지 마세요

[JSON 출력 형식]
===============================================
주석이나 부가설명 없이 다음 JSON 형식으로만 결과를 반환하세요:
```json
{{
    "analysis": [
        {{
            "table": {{
                "schema": "스키마명 또는 null",
                "name": "테이블명"
            }},
            "columns": [
                {{
                    "name": "컬럼명",
                    "type": "데이터타입",
                    "nullable": "true/false"
                }}
            ],
            "keys": {{
                "primary": ["컬럼명1", "컬럼명2"],
                "foreign": [
                    {{
                        "column": "현재 테이블의 컬럼",
                        "references": {{
                            "schema": "참조 스키마 또는 null",
                            "table": "참조하는 테이블",
                            "column": "참조하는 컬럼"
                        }}
                    }}
                ]
            }}
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

    try:
        llm = get_llm(max_tokens=8192, api_key=api_key)

        # 일부 OpenAI-호환 게이트웨이에서 대형 JSON 문자열의 대괄호/따옴표를 헤더로 오인식하는 이슈가 있어
        # DDL은 생 문자열로 전달합니다.
        ddl_content = ddl_content

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