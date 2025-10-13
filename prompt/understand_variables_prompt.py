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
당신은 SQL 프로시저의 변수를 분석하는 전문가입니다. 주어진 코드에서 모든 변수 선언을 찾아 변수명과 데이터 타입을 추출하는 작업을 수행합니다.


사용자 언어 설정 : {locale}, 입니다. 이를 반영하여 결과를 생성해주세요.


프로시저 코드입니다:
{declaration_code}


[분석 규칙]
===============================================
1. 변수 선언 식별
   - DECLARE 섹션의 변수 선언 (parameter_type: 'LOCAL')
   - 프로시저/함수의 파라미터 식별
     * IN 파라미터 (parameter_type: 'IN')
     * OUT 파라미터 (parameter_type: 'OUT')
     * IN OUT 파라미터 (parameter_type: 'IN_OUT')
   - 커서 변수 식별 및 포함 (REF CURSOR, SYS_REFCURSOR, CURSOR 정의)
     * 명명형 커서: "CURSOR 커서명 IS <쿼리>" 형태 (쿼리에 SELECT/MERGE/UPDATE/DELETE 등 무엇이든 포함 가능)
     * 참조 커서(REF CURSOR) 타입 변수: TYPE ... IS REF CURSOR 또는 SYS_REFCURSOR
     * parameter_type은 명시되지 않으면 LOCAL로 간주
   - 주석이 아닌 실제 선언된 변수만 추출

2. 변수 유형
   - 일반 변수 (v_, p_, i_, o_ 등의 접두사로 구분)
   - %ROWTYPE 변수
   - %TYPE 변수
   - 사용자 정의 타입 변수
   - 커서 변수: 두 가지 타입으로만 구분하여 지정
     * 명명형 커서: type = "CURSOR"
     * 참조 커서(REF CURSOR/SYS_REFCURSOR/TYPE 기반): type = "REF CURSOR"

3. 데이터 타입/값 추출
   - 기본 타입/ROWTYPE/%TYPE 규칙은 동일
   - 커서 변수의 경우
     * 명명형 커서: value에 커서 선언/정의의 원문 전체(쿼리 전문 포함, 포맷 유지)를 그대로 포함
     * 참조 커서: value에 선언 원문(예: "CUR_TEST TYPE_CUR;") 또는 식별 불가 시 null

4. 변수 값 추출
   - DECLARE 섹션의 변수 선언과 초기값
   - 프로시저/함수의 파라미터와 기본값
   - 초기 값이 식별되지 않을 경우 'None'

5. 특수 처리
   - 기본값이 있는 경우에도 변수로 인식 (기본값은 무시)
   - 길이/정밀도 지정이 있는 경우 (예: VARCHAR2(100)) 데이터 타입만 추출
   - 커서 변수는 결과에 반드시 포함

6. 변수 선언부 요약
   - 1-2줄 요약. 커서/파라미터/로컬 변수의 역할을 간단히 설명

7. 테이블 타입 변수 마킹(중요)
   - %ROWTYPE, %TYPE, 테이블 컬럼/레코드 타입 등 테이블을 직접 참조하는 변수는 다음 규칙을 반드시 따릅니다.
     * type: 참조하는 테이블명을 기입 (가능하면 SCHEMA.TABLE, 없으면 TABLE)
     * value: "Table: <테이블명>" 형식의 마커 문자열을 기입 (예: "Table: SALES.ORDER_DETAIL")
   - 이 마커는 후처리에서 Neo4j의 Table 노드와 연결하는 데 사용되므로 오탈자 없이 정확히 출력하세요.

[JSON 출력 형식]
===============================================
주석이나 부가설명 없이 다음 JSON 형식으로만 결과를 반환하세요:
```json
{{
    "variables": [
        {{
            "name": "변수명",
            "type": "데이터타입 또는 테이블명 또는 CURSOR/REF CURSOR",
            "value": "원문 전체 (명명형: 선언/쿼리 전문, 참조: 선언 원문 또는 null, 테이블 타입: 'Table: <테이블명>')",
            "parameter_type": "IN/OUT/IN_OUT/LOCAL"
        }}
    ],
    "summary": "변수 선언부 요약 설명"
}}
```
"""
)

# 역할: PL/SQL 코드에서 선언된 모든 변수를 분석하여 정보를 추출합니다.
#      LLM을 통해 변수의 선언부를 파싱하고, 각 변수의 이름, 데이터 타입,
#      용도(IN/OUT 파라미터, 커서, ROWTYPE 등)를 식별합니다.
#      추후 Java 변수로의 변환을 위한 기초 정보를 제공합니다.
# 매개변수: 
#   - declaration_code: 분석할 PL/SQL 코드의 변수 선언부 (DECLARE 섹션, 파라미터 선언, 커서 선언 등)
#   - api_key: OpenAI API 키
# 반환값: 
#   - result: LLM이 분석한 변수 정보 목록 (각 변수의 이름, 데이터 타입, 용도 등이 포함된 구조화된 데이터)
def understand_variables(declaration_code, api_key, locale):

    try:
        llm = get_llm(api_key=api_key)

        chain = (
            RunnablePassthrough()
            | prompt
            | llm
            | JsonOutputParser()
        )
        result = chain.invoke({"declaration_code": declaration_code, "locale": locale})
        return result
    except Exception as e:
        err_msg = f"Understanding 과정에서 변수 관련 LLM 호출하는 도중 오류가 발생했습니다: {str(e)}"
        logging.error(err_msg)
        raise LLMCallError(err_msg)


# 간단 후처리: 테이블 메타를 기반으로 변수 타입을 해석합니다.
resolve_prompt = PromptTemplate.from_template(
"""
[ROLE]
당신은 PL/SQL 변수 타입 해석 전문가입니다. 주어진 변수 선언 타입과 테이블 메타(컬럼명/데이터타입)를 바탕으로 변수의 실제 타입을 결정하세요.

[LANGUAGE]
응답 언어: {locale}

[INPUT]
- 변수명: {var_name}
- 선언 타입: {declared_type}
- 참조 테이블: {table_schema}.{table_name}
- 테이블 컬럼(JSON): {columns_json}

[RULES]
- %TYPE: "TABLE.COLUMN%TYPE"이면 해당 컬럼의 DDL 타입을 그대로 반환
- %ROWTYPE: "TABLE%ROWTYPE"이면 "ROWTYPE(SCHEMA.TABLE)" 형식으로 반환
- 위 두 경우가 아니면 선언 타입을 그대로 반환
- 스키마/테이블/컬럼명이 없거나 테이블 메타가 없으면 선언 타입 그대로 반환

[OUTPUT]
아래 JSON만 반환:
```json
{{
  "resolvedType": "최종 타입 문자열"
}}
```
"""
)


async def resolve_table_variable_type(var_name: str, declared_type: str, table_schema: str | None, table_name: str | None, columns: list | None, api_key: str, locale: str):
    try:
        llm = get_llm(api_key=api_key)
        chain = (
            RunnablePassthrough()
            | resolve_prompt
            | llm
            | JsonOutputParser()
        )
        columns_json = json.dumps(columns or [], ensure_ascii=False)
        result = await chain.ainvoke({
            "var_name": var_name,
            "declared_type": declared_type,
            "table_schema": table_schema or "",
            "table_name": table_name or "",
            "columns_json": columns_json,
            "locale": locale,
        })
        return result
    except Exception as e:
        err_msg = f"변수 타입 LLM 후처리 호출 중 오류: {str(e)}"
        logging.error(err_msg)
        raise LLMCallError(err_msg)