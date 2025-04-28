import json
import logging
import os
from langchain.globals import set_llm_cache
from langchain_community.cache import SQLiteCache
from langchain_openai import ChatOpenAI
from langchain_anthropic import ChatAnthropic
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

프로시저 코드입니다:
{declaration_code}

테이블 정보입니다:
{ddl_tables}


[분석 규칙]
===============================================
1. 변수 선언 식별
   - DECLARE 섹션의 변수 선언 (parameter_type: 'LOCAL')
   - 프로시저/함수의 파라미터 식별
     * IN 파라미터 (parameter_type: 'IN')
     * OUT 파라미터 (parameter_type: 'OUT')
     * IN OUT 파라미터 (parameter_type: 'IN_OUT')
   - 주석이 아닌 실제 선언된 변수만 추출

2. 변수 유형
   - 일반 변수 (v_, p_, i_, o_ 등의 접두사로 구분)
   - %ROWTYPE 변수
   - %TYPE 변수
   - 사용자 정의 타입 변수

3. 데이터 타입 추출
   - 기본 데이터 타입 (NUMBER, VARCHAR2, DATE 등)
   - %ROWTYPE의 경우 테이블명을 type으로 지정 (예: "TPJ_TMF_SYNC_JOB_STATUS%ROWTYPE" -> "TPJ_TMF_SYNC_JOB_STATUS")
   - %TYPE의 경우 DDL 정보를 참조하여 실제 컬럼 타입으로 변환
   - 사용자 정의 타입은 원본 그대로 사용
   - 대소문자 구분하여 추출
   
4. 변수 값 추출
   - DECLARE 섹션의 변수 선언과 초기값
   - 프로시저/함수의 파라미터와 기본값
   - 만약 초기 값이 식별되지 않을 경우 'None'으로 표현
   - %ROWTYPE 변수의 경우 참조하고 있는 테이블 이름을 값으로 추출 (예: Employee%ROWTYPE -> Employee)

5. 특수 처리
   - 기본값이 있는 경우에도 변수로 인식 (기본값은 무시)
   - 길이/정밀도 지정이 있는 경우 (예: VARCHAR2(100)) 데이터 타입만 추출

   
6. 변수 선언부 요약
   - 선언된 모든 변수들을 종합적으로 분석하여 1-2줄로 요약
   - 요약에는 반드시 변수 명과 용도를 포함
   - 예시: "프로시저 실행 결과를 저장하는 OUT 파라미터(o_result)와 임시 데이터를 저장하는 로컬 변수들(v_temp_id, v_temp_name)이 선언되어 있음"


[JSON 출력 형식]
===============================================
주석이나 부가설명 없이 다음 JSON 형식으로만 결과를 반환하세요:
{{
    "variables": [
        {{
            "name": "변수명",
            "type": "데이터타입",
            "value": "할당값 또는, 0, 빈 문자열",
            "parameter_type": "IN/OUT/IN_OUT/LOCAL",
        }}
    ],
    "summary": "변수 선언부 요약 설명"
}}
"""
)

# 역할: PL/SQL 코드에서 선언된 모든 변수를 분석하여 정보를 추출합니다.
#      LLM을 통해 변수의 선언부를 파싱하고, 각 변수의 이름, 데이터 타입,
#      용도(IN/OUT 파라미터, 커서, ROWTYPE 등)를 식별합니다.
#      추후 Java 변수로의 변환을 위한 기초 정보를 제공합니다.
# 매개변수: 
#   - declaration_code: 분석할 PL/SQL 코드의 변수 선언부 (DECLARE 섹션, 파라미터 선언, 커서 선언 등)
#   - ddl_tables: 테이블 DDL 정보
#   - api_key: OpenAI API 키
# 반환값: 
#   - result: LLM이 분석한 변수 정보 목록 (각 변수의 이름, 데이터 타입, 용도 등이 포함된 구조화된 데이터)
def understand_variables(declaration_code, ddl_tables, api_key):

    try:
        ddl_tables = json.dumps(ddl_tables, ensure_ascii=False, indent=2)

        # 전달받은 API 키로 Anthropic Claude LLM 인스턴스 생성
        llm = ChatAnthropic(
            model="claude-3-7-sonnet-latest", 
            max_tokens=8192,
            api_key=api_key
        )

        chain = (
            RunnablePassthrough()
            | prompt
            | llm
            | JsonOutputParser()
        )
        result = chain.invoke({"declaration_code": declaration_code, "ddl_tables": ddl_tables})
        return result
    except Exception as e:
        err_msg = f"Understanding 과정에서 변수 관련 LLM 호출하는 도중 오류가 발생했습니다: {str(e)}"
        logging.error(err_msg)
        raise LLMCallError(err_msg)