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

api_key = os.getenv("OPENAI_API_KEY")
if api_key is None:
    raise ValueError("OPENAI_API_KEY 환경 변수가 설정되지 않았습니다.")

# llm = ChatOpenAI(api_key=api_key, model_name="gpt-4o")
llm = ChatAnthropic(model="claude-3-5-sonnet-20240620", max_tokens=8000)

prompt = PromptTemplate.from_template(
"""
당신은 SQL 프로시저의 변수 선언부를 분석하는 전문가입니다. 주어진 프로시저 선언부에서 변수명과 데이터 타입을 추출하는 작업을 수행합니다.


프로시저 선언부 코드입니다:
{declaration_code}


[분석 규칙]
===============================================
1. 변수 식별
   - 프로시저 파라미터로 선언된 모든 변수를 식별
   - 변수명은 정확히 선언된 형태로 추출 (p_employee_id 등)
   
2. 데이터 타입 추출
   - 각 변수의 데이터 타입을 정확히 식별
   - 기본값이 있는 경우에도 데이터 타입만 추출
   - 대소문자 구분하여 추출 (NUMBER, VARCHAR2 등)

3. 기본값 처리
   - 기본값이 있는 경우에도 변수로 인식
   - 기본값 자체는 결과에 포함하지 않음

   
[JSON 출력 형식]
===============================================
다음 JSON 형식으로만 결과를 반환하세요:
{{
    "variables": [
        {{
            "name": "변수명",
            "type": "데이터타입"
        }}
    ]
}}
"""
)


# 역할 : 테이블 정보를 기반으로 스프링 부트 기반의 엔티티 클래스를 생성합니다
# 매개변수: 
#   - table_data : 테이블 노드 정보
# 반환값 : 
#   - result : 엔티티 클래스
def understand_variables(declaration_code):
    
    try:
        chain = (
            RunnablePassthrough()
            | prompt
            | llm
            | JsonOutputParser()
        )
        result = chain.invoke({"declaration_code": declaration_code})
        return result
    except Exception:
        err_msg = "Understanding 과정에서 LLM 호출하는 도중 오류가 발생했습니다."
        logging.error(err_msg, exc_info=False)
        raise LLMCallError(err_msg)