import json
import logging
import os
from langchain.globals import set_llm_cache
from langchain_community.cache import SQLiteCache
from langchain_openai import ChatOpenAI
from langchain_core.output_parsers import JsonOutputParser
from langchain.prompts import PromptTemplate
from langchain.schema.runnable import RunnablePassthrough
from util.exception import LLMCallError


db_path = os.path.join(os.path.dirname(__file__), 'langchain.db')
set_llm_cache(SQLiteCache(database_path=db_path))
llm = ChatOpenAI(model_name="gpt-4o")
prompt = PromptTemplate.from_template(
"""
당신은 클린 아키텍처 원칙을 따르는 스프링부트 기반의 자바 애플리케이션을 개발하는 소프트웨어 엔지니어입니다. 주어진 스토어드 프로시저 코드를 기반으로 임시 자바 코드를 생성하는 작업을 맡았습니다.


summarized_stored_procedure_code:
{summarized_code}


[SECTION 1] 코드 변환 규칙
===============================================
1. 기본 규칙
   - summarized_stored_procedure_code에서 ...code...가 식별되는 시작 라인만 자바 코드에 포함


2. 예시 형식
   while (condition) {{
   722: ...code...
   
   723: ...code...
   
   ...
   
   740: ...code...
   }}


[SECTION 2] JSON 출력 형식
===============================================
부가 설명 없이 결과만을 포함하여, 다음 JSON 형식으로 반환하세요:
{{
    "code": "Java Code"
}}
"""
)

# 역할 : 요약된 코드를 기반으로, 요약된 자바 코드를 받는 함수
# 매개변수: 
#   - summarized_code : 자식들이 전부 요약된 코드
# 반환값: 
#   - result : 요약된 자바 코드
def convert_parent_skeleton(summarized_code):
    
    try:
        chain = (
            RunnablePassthrough()
            | prompt
            | llm
            | JsonOutputParser()
        )
        result = chain.invoke({"summarized_code": summarized_code})
        return result
    
    except Exception:
        err_msg = "서비스 클래스 생성 과정에서 LLM 호출하는 도중 오류가 발생했습니다."
        logging.exception(err_msg)
        raise LLMCallError(err_msg)