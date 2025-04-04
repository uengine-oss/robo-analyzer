import json
import logging
import os
from langchain.globals import set_llm_cache
from langchain_community.cache import SQLiteCache
from langchain_anthropic import ChatAnthropic
from langchain_core.output_parsers import JsonOutputParser
from langchain.prompts import PromptTemplate
from langchain.schema.runnable import RunnablePassthrough
from util.exception import LLMCallError


db_path = os.path.join(os.path.dirname(__file__), 'langchain.db')
set_llm_cache(SQLiteCache(database_path=db_path))


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

# 역할: 토큰 수가 제한을 초과하는 대형 PL/SQL 코드 블록을 처리하는 함수입니다.
#      LLM을 통해 자식 노드들이 "...code..."로 요약된 코드를 분석하고,
#      코드의 전체적인 구조와 흐름을 유지하면서
#      Java 코드의 골격(skeleton)을 생성합니다.
# 매개변수: 
#   - summarized_code : 자식 노드들이 "...code..."로 요약된 PL/SQL 코드
#      (큰 코드 블록의 구조를 파악할 수 있는 요약본)
#   - api_key : OpenAI API 키
#
# 반환값: 
#   - result : LLM이 생성한 요약된 형태의 Java 코드
#      (실제 구현은 나중에 채워질 수 있도록 자리 표시자를 포함)
def convert_summarized_code(summarized_code, api_key):
    
    try:
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
        result = chain.invoke({"summarized_code": summarized_code})
        return result
    
    except Exception as e:
        err_msg = f"서비스 클래스 생성 과정에서 LLM 호출하는 도중 오류가 발생했습니다: {str(e)}"
        logging.error(err_msg)
        raise LLMCallError(err_msg)