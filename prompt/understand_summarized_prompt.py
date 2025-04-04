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
당신은 PL/SQL 프로시저와 함수의 동작을 분석하고 요약하는 전문가입니다.
주어진 코드 분석 요약들을 바탕으로 전체 프로시저/함수의 핵심 기능을 간단명료하게 설명해주세요.

분석된 요약 내용:
{summaries}

[분석 규칙]
===============================================
1. 핵심 기능 파악
   - 프로시저/함수가 수행하는 주요 작업
   - 입력과 출력의 흐름
   - 중요한 비즈니스 로직

2. 요약 방식
   - 최소 3~4줄로 상세하게 정리
   - 기술적인 용어는 최소화
   - 비즈니스 관점에서 이해하기 쉽게 설명
   예시) 직원의 인사 정보를 갱신하는 프로시저로,
         입력받은 직원 ID를 기준으로 부서 이동, 직급 변경, 급여 조정 등의 정보를 처리하며,
         변경된 정보는 인사 이력 테이블에 자동으로 기록됩니다.
         또한 변경 사항에 따라 관련 부서장과 인사팀에 이메일 알림을 발송합니다."

   예시) 월별 급여 지급 처리를 수행하는 프로시저로,
         해당 월의 근태 기록과 수당 정보를 집계하여 실지급액을 계산하고,
         각 직원별 급여 명세서를 생성합니다.
         계산된 급여는 지정된 은행 계좌로 일괄 이체 요청됩니다."
   

[JSON 출력 형식]
===============================================
주석이나 부가설명 없이 다음 JSON 형식으로만 결과를 반환하세요:
{{
    "summary": "프로시저/함수의 흐름을 요약한 문장"
}}
"""
)

def understand_summary(summaries, api_key):
    try:

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
        result = chain.invoke({"summaries": summaries})
        return result
    except Exception as e:
        err_msg = f"Understanding 과정에서 요약 관련 LLM 호출하는 도중 오류가 발생했습니다: {str(e)}"
        logging.error(err_msg)
        raise LLMCallError(err_msg)