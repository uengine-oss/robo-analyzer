import os
import json
import logging
from langchain.globals import set_llm_cache
from langchain_community.cache import SQLiteCache
from langchain_openai import ChatOpenAI
from langchain_anthropic import ChatAnthropic
from langchain_core.output_parsers import JsonOutputParser
from langchain.prompts import PromptTemplate
from langchain.schema.runnable import RunnablePassthrough

from util.exception import LLMCallError

db_path = os.path.join(os.path.dirname(__file__), 'langchain.db')
set_llm_cache(SQLiteCache(database_path=db_path))
llm = ChatAnthropic(model="claude-3-5-sonnet-20241022", max_tokens=8000, temperature=0.0)
prompt = PromptTemplate.from_template(
"""
당신은 Java 컴파일 오류 로그를 분석하는 전문가입니다. 컴파일 오류 로그를 분석하여 오류의 원인을 정확히 설명하는 텍스트를 생성해야 합니다.

[분석할 데이터]
1. Java 컴파일 오류 로그:
{error_log}

[출력 형식]
부가 설명 없이 결과만을 포함하여, 다음 JSON 형식으로 반환하세요:
{{
    "error_text": "content"
}}
"""
)


def generate_error_log(error_log: str) -> str:
    try:
        chain = (
            RunnablePassthrough()
            | prompt
            | llm
            | JsonOutputParser()
        )
        
        result = chain.invoke({
            "error_log": error_log
        })
        
        return result
    except Exception as e:
        err_msg = f"컴파일 오류 로그 생성 중 오류가 발생했습니다: {str(e)}"
        logging.error(err_msg)
        raise LLMCallError(err_msg)