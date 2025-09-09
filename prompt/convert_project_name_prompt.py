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

# 프로젝트 이름 생성을 위한 프롬프트 템플릿
project_name_prompt = PromptTemplate.from_template(
"""
당신은 PL/SQL 코드를 스프링부트 자바 프로젝트로 변환하는 전문가입니다. 
파일 목록과 객체 이름을 분석하여 의미있는 스프링부트 프로젝트 이름을 생성해야 합니다.

[SECTION 1] 프로젝트 이름 생성 규칙
===============================================
1. 프로젝트 이름은 명사 형태여야 합니다
2. 프로젝트 이름은 적절한 PascalCase 형태여야 합니다.
3. 이름은 간결해야 합니다
4. 프로젝트 성격에 맞는 영어 단어를 사용하세요
5. 너무 긴 이름은 피하세요. 두 단어를 조합하여 이름을 생성하되, 적당히 축약하여 표현해주세요. 
6. 이름은 8~10글자를 넘기마세요.


[SECTION 2] 분석할 데이터
===============================================
파일 목록과 객체 이름:
{file_data}

[SECTION 3] JSON 출력 형식
===============================================
부가 설명 없이 결과만을 포함하여, 다음 JSON 형식으로 반환하세요:
{{
  "project_name": "의미있는 프로젝트 이름(PascalCase)",
}}
"""
)

# 역할: 파일 목록을 분석하여 스프링부트 프로젝트 이름을 생성합니다.
#
# 매개변수:
#   - file_data: 분석할 파일 목록과 객체 이름
#   - api_key: Anthropic API 키
#
# 반환값:
#   - 생성된 프로젝트 이름
async def generate_project_name_prompt(file_data: list, api_key: str) -> str:
    try:
        llm = get_llm(max_tokens=1000, api_key=api_key)

        file_data_str = json.dumps(file_data, ensure_ascii=False)
        prompt_data = {"file_data": file_data_str}

        chain = (
            RunnablePassthrough()
            | project_name_prompt
            | llm
            | JsonOutputParser()
        )
        
        result = chain.invoke(prompt_data)
        return result["project_name"]
    
    except Exception as e:
        err_msg = f"프로젝트 이름 생성 중 오류가 발생했습니다: {str(e)}"
        logging.error(err_msg)
        raise LLMCallError(err_msg)