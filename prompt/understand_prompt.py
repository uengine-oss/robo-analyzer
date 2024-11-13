import json
import logging
import os
from langchain.globals import set_llm_cache
from langchain_community.cache import SQLiteCache
from langchain.prompts import PromptTemplate
from langchain.schema.runnable import RunnablePassthrough
from langchain_anthropic import ChatAnthropic
from langchain_openai import ChatOpenAI
from util.exception  import LLMCallError
from langchain_core.output_parsers import JsonOutputParser


db_path = os.path.join(os.path.dirname(__file__), 'langchain.db')
set_llm_cache(SQLiteCache(database_path=db_path))
llm = ChatAnthropic(model="claude-3-5-sonnet-20240620", max_tokens=8000)

prompt = PromptTemplate.from_template(
"""
당신은 Oracle PLSQL 전문가입니다. 주어진 Stored Procedure Code를 철저히 분석하세요.


분석할 Stored Procedure Code:
{code}


분석할 Stored Procedure Code의 범위 목록:
{ranges}


반드시 지켜야할 주의사항:
1. 분석할 Stored Procedure Code의 범위 개수는 {count}개로, 반드시 'analysis'는  {count}개의 요소를 가져야합니다.
2. 테이블의 별칭과 스키마 이름을 제외하고, 오로직 테이블 이름만을 사용하세요.
3. 테이블의 컬럼이 'variable'에 포함되지 않도록, 테이블의 컬럼과 변수에 대한 구분을 확실히 하여 결과를 생성하세요.
4. 테이블에 대한 정보가 식별되지 않을 경우, 'Tables'는 빈 사전 {{}}으로 반환하고, 테이블의 컬럼 타입이 식별되지 않을 경우, 적절한 타입을 넣으세요.


지정된 범위의 Stored Procedure Code 에서 다음 정보를 추출하세요:
1. 코드의 역할과 동작을 2-3줄로 상세하게 설명하세요:
   - 어떤 비즈니스 로직을 수행하는지
   - 어떤 데이터를 처리하고 어떤 결과를 생성하는지
   - 주요 처리 단계나 중요한 로직은 무엇인지
   - 주석이 있다면 주석을 참고하여 작성하세요.

2. 각 범위에서 사용된 모든 변수들을 식별하세요. 변수는 다음과 같은 유형을 모두 포함합니다:
   - 일반 변수 (보통 'v_', 'p_', 'i_', 'o_' 접두사)
   - %ROWTYPE 변수
   - %TYPE 변수

3. 코드 내에서 프로시저, 패키지, 함수 호출을 식별하세요:
   - 외부 패키지의 호출: 'PACKAGE_NAME.PROCEDURE_NAME' 형식으로 저장
   - 현재 패키지 내부 호출: 'PROCEDURE_NAME' 형식으로 저장
   - 모든 호출을 'calls' 배열에 저장하세요.

   
전체 Stored Procedure Code 에서 다음 정보를 추출하세요:
1. SQL CRUD 문에서 'INSERT INTO', 'MERGE INTO', 'FROM', 'UPDATE' 절 이후에 나오는 테이블 이름을 찾아 순서대로 식별합니다.
2. SQL CRUD 문에서 사용된 모든 테이블의 모든 컬럼들과 컬럼의 타입을 식별하세요.
3. SQL CRUD 문을 분석하여 여러 테이블 JOIN 관계를 'source'와 'target' 형태로 표현합니다.


아래는 예시 결과로, 식별된 정보만 담아서 json 형식으로 나타내고, 주석이나 부가 설명은 피해주세요:
{{
    "analysis": [
        {{
            "startLine": startLine,
            "endLine": endLine,
            "summary": "summary of the code",
            "tableNames": ["tableName1", "tableName2"],
            "calls": ["procedure1", "function1", "package1"], 
            "variables": ["variable1", "variable2"]
        }}
    ],
    "Tables": {{
        "tableName1": ["type:field1", "type:field2"], 
        "tableName2": []
    }},
    "tableReference": [{{"source": "tableName1", "target": "tableName2"}}]
}}
""")

# 역할 : 주어진 스토어드 프로시저 코드  분석하여, 사이퍼쿼리 생성에 필요한 정보 받습니다
# 매개변수: 
#   - sp_code: 분석할 스토어드 프로시저 코드 
#   - context_ranges : 분석할 스토어드 프로시저 코드의 범위 
#   - context_range_count : 분석할 스토어드 프로시저 범위의 개수(결과 개수 유지를 위함)
# 반환값 : 
#   - parsed_content : JSON으로 파싱된 llm의 분석 결과
def understand_code(sp_code, context_ranges, context_range_count):
    try:
        ranges_json = json.dumps(context_ranges)

        chain = (
            RunnablePassthrough()
            | prompt
            | llm
        )

        json_parser = JsonOutputParser()
        result = chain.invoke({"code": sp_code, "ranges": ranges_json, "count": context_range_count})
        # TODO 여기서 최대 출력 토큰만 4096이 넘은 경우 처리가 필요
        json_parsed_content = json_parser.parse(result.content)
        logging.info(f"토큰 수: {result.usage_metadata}")     
        return json_parsed_content
    
    except Exception:
        err_msg = "Understanding 과정에서 LLM 호출하는 도중 오류가 발생했습니다."
        logging.exception(err_msg)
        raise LLMCallError(err_msg)