import json
import logging
import os
from langchain.globals import set_llm_cache
from langchain_community.cache import SQLiteCache
from langchain_core.output_parsers import JsonOutputParser
from langchain.prompts import PromptTemplate
from langchain.schema.runnable import RunnablePassthrough
from langchain_anthropic import ChatAnthropic
from langchain_openai import ChatOpenAI
from util.exception  import LLMCallError


db_path = os.path.join(os.path.dirname(__file__), 'langchain.db')
set_llm_cache(SQLiteCache(database_path=db_path))
llm = ChatOpenAI(model="gpt-4o")

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
4. 변수의 역할의 경우, 어떤 테이블에서 사용되었고, 어떤 값을 저장했고, 어떤 목적에 사용되는지를 반드시 명시해야합니다. (예 : 직원 id를 저장하여, 직원 테이블 검색에 사용) 
5. 테이블에 대한 정보가 식별되지 않을 경우, 'Tables'는 빈 사전 {{}}으로 반환하고, 테이블의 컬럼 타입이 식별되지 않을 경우, 적절한 타입을 넣으세요.


지정된 범위의 Stored Procedure Code 에서 다음 정보를 추출하세요:
1. 코드의 주요 내용을 한 문장으로 요약하세요.
2. 각 범위에서 사용된 모든 변수들을 식별하고, 역할을 상세히 설명하세요. 일반적으로 변수는 이름 앞에 'V_' 또는 'p_' 접두사가 붙습니다.


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
            "tableName": ["tableName1", "tableName2"]
        }}
    ],
    "Tables": {{
        "tableName1": ["type:field1", "type:field2"], 
        "tableName2": []
    }},
    "tableReference": [{{"source": "tableName1", "target": "tableName2"}}],
    "variable": {{
        "startLine~endLine": [
            {{"name": "var1", "role": "역할"}}, 
            {{"name": "var2", "role": "역할"}}
        ],
        "startLine~endLine": [
            {{"name": "var1", "role": "역할"}}
        ],
    }}
}}
""")


# 역할 : 주어진 스토어드 프로시저 코드  분석하여, 그래프 생성에 필요한 정보 받습니다
# 매개변수: 
#   - sp_code: 분석할 스토어드 프로시저 코드 
#   - context_ranges : 분석할 스토어드 프로시저 코드의 범위 
#   - context_range_count : 분석할 스토어드 프로시저 범위의 개수(결과 개수 유지를 위함)
# 반환값 : 
#   - result : llm의 분석 결과
#   - prompt.template : 프롬포트 템플릿
def understand_code(sp_code, context_ranges, context_range_count):
    try:
        ranges_json = json.dumps(context_ranges)

        chain = (
            RunnablePassthrough()
            | prompt
            | llm
            | JsonOutputParser()
        )
        result = chain.invoke({"code": sp_code, "ranges": ranges_json, "count": context_range_count})
        return result, prompt.template
    except Exception as e:
        err_msg = "Understanding 과정에서 LLM 호출하는 도중 오류가 발생했습니다."
        logging.exception(err_msg)
        raise LLMCallError(err_msg)