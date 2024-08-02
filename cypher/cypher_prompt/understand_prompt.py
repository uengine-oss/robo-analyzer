import json
import os
from langchain.globals import set_llm_cache
from langchain_community.cache import SQLiteCache
from langchain_core.output_parsers import JsonOutputParser
from langchain.prompts import PromptTemplate
from langchain.schema.runnable import RunnablePassthrough
from langchain_anthropic import ChatAnthropic
from langchain_openai import ChatOpenAI


# 역할 : 주어진 PLSQL 파일을 분석하여, 필요한 정보를 추출합니다
# 매개변수: 
#      - code: 분석할 스토어드 프로시저 코드 
#      - ontext_ranges : 분석할 스토어드 프로시저 코드의 범위 
#      - context_range_count : 분석할 스토어드 프로시저 범위의 개수(결과 개수 유지를 위함)
# 반환값 : 추후에 분석에 사용되는 필요한 정보(변수, 테이블, 테이블 필드.. 등등)
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


분석할 Stored Procedure Code의 범위 개수는 {count}개입니다. 정확히 {count}개의 'analysis' 항목을 생성해야 합니다.


지정된 범위의 코드 내용에 대해 다음 정보를 추출하세요:
1. SQL CRUD 문에서 'INSERT INTO', 'MERGE INTO', 'FROM', 'UPDATE' 절 이후에 나오는 테이블 이름을 찾아 순서대로 식별합니다.
2. 코드의 주요 내용을 간단한 한 문장으로 요약합니다.
3. SQL CRUD 문에서 사용된 모든 테이블의 속성(필드)을 식별하세요.
4. SQL CRUD 문을 분석하여 여러 테이블 간의 참조(JOIN) 관계를 'source'와 'target' 형태로 표현합니다.
5. 주어진 범위내에 Stored Procedure Code에서 사용된 모든 변수들을 식별하고, 각 변수의 핵심 역할을 간단히 설명합니다.


반드시 지켜야할 주의사항:
1. 테이블의 속성(필드)이 식별되지 않을 경우, 주석이나 부가설명을 피하고, 되도록이면 빈 배열 []로 표시해주세요.
2. 사용된 변수를 식별할 때, 반드시 'p'로 시작하는 프로시저 입력 파라미터 들도 일반 변수들과 같이 'variable'에 포함하도록 하세요.
3. 사용된 변수의 역할을 설명할 때, 어떤 결과값을 저장하는지, 어디에 사용되는지를 중점으로 설명하세요. (예 : 특정 값을 저장해서 연산에 사용됩니다.)
4. 테이블 이름 뒤에 공백이 있을 경우, 그 공백 이후의 텍스트는 테이블의 별칭이므로, 테이블 이름만을 사용하세요.
5. 테이블의 속성이나 변수에 대해서는 생략하거나 요약하지 마세요. 모든 속성 및 변수을 명시적으로 나열하세요. ( 변수에 대해서는 *를 사용 하지마세요.)


아래는 예시 결과입니다. 식별된 데이터만 담아서 json 형식으로 나타내고, 주석이나 부가 설명은 되도록이면 피해주세요:
{{

    "analysis": [

        {{
            "range": {{"startLine": startLine, "endLine": endLine}},
            "summary": "코드의 한 줄 요약"
            "Tables": [{{"tableName1": ["field1", "field2", "field3"]}}, {{"tableName2": ["field4", "field5"]}}],
            "tableReference": [{{"source": "tableName1", "target": "tableName2"}}],
            "variable": [
                {{"name": "var1", "role": "사용자 ID 저장"}},
                {{"name": "var2", "role": "총액 누적"}}
            ]
        }}
    ]
}}
""")

def understand_code(code, context_ranges, context_range_count):
    ranges_json = json.dumps(context_ranges)

    chain = (
        RunnablePassthrough()
        | prompt
        | llm
        | JsonOutputParser()
    )
    result = chain.invoke({"code": code, "ranges": ranges_json, "count": context_range_count})
    return result