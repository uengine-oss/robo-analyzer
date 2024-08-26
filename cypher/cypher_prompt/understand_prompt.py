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


지정된 범위의 Stored Procedure Code 내용에 대해 다음 정보를 추출하세요:
1. 코드의 주요 내용을 한 문장으로 요약하여, analysis를 생성하세요.
2. 각 범위에서 사용된 모든 변수들을 식별한 뒤, 어떤 결과값을 저장했고, 어디에 사용되는지를 역할로 간단히 설명하세요.
3. **중요** : 'variable'에서 'startLine'은 'analysis'의 'startLine'과 동일합니다. 즉, 변수가 실제로 사용된 라인이 아닌, 변수가 어떤 구문에 속해있는지를 나타냅니다.


전체 Stored Procedure Code에 대해 다음 정보를 추출하세요:
1. SQL CRUD 문에서 'INSERT INTO', 'MERGE INTO', 'FROM', 'UPDATE' 절 이후에 나오는 테이블 이름을 찾아 순서대로 식별합니다.
2. SQL CRUD 문에서 사용된 모든 테이블의 속성(필드)을 식별하세요.
3. SQL CRUD 문을 분석하여 여러 테이블 간의 참조(JOIN) 관계를 'source'와 'target' 형태로 표현합니다.


반드시 지켜야할 주의사항:
1. 사용된 변수를 식별할 때, 반드시 'p'로 시작하는 프로시저 입력 파라미터 들도 일반 변수들과 같이 'variable'에 포함하도록 하세요.
2. 테이블 이름 뒤에 공백이 있을 경우, 그 공백 이후의 텍스트는 테이블의 별칭이므로, 테이블 이름만을 사용하세요.
3. 만약 결과를 생성할 때, 식별되지 않거나 존재하지 않는 정보에 대해서는 절대로 생략하지 마시고, 반드시 빈 문자열 "" 이나 빈 배열 []로 제공하세요.
4. 테이블의 필드와  변수에 대한 구분을 확실히하세요.


아래는 예시 결과입니다. 식별된 데이터만 담아서 json 형식으로 나타내고, 주석이나 부가 설명은 되도록이면 피해주세요:
{{
    "analysis": [
        {{
            "startLine": startLine,
            "endLine": endLine,
            "summary": "코드의 한 줄 요약"
            "tableName": ["식별된 테이블 이름1", "식별된 테이블 이름2"]
        }}
    ],
    "Tables": [{{"tableName1": ["type:field1", "type:field2"]}}, {{"tableName2": []}}],
    "tableReference": [{{"source": "tableName1", "target": "tableName2"}}],
    "variable": {{
        "startLine": [
            {{"name": "var1", "role": "역할", "type": "VARCHAR2"}}, 
            {{"name": "var2", "role": "역할", "type": "NUMBER"}},
        ],
        "startLine": [
            {{"name": "var1", "role": "역할", "type": "VARCHAR2"}}, 
        ],
    }}
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