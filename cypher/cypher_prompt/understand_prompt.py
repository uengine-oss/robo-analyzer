import json
import os
from langchain.globals import set_llm_cache
from langchain_community.cache import SQLiteCache
from langchain_openai import ChatOpenAI
from langchain_core.output_parsers import JsonOutputParser
from langchain.prompts import PromptTemplate
from langchain.schema.runnable import RunnablePassthrough

# 역할 : 주어진 PLSQL 파일을 분석하여, 필요한 정보를 추출합니다
# 매개변수: 
#      - code: 분석할 스토어드 프로시저 코드 
#      - ontext_ranges : 분석할 스토어드 프로시저 코드의 범위 
#      - context_range_count : 분석할 스토어드 프로시저 범위의 개수(결과 개수 유지를 위함)
# 반환값 : 추후에 분석에 사용되는 필요한 정보(변수, 테이블, 테이블 필드.. 등등)
db_path = os.path.join(os.path.dirname(__file__), 'langchain.db')
set_llm_cache(SQLiteCache(database_path=db_path))

llm = ChatOpenAI(model_name="gpt-4o")

prompt = PromptTemplate.from_template(
"""
당신은 Oracle PLSQL 전문가입니다. 주어진 Stored Procedure Code를 철저히 분석하세요.


분석할 Stored Procedure Code:
{code}


분석할 Stored Procedure Code의 범위 목록:
{ranges}


분석할 Stored Procedure Code의 범위 개수는 {count}개입니다. 정확히 {count}개의 'analysis' 항목을 생성해야 합니다.


각 지정된 범위의 코드에 대해 다음 정보를 추출하세요:
- 'range': 분석할 스토어드 프로시저의 정확한 범위
- 'summary': 해당 범위 코드의 핵심 기능을 간결하게 요약 (한 문장으로)
- 'Tables': 사용된 모든 테이블 이름과 그 필드(속성)
- 'tableReference': 테이블 간의 참조 관계
- 'variable': 사용된 변수, 데이터 타입, 주요 역할
- 'primaryKey': 식별된 테이블의 기본키
** 주의: 식별되지 않는 정보는 빈 배열 [] 또는 빈 문자열 ""로 표시하세요. 추측이나 부가 설명은 피하세요.** 


세부 지침:
1. 'range'와 'summary' 정보를 추출하기 위한 지침사항:
   - 모든 제공된 범위에 대해 'analysis' 항목을 생성하세요.
   - 각 범위의 코드 내용을 정확하고 간결하게 요약하세요.


2. 'Tables', 'tableReferense', 'primaryKey' 정보를 추출하기 위한 지침사항:
   - SQL CRUD 문에서 정확한 테이블 이름을 식별하고 나열하세요. 이때 별칭은 제외하고 실제 테이블 이름만 사용하세요.
   - SQL CRUD 문에서 사용된 테이블의 모든 속성(필드)을 적절한 데이터 타입으로 식별하세요.
   - SQL CRUD 문에서 여러 테이블 간의 외래 키 관계를 'source'와 'target' 형태로 표현합니다. (WHERE 절에서 다른 테이블의 필드로 조인하는 경우, 이를 외래 키 관계로 간주하세요.)
   - SQL CRUD 문에서 테이블의 primaryKey를 식별하세요.


3. 'variable' 정보를 추출하기 위한 지침사항:
   - 코드 내에서 변수로 추론될 수 있는 모든 요소를 식별하고, 각 변수의 데이터 타입과 주요 역할을 간결히 설명하세요.
   - 테이블 속성(필드)와 일반 변수를 명확히 구분하세요.


아래 결과는 예시로, 다음 JSON 형식으로 제공하고, 추가 설명은 포함하지 마세요:
{{
    "analysis": [
        {
            "range": {{"startLine": 1, "endLine": 10}},
            "summary": "사용자 정보를 조회하고 업데이트하는 프로시저",
            "Tables": [
                {{"USER_INFO": ["USER_ID", "USER_NAME", "EMAIL", "CREATED_DATE"]}},
                {{"ORDER_HISTORY": ["ORDER_ID", "USER_ID", "ORDER_DATE", "TOTAL_AMOUNT"]}}
            ],
            "tableReference": [{{"source": "ORDER_HISTORY", "target": "USER_INFO"}}],
            "variable": [
                {{"name": "v_user_id", "type": "NUMBER", "role": "사용자 ID 저장"}},
                {{"name": "v_total_orders", "type": "NUMBER", "role": "사용자의 총 주문 수 계산"}}
            ],
            "primaryKey": [
                {{"name": "v_user_id", "type": "NUMBER", "table": "USER_INFO"}},
                {{"name": "v_total_orders", "type": "NUMBER", "table": "ORDER_HISTORY"}},
            ]
        }
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