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

# TODO 일단 기본키 외래키 같은것들도 정보에 추가해야하고, 프롬포트로 수정 필요(기본키 외래키가 식별될 경우에만 추가하도록)
db_path = os.path.join(os.path.dirname(__file__), 'langchain.db')
set_llm_cache(SQLiteCache(database_path=db_path))

prompt = PromptTemplate.from_template(
"""
당신은 PostgreSQL(PL/pgSQL) 전문가입니다. 주어진 함수/프로시저 코드를 철저히 분석하세요.


분석할 패키지(스키마마) 이름:
{schema_name}


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
1. 코드의 역할과 동작을 상세하게 설명하세요:
   - 주어진 코드 범위의 전체 맥락을 파악하여 다음 내용을 포함하여 설명하세요:
   - 해당 코드가 속한 프로시저 이름을 반드시 명시
   - 각 변수 할당의 목적과 의미를 설명 (예: "vcount에 10을 할당하여 최대 반복 횟수를 설정")
   - 조건문(IF, CASE 등)의 판단 기준과 각 분기의 목적을 설명
   - 반복문(FOR, WHILE 등)의 반복 조건과 수행 목적을 설명
   - SQL 작업(INSERT/UPDATE/DELETE/SELECT)의 대상 테이블과 처리 목적을 설명
   - 해당 코드 범위가 전체 프로시저에서 수행하는 역할과 목적을 설명
   예시) "v_process_date에 현재 날짜를 할당하여 처리 기준일을 설정하고, v_count가 임계값(10)을 초과하는지 확인하여 처리량을 제한합니다. CUSTOMER 테이블에서 활성 고객만을 SELECT하여, ORDER_HISTORY 테이블에 집계 데이터를 생성합니다."

2. 각 범위에서 사용된 모든 변수들을 식별하세요. 변수는 다음과 같은 유형을 모두 포함합니다:
   - 일반 변수 (보통 'v_', 'p_', 'i_', 'o_' 접두사)
   - %ROWTYPE 변수
   - %TYPE 변수
   
   주의사항:
   - 각 범위는 독립적으로 처리되어야 하며, 다른 범위와 중첩되더라도 해당 범위 내에서 직접 사용된 변수만 포함합니다.
   - 예를 들어, 223~250 라인과 240~241 라인이 중첩된 경우, 각각의 범위에서 실제로 사용된 변수만 독립적으로 식별합니다.
   - 상수나 열거형 값은 변수로 식별하지 않습니다.

3. 코드 내에서 프로시저, 패키지, 함수 호출을 식별하세요:
   - 외부 패키지의 호출: 'PACKAGE_NAME.PROCEDURE_NAME' 형식으로 저장
   - 현재 패키지 내부 호출: 'PROCEDURE_NAME' 형식으로 저장
   - 시퀀스 객체의 NEXTVAL, CURRVAL 참조는 프로시저/함수 호출로 식별하지 마세요
   - 모든 호출을 'calls' 배열에 저장하세요.

4. 코드 내에서 사용된 테이블 식별하세요:
  - 'INSERT INTO', 'MERGE INTO', 'FROM', 'UPDATE' 절 이후에 나오는 테이블의 전체 이름을 'tableNames'로 반환하세요.
  - TPJ_ 같은 접두어를 유지한 채 테이블의 풀 네임을 반환하세요.


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

# 역할: PL/SQL 프로시저 코드를 심층 분석하여 Neo4j 사이퍼 쿼리 생성에 필요한 정보를 추출하는 함수입니다.
#
# 매개변수: 
#   - sp_code : 분석 대상 PL/SQL 프로시저의 전체 코드
#   - context_ranges : 분석이 필요한 코드 범위 목록
#   - context_range_count : 분석해야 할 코드 범위의 총 개수
#   - object_name : 패키지지 이름
#   - api_key : OpenAI API 키
#
# 반환값: 
#   - parsed_content : LLM의 코드 분석 결과
#      (테이블 관계, 변수 정보, 프로시저 호출 관계 등이 포함된 구조화된 데이터)
def understand_code(sp_code, context_ranges, context_range_count, object_name, api_key):
    try:
        ranges_json = json.dumps(context_ranges)
        
        # 전달받은 API 키로 Anthropic Claude LLM 인스턴스 생성
        llm = ChatAnthropic(
            model="claude-3-7-sonnet-latest", 
            temperature=0,
            max_tokens=8192,
            api_key=api_key
        )
        
        chain = (
            RunnablePassthrough()
            | prompt
            | llm
        )

        json_parser = JsonOutputParser()
        # TODO 여기서 최대 출력 토큰만 4096이 넘은 경우 처리가 필요
        result = chain.invoke({"code": sp_code, "ranges": ranges_json, "count": context_range_count, "schema_name": object_name})
        json_parsed_content = json_parser.parse(result.content)
        logging.info(f"토큰 수: {result.usage_metadata}")     
        return json_parsed_content
    
    except Exception as e:
        err_msg = f"Understanding 과정에서 분석 관련 LLM 호출하는 도중 오류가 발생했습니다: {str(e)}"
        logging.error(err_msg)
        raise LLMCallError(err_msg)