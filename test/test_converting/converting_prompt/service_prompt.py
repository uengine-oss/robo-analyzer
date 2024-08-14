import json
import os
from langchain.globals import set_llm_cache
from langchain_community.cache import SQLiteCache
from langchain_openai import ChatOpenAI
from langchain_core.output_parsers import JsonOutputParser
from langchain.prompts import PromptTemplate
from langchain.schema.runnable import RunnablePassthrough
from langchain_anthropic import ChatAnthropic

# 역할 : 주어진 프로시저 코드를 기반으로 Service 클래스를 생성합니다.
# 매개변수: 
#   - clean_code : 프로시저 코드 
#   - service_code : (이전)서비스 코드
#   - variable_list : 사용된 변수 목록
#   - jpa_query_methods : 사용된 JPA 쿼리 메서드
#   - procedure_variables : command 클래스에 선언된 변수 목록
#   - spFile_Name : 스토어드 프로시저 파일 내용
# 반환값 : 서비스 클래스
# TODO statementType 부분 인지를 잘 못함 수정 필요 
db_path = os.path.join(os.path.dirname(__file__), 'langchain.db')
set_llm_cache(SQLiteCache(database_path=db_path))
llm = ChatOpenAI(model="gpt-4o")

prompt = PromptTemplate.from_template(
"""
당신은 클린 아키텍처 원칙을 따르는 스프링부트 기반의 자바 애플리케이션을 개발하는 소프트웨어 엔지니어입니다. 
주어진 Stored Procedure Code를 기반으로 Service Class의 비즈니스 로직을 생성하는 작업을 맡았습니다.


Stored Procedure Code:
{code}


Service Class Code:
{service}


Used Variable:
{variable}


Command Class Variable:
{command_variables}


context_range:
{context_range}


아래는 작업을 시작하기 전에 참고해야 할 전달된 데이터에 대한 설명입니다:
- Stored Procedure Code: Service Class의 비즈니스 로직으로 전환될 스토어드 프로시저 코드입니다.
- Used Variable: Stored Procedure Code에서 사용된 변수 목록으로, 비즈니스 로직 생성에 활용하세요.
- Command Class Variable: Command 클래스에 선언된 변수 목록으로, 비즈니스 로직 생성에 활용하세요. 
- Service Class Code: Service Class 코드로, 이 코드에 이어서 비즈니스 로직을 추가하세요.
- context_range: 스토어드 프로시저의 범위를 나타냅니다.


반드시 지켜야하는 규칙사항: 
- 모든 Entity의 이름은 복수형이 아닌 단수형으로 표현됩니다. (예: Employees -> Employee)
- 필요한 경우 'Serivce Class Code'를 참고하여, 자바 코드를 생성하세요.


'Stored Procedure Code'를 'Serivce Class Code'로 전환할 때, 아래를 참고하여 작업하세요:
1. 'SELECT', 'DELETE', 'UPDATE', 'MERGE', 'INSERT'와 같은 SQL 키워드가 식별될 때:
   - 로직에 알맞는 JPA 쿼리 메서드를 생성하여, CRUD 작업을 로직을 생성합니다.

   
2. 비즈니스 로직이 식별될 때:
   - 식별된 비즈니스 로직을 자바로 전환하고, 부가 설명이나 주석 및 다른 정보는 포함하지마세요.

   
아래는 결과 예시로, 부가 설명 없이 결과만을 포함하여, 다음 JSON 형식으로 반환하세요:
{{
   "analysis": [
      {{
         "range": {{"startLine": startLine, "endLine": endLine}},
         "code": "Service Code",
      }}
   ]
}}
""")

def convert_code(convert_sp_code, service_code, variable_list, procedure_variables, spFile_Name, context_range):
    context_range_json = json.dumps(context_range)
    procedure_variables_json = json.dumps(procedure_variables)

    chain = (
        RunnablePassthrough()
        | prompt
        | llm
        | JsonOutputParser()
    )
    result = chain.invoke({"code": convert_sp_code, "service": service_code, "variable": variable_list, "command_variables": procedure_variables_json, "projectName": spFile_Name, "context_range": context_range_json})
    return result