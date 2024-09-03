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
주어진 Stored Procedure Code를 기반으로 자바 서비스의 비즈니스 로직을 생성하는 작업을 맡았습니다.


Stored Procedure Code:
{code}


Service Class Code:
{service}


Used Variable:
{variable}


Command Class Variable:
{command_variables}


Context Range:
{context_range}


jpa_method_list:
{jpa_method_list}


** 중요 ** : 'code'는 + 연산자를 사용하지말고, 하나의 문자열로 제공하여 OutputParserException, JSONDecodeError가 발생하지 않게하세요.
 

반드시 지켜야하는 규칙사항: 
- 'Context Range'에서 주어진 범위내에 Stored Procedure Code를 'code'로 전환하세요. 
- 'Serivce Class Code'에, //Here is business logic 위치에 들어갈 비즈니스 로직만을 생성하고, 들여쓰기를 적용하여 소스 코드 형태로 주세요.
- 'Context Range' 범위는 {count}개로 총 {count}개의 결과를 생성하세요.
- 모든 변수는 이미 선언되어 있으며, 'Used Variable'를 참고하여, 변수 선언 없이, 값 초기화하는 로직만 추가하세요.


'Stored Procedure Code'를 'Serivce Class Code'로 전환할 때, 아래를 참고하여 작업하세요:
1. 'SELECT', 'DELETE', 'UPDATE', 'MERGE', 'INSERT'와 같은 SQL 키워드가 식별될 때:
   - '테이블 이름 앞에 .으로 데이터베이스 및 스키마 이름이 식별된 경우, 오로직 테이블이름만 사용하세요. 
   - 'jpa_method_list'에서 범위에 알맞는 JPA Query Method를 사용하여 CRUD로직을 생성하세요. 


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

def convert_code(convert_sp_code, service_code, variable_list, procedure_variables, context_range, count, jpa_method_list):
    context_range_json = json.dumps(context_range)
    procedure_variables_json = json.dumps(procedure_variables)

    chain = (
        RunnablePassthrough()
        | prompt
        | llm
        | JsonOutputParser()
    )
    result = chain.invoke({"code": convert_sp_code, "service": service_code, "variable": variable_list, "command_variables": procedure_variables_json, "context_range": context_range_json, "count": count, "jpa_method_list": jpa_method_list})
    return result