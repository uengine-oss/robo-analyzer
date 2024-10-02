import json
import logging
import os
import re
from langchain.globals import set_llm_cache
from langchain_community.cache import SQLiteCache
from langchain_openai import ChatOpenAI
from langchain.prompts import PromptTemplate
from langchain.schema.runnable import RunnablePassthrough
from langchain_anthropic import ChatAnthropic
from util.exception import LLMCallError
from langchain_core.output_parsers import JsonOutputParser
import pyjson5 as json5

db_path = os.path.join(os.path.dirname(__file__), 'langchain.db')
set_llm_cache(SQLiteCache(database_path=db_path))
llm = ChatAnthropic(model="claude-3-5-sonnet-20240620", max_tokens=8000)
prompt = PromptTemplate.from_template(
"""
당신은 클린 아키텍처 원칙을 따르는 스프링부트 기반의 자바 애플리케이션을 개발하는 소프트웨어 엔지니어입니다. 
주어진 Stored Procedure Code를 기반으로 간략하고 가독성 좋은 클린 코드 형태인 비즈니스 로직을 생성하는 작업을 맡았습니다.


**자바 코드 생성 시 JSON 문자열 처리 가이드라인** : 
- JSON에서는 특수 문자(예: `\n` 줄바꿈)도 이스케이프 처리가 필요합니다. 예를 들어, 줄바꿈은 `\\n`으로 표현됩니다.
- JSON 문자열을 생성할 때, 문자열 내의 따옴표는 `\\"`로 이스케이프 처리해야 합니다.
- JSON 문자열 내에서 백슬래시(`\\`)를 사용하려면 `\\\\`로 이스케이프 처리해야 합니다.
- JSON 문자열 내에서 작은따옴표(')를 사용할 때는 `\\'`로 이스케이프 처리해야 합니다.
- 문자열 연결을 위해 '+' 연산자를 사용하지 마세요. 대신 하나의 긴 문자열로 표현하세요.
- 문자열의 끝을 올바르게 인식하도록 즉, 모든 문자열이 올바르게 열리고 닫히도록 큰 따옴표에 대해서 이스케이프 처리를 하세요.
- JSON으로 파싱이 잘 되도록 적절한 이스케이프 처리를 하여 오류가 발생하지 않도록 하세요.


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


전달된 데이터에 대한 설명:
- Stored Procedure Code : 자바로 전환해야할 스토어드 프로시저 코드입니다.
- Service Class Code : 자바로 전환할 때 참고할 템플릿입니다.
- Used Variable : 현재 변수들에 할당된 값에 대한 정보로, 이전 작업의 결과입니다.
- Command Class Variable : DTO 객체 역할인 Command Class에 선언된 변수목록입니다.
- Context Range : 분석하여, 자바로 전환할 스토오드 프로시저 코드의 범위 목록입니다.
- jpa_method_list : 현재 범위에서 사용된 JPA 쿼리 메서드 목록입니다.


자바로 전환시 반드시 지켜야하는 규칙사항: 
- 'Context Range'에서 주어진 범위내 Stored Procedure Code만 자바로 전환하고, 범위를 벗어나지 않도록 주의하세요. (예 : startLine": 212, "endLine": 212이면 해당하는 라인만 자바로 전환하세요.)
- 'Serivce Class Code'에, //Here is business logic 위치에 들어갈 비즈니스 로직만을 생성하고, 들여쓰기를 적용하여 소스 코드 형태로 주세요.
- command 클래스 객체는 카멜 표기법을 사용하세요.
- 변수 선언은 하지말고, 'Used Variable'에 있는 변수에 새로운 값을 할당하도록 하세요.


'Stored Procedure Code'를 'Serivce Class Code'로 전환할 때, 아래를 참고하여 작업하세요:
1. 'SELECT', 'DELETE', 'UPDATE', 'MERGE', 'INSERT'와 같은 SQL 키워드가 식별될 때:
   - 'jpa_method_list'에서 범위에 알맞는 JPA Query Method를 사용하여 CRUD로직을 생성하세요. 
   - 'UPDATE'와 'MERGE' 같이 수정하는 작업에 대해서는 'save()' 를 필수로 진행하세요.
   - 만약  상위구문 : '1925~1977', 하위 구문 : '1942~1977' 범위가 있을 때, 상위 구문이 'INSERT INTO SELECT FROM' 영역이고, 'SELECT FROM' 영역이 하위구문이면, 하위 구문 범위만 정확하게 자바로 전환하세요. 즉, 데이터를 찾는 로직만 포함하세요. 'UPDATE'나 'MERGE' 구문 또한 SELECT 구문을 포함하고 있을 경우, 똑같이 진행하세요.

  
2. 비즈니스 로직이 식별될 때:
   - 식별된 비즈니스 로직을 자바로 전환하고, 부가 설명이나 주석 및 다른 정보는 포함하지마세요.


** 'analysis' 결과를 생성시 아래 지시사항을 반드시 숙지하세요 ** :
- 'Context Range' 범위는 {count}개로, 'code'는 총 {count}개의 자바 코드를 가져야 합니다. 모든 범위에 대해서 자바 코드로 전환해야하며, 범위가 중복되거나 겹치더라도, 생략하지 말고, 각각의 범위에 맞는 자바 코드가 생성되어야 합니다.
- 'Used Variable'의 모든 변수들에 대해서 할당된 값에 대한 상세한 정보를 Stored Procedure Code를 분석하여, 추적 및 업데이트하고, 그 결과를 JSON 응답의 'variables' 부분에 포함시켜야 합니다. (예 : 동적 SQL 구성을 위해 "SELECT FROM users WHERE 1=1"를 할당. 이후 검색 조건에 따라 AND 절 추가 예정)
- 'Command Class Variable' 변수들의 저장된 값은 Dto 객체에서 얻은 값으로 고정하고, 역할은 분석하여 할당하세요.


예시 : {{startLine : 415, endLine: 478}}, {{startLine : 435, endLine: 478}}인 범위가 있을 경우, 큰 범위가 작은 범위를 포함하고 있는데, 이런 경우에도 각각의 범위에 해당하는 'Stored Procedure Code'만 자바로 전환하고, 그 결과를 JSON 응답의 'code' 부분에 포함시켜야 합니다. 


아래는 결과 예시로, 부가 설명 없이 결과만을 포함하여, 다음 JSON 형식으로 반환하세요.('analysis' 결과는 반드시 리스트가 아닌 dictionary(사전) 형태여야 합니다.):
{{
   "analysis": {{
      "code": {{
         "startLine~endLine": "Service Code",
         "startLine~endLine": "Service Code"
      }},
      "variables": {{
         "name": "initialized value and role",
         "name": "initialized value and role"
      }}
   }}
}}
""")


# 역할 : 주어진 프로시저 코드를 기반으로 Service 클래스 코드를 생성합니다.
# 매개변수: 
#  - clean_code : 스토어드 프로시저 코드 
#  - service_skeleton : 서비스 스켈레톤
#  - variable_list : 사용된 변수 목록
#  - jpa_query_methods : 사용된 JPA 쿼리 메서드
#  - procedure_variables : command 클래스에 선언된 변수 목록
#  - context_range: 분석할 범위
#  - count : 분석할 범위 개수
# 반환값 : 
#  - json_parsed_content : 서비스 클래스를 생성하기 위한 정보
# TODO 토큰 초과시 로직 추가 필요
def convert_service_code(convert_sp_code, service_skeleton, variable_list, procedure_variables, context_range, count, jpa_method_list):
   
   try:  
      context_range_json = json.dumps(context_range)
      procedure_variables_json = json.dumps(procedure_variables)

      chain = (
         RunnablePassthrough()
         | prompt
         | llm
      )
      result = chain.invoke({"code": convert_sp_code, "service": service_skeleton, "variable": variable_list, "command_variables": procedure_variables_json, "context_range": context_range_json, "count": count, "jpa_method_list": jpa_method_list})
      # TODO 여기서 최대 토큰이 4096이 넘은 경우 처리가 필요
      logging.info(f"토큰 수: {result.usage_metadata}") 
      output_tokens = result.usage_metadata['output_tokens']
      if output_tokens > 4096:
         logging.warning(f"출력 토큰 수가 4096을 초과했습니다: {output_tokens}")

      json_parsed_content = json5.loads(result.content)
      return json_parsed_content

   except Exception:
      err_msg = "(전처리) 서비스 코드 생성 과정에서 LLM 호출하는 도중 오류가 발생했습니다."
      logging.exception(err_msg)
      raise LLMCallError(err_msg)