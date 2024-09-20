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
주어진 Stored Procedure Code를 기반으로 자바 서비스의 비즈니스 로직을 생성하는 작업을 맡았습니다.


**결과를 생성시 반드시 지켜야할 사항** : 
- JSON에서는 특수 문자(예: `\n` 줄바꿈)도 이스케이프 처리가 필요합니다. 예를 들어, 줄바꿈은 `\\n`으로 표현됩니다.
- JSON 문자열을 생성할 때, 문자열 내의 따옴표는 `\\"`로 이스케이프 처리해야 합니다.
- JSON 문자열 내에서 백슬래시(`\\`)를 사용하려면 `\\\\`로 이스케이프 처리해야 합니다.


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


반드시 지켜야하는 규칙사항: 
- 자바로 전환시, 비즈니스 로직은 최대한 간략하고 가독성 좋은 클린 코드 형태로 주세요.
- 'Context Range'에서 주어진 범위내 Stored Procedure Code만 자바로 전환하고, 범위를 벗어나지 않도록 주의하세요. (예 : startLine": 212, "endLine": 212이면 해당하는 라인만 자바로 전환하세요.)
- 'Serivce Class Code'에, //Here is business logic 위치에 들어갈 비즈니스 로직만을 생성하고, 들여쓰기를 적용하여 소스 코드 형태로 주세요.
- 절대로 변수 선언은 하지말고, 'Used Variable'에 있는 변수를 사용하여 값만 대입하세요. 카멜 표기법을 사용 (예 : baseSalary = employee.getBaseSalary();)
- 'Context Range' 범위는 {count}개로 총 {count}개의 'analysis'를 생성하세요.
- 숫자 관련은 전부 'long' 타입을 쓰도록 하고, command 클래스 객체의 대소문자를 주의하세요. (카멜 표기법)


'Stored Procedure Code'를 'Serivce Class Code'로 전환할 때, 아래를 참고하여 작업하세요:
1. 'SELECT', 'DELETE', 'UPDATE', 'MERGE', 'INSERT'와 같은 SQL 키워드가 식별될 때:
   - 'jpa_method_list'에서 범위에 알맞는 JPA Query Method를 사용하여 CRUD로직을 생성하세요. 
   - UPDATE와 MERGE 같이 수정하는 작업에 대해서는 'save()' 를 필수로 진행하세요.

   
2. 비즈니스 로직이 식별될 때:
   - 식별된 비즈니스 로직을 자바로 전환하고, 부가 설명이나 주석 및 다른 정보는 포함하지마세요.
   

** 잊지마세요 반드시 이스케이프처리를 해야합니다. **

   
아래는 결과 예시로, 부가 설명 없이 결과만을 포함하여, 다음 JSON 형식으로 반환하세요:
{{
   "analysis": [
      {{
         "startLine~endLine": "Service Code",
         "startLine~endLine": "Service Code"
      }}
   ]
}}
""")

def preprocess_json(json_str):
   def escape_code(code):
      return json.dumps(code)[1:-1]  # JSON 인코딩 후 따옴표 제거

   pattern = r'"(\d+~\d+)"\s*:\s*"((?:\\.|[^"\\])*)"'

   matches = re.findall(pattern, json_str)

   # 결과 출력
   for match in matches:
      print(f"키: {match[0]}")
      print(f"값: {match[1]}")
      print("-" * 50)

   processed = re.sub(pattern, lambda m: f'"{m.group(1)}": "{escape_code(m.group(2))}"', json_str)
   return processed


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
#  - result : 서비스 클래스 코드
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
      data = json5.loads(result.content)
      # processed_json = preprocess_json(result.content)
      
      transform_result = {
         "content": data,
         "usage_metadata": result.usage_metadata
      }      
      return transform_result
    
   except Exception:
      err_msg = "(전처리) 서비스 코드 생성 과정에서 LLM 호출하는 도중 오류가 발생했습니다."
      logging.exception(err_msg)
      raise LLMCallError(err_msg)