import json
import os
from langchain.globals import set_llm_cache
from langchain_community.cache import SQLiteCache
from langchain_openai import ChatOpenAI
from langchain_core.output_parsers import JsonOutputParser
from langchain.prompts import PromptTemplate
from langchain.schema.runnable import RunnablePassthrough

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

llm = ChatOpenAI(model_name="gpt-4o")

prompt = PromptTemplate.from_template(
"""
당신은 클린 아키텍처 원칙을 따르는 스프링부트 기반의 자바 애플리케이션을 개발하는 소프트웨어 엔지니어입니다. 
주어진 Stored Procedure Code를 기반으로 자바 Service 클래스를 생성하는 작업을 맡았습니다.


Stored Procedure Code:
{code}


Service Class Code:
{service}


Used Variable :
{variable}


Command Class Variable :
{command_variables}


Used Jpa Query Methods :
{jpa_query_methods}


아래는 작업을 시작하기 전에 참고해야 할 전달된 데이터에 대한 설명입니다:
- Stored Procedure Code: 분석할 스토어드 프로시저 코드입니다. 이를 Service Class의 비즈니스 로직으로 변환하세요.
- Used Variable: Stored Procedure Code에서 사용된 변수 목록입니다. 이 변수들을 비즈니스 로직 생성에 활용하세요.
- Command Class Variable: Command 클래스에 선언된 변수 목록입니다. 이 변수들을 비즈니스 로직 생성에 활용하세요. 
- Used Jpa Query Methods: Stored Procedure Code에서 사용된 JPA 쿼리 메서드 목록입니다. 이 목록에서만 필요한 메서드를 선택하여 사용하세요.
- Service Class Code: 기존의 Service Class 코드입니다. 이 코드에 이어서 비즈니스 로직을 추가하세요.


중요: ** 모든 Entity의 이름은 복수형이 아닌 단수형으로 표현됩니다. (예: Employees -> Employee) **


Stored Procedure Code를 Service 클래스로 전환할 때, 아래를 참고하여 작업하세요:
1. 'SELECT', 'DELETE', 'UPDATE', 'MERGE, 'INSERT'd와 같은 SQL 키워드가 식별될 때:
   - 오로직 Used Jpa Query Methods에 있는 JPA 쿼리 메서드만을 선택하여, CRUD 작업을 로직을 Serivce Class Code에 이어서 추가합니다.

   
2. 일반적인 비즈니스 로직이 식별될 때:
   - 식별된 비즈니스 로직만을 Serivce Class Code에 이어서 코드를 추가하고, 부가 설명이나 주석 및 다른 정보는 포함하지마세요.

   
3. 전달된 Service Class Code가 있을 경우:
   - Service Class Code를 유지한 채로, 새로운 로직만 추가하거나 필요한 경우 기존 로직을 수정하세요. 단, 변수 선언, import, package 등의 선언부는 그대로 유지하세요.
   - getter setter를 사용할 때, 예시 처럼 메서드의 이름을 표현하도록 하세요. (예: private long pEmployeeId -> command.getPEmployeeId())
   

4. import 선인이 필요한 경우:
   - Entity Class에 대한 import가 없을 경우, 새로 생성하세요. (형식: com.example.{projectName}.entity.EntityName(실제 Entity의 이름으로 대체하세요.))
   - Repository Interface에 대한 import가 없을 경우, 새로 생성하세요. (형식: com.example.{projectName}.repository.EntityName+Repository(실제 Repository Interface의 이름으로 대체하세요.))
   (예 : com.example.{projectName}.entity.Employee, com.example.{projectName}.entity.EmployeeRepository )

   
5. 변수의 타입이 맞지 않을 경우:
   - 명시적으로 캐스팅을 진행하세요. (예 : overtimePay = (long) (overtimeHours * (baseSalary / 160) * overtimeRate);) 


   
아래는 결과 예시로, 부가 설명 없이 결과만을 포함하여, 다음 JSON 형식으로 반환하세요:
{{
    "name": "Service Class Name",
    "code": "Service Code"
}}
""")

def convert_code(clean_code, service_code, variable_list, jpa_query_methods, procedure_variables, spFile_Name):
    variable_json = json.dumps(variable_list)
    jpa_query_methods_json = json.dumps(jpa_query_methods)
    procedure_variables_json = json.dumps(procedure_variables)

    chain = (
        RunnablePassthrough()
        | prompt
        | llm
        | JsonOutputParser()
    )
    result = chain.invoke({"code": clean_code, "service": service_code, "variable": variable_json, "jpa_query_methods": jpa_query_methods_json, "command_variables": procedure_variables_json, "projectName": spFile_Name})
    return result