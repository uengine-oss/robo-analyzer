import json
import logging
import os
from langchain.globals import set_llm_cache
from langchain_community.cache import SQLiteCache
from langchain_openai import ChatOpenAI
from langchain_anthropic import ChatAnthropic
from langchain_core.output_parsers import JsonOutputParser
from langchain.prompts import PromptTemplate
from langchain.schema.runnable import RunnablePassthrough
from util.exception import LLMCallError


db_path = os.path.join(os.path.dirname(__file__), 'langchain.db')
set_llm_cache(SQLiteCache(database_path=db_path))
llm = ChatAnthropic(model="claude-3-5-sonnet-20241022", max_tokens=8000, temperature=0.1)

prompt = PromptTemplate.from_template(
"""
당신은 Java와 PL/SQL 테스트 데이터 생성 전문가입니다. Command 클래스와 PL/SQL 프로시저의 입력 매개변수를 분석하여 적절한 테스트 데이터를 생성해야 합니다.

[입력 정보]
===============================================
1. Java Command 클래스 필드 정보:
{command_class}

2. PL/SQL 프로시저 매개변수 정보:
{procedure_params}

[요구사항]
===============================================
1. Java Command 클래스와 PL/SQL 프로시저에서 사용되는 동일한 의미의 데이터는 같은 값을 가져야 합니다.
2. 데이터 생성 규칙:
   - String: 의미 있는 실제 데이터 (예: 이름이면 "홍길동", 주소면 "서울시 강남구")
   - Integer/Long: 업무 도메인에 맞는 현실적인 값
   - Date/LocalDate: 현재 날짜 기준 최근 데이터
   - Boolean: true/false 중 업무 맥락에 맞는 값
   - Enum: 해당 Enum의 실제 가능한 값
   - List/Array: 최소 2개 이상의 의미 있는 데이터
   - BigDecimal: 금액의 경우 현실적인 범위 내의 값
   - NUMBER(PL/SQL): Java의 Integer/Long/BigDecimal에 맞게 변환
   

[예시]
===============================================
입력된 필드가 다음과 같을 경우:

Java Command 클래스:
private String employeeName;
private Integer age;
private BigDecimal salary;

PL/SQL 프로시저:
PROCEDURE calculate_salary(
    p_emp_name IN VARCHAR2,
    p_age IN NUMBER,
    p_salary IN NUMBER
)

출력:
{{
    "testData": [
        {{
            "fieldName": "employeeName",
            "javaType": "String",
            "plsqlParam": "p_emp_name",
            "plsqlType": "VARCHAR2",
            "value": "김철수"
        }},
        {{
            "fieldName": "age",
            "javaType": "Integer",
            "plsqlParam": "p_age",
            "plsqlType": "NUMBER",
            "value": "35"
        }},
        {{
            "fieldName": "salary",
            "javaType": "BigDecimal",
            "plsqlParam": "p_salary",
            "plsqlType": "NUMBER",
            "value": "45000000"
        }}
    ]
}}


[출력 형식]
===============================================
부가 설명이나 주석 없이 다음 JSON 형식으로만 반환:
{{
    "testData": [
        {{
            "fieldName": "Java 필드명",
            "javaType": "Java 타입",
            "plsqlParam": "PL/SQL 매개변수명",
            "plsqlType": "PL/SQL 타입",
            "value": "생성된 테스트 값"
        }}
    ]
}}
"""
)

# 역할: Command 클래스의 필드 정보를 분석하여 테스트에 사용할 적절한 데이터를 생성하는 함수입니다.
#      LLM을 통해 각 필드의 타입과 의미를 분석하여 현실적이고 의미 있는 테스트 데이터를 생성합니다.
# 매개변수: 
#   - command_class : Java Command 클래스의 필드 정보
# 반환값: 
#   - result : 생성된 테스트 데이터 정보 (필드명, 타입, 값, 설명을 포함한 JSON)
def generate_given_parameters(command_class: str, parameters: str):
    try:
        chain = (
            RunnablePassthrough()
            | prompt
            | llm
            | JsonOutputParser()
        )
        result = chain.invoke({
            "command_class": command_class,
            "parameters": parameters
        })
        return result
    except Exception:
        err_msg = "테스트 데이터 생성 과정에서 LLM 호출 중 오류가 발생했습니다."
        logging.error(err_msg, exc_info=False)
        raise LLMCallError(err_msg)