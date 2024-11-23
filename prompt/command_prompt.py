import json
import logging
import os
from langchain.globals import set_llm_cache
from langchain_community.cache import SQLiteCache
from langchain_openai import ChatOpenAI
from langchain_core.output_parsers import JsonOutputParser
from langchain.prompts import PromptTemplate
from langchain.schema.runnable import RunnablePassthrough
from util.exception import LLMCallError


db_path = os.path.join(os.path.dirname(__file__), 'langchain.db')
set_llm_cache(SQLiteCache(database_path=db_path))
llm = ChatOpenAI(model_name="gpt-4o")
prompt = PromptTemplate.from_template(
"""
당신은 PL/SQL 프로시저를 스프링부트 애플리케이션으로 마이그레이션하는 전문가입니다.
주어진 JSON 데이터에서 'parameters'와 'procedure_name' 정보를 활용하여 Command 클래스를 생성합니다.


[입력 데이터 구조 설명]
===============================================
입력되는 JSON 데이터는 다음 구조를 가집니다:
{command_class_data}

- parameters: 프로시저의 입력 파라미터 목록
- procedure_name: 프로시저 이름


[SECTION 1] Command 클래스 생성 규칙
===============================================
1. 기본 구조
   - JSON의 'procedure_name' 값을 사용하여 클래스명 생성
   - 형식: {{procedure_name}}Command
   - 예시) procedure_name이 'UPDATE_EMPLOYEE'인 경우 -> UpdateEmployeeCommand
   - 첫 글자는 대문자로 변환 필수

2. 필드 생성 규칙
   - JSON의 'parameters' 배열의 각 항목을 필드로 변환
   - 접근제한자: private 필수
   - 명명규칙: 카멜케이스 (첫 글자 소문자)
   - 데이터 타입 매핑:
     * NUMBER, NUMERIC -> Long
     * VARCHAR, VARCHAR2, CHAR -> String
     * DATE -> LocalDate
     * TIMESTAMP -> LocalDateTime
     * ROWTYPE이 포함된 타입 (예: TABLE_NAME.ROWTYPE) -> Object
     * 복합 데이터 타입이나 사용자 정의 타입 -> Object
   - 모든 필드에 대해 Lombok @Getter @Setter 사용
   - ROWTYPE이나 복합 데이터 타입의 경우 @ToDo 어노테이션으로 원본 타입 정보를 주석 처리


3. import 추가 규칙
   - 필요한 import문은 다음과 같습니다.
     * java.time.LocalDate (날짜 타입 사용시)
     * java.time.LocalDateTime (타임스탬프 사용시)
     * lombok.Getter
     * lombok.Setter
   - 추가적인 import문은 필요에 따라 작성하세요.



[SECTION 2] Command 클래스 예시
===============================================
예시:
package com.example.demo.command;
import java.time.LocalDate;
import lombok.Getter;
import lombok.Setter;

@Getter
@Setter
public class ExampleCommand {{
    private Long id;
    private String name;
    private LocalDate date;
    @ToDo("Original Type: EMPLOYEE_TABLE.ROWTYPE")
    private Object employeeRow;
}}


[SECTION 3] 출력 형식
===============================================
부가 설명 없이 결과만을 포함하여, 다음 JSON 형식으로 반환하세요:
{{
    "commandName": "Command Class Name",
    "command": "Command Java Code",
    "command_class_variable": [
        "Command Class에 선언된 모든 변수들을 '타입:이름' 형태로 채워넣으세요."
    ]
}}
"""
)

# 역할 : 프로시저 노드 데이터를 기반으로, 커맨드 클래스를 생성합니다.
# 매개변수: 
#   - command_class_data : 커맨드 클래스 생성에 필요한 데이터
#   - object_name : 패키지 및 프로시저 이름
# 반환값 : 
#   - result : Command 클래스
def convert_command_code(command_class_data, object_name):
    
    try:
        command_class_data = json.dumps(command_class_data)

        chain = (
            RunnablePassthrough()
            | prompt
            | llm
            | JsonOutputParser()
        )
        result = chain.invoke({"command_class_data": command_class_data, "object_name": object_name})
        return result
    except Exception:
        err_msg = "Command 생성 과정에서 LLM 호출하는 도중 오류가 발생했습니다."
        logging.error(err_msg, exc_info=False)
        raise LLMCallError(err_msg)