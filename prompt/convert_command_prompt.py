import json
import logging
import os
from langchain.globals import set_llm_cache
from langchain_community.cache import SQLiteCache
from langchain_anthropic import ChatAnthropic
from langchain_core.output_parsers import JsonOutputParser
from langchain.prompts import PromptTemplate
from langchain.schema.runnable import RunnablePassthrough
from util.exception import LLMCallError


db_path = os.path.join(os.path.dirname(__file__), 'langchain.db')
set_llm_cache(SQLiteCache(database_path=db_path))
llm = ChatAnthropic(model="claude-3-7-sonnet-20250219", max_tokens=8000, temperature=0.1)
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

{dir_name}

- dir_name: 클래스가 저장될 디렉토리 이름(import문에 사용)


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
     * NUMBER, NUMERIC, INTEGER, DECIMAL, BIGDECIMAL 등 모든 숫자 타입 -> 반드시 Long만 사용 (INTEGER나 BIGDECIMAL 사용 금지)
     * VARCHAR, VARCHAR2, CHAR -> String
     * DATE -> LocalDate
     * TIMESTAMP -> LocalDateTime
     * 테이블 이름의 경우 테이블 명을 타입으로 사용하세요. (엔티티 클래스를 타입으로 설정)
    - 'parameters'에 없는 변수는 절대 생성하지 않음

3. import 추가 규칙
   - Command 클래스 예시에 있는 import문들은 반드시 추가하세요.
   - 필요한 import문은 다음과 같습니다.
     * java.time.*  
     * lombok.Getter
     * lombok.Setter
     * com.example.demo.entity.EntityName
   - 추가적인 import문은 반드시 필요에 따라 작성하세요.


[SECTION 2] Command 클래스 예시
===============================================
예시:
package com.example.demo.command.{dir_name};
import java.time.*;
import lombok.Getter;
import lombok.Setter;
import com.example.demo.entity.*;

@Getter
@Setter

public class ExampleCommand {{

    private Long id;
    private String name;
    private LocalDate date;
    private LocalDateTime time;
    private Employee employee;
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

# 역할: PL/SQL 프로시저의 파라미터 정보를 기반으로 Java Command 클래스를 생성하는 함수입니다.
#      LLM을 통해 프로시저의 입력 파라미터들을 Java 데이터 타입으로 변환하고,
#      Getter/Setter가 포함된 완성된 Command 클래스를 생성합니다.
# 매개변수: 
#   - command_class_data : Command 클래스 생성에 필요한 데이터
#   - dir_name : 클래스가 저장될 디렉토리 이름
# 반환값: 
#   - result : LLM이 생성한 Command 클래스 정보
def convert_command_code(command_class_data, dir_name):
    
    try:
        command_class_data = json.dumps(command_class_data, ensure_ascii=False, indent=2)

        chain = (
            RunnablePassthrough()
            | prompt
            | llm
            | JsonOutputParser()
        )
        result = chain.invoke({"command_class_data": command_class_data, "dir_name": dir_name})
        return result
    except Exception as e:
        err_msg = f"Command 생성 과정에서 LLM 호출하는 도중 오류가 발생했습니다: {str(e)}"
        logging.error(err_msg)
        raise LLMCallError(err_msg)
    



prompt = PromptTemplate.from_template(
"""
당신은 PL/SQL 프로시저를 파이썬 애플리케이션으로 마이그레이션하는 전문가입니다.
주어진 JSON 데이터에서 'parameters'와 'procedure_name' 정보를 활용하여 Command 클래스를 생성합니다.


[입력 데이터 구조 설명]
===============================================
입력되는 JSON 데이터는 다음 구조를 가집니다:
{command_class_data}

- parameters: 프로시저의 입력 파라미터 목록
- procedure_name: 프로시저 이름

{dir_name}

- dir_name: 클래스가 저장될 디렉토리 이름(import문에 사용)


[SECTION 1] Command 클래스 생성 규칙
===============================================
1. 기본 구조
   - JSON의 'procedure_name' 값을 사용하여 클래스명 생성
   - 형식: {{procedure_name}}Command
   - 예시) procedure_name이 'UPDATE_EMPLOYEE'인 경우 -> UpdateEmployeeCommand
   - 첫 글자는 대문자로 변환 필수

2. 필드 생성 규칙
   - JSON의 'parameters' 배열의 각 항목을 필드로 변환
   - 명명규칙: 스네이크 케이스 (첫 글자 소문자)
   - 데이터 타입 매핑:
     * NUMBER, NUMERIC, INTEGER, DECIMAL, BIGDECIMAL 등 모든 숫자 타입 -> 반드시 int만 사용
     * VARCHAR, VARCHAR2, CHAR -> str
     * DATE -> date
     * TIMESTAMP -> datetime
     * 테이블 이름의 경우 테이블 명을 타입으로 사용하세요. (엔티티 클래스를 타입으로 설정)
    - 'parameters'에 없는 변수는 절대 생성하지 않음

3. import 추가 규칙
   - Command 클래스 예시에 있는 import문들은 반드시 추가하세요.
   - 필요한 import문은 다음과 같습니다.
     * from dataclasses import dataclass
     * from datetime import date, datetime
     * from app.entity.EntityName import EntityName
   - 추가적인 import문은 반드시 필요에 따라 작성하세요.


[SECTION 2] Command 클래스 예시
===============================================
예시:
from dataclasses import dataclass
from datetime import date, datetime
from app.entity.{dir_name} import Employee

@dataclass
class ExampleCommand:
    id: int
    name: str
    date_value: date
    time_value: datetime
    employee: Employee


[SECTION 3] 출력 형식
===============================================
부가 설명 없이 결과만을 포함하여, 다음 JSON 형식으로 반환하세요:
{{
    "commandName": "Command Class Name",
    "command": "Command Python Code",
    "command_class_variable": [
        "Command Class에 선언된 모든 변수들을 '타입:이름' 형태로 채워넣으세요."
    ]
}}
"""
)

# 역할: PL/SQL 프로시저의 파라미터 정보를 기반으로 Python Command 클래스를 생성하는 함수입니다.
#      LLM을 통해 프로시저의 입력 파라미터들을 Python 데이터 타입으로 변환하고,
#      dataclass로 완성된 Command 클래스를 생성합니다.
# 매개변수: 
#   - command_class_data : Command 클래스 생성에 필요한 데이터
#   - dir_name : 클래스가 저장될 디렉토리 이름
# 반환값: 
#   - result : LLM이 생성한 Command 클래스 정보
def convert_command_code_python(command_class_data, dir_name):
    
    try:
        command_class_data = json.dumps(command_class_data, ensure_ascii=False, indent=2)

        chain = (
            RunnablePassthrough()
            | prompt
            | llm
            | JsonOutputParser()
        )
        result = chain.invoke({"command_class_data": command_class_data, "dir_name": dir_name})
        return result
    except Exception as e:
        err_msg = f"Command 생성 과정에서 LLM 호출하는 도중 오류가 발생했습니다: {str(e)}"
        logging.error(err_msg)
        raise LLMCallError(err_msg)