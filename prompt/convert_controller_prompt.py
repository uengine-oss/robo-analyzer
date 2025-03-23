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
llm = ChatAnthropic(model="claude-3-7-sonnet-20250219", max_tokens=8000, temperature=0.0)

prompt = PromptTemplate.from_template(
"""
당신은 스프링부트 컨트롤러 메서드를 생성하는 전문가입니다.
주어진 데이터를 기반으로 컨트롤러 메서드를 생성합니다.


[입력 데이터 구조 설명]
===============================================
method_signature: 서비스 클래스에 정의된 메서드의 시그니처
{method_signature}

procedure_name : 원본 PL/SQL의 프로시저/함수 이름
{procedure_name}

command_class_variable: Command DTO 클래스에 정의된 필드 목록으로 메서드 호출 시 필요한 파라미터 추출에 사용
{command_class_variable}

command_class_name: Command 클래스의 이름
{command_class_name}

controller_skeleton: 컨트롤러 클래스의 기본 구조 코드
{controller_skeleton}


[ 주요 작업 ]
===============================================
1. 'controller_skeleton'에서 ColdPlaceHolder 위치에 들어갈 컨트롤러 메서드 로직을 생성하세요.
2. 결과는 클래스 선언부를 제외하고 오직 단일 컨트롤러 메서드 코드만 반환하세요.
3. Command Class에 대한 정보가 없을 경우 파라미터 없이 메서드를 정의하세요.
(예 : public ResponseEntity<String> getAllEmployees())


[메서드 생성 규칙]
===============================================
1. 메서드 명명 규칙
    - 'procedure_name'을 카멜케이스로 변환하여 사용
    - 첫 글자는 소문자로 시작
    예시) 
    - procedure_name: "UPDATE_EMPLOYEE" 
    - 메서드명: "updateEmployee"

    
2. URL 엔드포인트 규칙
    - @PostMapping의 URL은 메서드명과 동일하게 설정
    - 반드시 '/' 로 시작
    예시) 
    - 메서드명: "updateEmployee"
    - URL: "/updateEmployee"


3. 메서드 시그니처 규칙
    - 반환타입은 항상 ResponseEntity<String>
    - @RequestBody 어노테이션으로 Command DTO 수신
    예시)
    public ResponseEntity<String> updateEmployee(@RequestBody UpdateEmployeeCommand command)

    
4. 메서드 구현 규칙
    - method_signature에 정의된 메서드 이름으로 서비스 메서드 호출
    - 서비스 메서드의 인자로는 Command DTO에서 getter로 값을 추출하여 전달
    - 서비스 메서드 호출만 하고, 결과와 관계없이 성공 메시지 반환
    - 성공 메시지는 이름에 따라 알아서 설정


[컨트롤러 메서드 예시]
===============================================
@PostMapping("/updateEmployee")
public ResponseEntity<String> updateEmployee(@RequestBody UpdateEmployeeCommand command) {{
    employeeService.updateEmployee(command.getId(),command.getName());
    return ResponseEntity.ok("Update Employee Completed Successfully");
}}


[JSON 출력 형식]
===============================================
부가 설명 없이 결과만을 포함하여, 다음 JSON 형식으로 반환하세요:
{{
    "method": "작성된 컨트롤러 메서드 코드"
}}
"""
)


# 역할: 컨트롤러 메서드 코드를 생성하는 함수입니다.
#      LLM을 통해 서비스 메서드 호출 로직을 포함한 
#      스프링 부트 컨트롤러 메서드를 생성합니다.
#
# 매개변수:
#   - method_signature: 서비스 메서드의 시그니처
#   - procedure_name: 원본 프로시저/함수 이름
#   - command_class_variable: Command DTO 필드 목록
#   - command_class_name: Command 클래스 이름
#   - controller_skeleton: 컨트롤러 클래스 기본 구조
#
# 반환값:
#   - result: LLM이 생성한 컨트롤러 메서드 코드
def convert_controller_method_code(method_signature: str, procedure_name: str, command_class_variable: str, command_class_name: str, controller_skeleton: str) -> str:
    
    try:
        command_class_variable = json.dumps(command_class_variable, ensure_ascii=False, indent=2)

        chain = (
            RunnablePassthrough()
            | prompt
            | llm
            | JsonOutputParser()
        )
        result = chain.invoke({"method_signature": method_signature, "procedure_name": procedure_name, "command_class_variable": command_class_variable, "command_class_name": command_class_name, "controller_skeleton": controller_skeleton})
        return result
    except Exception as e:
        err_msg = f"컨트롤러 메서드 생성 과정에서 LLM 호출하는 도중 오류가 발생했습니다: {str(e)}"
        logging.error(err_msg)
        raise LLMCallError(err_msg)
    



prompt = PromptTemplate.from_template(
"""
당신은 FastAPI 라우터 함수를 생성하는 전문가입니다.
주어진 데이터를 기반으로 라우터 함수를 생성합니다.


[입력 데이터 구조 설명]
===============================================
method_signature: 서비스 클래스에 정의된 메서드의 시그니처
{method_signature}

procedure_name : 원본 PL/SQL의 프로시저/함수 이름
{procedure_name}

command_class_variable: Command DTO 클래스에 정의된 필드 목록으로 메서드 호출 시 필요한 파라미터 추출에 사용
{command_class_variable}

command_class_name: Command 클래스의 이름
{command_class_name}

controller_skeleton: 컨트롤러 클래스의 기본 구조 코드
{controller_skeleton}


[ 주요 작업 ]
===============================================
1. 'controller_skeleton'에서 ColdPlaceHolder 위치에 들어갈 라우터 함수 로직을 생성하세요.
2. 결과는 클래스 선언부를 제외하고 오직 단일 라우터 함수 코드만 반환하세요.
3. Command Class에 대한 정보가 없을 경우 파라미터 없이 메서드를 정의하세요.
(예 : @router.post("/get_all_employees")\ndef get_all_employees())


[메서드 생성 규칙]
===============================================
1. 메서드 명명 규칙
    - 'procedure_name'을 스네이크케이스로 변환하여 사용
    - 첫 글자는 소문자로 시작
    예시) 
    - procedure_name: "UPDATE_EMPLOYEE" 
    - 메서드명: "update_employee"

    
2. URL 엔드포인트 규칙
    - @router.post의 URL은 메서드명과 동일하게 설정
    - 반드시 '/' 로 시작
    예시) 
    - 메서드명: "update_employee"
    - URL: "/update_employee"


3. 메서드 시그니처 규칙
    - 반환타입은 항상 dict
    - Command DTO는 함수 파라미터로 직접 수신
    예시)
    @router.post("/update_employee")
    def update_employee(command: UpdateEmployeeCommand)

    
4. 메서드 구현 규칙
    - method_signature에 정의된 메서드 이름으로 서비스 메서드 호출
    - 서비스 메서드의 인자로는 Command DTO에서 직접 속성으로 값을 추출하여 전달
    - 서비스 메서드 호출만 하고, 결과와 관계없이 성공 메시지 반환
    - 성공 메시지는 이름에 따라 알아서 설정


[컨트롤러 메서드 예시]
===============================================
@router.post("/update_employee")
def update_employee(command: UpdateEmployeeCommand):
    employee_service.update_employee(command.id, command.name)
    return {{"message": "Update Employee Completed Successfully"}}


[JSON 출력 형식]
===============================================
부가 설명 없이 결과만을 포함하여, 다음 JSON 형식으로 반환하세요:
{{
    "method": "작성된 컨트롤러 메서드 코드"
}}
"""
)


# 역할: 라우터 함수 코드를 생성하는 함수입니다.
#      LLM을 통해 서비스 메서드 호출 로직을 포함한 
#      FastAPI 라우터 함수를 생성합니다.
#
# 매개변수:
#   - method_signature: 서비스 메서드의 시그니처
#   - procedure_name: 원본 프로시저/함수 이름
#   - command_class_variable: Command DTO 필드 목록
#   - command_class_name: Command 클래스 이름
#   - controller_skeleton: 컨트롤러 클래스 기본 구조
#
# 반환값:
#   - result: LLM이 생성한 라우터 함수 코드
def convert_controller_method_code_python(method_signature: str, procedure_name: str, command_class_variable: str, command_class_name: str, controller_skeleton: str) -> str:
    
    try:
        command_class_variable = json.dumps(command_class_variable, ensure_ascii=False, indent=2)

        chain = (
            RunnablePassthrough()
            | prompt
            | llm
            | JsonOutputParser()
        )
        result = chain.invoke({"method_signature": method_signature, "procedure_name": procedure_name, "command_class_variable": command_class_variable, "command_class_name": command_class_name, "controller_skeleton": controller_skeleton})
        return result
    except Exception as e:
        err_msg = f"라우터 함수 생성 과정에서 LLM 호출하는 도중 오류가 발생했습니다: {str(e)}"
        logging.error(err_msg)
        raise LLMCallError(err_msg)