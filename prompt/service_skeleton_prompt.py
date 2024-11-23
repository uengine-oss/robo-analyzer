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
llm = ChatAnthropic(model="claude-3-5-sonnet-20240620", max_tokens=8000)
controller_method_prompt = PromptTemplate.from_template(
"""
당신은 PL/SQL 프로시저를 스프링부트 컨트롤러 메서드로 변환하는 전문가입니다.
주어진 JSON 데이터를 기반으로 컨트롤러 메서드를 생성합니다.



[입력 데이터 구조 설명]
===============================================
입력되는 JSON 데이터는 다음 구조를 가집니다:
{method_skeleton_data}

- procedure_name: 프로시저/함수 이름
- local_variables: 로컬 변수 목록
- return_code: 리턴 노드 코드
- node_type: 노드 타입 (create_procedure_body, procedure, function)


[SECTION 1] 메서드 생성 규칙
===============================================
1. 메서드 명명 규칙
    - 'procedure_name'을 카멜케이스로 변환하여 메서드명 생성
    - 예시: UPDATE_EMPLOYEE -> updateEmployee
    - 첫 글자는 소문자로 시작

2. 메서드 생성 규칙
    - @PostMapping 어노테이션 사용
    - 반환타입: void
    - 파라미터: @RequestBody {command_class_name} {commandClassNameDto} (카멜케이스)
    
3. 메서드의 필드 규칙
    - 'local_variables'의 변수 목록을 기반으로 컨트롤러 메서드 내부에 변수를 생성
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
   
4. 코드 구조
   - 문자열 "CodePlaceHolder"를 그대로 유지 (변경하지 않음)
   - 메서드 내부에는 "CodePlaceHolder" 문자열만 존재해야 함
   - 메서드 내부 구현은 비워둠


[SECTION 2] 컨트롤러 메서드 예시 템플릿
===============================================
@PostMapping(path="/{procedure_name}")
public ResponseEntity<String> {procedure_name}(@RequestBody {command_class_name} {command_class_name}Dto) {{
    private Long id;
    private String name;
    @ToDo("Original Type: EMPLOYEE_TABLE.ROWTYPE")
    private Object employeeRow;

CodePlaceHolder
    return ResponseEntity.ok("Operation completed successfully");
}}


[SECTION 3] JSON 출력 형식
===============================================
부가 설명 없이 결과만을 포함하여, 다음 JSON 형식으로 반환하세요:
{{
    "methodName": "updateEmployee",
    "method": "@PostMapping(\"/updateEmployee\")\npublic ResponseEntity<String> updateEmployee(@RequestBody UpdateEmployeeCommand command) {{\n    CodePlaceHolder\n    return ResponseEntity.ok(\"Operation completed successfully\");\n}}"
}}
"""
)

# 역할 : 컨트롤러 메서드 틀 데이터를 기반으로, 메서드 틀을 생성합니다
# 매개변수: 
#   - method_skeleton_data : 메서드 틀 데이터
#   - command_class_name : command 클래스 이름
#   - object_name : 패키지 이름
# 반환값 : 
#   - result : 컨트롤러 메서드 틀 코드
def convert_method_skeleton_code(method_skeleton_data, command_class_name, object_name):
    
    try:
        method_skeleton_data = json.dumps(method_skeleton_data)

        chain = (
            RunnablePassthrough()
            | controller_method_prompt
            | llm
            | JsonOutputParser()
        )
        result = chain.invoke({"method_skeleton_data": method_skeleton_data, "command_class_name": command_class_name, "object_name": object_name})
        return result
    except Exception:
        err_msg = "컨트롤러 메서드 틀 생성 과정에서 LLM 호출하는 도중 오류가 발생했습니다."
        logging.error(err_msg, exc_info=False)
        raise LLMCallError(err_msg)
    

function_prompt = PromptTemplate.from_template(
"""
당신은 PL/SQL 함수를 스프링부트 일반 메서드로 변환하는 전문가입니다.
주어진 JSON 데이터를 기반으로 일반 메서드를 생성합니다.

[입력 데이터 구조 설명]
===============================================
1. 메서드 데이터:
{method_skeleton_data}
- procedure_name: 함수 이름
- local_variables: 로컬 변수 목록
- return_code: 리턴 노드 코드
- node_type: 노드 타입 (function)

2. 파라미터 데이터:
{parameter_data}
- parameters: 파라미터 목록
- procedure_name: 함수 이름


[SECTION 1] 메서드 생성 규칙
===============================================
1. 메서드 명명 규칙
    - 'procedure_name'을 카멜케이스로 변환하여 메서드명 생성
    - 예시: GET_EMPLOYEE_COUNT -> getEmployeeCount
    - 첫 글자는 소문자로 시작

2. 메서드 생성 규칙
    - 일반 메서드로 생성 (어노테이션 없음)
    - 반환타입: return_code를 기반으로 결정 (주로 Long, String, Boolean 등)
    - 파라미터: 'parameter_data'의 'parameters' 정보를 기반으로 생성
    
3. 메서드의 필드 규칙
    - 'local_variables'의 변수 목록을 기반으로 컨트롤러 메서드 내부에 변수를 생성
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
    
4. 코드 구조
    - 문자열 "CodePlaceHolder"를 그대로 유지 (변경하지 않음)
    - 메서드 내부에는 "CodePlaceHolder" 문자열만 존재해야 함
    - 메서드 내부 구현은 비워둠


[SECTION 2] 일반 메서드 예시 템플릿
===============================================
public ReturnType {procedure_name}(Type1 param1, Type2 param2) {{
    private Long id;
    private String name;
    @ToDo("Original Type: EMPLOYEE_TABLE.ROWTYPE")
    private Object employeeRow;
    
CodePlaceHolder
    return result;
}}


[SECTION 3] JSON 출력 형식
===============================================
부가 설명 없이 결과만을 포함하여, 다음 JSON 형식으로 반환하세요:
{{
    "methodName": "getEmployeeCount",
    "method": "public Long getEmployeeCount(Long employeeId, String departmentCode) {{\nCodePlaceHolder\n    return result;\n}}"
}}
"""
)

# 역할 : 일반 메서드 틀 데이터를 기반으로, 메서드 틀을 생성합니다
# 매개변수: 
#   - method_skeleton_data : 메서드 틀 데이터
#   - parameter_data : 파라미터 데이터
#   - object_name : 패키지 이름
# 반환값 : 
#   - result : 일반 메서드 틀 코드
def convert_function_code(method_skeleton_data, parameter_data, object_name):
    
    try:
        method_skeleton_data = json.dumps(method_skeleton_data)
        parameter_data = json.dumps(parameter_data)
        chain = (
            RunnablePassthrough()
            | function_prompt
            | llm
            | JsonOutputParser()
        )
        result = chain.invoke({"method_skeleton_data": method_skeleton_data, "parameter_data": parameter_data, "object_name": object_name})
        return result
    except Exception:
        err_msg = "일반 메서드 틀 생성 과정에서 LLM 호출하는 도중 오류가 발생했습니다."
        logging.error(err_msg, exc_info=False)
        raise LLMCallError(err_msg)