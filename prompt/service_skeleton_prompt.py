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
llm = ChatAnthropic(model="claude-3-5-sonnet-20241022", max_tokens=8000, temperature=0.0)
controller_method_prompt = PromptTemplate.from_template(
"""
당신은 PL/SQL 프로시저를 스프링부트 컨트롤러 메서드로 변환하는 전문가입니다.
주어진 JSON 데이터를 기반으로 컨트롤러 메서드를 생성합니다.


[입력 데이터 구조 설명]
===============================================
입력되는 JSON 데이터는 다음 구조를 가집니다:
{method_skeleton_data}

- procedure_name: 프로시저/함수 이름
- local_variables: 로컬 변수 목록으로 메서드 내부에 필드로 생성될 정보 (각 변수는 name과 type 속성을 가짐)
- declaration: 선언부 코드 (리턴타입, 입력 매개변수가 선언된 부분)
- node_type: 노드 타입 (create_procedure_body, procedure, function)


[SECTION 1] 메서드 생성 규칙
===============================================
1. 메서드 명명 규칙
    - 'procedure_name'을 카멜케이스로 변환하여 메서드명 생성
    - 예시: UPDATE_EMPLOYEE -> updateEmployee
    - 첫 글자는 소문자로 시작
    - /endpoint 또한 'procedure_name'를 카멜케이스로 변환하여 사용

2. 메서드 생성 규칙
    - @PostMapping 어노테이션 사용
    - 반환타입: void
    - 파라미터: @RequestBody {{command_class_name}} {{command_class_name}}Dto (카멜케이스)
    
3. 메서드의 필드 규칙
    - 오직 'local_variables' 목록에 있는 변수만 메서드 내부 필드로 생성
    - 명명규칙: 카멜케이스 (첫 글자 소문자)
    - 데이터 타입 매핑:
        * NUMBER, NUMERIC -> Long
        * VARCHAR, VARCHAR2, CHAR -> String
        * DATE -> LocalDate
        * TIMESTAMP -> LocalDateTime
        * 테이블 이름의 경우 테이블명을 타입으로 사용하세요 (엔티티 클래스를 타입으로 설정)
    - 'local_variables' 배열이 비어있다면 메서드의 필드는 생성하지않음

4. 코드 구조
   - 문자열 "CodePlaceHolder"는 그대로 유지하고, 변경하지 마세요.
   - 메서드 내부 구현 없이, "CodePlaceHolder" 문자열만 존재해야 함


[SECTION 2] 컨트롤러 메서드 예시 템플릿
===============================================
@PostMapping(path="/endpoint")
public ResponseEntity<String> methodName(@RequestBody {command_class_name} {command_class_name}Dto) {{
    Long id;
    String name;
    LocalDate date;
    LocalDateTime time;
    Employee employee;

    CodePlaceHolder
    return ResponseEntity.ok("message");
}}


[SECTION 4] JSON 출력 형식
===============================================
부가 설명 없이 결과만을 포함하여, 다음 JSON 형식으로 반환하세요.
문자열 내의 모든 따옴표(")는 반드시 이스케이프(\\")처리가 되어야 합니다:
{{
    "methodName": "updateEmployee",
    "method": "@PostMapping(\"/updateEmployee\")\npublic ResponseEntity<String> updateEmployee(@RequestBody UpdateEmployeeCommand command) {{\n    CodePlaceHolder\n    return ResponseEntity.ok(\"Operation completed successfully\");\n}}"
}}
"""
)


# 역할: PL/SQL 프로시저를 스프링 부트 컨트롤러 메서드로 변환하는 함수입니다.
#      LLM을 통해 프로시저의 시그니처를 분석하고,
#      RESTful API 엔드포인트와 HTTP 메서드를 결정하여
#      컨트롤러 메서드의 기본 구조를 생성합니다.
# 매개변수: 
#   - method_skeleton_data : 프로시저의 메타데이터 정보
#   - command_class_name : 요청 데이터를 담을 Command 클래스의 이름
# 반환값: 
#   - result : LLM이 생성한 컨트롤러 메서드 정보
def convert_controller_method_code(method_skeleton_data, command_class_name):
    
    try:
        method_skeleton_data = json.dumps(method_skeleton_data, ensure_ascii=False, indent=2)

        chain = (
            RunnablePassthrough()
            | controller_method_prompt
            | llm
            | JsonOutputParser()
        )
        result = chain.invoke({"method_skeleton_data": method_skeleton_data, "command_class_name": command_class_name})
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
- procedure_name: 프로시저/함수 이름
- local_variables: 로컬 변수 목록 (각 변수는 name과 type 속성을 가짐)
- declaration: 선언부 코드 (리턴타입, 입력 매개변수 등이 선언된 부분)
- node_type: 노드 타입 (create_procedure_body, procedure, function)

2. 파라미터 데이터:
{parameter_data}
- parameters: 파라미터 목록 (각 파라미터는 name과 type 속성을 가짐)
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
    - 오직 'local_variables' 목록에 있는 변수만 메서드 내부 변수로 생성
    - 명명규칙: 카멜케이스 (첫 글자 소문자)
    - 데이터 타입 매핑:
        * NUMBER, NUMERIC -> Long
        * VARCHAR, VARCHAR2, CHAR -> String
        * DATE -> LocalDate
        * TIMESTAMP -> LocalDateTime
        * 테이블 이름의 경우 테이블명을 타입으로 사용하세요 (엔티티 클래스를 타입으로 설정)
    - 'local_variables' 배열이 비어있다면 메서드의 필드는 생성하지않음

4. 메서드의 파라미터 규칙
    - 'parameter_data'의 'parameters' 목록에 있는 파라미터만 메서드 파라미터로 생성
    - 명명규칙: 카멜케이스 (첫 글자 소문자)
    - 데이터 타입 매핑:
        * NUMBER, NUMERIC -> Long
        * VARCHAR, VARCHAR2, CHAR -> String
        * DATE -> LocalDate
        * TIMESTAMP -> LocalDateTime

5. 코드 구조
    - 문자열 "CodePlaceHolder"는 그대로 유지하고, 변경하지 마세요.
    - 메서드 내부 구현 없이, "CodePlaceHolder" 문자열만 존재해야 함
    - return 문 또한 별도로 추가 예정으로, return을 추가하지말고, 템플릿 구조를 지키세요.

[SECTION 2] 일반 메서드 예시 템플릿
===============================================
public ReturnType methodName(Type1 param1, Type2 param2) {{
    Long id;
    String name;
    LocalDate date;
    LocalDateTime time;
    Employee employee;
    
    CodePlaceHolder
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

# 역할: PL/SQL 함수/프로시저의 시그니처를 분석하여 Java 메서드의 기본 구조를 생성하는 함수입니다.
#      LLM을 통해 PL/SQL 파라미터와 리턴 타입을 Java 데이터 타입으로 매핑하고,
#      메서드의 기본 구조(시그니처, 예외 처리, 리턴문 등)를 생성합니다.
# 매개변수: 
#   - method_skeleton_data : 메서드 기본 구조 생성에 필요한 데이터
#   - parameter_data : 함수/프로시저의 파라미터 정보
# 반환값: 
#   - result : LLM이 생성한 메서드 기본 구조 정보
def convert_function_code(method_skeleton_data, parameter_data):
    
    try:
        method_skeleton_data = json.dumps(method_skeleton_data, ensure_ascii=False, indent=2)
        parameter_data = json.dumps(parameter_data, ensure_ascii=False, indent=2)
        chain = (
            RunnablePassthrough()
            | function_prompt
            | llm
            | JsonOutputParser()
        )
        result = chain.invoke({"method_skeleton_data": method_skeleton_data, "parameter_data": parameter_data})
        return result
    except Exception:
        err_msg = "일반 메서드 틀 생성 과정에서 LLM 호출하는 도중 오류가 발생했습니다."
        logging.error(err_msg, exc_info=False)
        raise LLMCallError(err_msg)