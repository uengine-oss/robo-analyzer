import json
import logging
import os
from langchain_core.globals import set_llm_cache
from langchain_community.cache import SQLiteCache
from util.llm_client import get_llm
from langchain_core.output_parsers import JsonOutputParser
from langchain_core.prompts import PromptTemplate
from langchain_core.runnables import RunnablePassthrough
from util.exception import LLMCallError

db_path = os.path.join(os.path.dirname(__file__), 'langchain.db')
set_llm_cache(SQLiteCache(database_path=db_path))

prompt = PromptTemplate.from_template(
"""
당신은 PL/SQL 함수를 스프링부트 메서드로 변환하는 전문가입니다.
주어진 JSON 데이터를 기반으로 메서드를 생성합니다.


사용자 언어 설정 : {locale}, 입니다. 이를 반영하여 결과를 생성해주세요.


[입력 데이터 구조 설명]
===============================================
1. 메서드 데이터:
{method_skeleton_data}
- procedure_name: 프로시저/함수 이름
- local_variables: 로컬 변수 목록 (각 변수는 name, type, value 속성을 가짐)
- declaration: 선언부 코드 (리턴타입, 입력 매개변수 등이 선언된 부분)

2. 파라미터 데이터:
{parameter_data}
- in_parameters: 입력 파라미터 목록 (IN, IN_OUT 타입만 포함, 각 파라미터는 name, type, value 속성을 가짐)
- out_parameters: 출력 파라미터 목록 (OUT 타입만 포함, 반환 타입 결정에만 사용)
- out_count: OUT 파라미터 개수 (0, 1, 2 이상)
- procedure_name: 함수 이름

⚠️ 중요: out_parameters는 반환 타입 결정에만 사용하고, 지역변수 선언은 local_variables만 사용하세요!


[SECTION 1] 메서드 생성 규칙
===============================================
1. 메서드 명명 규칙
    - 'procedure_name'을 카멜케이스로 변환하여 메서드명 생성
    - v, p, i, o 와 같은 접두어를 제거하지말고, 풀네임을 메서드 명으로 전환
    - 예시: GET_EMPLOYEE_COUNT -> getEmployeeCount
    - 첫 글자는 소문자로 시작

2. 메서드 생성 규칙
    - 메서드로 생성 (어노테이션 없음)
    - 반환타입:
        * OUT 파라미터 개수에 따라 결정 (out_count 참조):
            
            [CASE 1] out_count == 0 (OUT 파라미터 없음):
                - declaration에서 반환타입이 명시되지 않은 경우 void로 설정
                - declaration에 반환타입이 있는 경우 해당 타입으로 매핑
            
            [CASE 2] out_count == 1 (OUT 파라미터 1개):
                - out_parameters[0]의 type을 메서드의 반환 타입으로 사용
                - 타입 매핑 규칙 적용
                - OUT 파라미터는 이미 local_variables에 포함되어 있으므로 별도 선언 불필요
            
            [CASE 3] out_count >= 2 (OUT 파라미터 2개 이상):
                - 반환타입을 Map<String, Object>로 설정
                - OUT 파라미터들은 이미 local_variables에 포함되어 있으므로 별도 선언 불필요
                - 실제 Map 생성 및 반환 로직은 CodePlaceHolder에서 처리됨
        
        * 타입 매핑 규칙:
            - NUMBER -> Long
            - VARCHAR2, CHAR -> String
            - DATE -> LocalDate
            - TIME -> LocalDateTime
            - BOOLEAN -> Boolean
    
    - 파라미터: 'parameter_data'의 'in_parameters' 정보만 사용하여 메서드 파라미터 생성
        * out_parameters는 메서드 파라미터로 사용하지 않고, 지역변수로만 선언
        * Java는 OUT 파라미터가 없으므로 메서드 파라미터에서 제외됨
    
3. 메서드의 지역변수 규칙
    - ⚠️ 중요: 'local_variables' 목록에만 의존하여 지역변수 생성
        * local_variables에는 이미 OUT 파라미터가 포함되어 있음 (DECLARE 변수 + OUT 파라미터)
        * out_parameters는 지역변수 선언에 사용하지 않음 (반환 타입 결정에만 사용)
    - 명명규칙: 접두어 제거하지 말고, 원본 이름을 그대로 카멜케이스로 표현 (첫 글자 소문자)
    - 데이터 타입 매핑:
        * NUMBER, NUMERIC -> Long
        * VARCHAR, VARCHAR2, CHAR -> String
        * 컬럼명에 'TIME'이 포함된 경우 -> LocalDateTime (예 : CurrentTime, EndTime, StartTime)
        * 컬럼명에 'DATE'만 포함되고 'TIME'이 없는 경우 -> LocalDate (예 : CurrentDate, EndDate, StartDate)
        * 테이블 이름의 경우: 테이블 명을 타입으로 사용 (엔티티 클래스를 타입으로 설정)
    - 변수 초기화:
        - local_variables의 value 값이 존재하는 경우에만 해당 값으로 초기화합니다.
        - 테이블 명, 엔티티 클래스가 타입으로 선정된 경우 new EntityClass() 형태로 표현
        - 그 외 기본 타입들은 'value' 값이 없다면 변수 초기화 하지 않고 선언만 합니다.

4. 메서드의 파라미터 규칙
    - 'parameter_data'의 'in_parameters' 목록에 있는 파라미터만 메서드 파라미터로 생성
    - ⚠️ 중요: 'out_parameters'는 메서드 파라미터로 생성하지 않음 (반환 타입 결정에만 사용)
    - 명명규칙: 카멜케이스 (첫 글자 소문자)
    - 데이터 타입 매핑:
        * NUMBER, NUMERIC -> Long
        * VARCHAR, VARCHAR2, CHAR -> String
        * 컬럼명에 'TIME'이 포함된 경우 -> LocalDateTime (예 : CurrentTime, EndTime, StartTime)
        * 컬럼명에 'DATE'만 포함되고 'TIME'이 없는 경우 -> LocalDate (예 : CurrentDate, EndDate, StartDate)
        * 테이블 이름의 경우: 테이블 명을 타입으로 사용 (엔티티 클래스를 타입으로 설정)

5. 코드 구조
    - 문자열 "CodePlaceHolder"는 그대로 유지하고, 변경하지 마세요.
    - 메서드 내부 구현 없이, "CodePlaceHolder" 문자열만 존재해야 함
    - return 문 또한 별도로 추가 예정으로, return을 추가하지말고, 템플릿 구조를 지키세요.

    
[SECTION 2] 메서드 예시 템플릿
===============================================

[예시 1] OUT 파라미터 없음 (out_count = 0, in_parameters = 2개):
public void methodName(Long param1, String param2) {{  // IN 파라미터만 메서드 인자로
    Long employeeId;   // DECLARE 변수
    String name;       // DECLARE 변수
    
    CodePlaceHolder
}}

[예시 2] OUT 파라미터 1개 (out_count = 1, in_parameters = 1개, out_parameters = 1개):
public String methodName(Long param1) {{  // IN 파라미터만 메서드 인자로 (OUT은 제외!)
    String outResult;  // local_variables에서 가져옴 (OUT 파라미터 포함)
    Long employeeId;   // local_variables에서 가져옴 (DECLARE 변수)
    
    CodePlaceHolder
}}

[예시 3] OUT 파라미터 2개 이상 (out_count = 3, in_parameters = 1개, out_parameters = 3개):
public Map<String, Object> methodName(Long param1) {{  // IN 파라미터만 메서드 인자로 (OUT들은 제외!)
    String outName;     // local_variables에서 가져옴 (OUT 파라미터)
    Long outAge;        // local_variables에서 가져옴 (OUT 파라미터)
    String outAddress;  // local_variables에서 가져옴 (OUT 파라미터)
    Long employeeId;    // local_variables에서 가져옴 (DECLARE 변수)
    
    CodePlaceHolder
}}


[SECTION 3] JSON 출력 형식
===============================================
부가 설명 없이 결과만을 포함하여, 다음 JSON 형식으로 반환하세요:
{{
    "methodName": "getEmployeeCount",
    "method": "public Long getEmployeeCount(Long employeeId, String departmentCode) {{\nCodePlaceHolder\n    return result;\n}}",
    "methodSignature": "Long getEmployeeCount(Long employeeId, String departmentCode)"
}}
"""
)


# 역할: PL/SQL 함수/프로시저의 시그니처를 분석하여 Java 메서드의 기본 구조를 생성하는 함수입니다.
#
# 매개변수: 
#   - method_skeleton_data : 메서드 기본 구조 생성에 필요한 데이터
#   - parameter_data : 함수/프로시저의 파라미터 정보
#
# 반환값: 
#   - result : LLM이 생성한 메서드 기본 구조 정보
def convert_method_code(method_skeleton_data, parameter_data, api_key, locale):
    
    try:

        llm = get_llm(max_tokens=8192, api_key=api_key)
        
        method_skeleton_data = json.dumps(method_skeleton_data, ensure_ascii=False, indent=2)
        parameter_data = json.dumps(parameter_data, ensure_ascii=False, indent=2)
        chain = (
            RunnablePassthrough()
            | prompt
            | llm
            | JsonOutputParser()
        )
        result = chain.invoke({"method_skeleton_data": method_skeleton_data, "parameter_data": parameter_data, "locale": locale})
        return result
    
    except Exception as e:
        err_msg = f"메서드 틀 생성 과정에서 LLM 호출하는 도중 오류가 발생했습니다: {str(e)}"
        logging.error(err_msg)
        raise LLMCallError(err_msg)