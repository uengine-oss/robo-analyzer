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
llm = ChatAnthropic(model="claude-3-5-sonnet-20241022", max_tokens=8000, temperature=0.0)
prompt = PromptTemplate.from_template(
"""
당신은 클린 아키텍처 원칙을 따르는 스프링부트 기반의 자바 애플리케이션을 개발하는 소프트웨어 엔지니어입니다. 
주어진 Stored Procedure Code를 기반으로 서비스 클래스의 메서드 바디 부분을 간결하고 가독성 좋은 클린 코드 형태로 구현하는 작업을 맡았습니다.


[입력 데이터]
Stored Procedure Code:
{code}

Method Signature:
{method_skeleton}

Used Variable:
{variable}

Command Class Variable:
{command_variables}

Context Range:
{context_range}

JPA Method List:
{jpa_method_list}


[SECTION 1] 입력 데이터 설명
===============================================
입력 데이터
   - Stored Procedure Code: 자바로 변환할 프로시저 코드
   - Method Template: 구현할 메서드의 시그니처와 기본 구조
   - Used Variable: 현재 변수들의 할당값 정보 (이전 작업 결과)
   - Command Class Variable: DTO 역할의 Command 클래스 변수 목록
   - Context Range: 변환 대상 코드의 시작/종료 라인 정보
   - jpa_method_list: 현재 범위에서 사용 가능한 JPA 쿼리 메서드 목록

주요 작업
   - 'Method Signature'을 참고하여, CodePlaceHolder 위치에 들어갈 코드만 구현하세요.
   - 'Method Signature'는 제외하고 메서드 내부의 실제 구현 코드만 결과로 반환하세요.
   - 자바코드는 클린코드 및 가독성이 좋아야 하며, 들여쓰기가 적용된 상태로 반환하세요.

[SECTION 2] 상황별 자바 코드 변환 규칙
===============================================

1. 범위 처리 규칙
   - 모든 범위({count}개)에 대해 누락 및 생략 없이 {count}개의 'code'를 반환
   - 이미 상위 블록에서 다뤄진 코드라도 주석 처리나 생략 없이 온전한 코드로 반환
   - Context Range의 각 범위를 독립적으로 처리하며, 자신의 범위에 해당하는 코드만 변환
   - 다른 범위와 중첩되더라도 자신의 범위 내 코드만 처리하고 범위를 합치지 마세요
   - 중첩 범위의 예시:
     상위범위(1925~1977), 하위범위(1942~1977)인 경우
     → 상위범위: 1925~1941 구간의 코드만 변환
     → 하위범위: 1942~1977 구간의 코드만 변환
   - 각 범위는 다른 범위의 코드를 참조하거나 포함하지 않음
   - 예외처리(try-catch)를 하지마세요
   
2. SQL 구문 변환 규칙
   A. SELECT 구문
      - jpa_method_list에서 적절한 조회 메서드 사용
      - 결과는 엔티티 객체나 컬렉션으로 받음
   
   B. UPDATE/MERGE 구문
      - jpa_method_list에서 수정할 엔티티 먼저 조회하는 메서드 사용
      - 조회한 엔티티의 필드값을 자바 코드(비즈니스 로직)을 이용하여 변경
      - save() 메서드로 변경사항 저장
      예시:
      Employee employee = employeeRepository.findById(id);
      employee.setStatus(newStatus);
      employeeRepository.save(employee);
   
   C. INSERT 구문
      - INSERT INTO ... SELECT FROM 구조인 경우:
        * SELECT 부분만 jpa_method_list의 조회 메서드로 변환
        * 조회된 데이터로 새 엔티티 생성 후 save() 수행
        예시:
        List<SourceEntity> sourceList = sourceRepository.findByCondition(param);
        for (SourceEntity source : sourceList) {{
            TargetEntity target = new TargetEntity();
            target.setField(source.getField());
            targetRepository.save(target);
        }}

      - 순수 INSERT 구문의 경우:
        * 새 엔티티 객체 생성
        * save() 메서드로 저장
        예시:
        NewEntity entity = new NewEntity();
        entity.setField(value);
        repository.save(entity);

   D. DELETE 구문
      - 적절한 삭제 메서드 사용

3. 변수 처리 규칙
   A. 변수값 추적 (Used Variable)
      - 'Used Variable'로 전달된 모든 변수들의 값 변화를 매우 상세하게 추적하여 기록
      - 추적 대상:
        * SQL 실행 결과값 저장
        * 조건문에 따른 값 변경
        * 연산에 의한 값 변경
      - 추적된 값은 'variables' 객체에 저장(변수 이름은 전달된 변수 이름과 동일하게 반환)
      예시:
        "vEmpId": "SQL 조회 결과(사원번호) -> 초기값은 NULL, TB_EMPLOYEE 테이블에서 조회한 EMP_ID 값이 할당됨 -> 이후 UPDATE 문의 조건절에서 사용",
        "vStatus": "초기값 'N' -> 사원정보가 존재하면 'Y'로 변경 -> 'Y'인 경우 추가 데이터 처리 수행 -> 모든 처리가 정상적으로 완료되면 최종적으로 'S'로 설정",

   B. 변수 사용 규칙
      1) Used Variable
         - 새로운 변수 선언 금지 (전달된 변수만 사용)
         - 실제 사용된 변수명을 그대로 사용하고, 접두어를 제거하지마세요.
         - 카멜 케이스 명명규칙 적용
         예시: vEmpId, vDeptCode, vRow

      2) Command Class Variable
         - DTO의 getter 메서드로만 접근
         - 단순 입력 매개변수로 사용
         - 카멜 케이스 명명규칙 적용
         예시: employeeDto.getEmployeeId()
  
4. 프로시저 호출 변환 규칙
   A. 외부 프로시저 호출
      - 형식: {{스키마명}}.{{프로시저명}}({{파라미터}})
      - 변환: {{스키마명}}Service.{{프로시저명}}({{파라미터}})
      예시:
      * 프로시저: TPX_PROJECT.SET_KEY(iProjKey)
      * 변환: tpxProjectService.setKey(projKey)

   B. 내부 프로시저 호출
      - 형식: {{프로시저명}}({{파라미터}})
      - 변환: private 메서드로 추출하여 호출
      예시:
      * 프로시저: INPUT(idata)
      * 변환: input(data)

   C. 명명 규칙
      - 스네이크 케이스를 카멜 케이스로 변환
      - 서비스 클래스명은 스키마명 + Service
      - 메서드명은 프로시저명을 카멜 케이스로 변환

5. 'EXCEPTION' 예외처리 변환 규칙
   A. 코드 반환 형식
      - 'EXCEPTION' 키워드가 식별되는 코드 범위만 다음 try-catch 형식으로 예외처리하세요.
        try {{
         CodePlaceHolder
        }} catch (Exception e) {{
            * // EXCEPTION 키워드가 있는 코드 범위를 자바로 변환한 코드
        }}
      - 'CodePlaceHolder' 문자열은 하드코딩 그대로 반환하고, try 안에 내용을 채우지 마세요.
      - 그 이외에 모든 코드 범위는 예외처리(try-catch) 없이 자바 코드로 반환   
   
   예시:
   * PL/SQL:
     203: 203: INSERT INTO TABLE VALUES row;
     204: 204: EXCEPTION WHEN OTHERS THEN
     205: 205:     RAISE_APPLICATION_ERROR(-20102, SQLERRM);
   
   * 변환 결과:
     203~203: "repository.save(entity);"  
     204~205: "try CodePlaceHolder catch (Exception e) {{ 
         throw new Exception(\"Cannot insert: \" + e.getMessage());
     }}"

     
[SECTION 3] 자바 코드 생성시 JSON 문자열 처리 규칙
===============================================
1. 특수 문자 이스케이프 처리
   - 줄바꿈: \\n
   - 큰따옴표: \\"
   - 백슬래시: \\\\
   - 작은따옴표: \\'

2. 문자열 작성 규칙
   - 문자열 연결 시 '+' 연산자 사용 금지
   - 하나의 연속된 문자열로 작성
   - 모든 따옴표 이스케이프 처리 확인
   - JSON 파싱 오류 방지를 위한 철저한 이스케이프 처리     
      

[SECTION 4] JSON 출력 형식
===============================================
부가 설명 없이 결과만을 포함하여, 다음과 같은 dictionary(사전) 형태의 JSON 형식으로 반환하세요:
{{
   "analysis": {{
      "code": {{
         "startLine~endLine": "Method Code",
         "startLine~endLine": "Method Code"
      }},
      "variables": {{
         "name": "initialized value and role",
         "name": "initialized value and role"
      }}
   }}
}}
""")


# 역할: PL/SQL 프로시저를 Java 서비스 계층의 메서드로 변환하는 함수입니다.
#      LLM을 통해 프로시저의 비즈니스 로직을 분석하고,
#      클린 아키텍처와 DDD 원칙을 따르는 서비스 메서드를 생성합니다.
#      SQL 작업은 Repository 계층으로 위임하고, 트랜잭션 관리와 비즈니스 규칙을 처리합니다.
# 매개변수: 
#  - clean_code : 변환할 PL/SQL 프로시저 코드
#  - method_skeleton : 생성될 Java 메서드의 기본 구조
#  - variable_list : PL/SQL에서 사용된 변수들의 정보
#  - jpa_query_methods : 사용 가능한 JPA Repository 메서드 목록
#  - command_class_variable : Command 클래스의 필드 정보
#  - context_range : 코드 변환 범위 정보
#  - count : context_range의 범위 개수
# 반환값: 
#  - json_parsed_content : LLM이 생성한 서비스 메서드 정보
# TODO 토큰 초과시 로직 추가 필요
def convert_service_code(convert_sp_code, method_skeleton, variable_list, command_class_variable, context_range, count, jpa_method_list):
   
   try:  
      context_range_json = json.dumps(context_range, indent=2)
      command_class_variable = json.dumps(command_class_variable, ensure_ascii=False, indent=2)

      chain = (
         RunnablePassthrough()
         | prompt
         | llm
      )
      result = chain.invoke({"code": convert_sp_code, "method_skeleton": method_skeleton, "variable": variable_list, "command_variables": command_class_variable, "context_range": context_range_json, "count": count, "jpa_method_list": jpa_method_list})
      # TODO 여기서 최대 토큰이 4096이 넘은 경우 처리가 필요
      logging.info(f"토큰 수: {result.usage_metadata}") 
      output_tokens = result.usage_metadata['output_tokens']
      if output_tokens > 4096:
         logging.warning(f"출력 토큰 수가 4096을 초과했습니다: {output_tokens}")

      json_parsed_content = json5.loads(result.content)
      return json_parsed_content

   except Exception:
      err_msg = "(전처리) 서비스 코드 생성 과정에서 LLM 호출하는 도중 오류가 발생했습니다."
      logging.error(err_msg, exc_info=False)
      raise LLMCallError(err_msg)