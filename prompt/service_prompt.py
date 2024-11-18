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
주어진 Stored Procedure Code를 기반으로 간략하고 가독성 좋은 클린 코드 형태인 비즈니스 로직을 생성하는 작업을 맡았습니다.


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


[SECTION 1] 입력 데이터 설명
===============================================
입력 데이터
   - Stored Procedure Code: 자바로 변환할 프로시저 코드
   - Service Class Code: 비즈니스 로직이 들어갈 서비스 클래스 템플릿
   - Used Variable: 현재 변수들의 할당값 정보 (이전 작업 결과)
   - Command Class Variable: DTO 역할의 Command 클래스 변수 목록
   - Context Range: 변환 대상 코드의 시작/종료 라인 정보
   - jpa_method_list: 현재 범위에서 사용 가능한 JPA 쿼리 메서드 목록


[SECTION 2] 상황별 자바 코드 변환 규칙
===============================================
1. SQL 구문 변환 규칙
   A. SELECT 구문
      - jpa_method_list에서 적절한 조회 메서드 사용
      - 결과는 엔티티 객체나 컬렉션으로 받음
      예시:
      * 단일 조회: Employee employee = employeeRepository.findById(id);
      * 목록 조회: List<Employee> employees = employeeRepository.findByDepartment(deptCode);
   
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
        for (SourceEntity source : sourceList) {
            TargetEntity target = new TargetEntity();
            target.setField(source.getField());
            targetRepository.save(target);
        }

      - 순수 INSERT 구문의 경우:
        * 새 엔티티 객체 생성
        * save() 메서드로 저장
        예시:
        NewEntity entity = new NewEntity();
        entity.setField(value);
        repository.save(entity);

   D. DELETE 구문
      - 적절한 삭제 메서드 사용

2. 범위 처리 규칙
   - Context Range의 각 범위를 독립적으로 처리
   - 중첩 범위의 예시:
     상위범위(1925~1977), 하위범위(1942~1977)
     → 각각 독립적으로 변환하여 별도 코드 생성
   - 모든 범위({count}개)에 대해 누락 없이 변환

3. 변수 처리 규칙
   A. Used Variable (값 추적 필요)
      - 새로운 변수 선언 금지 (기존 변수만 사용)
      - 변수값 변화 추적:
        * 초기값 → 중간값 → 최종값 순서로 기록
        * SQL 실행 결과 저장값 추적
        * 조건문에 따른 값 변경 추적
      - 변수 용도 분석:
        * 쿼리 결과 저장
        * 임시 데이터 보관
        * 상태 플래그
      예시:
      "resultCount": "0 → 조회결과 건수 저장 → 최종 처리된 레코드 수"
   
   B. Command Class Variable (추적 불필요)
      - 단순 입력 매개변수로만 처리
      - DTO의 getter 메서드로 값 획득
      - 카멜 케이스 명명규칙 적용
      예시:
      * employeeDto.getEmployeeId()
      * employeeDto.getDepartmentCode()

      
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
부가 설명 없이 결과만을 포함하여, 다음 JSON 형식으로 반환하세요. ('analysis' 결과는 반드시 리스트가 아닌 dictionary(사전) 형태여야 합니다.):
{{
   "analysis": {{
      "code": {{
         "startLine~endLine": "Service Code",
         "startLine~endLine": "Service Code"
      }},
      "variables": {{
         "name": "initialized value and role",
         "name": "initialized value and role"
      }}
   }}
}}
""")


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
#  - json_parsed_content : 서비스 클래스를 생성하기 위한 정보
# TODO 토큰 초과시 로직 추가 필요
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
      # TODO 여기서 최대 토큰이 4096이 넘은 경우 처리가 필요
      logging.info(f"토큰 수: {result.usage_metadata}") 
      output_tokens = result.usage_metadata['output_tokens']
      if output_tokens > 4096:
         logging.warning(f"출력 토큰 수가 4096을 초과했습니다: {output_tokens}")

      json_parsed_content = json5.loads(result.content)
      return json_parsed_content

   except Exception:
      err_msg = "(전처리) 서비스 코드 생성 과정에서 LLM 호출하는 도중 오류가 발생했습니다."
      logging.exception(err_msg)
      raise LLMCallError(err_msg)