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

Service Signature:
{service_skeleton}

Used Variable:
{variable}

Context Range:
{context_range}

JPA Method List:
{jpa_method_list}


[SECTION 1] 입력 데이터 설명
===============================================
입력 데이터
   - Stored Procedure Code: 자바로 변환할 프로시저 코드
   - Service Signature: 구현할 메서드의 시그니처와 기본 구조
   - Used Variable: 현재 변수들의 할당값 정보 (이전 작업 결과)
   - Context Range: 변환 대상 코드의 시작/종료 라인 정보
   - JPA Method List: 현재 범위에서 사용 가능한 JPA 쿼리 메서드 목록

주요 작업
   - 'Service Signature'을 참고하여, CodePlaceHolder 위치에 들어갈 코드를 구현하세요.
   - 'Service Signature'는 제외하고 메서드 내부의 실제 구현 코드만 결과로 반환하세요.
   - 자바코드는 클린코드 및 가독성이 좋아야 하며, 들여쓰기가 적용된 상태로 반환하세요.

   
[SECTION 2] 코드 범위 처리 규칙
===============================================
1. Context Range 처리
   - Context Range에 명시된 모든 범위를 처리하여 정확히 {count}개의 code 항목 생성
   - 각 범위는 완전히 독립적으로 처리 (다른 범위와 병합하지 않음)
   - 각 범위는 시작 라인부터 종료 라인까지의 모든 코드를 완전한 형태로 변환 
   (예 : return문 있을 경우, 누락 없이 변환)

2. 범위 중첩 처리
   예시 1) 중첩 범위:
   - 1925~1977, 1942~1977가 주어진 경우
     * 1925~1977: 전체 범위의 완전한 코드
     * 1942~1977: 해당 범위만의 완전한 코드
   
   예시 2) 연속 범위:
   - 321~322, 321~321, 322~322가 주어진 경우
     * 321~322: 두 라인을 포함한 완전한 코드
     * 321~321: 321 라인의 독립적인 코드
     * 322~322: 322 라인의 독립적인 코드

3. 제외 사항
   - try-catch 블록 사용 금지
   - CHR(10)가 식별되는 경우 줄바꿈 \n으로 처리하지 말고 무시하세요.
   - Context Range에 명시되지 않은 범위는 처리하지 않음


[SECTION 3] 프로시저 호출 처리 규칙
===============================================   
1. 기본 원칙
   - 프로시저 호출이 발견되면 무조건 메서드 호출로 변환
   - JPA 쿼리 메서드 사용 금지

2. 외부 프로시저 호출
   형식: {{스키마명}}.{{프로시저명}}({{파라미터}})
   변환: {{스키마명}}Service.{{프로시저명}}({{파라미터}})
   예시:
   - 프로시저: TPX_PROJECT.SET_KEY(iProjKey)
   - 변환: tpxProjectService.setKey(iProjKey)  // 접두어 'i' 유지

3. 내부 프로시저 호출
   형식: {{프로시저명}}({{파라미터}})
   변환: 동일 클래스의 private 메서드로 호출
   예시:
   - 프로시저:SET_KEY(iProjKey)
   - 변환: setKey(iProjKey)  // 접두어 'p', 'i' 유지
   - 주의: 외부 클래스의 메서드로 호출이 아닌, 자신의 클래스 내부에 있는 private 메서드로 호출하는 것

4. 메서드명 규칙
   - 원본 프로시저명의 모든 접두어(i, p, o, v) 유지
   - 언더스코어를 카멜케이스로 변환
   - 예시: p_get_data -> pGetData, i_user_key -> iUserKey


[SECTION 4] SQL 구문 처리 규칙
=============================================== 
1. 기본 원칙
   - SELECT, UPDATE, INSERT, DELETE 키워드가 식별된 경우에만 적용
   - jpa_method_list에서 제공된 메서드만 사용
   - 시퀀스 관련 로직(NEXTVAL, CURRVAL 등)은 무시 (엔티티의 @GeneratedValue로 자동 처리)

2. SELECT 구문
   - jpa_method_list에서 적절한 조회 메서드 사용
   - 결과는 엔티티 객체나 컬렉션으로 받음
   - 조회를 했지만 데이터를 찾지 못한 경우 EntityNotFoundException 발생시키는 로직을 추가하세요.
      예시: 
      Employee employee = employeeRepository.findById(id);
      if (employee == null) {{
            throw new EntityNotFoundException("Employee not found with id: " + id);
      }}

2. UPDATE/MERGE 구문 변환
   - jpa_method_list에서 수정할 엔티티 먼저 조회하는 메서드 사용
   - 조회한 엔티티의 필드값을 자바 코드(비즈니스 로직)을 이용하여 변경
   - 만약 엔티티의 모든 필드를 업데이트 해야한다면, BeanUtils.copyProperties를 사용하세요.
   - save() 메서드로 변경사항 저장
   예시:
   Employee employee = employeeRepository.findById(id);
   employee.setStatus(newStatus);
   employeeRepository.save(employee);
   
3. INSERT 구문 변환
   - SYS_GUID() 함수는 UUID.randomUUID().toString()으로 변환
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

4. DELETE 구문
   - 적절한 삭제 메서드 사용

  
[SECTION 5] 예외 처리 규칙
===============================================
1. 기본 원칙
   - 'EXCEPTION' 키워드가 있는 코드 범위만 try-catch로 변환
   - 다른 모든 코드는 예외 처리 없이 순수 자바 코드로 변환
   - try 블록 내용은 항상 'CodePlaceHolder' 문자열로 유지

2. 예외 처리 패턴   
   try {{
      CodePlaceHolder
   }} catch (Exception e) {{
      * // EXCEPTION 블록의 변환 코드
   }}

   예시:
      * 원본 PL/SQL:
      203: 203: INSERT INTO TABLE VALUES row;
      204: 204: EXCEPTION WHEN OTHERS THEN
      205: 205:     RAISE_APPLICATION_ERROR(-20102, SQLERRM);
   
      * 자바 변환 결과:
      203~203: "repository.save(entity);"  
      204~205: "try {{ 
                  CodePlaceHolder 
               }} catch (Exception e) {{ 
                     throw new RuntimeException(\"Cannot insert: \" + e.getMessage());
               }}"
      
3. 주의사항
   - try 블록에는 반드시 'CodePlaceHolder' 문자열만 사용
   - EXCEPTION 키워드가 없는 코드는 절대 try-catch로 감싸지 않음
   - 코드 포맷은 들여쓰기가 적용된 상태로 반환하세요.

     
[SECTION 6] 변수 처리 규칙
===============================================
1. 변수 추적 원칙
   A. 추적 대상
      - SQL 실행 결과 할당
      - 조건문에 의한 값 변경
      - 연산에 의한 값 변경
      - 메서드 호출 결과 할당
      - 객체 상태 변경

   B. 추적 형식
      "변수명": "시점별 값 변경 내역 -> 다음 변경 내역 -> 최종 상태"
      예시:    
         {{
            "vEmpId": "초기값 NULL -> TB_EMPLOYEE 조회결과(EMP0001) 할당 -> UPDATE 조건절에서 사용",
            "vStatus": "초기값 'N' -> 사원정보 존재시 'Y' -> 처리완료후 'S'로 최종 설정"
         }}    


2. 변수 선언 및 할당 규칙
   A. 기본 원칙
      - 'Used Variable' 목록의 변수는 재선언 금지
      - 'Service Signature'에 있는 필드(변수)는 재선언 금지
      - 필요한 경우에만 새 변수 선언

   B. 객체 타입 변수 처리
      올바른 예:    
         * 초기 할당
         TpjEmployee vEmployee = new TpjEmployee();
         * 재할당 필요시
         vEmployee = tpjEmployeeRepository.findByEmpKey(iEmpKey);      ```
      
      잘못된 예:
         * 이미 선언된 변수를 재선언 (금지)
         TpjEmployee vEmployee = new TpjEmployee();
         TpjEmployee vEmployee = tpjEmployeeRepository.findByEmpKey(iEmpKey);      ```

   C. 기본 타입 변수 처리
      올바른 예:
         * 기존 변수에 값 할당
         vEmpName = "홍길동";
         vCount = 1;
      잘못된 예:
         * 이미 선언된 변수를 재선언 (금지)
         String vEmpName = "홍길동";
         Integer vCount = 1;

               
[SECTION 7] 날짜/시간 처리 규칙
===============================================
1. 필드명에 Time이 포함된 경우(*Time, *DateTime, *At으로 끝나는 필드)
   - 해당 필드는 무조건 LocalDateTime 타입이므로, 형변환이 필요하다면 아래
   
   * 해당 변수가 선언되었다고 가정 
   LocalDate vCurrentTime = LocalDate.now();
   
   * 잘못된 예
   vRow.setEndTime(vCurrentTime);  // LocalDate를 LocalDateTime 필드에 직접 할당 불가
   
   * 올바른 예
   vRow.setEndTime(vCurrentTime.atTime(LocalTime.now()));     // 현재 시간 포함

               
[SECTION 8] 자바 코드 생성시 JSON 문자열 처리 규칙
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

    
[ **IMPORTANT 반드시 지켜야하는 필수 사항  ** ]
1. 프로시저 호출 로직 식별시 절대로 JPA 쿼리 메서드를 사용하지마세요. 이건 단순히 서비스 클래스 내에 메서드 호출일 뿐입니다:
   올바른 예) p_GET_ROW(ID_KEY) -> pGetRow(idKey) // 프로시저 호출을 단순 메서드 호출 형태로 전환
   잘못된 예) p_GET_ROW(ID_KEY) -> findbyId(idKey)  // JPA 쿼리 메서드를 사용하면 안됨

2. 제공된 모든 Context Range에 대해서 코드 변환을 완료해야 합니다. 'code' 요소 개수는 {count}개와 일치해야 하며, 누락 및 생략 없이 결과를 생성하세요. 반드시 'analysis'의 'code' 요소의 개수가 일치한지 검토하세요. 단 한 개의 누락 및 생략이 있어서는 안됩니다.

3. Exception에 해당하는 구문 처리시 try문에는 'CodePlaceHolder'만 있어야합니다. 

4. 'CHR(10)'가 식별되는 경우 줄바꿈 '\n'으로 처리하지 말고 무시하세요.


[SECTION 9] JSON 출력 형식
===============================================
부가 설명 없이 결과만을 포함하여, 다음과 같은 dictionary(사전) 형태의 JSON 형식으로 반환하세요:
{{
   "analysis": {{
      "code": {{
         "startLine~endLine": "Java Code",
         "startLine~endLine": "Java Code"
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
#  - service_skeleton : 생성될 Java Service 클래스의 기본 구조
#  - variable_list : PL/SQL에서 사용된 변수들의 정보
#  - jpa_query_methods : 사용 가능한 JPA Repository 메서드 목록
#  - command_class_variable : Command 클래스의 필드 정보
#  - context_range : 코드 변환 범위 정보
#  - count : context_range의 범위 개수
# 반환값: 
#  - json_parsed_content : LLM이 생성한 서비스 메서드 정보
# TODO 토큰 초과시 로직 추가 필요
def convert_service_code(convert_sp_code, service_skeleton, variable_list, command_class_variable, context_range, count, jpa_method_list):
   
   try:  
      context_range_json = json.dumps(context_range, indent=2)
      command_class_variable = json.dumps(command_class_variable, ensure_ascii=False, indent=2)

      chain = (
         RunnablePassthrough()
         | prompt
         | llm
      )
      result = chain.invoke({"code": convert_sp_code, "service_skeleton": service_skeleton, "variable": variable_list, "command_variables": command_class_variable, "context_range": context_range_json, "count": count, "jpa_method_list": jpa_method_list})
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