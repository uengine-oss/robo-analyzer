import json
import logging
import os
import time
from langchain_core.globals import set_llm_cache
from langchain_community.cache import SQLiteCache
from langchain_core.prompts import PromptTemplate
from langchain_core.runnables import RunnablePassthrough
from util.llm_client import get_llm
from util.exception import LLMCallError
from langchain_core.output_parsers import JsonOutputParser

db_path = os.path.join(os.path.dirname(__file__), 'langchain.db')
set_llm_cache(SQLiteCache(database_path=db_path))

# jpa 기반의 서비스 레이어 프롬프트
jpa_prompt = PromptTemplate.from_template(
"""
당신은 클린 아키텍처 원칙을 따르는 스프링부트 기반의 자바 애플리케이션을 개발하는 소프트웨어 엔지니어입니다. 
주어진 Stored Procedure Code 전체를 기반으로 서비스 클래스의 메서드 바디 부분만 간결하고 가독성 좋은 클린 코드 형태로 구현하는 작업을 맡았습니다.


사용자 언어 설정 : {locale}, 입니다. 이를 반영하여 결과를 생성해주세요.


[입력 데이터]
Stored Procedure Code:
{code}

Service Signature:
{service_skeleton}

Used Variable:
{variable}

JPA Method List:
{query_method_list}

Sequence Method List:
{sequence_methods}

Parent Context (부모 노드의 Java 스켈레톤):
{parent_code}


[SECTION 1] 입력 데이터 설명
===============================================
1. Stored Procedure Code
   - 자바로 변환할 전체 프로시저 코드 블록
   
2. Service Signature
   - 구현할 메서드의 시그니처와 기본 구조
   - CodePlaceHolder 위치에 코드를 삽입해야 함
   
3. Used Variable
   - 현재 변수들의 할당값 정보 (이전 작업 결과)
   
4. JPA Method List
   - 사용 가능한 JPA 쿼리 메서드 목록
   
5. Sequence Method List
   - 사용 가능한 시퀀스 메서드 목록

6. Parent Context (부모 노드의 Java 스켈레톤)
   - 현재 처리 중인 코드가 큰 부모 노드의 자식인 경우, 부모의 Java 스켈레톤이 제공됩니다
   - 부모 스켈레톤에는 '...code...' 플레이스홀더가 있으며, 현재 코드는 이 안에 들어갈 내용입니다
   - 부모 구조를 참고하여 if/else 조건, 변수 스코프 등을 올바르게 처리하세요
   - Parent Context가 비어있으면 최상위 레벨 코드입니다


[SECTION 2] 작업 지침 (반드시 준수)
===============================================

⚠️⚠️⚠️ 최우선 원칙: SP 코드의 의미와 흐름을 그대로 유지 ⚠️⚠️⚠️

1. Parent Context 활용 (부모 노드가 있는 경우)
   - Parent Context가 제공되면, 현재 SP 코드는 부모 Java 스켈레톤의 '...code...' 부분에 들어갈 코드입니다
   - 부모 스켈레톤의 if/else/while/for 구조를 참고하여 현재 코드의 위치를 파악하세요
   - 부모에서 이미 선언된 변수는 재선언하지 마세요
   - 부모의 조건문 내부에 있다면, 해당 스코프에 맞게 코드를 생성하세요
   - Parent Context가 비어있으면 최상위 레벨 코드입니다.

2. 로직 충실성 (가장 중요!)
   - 원본 Stored Procedure의 로직 흐름, 순서, 의미를 절대 변경하지 마세요
   - SP 코드에 있는 그대로를 Java로 충실히 번역만 하세요
   - 임의로 로직을 재구성, 최적화, 단순화하지 마세요
   - 코드 순서를 바꾸지 마세요

3. 제어 구조 정확성
   A. IF-ELSIF-ELSE 변환 (매우 중요!)
      원칙:
      - SP의 IF-ELSIF-ELSE는 반드시 Java의 if-else if-else로 변환하세요
      - ELSIF를 중첩 if로 변환하면 안 됩니다! (절대 금지)
      - 조건의 순서를 절대 바꾸지 마세요
      - 조건을 병합하거나 분리하지 마세요
      
      예시 1) 원본 SP 코드:
      IF vCount = 0 AND pAction = 'A' THEN
         코드블록1
      ELSIF vCount = 0 THEN
         코드블록2
      END IF;
      
      ✅ 올바른 Java 변환 (else if 사용):
      if (vCount == 0 && pAction.equals("A")) {{
         코드블록1
      }} else if (vCount == 0) {{
         코드블록2
      }}
      
      ❌ 절대 금지 - 잘못된 변환 (중첩 if):
      if (vCount == 0 && pAction.equals("A")) {{
         코드블록1
         if (vCount == 0) {{  // ← 이렇게 중첩하면 안됨!
            코드블록2
         }}
      }}
      
      핵심 차이:
      - ELSIF는 첫 번째 IF와 **같은 레벨**의 조건 분기입니다
      - else if는 첫 번째 if가 **거짓일 때** 실행됩니다
      - 중첩 if는 첫 번째 if가 **참일 때** 실행됩니다 (완전히 반대 의미!)
      
   B. 중첩 구조 유지
      - 원본에 중첩된 IF가 있다면 Java에서도 정확히 같은 레벨로 중첩하세요
      - 중첩 깊이(depth)를 변경하지 마세요
      
   C. 반복문 구조
      - LOOP, FOR 등 반복 로직을 정확히 유지하세요
      - 반복 순서, 조건, 범위를 변경하지 마세요
      
   D. RETURN 문 위치
      - SP 코드의 RETURN 위치를 정확히 유지하세요
      - 임의로 early return으로 변경하지 마세요

   즉, SP 코드의 원본 의미 및 순서 흐름을 그대로 유지하고 커스텀 및 변형하지 말 것

3. 출력 형식
   - 전달된 Stored Procedure Code만 정확히 자바로 전환하세요
   - 메서드 시그니처(public ... method(...))는 절대 포함하지 마세요!
   - 오직 메서드 내부의 실행 코드만 반환하세요!
   - 클린코드 및 가독성이 좋아야 하며, 들여쓰기를 적용하세요


[SECTION 3] 프로시저 호출 처리 규칙
===============================================   
1. 기본 원칙
   - 프로시저 호출이 발견되면 무조건 메서드 호출로 변환
   - JPA 쿼리 메서드 사용 금지 (findById(), save(), delete() 같은 메서드는 절대 사용하지 않습니다.)

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

5. 메서드 파라미터 규칙
   - 파라미터는 알파벳 순으로 정렬하여 반환하세요.
   예 : 
   - 원본: p_get_data(iUserKey, iProjKey, iDeptCode)
   - 변환: pGetData(iDeptCode, iProjKey, iUserKey)
   

[SECTION 4] SQL 구문 처리 규칙
=============================================== 
1. 기본 원칙
   - SELECT, UPDATE, INSERT, DELETE 키워드가 식별된 경우에만 적용
   - JPA Method List에서 제공된 메서드만 사용

2. SELECT 구문
   - JPA Method List에서 적절한 조회 메서드 사용
   - 결과는 엔티티 객체나 컬렉션으로 받음
   - 조회를 했지만 데이터를 찾지 못한 경우 EntityNotFoundException 발생시키는 로직을 추가하세요.
   - 조회 결과를 새로운 변수 및 객체를 생성해서 저장하지말고, 기존에 선언된 객체에 재할당하세요.
      예시: 
      Employee employee = employeeRepository.findById(id);
      if (employee == null) {{
            throw new EntityNotFoundException("Employee not found with id: " + id);
      }}

3. UPDATE/MERGE 구문 변환
   - JPA Method List에서 수정할 엔티티 먼저 조회하는 메서드 사용
   - 조회한 엔티티의 필드값을 자바 코드(비즈니스 로직)을 이용하여 변경
   - 만약 엔티티의 모든 필드를 업데이트 해야한다면, BeanUtils.copyProperties를 사용하세요.
   - save() 메서드로 변경사항 저장
   예시:
   Employee employee = employeeRepository.findById(id);
   employee.setStatus(newStatus);
   employeeRepository.save(employee);
   
4. INSERT 구문 변환
   - SYS_GUID() 함수는 UUID.randomUUID().toString()으로 변환
   - INSERT INTO ... SELECT FROM 구조인 경우:
      * SELECT 부분만 JPA Method List의 조회 메서드로 변환
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

5. DELETE 구문
   - 적절한 삭제 메서드 사용

6. 시퀀스 처리
   - 시퀀스 관련 로직(NEXTVAL, CURRVAL 등)이 식별되면 Sequence Method List 확인
   - Sequence Method List에 해당 시퀀스 필드가 존재하는 경우, 해당 시퀀스 메서드를 사용
   
   * 예시:
   - 원본: SELECT SEQ_USER_KEY.NEXTVAL FROM DUAL
   - 변환: Long nextVal = sequenceMapper.getNextUserKeySequence();


[SECTION 5] 변수 처리 규칙
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
      - 새 변수 선언 조건:
        * 'Service Signature'에 존재하지 않는 변수 AND
        * 'Used Variable' 목록에도 정보가 없는 경우
        * → 해당 변수를 새로 선언하세요
      - 변수 선언 시 SP 코드의 데이터 타입을 Java 타입으로 매핑하여 선언

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

              
[SECTION 6] 날짜/시간 처리 규칙
===============================================
1. 필드명에 Time이 포함된 경우(*Time, *DateTime, *At으로 끝나는 필드)
   - 해당 필드는 무조건 LocalDateTime 타입이므로, 형변환이 필요하다면 아래
   
   * 해당 변수가 선언되었다고 가정 
   LocalDate vCurrentTime = LocalDate.now();
   
   * 잘못된 예
   vRow.setEndTime(vCurrentTime);  // LocalDate를 LocalDateTime 필드에 직접 할당 불가
   
   * 올바른 예
   vRow.setEndTime(vCurrentTime.atTime(LocalTime.now()));     // 현재 시간 포함

              
[SECTION 7] 자바 코드 생성시 JSON 문자열 처리 규칙
===============================================
1. 특수 문자 이스케이프 처리
   줄바꿈은 반드시 \n으로 표현할 것 (실제 줄바꿈 사용 금지)
   특수 문자 이스케이프 처리 규칙
   - 줄바꿈: \\n   - 큰따옴표: \\
   - 백슬래시: \\\\
   - 작은따옴표: \\'
   올바르게 이스케이프 처리된 예시 : 
   {{
    "method": "    @PostMapping(\\\"/insEmployee\\\")\\n    public ResponseEntity<String> insEmployee(@RequestBody InsEmployeeCommand command) {{\\n        tpxEmployeeService.insEmployee(command.getEmpKey(), command.getEmpName(), command.getDeptCode(), command.getRegularYn());\\n        return ResponseEntity.ok(\\\"Employee inserted successfully\\\");\\n    }}"
   }}

2. 절대로 코드블록(```)이나 추가 설명을 포함하지 말 것.
   
3. 문자열 작성 규칙
   - 문자열 연결 시 '+' 연산자 사용 금지
   - 하나의 연속된 문자열로 작성
   - 모든 따옴표 이스케이프 처리 확인
   - JSON 파싱 오류 방지를 위한 철저한 이스케이프 처리     

    
[ **IMPORTANT 반드시 지켜야하는 필수 사항  ** ]

1. 가장 중요: 메서드 바디만 반환
- 절대로 메서드 시그니처를 포함하지 마세요!
- 오직 메서드 {{ }} 안의 내용만 반환하세요!

잘못된 예시:
public ResponseEntity<?> handleMethod(...) {{
      int x = 0;
      return ResponseEntity.ok("success");
}}

올바른 예시:
int x = 0;
return ResponseEntity.ok("success");

2. 프로시저 호출 로직 식별시 절대로 JPA 쿼리 메서드를 사용하지마세요. 이건 단순히 서비스 클래스 내에 메서드 호출일 뿐입니다:
   올바른 예) p_GET_ROW(ID_KEY) -> pGetRow(idKey) // 프로시저 호출을 단순 메서드 호출 형태로 전환
   잘못된 예) p_GET_ROW(ID_KEY) -> findbyId(idKey)  // JPA 쿼리 메서드를 사용하면 안됨
   프로시저 호출시 이름이 GET_ROW, INPUT, DELETE 등의 이름이 포함되어 있어도, 그냥 메서드 호출 로직으로만 전환하고, Mapper 메서드는 절대 사용하지 않습니다.
   * 예 : p_GET_ROW(ID_KEY) -> pGetRow(idKey) // 프로시저 호출을 단순 메서드 호출 형태로 전환, INPUT(vRow) -> input(vRow) // 프로시저 호출을 단순 메서드 호출 형태로 전환
   
3. 본 작업은 단일 코드 변환이며, 결과의 'code'는 반드시 하나의 문자열이어야 합니다.

4. 'CHR(10)'가 식별되는 경우 줄바꿈 '\n'으로 처리하지 말고 무시하세요.


[SECTION 8] JSON 출력 형식
===============================================
부가 설명 없이 결과만을 포함하여, 백틱이 없이 다음과 같은 dictionary(사전) 형태의 순수 JSON 형식으로 반환하세요:
{{
   "analysis": {{
      "code": "메서드 바디 내용만 (메서드 시그니처 제외, 중괄호 제외)",
      "variables": {{
         "name": "initialized value and role",
         "name": "initialized value and role"
      }}
   }}
}}
""")


def convert_service_code(convert_sp_code: str, service_skeleton: str, variable_list: str, command_class_variable: str, query_method_list: str, sequence_methods:list, api_key: str, locale: str, parent_code: str = "") -> dict:
    
    try:  
        command_class_variable = json.dumps(command_class_variable, ensure_ascii=False, indent=2)
        prompt_data = {
            "code": convert_sp_code,
            "service_skeleton": service_skeleton,
            "variable": variable_list,
            "command_variables": command_class_variable,
            "query_method_list": query_method_list,
            "sequence_methods": sequence_methods,
            "locale": locale,
            "parent_code": parent_code
        }
        
        llm = get_llm(max_tokens=8192, api_key=api_key)

        parser = JsonOutputParser()

        chain = (
            RunnablePassthrough()
            | jpa_prompt.partial(format_instructions=parser.get_format_instructions())
            | llm
            | parser
        )

        max_attempts = 10
        last_error = None

        for attempt in range(1, max_attempts + 1):
            try:
                result = chain.invoke(prompt_data)

                if not result or not isinstance(result, dict):
                    raise ValueError("LLM 결과가 비어있거나 잘못된 형식입니다.")

                analysis = result.get("analysis")
                if not analysis or not isinstance(analysis, dict):
                    raise ValueError("'analysis' 키가 없거나 형식이 올바르지 않습니다.")

                code_obj = analysis.get("code")
                if not code_obj or not isinstance(code_obj, str) or len(code_obj.strip()) == 0:
                    raise ValueError("'analysis.code'가 비어있거나 문자열 형식이 아닙니다.")

                return result
            except Exception as retry_error:
                last_error = retry_error
                logging.warning(f"LLM 호출/파싱 실패로 재시도 진행: {attempt}/{max_attempts} - {retry_error}")
                time.sleep(min(0.5 * attempt, 5))

        raise RuntimeError(f"LLM 결과 생성 실패(재시도 {max_attempts}회 모두 실패): {last_error}")

    except Exception as e:
        err_msg = f"(전처리) 서비스 코드 생성 과정에서 LLM 호출 중 오류 발생: {str(e)}"
        logging.error(err_msg)
        raise LLMCallError(err_msg)


# ----- EXCEPTION 노드 처리 프롬프트 -----
exception_prompt = PromptTemplate.from_template(
"""
당신은 PL/SQL EXCEPTION 블록을 Java try-catch 구조로 변환하는 전문가입니다.

사용자 언어 설정: {locale}

[입력 데이터]
===============================================
EXCEPTION 블록 코드:
{exception_code}


[변환 규칙]
===============================================
1. Java try-catch 구조 생성
   - try 블록 내부에 "CodePlaceHolder" 문자열만 삽입
   - catch 블록에 EXCEPTION 블록의 예외 처리 로직 변환
   
2. 예외 타입 매핑
   - WHEN OTHERS -> catch (Exception e)
   - WHEN NO_DATA_FOUND -> catch (EntityNotFoundException e)
   - WHEN TOO_MANY_ROWS -> catch (NonUniqueResultException e)
   - WHEN VALUE_ERROR -> catch (IllegalArgumentException e)
   - WHEN DUP_VAL_ON_INDEX -> catch (DataIntegrityViolationException e)
   
3. 예외 처리 로직 변환
   - RAISE_APPLICATION_ERROR -> throw new RuntimeException(message)
   - ROLLBACK -> 생략 (스프링 @Transactional에서 자동 처리)
   - DBMS_OUTPUT.PUT_LINE -> logger.error(message)
   - 기타 로직은 Java로 변환

⚠️ 핵심: try 블록 내부는 반드시 "CodePlaceHolder" 문자열만 포함!


[예시]
PL/SQL:
EXCEPTION
    WHEN OTHERS THEN
        RAISE_APPLICATION_ERROR(-20001, '오류 발생');
        
Java:
{{
    "code": "try {{\\n    CodePlaceHolder\\n}} catch (Exception e) {{\\n    throw new RuntimeException(\\"오류 발생\\");\\n}}"
}}


[출력 형식]
===============================================
JSON 형식으로 반환:
{{
    "code": "try {{\\n    CodePlaceHolder\\n}} catch (Exception e) {{\\n    logger.error(e.getMessage());\\n}}"
}}
"""
)

def convert_exception_code(exception_sp_code: str, api_key: str, locale: str = 'ko'):
    """
    EXCEPTION 노드를 Java try-catch 구조로 변환
    
    Args:
        exception_sp_code: EXCEPTION 블록의 PL/SQL 코드
        api_key: LLM API 키
        locale: 언어 설정
    
    Returns:
        dict: {"code": "try { CodePlaceHolder } catch (...) { ... }"}
    """
    try:
        llm = get_llm(max_tokens=8192, api_key=api_key)
        
        chain = (
            RunnablePassthrough()
            | exception_prompt
            | llm
            | JsonOutputParser()
        )
        
        result = chain.invoke({
            "exception_code": exception_sp_code,
            "locale": locale
        })
        
        return result
        
    except Exception as e:
        err_msg = f"EXCEPTION 코드 변환 중 LLM 호출 오류: {str(e)}"
        logging.error(err_msg)
        raise LLMCallError(err_msg)