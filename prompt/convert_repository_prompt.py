import json
import logging
import os
from langchain.globals import set_llm_cache
from langchain_community.cache import SQLiteCache
from langchain_openai import ChatOpenAI
from langchain_core.output_parsers import JsonOutputParser
from langchain.prompts import PromptTemplate
from langchain.schema.runnable import RunnablePassthrough
from langchain_anthropic import ChatAnthropic
from util.exception import LLMCallError

db_path = os.path.join(os.path.dirname(__file__), 'langchain.db')
set_llm_cache(SQLiteCache(database_path=db_path))
llm = ChatAnthropic(model="claude-3-5-sonnet-20241022", max_tokens=8000, temperature=0.1)


# MyBatis Mapper 인터페이스 생성 프롬프트
myBatis_prompt = PromptTemplate.from_template(
"""
당신은 클린 아키텍처 원칙을 따르는 스프링부트 기반의 자바 애플리케이션을 개발하는 소프트웨어 엔지니어입니다. 
주어진 데이터를 기반으로 MyBatis Mapper 인터페이스의 메서드를 생성하는 작업을 맡았습니다.


Stored Procedure Code:
{repository_nodes}


Used Variable:
{used_variable_nodes}


Global Variable:
{global_variable_nodes}


Sequence Info:
{sequence_data}


생성될 Mapper 메서드는 {count}개입니다.
'Global Variable'들은 애플리케이션 전반에서 전역적으로 사용되는 변수들로 필요한 경우 활용하세요.


[SECTION 1] Mapper 메서드 생성 지침
===============================================
1. 변환 범위
   - 각 JSON 객체는 독립적으로 Mapper 메서드로 변환
   - ...code... 표시된 부분은 제외하고 변환

2. 변환 규칙
   - 각 JSON 객체는 자신의 Stored Procedure Code만 참조
   - 다른 객체의 코드는 참고하지 않음
   - 명명 규칙: 단수형 파스칼 케이스 (예: Employee)

3. 매개변수 처리
   - 모든 매개변수에 @Param 어노테이션 필수
   - 'Used Variable' 목록의 모든 변수는 매개변수로 포함
   - 누락된 매개변수 없이 완전한 매핑 필요

4. 시퀀스 처리
   - 별도의 시퀀스 조회 메서드 생성
   - 시퀀스 메서드 명명 규칙: getNext[시퀀스명]
   - 반환 타입은 Long으로 통일
   - @Select 어노테이션으로 직접 시퀀스 조회
   - 시퀀스가 사용되는 필드를 식별하여 'field' 필드에 포함
   - 예시: @Select("SELECT SEQUENCE_NAME.NEXTVAL FROM DUAL")\nLong getSequenceNextVal();

    
[SECTION 2] Mapper 메서드 필수 구현 규칙
===============================================
1. 반환 타입 규칙
   - SELECT 단건 조회: 엔티티 객체
   - SELECT 목록 조회: List<Employee>
   - INSERT/UPDATE/DELETE: void
   - 예시: List<Employee> findAll();
   - 엔티티 이름은 전달된 테이블 명을 그대로 파스칼 케이스로 전환하여 사용하세요. (예: TPJ_EMPLOYEE -> TpjEmployee)

2. 어노테이션 규칙
   - SELECT: @Select
   - INSERT: @Insert
   - UPDATE: @Update
   - DELETE: @Delete
   - 복잡한 동적 쿼리는 어노테이션 생략 (XML에서 처리)

3. 매개변수 규칙
   - 모든 매개변수에 @Param 어노테이션 사용
   - 예시: @Param("empNo") String empNo

4. 명명 규칙
   - 조회: findBy[조건]
   - 삽입: insert[엔티티명]
   - 수정: update[엔티티명]
   - 삭제: deleteBy[조건]

5. 날짜 기간 처리
   - TRUNC 함수 사용 금지
   - 시작일자(startDate)와 종료일자(endDate) 매개변수 사용
   - BETWEEN 절을 통한 기간 필터링

   
[SECTION 3] Mapper 메서드 작성 예시
===============================================
출력 형식:
- @Mapper 어노테이션 제외
- 순수 메서드만 'method'에 포함
- SQL 어노테이션과 메서드 시그니처를 개행문자(\\n)로 구분

예시 출력:
@Select("SELECT * FROM EMPLOYEE WHERE emp_no = #{{empNo}}")
\nEmployee findByEmpNo(@Param("empNo") String empNo);


[SECTION 4] JSON 출력 형식
===============================================
부가 설명 없이 결과만을 포함하여, 다음 JSON 형식으로 반환하세요:
{{
    "analysis": [
        {{
            "tableName": "테이블명",
            "method": "@Select(\"SELECT * FROM table WHERE id = #{{id}}\")\nType methodName(@Param(\"id\") Type id);",
            "range": [
               {{
                  "startLine": 시작라인번호,
                  "endLine": 끝라인번호,
               }},
            ],
        }}
    ],
    "seq_method": [
        {{
            "method": "@Select("SELECT SAMPLE_SEQ.NEXTVAL FROM DUAL")\nLong getNextEmployeeSequence();",
            "field": "필드명",
        }}
    ]
}}
"""
)



# JPA Repository 인터페이스 생성 프롬프트
jpa_prompt = PromptTemplate.from_template(
"""
당신은 클린 아키텍처 원칙을 따르는 스프링부트 기반의 자바 애플리케이션을 개발하는 소프트웨어 엔지니어입니다. 
주어진 데이터를 기반으로 Repository Interface의 JPA Query Methods를 생성하는 작업을 맡았습니다.


Stored Procedure Code:
{repository_nodes}


Used Variable:
{used_variable_nodes}


Global Variable:
{global_variable_nodes}


Sequence Info:
{sequence_data}


생성될 JPA Query Methods는 {count}개입니다.
'Global Variable'들은 애플리케이션 전반에서 전역적으로 사용되는 변수들로 필요한 경우 활용하세요.


[SECTION 1] JPA Query Methods 생성 지침
===============================================
1. 변환 범위
   - 각 JSON 객체는 독립적으로 JPA Query Methods로 변환
   - ...code... 표시된 부분은 제외하고 변환

2. 변환 규칙
   - 각 JSON 객체는 자신의 Stored Procedure Code만 참조
   - 다른 객체의 코드는 참고하지 않음
   - Entity 명명 규칙: 단수형 파스칼 케이스 (예: Employee)

3. 매개변수 처리
   - 'Used Variable' 목록의 모든 변수는 매개변수로 포함
   - 누락된 매개변수 없이 완전한 매핑 필요
   - 'Used Variable'에 명시되지 않은 변수라도 SP 코드에서 식별되면 적절한 타입으로 매개변수화

4. 동일한 JPA 쿼리 메서드 처리
  - 서로 다른 객체에서 @query문이 같을 경우 똑같은 JPA 쿼리 메서드로 보세요
  - 별도로 'method'를 생성하지말고 똑같은 'method'의 'ragne'에 범위만 추가하세요
   
  
5. 시퀀스 처리
  - 별도의 시퀀스 조회 메서드 생성
  - 시퀀스 메서드 명명 규칙: getNext[시퀀스명]
  - 반환 타입은 Long으로 통일
  - @Select 어노테이션으로 직접 시퀀스 조회
  - 시퀀스가 사용되는 필드를 식별하여 'field' 필드에 포함
  - 예시: "@Query(value = "SELECT SAMPLE_SEQ.NEXTVAL FROM DUAL", nativeQuery = true)\nLong getSequenceNextVal();"


[SECTION 2] JPA Query Methods 필수 구현 규칙
===============================================
1. 반환 타입 규칙
   - SELECT 구문: 항상 전체 엔티티 객체 반환
   - 엔티티 이름은 전달된 테이블 명을 그대로 파스칼 케이스로 전환하여 사용하세요. (예: TPJ_EMPLOYEE -> TpjEmployee)
   - 부분 필드 조회 지양
   - 예시: Person findById(Long id)

2. 읽기 전용 원칙
   - 모든 쿼리는 SELECT 문으로만 변환
   - UPDATE/INSERT/DELETE 문도 SELECT 문으로 변환하여 데이터 조회
   - 예시:
     * UPDATE 문의 경우: 업데이트할 데이터를 먼저 조회
     * 입력: "UPDATE Users SET name = 'John' WHERE id = 1"
     * 변환: "SELECT * FROM Users WHERE id = 1"
   
   - INSERT 문의 경우: 삽입할 데이터의 중복 체크를 위한 조회
   - DELETE 문의 경우: 삭제할 데이터를 먼저 조회
   - 데이터 변경 작업은 서비스 레이어에서 구현
   예시:
      Employee employee = employeeRepository.findById(id);
      employee.setStatus(newStatus);
      employeeRepository.save(employee);

3. 복잡한 쿼리 처리
   - 복잡한 조건문의 경우 @Query 어노테이션 사용
   - JPQL 또는 네이티브 쿼리 활용

4. 날짜 기간 처리
   - TRUNC 함수 사용 금지
   - 시작일자(startDate)와 종료일자(endDate) 매개변수 사용
   - BETWEEN 절을 통한 기간 필터링
   

[SECTION 3] JPA Query Methods 작성 예시
===============================================
출력 형식:
- 인터페이스나 클래스 선언부 제외
- @Repository 어노테이션 제외
- 순수 쿼리 메서드만 'jpaQueryMethod'에 포함
- 'method' 필드는 반드시 @Query 어노테이션과 메소드 시그니처를 개행문자(\n)로 구분하여 포함해야 합니다

예시 출력:
@Query("적절한 쿼리문, value= 를 쓰지마세요")
\nType exampleJPAQueryMethod(@Param("Type TableColumn") Type exampleField, ...)


[SECTION 4] JSON 출력 형식
===============================================
부가 설명 없이 결과만을 포함하여, 다음 JSON 형식으로 반환하세요:
{{
    "analysis": [
        {{
            "tableName": "테이블명",
            "method": "@Query(\"SELECT e FROM Entity e WHERE e.column = :param\")\nType methodName(@Param(\"param\") Type param);"
            "range": [
               {{
                  "startLine": 시작라인번호,
                  "endLine": 끝라인번호,
               }},
            ]
        }}
    ],
    "seq_method": [
        {{
         "method": "@Query(value = "SELECT SAMPLE_SEQ.NEXTVAL FROM DUAL", nativeQuery = true)\nLong getSequenceNextVal();",
         "field": "필드명",
        }}
    ]
}}
"""
)

# 역할: 테이블과 직접 연결된 PL/SQL 노드를 분석하여 Repository 인터페이스를 생성하는 함수입니다.
#
# 매개변수: 
#   - repository_nodes : 테이블과 직접 연결된 PL/SQL 노드 정보
#   - used_variable_nodes : SQL에서 사용된 변수들의 정보
#   - convert_data_count : 하나의 SQL 문에서 생성될 Query Method의 수
#   - global_variable_nodes : 전역 변수 노드 정보
#   - sequence_data : 시퀀스 정보
#   - orm_type : 사용할 ORM 유형
#
# 반환값: 
#   - result : LLM이 생성한 Repository 메서드 정보
def convert_repository_code(repository_nodes: dict, used_variable_nodes: dict, data_count: int, global_variable_nodes: dict, sequence_data: str, orm_type: str) -> dict:
    
    try: 
        repository_nodes = json.dumps(repository_nodes, ensure_ascii=False, indent=2)
        used_variable_nodes = json.dumps(used_variable_nodes, ensure_ascii=False, indent=2)
        global_variable_nodes = json.dumps(global_variable_nodes, ensure_ascii=False, indent=2)
        prompt_data = {
            "repository_nodes": repository_nodes,
            "used_variable_nodes": used_variable_nodes,
            "count": data_count,
            "global_variable_nodes": global_variable_nodes,
            "sequence_data": sequence_data
        }


        # * 프레임워크별 프롬프트 선택
        selected_prompt = jpa_prompt if orm_type == "jpa" else myBatis_prompt   
  

        chain = (
            RunnablePassthrough()
            | selected_prompt
            | llm
            | JsonOutputParser()
        )
        result = chain.invoke(prompt_data)
        return result
    
    except Exception:
        err_msg = "리포지토리 인터페이스 생성 과정에서 LLM 호출하는 도중 오류가 발생했습니다."
        logging.error(err_msg)
        raise LLMCallError(err_msg)