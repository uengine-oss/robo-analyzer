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
# llm = ChatOpenAI(model="gpt-4o", max_tokens=8000)
llm = ChatAnthropic(model="claude-3-5-sonnet-20241022", max_tokens=8000, temperature=0.1)
# TODO 엔티티클래스 필드 정보가 필요해보임
prompt = PromptTemplate.from_template(
"""
당신은 클린 아키텍처 원칙을 따르는 스프링부트 기반의 자바 애플리케이션을 개발하는 소프트웨어 엔지니어입니다. 
주어진 데이터를 기반으로 Repository Interface의 JPA Query Methods를 생성하는 작업을 맡았습니다.


Stored Procedure Code:
{repository_nodes}


Used Variable:
{used_variable_nodes}


Global Variable:
{global_variable_nodes}


생성될 JPA Query Methods는 {count}개입니다.
'Global Variable'들은 애플리케이션 전반에서 전역적으로 사용되는 변수들로 필요한 경우 활용하세요.


[SECTION 1] JPA Query Methods 생성 지침
===============================================
1. 변환 범위
   - 각 JSON 객체는 독립적으로 JPA Query Methods로 변환
   - 하나의 객체에서 복수의 Query Methods 생성 가능
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
    ]
}}
"""
)

# 역할: 테이블과 직접 연결된 PL/SQL 노드를 분석하여 JPA Repository 인터페이스를 생성하는 함수입니다.
#
# 매개변수: 
#   - repository_nodes : 테이블과 직접 연결된 PL/SQL 노드 정보
#   - used_variable_nodes : SQL에서 사용된 변수들의 정보
#   - convert_data_count : 하나의 SQL 문에서 생성될 JPA Query Method의 수
#
# 반환값: 
#   - result : LLM이 생성한 Repository 메서드 정보
def convert_repository_code(repository_nodes: dict, used_variable_nodes: dict, data_count: int, global_variable_nodes: dict) -> dict:
    
    try: 
        repository_nodes = json.dumps(repository_nodes, ensure_ascii=False, indent=2)
        used_variable_nodes = json.dumps(used_variable_nodes, ensure_ascii=False, indent=2)
        global_variable_nodes = json.dumps(global_variable_nodes, ensure_ascii=False, indent=2)

        chain = (
            RunnablePassthrough()
            | prompt
            | llm
            | JsonOutputParser()
        )
        result = chain.invoke({"repository_nodes": repository_nodes, "used_variable_nodes": used_variable_nodes, "count": data_count, "global_variable_nodes": global_variable_nodes})
        return result
    
    except Exception:
        err_msg = "리포지토리 인터페이스 생성 과정에서 LLM 호출하는 도중 오류가 발생했습니다."
        logging.error(err_msg, exc_info=False)
        raise LLMCallError(err_msg)