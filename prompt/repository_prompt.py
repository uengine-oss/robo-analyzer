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
llm = ChatAnthropic(model="claude-3-5-sonnet-20240620", max_tokens=8000)
prompt = PromptTemplate.from_template(
"""
당신은 클린 아키텍처 원칙을 따르는 스프링부트 기반의 자바 애플리케이션을 개발하는 소프트웨어 엔지니어입니다. 
주어진 데이터를 기반으로 Repository Interface의 JPA Query Methods를 생성하는 작업을 맡았습니다.


Stored Procedure Code:
{node_json}


Used Variable:
{variable_node}


생성될 JPA Query Methods는 {count}개입니다.


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


[SECTION 2] JPA Query Methods 필수 구현 규칙
===============================================
1. 반환 타입 규칙
   - SELECT 구문: 항상 전체 엔티티 객체 반환
   - 부분 필드 조회 지양
   - 예시: Person findById(Long id)

2. 읽기 전용 원칙
   - 모든 쿼리 메서드는 조회(Read) 작업만 수행
   - CUD(Create, Update, Delete) 작업은 비즈니스 로직으로 처리
   - 데이터 변경 작업은 서비스 레이어에서 구현

3. 복잡한 쿼리 처리
   - 복잡한 조건문의 경우 @Query 어노테이션 사용
   - JPQL 또는 네이티브 쿼리 활용

4. 날짜 기간 처리
   - TRUNC 함수 사용 금지
   - 시작일자(startDate)와 종료일자(endDate) 매개변수 사용
   - BETWEEN 절을 통한 기간 필터링
   - 예시:
     @Query("SELECT e FROM Entity e WHERE e.date BETWEEN :startDate AND :endDate")
     List<Entity> findByDateBetween(@Param("startDate") LocalDate startDate, 
                                  @Param("endDate") LocalDate endDate);


[SECTION 3] JPA Query Methods 작성 예시
===============================================
출력 형식:
- 인터페이스나 클래스 선언부 제외
- @Repository 어노테이션 제외
- 순수 쿼리 메서드만 'jpaQueryMethod'에 포함

예시 출력:
❌ 잘못된 형식:
@Repository
public interface EmployeeRepository extends JpaRepository<Employee, Long> {
    Person findByEmployeeId(Long employeeId);
}

✅ 올바른 형식:
@Query("적절한 쿼리문, value= 를 쓰지마세요")
\nType exampleJPAQueryMethod(@Param("Type TableColumn") Type exampleField, ...)


[SECTION 4] JSON 출력 형식
===============================================
부가 설명 없이 결과만을 포함하여, 다음 JSON 형식으로 반환하세요:
{{
    "analysis": [
        {{
            "tableName": "테이블명",
            "startLine": 시작라인번호,
            "endLine": 끝라인번호,
            "method": "@Query(\"SELECT e FROM Entity e WHERE e.column = :param\")\nType methodName(@Param(\"param\") Type param);"
        }}
    ]
}}
"""
)

# 역할 : 테이블과 직접적으로 연결된 노드를 기반으로, 리포지토리 인터페이스에 대한 정보를 받는 함수
# 매개변수: 
#   - node_data : 테이블과 직접적으로 연결된 노드
#   - variable_nodes_context : 해당 노드에서 사용된 변수목록
#   - convert_data_count : 분석할 데이터의 개수
# 반환값 : 
#   - result : 리포지토리 인터페이스에 대한 정보
def convert_repository_code(node_data, variable_nodes_context, data_count):
    
    try: 
        node_json = json.dumps(node_data)

        chain = (
            RunnablePassthrough()
            | prompt
            | llm
            | JsonOutputParser()
        )
        result = chain.invoke({"node_json": node_json, "variable_node": variable_nodes_context, "count": data_count})
        return result
    
    except Exception:
        err_msg = "리포지토리 인터페이스 생성 과정에서 LLM 호출하는 도중 오류가 발생했습니다."
        logging.exception(err_msg)
        raise LLMCallError(err_msg)