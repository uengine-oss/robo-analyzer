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


아래의 지시사항을 참고하여, 제공된 데이터를 분석하고, JPA Query Methods를 생성하십시오:
1. Stored Procedure Code에서 각 JSON 객체들은 JPA Query Methods로 전환되어야 합니다. 하나의 객체에서 여러 개의 JPA Query Methods가 생성될 수 도 있습니다.
2. 전달된 Json 객체에 해당하는 Stored Procedure Code만 JPA Query Methods로 전환하고, ...code... 부분이 있다면, 나머지 부분만 JPA Query Methods로 전환하세요. 서로 Json 객체 끼리 범위가 겹친다고 해서, 다른 객체에 있는 스토어드 프로시저 코드를 참고하지마세요. 
3. 'Used Variable'는 현재 구문에서 사용된 변수 목록으로, JPA Query Methods의 매개변수 식별에 사용하세요. 이떄 누락된 매개변수가 없어야 합니다.
4. 모든 Entity의 이름은 복수형이 아닌 단수형인 파스칼 표기법으로 표현됩니다. (예: Employees -> Employee)


생성될 JPA Query Methods는 {count}개입니다.


JPA Query Methods 생성시 반드시 숙지해야할 요구사항:
1. SELECT 구문의 경우, 특정 필드(컬럼)이 아닌 전체 필드를 포함하는 객체 자체를 반환하는 쿼리 메서드로 전환하세요.(예 : Person findById(Long id))
2. UPDATE, MERGE, DELETE, INSERT 상관없이 모든 JPA Query Methods는 데이터를 조회(Read)하는 데 중점 두고, 생성하세요. 데이터 삭제, 수정 삽입은  JPA Query Methods에서 직접 구현하지 않고, 자바 비즈니스 로직으로 해결할 것입니다. 
3. 쿼리가 매우 복잡한 경우 @Query 어노테이션을 사용하세요.
4. 특정 기간 및 시간 내 데이터를 조회할 때는 TRUNC 함수를 사용하지 않고, 시작 날짜와 종료 날짜를 매개변수로 받아 해당 기간 동안의 데이터만을 필터링하는 쿼리를 작성하세요.
(예 : @Query("SELECT COALESCE(SUM(w.overHours), 0) FROM WorkLog w WHERE w.employeeId = :employeeId AND w.workDate BETWEEN :startDate AND :endDate") Long findOvertimeHoursByEmployeeId(@Param("employeeId") Long employeeId, @Param("startDate") LocalDate startDate, @Param("endDate") LocalDate endDate);)


** 중요 ** : Repository Interface의 전체 틀이 아닌, 오로직 JPA Query Method만 'jpaQueryMethod'의 결과에 담아서 반환하세요.


아래는 JPA Query Methods의 기본 구조 입니다:
@Query("적절한 쿼리문, value= 를 쓰지마세요")
\nType exampleJPAQueryMethod(@Param("Type TableColumn") Type exampleField, ...)


아래는 결과 예시로, 부가 설명 없이 결과만을 포함하여, 다음 JSON 형식으로 반환하세요:
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