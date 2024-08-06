import json
import os
from langchain.globals import set_llm_cache
from langchain_community.cache import SQLiteCache
from langchain_openai import ChatOpenAI
from langchain_core.output_parsers import JsonOutputParser
from langchain.prompts import PromptTemplate
from langchain.schema.runnable import RunnablePassthrough

# 역할 : 테이블과 직접적으로 연결된 노드를 기반으로, 스프링부트 기반의 자바 리포지토리 인터페이스를 생성합니다
# 매개변수: 
#   - node_data : 테이블과 직접적으로 연결된 노드
#   - variable_nodes_context : 해당 노드에서 사용된 변수목록
# 반환값 : 리포지토리 인터페이스
# TODO 그냥 JPA 쿼리 메서드만 반환하게 한다? 
db_path = os.path.join(os.path.dirname(__file__), 'langchain.db')
set_llm_cache(SQLiteCache(database_path=db_path))

llm = ChatOpenAI(model_name="gpt-4o")

prompt = PromptTemplate.from_template(
"""
당신은 클린 아키텍처 원칙을 따르는 스프링부트 기반의 자바 애플리케이션을 개발하는 소프트웨어 엔지니어입니다. 
주어진 데이터를 기반으로 JPA 기반의 Repository Interface를 생성하는 작업을 맡았습니다.


Stored Procedure Code:
{node_json}


Used Variable:
{variable_node}


반드시 지켜야할 요구사항: 
- 'MERGE INTO', 'INSERT INTO', 'UPDATE' 구문이 있는 경우, 해당 데이터를 조회하는 JPA 쿼리 메서드를 생성하세요. 실제 데이터의 삽입, 업데이트, 또는 복합 작업은 Java 로직에서 처리해야 합니다. 예를 들어, 'MERGE INTO A USING B ON ... WHEN MATCHED THEN UPDATE ...' 구문이 있을 경우, 'B' 테이블에 대한 조회를 수행하는 JPA 쿼리 메서드를 작성하고, 결과를 사용하여 자바 로직으로 'A' 테이블에 데이터를 업데이트하거나 삽입하세요. ( 반드시 'pascalName', 'camelName', 'code', 'methodList'는 'A' 테이블이 아닌 'B' 테이블을 기준으로 생성하세요.)
- 'DELETE FROM' 구문이 있는 경우, 해당 테이블의 모든 데이터를 삭제하는 JPA 쿼리 메서드를 생성하세요. 
- 결과는 단일 사전 형태로 반환해야 하며, 리스트 형태로 반환하면 안 됩니다.
- Json 문자열을 위해서 결과에서 'code' 부분은 + 연산자를 사용하지말고, 반드시 하나의 문자열 형태로 반환하세요.


아래의 지시사항을 참고하여, 제공된 데이터를 분석하고, Repository Interface를 생성하십시오:
1. Stored Procedure Code에서 각 JSON 객체들은 JPA 쿼리 메소드로 전환되어야 합니다.
2. Used Variable는 현재 구문에서 사용된 변수 목록으로, 해당 데이터를 참고해서 JPA 쿼리 메서드의 매개변수로 식별하세요.
3. 모든 Entity의 이름은 복수형이 아닌 단수형으로 표현됩니다. (예: Employees -> Employee)
4. 되도록이면 네이밍 규칙을 이용해서 JPA 쿼리 메서드를 생성하세요.
5. SELECT 구문의 경우, 쿼리 메서드의 타입은 ENTITY 또는 List<Entity> 구성되어야 하며, 특정 필드(컬럼)이 아닌 전체 필드를 포함하는 객체 자체를 반환하는 쿼리 메서드로 전환하세요.(예 : Person findById(Long id))
6. 쿼리가 매우 복잡한 경우 @Query 어노테이션을 사용하세요. 


아래는 Repository Interface의 기본 구조입니다:
public interface EntityNameRepository extends JpaRepository<EntityName, Long> {{
    @Query("적절한 쿼리문, value= 를 쓰지마세요")
    Type exampleJPAQueryMethod(@Param("Type TableColumn") Type exampleField, ...)
    ...
}}


아래는 결과 예시로, 부가 설명 없이 결과만을 포함해야 하고, 다음 JSON 형식으로 반환하세요:
{{
    "pascalName": "(예: B_CONTRACT_CAR_STND_CONFS -> BContractCarStndcConf)",  
    "camelName": "(예: B_CONTRACT_CAR_STND_CONFS -> bContractCarStndcConf)",
    "code": "Repository Interface and JPA Query Method Code",
    "primaryKeyType": "Primary Key Type",
    "methodList" : {{
        "entityName_startLine~endLine": "Type JPAQueryMethod(@Param("Type Column") Type Field, ...)",
        "entityName_startLine~endLine": "Type JPAQueryMethod(@Param("Type Column") Type Field, ...)",
    }}
}}
"""
)

def convert_repository_code(node_data, variable_nodes_context):
    node_json = json.dumps(node_data)

    chain = (
        RunnablePassthrough()
        | prompt
        | llm
        | JsonOutputParser()
    )
    result = chain.invoke({"node_json": node_json, "variable_node": variable_nodes_context})
    return result