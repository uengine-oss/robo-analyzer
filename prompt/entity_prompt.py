import json
import logging
import os
from langchain.globals import set_llm_cache
from langchain_community.cache import SQLiteCache
from langchain_openai import ChatOpenAI
from langchain_anthropic import ChatAnthropic
from langchain_core.output_parsers import JsonOutputParser
from langchain.prompts import PromptTemplate
from langchain.schema.runnable import RunnablePassthrough
from util.exception import LLMCallError
import openai

# TODO 외래키 처리가 필요 할 수 도 있음 (추후에 검토)
db_path = os.path.join(os.path.dirname(__file__), 'langchain.db')
set_llm_cache(SQLiteCache(database_path=db_path))
api_key = os.getenv("OPENAI_API_KEY")
if api_key is None:
    raise ValueError("OPENAI_API_KEY 환경 변수가 설정되지 않았습니다.")

# llm = ChatOpenAI(api_key=api_key, model_name="gpt-4o")
llm = ChatAnthropic(model="claude-3-5-sonnet-20241022", max_tokens=8000, temperature=0.1)

prompt = PromptTemplate.from_template(
"""
당신은 클린 아키텍처 원칙을 따르는 스프링부트 기반의 자바 애플리케이션을 개발하는 소프트웨어 엔지니어입니다. 주어진 JSON 형식의 테이블 데이터를 기반으로 자바 Entity 클래스를 생성하는 작업을 맡았습니다.


테이블 데이터(JSON)입니다:
{table_json_data}


[SECTION 1] Entity 클래스 생성 규칙
===============================================
1. 변환 범위
   - 각 테이블(JSON) 객체는 독립적인 Entity 클래스로 변환
   - 하나의 JSON 객체 -> 하나의 Entity 클래스

2. 클래스 명명 규칙
   - JSON 객체의 'name' 필드를 파스칼 케이스로 변환
   - 복수형을 단수형으로 변경
   - entityName도 동일한 규칙 적용
   예시) B_Plcy_Month -> BPlcyMonth
        Employees -> Employee

3. 필드 규칙
   - 접근제한자: private
   - 명명규칙: 카멜 케이스
   - JSON의 'fields' 배열의 각 항목을 클래스 속성으로 변환
   예시) B_PLCY_MONTH -> bPlcyMonth

4. 기본키(Primary Key) 선정 규칙
   - 모든 엔티티는 새로운 Long 타입의 'id' 필드를 기본키로 사용
     * @Id와 @GeneratedValue(strategy = GenerationType.IDENTITY) 어노테이션 적용
   
   - 테이블 메타 데이터에 기본키 정보가 있을 경우:
     * 테이블 메타데이터의 기본키 필드들은 일반 필드로 변환
     * @Column(unique = true) 어노테이션 적용
     * 복합키였던 경우 각 필드에 @Column(unique = true) 적용

5. 데이터 타입 매핑
   - 정수: long (기본 데이터 타입 사용)
   - 실수: double (기본 데이터 타입 사용)
   - 날짜/시간: LocalDate
   - 문자/문자열: String (char 사용 금지)

6. Import 선언
   - 기본 제공되는 import문 유지
   - 추가로 필요한 import문 선언


[SECTION 2] Entity 클래스 기본 템플릿
===============================================
package com.example.demo.entity;

import jakarta.persistence.*;
import lombok.*;
import java.time.*;

@Entity
@Table(name = "TableName")
@Data
public class EntityName {{
    @Id
    @GeneratedValue(strategy = GenerationType.AUTO)
    private long id;
    
    @Column(unique = true)
    private String originalPrimaryKey;

    @Column(nullable = false)
    private String requiredField;
    
    private String optionalField;
    ...
}}


[SECTION 3] JSON 출력 형식
===============================================
부가 설명 없이 결과만을 포함하여, 다음 JSON 형식으로 반환하세요:
{{
    "analysis": [
        {{
            "entityName": "EntityName",
            "code": "Java Code"
        }}
    ]
}}
"""
)


# 역할: Neo4j에서 추출한 테이블 메타데이터를 기반으로 JPA Entity 클래스를 생성하는 함수입니다.
#      LLM을 통해 테이블의 컬럼들을 Java 데이터 타입으로 매핑하고,
#      클린 아키텍처 원칙을 따르는 스프링 부트 엔티티 클래스를 생성합니다.
# 매개변수: 
#   - table_data : 테이블 노드의 메타데이터 정보
# 반환값: 
#   - result : LLM이 생성한 Entity 클래스 정보
def convert_entity_code(table_data):
    
    try:
        table_json_data = json.dumps(table_data, ensure_ascii=False, indent=2)

        chain = (
            RunnablePassthrough()
            | prompt
            | llm
            | JsonOutputParser()
        )
        result = chain.invoke({"table_json_data": table_json_data})
        return result
    except Exception:
        err_msg = "엔티티 생성 과정에서 LLM 호출하는 도중 오류가 발생했습니다."
        logging.error(err_msg, exc_info=False)
        raise LLMCallError(err_msg)