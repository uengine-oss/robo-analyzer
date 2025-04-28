import json
import logging
import os
from langchain.globals import set_llm_cache
from langchain_community.cache import SQLiteCache
from langchain_anthropic import ChatAnthropic
from langchain_core.output_parsers import JsonOutputParser
from langchain.prompts import PromptTemplate
from langchain.schema.runnable import RunnablePassthrough
from util.exception import LLMCallError

db_path = os.path.join(os.path.dirname(__file__), 'langchain.db')
set_llm_cache(SQLiteCache(database_path=db_path))


# JPA Entity 클래스 생성 프롬프트
jpa_prompt = PromptTemplate.from_template(
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
   - TPJ_ , DDL_ 같은 접두사를 절대적으로 유지하세요
   예시) B_Plcy_Month -> BPlcyMonth
        Employees -> Employee
        TPJ_PLCY_MONTH -> TpjPlcyMonth
        DDL_PLCY_MONTH -> DdlPlcyMonth

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
   - NUMBER, NUMERIC, INTEGER, DECIMAL, BIGDECIMAL, FLOAT, DOUBLE, REAL 등 모든 숫자 타입(정수 및 실수 포함) -> 반드시 Long만 사용
   - VARCHAR2, CHAR: String
   - DATE & TIME: 
        * 컬럼명에 'TIME'이 포함된 경우 -> LocalDateTime
        * 컬럼명에 'DATE'만 포함되고 'TIME'이 없는 경우 -> LocalDate
   - CLOB: String
   - BLOB: byte[]
   - RAW: byte[]
   - BOOLEAN: Boolean

6. Import 선언
   - 기본 제공되는 import문 유지
   - 추가로 필요한 import문 선언

   
[SECTION 2] Entity 클래스 기본 템플릿
===============================================
package com.example.{project_name}.entity;

import jakarta.persistence.*;
import lombok.*;
import java.time.*;

@Entity
@Table(name = "TableName")
@Data
public class EntityName {{
    @Id
    @GeneratedValue(strategy = GenerationType.AUTO)
    private Long id;
    
    @Column(unique = true)
    private String originalPrimaryKey;

    @Column(nullable = false)
    private LocalDate requiredField;
    
    private LocalDateTime optionalField;
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


# 역할: Neo4j에서 추출한 테이블 메타데이터를 기반으로 Entity 클래스를 생성하는 함수입니다.
#
# 매개변수: 
#   - table_data : 테이블 노드의 메타데이터 정보
#   - api_key : OpenAI API 키
#   - project_name : 프로젝트 이름
#
# 반환값: 
#   - result : LLM이 생성한 Entity 클래스 정보
def convert_entity_code(table_data: dict, api_key: str, project_name: str) -> dict:
    
    try:
        llm = ChatAnthropic(
            model="claude-3-7-sonnet-latest", 
            max_tokens=8192,
            api_key=api_key
        )

        table_json_data = json.dumps(table_data, ensure_ascii=False, indent=2)
        prompt_data = {
                "table_json_data": table_json_data,
                "project_name": project_name
            }

        chain = (
            RunnablePassthrough()
            | jpa_prompt
            | llm
            | JsonOutputParser()
        )
        result = chain.invoke(prompt_data)
        return result
    except Exception as e:
        err_msg = f"엔티티 생성 과정에서 LLM 호출하는 도중 오류가 발생했습니다: {str(e)}"
        logging.error(err_msg)
        raise LLMCallError(err_msg)