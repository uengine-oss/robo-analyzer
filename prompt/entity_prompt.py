import json
import logging
import os
from langchain.globals import set_llm_cache
from langchain_community.cache import SQLiteCache
from langchain_openai import ChatOpenAI
from langchain_core.output_parsers import JsonOutputParser
from langchain.prompts import PromptTemplate
from langchain.schema.runnable import RunnablePassthrough
from util.exception import LLMCallError


db_path = os.path.join(os.path.dirname(__file__), 'langchain.db')
set_llm_cache(SQLiteCache(database_path=db_path))
llm = ChatOpenAI(model_name="gpt-4o")
prompt = PromptTemplate.from_template(
"""
당신은 클린 아키텍처 원칙을 따르는 스프링부트 기반의 자바 애플리케이션을 개발하는 소프트웨어 엔지니어입니다. 주어진 JSON 형식의 테이블 데이터를 기반으로 자바 Entity 클래스를 생성하는 작업을 맡았습니다.


테이블 데이터(JSON)입니다:
{table_json_data}


테이블 데이터(JSON)을 Entity 클래스로 전환할 때, 아래를 참고하여 작업하세요:
1. 각 테이블(JSON) 객체는 하나의 Entity 클래스로 변환되어야 합니다.
2. 각 테이블(JSON) 객체의 'name'은 파스칼 표기법을 적용한 클래스 이름으로 사용됩니다. (예: B_Plcy_Month -> BPlcyMonth)
3. 클래스의 이름과 'entityName'은 복수형이 아닌 단수형으로 표현하세요. (예: Employees -> Employee)
4. 'fields' 배열의 각 항목은 카멜 표기법을 적용한 클래스의 속성으로 사용됩니다. (예: B_Plcy_Month -> bPlcyMonth)
5. 각 속성은 private 접근 제한자를 가져야하며, 속성명을 참고하여 적절한 자바 데이터 타입으로 설정하도록 하세요. 
6. 날짜나 시간을 다루는 필드의 경우 LocalDate를 사용하도록 하세요.
7. 필요에 따라 추가적인 import문을 선언하세요.


아래는 필드 타입의 규칙이며, 기본 데이터 타입(primitive types)을 사용하도록 하세요.
정수 -> long
실수 -> double
날짜 및 시간 -> LocalDate
문자 및 문자열 -> String (char를 쓰지마세요.)


아래는 자바 Entity 클래스의 기본 구조입니다:
package com.example.{project_name}.entity;

import jakarta.persistence.*;
import lombok.Data;
import java.time.LocalDate

@Entity
@Table(name = "TableName")
@Data
public class EntityName {{
    @Id
    @GeneratedValue(strategy = GenerationType.AUTO)
    private long id;
    private DataType fieldName1;
    private DataType fieldName2;
    ...
}}


아래는 결과 예시로, 부가 설명 없이 결과만을 포함하여, 다음 JSON 형식으로 반환하세요:
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


# 역할 : 테이블 정보를 기반으로 스프링 부트 기반의 엔티티 클래스를 생성합니다
# 매개변수: 
#   - table_data : 테이블 노드 정보
#   - spFile_name : 소문자로 구성된 프로젝트 이름
# 반환값 : 
#   - result : 엔티티 클래스
def convert_entity_code(table_data, lower_file_name):
    
    try:
        table_json_data = json.dumps(table_data)

        chain = (
            RunnablePassthrough()
            | prompt
            | llm
            | JsonOutputParser()
        )
        result = chain.invoke({"table_json_data": table_json_data, "project_name": lower_file_name})
        return result
    except Exception:
        err_msg = "엔티티 생성 과정에서 LLM 호출하는 도중 오류가 발생했습니다."
        logging.exception(err_msg)
        raise LLMCallError(err_msg)