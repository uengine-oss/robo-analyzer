import json
import os
from langchain.globals import set_llm_cache
from langchain_community.cache import SQLiteCache
from langchain_openai import ChatOpenAI
from langchain_core.output_parsers import JsonOutputParser
from langchain.prompts import PromptTemplate
from langchain.schema.runnable import RunnablePassthrough

# 역할 : 프로시저 노드 정보를 기반으로, 서비스 골격과 커맨드 클래스를 생성합니다
# 매개변수: 
#   - procedure_data : 프로시저 노드 정보
# 반환값 : Command 클래스, Service 골격
db_path = os.path.join(os.path.dirname(__file__), 'langchain.db')
set_llm_cache(SQLiteCache(database_path=db_path))

llm = ChatOpenAI(model_name="gpt-4o")

prompt = PromptTemplate.from_template(
"""
당신은 클린 아키텍처 원칙을 따르는 스프링부트 기반의 자바 애플리케이션을 개발하는 소프트웨어 엔지니어입니다. 주어진 JSON 형식의 프로시저 선언부 데이터를 기반으로 자바 클래스를 생성하는 작업을 맡았습니다.


프로시저 선언부 데이터(JSON)입니다:
{procedure_json}


프로시저 선언부 데이터를 Service 클래스로 전환할 때, 다음 지침을 따르세요:
1. 전달된 JSON 객체 중에서 'type' 필드의 값이 'procedure'인 항목들의 'code' 필드에서 사용된 모든 변수들을 포함한 Command Class의 기본 구조를 작성하세요.
2. 모든 변수는 적절한 자바 데이터 타입을 사용하고, private 접근 제한자와 카멜 표기법을 적용하세요. (데이터 타입의 경우, 되도록이면 int 대신 long을 사용하세요.)
5. Command 클래스의 이름은 로직에 알맞게 작성해주세요.


아래는 Command의 기본 구조입니다:
package com.exmaple.{proejct_name}.service;

import lombok.*;

@Data
public class CommandClassName {{
    private DataType variable1;
    private DataType variable2;
    ...
}}


아래는 결과 예시로, 부가 설명 없이 결과만을 포함하여, 다음 JSON 형식으로 반환하세요:
{{
    "commandName": "Command Class Name",
    "command": "Command Java Code",
    "command_class_variable": [
        "Command Class에 선언된 변수들을 여기에 채워넣으세요."
    ]
}}
"""
)


def convert_command_code(procedure_data, spFile_Name):
    procedure_json = json.dumps(procedure_data)

    chain = (
        RunnablePassthrough()
        | prompt
        | llm
        | JsonOutputParser()
    )
    result = chain.invoke({"procedure_json": procedure_json, "proejct_name": spFile_Name})
    return result