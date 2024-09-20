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
당신은 클린 아키텍처 원칙을 따르는 스프링부트 기반의 자바 애플리케이션을 개발하는 소프트웨어 엔지니어입니다. 주어진 JSON 형식의 데이터를 기반으로 자바 클래스를 생성하는 작업을 맡았습니다.


데이터(JSON)입니다:
{declare_json}


프로시저 선언부 데이터를 Service 클래스로 전환할 때, 다음 지침을 따르세요:
1. 전달된 JSON 객체 중에서 'type' 필드의 값이 'declare'인 항목들의 'code' 필드에서 사용된 모든 변수들을 포함한 Service Class의 기본 구조를 작성하세요.
2. 모든 변수는 적절한 자바 데이터 타입을 사용하고, private 접근 제한자와 카멜 표기법을 적용하세요. (데이터 타입의 경우, 되도록이면 int 대신 long을 사용하세요.)
3. Service의 이름은 로직에 알맞게 작성해주세요.
4. 'CodePlaceHolder1', 'CodePlaceHolder2' 이 부분 하드코딩으로 그대로 반환하고, 위치를 변경하지마세요. 추후에 사용될 예정입니다.
5. 날짜나 시간을 다루는 필드의 경우 LocalDate를 사용하도록 하세요.
6. 필요에 따라 추가적인 import문을 선언하세요.


아래는 Service의 기본 구조입니다:
package com.example.{proejct_name}.service;

import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RestController;
import com.example.{proejct_name}.command.commandClassName(실제 Command Class 이름으로 대체);
import org.springframework.beans.factory.annotation.Autowired;
import java.util.List;
import org.springframework.transaction.annotation.Transactional;

@RestController
@Transactional
public class ExampleController {{

CodePlaceHolder1

    Type variable1 = 0;
    Type variable2 = 0;

    @PostMapping(path="/Endpoint")
    public ResponseEntity<String> methodName(@RequestBody {command_class_name} {command_class_name}Dto) {{

CodePlaceHolder2

    return ResponseEntity.ok("Operation completed successfully");
    }}
}}



아래는 결과 예시로, 부가 설명 없이 결과만을 포함하여, 다음 JSON 형식으로 반환하세요:
{{
    "serviceName": "Service Class Name",
    "service": "Service Java Code",
}}
"""
)

# 역할 : 선언 노드 정보를 기반으로, 서비스 골격 클래스를 생성합니다
# 매개변수: 
#   - declare_data : 프로시저 노드 정보
#   - spFile_Name : 스토어드 프로시저 파일 이름
#   - command_class_name : command 클래스 이름
# 반환값 : 
#   - result : 서비스 골격 클래스
def convert_service_skeleton_code(declare_data, spFile_Name, command_class_name):
    
    try:
        declare_json = json.dumps(declare_data)

        chain = (
            RunnablePassthrough()
            | prompt
            | llm
            | JsonOutputParser()
        )
        result = chain.invoke({"declare_json": declare_json, "proejct_name": spFile_Name, "command_class_name": command_class_name})
        return result
    except Exception:
        err_msg = "서비스 골격 클래스 생성 과정에서 LLM 호출하는 도중 오류가 발생했습니다."
        logging.exception(err_msg)
        raise LLMCallError(err_msg)