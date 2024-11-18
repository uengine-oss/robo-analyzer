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
당신은 클린 아키텍처 원칙을 따르는 스프링부트 기반의 자바 애플리케이션을 개발하는 소프트웨어 엔지니어입니다. 주어진 JSON 형식의 변수 데이터를 기반으로 서비스 클래스를 생성하는 작업을 맡았습니다.


변수 데이터(JSON)입니다:
{variable_json}

[SECTION 1] Service 클래스 생성 규칙
===============================================
1. 대상 선정
   - JSON 객체의 데이터를 기반으로 Service 클래스의 필드 변수로 선정
   - 클래스명은 {command_class_name}을 참고하여, '이름Service' 형식으로 작성
     예시) UpdateEmployeeCommand -> UpdateEmployeeService
     
2. 필드 규칙
   - 접근제한자: private
   - 명명규칙: 카멜 케이스
   - 숫자타입: Long 사용 권장
   - 날짜타입: LocalDate 사용

3. 코드 구조 유지
   - CodePlaceHolder1, CodePlaceHolder2 위치 유지
   - 하드코딩된 값 그대로 사용

4. Import 선언
   - 기본 제공되는 import문 유지
   - 추가로 필요한 import문 선언


[SECTION 2] Service 클래스 기본 템플릿
===============================================
package com.example.demo.service;

import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RestController;
import com.example.demo.command.{command_class_name};
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


[SECTION 3] JSON 출력 형식
===============================================
부가 설명 없이 결과만을 포함하여, 다음 JSON 형식으로 반환하세요:
{{
    "serviceName": "Service Class Name",
    "service": "Service Java Code",
}}
"""
)

# 역할 : 선언 노드 정보를 기반으로, 서비스 골격 클래스를 생성합니다
# 매개변수: 
#   - variable_data : 지역 변수 노드 정보
#   - command_class_name : command 클래스 이름
# 반환값 : 
#   - result : 서비스 골격 클래스
def convert_service_skeleton_code(variable_data, command_class_name):
    
    try:
        declare_json = json.dumps(variable_data)

        chain = (
            RunnablePassthrough()
            | prompt
            | llm
            | JsonOutputParser()
        )
        result = chain.invoke({"declare_json": declare_json, "command_class_name": command_class_name})
        return result
    except Exception:
        err_msg = "서비스 골격 클래스 생성 과정에서 LLM 호출하는 도중 오류가 발생했습니다."
        logging.exception(err_msg)
        raise LLMCallError(err_msg)