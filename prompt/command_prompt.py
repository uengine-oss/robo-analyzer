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
당신은 클린 아키텍처 원칙을 따르는 스프링부트 기반의 자바 애플리케이션을 개발하는 소프트웨어 엔지니어입니다. 주어진 JSON 형식의 입력 매개변수를 기반으로 Command 클래스를 생성하는 작업을 맡았습니다.


입력 매개변수 데이터(JSON)입니다:
{input_variable_json}


[SECTION 1] Command 클래스 생성 규칙
===============================================
1. 기본 구조
   - JSON 객체의 데이터를 기반으로 Command 클래스 구조 생성
   - 클래스명은 {object_name}을 참고하여, '이름Command' 형식으로 작성
     예시) updateEmployee -> UpdateEmployeeCommand

2. 필드 규칙
   - 접근제한자: private
   - 명명규칙: 카멜 케이스
   - 숫자타입: Long 사용 권장 (int 지양)
   - 날짜타입: LocalDate 사용
   - Lombok @Getter @Setter 활용

3. 필수 import
   - java.time.LocalDate
   - lombok.Getter
   - lombok.Setter
   - 기타 필요한 import



[SECTION 2] Command 클래스 예시
===============================================
예시:
import java.time.LocalDate;
import lombok.Getter;
import lombok.Setter;

@Getter
@Setter
public class ExampleCommand {{
    private Long id;
    private String name;
    private LocalDate date;
}}


[SECTION 3] 출력 형식
===============================================
부가 설명 없이 결과만을 포함하여, 다음 JSON 형식으로 반환하세요:
{{
    "commandName": "Command Class Name",
    "command": "Command Java Code",
    "command_class_variable": [
        "Command Class에 선언된 모든 변수들을 '타입:이름' 형태로 채워넣으세요."
    ]
}}
"""
)

# 역할 : 입력 매개변수 정보를 기반으로, 커맨드 클래스를 생성합니다
# 매개변수: 
#   - input_data : 프로시저 노드 정보
#   - object_name : 패키지 및 프로시저 이름
# 반환값 : 
#   - result : Command 클래스
def convert_command_code(input_data, object_name):
    
    try:
        input_data_json = json.dumps(input_data)

        chain = (
            RunnablePassthrough()
            | prompt
            | llm
            | JsonOutputParser()
        )
        result = chain.invoke({"input_variable_json": input_data_json, "object_name": object_name})
        return result
    except Exception:
        err_msg = "Command 생성 과정에서 LLM 호출하는 도중 오류가 발생했습니다."
        logging.exception(err_msg)
        raise LLMCallError(err_msg)