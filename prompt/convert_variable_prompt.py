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


db_path = os.path.join(os.path.dirname(__file__), 'langchain.db')
set_llm_cache(SQLiteCache(database_path=db_path))

prompt = PromptTemplate.from_template(
"""
당신은 PL/SQL 변수를 Java 변수 타입으로 변환하는 전문가입니다.
주어진 JSON 데이터를 기반으로 Java 변수 타입을 결정합니다.

[입력 데이터 구조 설명]
===============================================
입력되는 JSON 데이터는 다음 구조를 가집니다:
{variables}

[변수 타입 변환 규칙]
===============================================
1. 기본 데이터 타입 매핑:
   - NUMBER, NUMERIC -> Long (소수점이 필요한 경우 Double)
   - INTEGER -> Integer
   - VARCHAR, VARCHAR2, CHAR -> String
   - DATE -> LocalDate
   - TIMESTAMP -> LocalDateTime
   - BOOLEAN -> Boolean
   - CLOB -> String
   - BLOB -> byte[]

2. 테이블 타입 매핑:
   - 테이블 이름이 타입으로 전달된 경우 -> 해당 테이블 명을 그대로 타입으로 사용
   예시) 
   - EMPLOYEE -> Employee
   - TB_USER -> User
   - CUSTOMER_ORDER -> CustomerOrder

3. 컬렉션 타입 매핑:
   - TABLE OF -> List<타입>
   - VARRAY -> List<타입>
   - NESTED TABLE -> List<타입>
   
4. 변수 값 식별
   - 'value' 에 있는 값을 변수의 초기 값으로 선정
   - 'value' 가 null 또는 0인 경우 타입별 기본값 적용:
     * Long -> 0
     * Double -> 0.0
     * String -> ""
     * LocalDate -> LocalDate.now()
     * LocalDateTime -> LocalDateTime.now()
     * Boolean -> false
     * byte[] -> new byte[0]
     * List<?> -> new ArrayList<>()
     * 엔티티 클래스 -> new EntityClass()
   - 테이블 명, 엔티티 클래스가 타입으로 선정된 경우 new EntityClass() 형태로 표현

5. 특수 규칙:
   - 모든 변수는 카멜케이스로 변환되어야 합니다.
   - 테이블명을 타입으로 사용할 때는 파스칼케이스로 변환하세요.

   
[출력 형식]
===============================================
부가 설명 없이 결과만을 포함하여, 다음 JSON 형식으로 반환하세요:
{{
    "variables": [
        {{
            "javaName": "employeeId",
            "javaType": "Long",
            "value": "할당값 또는 new Object() 또는 '', 0",
        }}
    ]
}}
"""
)


# 역할: PL/SQL 변수를 Java 변수 타입으로 변환하는 함수입니다.
#      LLM을 통해 변수의 타입을 분석하고,
#      적절한 Java 타입으로 변환합니다.
# 매개변수: 
#   - variable_metadata : PL/SQL 변수의 메타데이터 정보
# 반환값: 
#   - result : LLM이 생성한 변수 타입 변환 정보
def convert_variables(variables, api_key):
    
    try:
        variables = json.dumps(variables, ensure_ascii=False, indent=2)

        llm = ChatAnthropic(model="claude-3-7-sonnet-latest", max_tokens=8000, temperature=0.0, api_key=api_key)

        chain = (
            RunnablePassthrough()
            | prompt
            | llm
            | JsonOutputParser()
        )
        result = chain.invoke({"variables": variables})
        return result
    
    except Exception as e:
        err_msg = f"자바 클래스 필드로 전환하는 과정에서 LLM 호출하는 도중 오류가 발생했습니다: {str(e)}"
        logging.error(err_msg)
        raise LLMCallError(err_msg)
