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
당신은 클린 아키텍처 원칙을 따르는 스프링부트 기반의 자바 애플리케이션을 개발하는 소프트웨어 엔지니어입니다. 주어진 스토어드 프로시저 코드를 기반으로 임시 자바 코드를 생성하는 작업을 맡았습니다.


summarized_stored_procedure_code:
{summarized_code}




summarized_stored_procedure_code를 자바 코드로 전환할 때, 다음 지침을 따르세요:
1. 반드시 라인번호: ...code...로 되어있는 부분만 식별하여, 임시 자바 코드를 생성하세요. 즉, 단순 라인번호: 라인번호:  형태는 무시하고 넘어가고 ...code... 만 찾으세요.
( 예, 456: ...code ... 로 된 부분만 식별하여 작업을 진행하세요.) 



아래는 생성될 자바 코드의 예시입니다:
while (condition) {{
722: ...code...

723: ...code...

...

740: ...code...
}}


아래는 결과 예시로, 부가 설명 없이 결과만을 포함하여, 다음 JSON 형식으로 반환하세요:
{{
    "code": "Java Code"
}}
"""
)


def convert_parent_skeleton(summarized_code):

    chain = (
        RunnablePassthrough()
        | prompt
        | llm
        | JsonOutputParser()
    )
    result = chain.invoke({"summarized_code": summarized_code})
    return result