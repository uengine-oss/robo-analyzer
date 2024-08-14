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

1159: 1159:         WHILE (I_IDX <= I_MAX) LOOP
1160: 1160: 
1161: 1161:                 
1162: ... code ...
1166: 1166: 
1167: 1167:                 
1168: 1168:                 
1169: 1169:                 
1170: ... code ...
1171: ... code ...
1172: ... code ...
1173: ... code ...
1174: 1174: 
1175: 1175:                 
1176: 1176:                 
1177: ... code ...
1178: ... code ...
1179: ... code ...
1180: ... code ...
1181: ... code ...
1182: ... code ...
1183: ... code ...
1184: ... code ...
1185: ... code ...
1186: ... code ...
1187: 1187:                 
1188: 1188: 
1189: 1189:                 
1190: ... code ...
1199: 1199: 
1200: 1200:                 
1201: ... code ...
1202: 1202:         END LOOP;



summarized_stored_procedure_code를 자바 코드로 전환할 때, 다음 지침을 따르세요:
1. 라인번호: ...code...로 되어있는 경우, 자바 코드에도 동일하게 적용하세요.



아래는 생성될 자바 코드의 예시입니다:
while (condition) {{
    // 722~722 로직
    722: ...code...
    
    // 723~723 로직
    723: ...code...

    ...
    
    // 740~765 로직
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