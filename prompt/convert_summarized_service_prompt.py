import json
import logging
import os
from langchain.globals import set_llm_cache
from langchain_community.cache import SQLiteCache
from util.llm_client import get_llm
from langchain_core.output_parsers import JsonOutputParser
from langchain.prompts import PromptTemplate
from langchain.schema.runnable import RunnablePassthrough
from util.exception import LLMCallError


db_path = os.path.join(os.path.dirname(__file__), 'langchain.db')
set_llm_cache(SQLiteCache(database_path=db_path))


prompt = PromptTemplate.from_template(
"""
당신은 클린 아키텍처 원칙을 따르는 스프링부트 기반의 자바 애플리케이션을 개발하는 소프트웨어 엔지니어입니다.
입력으로 제공되는 **요약본 Stored Procedure 코드**를 기반으로, 부모(현재 레벨) 코드만 Java로 변환하고 자식(하위 로직)은 단 하나의 `...code...`로 축약한 **임시 Java 코드 골격**을 생성하세요.

사용자 언어 설정: {locale}


[입력 데이터]
===============================================
- summarized_stored_procedure_code: 요약된 Stored Procedure 코드 블록
  * 특징: 부모 코드는 실제 SP 형태로 남아 있고, 자식은 `라인번호: ... code ...` 형태로 여러 개 나열될 수 있음
- service_skeleton: 구현할 메서드의 시그니처와 기본 구조
- variable: 현재 변수들의 할당값 정보 (이전 작업 결과)
- command_class_variable: Command 클래스에서 전달받는 파라미터 정보
- query_method_list: 사용 가능한 JPA 쿼리 메서드 목록
- sequence_methods: 사용 가능한 시퀀스 메서드 목록

summarized_stored_procedure_code:
{summarized_code}

service_skeleton:
{service_skeleton}

used_variable:
{variable}

command_class_variable:
{command_variables}

jpa_method_list:
{query_method_list}

sequence_method_list:
{sequence_methods}


[SECTION 1] 주요 작업
===============================================
1) **부모(현재 레벨) 코드만 Java로 변환**합니다. (제어구조 헤더, 현재 레벨의 프로시저 호출 등)
2) 자식(하위 블록/세부 로직)은 입력에 `라인번호: ... code ...`가 여러 개 있더라도, **출력에서는 라인번호를 제거하고 `...code...` 단 하나만** 남깁니다.
   - 즉, `라인번호: ... code ...` → `...code...` 로 통일
   - 같은 블록 내부에 `...code...`가 2회 이상 등장하면 안 됩니다.
3) 부모가 무엇인지는 고정되어 있지 않습니다(IF/CASE/FOR/WHILE/LOOP 등). 입력의 블록 토큰으로 현재 레벨을 동적으로 식별하세요.
4) 이 단계는 **골격 생성** 단계입니다. 세부 SQL/JPA/시퀀스/예외 구현은 작성하지 않습니다(자식으로 간주되어 축약 대상).


[SECTION 2] 변환 규칙
===============================================
1. 제어구조
   - 현재 레벨의 제어구조(조건/반복 등) 헤더와 중괄호만 Java로 남기고, 내부는 `...code...` 1회로 대체합니다.
   - 어떤 제어구조든 동일 원칙을 적용합니다(예시 비한정).

2. 프로시저/함수 호출 (현재 레벨에 한정)
   - 외부: SCHEMA.PROC(params) → schemaService.proc(params)
   - 내부: PROC(params) → 동일 클래스의 private 메서드 호출 (예: proc(params))
   - 메서드명 규칙: 접두어(i,p,o,v) 유지 + 스네이크→카멜 (p_get_data → pGetData)
   - 파라미터는 알파벳 순 정렬

3. SQL/JPA/시퀀스
   - 상세 구현은 하위 로직으로 간주하여 `...code...`로 축약합니다.

4. `...code...` 수량/형태
   - 각 블록 내부에는 **`...code...`가 정확히 1회**만 존재해야 합니다.
   - **라인번호를 포함하지 않습니다.** (예: `131: ...code...` → `...code...`)
   - 여러 개의 `라인번호: ... code ...`가 들어와도 하나로 합쳐 `...code...`로만 출력합니다.

5. 포맷/스타일
   - 결과는 들여쓰기가 적용된 **단일 Java 코드 문자열**이어야 합니다.
   - 불필요한 임포트/주석/설명 금지. 백틱(```) 금지.


[SECTION 3] 예시 (입력 형태 대비 변환 의도 설명용, 한정 아님)
===============================================
입력 요약 예(예시):
3: FOR LOO_DATA IN (
4: ... code ...
5: ... code ...
6: )
7: LOOP
8: ... code ...
9: ... code ...
10: END LOOP;

변환 출력 의도:
for (var genRuntime : someIterable) {{
    ...code...
}}

또 다른 입력 예(예시):
IF 조건 THEN
   42: ... code ...
   43: ... code ...
END IF;

변환 출력 의도:
if (condition) {{
    ...code...
}}


[SECTION 4] JSON 출력 형식
===============================================
부가 설명 없이, 아래 **정확한** JSON 형식으로만 반환하세요:
{{
   "code": "Java Code"
}}
"""
)

# 역할: 토큰 수가 제한을 초과하는 대형 PL/SQL 코드 블록을 처리하는 함수입니다.
#      LLM을 통해 자식 노드들이 "...code..."로 요약된 코드를 분석하고,
#      코드의 전체적인 구조와 흐름을 유지하면서
#      Java 코드의 골격(skeleton)을 생성합니다.
# 매개변수: 
#   - summarized_code : 자식 노드들이 "...code..."로 요약된 PL/SQL 코드
#      (큰 코드 블록의 구조를 파악할 수 있는 요약본)
#   - api_key : OpenAI API 키
#
# 반환값: 
#   - result : LLM이 생성한 요약된 형태의 Java 코드
#      (실제 구현은 나중에 채워질 수 있도록 자리 표시자를 포함)
def convert_summarized_code(summarized_code, service_skeleton, variable, command_class_variable, query_method_list, sequence_methods, api_key, locale):
    
    try:
        summarized_code = json.dumps(summarized_code, ensure_ascii=False, indent=2)
        service_skeleton = json.dumps(service_skeleton, ensure_ascii=False, indent=2)
        variable = json.dumps(variable, ensure_ascii=False, indent=2)
        command_class_variable = json.dumps(command_class_variable, ensure_ascii=False, indent=2)
        query_method_list = json.dumps(query_method_list, ensure_ascii=False, indent=2)
        sequence_methods = json.dumps(sequence_methods, ensure_ascii=False, indent=2)
        llm = get_llm(max_tokens=8192, api_key=api_key)

        chain = (
            RunnablePassthrough()
            | prompt
            | llm
            | JsonOutputParser()
        )
        result = chain.invoke({"summarized_code": summarized_code, "service_skeleton": service_skeleton, "variable": variable, "command_class_variable": command_class_variable, "query_method_list": query_method_list, "sequence_methods": sequence_methods, "locale": locale})
        return result
    
    except Exception as e:
        err_msg = f"서비스 클래스 생성 과정에서 LLM 호출하는 도중 오류가 발생했습니다: {str(e)}"
        logging.error(err_msg)
        raise LLMCallError(err_msg)