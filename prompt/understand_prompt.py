import json
import logging
import os
import re
from langchain_core.globals import set_llm_cache
from langchain_community.cache import SQLiteCache
from langchain_core.prompts import PromptTemplate
from langchain_core.runnables import RunnablePassthrough, RunnableConfig
from util.llm_client import get_llm
from util.exception  import LLMCallError
from util.llm_audit import invoke_with_audit
db_path = os.path.join(os.path.dirname(__file__), 'langchain.db')
set_llm_cache(SQLiteCache(database_path=db_path))

prompt = PromptTemplate.from_template(
"""
[ROLE]
당신은 PostgreSQL 및 PL/pgSQL 코드 분석 전문가입니다. 
주어진 저장 프로시저/함수 코드를 체계적이고 정확하게 분석하여 구조화된 정보를 추출해야 합니다.


[LANGUAGE_SETTING]
응답 언어: {locale}
모든 분석 결과와 설명은 지정된 언어로 생성해야 합니다.


[INPUT_DATA]
분석 대상 저장 프로시저 코드:
{code}


분석할 코드 범위 목록:
{ranges}


[CONSTRAINTS]
필수 준수 사항:
- 분석 범위 총 개수: {count}개
- 출력 'analysis' 배열 요소 개수: 정확히 {count}개
- 큰 범위가 작은 범위를 포함하더라도 의도된 것으로 생략 없이, 범위 개수와 결과 개수는 동일해야 함.
- 각 범위는 독립적으로 분석되어야 하며, 누락 또는 초과된 분석 결과는 허용되지 않음
- 강제 검증 규칙:
   - analysis 각 요소는 startLine/endLine을 반드시 포함할 것 (정수)
   - 값이 없으면 해당 필드는 다음 기본값을 사용: calls=[], variables=[]
   - null 사용 절대 금지. 빈값은 빈 배열([]) 또는 빈 객체({{}})로만 표현
   - 코드펜스(```), 주석(//, /* */), 트레일링 콤마 허용 금지. 순수 JSON만 출력


[ANALYSIS_REQUIREMENTS]
각 지정된 코드 범위에 대해 다음 섹션의 정보를 정확히 추출하세요:


[SECTION_1_CODE_SUMMARY]
코드 동작 분석 및 요약:

분석 범위:
- 각 코드 범위의 목적과 역할을 파악

세부 분석 요소:
- 변수 할당 및 초기화: 각 변수의 목적과 설정 의미
- 조건 분기 로직: IF, CASE, WHEN 문의 판단 기준과 분기별 목적
- 반복 처리 로직: FOR, WHILE, LOOP 문의 조건과 반복 목적
- 데이터베이스 작업: DML 관련(SELECT, INSERT, UPDATE, DELETE, MERGE, EXECUTE_IMMEDIATE, FETCH) 작업의 대상과 목적
- 예외 처리: EXCEPTION 블록의 처리 방식과 목적
- 트랜잭션 제어: COMMIT, ROLLBACK 등의 사용 목적

예시 1(변수/조건): "v_order_date를 CURRENT_DATE로 설정하고 v_total_count가 100을 넘으면 대량 주문으로 분기합니다."
예시 2(INSERT/JOIN): "ORDER_MASTER와 ORDER_DETAIL 테이블을 조인해 조회하고, 결과를 ORDER_HISTORY 테이블에 기록합니다."
예시 3(SELECT): "ORDER_MASTER 테이블에서 ORDER_ID, ORDER_DATE, TOTAL_AMOUNT... 등 을 조회합니다."

[SECTION_2_VARIABLE_IDENTIFICATION]
변수 식별 및 분류:

식별 규칙:
- 각 범위 내에서 실제로 사용되거나 참조된 변수만 포함
- 범위 중첩 시 해당 범위에서 직접 사용된 변수만 독립적으로 식별

식별 대상 변수 유형:
- 로컬 변수: v_, l_, temp_ 등의 접두사를 가진 변수
- 매개변수: p_, param_, in_, out_, inout_ 등의 접두사를 가진 변수
- 시스템 변수: i_, o_ 등의 입출력 변수
- 레코드 타입: %ROWTYPE으로 선언된 변수
- 컬럼 타입: %TYPE으로 선언된 변수
- 커서 변수: CURSOR로 선언된 변수
- 예외 변수: EXCEPTION으로 선언된 변수


[SECTION_3_PROCEDURE_CALLS]
프로시저/함수 호출 식별:

호출 형식 분류:
- 외부 패키지 호출: PACKAGE_NAME 및 SCHEMA_NAME.PROCEDURE_NAME 형식
- 현재 패키지 내부 호출: PROCEDURE_NAME만 명시된 형식
- 사용자 정의 함수: 사용자가 생성한 함수

제외 대상:
- 시퀀스 객체의 NEXTVAL, CURRVAL 호출
- 내장 함수 호출: 시스템 제공 함수 (SUBSTR, TO_DATE 등)

식별 방법:
- 프로시저/함수명(인자1, 인자2, ...) 형태로 호출되는 경우
- call 라는 키워드가 있는 경우
- EXECUTE_IMMEDIATE 문 내의 동적 호출도 포함

식별 패턴 예시:
CALL [schema_name.][package_name.]proc_name(args)
[schema_name.][package_name.]proc_name(args)
SELECT [schema_name.][package_name.]func_name(args)
EXECUTE IMMEDIATE 'CALL [schema_name.][package_name.]proc_name(args)'
대괄호 [ ] : 생략 가능(옵션)
파이프 | : 둘 중 하나 가능
점 . : 이름공간 구분자(schema 또는 package 구분)

중요 제약:
- calls 배열에는 "이름만" 반환 (괄호/인자/공백 제거)
- 예: "[schema_name | package_name].PROC"


[SECTION_7_OUTPUT_FORMAT]
출력 형식 및 구조:

JSON 스키마:
```json
{{
  "analysis": [
    {{
      "startLine": 범위_시작_라인_번호,
      "endLine": 범위_종료_라인_번호,
      "summary": "코드_동작_요약_설명",
      "calls": ["호출된 프로시저/함수명칭1", "호출된 프로시저/함수명칭2"],
      "variables": ["변수명1", "변수명2"],
    }}
  ]
}}
```

출력 제약사항:
- JSON 형식 외의 부가 설명이나 주석 금지
- "calls", "variables" 배열 요소는 문자열 타입으로 출력
- analysis[i]에는 startLine/endLine이 필수
- 빈 배열도 허용되며 null 값은 사용 금지
- summary는 한국어로 작성하며, 위의 길이 규칙을 준수해 핵심만 압축 표현
- 코드펜스(```json ... ``` 등) 포함 금지, 트레일링 콤마 금지
""")


def _sanitize_llm_json_output(text: str) -> str:
    """LLM 출력에서 주석/코드펜스/트레일링 콤마를 제거하여 표준 JSON으로 정화합니다."""
    try:
        cleaned = text.strip()
        # 코드펜스 제거
        cleaned = re.sub(r"^```(?:json)?\s*", "", cleaned)
        cleaned = re.sub(r"\s*```$", "", cleaned)
        # 블록 주석 제거
        cleaned = re.sub(r"/\*[\s\S]*?\*/", "", cleaned)
        # 라인 주석 제거
        cleaned = re.sub(r"(^|\s)//.*?$", "", cleaned, flags=re.MULTILINE)
        # 트레일링 콤마 제거
        cleaned = re.sub(r",\s*(\}|\])", r"\1", cleaned)
        return cleaned.strip()
    except Exception:
        return text


def _normalize_analysis_structure(obj: dict) -> dict:
    """analysis 구조의 누락 필드를 기본값으로 보정합니다."""
    if not isinstance(obj, dict):
        return {"analysis": []}
    analysis = obj.get("analysis")
    if not isinstance(analysis, list):
        analysis = []
    normalized = []
    for item in analysis:
        if not isinstance(item, dict):
            continue
        item.setdefault("localTables", [])
        item.setdefault("calls", [])
        item.setdefault("variables", [])
        item.setdefault("fkRelations", [])
        item.setdefault("dbLinks", [])
        normalized.append(item)
    obj["analysis"] = normalized
    return obj


def understand_code(sp_code, context_ranges, context_range_count, api_key, locale):
    try:
        ranges_json = json.dumps(context_ranges)
        llm = get_llm(temperature=0, api_key=api_key)

        chain = (
            RunnablePassthrough()
            | prompt
            | llm
        )

        payload = {
            "code": sp_code,
            "ranges": ranges_json,
            "count": context_range_count,
            "locale": locale,
        }

        starts = [item.get("startLine") for item in context_ranges or [] if isinstance(item, dict)]
        ends = [item.get("endLine") for item in context_ranges or [] if isinstance(item, dict)]
        min_start = min((value for value in starts if isinstance(value, int)), default=None)
        max_end = max((value for value in ends if isinstance(value, int)), default=None)

        result = invoke_with_audit(
            chain,
            payload,
            prompt_name="prompt/understand_prompt.py",
            input_payload={
                "code": sp_code,
                "ranges": context_ranges,
                "count": context_range_count,
                "locale": locale,
            },
            metadata={
                "ranges": context_ranges,
                "startLine": min_start,
                "endLine": max_end,
            },
            sort_key=min_start,
            config=RunnableConfig(
                prompt_type="understand_code"
            )
        )

        content = getattr(result, "content", str(result))
        sanitized = _sanitize_llm_json_output(content)
        parsed = json.loads(sanitized)
        normalized = _normalize_analysis_structure(parsed)
        return normalized

    except Exception as e:
        err_msg = f"Understanding 과정에서 분석 관련 LLM 호출하는 도중 오류가 발생했습니다: {str(e)}"
        logging.error(err_msg)
        raise LLMCallError(err_msg)