import json
import logging
import os
from langchain.globals import set_llm_cache
from langchain_community.cache import SQLiteCache
from langchain.prompts import PromptTemplate
from langchain.schema.runnable import RunnablePassthrough
from util.llm_client import get_llm
from util.exception  import LLMCallError
from langchain_core.output_parsers import JsonOutputParser
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
- 각 범위는 독립적으로 분석되어야 함
- 누락 또는 초과된 분석 결과는 허용되지 않음
 - 강제 검증 규칙:
   - localTables에는 '@'가 포함된 DB 링크 표기를 절대 넣지 말 것(로컬 DB만)
   - dbLinks의 name에는 반드시 '@'가 포함될 것(외부 DB만)
   - 동일 테이블을 localTables와 dbLinks에 동시에 넣지 말 것(상호 배타)


[ANALYSIS_REQUIREMENTS]
각 지정된 코드 범위에 대해 다음 네 가지 섹션의 정보를 정확히 추출하세요:


[SECTION_1_CODE_SUMMARY]
코드 동작 분석 및 요약:

분석 범위:
- 해당 코드 범위가 속한 프로시저/함수명을 명시
- 코드 범위의 전체적인 목적과 역할을 파악

세부 분석 요소:
- 변수 할당 및 초기화: 각 변수의 목적과 설정 의미
- 조건 분기 로직: IF, CASE, WHEN 문의 판단 기준과 분기별 목적
- 반복 처리 로직: FOR, WHILE, LOOP 문의 조건과 반복 목적
- 데이터베이스 작업: SELECT, INSERT, UPDATE, DELETE, MERGE 작업의 대상과 목적
- 예외 처리: EXCEPTION 블록의 처리 방식과 목적
- 트랜잭션 제어: COMMIT, ROLLBACK 등의 사용 목적

출력 형식:
프로시저명을 포함한 상세한 설명을 자연어로 작성
예시: "FN_PROCESS_ORDER 프로시저에서 v_order_date에 SYSDATE를 할당하여 주문 처리 기준일을 설정하고, v_total_count > 100 조건으로 대량 주문 여부를 판단합니다. ORDER_MASTER 테이블에서 미처리 주문을 조회하여 ORDER_HISTORY 테이블에 처리 이력을 생성합니다."


[SECTION_2_VARIABLE_IDENTIFICATION]
변수 식별 및 분류:

식별 대상 변수 유형:
- 로컬 변수: v_, l_, temp_ 등의 접두사를 가진 변수
- 매개변수: p_, param_, in_, out_, inout_ 등의 접두사를 가진 변수
- 시스템 변수: i_, o_ 등의 입출력 변수
- 레코드 타입: %ROWTYPE으로 선언된 변수
- 컬럼 타입: %TYPE으로 선언된 변수
- 커서 변수: CURSOR로 선언된 변수
- 예외 변수: EXCEPTION으로 선언된 변수

식별 규칙:
- 각 범위 내에서 실제로 사용되거나 참조된 변수만 포함
- 변수 선언부가 범위 밖에 있어도 해당 범위에서 사용되면 포함
- 범위 중첩 시 해당 범위에서 직접 사용된 변수만 독립적으로 식별


[SECTION_3_PROCEDURE_CALLS]
프로시저/함수 호출 식별:

호출 형식 분류:
- 외부 패키지 호출: PACKAGE_NAME.PROCEDURE_NAME 형식
- 현재 패키지 내부 호출: PROCEDURE_NAME만 명시된 형식
- 사용자 정의 함수: 사용자가 생성한 함수

제외 대상:
- 시퀀스 객체의 NEXTVAL, CURRVAL 호출
- 내장 함수 호출: 시스템 제공 함수 (SUBSTR, TO_DATE 등)

식별 방법:
- 점(.)으로 구분된 패키지.프로시저 형태 인식
- EXECUTE 문 내의 동적 호출도 포함

중요 제약:
- calls 배열에는 "이름만" 반환 (괄호/인자/공백 제거)
- 예: "PKG.PROC(arg1, arg2)" → "PKG.PROC"


[SECTION_4_TABLE_IDENTIFICATION]
테이블 및 뷰 식별:

식별 대상 SQL 절:
- INSERT INTO 절의 대상 테이블
- UPDATE 절의 대상 테이블  
- DELETE FROM 절의 대상 테이블
- SELECT ... FROM 절의 원본 테이블/뷰
- MERGE INTO 절의 대상 테이블
- JOIN 절의 참조 테이블/뷰
- EXECUTE IMMEDIATE 절의 대상 테이블

테이블명 추출 규칙:
- 스키마명을 포함한 전체 테이블명 추출 (SCHEMA.TABLE_NAME)
- 별칭(alias) 제거하고 실제 테이블명만 추출
- 테이블 접두사 (TPJ_, TBL_, MST_ 등) 유지
- 서브쿼리 내의 테이블도 포함
- JOIN 절의 모든 테이블 포함
 - localTables에는 DB 링크 참조('@' 포함)을 절대 포함하지 않음. 로컬 DB 테이블만 포함

제외 대상:
- 임시 테이블 (TEMPORARY TABLE, TEMP TABLE)
- CTE(Common Table Expression)의 별칭
- WITH 절에서 정의된 임시 결과 집합
- DUAL 테이블 (Oracle 시스템 테이블)
- 시퀀스 객체

DB 링크 감지 및 표기 규칙:
- 테이블/뷰 참조에 '@' 기호가 포함되어 있으면 DB 링크 사용으로 간주
- 반환 시 원문 표기를 그대로 유지 (예: '스키마.테이블명@DB_링크명')
- 스키마, 테이블명, DB 링크 이름을 모두 그대로 유지
- 동적 SQL(EXECUTE IMMEDIATE) 내에서도 동일 규칙 적용
 - DB 링크 대상은 localTables에 넣지 말고 dbLinks로만 반환


[SECTION_5_DB_LINK_ACCESS_CLASSIFICATION]
DB 링크 읽기/쓰기 구분 규칙:

- '읽기(r)': 외부 DB로부터 데이터를 조회만 하는 경우
  - 예: SELECT ... FROM 스키마.테이블@링크, SELECT ... JOIN 스키마.테이블@링크
- '쓰기(w)': 외부 DB의 테이블에 직접 쓰기/갱신/삭제하는 경우
  - 예: INSERT INTO 스키마.테이블@링크, UPDATE 스키마.테이블@링크, DELETE FROM 스키마.테이블@링크, MERGE INTO 스키마.테이블@링크
- 추가 원칙: 외부 DB에서 읽어와 내 DB에 쓰는 경우는 'r'로 분류하며, 외부 DB에 직접 쓰는 경우에만 'w'로 분류
- EXECUTE IMMEDIATE 등 동적 SQL인 경우에도 위 규칙을 기준으로 실제 수행 의도를 분석하여 r/w를 판별
- 동일 범위 내에 여러 DB 링크 테이블이 있을 수 있으며 각 테이블별로 개별적으로 r/w를 판별


[SECTION_5_OUTPUT_FORMAT]
출력 형식 및 구조:

JSON 스키마:
```json
{{
    "analysis": [
        {{
            "startLine": 범위_시작_라인_번호,
            "endLine": 범위_종료_라인_번호,
            "summary": "코드_동작_요약_설명",
            "localTables": ["스키마.테이블명1", "스키마.테이블명2"],
            "calls": ["호출된 프로시저/함수명칭1", "호출된 프로시저/함수명칭2"],
            "variables": ["변수명1", "변수명2"],
            "dbLinks": [
                {{ "name": "스키마.테이블명@DB_링크명", "mode": "r" }},
                {{ "name": "스키마.테이블명@DB_링크명", "mode": "w" }}
            ]
        }}
    ]
}}
```

출력 제약사항:
- JSON 형식 외의 부가 설명이나 주석 금지
- "localTables", "calls", "variables" 배열 요소는 문자열 타입으로 출력
- "dbLinks" 배열 요소는 객체이며 각 객체는 name(문자열), mode(문자열: 'r' 또는 'w') 필드를 가짐
- 빈 배열도 허용되며 null 값은 사용 금지
- startLine과 endLine은 정수 타입으로 정확히 명시
- summary는 상세하고 구체적인 설명을 한국어로 작성
""")


def understand_code(sp_code, context_ranges, context_range_count, api_key, locale):
    try:
        ranges_json = json.dumps(context_ranges)
        llm = get_llm(temperature=0, api_key=api_key)
        
        chain = (
            RunnablePassthrough()
            | prompt
            | llm
        )

        json_parser = JsonOutputParser()
        result = chain.invoke({"code": sp_code, "ranges": ranges_json, "count": context_range_count, "locale": locale})
        json_parsed_content = json_parser.parse(result.content)
        # logging.info(f"토큰 수: {result.usage_metadata}")     
        return json_parsed_content
    
    except Exception as e:
        err_msg = f"Understanding 과정에서 분석 관련 LLM 호출하는 도중 오류가 발생했습니다: {str(e)}"
        logging.error(err_msg)
        raise LLMCallError(err_msg)