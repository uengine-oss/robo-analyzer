import json
import logging
import os
import re
from langchain.globals import set_llm_cache
from langchain_community.cache import SQLiteCache
from langchain.prompts import PromptTemplate
from langchain.schema.runnable import RunnablePassthrough
from util.llm_client import get_llm
from util.exception  import LLMCallError
db_path = os.path.join(os.path.dirname(__file__), 'langchain.db')
set_llm_cache(SQLiteCache(database_path=db_path))

prompt = PromptTemplate.from_template(
f"""
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
   - analysis 각 요소는 startLine/endLine을 반드시 포함할 것 (정수)
   - 값이 없으면 해당 필드는 다음 기본값을 사용: localTables=[], calls=[], variables=[], fkRelations=[], dbLinks[]
   - null 사용 절대 금지. 빈값은 빈 배열([]) 또는 빈 객체({{}})로만 표현
   - 코드펜스(```), 주석(//, /* */), 트레일링 콤마 허용 금지. 순수 JSON만 출력


[ANALYSIS_REQUIREMENTS]
각 지정된 코드 범위에 대해 다음 섹션의 정보를 정확히 추출하세요:


[SECTION_1_CODE_SUMMARY]
코드 동작 분석 및 요약:

분석 범위:
- 코드 범위의 전체적인 목적과 역할을 파악

세부 분석 요소:
- 변수 할당 및 초기화: 각 변수의 목적과 설정 의미
- 조건 분기 로직: IF, CASE, WHEN 문의 판단 기준과 분기별 목적
- 반복 처리 로직: FOR, WHILE, LOOP 문의 조건과 반복 목적
- 데이터베이스 작업: DML 관련(SELECT, INSERT, UPDATE, DELETE, MERGE, EXECUTE_IMMEDIATE, FETCH) 작업의 대상과 목적
- 컬럼 및 키/관계 명시: 모든 DML/JOIN에 대해 구체적인 컬럼명을 스키마.테이블.컬럼 형식으로 명확히 나열하고, 기본키(PK), 고유키(UK), 외래키(FK) 여부를 명시. 외래키는 "로컬테이블.컬럼 -> 참조테이블.참조컬럼" 형식으로 정확히 표기. JOIN 조건의 컬럼 쌍도 모두 명시
- 예외 처리: EXCEPTION 블록의 처리 방식과 목적
- 트랜잭션 제어: COMMIT, ROLLBACK 등의 사용 목적

출력 형식:
상세한 설명을 자연어로 작성
예시: "v_order_date에 CURRENT_DATE를 할당하여 주문 처리 기준일을 설정하고, v_total_count > 100 조건으로 대량 주문 여부를 판단합니다. TPJ.ORDER_MASTER OM과 TPJ.ORDER_DETAIL OD를 OM.ORDER_ID = OD.ORDER_ID로 조인하여 미처리 주문을 조회합니다(조인 컬럼: TPJ.ORDER_MASTER.ORDER_ID[PK] = TPJ.ORDER_DETAIL.ORDER_ID[FK]). INSERT 대상은 TPJ.ORDER_HISTORY(컬럼: ORDER_ID, STATUS, CREATED_AT)이며, TPJ.ORDER_MASTER(컬럼: ORDER_ID[PK], CUSTOMER_ID[FK -> TPJ.CUSTOMER(CUSTOMER_ID)], ORDER_DATE)에서 값을 읽어 처리 이력을 생성합니다. FK 관계는 TPJ.ORDER_MASTER.CUSTOMER_ID -> TPJ.CUSTOMER.CUSTOMER_ID로 명시합니다."


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
- EXECUTE_IMMEDIATE 문 내의 동적 호출도 포함

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
- 변수에 저장된 SQL 문자열
- 프로시저 및 함수 호출 인자로 SQL 관련 문자열이 있을 경우

테이블명 추출 규칙:
- 스키마명을 포함한 전체 테이블명 추출 (SCHEMA.TABLE_NAME)
- 별칭(alias) 제거하고 실제 테이블명만 추출
- 테이블 접두사 (TPJ_, TBL_, MST_ 등) 유지
- 서브쿼리 내의 테이블도 포함
- JOIN 절의 모든 테이블 포함
- localTables에는 DB 링크 표기('@')가 포함된 테이블을 절대 넣지 않음 (로컬 DB만)

제외 대상:
- 임시 테이블 (TEMPORARY TABLE, TEMP TABLE)
- CTE(Common Table Expression)의 별칭
- WITH 절에서 정의된 임시 결과 집합
- DUAL 테이블 (Oracle 시스템 테이블)
- 시퀀스 객체



[SECTION_5_FOREIGN_KEY_RELATIONS]
외래키(FK) 관계 식별 및 표기:

수집 대상:
- 테이블 간에 외래키 관계가 있는 경우
- JOIN 조건에서 동등 비교(=)로 연결된 컬럼 쌍 중 외래키로 해석되는 관계
- INSERT/UPDATE 시 참조 무결성을 전제로 사용하는 컬럼-테이블 관계
- 동적 SQL(EXECUTE IMMEDIATE, 변수에 저장된 SQL 문자열)에서 식별 가능한 FK 관계

식별 및 표기 규칙:
- 방향은 반드시 FK → PK/UK로 기술합니다.
- 각 관계는 객체로 표현하며 다음 필드를 포함합니다: sourceTable, sourceColumn, targetTable, targetColumn
- 모든 값은 스키마를 포함한 완전 수식명을 사용합니다(예: TPJ.ORDER_DETAIL, TPJ.ORDER_DETAIL.ORDER_ID).
- targetColumn은 기본키(PK) 또는 고유키(UK)여야 하며, 파악이 어려운 경우에도 코드 상 명시된 조인/참조 컬럼을 사용합니다.
- 동일 관계가 여러 번 등장하면 중복 없이 한 번만 기록합니다.
- 컬럼명 과 테이블명은 SP 코드에서 명시된 이름 그대로 사용합니다. 임의로 변경하거나 축약하지 마세요.  

예시:
- {{ "sourceTable": "TPJ.ORDER_DETAIL", "sourceColumn": "ORDER_ID", "targetTable": "TPJ.ORDER_MASTER", "targetColumn": "ORDER_ID" }}
- {{ "sourceTable": "TPJ.ORDER_MASTER", "sourceColumn": "CUSTOMER_ID", "targetTable": "TPJ.CUSTOMER", "targetColumn": "CUSTOMER_ID" }}  


[SECTION_6_DB_LINK_IDENTIFICATION]
DB 링크 식별 및 범위 규칙:
- 직접 DB 링크 접근 여부, 실제 DB에 읽기/쓰기 여부와 상관없이 그저 DB 링크 패턴 및 표기가 발견되면 dbLinks에 포함합니다. 즉, 프로시저 인자로 전달되는 문자열, 변수에 할당된 문자열이라도, DB 링크로 판단
- 식별 범위: 코드 전반(문자열 리터럴, 변수 값, 프로시저/함수 인자, 테이블/뷰 식별자, 동적 SQL 문자열)에서 '스키마.테이블@링크' 또는 '테이블@링크' 패턴 및 표기가 발견되면 dbLinks에 포함합니다. 주석은 제외합니다.
- name에는 반드시 '@'가 포함되어야 하며, 원문 표기를 그대로 사용합니다(대소문자/스키마/링크명 보존).
- localTables에는 '@'가 포함된 대상을 절대 넣지 않습니다. 모든 외부 대상은 analysis[i].dbLinks에만 기록합니다.
- 동일 대상이 여러 번 등장하면 중복 없이 한 번만 기록합니다.
- 실제 DB 링크 식별은 의미로 하지말고 'Table@DBLINK' 패턴 및 표기 자체가 있는 구문 범위에만 결과로 dblinks에 포함하면되며, 해당 패턴 및 표기가가 존재하지않는 구문 범위에는 포함하지 마세요.   
- 즉 해당 DB 링크를 실행하거나 변수에 담겨 있는 의미로 판단하지말고, 해당 범위에 표기만 존재하는지를 판단하여 표기가 존재하지 않는 범위에 대해서는 결과로 dblinks는 [] 빈 리스트로 반환 의미 부여 x 


DB 링크 읽기/쓰기 구분 규칙:
- 실제 DB 읽기 쓰기 같은건 없거나, 단순히 문자열 리터럴인 경우, 읽기모드 'r'로 판단
- 실행 여부가 모호한 경우에는 mode='r'로 분류합니다.
- 동적 SQL 또는 DML 문맥(SELECT/INSERT/UPDATE/DELETE/MERGE/EXECUTE IMMEDIATE)로 외부 대상에 실제 접근이 명확한 경우 r/w를 판별합니다.
- '읽기(r)': 외부 DB로부터 데이터를 조회만 하는 경우
  - 예: SELECT ... FROM 스키마.테이블@링크, SELECT ... JOIN 스키마.테이블@링크
- '쓰기(w)': 외부 DB의 테이블에 직접 쓰기/갱신/삭제하는 경우
  - 예: INSERT INTO 스키마.테이블@링크, UPDATE 스키마.테이블@링크, DELETE FROM 스키마.테이블@링크, MERGE INTO 스키마.테이블@링크
- 추가 원칙: 외부 DB에서 읽어와 내 DB에 쓰는 경우는 'r'로 분류하며, 외부 DB에 직접 쓰는 경우에만 'w'로 분류
- EXECUTE IMMEDIATE 등 동적 SQL인 경우에도 위 규칙을 기준으로 실제 수행 의도를 분석하여 r/w를 판별
- 동일 범위 내에 여러 DB 링크 테이블이 있을 수 있으며 각 테이블별로 개별적으로 r/w를 판별


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
      "localTables": ["스키마.테이블명1", "스키마.테이블명2"],
      "calls": ["호출된 프로시저/함수명칭1", "호출된 프로시저/함수명칭2"],
      "variables": ["변수명1", "변수명2"],
      "fkRelations": [
        {{ "sourceTable": "스키마.테이블명", "sourceColumn": "컬럼명", "targetTable": "스키마.참조테이블명", "targetColumn": "참조컬럼명" }}
      ],
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
- analysis[i]에는 startLine/endLine이 필수
- analysis[i].fkRelations 요소는 객체이며 sourceTable, sourceColumn, targetTable, targetColumn(모두 문자열)만 포함
- analysis[i].dbLinks 요소는 객체이며 name(문자열, '@' 권장), mode('r'|'w')만 포함
- 빈 배열도 허용되며 null 값은 사용 금지
- summary는 상세하고 구체적인 설명을 한국어로 작성
- 코드펜스(```json ... ``` 등) 포함 금지, 트레일링 콤마 금지
- dbLinks.name은 반드시 '테이블@링크' 또는 '스키마.테이블@링크' 형식. 해당 패턴이 범위 내에 없으면 dbLinks는 []
- localTables에도 테이블 표기가 없으면 localTables는 []
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

        result = chain.invoke({"code": sp_code, "ranges": ranges_json, "count": context_range_count, "locale": locale})
        content = getattr(result, "content", str(result))
        sanitized = _sanitize_llm_json_output(content)
        parsed = json.loads(sanitized)
        normalized = _normalize_analysis_structure(parsed)
        return normalized

    except Exception as e:
        err_msg = f"Understanding 과정에서 분석 관련 LLM 호출하는 도중 오류가 발생했습니다: {str(e)}"
        logging.error(err_msg)
        raise LLMCallError(err_msg)