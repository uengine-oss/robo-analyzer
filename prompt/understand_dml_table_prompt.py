import json
import logging
import os
from langchain_core.globals import set_llm_cache
from langchain_community.cache import SQLiteCache
from langchain_core.prompts import PromptTemplate
from langchain_core.output_parsers import JsonOutputParser
from langchain_core.runnables import RunnablePassthrough, RunnableConfig
from util.llm_client import get_llm
from util.llm_audit import invoke_with_audit
from util.exception import LLMCallError


db_path = os.path.join(os.path.dirname(__file__), 'langchain.db')
set_llm_cache(SQLiteCache(database_path=db_path))


prompt = PromptTemplate.from_template(
"""
[ROLE]
당신은 PostgreSQL 및 PL/pgSQL DML 코드를 분석하여 테이블, 컬럼, 관계 메타데이터를 정확하게 추출하는 전문가입니다.
주어진 DML 구문을 체계적으로 분석하여 구조화된 메타데이터를 생성해야 합니다.


[LANGUAGE_SETTING]
응답 언어: {locale}
모든 분석 결과와 설명은 지정된 언어로 생성해야 합니다.


[INPUT_DATA]
분석 대상 DML 코드:
{code}


분석할 코드 범위 목록:
{ranges}


[ANALYSIS_OBJECTIVES]
각 DML 구문(SELECT, INSERT, UPDATE, DELETE, MERGE, EXECUTE IMMEDIATE, CTE 등)에 대해 다음 정보를 추출합니다:
1. 테이블 메타데이터: 테이블명, accessMode(r/w), 테이블 역할 설명
2. 컬럼 메타데이터: 컬럼명, 데이터 타입, NULL 가능 여부, 컬럼 역할 설명
3. 외래키 관계: sourceTable, sourceColumn, targetTable, targetColumn
4. DB 링크 정보: name, mode


[SECTION_1_TABLE_AND_COLUMN_IDENTIFICATION]
테이블 및 컬럼 식별:

=== 테이블 식별 대상 SQL 절 ===
- INSERT INTO 절의 대상 테이블
- UPDATE 절의 대상 테이블
- DELETE FROM 절의 대상 테이블
- SELECT ... FROM 절의 원본 테이블/뷰
- MERGE INTO 절의 대상 테이블
- JOIN 절의 참조 테이블/뷰 (INNER JOIN, LEFT JOIN, RIGHT JOIN, FULL JOIN 등)
- EXECUTE IMMEDIATE 절의 대상 테이블
- 변수에 저장된 SQL 문자열 내의 테이블
- 프로시저 및 함수 호출 인자로 전달되는 SQL 관련 문자열의 테이블
- CTE(Common Table Expression) 내부의 실제 테이블 (WITH 절 정의 자체는 제외)

=== 테이블명 추출 규칙 ===
- 스키마명을 포함한 전체 테이블명 추출 (SCHEMA.TABLE_NAME)
- 별칭(alias)을 제거하고 실제 테이블명만 추출
- 테이블 접두사 (TPJ_, TBL_, MST_, TB_ 등)는 원문 그대로 유지
- 서브쿼리 내의 테이블도 모두 포함
- JOIN 절의 모든 테이블을 빠짐없이 포함
- 대소문자는 코드에 명시된 원문 그대로 유지
- DB 링크 표기('@')가 포함된 테이블은 table 필드에 넣지 않고 dbLinks 배열에만 기록

=== accessMode(r/w) 분류 규칙 ===
- 'w': INSERT, UPDATE, DELETE, MERGE 등으로 해당 테이블에 데이터가 직접 쓰이거나 삭제되는 경우
- 'r': SELECT, JOIN, WHERE, 서브쿼리 등 읽기 전용으로 사용되는 경우
- 동일 범위에서 읽기와 쓰기가 모두 발생하면 'w'를 우선 적용
- 하나의 DML 범위에 여러 테이블이 포함될 수 있으므로 각 테이블을 독립적으로 판단

=== 테이블 제외 대상 ===
- 임시 테이블 (TEMPORARY TABLE, TEMP TABLE)
- CTE(Common Table Expression)의 별칭 자체
- WITH 절에서 정의된 임시 결과 집합의 이름
- DUAL 테이블 (Oracle 시스템 테이블)
- 시퀀스 객체
- 시스템 카탈로그 테이블 (pg_catalog, information_schema 등)

=== 컬럼 식별 대상 ===
- SELECT 절에 명시된 컬럼 (*, 집계 함수 제외)
- INSERT INTO 절의 컬럼 목록
- UPDATE SET 절의 대상 컬럼
- WHERE 절에 사용된 조건 컬럼
- JOIN 조건에 사용된 컬럼
- ORDER BY, GROUP BY 절의 컬럼
- RETURNING 절의 컬럼

=== 컬럼 메타데이터 추출 규칙 ===
- 컬럼명(name): 코드에 명시된 원문 그대로 추출 (대소문자 유지)
- 데이터 타입(dtype): 코드에서 추론 가능한 경우에만 기록, 불명확하면 빈 문자열
- NULL 가능 여부(nullable): 추론 가능하면 true/false, 불명확하면 true로 기본 설정
- 컬럼 설명(description): 해당 컬럼이 DML 구문에서 수행하는 역할을 자연어로 간결하게 작성
  - 예: "주문 번호를 저장", "고객 ID로 조회 조건 설정", "총 금액을 업데이트"
  - 중복 표현 금지, 핵심만 서술

=== 테이블 설명(tableDescription) 작성 규칙 ===
- 해당 범위에서 테이블이 수행하는 핵심 역할을 1~2문장으로 요약
- DML 종류와 주요 목적을 명시
- 예시 1: "주문 마스터 테이블에서 주문 정보를 조회하여 검증합니다."
- 예시 2: "고객 상세 테이블에 신규 고객 데이터를 삽입합니다."
- 예시 3: "주문 상세와 상품 마스터를 조인하여 주문 내역을 조회합니다."


[SECTION_2_FOREIGN_KEY_RELATIONS]
외래키(FK) 관계 식별 및 표기:

=== 수집 기준 ===
- 단순히 컬럼명이 같거나 의미가 비슷하다는 이유로는 외래키로 간주하지 않습니다.
- 한 테이블이 다른 테이블의 기본키(PK)나 고유키(UK)를 참조한다는 구조가 코드에 명확히 드러날 때만 기록합니다.
- 명확한 근거 없이 추론하거나 추측하지 마십시오. 근거가 없으면 fkRelations는 빈 배열([])로 유지합니다.

=== 식별 예시 ===
- 조인 조건에서 FK→PK 구조가 드러나는 경우: `FROM DETAIL d JOIN MASTER m ON d.MASTER_ID = m.ID`
- INSERT/UPDATE 구문에서 참조 무결성을 전제로 하는 경우: `INSERT INTO DETAIL (MASTER_ID, ...)`
- 서브쿼리나 동적 SQL에서도 FK→PK 구조가 확실히 나타나면 동일하게 기록합니다.

=== 표기 형식 ===
- 각 관계는 다음 필드를 모두 포함해야 합니다.
  - sourceTable: FK를 가진 테이블 (스키마 포함)
  - sourceColumn: FK 컬럼명
  - targetTable: 참조 대상 테이블 (스키마 포함)
  - targetColumn: 참조 대상 컬럼명 (PK 또는 UK)
- 모든 문자열은 코드에 나타난 원문 케이스를 유지합니다.


[SECTION_3_DB_LINK_IDENTIFICATION]
DB 링크 식별 및 범위 규칙:

=== DB 링크 식별 기준 ===
- 직접 DB 링크 접근 여부, 실제 DB 읽기/쓰기 여부와 무관하게 DB 링크 패턴 및 표기가 발견되면 dbLinks에 포함
- 프로시저 인자로 전달되는 문자열, 변수에 할당된 문자열이라도 DB 링크 패턴이 있으면 포함
- 식별 범위: 코드 전반에서 다음 패턴을 탐색
  - 문자열 리터럴
  - 변수 값
  - 프로시저/함수 인자
  - 테이블/뷰 식별자
  - 동적 SQL 문자열
  - '스키마.테이블@링크' 또는 '테이블@링크' 패턴
- 주석 내 표기는 제외

=== DB 링크 표기 규칙 ===
- name 필드에는 반드시 '@' 문자가 포함되어야 함
- 원문 표기를 그대로 사용 (대소문자, 스키마명, 링크명 모두 보존)
- 예시: "TPJ.ORDER_MASTER@DBLINK01", "CUSTOMER@REMOTE_DB", "SCHEMA.TABLE@LINK"
- table 필드(로컬 테이블)에는 '@'가 포함된 대상을 절대 넣지 않음
- 모든 외부 대상은 dbLinks 배열에만 기록
- 동일 대상이 여러 번 등장하면 중복 없이 한 번만 기록

=== DB 링크 범위 판별 규칙 ===
- 실제 DB 링크 식별은 의미 해석이 아닌 'Table@DBLINK' 패턴 표기 자체의 존재 여부로 판단
- 해당 패턴 표기가 존재하는 구문 범위에만 dbLinks에 포함
- 해당 패턴 표기가 존재하지 않는 구문 범위에는 dbLinks를 빈 배열([])로 반환
- DB 링크를 실행하거나 변수에 담겨 있다는 의미로 판단하지 말 것
- 해당 범위에 표기가 실제로 존재하는지만 판단

=== DB 링크 읽기/쓰기 구분 규칙 ===
- 실제 DB 읽기/쓰기가 없거나 단순히 문자열 리터럴인 경우: mode='r' (읽기 모드)
- 실행 여부가 모호한 경우: mode='r'로 기본 분류
- 동적 SQL 또는 DML 문맥(SELECT/INSERT/UPDATE/DELETE/MERGE/EXECUTE IMMEDIATE)로 외부 대상에 실제 접근이 명확한 경우 r/w를 판별

- 읽기(r): 외부 DB로부터 데이터를 조회만 하는 경우
  - 예: SELECT ... FROM 스키마.테이블@링크
  - 예: SELECT ... JOIN 스키마.테이블@링크
  - 예: WHERE EXISTS (SELECT ... FROM 테이블@링크)

- 쓰기(w): 외부 DB의 테이블에 직접 쓰기/갱신/삭제하는 경우
  - 예: INSERT INTO 스키마.테이블@링크
  - 예: UPDATE 스키마.테이블@링크 SET ...
  - 예: DELETE FROM 스키마.테이블@링크
  - 예: MERGE INTO 스키마.테이블@링크

- 추가 원칙:
  - 외부 DB에서 읽어와 내 DB에 쓰는 경우는 'r'로 분류
  - 외부 DB에 직접 쓰는 경우에만 'w'로 분류
  - EXECUTE IMMEDIATE 등 동적 SQL인 경우에도 위 규칙을 기준으로 실제 수행 의도를 분석하여 r/w 판별
  - 동일 범위 내에 여러 DB 링크 테이블이 있을 수 있으며 각 테이블별로 개별적으로 r/w를 판별


[CONSTRAINTS]
필수 준수 사항:
- ranges 배열 순서대로 결과를 작성하고 startLine/endLine을 정확히 포함
- 각 범위는 독립적으로 분석되어야 함
- null 사용 절대 금지. 빈 값은 빈 배열([]) 또는 빈 문자열("")로만 표현
- 코드펜스(```), 주석(//, /* */), 트레일링 콤마 허용 금지. 순수 JSON만 출력
- 테이블/컬럼명은 입력 코드에 나온 원문 케이스 그대로 유지
- 테이블 설명과 컬럼 설명은 자연어 문장으로 작성하며 중복 표현 금지
- DDL 형태로 추측하지 말고 코드에서 식별 가능한 정보만 제공
- 동일 테이블이 여러 번 등장해도 한 엔트리로 통합하되 컬럼과 관계는 전체 누적


[OUTPUT_FORMAT]
출력 JSON 스키마:
```json
{{
  "ranges": [
    {{
      "startLine": 범위_시작_라인_번호,
      "endLine": 범위_종료_라인_번호,
      "tables": [
        {{
          "table": "SCHEMA.TABLE_NAME",
          "accessMode": "r|w",
          "tableDescription": "테이블 역할 요약 (1~2문장)",
          "columns": [
            {{
              "name": "컬럼명",
              "dtype": "데이터타입",
              "nullable": true,
              "description": "컬럼 역할 설명"
            }}
          ],
          "fkRelations": [
            {{
              "sourceTable": "스키마.테이블명",
              "sourceColumn": "컬럼명",
              "targetTable": "스키마.참조테이블명",
              "targetColumn": "참조컬럼명"
            }}
          ],
          "dbLinks": [
            {{
              "name": "스키마.테이블명@DB_링크명",
              "mode": "r"
            }}
          ]
        }}
      ]
    }}
  ]
}}
```

출력 제약사항:
- JSON 형식 외의 부가 설명이나 주석 금지
- "ranges" 배열의 길이는 입력 ranges와 동일해야 함
- 각 range 항목의 startLine/endLine은 입력과 동일하게 유지
- tables 배열 안의 객체들은 위 스키마를 정확히 따름
- ranges 배열의 순서는 입력 ranges 순서를 그대로 따라야 함
- 각 range 항목에는 최소한 빈 배열이라도 tables 필드를 포함해야 함
- accessMode는 반드시 'r' 또는 'w'
- columns, fkRelations, dbLinks 배열은 빈 배열 가능
- 빈 값은 null 대신 빈 배열([]) 또는 빈 문자열("") 사용
- 코드펜스(```json ... ``` 등) 포함 금지
- 트레일링 콤마 금지
- dbLinks.name은 반드시 '@' 포함, 해당 패턴이 범위 내에 없으면 dbLinks는 []
- table 필드에는 '@' 포함 금지 (로컬 테이블만)
"""
)


def understand_dml_tables(code: str, ranges: list[dict], api_key: str, locale: str) -> dict:
    try:
        ranges_json = json.dumps(ranges, ensure_ascii=False)
        llm = get_llm(api_key=api_key)

        chain = (
            RunnablePassthrough()
            | prompt
            | llm
            | JsonOutputParser()
        )

        payload = {
            "code": code,
            "ranges": ranges_json,
            "locale": locale,
        }

        starts = [item.get("startLine") for item in ranges or [] if isinstance(item, dict)]
        min_start = min((value for value in starts if isinstance(value, int)), default=None)

        result = invoke_with_audit(
            chain,
            payload,
            prompt_name="prompt/understand_dml_table_prompt.py",
            input_payload={"code": code, "ranges": ranges, "locale": locale},
            metadata={"ranges": ranges},
            sort_key=min_start,
            config=RunnableConfig(
                prompt_type="understand_dml_tables"
            )
        )
        if not isinstance(result, dict):
            return {"ranges": []}
        result.setdefault("ranges", [])
        return result

    except Exception as e:
        err_msg = f"Understanding 과정에서 DML 테이블 메타를 추출하는 도중 오류가 발생했습니다: {str(e)}"
        logging.error(err_msg)
        raise LLMCallError(err_msg)

