import logging
import os
import json
from abc import ABC, abstractmethod
from typing import List, Tuple, AsyncGenerator, Any, Dict

# LLM 통합을 위한 임포트
from langchain.globals import set_llm_cache
from langchain_community.cache import SQLiteCache
from langchain_anthropic import ChatAnthropic
from langchain_core.output_parsers import JsonOutputParser
from langchain.prompts import PromptTemplate
from langchain.schema.runnable import RunnablePassthrough
from util.exception import LLMCallError

logger = logging.getLogger(__name__)
db_path = os.path.join(os.path.dirname(__file__), 'langchain.db')
set_llm_cache(SQLiteCache(database_path=db_path))


# =============================================================================
# 기본 전략 클래스
# =============================================================================
class UnderstandingStrategy(ABC):
    """
    DBMS별 Understanding 전략을 정의하는 추상 클래스
    
    각 DBMS 특화 전략은 이 클래스를 상속받아 구현합니다.
    공통적인 분석 메서드와 DBMS별 특화 메서드를 포함합니다.
    """
    

    def __init__(self):
        """
        전략 초기화
        기본값으로 DBMS 유형을 unknown으로 설정
        """
        self.dbms_type = "unknown"
    

    def get_prompt_templates(self) -> Dict[str, str]:
        """
        프롬프트 템플릿 반환 - 기본 템플릿 제공
        
        Returns:
            Dict[str, str]: 프롬프트 유형별 템플릿 사전
                - ddl: DDL 분석 프롬프트
                - variables: 변수 분석 프롬프트
                - code: DBMS 특화 코드 분석 프롬프트
                - summary: 요약 프롬프트
        """
        return {
            "ddl": self._get_common_ddl_prompt(),
            "variables": self._get_common_variables_prompt(),
            "code": self._get_dbms_specific_code_prompt(),
            "summary": self._get_common_summary_prompt()  # 공통 요약 프롬프트 사용
        }
    
    
    async def analyze_ddl(self, ddl_content: str, api_key: str) -> Dict:
        """
        DDL 분석 실행
        
        Args:
            ddl_content: 분석할 DDL 내용
            api_key: Anthropic API 키

        Returns:
            Dict: DDL 분석 결과를 JSON 형식으로 반환
            
        Raises:
            LLMCallError: LLM 호출 중 오류 발생 시
        """
        try:
            # DDL 프롬프트 템플릿 가져오기
            template = self._get_common_ddl_prompt()
            prompt = PromptTemplate.from_template(template)
            llm = ChatAnthropic(model="claude-3-7-sonnet-20250219", max_tokens=8000, temperature=0.1, api_key=api_key)
            
            # LLM 분석 체인 구성
            chain = (
                RunnablePassthrough()
                | prompt
                | llm
                | JsonOutputParser()
            )
            
            # 분석 실행 및 결과 반환
            result = chain.invoke({"ddl_content": ddl_content})
            return result
        except Exception as e:
            err_msg = f"DDL 분석 중 LLM 호출 오류 발생: {str(e)}"
            logger.error(err_msg)
            raise LLMCallError(err_msg)
    
    
    async def analyze_variables(self, declaration_code: str, ddl_tables: str, api_key: str) -> Dict:
        """
        변수 분석 실행
        
        Args:
            declaration_code: 분석할 변수 선언 코드
            ddl_tables: 테이블 정보 (DDL 분석 결과)
            api_key: Anthropic API 키

        Returns:
            Dict: 변수 분석 결과를 JSON 형식으로 반환
            
        Raises:
            LLMCallError: LLM 호출 중 오류 발생 시
        """
        try:
            # 변수 분석 프롬프트 템플릿 가져오기
            template = self._get_common_variables_prompt()
            prompt = PromptTemplate.from_template(template)
            llm = ChatAnthropic(model="claude-3-7-sonnet-20250219", max_tokens=8000, temperature=0.1, api_key=api_key)

            # LLM 분석 체인 구성
            chain = (
                RunnablePassthrough()
                | prompt
                | llm
                | JsonOutputParser()
            )
            
            # 분석 실행 및 결과 반환
            result = chain.invoke({
                "declaration_code": declaration_code,
                "ddl_tables": ddl_tables
            })
            return result
        except Exception as e:
            err_msg = f"변수 분석 중 LLM 호출 오류 발생: {str(e)}"
            logger.error(err_msg)
            raise LLMCallError(err_msg)
    
    
    async def analyze_code(self, schema_name, code, ranges, count, api_key: str) -> Dict:
        """
        DBMS 특화 코드 분석 실행
        
        Args:
            schema_name: 스키마 이름
            code: 분석할 코드
            ranges: 코드 분석 범위
            count: 범위 개수
            api_key: Anthropic API 키

        Returns:
            Dict: 코드 분석 결과를 JSON 형식으로 반환
            
        Raises:
            LLMCallError: LLM 호출 중 오류 발생 시
        """
        try:
            # DBMS 특화 코드 분석 프롬프트 템플릿 가져오기
            template = self._get_dbms_specific_code_prompt()
            prompt = PromptTemplate.from_template(template)
            llm = ChatAnthropic(model="claude-3-7-sonnet-20250219", max_tokens=8000, temperature=0.1, api_key=api_key)

            # LLM 분석 체인 구성
            chain = (
                RunnablePassthrough()
                | prompt
                | llm
                | JsonOutputParser()
            )
            
            # 분석 실행 및 결과 반환
            kwargs = {"schema_name": schema_name, "code": code, "ranges": ranges, "count": count}
            result = chain.invoke(kwargs)
            return result
        except Exception as e:
            err_msg = f"코드 분석 중 LLM 호출 오류 발생: {str(e)}"
            logger.error(err_msg)
            raise LLMCallError(err_msg)
    
    
    async def generate_summary(self, summaries: str, api_key: str) -> Dict:
        """
        요약 생성 실행
        
        Args:
            summaries: 요약할 분석 결과 모음
            api_key: Anthropic API 키

        Returns:
            Dict: 생성된 요약을 JSON 형식으로 반환
            
        Raises:
            LLMCallError: LLM 호출 중 오류 발생 시
        """
        try:
            # 공통 요약 프롬프트 템플릿 가져오기 및 DBMS 유형 반영
            template = self._get_common_summary_prompt().replace("{dbms_type}", self.dbms_type)
            prompt = PromptTemplate.from_template(template)
            llm = ChatAnthropic(model="claude-3-7-sonnet-20250219", max_tokens=8000, temperature=0.1, api_key=api_key)

            # LLM 분석 체인 구성
            chain = (
                RunnablePassthrough()
                | prompt
                | llm
                | JsonOutputParser()
            )
            
            # 분석 실행 및 결과 반환
            result = chain.invoke({"summaries": summaries})
            return result
        except Exception as e:
            err_msg = f"요약 생성 중 LLM 호출 오류 발생: {str(e)}"
            logger.error(err_msg)
            raise LLMCallError(err_msg)
    

    def _get_common_ddl_prompt(self) -> str:
        """
        모든 DBMS에 공통으로 사용할 DDL 프롬프트 템플릿
        
        Returns:
            str: DDL 분석용 프롬프트 템플릿
        """
        return """
        당신은 DDL을 분석하여 테이블 구조를 파악하는 전문가입니다. 주어진 DDL에서 테이블 정보와 관계를 추출합니다.
        
        DDL 내용입니다:
        {ddl_content}
        
        [분석 규칙]
        ===============================================
        1. 전달된 모든 테이블 정보 추출
           - 테이블 이름
           
        2. 전달된 모든 테이블의 컬럼 정보 추출
           - 컬럼명
           - 데이터 타입
           - null 허용 여부
        
        3. 전달된 모든 테이블의 키 정보 추출
           - Primary Key 컬럼
           - Foreign Key 관계 (참조하는 테이블과 컬럼)
        
        [JSON 출력 형식]
        ===============================================
        주석이나 부가설명 없이 다음 JSON 형식으로만 결과를 반환하세요:
        {{
            "analysis": [
                {{
                    "table": {{
                        "name": "테이블명",
                    }},
                    "columns": [
                        {{
                            "name": "컬럼명",
                            "type": "데이터타입",
                            "nullable": "true/false"
                        }}
                    ],
                    "keys": {{
                        "primary": ["컬럼명1", "컬럼명2"],
                        "foreign": [
                            {{
                                "column": "현재 테이블의 컬럼",
                                "references": {{
                                    "table": "참조하는 테이블",
                                    "column": "참조하는 컬럼"
                                }}
                            }}
                        ]
                    }}
                }}
            ]
        }}
        """
    

    def _get_common_variables_prompt(self) -> str:
        """
        모든 DBMS에 공통으로 사용할 변수 프롬프트 템플릿
        
        Returns:
            str: 변수 분석용 프롬프트 템플릿
        """
        return """
        당신은 다양한 DBMS(Oracle, MySQL, PostgreSQL, SQL Server 등)의 저장 프로시저/함수에서 변수를 분석하는 전문가입니다. 주어진 코드에서 모든 변수 선언을 찾아 변수명과 데이터 타입을 추출하는 작업을 수행합니다.

        프로시저/함수 코드입니다:
        {declaration_code}

        테이블 정보입니다:
        {ddl_tables}


        [분석 규칙]
        ===============================================
        1. 변수 선언 식별 (DBMS별 구문 차이 고려)
        - Oracle PL/SQL: DECLARE 섹션의 변수 선언, :new, :old 등의 트리거 변수
        - MySQL: DECLARE로 시작하는 변수 선언, SET으로 초기화
        - PostgreSQL: DECLARE로 시작하는 변수 선언
        - SQL Server: DECLARE @variable 형식의 변수 선언, 테이블 변수(@table TABLE)
        
        - 파라미터 유형 식별 (공통):
            * IN/IN OUT 파라미터 (parameter_type: 'IN'/'IN_OUT'/'INOUT')
            * OUT 파라미터 (parameter_type: 'OUT')
            * 로컬 변수는 parameter_type: 'LOCAL'로 표시
        - 주석이 아닌 실제 선언된 변수만 추출

        2. 변수 유형 (DBMS별 특성)
        - 일반 변수 (접두사 규칙: v_, p_, i_, o_ 등이 있는 경우)
        - Oracle: %ROWTYPE, %TYPE 변수
        - PostgreSQL: %TYPE 변수, RECORD 타입
        - MySQL: 사용자 정의 타입 변수, 커서 변수
        - SQL Server: 테이블 변수, 사용자 정의 타입 변수
        - 모든 DBMS: 기본 스칼라 타입 변수

        3. 데이터 타입 추출 (DBMS별 구문 고려)
        - 기본 데이터 타입 (INTEGER, VARCHAR/VARCHAR2, DATETIME/DATE 등)
        - Oracle: %ROWTYPE의 경우 테이블명을 type으로 지정 (예: "EMPLOYEE%ROWTYPE" -> "EMPLOYEE")
        - Oracle/PostgreSQL: %TYPE의 경우 DDL 정보 참조하여 실제 타입으로 변환
        - SQL Server: 사용자 정의 타입, 테이블 타입
        - 대소문자 구분하여 추출
        
        4. 변수 값 추출
        - 초기값이 설정된 경우 추출 (DEFAULT, :=, =, SET 등의 할당 구문 고려)
        - 초기값이 없는 경우 'None'으로 설정
        - Oracle: %ROWTYPE 변수의 경우 참조 테이블 이름을 값으로 추출
        - SQL Server: 테이블 변수의 경우 스키마 구조 추출

        5. 특수 처리
        - 모든 DBMS의 주석 처리 (/* */, --, #, REM 등)
        - 커서, 커서 파라미터
        - 임시 테이블 선언
        - 기본값이 있는 경우에도 변수로 인식
        - 길이/정밀도 지정 무시하고 데이터 타입만 추출 (예: VARCHAR(100) -> VARCHAR)

        6. 변수 선언부 요약
        - 선언된 모든 변수들을 종합적으로 분석하여 1-2줄로 요약
        - 요약에는 반드시 변수명과 용도를 포함
        - 예시: "프로시저 실행 결과를 저장하는 OUT 파라미터(o_result)와 임시 데이터를 저장하는 로컬 변수들(v_temp_id, v_temp_name)이 선언되어 있음"


        [JSON 출력 형식]
        ===============================================
        주석이나 부가설명 없이 다음 JSON 형식으로만 결과를 반환하세요:
        {{
            "variables": [
                {{
                    "name": "변수명",
                    "type": "데이터타입",
                    "value": "할당값 또는 null, 0",
                    "parameter_type": "IN/OUT/IN_OUT/INOUT/LOCAL",
                }}
            ],
            "summary": "변수 선언부 요약 설명"
        }}
        """
        
    
    def _get_common_summary_prompt(self) -> str:
        """
        모든 DBMS에 공통으로 사용할 요약 프롬프트 템플릿
        
        Returns:
            str: 요약 생성용 프롬프트 템플릿
        """
        return """
        당신은 {dbms_type} 함수와 프로시저의 동작을 분석하고 요약하는 전문가입니다.
        주어진 코드 분석 요약들을 바탕으로 전체 함수/프로시저의 핵심 기능을 간단명료하게 설명해주세요.
        
        분석된 요약 내용:
        {summaries}
                
        [분석 규칙]
        ===============================================
        1. 핵심 기능 파악
        - 프로시저/함수가 수행하는 주요 작업
        - 입력과 출력의 흐름
        - 중요한 비즈니스 로직

        2. 요약 방식
        - 최소 3~4줄로 상세하게 정리
        - 기술적인 용어는 최소화
        - 비즈니스 관점에서 이해하기 쉽게 설명

        예시) 직원의 인사 정보를 갱신하는 프로시저로,
              입력받은 직원 ID를 기준으로 부서 이동, 직급 변경, 급여 조정 등의 정보를 처리하며,
              변경된 정보는 인사 이력 테이블에 자동으로 기록됩니다.
              또한 변경 사항에 따라 관련 부서장과 인사팀에 이메일 알림을 발송합니다."

        예시) 월별 급여 지급 처리를 수행하는 프로시저로,
              해당 월의 근태 기록과 수당 정보를 집계하여 실지급액을 계산하고,
              각 직원별 급여 명세서를 생성합니다.
              계산된 급여는 지정된 은행 계좌로 일괄 이체 요청됩니다."
                
        [JSON 출력 형식]
        ===============================================
        주석이나 부가설명 없이 다음 JSON 형식으로만 결과를 반환하세요:
        {{
            "summary": "함수/프로시저의 흐름을 요약한 문장"
        }}
        """
    

    @abstractmethod
    def _get_dbms_specific_code_prompt(self) -> str:
        """
        DBMS별 특화된 코드 분석 프롬프트 템플릿 (추상 메서드)
        
        각 DBMS 전략 클래스에서 구현해야 함
        
        Returns:
            str: DBMS 특화 코드 분석용 프롬프트 템플릿
        """
        pass


# =============================================================================
# Oracle DB 전략 클래스
# =============================================================================

class OracleUnderstandingStrategy(UnderstandingStrategy):
    """
    Oracle DB 이해를 위한 전략 구현 클래스
    
    PL/SQL 코드를 분석하여 테이블, 변수, 프로시저 호출 관계 등을 파악합니다.
    """
    

    def __init__(self):
        """
        Oracle 전략 초기화
        
        DBMS 유형을 Oracle PL/SQL로 설정
        """
        super().__init__()
        self.dbms_type = "Oracle PL/SQL"
    

    def _get_dbms_specific_code_prompt(self) -> str:
        """
        Oracle 특화 코드 분석 프롬프트
        
        PL/SQL 코드 분석을 위한 상세 가이드라인과 출력 형식을 정의합니다.
        
        Returns:
            str: Oracle PL/SQL 코드 분석용 프롬프트 템플릿
        """
        return """
        당신은 Oracle PL/SQL 전문가입니다. 주어진 함수/프로시저 코드를 철저히 분석하세요.


        분석할 패키지(스키마마) 이름:
        {schema_name}


        분석할 Stored Procedure Code:
        {code}


        분석할 Stored Procedure Code의 범위 목록:
        {ranges}


        반드시 지켜야할 주의사항:
        1. 분석할 Stored Procedure Code의 범위 개수는 {count}개로, 반드시 'analysis'는  {count}개의 요소를 가져야합니다.
        2. 테이블의 별칭과 스키마 이름을 제외하고, 오로직 테이블 이름만을 사용하세요.
        3. 테이블의 컬럼이 'variable'에 포함되지 않도록, 테이블의 컬럼과 변수에 대한 구분을 확실히 하여 결과를 생성하세요.
        4. 테이블에 대한 정보가 식별되지 않을 경우, 'Tables'는 빈 사전 {{}}으로 반환하고, 테이블의 컬럼 타입이 식별되지 않을 경우, 적절한 타입을 넣으세요.


        지정된 범위의 Stored Procedure Code 에서 다음 정보를 추출하세요:
        1. 코드의 역할과 동작을 상세하게 설명하세요:
        - 주어진 코드 범위의 전체 맥락을 파악하여 다음 내용을 포함하여 설명하세요:
        - 해당 코드가 속한 프로시저 이름을 반드시 명시
        - 각 변수 할당의 목적과 의미를 설명 (예: "vcount에 10을 할당하여 최대 반복 횟수를 설정")
        - 조건문(IF, CASE 등)의 판단 기준과 각 분기의 목적을 설명
        - 반복문(FOR, WHILE 등)의 반복 조건과 수행 목적을 설명
        - SQL 작업(INSERT/UPDATE/DELETE/SELECT)의 대상 테이블과 처리 목적을 설명
        - 해당 코드 범위가 전체 프로시저에서 수행하는 역할과 목적을 설명
        예시) "v_process_date에 현재 날짜를 할당하여 처리 기준일을 설정하고, v_count가 임계값(10)을 초과하는지 확인하여 처리량을 제한합니다. CUSTOMER 테이블에서 활성 고객만을 SELECT하여, ORDER_HISTORY 테이블에 집계 데이터를 생성합니다."

        2. 각 범위에서 사용된 모든 변수들을 식별하세요. 변수는 다음과 같은 유형을 모두 포함합니다:
        - 일반 변수 (보통 'v_', 'p_', 'i_', 'o_' 접두사)
        - %ROWTYPE 변수
        - %TYPE 변수
        
        주의사항:
        - 각 범위는 독립적으로 처리되어야 하며, 다른 범위와 중첩되더라도 해당 범위 내에서 직접 사용된 변수만 포함합니다.
        - 예를 들어, 223~250 라인과 240~241 라인이 중첩된 경우, 각각의 범위에서 실제로 사용된 변수만 독립적으로 식별합니다.
        - 상수나 열거형 값은 변수로 식별하지 않습니다.

        3. 코드 내에서 프로시저, 패키지, 함수 호출을 식별하세요:
        - 외부 패키지의 호출: 'PACKAGE_NAME.PROCEDURE_NAME' 형식으로 저장
        - 현재 패키지 내부 호출: 'PROCEDURE_NAME' 형식으로 저장
        - 시퀀스 객체의 NEXTVAL, CURRVAL 참조는 프로시저/함수 호출로 식별하지 마세요
        - 모든 호출을 'calls' 배열에 저장하세요.

        4. 코드 내에서 사용된 테이블 식별하세요:
        - 'INSERT INTO', 'MERGE INTO', 'FROM', 'UPDATE' 절 이후에 나오는 테이블의 전체 이름을 'tableNames'로 반환하세요.
        - TPJ_ 같은 접두어를 유지한 채 테이블의 풀 네임을 반환하세요.


        전체 Stored Procedure Code 에서 다음 정보를 추출하세요:
        1. SQL CRUD 문에서 'INSERT INTO', 'MERGE INTO', 'FROM', 'UPDATE' 절 이후에 나오는 테이블 이름을 찾아 순서대로 식별합니다.
        2. SQL CRUD 문에서 사용된 모든 테이블의 모든 컬럼들과 컬럼의 타입을 식별하세요.
        3. SQL CRUD 문을 분석하여 여러 테이블 JOIN 관계를 'source'와 'target' 형태로 표현합니다.


        아래는 예시 결과로, 식별된 정보만 담아서 json 형식으로 나타내고, 주석이나 부가 설명은 피해주세요:
        {{
            "analysis": [
                {{
                    "startLine": startLine,
                    "endLine": endLine,
                    "summary": "summary of the code",
                    "tableNames": ["tableName1", "tableName2"],
                    "calls": ["procedure1", "function1", "package1"], 
                    "variables": ["variable1", "variable2"]
                }}
            ],
            "Tables": {{
                "tableName1": ["type:field1", "type:field2"], 
                "tableName2": []
            }},
            "tableReference": [{{"source": "tableName1", "target": "tableName2"}}]
        }}
        """


# =============================================================================
# MySQL DB 전략 클래스
# =============================================================================

class MySQLUnderstandingStrategy(UnderstandingStrategy):
    """
    MySQL DB 이해를 위한 전략 구현 클래스
    
    MySQL 저장 프로시저를 분석하여 테이블, 변수, 함수 호출 관계 등을 파악합니다.
    """
    

    def __init__(self):
        """
        MySQL 전략 초기화
        
        DBMS 유형을 MySQL로 설정
        """
        super().__init__()
        self.dbms_type = "MySQL"
    

    def _get_dbms_specific_code_prompt(self) -> str:
        """
        MySQL 특화 코드 분석 프롬프트
        
        MySQL 저장 프로시저 코드 분석을 위한 상세 가이드라인과 출력 형식을 정의합니다.
        
        Returns:
            str: MySQL 코드 분석용 프롬프트 템플릿
        """
        return """
        당신은 MySQL 스토어드 프로시저의 코드 블록을 분석하는 전문가입니다.
        주어진 코드 블록들을 분석하여 각 블록의 역할과 기능을 정확히 파악해주세요.
        
        {context_block_info}
        
        [분석 규칙]
        ===============================================
        1. 각 코드 블록의 주요 기능 파악
           - SQL 문의 종류(SELECT, INSERT, UPDATE, DELETE 등)
           - MySQL 특화 구문(DECLARE HANDLER, SIGNAL 등)
           - 에러 처리 로직(DECLARE CONTINUE/EXIT HANDLER)
           - 스토어드 프로시저 호출 관계
        
        2. 테이블 참조 관계 식별
           - 각 코드 블록에서 참조하는 테이블 이름
           - 테이블과의 관계(조회, 삽입, 수정, 삭제)
        
        [JSON 출력 형식]
        ===============================================
        주석이나 부가설명 없이 다음 JSON 형식으로만 결과를 반환하세요:
        {
            "tableReference": [
                {{
                    "tableName": "테이블명",
                    "relationship": "READ/WRITE/UPDATE/DELETE"
                }}
            ],
            "analysis": [
                {{
                    "blockId": 0,
                    "startLine": 시작라인번호,
                    "endLine": 끝라인번호,
                    "summary": "이 코드 블록의 기능 요약",
                    "mainFunction": "주요 기능 설명 (SQL 실행, 조건 처리, 에러 처리 등)",
                    "tableRelationship": [
                        {{
                            "tableName": "테이블명",
                            "relationship": "READ/WRITE/UPDATE/DELETE"
                        }}
                    ]
                }}
            ]
        }}
        """


# =============================================================================
# PostgreSQL DB 전략 클래스
# =============================================================================

class PostgreSQLUnderstandingStrategy(UnderstandingStrategy):
    """
    PostgreSQL DB 이해를 위한 전략 구현 클래스
    
    PL/pgSQL 코드를 분석하여 테이블, 변수, 함수 호출 관계 등을 파악합니다.
    """
    

    def __init__(self):
        """
        PostgreSQL 전략 초기화
        
        DBMS 유형을 PostgreSQL로 설정
        """
        super().__init__()
        self.dbms_type = "PostgreSQL"
    

    def get_prompt_templates(self) -> Dict[str, str]:
        """
        PostgreSQL DB 특화 프롬프트 템플릿 반환
        
        PostgreSQL에 최적화된 템플릿으로 기본 템플릿을 확장합니다.
        
        Returns:
            Dict[str, str]: PostgreSQL 특화 프롬프트 템플릿 사전
        """
        templates = super().get_prompt_templates()
        
        # PostgreSQL 특화 DDL 프롬프트로 재정의
        templates["ddl"] = self._get_common_ddl_prompt().replace(
            "당신은 DDL을 분석하여 테이블 구조를 파악하는 전문가입니다.",
            "당신은 PostgreSQL DDL을 분석하여 테이블 구조를 파악하는 전문가입니다."
        ).replace(
            "데이터 타입",
            "데이터 타입 (PostgreSQL 특화 타입: integer, text, timestamp, jsonb 등)"
        )
        
        return templates
    

    def _get_dbms_specific_code_prompt(self) -> str:
        """
        PostgreSQL 특화 코드 분석 프롬프트
        
        PL/pgSQL 코드 분석을 위한 상세 가이드라인과 출력 형식을 정의합니다.
        
        Returns:
            str: PostgreSQL 코드 분석용 프롬프트 템플릿
        """
        return """
        당신은 PostgreSQL(PL/pgSQL) 전문가입니다. 주어진 함수/프로시저 코드를 철저히 분석하세요.


        분석할 패키지(스키마마) 이름:
        {schema_name}


        분석할 Stored Procedure Code:
        {code}


        분석할 Stored Procedure Code의 범위 목록:
        {ranges}


        반드시 지켜야할 주의사항:
        1. 분석할 Stored Procedure Code의 범위 개수는 {count}개로, 반드시 'analysis'는  {count}개의 요소를 가져야합니다.
        2. 테이블의 별칭과 스키마 이름을 제외하고, 오로직 테이블 이름만을 사용하세요.
        3. 테이블의 컬럼이 'variable'에 포함되지 않도록, 테이블의 컬럼과 변수에 대한 구분을 확실히 하여 결과를 생성하세요.
        4. 테이블에 대한 정보가 식별되지 않을 경우, 'Tables'는 빈 사전 {{}}으로 반환하고, 테이블의 컬럼 타입이 식별되지 않을 경우, 적절한 타입을 넣으세요.


        지정된 범위의 Stored Procedure Code 에서 다음 정보를 추출하세요:
        1. 코드의 역할과 동작을 상세하게 설명하세요:
           - 주어진 코드 범위의 전체 맥락을 파악하여 다음 내용을 포함하여 설명하세요:
           - 해당 코드가 속한 프로시저 이름을 반드시 명시
           - 각 변수 할당의 목적과 의미를 설명 (예: "vcount에 10을 할당하여 최대 반복 횟수를 설정")
           - 조건문(IF, CASE 등)의 판단 기준과 각 분기의 목적을 설명
           - 반복문(FOR, WHILE 등)의 반복 조건과 수행 목적을 설명
           - SQL 작업(INSERT/UPDATE/DELETE/SELECT)의 대상 테이블과 처리 목적을 설명
           - 해당 코드 범위가 전체 프로시저에서 수행하는 역할과 목적을 설명
           예시) "v_process_date에 현재 날짜를 할당하여 처리 기준일을 설정하고, v_count가 임계값(10)을 초과하는지 확인하여 처리량을 제한합니다. CUSTOMER 테이블에서 활성 고객만을 SELECT하여, ORDER_HISTORY 테이블에 집계 데이터를 생성합니다."

        2. 각 범위에서 사용된 모든 변수들을 식별하세요. 변수는 다음과 같은 유형을 모두 포함합니다:
           - 일반 변수 (보통 'v_', 'p_', 'i_', 'o_' 접두사)
           - %ROWTYPE 변수
           - %TYPE 변수
           
           주의사항:
           - 각 범위는 독립적으로 처리되어야 하며, 다른 범위와 중첩되더라도 해당 범위 내에서 직접 사용된 변수만 포함합니다.
           - 예를 들어, 223~250 라인과 240~241 라인이 중첩된 경우, 각각의 범위에서 실제로 사용된 변수만 독립적으로 식별합니다.
           - 상수나 열거형 값은 변수로 식별하지 않습니다.

        3. 코드 내에서 프로시저, 패키지, 함수 호출을 식별하세요:
           - 외부 패키지의 호출: 'PACKAGE_NAME.PROCEDURE_NAME' 형식으로 저장
           - 현재 패키지 내부 호출: 'PROCEDURE_NAME' 형식으로 저장
           - 시퀀스 객체의 NEXTVAL, CURRVAL 참조는 프로시저/함수 호출로 식별하지 마세요
           - 모든 호출을 'calls' 배열에 저장하세요.

        4. 코드 내에서 사용된 테이블 식별하세요:
          - 'INSERT INTO', 'MERGE INTO', 'FROM', 'UPDATE' 절 이후에 나오는 테이블의 전체 이름을 'tableNames'로 반환하세요.
          - TPJ_ 같은 접두어를 유지한 채 테이블의 풀 네임을 반환하세요.


        전체 Stored Procedure Code 에서 다음 정보를 추출하세요:
        1. SQL CRUD 문에서 'INSERT INTO', 'MERGE INTO', 'FROM', 'UPDATE' 절 이후에 나오는 테이블 이름을 찾아 순서대로 식별합니다.
        2. SQL CRUD 문에서 사용된 모든 테이블의 모든 컬럼들과 컬럼의 타입을 식별하세요.
        3. SQL CRUD 문을 분석하여 여러 테이블 JOIN 관계를 'source'와 'target' 형태로 표현합니다.


        아래는 예시 결과로, 식별된 정보만 담아서 json 형식으로 나타내고, 주석이나 부가 설명은 피해주세요:
        {{
            "analysis": [
                {{
                    "startLine": startLine,
                    "endLine": endLine,
                    "summary": "summary of the code",
                    "tableNames": ["tableName1", "tableName2"],
                    "calls": ["procedure1", "function1", "package1"], 
                    "variables": ["variable1", "variable2"]
                }}
            ],
            "Tables": {{
                "tableName1": ["type:field1", "type:field2"], 
                "tableName2": []
            }},
            "tableReference": [{{"source": "tableName1", "target": "tableName2"}}]
        }}
        """


# =============================================================================
# 팩토리 함수
# =============================================================================

def create_understanding_strategy(dbms: str) -> UnderstandingStrategy:
    """
    DBMS 타입에 맞는 Understanding 전략 객체 생성
    
    전략 패턴을 구현한 팩토리 함수로, DBMS 유형에 따라 적절한 전략 객체를 반환합니다.
    
    Args:
        dbms: 데이터베이스 관리 시스템 타입 (oracle, mysql, postgresql)
        
    Returns:
        UnderstandingStrategy: DBMS에 맞는 전략 객체
    """
    # 기본값은 Oracle로 설정
    dbms = dbms.lower() if dbms else 'oracle'
    
    if dbms == 'mysql':
        return MySQLUnderstandingStrategy()
    elif dbms == 'postgresql':
        return PostgreSQLUnderstandingStrategy()
    else:  # 기본값은 Oracle
        return OracleUnderstandingStrategy()