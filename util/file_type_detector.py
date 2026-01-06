"""파일 타입 자동 감지 유틸리티

파일 내용을 분석하여 소스 코드 타입을 자동으로 식별합니다.

지원하는 타입:
- java: Java 소스 코드
- oracle_sp: Oracle Stored Procedure/Function/Package
- oracle_ddl: Oracle DDL (CREATE TABLE, etc.)
- postgresql_sp: PostgreSQL Stored Procedure/Function
- postgresql_ddl: PostgreSQL DDL
- python: Python 소스 코드
- xml: XML 설정 파일
- unknown: 식별 불가
"""

import re
from dataclasses import dataclass
from enum import Enum
from typing import Optional


class FileType(str, Enum):
    """파일 타입 열거형"""
    JAVA = "java"
    ORACLE_SP = "oracle_sp"
    ORACLE_DDL = "oracle_ddl"
    POSTGRESQL_SP = "postgresql_sp"
    POSTGRESQL_DDL = "postgresql_ddl"
    PYTHON = "python"
    XML = "xml"
    SQL_GENERIC = "sql_generic"
    UNKNOWN = "unknown"


@dataclass
class FileTypeResult:
    """파일 타입 감지 결과"""
    file_name: str
    file_type: FileType
    confidence: float  # 0.0 ~ 1.0
    details: str  # 감지 근거
    suggested_strategy: str  # framework | dbms
    suggested_target: str  # java | oracle | postgresql | python


# =============================================================================
# 패턴 정의
# =============================================================================

# Java 패턴
JAVA_PATTERNS = [
    (r'\bpublic\s+class\s+\w+', 0.9, "public class 선언"),
    (r'\bprivate\s+class\s+\w+', 0.9, "private class 선언"),
    (r'\bclass\s+\w+\s*(extends|implements)', 0.9, "class extends/implements"),
    (r'\bpublic\s+interface\s+\w+', 0.9, "public interface 선언"),
    (r'\bpackage\s+[\w.]+;', 0.8, "package 선언"),
    (r'\bimport\s+[\w.]+;', 0.7, "import 문"),
    (r'@(Override|Autowired|Service|Repository|Controller|Component|Entity)', 0.85, "Java 어노테이션"),
    (r'\bpublic\s+static\s+void\s+main', 0.95, "main 메소드"),
    (r'\bnew\s+\w+\s*\(', 0.6, "new 키워드"),
    (r'System\.out\.print', 0.7, "System.out 사용"),
]

# Oracle PL/SQL Stored Procedure 패턴
ORACLE_SP_PATTERNS = [
    (r'CREATE\s+(OR\s+REPLACE\s+)?PROCEDURE\s+\w+', 0.95, "CREATE PROCEDURE"),
    (r'CREATE\s+(OR\s+REPLACE\s+)?FUNCTION\s+\w+', 0.95, "CREATE FUNCTION"),
    (r'CREATE\s+(OR\s+REPLACE\s+)?PACKAGE\s+(BODY\s+)?\w+', 0.95, "CREATE PACKAGE"),
    (r'CREATE\s+(OR\s+REPLACE\s+)?TRIGGER\s+\w+', 0.9, "CREATE TRIGGER"),
    (r'CREATE\s+(OR\s+REPLACE\s+)?TYPE\s+\w+', 0.85, "CREATE TYPE"),
    (r'\bDECLARE\b', 0.6, "DECLARE 블록"),
    (r'\bBEGIN\b[\s\S]*\bEND\s*;', 0.7, "BEGIN...END 블록"),
    (r'\bEXCEPTION\s+WHEN\b', 0.8, "EXCEPTION 처리"),
    (r'\bDBMS_\w+\.\w+', 0.85, "DBMS 패키지 사용"),
    (r'\bUTL_\w+\.\w+', 0.8, "UTL 패키지 사용"),
    (r'\b(IN|OUT|IN\s+OUT)\s+\w+\s+(VARCHAR2|NUMBER|DATE|CLOB|BLOB)', 0.85, "PL/SQL 파라미터"),
    (r'%TYPE\b', 0.8, "%TYPE 참조"),
    (r'%ROWTYPE\b', 0.8, "%ROWTYPE 참조"),
    (r'\bFOR\s+\w+\s+IN\s+\(', 0.7, "FOR...IN 루프"),
    (r'\bCURSOR\s+\w+\s+IS\b', 0.85, "CURSOR 선언"),
    (r'\bFETCH\s+\w+\s+INTO\b', 0.8, "FETCH INTO"),
    (r'\bRAISE_APPLICATION_ERROR\b', 0.9, "RAISE_APPLICATION_ERROR"),
    (r'/\s*$', 0.5, "PL/SQL 종료 슬래시"),
]

# Oracle DDL 패턴
ORACLE_DDL_PATTERNS = [
    (r'CREATE\s+TABLE\s+\w+', 0.9, "CREATE TABLE"),
    (r'CREATE\s+(UNIQUE\s+)?INDEX\s+\w+', 0.85, "CREATE INDEX"),
    (r'ALTER\s+TABLE\s+\w+', 0.85, "ALTER TABLE"),
    (r'DROP\s+TABLE\s+\w+', 0.8, "DROP TABLE"),
    (r'CREATE\s+SEQUENCE\s+\w+', 0.85, "CREATE SEQUENCE"),
    (r'CREATE\s+(OR\s+REPLACE\s+)?VIEW\s+\w+', 0.85, "CREATE VIEW"),
    (r'CONSTRAINT\s+\w+\s+(PRIMARY\s+KEY|FOREIGN\s+KEY|UNIQUE|CHECK)', 0.85, "CONSTRAINT 정의"),
    (r'REFERENCES\s+\w+\s*\(', 0.8, "REFERENCES (FK)"),
    (r'\b(VARCHAR2|NUMBER|DATE|CLOB|BLOB|RAW)\s*\(', 0.7, "Oracle 데이터타입"),
    (r'TABLESPACE\s+\w+', 0.75, "TABLESPACE 지정"),
    (r'STORAGE\s*\(', 0.7, "STORAGE 절"),
]

# PostgreSQL Stored Procedure 패턴
POSTGRESQL_SP_PATTERNS = [
    (r'CREATE\s+(OR\s+REPLACE\s+)?FUNCTION\s+\w+', 0.8, "CREATE FUNCTION"),
    (r'CREATE\s+(OR\s+REPLACE\s+)?PROCEDURE\s+\w+', 0.85, "CREATE PROCEDURE"),
    (r'\$\$\s*(DECLARE|BEGIN)', 0.9, "PL/pgSQL $$ 블록"),
    (r'LANGUAGE\s+(plpgsql|sql)', 0.9, "LANGUAGE plpgsql/sql"),
    (r'RETURNS\s+(VOID|SETOF|TABLE|TRIGGER)', 0.85, "RETURNS 절"),
    (r'\bRAISE\s+(NOTICE|EXCEPTION|WARNING)', 0.9, "RAISE 문"),
    (r'PERFORM\s+\w+', 0.85, "PERFORM 문"),
    (r'RETURN\s+(NEXT|QUERY)', 0.85, "RETURN NEXT/QUERY"),
    (r':=\s*', 0.6, "PL/pgSQL 대입 연산자"),
    (r'\bNEW\.\w+', 0.8, "트리거 NEW 참조"),
    (r'\bOLD\.\w+', 0.8, "트리거 OLD 참조"),
    (r'TG_\w+', 0.85, "트리거 변수 (TG_*)"),
]

# PostgreSQL DDL 패턴
POSTGRESQL_DDL_PATTERNS = [
    (r'CREATE\s+TABLE\s+(IF\s+NOT\s+EXISTS\s+)?\w+', 0.85, "CREATE TABLE"),
    (r'CREATE\s+(UNIQUE\s+)?INDEX\s+(CONCURRENTLY\s+)?\w+', 0.8, "CREATE INDEX"),
    (r'ALTER\s+TABLE\s+\w+', 0.8, "ALTER TABLE"),
    (r'\b(SERIAL|BIGSERIAL|SMALLSERIAL)\b', 0.85, "PostgreSQL SERIAL 타입"),
    (r'\b(TEXT|JSONB?|UUID|INET|CIDR|MACADDR)\b', 0.75, "PostgreSQL 특수 타입"),
    (r'CREATE\s+EXTENSION', 0.9, "CREATE EXTENSION"),
    (r'CREATE\s+SCHEMA', 0.85, "CREATE SCHEMA"),
    (r'ON\s+DELETE\s+(CASCADE|SET\s+NULL|RESTRICT)', 0.7, "FK ON DELETE"),
    (r'ON\s+UPDATE\s+(CASCADE|SET\s+NULL|RESTRICT)', 0.7, "FK ON UPDATE"),
]

# Python 패턴
PYTHON_PATTERNS = [
    (r'^def\s+\w+\s*\(', 0.8, "def 함수 정의"),
    (r'^class\s+\w+\s*[:\(]', 0.85, "class 정의"),
    (r'^import\s+\w+', 0.7, "import 문"),
    (r'^from\s+\w+\s+import', 0.75, "from import 문"),
    (r'if\s+__name__\s*==\s*["\']__main__["\']', 0.9, "__main__ 체크"),
    (r'@\w+\s*(\(|$)', 0.7, "데코레이터"),
    (r'self\.\w+', 0.75, "self 참조"),
    (r'print\s*\(', 0.5, "print 함수"),
    (r'async\s+def\s+\w+', 0.85, "async def"),
    (r'await\s+\w+', 0.8, "await"),
]

# XML 패턴
XML_PATTERNS = [
    (r'^<\?xml\s+', 0.95, "XML 선언"),
    (r'<beans\s+', 0.9, "Spring beans"),
    (r'<configuration\s*>', 0.85, "configuration 태그"),
    (r'<project\s+', 0.85, "Maven project"),
    (r'<dependency\s*>', 0.8, "Maven dependency"),
    (r'<!DOCTYPE\s+', 0.85, "DOCTYPE 선언"),
    (r'xmlns:', 0.7, "XML 네임스페이스"),
]


# =============================================================================
# 감지 함수
# =============================================================================

def _calculate_score(content: str, patterns: list[tuple[str, float, str]], flags: int = re.IGNORECASE | re.MULTILINE) -> tuple[float, list[str]]:
    """패턴 매칭 점수 계산"""
    total_weight = 0.0
    matched_count = 0
    matched_details = []
    
    for pattern, weight, description in patterns:
        if re.search(pattern, content, flags):
            total_weight += weight
            matched_count += 1
            matched_details.append(description)
    
    if matched_count == 0:
        return 0.0, []
    
    # 평균 가중치 * 매칭 수 기반 점수 (최대 1.0)
    score = min(1.0, (total_weight / matched_count) * min(1.0, matched_count / 3))
    return score, matched_details


def _is_oracle_vs_postgresql(content: str) -> tuple[str, float]:
    """Oracle vs PostgreSQL 구분"""
    oracle_indicators = [
        r'\b(VARCHAR2|NUMBER|CLOB|BLOB|RAW)\b',
        r'\b(DBMS_|UTL_)\w+',
        r'%TYPE\b',
        r'%ROWTYPE\b',
        r'/\s*$',  # 종료 슬래시
        r'SYSDATE\b',
        r'NVL\s*\(',
        r'DECODE\s*\(',
        r'ROWNUM\b',
        r'DUAL\b',
    ]
    
    postgresql_indicators = [
        r'\$\$',
        r'LANGUAGE\s+plpgsql',
        r'\b(TEXT|JSONB?|UUID|INET|SERIAL)\b',
        r'RAISE\s+(NOTICE|EXCEPTION)',
        r'PERFORM\s+',
        r'RETURN\s+(NEXT|QUERY)',
        r'COALESCE\s*\(',
        r'NOW\(\)',
        r'CURRENT_TIMESTAMP',
    ]
    
    oracle_count = sum(1 for p in oracle_indicators if re.search(p, content, re.IGNORECASE))
    pg_count = sum(1 for p in postgresql_indicators if re.search(p, content, re.IGNORECASE))
    
    if oracle_count > pg_count:
        confidence = min(1.0, oracle_count / 5)
        return "oracle", confidence
    elif pg_count > oracle_count:
        confidence = min(1.0, pg_count / 5)
        return "postgresql", confidence
    else:
        return "unknown", 0.5


def _is_ddl_vs_sp(content: str) -> tuple[str, float]:
    """DDL vs Stored Procedure 구분
    
    DDL: CREATE TABLE, ALTER TABLE 등 (BEGIN...END 없음)
    SP: CREATE PROCEDURE/FUNCTION + BEGIN...END 블록
    """
    # SP 특징 (프로시저/함수 본문)
    sp_indicators = [
        r'\bBEGIN\b[\s\S]*\bEND\s*[;/]',  # BEGIN...END 블록
        r'\bDECLARE\b',  # 변수 선언
        r'\bEXCEPTION\s+WHEN\b',  # 예외 처리
        r'\b(IN|OUT|IN\s+OUT)\s+\w+\s+(VARCHAR2|NUMBER|DATE)',  # 파라미터
        r'\bCURSOR\s+\w+\s+IS\b',  # 커서
        r'\bFOR\s+\w+\s+IN\s+\(',  # FOR 루프
        r'\bIF\s+.+\s+THEN\b',  # IF 문
        r'\bLOOP\b',  # LOOP
        r'\bFETCH\b',  # FETCH
        r':=',  # 대입 연산자
    ]
    
    # DDL 특징 (테이블/인덱스 정의)
    ddl_indicators = [
        r'CREATE\s+TABLE\s+\w+',
        r'ALTER\s+TABLE\s+\w+',
        r'DROP\s+TABLE\s+\w+',
        r'CREATE\s+(UNIQUE\s+)?INDEX\s+\w+',
        r'CREATE\s+SEQUENCE\s+\w+',
        r'CONSTRAINT\s+\w+\s+(PRIMARY|FOREIGN|UNIQUE|CHECK)',
        r'REFERENCES\s+\w+\s*\(',
        r'PRIMARY\s+KEY\s*\(',
        r'FOREIGN\s+KEY\s*\(',
    ]
    
    sp_count = sum(1 for p in sp_indicators if re.search(p, content, re.IGNORECASE | re.MULTILINE))
    ddl_count = sum(1 for p in ddl_indicators if re.search(p, content, re.IGNORECASE | re.MULTILINE))
    
    # BEGIN...END가 없으면 DDL 가능성 높음
    has_begin_end = bool(re.search(r'\bBEGIN\b[\s\S]*\bEND\s*[;/]', content, re.IGNORECASE))
    
    if ddl_count > 0 and not has_begin_end:
        confidence = min(1.0, ddl_count / 3)
        return "ddl", confidence
    elif sp_count > ddl_count:
        confidence = min(1.0, sp_count / 4)
        return "sp", confidence
    elif ddl_count > sp_count:
        confidence = min(1.0, ddl_count / 3)
        return "ddl", confidence
    else:
        return "unknown", 0.3


def detect_file_type(file_name: str, content: str) -> FileTypeResult:
    """파일 타입 감지
    
    Args:
        file_name: 파일명 (확장자 참고용)
        content: 파일 내용
        
    Returns:
        FileTypeResult: 감지 결과
    """
    # 빈 파일 처리
    if not content or not content.strip():
        return FileTypeResult(
            file_name=file_name,
            file_type=FileType.UNKNOWN,
            confidence=0.0,
            details="빈 파일",
            suggested_strategy="framework",
            suggested_target="java",
        )
    
    # 확장자 기반 힌트
    ext = file_name.lower().rsplit('.', 1)[-1] if '.' in file_name else ''
    
    # 각 타입별 점수 계산
    scores = {}
    details_map = {}
    
    # Java
    java_score, java_details = _calculate_score(content, JAVA_PATTERNS)
    if ext == 'java':
        java_score = min(1.0, java_score + 0.3)
    scores['java'] = java_score
    details_map['java'] = java_details
    
    # Python
    python_score, python_details = _calculate_score(content, PYTHON_PATTERNS)
    if ext == 'py':
        python_score = min(1.0, python_score + 0.3)
    scores['python'] = python_score
    details_map['python'] = python_details
    
    # XML
    xml_score, xml_details = _calculate_score(content, XML_PATTERNS, re.MULTILINE)
    if ext == 'xml':
        xml_score = min(1.0, xml_score + 0.3)
    scores['xml'] = xml_score
    details_map['xml'] = xml_details
    
    # SQL 관련 파일 분석 (확장자와 관계없이 내용 기반으로도 분석)
    # Oracle SP
    oracle_sp_score, oracle_sp_details = _calculate_score(content, ORACLE_SP_PATTERNS)
    scores['oracle_sp'] = oracle_sp_score
    details_map['oracle_sp'] = oracle_sp_details
    
    # Oracle DDL
    oracle_ddl_score, oracle_ddl_details = _calculate_score(content, ORACLE_DDL_PATTERNS)
    scores['oracle_ddl'] = oracle_ddl_score
    details_map['oracle_ddl'] = oracle_ddl_details
    
    # PostgreSQL SP
    pg_sp_score, pg_sp_details = _calculate_score(content, POSTGRESQL_SP_PATTERNS)
    scores['postgresql_sp'] = pg_sp_score
    details_map['postgresql_sp'] = pg_sp_details
    
    # PostgreSQL DDL
    pg_ddl_score, pg_ddl_details = _calculate_score(content, POSTGRESQL_DDL_PATTERNS)
    scores['postgresql_ddl'] = pg_ddl_score
    details_map['postgresql_ddl'] = pg_ddl_details
    
    # SQL 관련 확장자인 경우 보너스 점수
    if ext in ('sql', 'pls', 'pck', 'pkb', 'pks', 'trg', 'fnc', 'prc', 'ddl'):
        scores['oracle_sp'] = min(1.0, scores.get('oracle_sp', 0) + 0.1)
        scores['oracle_ddl'] = min(1.0, scores.get('oracle_ddl', 0) + 0.1)
        scores['postgresql_sp'] = min(1.0, scores.get('postgresql_sp', 0) + 0.1)
        scores['postgresql_ddl'] = min(1.0, scores.get('postgresql_ddl', 0) + 0.1)
    
    # Oracle vs PostgreSQL 판별
    db_type, db_confidence = _is_oracle_vs_postgresql(content)
    
    # DB 타입에 따라 점수 조정
    if db_type == "oracle":
        scores['oracle_sp'] = min(1.0, scores.get('oracle_sp', 0) + db_confidence * 0.2)
        scores['oracle_ddl'] = min(1.0, scores.get('oracle_ddl', 0) + db_confidence * 0.2)
        scores['postgresql_sp'] = max(0, scores.get('postgresql_sp', 0) - db_confidence * 0.2)
        scores['postgresql_ddl'] = max(0, scores.get('postgresql_ddl', 0) - db_confidence * 0.2)
    elif db_type == "postgresql":
        scores['postgresql_sp'] = min(1.0, scores.get('postgresql_sp', 0) + db_confidence * 0.2)
        scores['postgresql_ddl'] = min(1.0, scores.get('postgresql_ddl', 0) + db_confidence * 0.2)
        scores['oracle_sp'] = max(0, scores.get('oracle_sp', 0) - db_confidence * 0.2)
        scores['oracle_ddl'] = max(0, scores.get('oracle_ddl', 0) - db_confidence * 0.2)
    
    # DDL vs SP 판별 (SQL 관련 패턴이 감지된 경우)
    sql_scores = [scores.get('oracle_sp', 0), scores.get('oracle_ddl', 0),
                  scores.get('postgresql_sp', 0), scores.get('postgresql_ddl', 0)]
    if max(sql_scores) > 0.2:
        sql_type, sql_confidence = _is_ddl_vs_sp(content)
        
        if sql_type == "ddl":
            # DDL이면 DDL 점수 부스트, SP 점수 감소
            scores['oracle_ddl'] = min(1.0, scores.get('oracle_ddl', 0) + sql_confidence * 0.3)
            scores['postgresql_ddl'] = min(1.0, scores.get('postgresql_ddl', 0) + sql_confidence * 0.3)
            scores['oracle_sp'] = max(0, scores.get('oracle_sp', 0) - sql_confidence * 0.3)
            scores['postgresql_sp'] = max(0, scores.get('postgresql_sp', 0) - sql_confidence * 0.3)
        elif sql_type == "sp":
            # SP이면 SP 점수 부스트, DDL 점수 감소
            scores['oracle_sp'] = min(1.0, scores.get('oracle_sp', 0) + sql_confidence * 0.3)
            scores['postgresql_sp'] = min(1.0, scores.get('postgresql_sp', 0) + sql_confidence * 0.3)
            scores['oracle_ddl'] = max(0, scores.get('oracle_ddl', 0) - sql_confidence * 0.3)
            scores['postgresql_ddl'] = max(0, scores.get('postgresql_ddl', 0) - sql_confidence * 0.3)
    
    # 최고 점수 타입 선택
    best_type = max(scores.keys(), key=lambda k: scores[k])
    best_score = scores[best_type]
    best_details = details_map.get(best_type, [])
    
    # 점수가 너무 낮으면 unknown
    if best_score < 0.3:
        # SQL 확장자인 경우 generic SQL로 처리
        if ext in ('sql', 'pls', 'pck', 'pkb', 'pks', 'trg', 'fnc', 'prc'):
            return FileTypeResult(
                file_name=file_name,
                file_type=FileType.SQL_GENERIC,
                confidence=0.5,
                details="SQL 파일 (DB 타입 불명확)",
                suggested_strategy="dbms",
                suggested_target="oracle",  # 기본값
            )
        
        return FileTypeResult(
            file_name=file_name,
            file_type=FileType.UNKNOWN,
            confidence=best_score,
            details=f"확실한 패턴 없음 (최고 점수: {best_type}={best_score:.2f})",
            suggested_strategy="framework",
            suggested_target="java",
        )
    
    # 타입별 전략/타겟 매핑
    type_mapping = {
        'java': (FileType.JAVA, "framework", "java"),
        'python': (FileType.PYTHON, "framework", "python"),
        'xml': (FileType.XML, "framework", "java"),  # XML은 보통 Java와 함께
        'oracle_sp': (FileType.ORACLE_SP, "dbms", "oracle"),
        'oracle_ddl': (FileType.ORACLE_DDL, "dbms", "oracle"),
        'postgresql_sp': (FileType.POSTGRESQL_SP, "dbms", "postgresql"),
        'postgresql_ddl': (FileType.POSTGRESQL_DDL, "dbms", "postgresql"),
    }
    
    file_type, strategy, target = type_mapping.get(best_type, (FileType.UNKNOWN, "framework", "java"))
    
    return FileTypeResult(
        file_name=file_name,
        file_type=file_type,
        confidence=best_score,
        details=", ".join(best_details[:5]) if best_details else f"{best_type} 패턴 매칭",
        suggested_strategy=strategy,
        suggested_target=target,
    )


def detect_batch_file_types(files: list[tuple[str, str]]) -> dict:
    """여러 파일의 타입을 일괄 감지하고 전체 프로젝트 타입 추론
    
    Args:
        files: [(file_name, content), ...] 리스트
        
    Returns:
        {
            "files": [FileTypeResult, ...],
            "summary": {
                "total": int,
                "by_type": {"java": 5, "oracle_sp": 3, ...},
                "suggested_strategy": str,
                "suggested_target": str,
            }
        }
    """
    results = []
    type_counts = {}
    strategy_votes = {"framework": 0, "dbms": 0}
    target_votes = {}
    
    for file_name, content in files:
        result = detect_file_type(file_name, content)
        results.append({
            "fileName": result.file_name,
            "fileType": result.file_type.value,
            "confidence": result.confidence,
            "details": result.details,
            "suggestedStrategy": result.suggested_strategy,
            "suggestedTarget": result.suggested_target,
        })
        
        # 통계 집계
        type_key = result.file_type.value
        type_counts[type_key] = type_counts.get(type_key, 0) + 1
        
        # 투표 (신뢰도 가중)
        strategy_votes[result.suggested_strategy] = (
            strategy_votes.get(result.suggested_strategy, 0) + result.confidence
        )
        target_votes[result.suggested_target] = (
            target_votes.get(result.suggested_target, 0) + result.confidence
        )
    
    # 최종 추천 결정
    suggested_strategy = max(strategy_votes.keys(), key=lambda k: strategy_votes[k])
    suggested_target = max(target_votes.keys(), key=lambda k: target_votes[k]) if target_votes else "java"
    
    return {
        "files": results,
        "summary": {
            "total": len(files),
            "byType": type_counts,
            "suggestedStrategy": suggested_strategy,
            "suggestedTarget": suggested_target,
        }
    }

