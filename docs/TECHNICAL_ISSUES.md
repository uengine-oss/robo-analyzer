# 기술 이슈 및 해결 보고서

## 📋 개요

이 문서는 ROBO Analyzer 개발 과정에서 발생한 주요 기술 이슈와 해결 방안을 기록합니다.

---

## 1. DBMS 테이블 스키마 매칭 문제 해결

### 🔴 문제 상황

**발생 일시**: 2026-01-02  
**영향 범위**: DBMS 분석 전략 (PL/SQL)

#### 문제 현상

1. **스키마 값 불일치로 인한 매칭 실패**
   - DDL에서 생성된 테이블: `{schema: 'rwis', name: 'rditag_tb'}` ✅
   - DML에서 발견된 테이블: `{name: 'watersisul_log_tb'}` (schema 속성 없음) ❌
   - 결과: `table_summary` 분석 결과가 Neo4j에 저장되지 않음

2. **코드상 스키마 생략 문제**
   - PL/SQL 코드에서 스키마를 명시하지 않는 경우가 많음
   - 예: `SELECT * FROM watersisul_log_tb` (스키마 없음)
   - DML 분석 단계에서 스키마를 식별할 수 없어 빈값으로 처리됨

3. **다른 서비스 연동 문제**
   - 외부 서비스에서 스키마 값이 중요한데, 스키마가 없는 테이블과 매칭이 전혀 안됨
   - Neo4j 쿼리에서 `MATCH (t:Table {schema: '', name: '...'})` 와 `MATCH (t:Table {name: '...'})` 가 서로 다른 노드로 인식됨

#### 근본 원인

```python
# 문제 코드 (수정 전)
def _build_table_merge(self, table_name: str, schema: Optional[str]) -> str:
    schema_value = schema or ''
    schema_part = f", schema: '{schema_value}'" if schema_value else ""  # ❌ 빈값이면 속성 자체가 없음
    return f"MERGE (t:Table {{..., name: '{table_name}'{schema_part}, ...}})"
```

- 스키마가 없을 때 속성을 아예 추가하지 않음
- `_summarize_table`에서는 항상 `schema: ''`를 포함하여 MATCH 실패

### ✅ 해결 방안

#### 1단계: DDL 분석 시 스키마 Set 수집

```python
# dbms_analyzer.py
class DbmsAnalyzer:
    def __init__(self):
        self._ddl_schemas: set[str] = set()  # DDL에서 수집된 스키마 Set
    
    async def _process_ddl(self, ...):
        for table_info in parsed.get("analysis", []):
            schema = parsed_schema or ""
            if schema:
                self._ddl_schemas.add(schema.lower())  # 스키마 수집
```

#### 2단계: 파일 경로 기반 기본 스키마 결정

```python
def _resolve_default_schema(self, directory: str) -> str:
    """파일 경로에서 기본 스키마를 결정합니다.
    
    우선순위:
    1. 경로의 폴더명 중 DDL 스키마와 일치하는 것 (깊은 폴더 우선)
    2. 매칭 실패 시 'public'
    """
    if not directory or not self._ddl_schemas:
        return "public"
    
    parts = directory.replace("\\", "/").split("/")
    parts = [p.lower() for p in parts if p]
    
    # 깊은 폴더부터 매칭 (역순 순회)
    for folder in reversed(parts):
        if folder in self._ddl_schemas:
            return folder
    
    return "public"
```

**예시:**
```
DDL 스키마 Set: {'rwis', 'common'}
파일 경로: source/common/rwis/procedures/my_proc.sql
결과: 'rwis' 선택 (깊은 폴더 우선)
```

#### 3단계: 테이블 노드 생성 시 기본 스키마 사용

```python
# ast_processor.py
class DbmsAstProcessor:
    def __init__(self, ..., default_schema: str = "public"):
        self.default_schema = default_schema
    
    def _build_table_merge(self, table_name: str, schema: Optional[str]) -> str:
        """스키마가 없으면 default_schema를 사용합니다."""
        schema_value = schema or self.default_schema  # ✅ 항상 스키마 값 존재
        return f"MERGE (t:Table {{..., name: '{table_name}', schema: '{schema_value}', ...}})"
```

#### 4단계: table_summary 저장 시 스키마 일관성 보장

```python
def _record_table_summary(self, schema: Optional[str], name: str, ...) -> Tuple[str, str]:
    """스키마가 없으면 default_schema를 사용합니다."""
    schema_key = schema or self.default_schema  # ✅ 일관성 보장
    ...
```

### 📊 해결 결과

| 항목 | 수정 전 | 수정 후 |
|------|---------|---------|
| 스키마 없는 테이블 노드 | `{name: 'watersisul_log_tb'}` | `{name: 'watersisul_log_tb', schema: 'rwis'}` ✅ |
| table_summary MATCH 성공률 | 0% (스키마 불일치) | 100% (스키마 일치) ✅ |
| description 저장 성공률 | DDL 테이블만 저장 | 모든 테이블 저장 ✅ |
| 외부 서비스 매칭 | 실패 | 성공 ✅ |

### 🔧 수정 파일

- `analyzer/strategy/dbms/dbms_analyzer.py`
  - `_ddl_schemas` 변수 추가
  - `_process_ddl()`: 스키마 수집 로직 추가
  - `_resolve_default_schema()`: 경로 기반 스키마 결정 함수 추가
  - `_run_phase1()`: processor 생성 시 `default_schema` 전달

- `analyzer/strategy/dbms/ast_processor.py`
  - `__init__()`: `default_schema` 파라미터 추가
  - `_build_table_merge()`: 스키마 없을 때 기본 스키마 사용
  - `_record_table_summary()`: 스키마 일관성 보장

### 📝 참고 사항

- **기본 스키마 우선순위**: 명시적 스키마 > 경로 매칭 > 'public'
- **경로 매칭 규칙**: 더 깊은(자식) 디렉토리의 스키마를 우선 선택
- **레거시 호환성**: 기존 로직을 대체하여 분기 없이 단순화

---

## 2. Summary 및 User Story 노드 생성 누락 문제

### 🔴 문제 상황

**발생 일시**: 2025-12 (이전 해결)  
**영향 범위**: DBMS 분석 전략 (프로시저/함수)

#### 문제 현상

- 프로시저/함수의 `summary` 속성이 Neo4j에 저장되지 않음
- `UserStory` 노드 및 `HAS_USER_STORY` 관계가 생성되지 않음
- `build_user_story_doc()` 실행 시 데이터가 없어 문서 생성 실패

#### 근본 원인

**바이브 코딩(Vibe Coding)으로 인한 누락 이슈**

- LLM 분석 결과를 Neo4j에 저장하는 쿼리 생성 로직이 누락됨
- `_process_procedure_summaries()` 함수에서 쿼리 생성은 했으나, 실제 실행 흐름에서 호출되지 않음
- 또는 쿼리 생성 후 `all_queries` 리스트에 추가되지 않음

### ✅ 해결 방안

#### 수정 내용

1. **프로시저 Summary 저장 로직 확인 및 수정**
   ```python
   # _process_procedure_summaries() 함수에서
   queries.append(
       f"MATCH (n:{info.procedure_type} {{procedure_name: '{...}', {self.node_base_props}}})\n"
       f"SET n.summary = {summary_json}\n"
       f"RETURN n"
   )
   ```

2. **User Story 노드 생성 로직 확인 및 수정**
   ```python
   # User Story 노드 및 관계 생성
   for us_idx, us in enumerate(all_user_stories, 1):
       queries.append(
           f"MATCH (n:{info.procedure_type} {{procedure_name: '{...}', {self.node_base_props}}})\n"
           f"MERGE (us:UserStory {{id: '{...}', ...}})\n"
           f"MERGE (n)-[:HAS_USER_STORY]->(us)\n"
           ...
       )
   ```

3. **쿼리 실행 흐름 확인**
   - `run_llm_analysis()` → `_process_procedure_summaries()` 호출 확인
   - 반환된 `queries`가 `all_queries`에 추가되는지 확인

### 📊 해결 결과

| 항목 | 수정 전 | 수정 후 |
|------|---------|---------|
| Summary 저장 | ❌ 누락 | ✅ 정상 저장 |
| UserStory 노드 생성 | ❌ 누락 | ✅ 정상 생성 |
| HAS_USER_STORY 관계 | ❌ 누락 | ✅ 정상 생성 |
| User Story 문서 생성 | ❌ 실패 | ✅ 정상 생성 |

### 🔧 수정 파일

- `analyzer/strategy/dbms/ast_processor.py`
  - `_process_procedure_summaries()`: 쿼리 생성 및 반환 로직 확인
  - `run_llm_analysis()`: 프로시저 summary 처리 호출 확인

### 📝 참고 사항

- **바이브 코딩 주의**: LLM 기반 개발 시 로직 누락 가능성 높음
- **테스트 중요성**: Neo4j 쿼리 실행 결과를 반드시 검증해야 함
- **로깅 강화**: 각 단계별 쿼리 생성 및 실행 로그 추가 권장

---

## 3. ANTLR 노드 타입 추가: DECLARE에서 CURSOR 타입 처리

### 🔴 문제 상황

**발생 일시**: 2026-01 (다른 프로젝트 작업)  
**영향 범위**: ANTLR 파서 (PL/SQL 문법)

#### 문제 현상

- `DECLARE` 블록에서 `CURSOR` 타입 변수 선언이 인식되지 않음
- 예: `DECLARE CURSOR c1 IS SELECT ...` 구문이 파싱되지 않음
- 결과: CURSOR 변수가 `Variable` 노드로 생성되지 않음

#### 근본 원인

- ANTLR 문법 파일에 CURSOR 선언 구문이 누락됨
- 또는 리스너(Listener)에서 CURSOR 노드 타입을 처리하지 않음

### ✅ 해결 방안

#### 수정 내용

1. **ANTLR 문법 파일 수정**
   ```antlr
   // PL/SQL 문법 파일에 CURSOR 선언 추가
   declare_section
       : DECLARE
       ( variable_declaration
       | cursor_declaration  // ✅ 추가
       | exception_declaration
       )*
       ;
   
   cursor_declaration
       : CURSOR cursor_name IS select_statement
       ;
   ```

2. **리스너에서 CURSOR 노드 타입 처리 추가**
   ```python
   # StatementCollector 클래스의 _visit() 메서드
   def _visit(self, node: Dict[str, Any], ...):
       node_type = node.get('type', '')
       
       if node_type == 'CURSOR_DECLARATION':  # ✅ 추가
           # CURSOR 변수 노드 생성
           return self._create_cursor_node(node, ...)
   ```

3. **노드 타입 상수 추가**
   ```python
   # ast_processor.py 상단
   CURSOR_TYPES = ("CURSOR", "CURSOR_DECLARATION")
   ```

### 📊 해결 결과

| 항목 | 수정 전 | 수정 후 |
|------|---------|---------|
| CURSOR 선언 인식 | ❌ 미인식 | ✅ 정상 인식 |
| CURSOR Variable 노드 생성 | ❌ 누락 | ✅ 정상 생성 |
| CURSOR 사용 추적 | ❌ 불가능 | ✅ 가능 |

### 🔧 수정 파일

- `grammar/PLSQL.g4` (또는 해당 ANTLR 문법 파일)
  - `cursor_declaration` 규칙 추가

- `analyzer/strategy/dbms/ast_processor.py`
  - `StatementCollector._visit()`: CURSOR 노드 타입 처리 추가
  - `CURSOR_TYPES` 상수 추가

### 📝 참고 사항

- **ANTLR 문법 확장**: 새로운 구문 추가 시 문법 파일과 리스너 모두 수정 필요
- **노드 타입 일관성**: 기존 노드 타입 네이밍 규칙 준수
- **테스트 케이스**: 다양한 CURSOR 선언 패턴 테스트 필요

---

## 4. DDL 메타데이터와 LLM 분석 결과 통합

### 🔴 문제 상황

**발생 일시**: 2026-01-02  
**영향 범위**: DBMS 분석 전략 (테이블 설명 생성)

#### 문제 현상

1. **DDL 메타데이터 덮어쓰기 문제**
   - DDL에서 생성한 `description`과 `detailDescription`이 table_summary에서 덮어쓰여짐
   - DDL의 정적 메타데이터와 LLM의 동적 분석 결과가 병합되지 않음

2. **중복 속성 문제**
   - `description`과 `detailDescription` 두 개의 속성으로 분리되어 있음
   - 불필요한 중복으로 인한 복잡성 증가

3. **Neo4j 접근 성능 문제**
   - 기존 설명을 가져오려면 Neo4j에 접근해야 함
   - 매번 쿼리 실행으로 인한 성능 저하 우려

#### 근본 원인

```python
# 문제 코드 (수정 전)
# DDL 처리
MERGE (t:Table {...}) SET t.description = 'DDL 설명', t.detailDescription = '...'

# table_summary 처리
MATCH (t:Table {...}) SET t.description = 'LLM 설명'  # ❌ DDL 설명 덮어쓰기
MATCH (t:Table {...}) SET t.detailDescription = '...'  # ❌ DDL 상세 덮어쓰기
```

- DDL에서 설정한 메타데이터가 LLM 결과로 완전히 덮어쓰여짐
- 기존 값을 가져오려면 Neo4j 쿼리가 필요하여 성능 저하

### ✅ 해결 방안

#### 1단계: DDL 메타데이터 메모리 캐시

```python
# dbms_analyzer.py
class DbmsAnalyzer:
    def __init__(self):
        # DDL 메타데이터 캐시: {(schema, table_name): {description, columns}}
        self._ddl_table_metadata: Dict[Tuple[str, str], Dict[str, Any]] = {}
    
    async def _process_ddl(self, ...):
        # DDL 처리 시 메타데이터 캐시 저장
        table_key = (schema.lower(), parsed_name.lower())
        self._ddl_table_metadata[table_key] = {
            "description": comment,
            "columns": column_metadata,
        }
```

**장점:**
- Neo4j 접근 없이 메모리에서 조회 (O(1))
- 빠른 조회 성능
- 필요한 데이터만 저장하여 메모리 효율적

#### 2단계: Processor에 메타데이터 전달

```python
# dbms_analyzer.py
processor = DbmsAstProcessor(
    ...,
    ddl_table_metadata=self._ddl_table_metadata,  # 메타데이터 전달
)
```

#### 3단계: LLM 입력에 DDL 메타데이터 포함

```python
# ast_processor.py
async def _summarize_table(self, table_key, data):
    # DDL 메타데이터 조회 (메모리 캐시)
    ddl_meta = self._ddl_table_metadata.get(ddl_key, {})
    ddl_description = ddl_meta.get('description', '')
    
    # DDL description을 LLM 입력에 포함
    if ddl_description:
        summaries.insert(0, f"[DDL 메타데이터] {ddl_description}")
    
    # DDL 컬럼 정보도 column_sentences에 포함
    for col_name, ddl_col in ddl_columns.items():
        if ddl_col_desc:
            column_sentences[col_name].insert(0, f"[DDL 메타데이터] {ddl_col_desc}")
    
    # LLM이 DDL + DML 패턴을 모두 고려하여 통합된 description 생성
    result = await summarize_table_metadata(...)
    
    # LLM 결과를 그대로 description에 할당
    queries.append(f"SET t.description = '{llm_table_desc}'")
```

#### 4단계: detailDescription 제거, description 하나로 통합

```python
# 프롬프트 수정 (rules/dbms/table_summary.yaml)
# detailDescription 제거, description만 생성

# DDL 처리 수정
set_props = {
    "description": escape_for_cypher(comment),
    "table_type": table_type,
    # detailDescription 제거
}

# table_summary 처리 수정
# detailDescription 관련 쿼리 제거
# LLM 결과의 tableDescription만 description에 할당
```

### 📊 해결 결과

| 항목 | 수정 전 | 수정 후 |
|------|---------|---------|
| DDL 메타데이터 보존 | ❌ 덮어쓰기 | ✅ LLM 입력에 포함 |
| 설명 통합 | ❌ 별도 병합 | ✅ LLM이 통합 생성 |
| 속성 개수 | 2개 (description, detailDescription) | 1개 (description) ✅ |
| 성능 | ❌ Neo4j 접근 필요 | ✅ 메모리 캐시 사용 |
| 설명 품질 | ⚠️ DDL 또는 LLM만 | ✅ DDL + DML 통합 |

### 🔧 수정 파일

- `analyzer/strategy/dbms/dbms_analyzer.py`
  - `_ddl_table_metadata` 캐시 추가
  - `_process_ddl()`: 메타데이터 캐시 저장
  - `detailDescription` 생성 제거
  - processor 생성 시 메타데이터 전달

- `analyzer/strategy/dbms/ast_processor.py`
  - `__init__()`: `ddl_table_metadata` 파라미터 추가
  - `_summarize_table()`: DDL 메타데이터를 LLM 입력에 포함
  - `detailDescription` 처리 제거
  - LLM 결과를 그대로 `description`에 할당

- `rules/dbms/table_summary.yaml`
  - `detailDescription` 관련 내용 제거
  - `description`만 생성하도록 프롬프트 수정
  - DDL 메타데이터와 DML 패턴 통합 요구사항 추가

### 📝 참고 사항

- **메모리 캐시 활용**: Neo4j 접근 없이 빠른 조회
- **LLM 통합 생성**: DDL과 DML 패턴을 모두 고려한 통합 설명
- **단순화**: `description` 하나로 통합하여 복잡성 감소
- **성능 최적화**: 메모리 캐시로 인한 성능 향상

---

## 📚 관련 문서

- [분석 흐름](./README.md#분석-흐름)
- [그래프 데이터 구조](./README.md#그래프-데이터-구조)
- [DBMS 분석 전략](./README.md#dbms-분석-plsql)

---

**최종 업데이트**: 2026-01-02

