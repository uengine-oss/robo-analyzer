# ROBO Analyzer 개선 사항

## 🔧 스키마 대소문자 일관성 개선 (2024)

### 문제 상황

프론트엔드에서 지정한 `name_case` 옵션(original/uppercase/lowercase)이 DDL 분석과 SP(Stored Procedure) 분석 간에 일관되지 않게 적용되어, 다음과 같은 문제가 발생했습니다:

- **DDL 분석**: `name_case="uppercase"` → Table {schema: "RWIS"}
- **디렉토리 매칭**: 항상 소문자로 변환 → default_schema = "rwis"
- **FK 관계 생성**: `MATCH (t:Table {schema: 'rwis'})` → ❌ 매칭 실패
- **결과**: 외래키 관계가 생성되지 않음

### 해결 방법

#### 1. DDL 스키마 수집 개선

**변경 파일**: `analyzer/strategy/dbms/dbms_analyzer.py` (657~659줄)

```python
# 변경 전: 항상 소문자로 저장
if schema and schema.lower() != 'public':
    self._ddl_schemas.add(schema.lower())

# 변경 후: name_case 적용된 값 저장
if schema and schema.lower() != 'public':
    self._ddl_schemas.add(schema)
```

#### 2. 디렉토리 매칭 개선

**변경 파일**: `analyzer/strategy/dbms/dbms_analyzer.py` (840~871줄)

```python
def _resolve_default_schema(self, directory: str, name_case: str = 'original') -> str:
    """파일 경로에서 기본 스키마를 결정합니다.
    
    Args:
        directory: 파일이 위치한 디렉토리 경로
        name_case: 대소문자 변환 옵션 (original, uppercase, lowercase)
    """
    if not directory:
        return self._apply_name_case("public", name_case)
    
    parts = directory.replace("\\", "/").split("/")
    parts = [p for p in parts if p]
    
    if not parts:
        return self._apply_name_case("public", name_case)
    
    # DDL 스키마가 있으면 매칭 시도 (깊은 폴더부터)
    # 대소문자 무관 비교 후, DDL에 저장된 원본 대소문자 반환
    if self._ddl_schemas:
        ddl_schemas_lower_map = {s.lower(): s for s in self._ddl_schemas}
        for folder in reversed(parts):
            matched = ddl_schemas_lower_map.get(folder.lower())
            if matched:
                return matched  # DDL에서 name_case 적용된 값 그대로 반환
    
    # 매칭 실패 시 파일이 존재하는 디렉토리명(가장 깊은 폴더)에 name_case 적용
    return self._apply_name_case(parts[-1], name_case)
```

#### 3. 호출부 수정

**변경 파일**: `analyzer/strategy/dbms/dbms_analyzer.py` (922~929줄)

```python
async def process_file(ctx: FileAnalysisContext):
    async with self._file_semaphore:
        try:
            # name_case 옵션 가져오기
            name_case = getattr(orchestrator, 'name_case', 'original')
            
            # 파일 경로 기반 기본 스키마 결정 (name_case 적용)
            default_schema = self._resolve_default_schema(ctx.directory, name_case)
            
            processor = DbmsAstProcessor(
                # ... 기타 파라미터
                name_case=name_case,
            )
```

### 개선 효과

#### ✅ 데이터 정합성 보장
프론트엔드에서 지정한 대소문자 규칙이 전체 파이프라인에 일관되게 적용됩니다.

**이전 동작**:
```
프론트: name_case = "uppercase"
   ↓
DDL 저장: Table {schema: "RWIS"}
   ↓
디렉토리 매칭: default_schema = "rwis"  ❌ 불일치
   ↓
FK 관계: MATCH (t:Table {schema: 'rwis'})  ❌ 매칭 실패
```

**개선 후**:
```
프론트: name_case = "uppercase"
   ↓
DDL 저장: Table {schema: "RWIS"}
   ↓
DDL 스키마 수집: _ddl_schemas = {"RWIS"}  ✅ name_case 적용
   ↓
디렉토리 매칭: 
  폴더 "rwis" vs DDL "RWIS" → 소문자 비교로 매칭
  → default_schema = "RWIS"  ✅ DDL 원본 반환
   ↓
FK 관계: MATCH (t:Table {schema: 'RWIS'})  ✅ 매칭 성공!
```

#### ✅ FK 관계 정상 생성
테이블 간 외래키 관계가 정확히 추적되어 데이터 릴레이션 분석이 가능해졌습니다.

#### ✅ 사용자 의도 반영
사용자가 선택한 네이밍 컨벤션이 분석 결과에 정확히 반영되어, 기업의 코딩 표준을 준수한 분석이 가능합니다.

#### ✅ 디버깅 효율성
대소문자 불일치로 인한 매칭 실패 문제가 해소되어, 관계 생성 실패 원인 파악이 쉬워졌습니다.

### 영향 범위

- **DDL 분석**: 스키마 수집 로직 개선
- **SP 분석**: 디렉토리 기반 스키마 결정 로직 개선
- **FK 관계 생성**: 테이블 매칭 정확도 향상

---

## 🧹 로그 정리

### 개선 내용

FK 관계 생성 시 출력되던 상세 디버깅 로그를 제거하여 로그 가독성을 향상시켰습니다.

**변경 파일**: `analyzer/strategy/dbms/ast_processor.py`

**제거된 로그**:
- `[FK_RELATION] 처리 시작: ...`
- `[FK_RELATION] 불완전한 관계 스킵: ...`
- `[FK_RELATION] ... -> ... | effective_schema: ...`
- `[FK_RELATION] FK_TO_TABLE 쿼리: ...`
- `[FK_RELATION] FK_TO(Column) 쿼리: ...`

### 개선 효과

#### ✅ 로그 가독성 향상
불필요한 디버깅 로그 제거로 핵심 정보에 집중할 수 있습니다.

#### ✅ 성능 개선
로그 출력 오버헤드가 감소합니다 (미미하지만 누적 효과).

---

## 📊 기술적 가치 요약

### 1. 데이터 품질 향상
- **정확한 관계 추적**: FK 관계가 정상적으로 생성되어 데이터 릴레이션 분석 가능
- **일관된 네이밍**: 사용자 지정 네이밍 규칙이 전체 파이프라인에 일관되게 적용

### 2. 사용자 경험 개선
- **예측 가능한 동작**: 사용자가 지정한 옵션이 예상대로 작동
- **명확한 로그**: 핵심 정보만 표시되어 문제 파악 용이

### 3. 유지보수성 향상
- **명확한 로직**: 대소문자 처리 로직이 일관되게 적용되어 이해하기 쉬움
- **디버깅 용이**: 불필요한 로그 제거로 실제 문제 파악이 빠름

---

## 🔄 마이그레이션 가이드

기존에 분석된 데이터가 있다면, 다음을 권장합니다:

1. **Neo4j 데이터 재분석**: `name_case` 옵션을 지정하여 전체 소스 코드를 재분석
2. **FK 관계 확인**: 다음 쿼리로 FK 관계가 정상적으로 생성되었는지 확인

```cypher
MATCH (src)-[r:FK_TO_TABLE]->(t:Table)
WHERE r.source = "procedure"
RETURN DISTINCT src, t, r
LIMIT 10
```

---

## 📝 관련 파일

- `analyzer/strategy/dbms/dbms_analyzer.py`: DDL 스키마 수집 및 디렉토리 매칭 로직
- `analyzer/strategy/dbms/ast_processor.py`: FK 관계 생성 로직 및 로그 정리

