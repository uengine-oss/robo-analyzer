# 스펙 변경 TODO 반영 상태

## ✅ 완료된 항목

### 1. 환경변수 중앙화
- ✅ `config/settings.py` 생성
- ✅ `env.example` 생성
- ✅ 모든 `os.getenv()` 호출을 `settings`로 변경

### 2. 명칭 변경 (UNDERSTANDING → ANALYSIS)
- ✅ 디렉토리: `understand/` → `analyzer/`
- ✅ 모듈: `service/` → `api/`
- ✅ 클래스: `ServiceOrchestrator` → `AnalysisOrchestrator`
- ✅ 로그: `UNDERSTAND` → `ANALYZE`

### 3. 예외 처리 개선
- ✅ 계층적 예외 클래스 (`RoboAnalyzerError` 기반)
- ✅ 예외 전파 (`stream_with_error_boundary`)
- ✅ 로그 추가 (조용히 실패하는 부분)

### 4. 로깅 강화
- ✅ `util/logger.py` 생성
- ✅ `[ANALYZE]` 접두사 통일
- ✅ 컨텍스트 로깅 지원

### 5. 스트림 결과 메시지화
- ✅ `format_graph_result()` 함수 생성
- ✅ 노드/관계 생성 정보 스트리밍

---

## ⚠️ 부분 완료 / 수정 필요

### 1. 이중 병렬 처리 ❌

**요구사항**:
- 파일별 병렬 처리 (5개 동시)
- 청크별 병렬 처리

**현재 상태**:
- `parallel_executor.py`는 생성되었지만 **실제로 사용되지 않음**
- `framework_analyzer.py`와 `dbms_analyzer.py`에서 **순차 처리 중**

**수정 필요**:
```python
# 현재 (순차)
for file in files:
    async for chunk in analyze_file(file):
        yield chunk

# 필요 (병렬)
executor = ParallelExecutor()
async for event in executor.run_parallel(tasks, processor):
    yield event
```

### 2. 메서드 콜 통합 ❌

**요구사항**:
- 메서드 콜 프롬프트와 일반 ANALYSIS 프롬프트 통합
- `CALL` 배열로 응답
- `target.methodName` 형식
- AST에서 직접 조회 (임시 노드 제거)

**현재 상태**:
- `method_call.yaml`은 별도로 존재
- `_build_method_call_queries()`에서 `targetClass`, `methodName` 사용
- 하지만 **일반 분석 프롬프트와 통합되지 않음**

**수정 필요**:
```yaml
# rules/framework/analysis.yaml에 통합
output_format:
  calls: [
    {
      "target": "orderService",
      "methodName": "findAll"
    }
  ]
```

### 3. MERGE → MATCH 변경 ❌

**요구사항**:
- AST 구조가 이미 생성되어 있으므로 MERGE 대신 MATCH 사용

**현재 상태**:
- 여전히 MERGE 사용 중
- AST 기반 직접 조회 로직 없음

**수정 필요**:
```cypher
# 변경 전
MERGE (c:CLASS {...})
SET c.summary = '...'

# 변경 후
MATCH (c:CLASS {...})
SET c.summary = '...'
```

### 4. AST 기반 직접 조회 ❌

**요구사항**:
- 임시 노드 제거
- AST에서 직접 클래스/메서드 조회

**현재 상태**:
- 임시 노드 (`TEMP`) 사용 중
- AST 기반 직접 조회 로직 없음

---

## 📋 수정 계획

### Phase 1: 이중 병렬 처리 구현
1. `framework_analyzer.py`에서 `ParallelExecutor` 사용
2. `dbms_analyzer.py`에서 `ParallelExecutor` 사용
3. 파일별 병렬 처리 테스트

### Phase 2: 메서드 콜 통합
1. `rules/framework/analysis.yaml`에 `calls` 필드 추가
2. `_build_method_call_queries()` 수정 (target.methodName 파싱)
3. AST 기반 직접 조회 로직 추가

### Phase 3: MERGE → MATCH 변경
1. 모든 MERGE 쿼리 검색
2. AST 기반 MATCH로 변경
3. 임시 노드 제거

### Phase 4: 결과물 검증
1. 노드 속성 확인
2. 관계 타입 확인
3. 스트림 이벤트 확인

---

## 🔍 현재 코드 상태

### 병렬 처리
- ❌ 파일별 병렬: 구현되지 않음
- ✅ 청크별 병렬: `asyncio.gather()` 사용 중

### 메서드 콜
- ❌ 프롬프트 통합: 별도 프롬프트 사용
- ⚠️ 형식: `targetClass.methodName` (통합 필요)

### Cypher 쿼리
- ❌ MERGE → MATCH: 아직 MERGE 사용
- ❌ AST 직접 조회: 임시 노드 사용

### 스트림 메시지화
- ✅ `format_graph_result()` 함수 존재
- ⚠️ 사용: 부분적으로만 사용됨

