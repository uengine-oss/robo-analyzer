# ROBO Analyzer 리팩토링 검토 요약

## 📊 전체 검토 결과

### ✅ 완료된 항목

#### 1. 레거시 코드 제거
- ✅ `understand/`, `service/` 디렉토리 완전 삭제
- ✅ 모든 deprecated 별칭 제거
- ✅ 레거시 API 엔드포인트 제거
- ✅ `rules/understand/` → `rules/dbms/`, `rules/framework/` 변경

#### 2. 명칭 변경 (UNDERSTANDING → ANALYSIS)
- ✅ 모든 파일명, 클래스명, 함수명 변경
- ✅ 로그 메시지 변경 (`UNDERSTAND` → `ANALYZE`)
- ✅ 규칙 파일 경로 변경

#### 3. 예외 처리 개선
- ✅ 계층적 예외 클래스 구조
- ✅ 예외 전파 (`stream_with_error_boundary`)
- ✅ 조용히 실패하는 부분에 로그 추가
- ✅ 부분 실패 허용 정책 명확화

#### 4. 로깅 강화
- ✅ `util/logger.py` 생성
- ✅ `[ANALYZE]` 접두사 통일
- ✅ 컨텍스트 로깅 지원

#### 5. 환경변수 중앙화
- ✅ `config/settings.py` 생성
- ✅ `env.example` 생성

#### 6. 스트림 결과 메시지화
- ✅ `format_graph_result()` 함수 생성
- ✅ 노드/관계 생성 정보 스트리밍

---

## ⚠️ 부분 완료 / 수정 필요

### 1. 이중 병렬 처리 ❌

**현재 상태**:
```python
# framework_analyzer.py (현재)
for file_idx, (directory, file_name) in enumerate(file_names, 1):
    async for chunk in self._analyze_file(...):
        yield chunk  # 순차 처리
```

**필요한 변경**:
```python
# ParallelExecutor 사용
executor = ParallelExecutor()
tasks = [AnalysisTask(...) for ... in file_names]
async for event in executor.run_parallel(tasks, processor):
    yield event
```

**영향**: 성능 향상 (파일 5개 동시 처리)

### 2. 메서드 콜 통합 ❌

**현재 상태**:
- `rules/framework/method_call.yaml` 별도 존재
- `rules/framework/analysis.yaml`에 `calls` 필드 없음
- `_build_method_call_queries()`에서 `targetClass`, `methodName` 사용

**필요한 변경**:
```yaml
# rules/framework/analysis.yaml
output_format:
  analysis: [...]
  calls: [
    {
      "target": "orderService",
      "methodName": "findAll"
    }
  ]
```

**영향**: 프롬프트 통합, 일관성 향상

### 3. MERGE → MATCH 변경 ❌

**현재 상태**:
```cypher
MERGE (c:CLASS {...})
SET c.summary = '...'
```

**필요한 변경**:
```cypher
MATCH (c:CLASS {...})
SET c.summary = '...'
```

**이유**: AST 구조가 이미 생성되어 있으므로 MERGE 불필요

**영향**: 성능 향상, 중복 체크 제거

### 4. 임시 노드 제거 ❌

**현재 상태**:
```cypher
WHERE (t:CLASS OR t:INTERFACE OR t:ENUM OR t:TEMP)
```

**필요한 변경**:
```cypher
WHERE (t:CLASS OR t:INTERFACE OR t:ENUM)
# TEMP 제거, AST에서 직접 조회
```

**영향**: 그래프 구조 단순화

---

## 📋 결과물 형태 검증

### 노드 속성 (변경 없음)

#### CLASS 노드
```cypher
{
  user_id: string,
  project_name: string,
  class_name: string,
  class_kind: "CLASS" | "INTERFACE" | "ENUM",
  summary: string,
  startLine: int,
  endLine: int,
  file_name: string,
  directory: string
}
```

#### METHOD 노드
```cypher
{
  user_id: string,
  project_name: string,
  method_name: string,
  class_key: string,
  startLine: int,
  endLine: int
}
```

#### PROCEDURE 노드 (DBMS)
```cypher
{
  user_id: string,
  project_name: string,
  procedure_name: string,
  summary: string,
  startLine: int,
  endLine: int
}
```

### 관계 타입 (변경 없음)
- `EXTENDS`: 상속 관계
- `IMPLEMENTS`: 구현 관계
- `CALLS`: 메서드 호출 관계
- `FROM`: 테이블 읽기 (DBMS)
- `WRITES`: 테이블 쓰기 (DBMS)
- `HAS_COLUMN`: 테이블-컬럼 관계 (DBMS)

---

## 🔍 Cypher 쿼리 변경 사항

### 변경 전 (v1.x)
```cypher
# 임시 노드 생성
MERGE (t:TEMP {class_name: 'OrderService'})
# 나중에 실제 노드로 교체
MERGE (c:CLASS {class_name: 'OrderService'})
SET c.summary = '...'
```

### 변경 후 (v2.0 - 현재)
```cypher
# 여전히 MERGE 사용 중 (변경 필요)
MERGE (c:CLASS {...})
SET c.summary = '...'
```

### 목표 (v2.0 - 수정 필요)
```cypher
# AST에서 직접 조회
MATCH (c:CLASS {
  user_id: 'user',
  project_name: 'project',
  class_name: 'OrderService'
})
SET c.summary = '...'
```

---

## 📝 스트림 전달 방식

### 이벤트 흐름
```
1. message: "분석 시작"
2. message: "파일 1/10 처리 중"
3. data: Phase 1 (정적 그래프) 결과
   {
     "type": "data",
     "graph": {
       "Nodes": [...],
       "Relationships": [...]
     }
   }
4. message: "CLASS 노드 5개 생성"
5. message: "CALLS 관계 10개 연결"
6. data: Phase 2 (LLM 분석) 결과
7. complete: "분석 완료"
```

### 메시지화 함수
```python
# util/stream_utils.py
def format_graph_result(graph: dict) -> str:
    """Neo4j 그래프 결과를 사용자 친화적 메시지로 변환"""
    # 노드 타입별 집계
    # 관계 타입별 집계
    # 메시지 반환
```

---

## 🎯 수정 우선순위

### High Priority
1. **이중 병렬 처리 구현** - 성능 향상
2. **MERGE → MATCH 변경** - 성능 향상, 중복 제거

### Medium Priority
3. **메서드 콜 통합** - 일관성 향상
4. **임시 노드 제거** - 구조 단순화

### Low Priority
5. **스트림 메시지화 강화** - 사용자 경험 향상

---

## ✅ 검증 체크리스트

- [x] 레거시 코드 완전 제거
- [x] 명칭 변경 (UNDERSTANDING → ANALYSIS)
- [x] 예외 처리 개선
- [x] 로깅 강화
- [x] 환경변수 중앙화
- [x] 스트림 결과 메시지화 (부분)
- [ ] 이중 병렬 처리 구현
- [ ] 메서드 콜 통합
- [ ] MERGE → MATCH 변경
- [ ] 임시 노드 제거

