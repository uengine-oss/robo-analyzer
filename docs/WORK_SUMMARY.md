# 리팩토링 및 스펙 변경 작업 요약

이 문서는 ROBO Analyzer 프로젝트의 리팩토링 및 스펙 변경 작업 내용을 쉽게 이해할 수 있도록 정리한 것입니다.

---

## 📌 작업 목표

크게 두 가지 목표가 있었습니다:

1. **리팩토링**: 코드를 더 깔끔하고 이해하기 쉽게 정리
2. **스펙 변경**: 새로운 기능 추가 (병렬 처리 등)

---

## 🔄 무엇이 바뀌었나요?

### 1. 이름 변경 (UNDERSTANDING → ANALYSIS)

기존에는 "이해(Understanding)"라는 용어를 사용했는데, "분석(Analysis)"으로 변경했습니다.

| 기존 | 변경 후 | 이유 |
|------|---------|------|
| `understand/` 폴더 | `analyzer/` 폴더 | "ROBO-ANALYZER"라는 이름에 맞게 |
| `ServiceOrchestrator` | `AnalysisOrchestrator` | 역할이 명확하게 드러나도록 |
| `neo4j_connection.py` | `neo4j_client.py` | "연결"보다 "클라이언트"가 더 정확 |
| `code_analyzer.py` | `ast_processor.py` | AST(코드 구조) 처리라는 역할을 명확히 |

### 2. API 경로 변경

```
기존: /backend/understanding/
변경: /robo/analyze/
```

### 3. 폴더 구조 단순화

```
기존: rules/understand/dbms/
변경: rules/dbms/

기존: rules/understand/framework/
변경: rules/framework/
```

---

## ⚡ 병렬 처리는 어떻게 되나요?

### 이중 병렬 처리란?

한 번에 여러 파일을 동시에 분석하고, 각 파일 내에서도 여러 코드 조각을 동시에 분석합니다.

```
일반적인 방식 (순차 처리):
  파일1 분석 → 파일2 분석 → 파일3 분석 → ...
  (한 번에 하나씩, 느림)

이중 병렬 처리:
  ┌─ 파일1 ─┐   ┌─ 파일2 ─┐   ┌─ 파일3 ─┐
  │ 청크1   │   │ 청크1   │   │ 청크1   │
  │ 청크2   │   │ 청크2   │   │ 청크2   │
  │ 청크3   │   │ 청크3   │   │ 청크3   │
  └─────────┘   └─────────┘   └─────────┘
     ↓ 동시에 처리! ↓           ↓
```

### 설정 값

- **파일 동시 처리 수**: 5개 (FILE_CONCURRENCY 환경변수)
- **청크 동시 처리 수**: 5개 (MAX_CONCURRENCY 환경변수)

---

## 🔒 데이터베이스 동시성 문제는 어떻게 해결하나요?

여러 작업이 동시에 데이터베이스에 쓰려고 하면 충돌이 날 수 있습니다.

**해결책: 락(Lock) 사용**

```
작업1: Neo4j에 쓰기 요청 ─┐
작업2: Neo4j에 쓰기 요청 ─┼─→ 락으로 순서 보장 ─→ Neo4j
작업3: Neo4j에 쓰기 요청 ─┘
```

코드에서는 `asyncio.Lock()`을 사용합니다:

```python
self._cypher_lock = asyncio.Lock()

async with self._cypher_lock:
    # 한 번에 하나의 작업만 여기 들어올 수 있음
    await client.run_graph_query(queries)
```

---

## 📊 분석 과정 (2단계)

### Phase 1: 정적 그래프 생성 (뼈대 만들기)

코드를 읽어서 구조만 먼저 그래프로 만듭니다.

```
Java 코드:
  public class OrderService {
      private OrderRepository repo;
      public void save() { ... }
  }

        ↓ Phase 1

Neo4j 그래프:
  (CLASS: OrderService) ──PARENT_OF──→ (FIELD: repo)
                        ──PARENT_OF──→ (METHOD: save)
```

### Phase 2: AI 분석 (살 붙이기)

AI(LLM)가 각 코드 조각의 의미를 분석합니다.

```
AI 분석 결과:
  - 요약: "주문 저장 서비스"
  - 호출: ["repo.save"]
  - 의존성: ["OrderRepository"]

        ↓ Neo4j에 추가

  (CLASS: OrderService) ──CALLS──→ (CLASS: OrderRepository)
                        ──DEPENDENCY──→ (CLASS: Order)
```

---

## 📡 CALLS 관계는 어떻게 만들어지나요?

### 기존 방식 (복잡했음)

1. 별도의 "method_call" 분석 프롬프트 사용
2. 임시 노드 생성 후 나중에 교체
3. 복잡한 패턴 매칭

### 새로운 방식 (단순해짐)

1. 일반 분석에 `calls` 배열 포함
2. `target.methodName` 형식으로 반환
3. AST에서 이미 만든 클래스 노드와 바로 연결

```
AI 응답:
{
  "calls": ["orderRepo.save", "validator.check"]
}

        ↓ 파싱

target: orderRepo, method: save
target: validator, method: check

        ↓ 쿼리

MATCH (src:METHOD {...})
MATCH (dst:CLASS) WHERE dst.class_name = 'orderRepo'
MERGE (src)-[:CALLS {method: 'save'}]->(dst)
```

---

## ❌ 에러 처리는 어떻게 되나요?

### 부분 실패 허용 정책

| 실패 종류 | 동작 | 이유 |
|----------|------|------|
| 개별 노드 분석 실패 | 경고 로그 + 계속 진행 | 한 메서드 실패로 전체 중단은 과함 |
| 클래스 요약 실패 | 경고 로그 + 해당 클래스만 스킵 | 다른 클래스는 정상 처리 |
| 파이프라인 전체 실패 | 에러 전파 + 프론트엔드 알림 | 심각한 문제는 반드시 알림 |
| User Story 생성 실패 | 경고 로그 + 분석 결과는 유지 | 문서화는 부가 기능 |

### 로그 레벨

```
WARNING: 부분 실패 (계속 진행)
ERROR: 심각한 오류 (중단 또는 재시도 필요)
```

---

## 🗃️ MERGE vs MATCH 사용 기준

### MERGE가 필요한 경우

| 상황 | 이유 |
|------|------|
| 노드 생성 | 같은 분석을 다시 돌리면 중복 방지 |
| DDL 테이블 | 여러 DDL에서 같은 테이블 참조 가능 |
| 관계 생성 | 동일 관계 중복 생성 방지 |

### 현재 패턴

```cypher
-- 노드는 이미 있는지 확인 (MATCH)
MATCH (src:CLASS {...})
MATCH (dst:CLASS {...})

-- 관계만 없으면 생성 (MERGE)
MERGE (src)-[r:CALLS]->(dst)
```

---

## 📁 생성되는 노드와 관계

### Framework (Java/Kotlin)

**노드 속성**:
- `startLine`, `endLine`: 코드 위치
- `name`: 이름
- `class_name`: 클래스명 (CLASS/INTERFACE/ENUM)
- `summary`: AI가 생성한 요약
- `node_code`: 원본 코드

**관계**:
- `PARENT_OF`: 부모-자식 (클래스 → 메서드)
- `NEXT`: 형제 순서 (메서드1 → 메서드2)
- `EXTENDS`, `IMPLEMENTS`: 상속/구현
- `CALLS`: 메서드 호출
- `DEPENDENCY`: 타입 의존성
- `ASSOCIATION`, `COMPOSITION`: 클래스 관계

### DBMS (PL/SQL)

**노드 속성**:
- `procedure_name`: 프로시저 이름
- `schema`: 스키마명
- `summary`: AI가 생성한 요약

**관계**:
- `CALL`: 프로시저 호출
- `FROM`: 테이블 읽기
- `WRITES`: 테이블 쓰기
- `HAS_COLUMN`: 테이블 → 컬럼
- `FK_TO_TABLE`: 외래키 관계

---

## 📤 실시간 스트리밍

분석 진행 상황을 실시간으로 알려줍니다.

### 이벤트 종류

```json
// 메시지 (진행 상황)
{"type": "message", "content": "📄 [1/10] OrderService.java 분석 중..."}

// 데이터 (그래프 결과)
{"type": "data", "graph": {"Nodes": [...], "Relationships": [...]}}

// 노드 생성 알림
{"type": "node_event", "action": "created", "nodeType": "CLASS", "nodeName": "OrderService"}

// 관계 생성 알림
{"type": "relationship_event", "action": "created", "relType": "CALLS", "source": "save", "target": "OrderRepository"}

// 완료
{"type": "complete"}

// 에러
{"type": "error", "content": "분석 실패: ..."}
```

---

## ✅ 삭제된 기능

### _postprocess_variables 함수

변수 타입을 테이블 메타데이터로 해석하는 후처리 함수를 삭제했습니다.

- 사용자 요청으로 삭제
- 관련 프롬프트 파일(variable_type_resolve.yaml)도 삭제

---

## 📋 체크리스트

### 리팩토링 (docs/refactorign.txt)

| 항목 | 상태 | 내용 |
|------|------|------|
| 명칭 변경 | ✅ | UNDERSTANDING → ANALYSIS 전체 적용 |
| 예외 처리 | ✅ | 부분 실패 정책 명확화, 로그 구성 |
| 비효율 제거 | ✅ | 중복 코드 정리, 구조 단순화 |
| ENV EXAMPLE | ✅ | env.example 파일 생성 |
| 근본 변경 | ✅ | method_call 통합, 임시 노드 패턴 제거 |

### 스펙 변경 (docs/todo.txt)

| 항목 | 상태 | 내용 |
|------|------|------|
| 1. 이중 병렬 처리 | ✅ | ParallelExecutor + ChunkBatcher |
| 2. AST 먼저 생성 | ✅ | Phase1(정적) → Phase2(LLM) 분리 |
| 3. Cypher 동시성 | ✅ | asyncio.Lock() 적용 |
| 4. method_call 통합 | ✅ | calls 배열로 일반 분석에 통합 |
| 5. MERGE → MATCH | ✅ | 노드 MATCH 후 관계 MERGE |
| 6. 스트림 메시지화 | ✅ | emit_node_event, emit_relationship_event |

---

## 🎯 결론

이번 작업으로:

1. **코드가 더 깔끔해졌습니다** - 이름만 봐도 무슨 역할인지 알 수 있음
2. **분석 속도가 빨라졌습니다** - 이중 병렬 처리로 여러 파일을 동시에 분석
3. **안정성이 높아졌습니다** - 에러 처리가 명확하고, 데이터베이스 동시성 문제 해결
4. **결과물은 그대로입니다** - 노드와 관계의 속성은 기존과 동일하게 유지

---

*작성일: 2025-12-31*

