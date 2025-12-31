# ROBO Analyzer 마이그레이션 가이드

## 📋 변경 사항 요약

### 1. 프로젝트 구조 변경

#### 디렉토리 구조
```
변경 전 (v1.x)                    변경 후 (v2.0)
├── understand/                   ├── analyzer/
│   ├── neo4j_connection.py     │   ├── neo4j_client.py
│   └── strategy/                │   ├── parallel_executor.py (신규)
├── service/                      │   └── strategy/
│   ├── router.py                ├── api/
│   └── service.py               │   ├── router.py
└── rules/understand/            │   └── orchestrator.py
                                ├── config/ (신규)
                                └── rules/
                                    ├── dbms/
                                    └── framework/
```

#### 파일명 변경
| 변경 전 | 변경 후 | 이유 |
|---------|---------|------|
| `code_analyzer.py` | `ast_processor.py` | AST 처리 역할 명확화 |
| `ServiceOrchestrator` | `AnalysisOrchestrator` | 역할 명확화 |
| `Neo4jConnection` | `Neo4jClient` | 클라이언트 패턴 |

### 2. API 엔드포인트 변경

| 변경 전 | 변경 후 |
|---------|---------|
| `POST /backend/understanding/` | `POST /robo/analyze/` |
| `DELETE /backend/deleteAll/` | `DELETE /robo/data/` |

### 3. 스트리밍 응답 형식

#### 이벤트 타입
```json
// 메시지 이벤트
{"type": "message", "content": "분석 시작"}

// 데이터 이벤트
{
  "type": "data",
  "graph": {
    "Nodes": [...],
    "Relationships": [...]
  },
  "line_number": 100,
  "analysis_progress": 50,
  "current_file": "Test.java"
}

// 노드 이벤트
{
  "type": "node_event",
  "action": "created",
  "nodeType": "CLASS",
  "nodeName": "OrderService"
}

// 관계 이벤트
{
  "type": "relationship_event",
  "action": "created",
  "relType": "CALLS",
  "source": "OrderService",
  "target": "OrderRepository.save"
}

// 완료 이벤트
{"type": "complete"}
```

#### 스트리밍 흐름
```
1. 메시지: "분석 시작"
2. 메시지: "파일 1/10 처리 중"
3. 데이터: Phase 1 (정적 그래프) 결과
4. 노드 이벤트: CLASS 노드 생성
5. 관계 이벤트: CALLS 관계 생성
6. 데이터: Phase 2 (LLM 분석) 결과
7. 완료: "분석 완료"
```

### 4. Cypher 쿼리 변경

#### 변경 전 (MERGE 기반)
```cypher
MERGE (c:CLASS {
  user_id: 'user',
  project_name: 'project',
  class_name: 'OrderService'
})
SET c.summary = '...'
```

#### 변경 후 (MATCH 기반, AST 직접 조회)
```cypher
// 1단계: AST에서 이미 생성된 노드 조회
MATCH (c:CLASS {
  user_id: 'user',
  project_name: 'project',
  class_name: 'OrderService'
})

// 2단계: 속성 업데이트
SET c.summary = '...'
```

**이유**: AST 구조가 이미 생성되어 있으므로 MERGE 대신 MATCH로 직접 조회 가능

### 5. 메서드 콜 처리 변경

#### 변경 전 (패턴 기반)
```python
# 정규식으로 메서드 호출 패턴 추출
METHOD_CALL_PATTERN = re.compile(r'\w+\.\w+\s*\(')
```

#### 변경 후 (CALL 배열, target.methodName)
```python
# LLM 응답 형식
{
  "calls": [
    {
      "startLine": 100,
      "endLine": 100,
      "target": "orderService",
      "methodName": "findAll"
    }
  ]
}

# 파싱
target, method = call["target"], call["methodName"]
# AST에서 직접 조회
MATCH (source:CLASS {...})
MATCH (target:CLASS {class_name: target})
MATCH (method:METHOD {name: methodName, class_key: target.key})
MERGE (source)-[:CALLS]->(method)
```

**이유**: AST 구조가 이미 있으므로 임시 노드 없이 직접 연결 가능

### 6. 병렬 처리 변경

#### 변경 전 (순차 처리)
```
파일1 → 파일2 → 파일3 → ...
```

#### 변경 후 (이중 병렬 처리)
```
파일1 ─┬─ 청크1 → LLM
      ├─ 청크2 → LLM  } 병렬
      └─ 청크3 → LLM
파일2 ─┬─ 청크1 → LLM
      └─ 청크2 → LLM  } 병렬
파일3 ─┬─ 청크1 → LLM
      └─ 청크2 → LLM  } 병렬
```

**설정**:
- `FILE_CONCURRENCY=5`: 파일 5개 동시 처리
- `MAX_CONCURRENCY=5`: 청크 5개 동시 처리

### 7. 결과물 형태

#### 노드 속성 (변경 없음)
```cypher
// CLASS 노드
{
  user_id: string,
  project_name: string,
  class_name: string,
  class_kind: "CLASS" | "INTERFACE" | "ENUM",
  summary: string,
  startLine: int,
  endLine: int,
  ...
}

// METHOD 노드
{
  user_id: string,
  project_name: string,
  method_name: string,
  class_key: string,
  ...
}

// PROCEDURE 노드 (DBMS)
{
  user_id: string,
  project_name: string,
  procedure_name: string,
  ...
}
```

#### 관계 타입 (변경 없음)
- `EXTENDS`: 상속 관계
- `IMPLEMENTS`: 구현 관계
- `CALLS`: 메서드 호출 관계
- `FROM`: 테이블 읽기 (DBMS)
- `WRITES`: 테이블 쓰기 (DBMS)

### 8. 예외 처리 변경

#### 변경 전
```python
try:
    result = process()
except Exception:
    return []  # 조용히 실패
```

#### 변경 후
```python
try:
    result = process()
except Exception as e:
    logging.error("[ANALYZE] 처리 실패 | error=%s", e, exc_info=True)
    # 부분 실패 허용 (User Story 생성 등)
    return ""
    # 또는 전체 중단
    raise AnalysisError("처리 실패", cause=e)
```

### 9. 로깅 변경

#### 변경 전
```python
log_process("UNDERSTAND", "STAGE", "메시지")
```

#### 변경 후
```python
logging.info("[ANALYZE] 메시지 | key=value")
log_phase("AST", "파싱 완료", file_count=10)
```

---

## ⚠️ 주의사항

### 1. 레거시 코드 완전 제거
- `understand/`, `service/` 디렉토리 삭제됨
- 모든 deprecated 별칭 제거됨
- 레거시 API 엔드포인트 제거됨

### 2. 환경변수 변경
- `DOCKER_COMPOSE_CONTEXT` → `config/settings.py`로 중앙 관리
- `FILE_CONCURRENCY` 추가 (파일 병렬 처리 수)

### 3. 규칙 파일 경로
```
변경 전: rules/understand/dbms/analysis.yaml
변경 후: rules/dbms/analysis.yaml
```

---

## 🔄 마이그레이션 체크리스트

- [ ] API 엔드포인트 변경 (`/backend/understanding/` → `/robo/analyze/`)
- [ ] 환경변수 설정 (`env.example` 참조)
- [ ] 규칙 파일 경로 확인
- [ ] 예외 처리 로직 검토 (부분 실패 허용 범위)
- [ ] 로깅 포맷 확인 (`[ANALYZE]` 접두사)

