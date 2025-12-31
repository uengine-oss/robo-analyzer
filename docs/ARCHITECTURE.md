# ROBO-ANALYZER 아키텍처 문서

## 📋 개요

ROBO-ANALYZER는 레거시 코드를 분석하여 Neo4j 그래프 데이터베이스에 
클래스 다이어그램 및 프로시저 분석 결과를 저장하는 시스템입니다.

## 🏗️ 프로젝트 구조

```
robo_analyzer_core/
├── main.py                    # FastAPI 진입점
├── api/
│   ├── router.py              # API 라우터 (/robo/analyze/, /robo/data/)
│   └── orchestrator.py        # AnalysisOrchestrator (분석 파이프라인 조율)
├── analyzer/
│   ├── neo4j_client.py        # Neo4j 연결 및 쿼리 실행
│   ├── parallel_executor.py   # 이중 병렬 처리 (파일 + 청크)
│   └── strategy/
│       ├── base_analyzer.py   # AnalyzerStrategy 추상 클래스
│       ├── analyzer_factory.py # 전략 팩토리
│       ├── framework/
│       │   ├── framework_analyzer.py  # Java/Kotlin 분석 전략
│       │   └── ast_processor.py       # AST 처리 및 LLM 분석
│       └── dbms/
│           ├── dbms_analyzer.py       # PL/SQL 분석 전략
│           └── ast_processor.py       # DBMS AST 처리
├── config/
│   └── settings.py            # 환경 설정 (AnalyzerConfig 싱글톤)
├── rules/
│   ├── dbms/                  # DBMS 분석 프롬프트 (YAML)
│   └── framework/             # Framework 분석 프롬프트 (YAML)
└── util/
    ├── exception.py           # 예외 클래스 계층
    ├── logger.py              # 로깅 유틸리티
    ├── llm_client.py          # LLM 클라이언트 생성
    ├── rule_loader.py         # YAML 프롬프트 로더
    ├── stream_utils.py        # NDJSON 스트리밍 유틸리티
    └── utility_tool.py        # 범용 유틸리티
```

## 🔄 분석 흐름 (2단계 + 이중 병렬)

```
┌─────────────────────────────────────────────────────────────────┐
│                    AnalysisOrchestrator                         │
│  1. 사용자 인증 및 프로젝트 디렉토리 설정                           │
│  2. 소스 파일 목록 수집                                          │
│  3. 전략 선택 (Framework/DBMS) 및 분석 실행                       │
└─────────────────────────────────────────────────────────────────┘
                              │
                              ▼
┌─────────────────────────────────────────────────────────────────┐
│                   FrameworkAnalyzer / DbmsAnalyzer              │
│                                                                 │
│  ┌─────────────────────────────────────────────────────────┐    │
│  │  Phase 1: AST 그래프 생성 (파일 병렬 5개)                 │    │
│  │  - build_static_graph_queries() → MERGE로 노드 생성       │    │
│  │  - 모든 CLASS, METHOD, FIELD 노드 먼저 생성               │    │
│  └─────────────────────────────────────────────────────────┘    │
│                              │                                   │
│                              ▼                                   │
│  ┌─────────────────────────────────────────────────────────┐    │
│  │  Phase 2: LLM 분석 (파일 병렬 5개 + 청크 병렬)            │    │
│  │  - run_llm_analysis() → MATCH로 기존 노드 업데이트        │    │
│  │  - 요약, CALLS, DEPENDENCY 관계 생성                      │    │
│  └─────────────────────────────────────────────────────────┘    │
│                              │                                   │
│                              ▼                                   │
│  ┌─────────────────────────────────────────────────────────┐    │
│  │  Phase 3: User Story 문서 생성                            │    │
│  └─────────────────────────────────────────────────────────┘    │
└─────────────────────────────────────────────────────────────────┘
```

## 🔒 동시성 보호

### Cypher 쿼리 락
```python
# FrameworkAnalyzer
self._cypher_lock = asyncio.Lock()

async with self._cypher_lock:
    graph = await client.run_graph_query(queries)
```

### 파일 세마포어
```python
self._file_semaphore = asyncio.Semaphore(settings.concurrency.file_concurrency)

async with self._file_semaphore:
    # 최대 5개 파일 동시 처리
```

## 📊 Cypher 쿼리 전략

### Phase 1: 정적 그래프 생성 (MERGE)
```cypher
-- 클래스 노드 생성
MERGE (n:CLASS {class_name: 'OrderService', user_id: '...', project_name: '...'})
SET n.startLine = 10, n.directory = 'com/example', n.file_name = 'OrderService.java', ...
RETURN n

-- 일반 노드 생성
MERGE (n:METHOD {startLine: 20, directory: '...', file_name: '...', user_id: '...', project_name: '...'})
SET n.name = 'save', n.endLine = 35, ...
RETURN n
```

### Phase 2: LLM 분석 결과 적용 (MATCH)
```cypher
-- 요약 업데이트
MATCH (n:METHOD {startLine: 20, directory: '...', file_name: '...', ...})
SET n.summary = '주문을 저장합니다...'
RETURN n

-- CALLS 관계 (MATCH로 기존 클래스 조회)
MATCH (src:CLASS {startLine: 10, ...})
MATCH (dst) WHERE dst:CLASS AND toLower(dst.class_name) = toLower('OrderRepository')
  AND dst.user_id = '...' AND dst.project_name = '...'
MERGE (src)-[r:CALLS {method: 'save'}]->(dst)
RETURN r
```

### TEMP 노드 패턴 제거
- **이전**: 존재하지 않는 클래스에 대해 TEMP 노드 생성 후 나중에 실제 타입으로 대체
- **현재**: Phase 1에서 모든 클래스가 먼저 생성되므로 MATCH만 사용
- 외부 라이브러리 클래스에 대한 관계는 생성되지 않음 (의도된 동작)

## 📡 스트리밍 프로토콜

### NDJSON 이벤트 형식
```json
{"type": "message", "content": "🚀 분석 시작"}
{"type": "data", "graph": {...}, "analysis_progress": 50, "current_file": "..."}
{"type": "node_event", "action": "created", "nodeType": "CLASS", "nodeName": "OrderService"}
{"type": "relationship_event", "action": "created", "relType": "CALLS", "source": "...", "target": "..."}
{"type": "complete"}
```

### 결과 메시지화
```python
from util.stream_utils import format_graph_result

graph_msg = format_graph_result(graph)
# 출력: "→ CLASS 노드 3개 생성\n→ CALLS 관계 5개 연결"
```

## 🛡️ 예외 처리 정책

### 예외 계층
```
RoboAnalyzerError (기본)
├── ConfigError (설정 오류)
├── AnalysisError (분석 오류)
├── CodeProcessError (코드 처리 오류)
├── LLMCallError (LLM 호출 오류)
├── QueryExecutionError (Neo4j 쿼리 오류)
└── AuthenticationError (인증 오류)
```

### 부분 실패 허용 정책
| 영역 | 실패 시 동작 |
|------|------------|
| 개별 파일 분석 | 로그 기록, 다음 파일 진행 |
| 상속/필드/메서드 LLM | 로그 기록, 빈 결과 반환 |
| 배치 LLM 분석 | 로그 기록, 다음 배치 진행 |
| User Story 생성 | 로그 기록, 분석은 정상 완료 |
| 핵심 오류 | 예외 재발생, 프론트엔드로 전파 |

## 📝 프롬프트 통합

### 이전 (분리)
- `analysis.yaml`: 코드 요약, 변수, 의존성
- `method_call.yaml`: 메서드 호출 추출

### 현재 (통합)
- `analysis.yaml`: 모든 분석 통합
  - 코드 요약 (summary)
  - 변수 식별 (variables)
  - 로컬 의존성 (localDependencies)
  - **메서드 호출 (calls)** ← 통합됨

### calls 배열 형식
```json
{
  "analysis": [{
    "startLine": 10,
    "endLine": 25,
    "summary": "...",
    "variables": ["order", "customer"],
    "localDependencies": [{"type": "Customer", "sourceMember": "save"}],
    "calls": ["orderRepository.save", "eventPublisher.publish"]
  }]
}
```

## 📋 환경 변수

`env.example` 참조:
- `NEO4J_URI`: Neo4j 연결 URI
- `NEO4J_USER`: 사용자명
- `NEO4J_PASSWORD`: 비밀번호
- `OPENAI_API_KEY`: OpenAI API 키
- `FILE_CONCURRENCY`: 파일 병렬 처리 수 (기본: 5)
- `MAX_CONCURRENCY`: 청크 병렬 처리 수 (기본: 4)

