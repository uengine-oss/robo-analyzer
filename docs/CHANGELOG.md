# ROBO-ANALYZER 변경 이력

## v2.0.0 - 대규모 리팩토링 및 스펙 변경

### 🏗️ 아키텍처 변경

#### 2단계 분석 + 이중 병렬 처리
- **Phase 1**: 모든 파일 AST 그래프 생성 (파일 병렬 5개)
- **Phase 2**: 모든 파일 LLM 분석 (파일 병렬 5개 + 청크 병렬)
- **Phase 3**: User Story 문서 생성

#### Cypher 동시성 보호
- `asyncio.Lock`을 사용한 Cypher 쿼리 동시 실행 제어
- 파일별 세마포어로 병렬 처리 수 제한 (기본 5개)

#### TEMP 노드 패턴 제거
- **이전**: 존재하지 않는 클래스에 TEMP 노드 생성 후 나중에 대체
- **현재**: Phase 1에서 모든 클래스가 먼저 생성되므로 MATCH만 사용
- 외부 라이브러리 관계는 생성되지 않음 (의도된 동작)

### 📝 프롬프트 통합

#### method_call.yaml 제거
- `analysis.yaml`에 `calls` 배열 통합
- 형식: `["target.methodName", "target2.method2"]`
- LLM 호출 1회로 통합하여 비용 절감

### 📁 모듈 이름 변경

| 이전 | 현재 | 설명 |
|------|------|------|
| `understand/` | `analyzer/` | ROBO-ANALYZER 명칭과 일치 |
| `service/` | `api/` | 역할 명확화 |
| `service/service.py` | `api/orchestrator.py` | 오케스트레이터 역할 명확화 |
| `understand/neo4j_connection.py` | `analyzer/neo4j_client.py` | Neo4j 클라이언트 |
| `understand_*()` 함수들 | `analyze_*()` | 함수명 통일 |

### 🔧 API 엔드포인트 변경

| 이전 | 현재 |
|------|------|
| `/backend/understanding/` | `/robo/analyze/` |
| `/backend/deleteAll/` | `/robo/data/` (DELETE) |

### 📂 rules 폴더 구조 변경

| 이전 | 현재 |
|------|------|
| `rules/understand/dbms/` | `rules/dbms/` |
| `rules/understand/framework/` | `rules/framework/` |

### 🛡️ 예외 처리 개선

#### 예외 계층 구조
```
RoboAnalyzerError (기본)
├── ConfigError
├── AnalysisError
├── CodeProcessError
├── LLMCallError
├── QueryExecutionError
└── AuthenticationError
```

#### 부분 실패 허용 정책
- 개별 파일/배치 실패: 로그 기록 후 다음 진행
- 핵심 오류: 예외 재발생, 프론트엔드로 전파

### 📡 스트리밍 개선

#### 결과 메시지화
- `format_graph_result()`: Neo4j 결과를 사용자 친화적 메시지로 변환
- 노드/관계 생성 정보 실시간 스트리밍

### 🗑️ 레거시 코드 제거

- `service/` 디렉토리 완전 삭제
- `understand/` 디렉토리 완전 삭제
- `test/test_understanding.py` → `test/test_analyzer.py` 이동
- `CHANGELOG.md` (루트) → `docs/CHANGELOG.md` 이동
- 모든 `understand_*` 함수명 → `analyze_*`로 변경
- 모든 "Understanding" 로그 메시지 → "분석"으로 변경

### 📋 새로 추가된 파일

- `config/settings.py`: 중앙화된 환경 설정
- `util/logger.py`: 구조화된 로깅
- `util/stream_utils.py`: NDJSON 스트리밍 유틸리티
- `analyzer/parallel_executor.py`: 이중 병렬 처리
- `env.example`: 환경 변수 예시
- `docs/ARCHITECTURE.md`: 아키텍처 문서

### ⚠️ 주의사항

#### 결과물 형태 유지
- 노드 속성: 기존과 동일 (startLine, endLine, name, summary, class_name, etc.)
- 관계 타입: 기존과 동일 (CALLS, DEPENDENCY, EXTENDS, IMPLEMENTS, etc.)
- Neo4j 스키마: 변경 없음

#### 외부 라이브러리 관계
- Phase 1에서 생성되지 않은 클래스에 대한 관계는 생성되지 않음
- 예: `orderService.save()` 호출 시 `OrderService`가 프로젝트 내에 없으면 CALLS 관계 미생성
- 이것은 프로젝트 내 코드 관계에 집중하기 위한 의도된 동작

