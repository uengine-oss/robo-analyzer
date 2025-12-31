# ROBO Analyzer

소스 코드를 분석하여 Neo4j 그래프로 변환하는 AI 기반 코드 분석 서비스입니다.

## 주요 기능

- **Framework 분석**: Java, Kotlin 코드를 분석하여 클래스 다이어그램 그래프 생성
- **DBMS 분석**: PL/SQL 프로시저/함수를 분석하여 관계도 그래프 생성
- **이중 병렬 처리**: 파일 레벨 + 청크 레벨 동시 병렬로 빠른 분석
- **실시간 스트리밍**: NDJSON 형식으로 분석 진행 상황 실시간 전달
- **User Story 자동 생성**: 분석 결과에서 요구사항 문서 자동 생성

## 시작하기

### 요구사항

- Python 3.11+
- Neo4j 5.x
- LLM API (OpenAI 또는 호환 API)

### 설치

```bash
# 의존성 설치
pip install -r requirements.txt

# 환경변수 설정
cp env.example .env
# .env 파일을 편집하여 API 키 및 Neo4j 연결 정보 설정

# 필수 설정:
# - NEO4J_URI, NEO4J_USER, NEO4J_PASSWORD (Neo4j 연결)
# - LLM_API_KEY (LLM API 키)
# 
# 자세한 내용은 docs/ENV_SETUP.md 참조
```

### 실행

```bash
# 개발 서버
python main.py

# 또는 uvicorn 직접 실행
uvicorn main:app --host 0.0.0.0 --port 5502 --reload
```

## API 엔드포인트

| 메서드 | 경로 | 설명 |
|--------|------|------|
| POST | `/robo/analyze/` | 소스 코드 분석 (스트리밍 응답) |
| DELETE | `/robo/data/` | 사용자 데이터 삭제 |
| GET | `/` | 헬스체크 |
| GET | `/health` | 상세 헬스체크 |
| GET | `/docs` | API 문서 (Swagger UI) |

### 분석 요청 예시

```bash
curl -X POST http://localhost:5502/robo/analyze/ \
  -H "Content-Type: application/json" \
  -H "Session-UUID: your-session-id" \
  -H "OpenAI-Api-Key: your-api-key" \
  -d '{
    "projectName": "my-project",
    "strategy": "framework",
    "target": "java"
  }'
```

## 프로젝트 구조

```
robo_analyzer_core/
├── main.py                      # FastAPI 애플리케이션 진입점
├── api/                         # REST API 레이어
│   ├── router.py               # API 라우터 정의
│   └── orchestrator.py         # 분석 오케스트레이터 (인증, 파일 검색)
├── analyzer/                    # 분석 엔진 코어
│   ├── neo4j_client.py         # Neo4j 비동기 클라이언트
│   ├── parallel_executor.py    # 이중 병렬 처리 실행기
│   └── strategy/               # 분석 전략 패턴
│       ├── base_analyzer.py    # 분석기 기본 인터페이스
│       ├── analyzer_factory.py # 전략 팩토리
│       ├── framework/          # Framework (Java/Kotlin) 분석
│       │   ├── framework_analyzer.py  # 전략 진입점
│       │   └── ast_processor.py       # AST 처리 및 LLM 분석
│       └── dbms/               # DBMS (PL/SQL) 분석
│           ├── dbms_analyzer.py       # 전략 진입점
│           └── ast_processor.py       # AST 처리 및 LLM 분석
├── config/                      # 설정 관리
│   └── settings.py             # 환경변수 중앙 관리 (Singleton)
├── util/                        # 유틸리티 모듈
│   ├── exception.py            # 계층화된 예외 클래스
│   ├── stream_utils.py         # NDJSON 스트리밍 유틸리티
│   ├── logger.py               # 구조화된 로깅
│   ├── llm_client.py           # LLM 클라이언트 팩토리
│   ├── llm_audit.py            # LLM 호출 감사 로깅
│   ├── rule_loader.py          # YAML 프롬프트 규칙 로더
│   └── utility_tool.py         # 범용 유틸리티 함수
├── rules/                       # LLM 프롬프트 규칙 (YAML)
│   ├── framework/              # Framework 분석 프롬프트
│   │   ├── analysis.yaml       # 코드 분석 (calls 배열 포함)
│   │   ├── class_summary.yaml  # 클래스 요약
│   │   └── ...
│   └── dbms/                   # DBMS 분석 프롬프트
│       ├── analysis.yaml       # 코드 분석
│       ├── procedure_summary.yaml
│       └── ...
├── docs/                        # 문서
│   ├── ARCHITECTURE.md         # 아키텍처 설명
│   └── CHANGELOG.md            # 변경 이력
└── test/                        # 테스트
    └── test_analyzer.py        # 분석기 테스트
```

## 분석 흐름

```
┌─────────────────────────────────────────────────────────────────┐
│                     소스 파일 목록                               │
└─────────────────────────────────────────────────────────────────┘
                               │
                               ▼ (파일 5개 병렬 처리)
┌─────────────────────────────────────────────────────────────────┐
│  ┌──────────┐  ┌──────────┐  ┌──────────┐  ┌──────────┐  ...   │
│  │  File 1  │  │  File 2  │  │  File 3  │  │  File 4  │        │
│  │   AST    │  │   AST    │  │   AST    │  │   AST    │        │
│  └────┬─────┘  └────┬─────┘  └────┬─────┘  └────┬─────┘        │
│       │             │             │             │               │
│       ▼ (Phase 1)   ▼             ▼             ▼               │
│  ┌──────────┐  ┌──────────┐  ┌──────────┐  ┌──────────┐        │
│  │ 정적그래프│  │ 정적그래프│  │ 정적그래프│  │ 정적그래프│        │
│  │  노드생성│  │  노드생성│  │  노드생성│  │  노드생성│        │
│  └────┬─────┘  └────┬─────┘  └────┬─────┘  └────┬─────┘        │
│       │             │             │             │               │
│       ▼ (Phase 2)   ▼             ▼             ▼               │
│  ┌──────────┐  ┌──────────┐  ┌──────────┐  ┌──────────┐        │
│  │ LLM 분석 │  │ LLM 분석 │  │ LLM 분석 │  │ LLM 분석 │        │
│  │ (청크별) │  │ (청크별) │  │ (청크별) │  │ (청크별) │        │
│  └────┬─────┘  └────┬─────┘  └────┬─────┘  └────┬─────┘        │
└───────┼─────────────┼─────────────┼─────────────┼───────────────┘
        │             │             │             │
        └─────────────┴──────┬──────┴─────────────┘
                             │
                             ▼ (Cypher Lock 보호)
              ┌──────────────────────────────────┐
              │         Neo4j 그래프 저장         │
              └──────────────────────────────────┘
                             │
                             ▼
              ┌──────────────────────────────────┐
              │      User Story 문서 생성        │
              └──────────────────────────────────┘
```

## 환경변수

`env.example` 파일을 참조하세요. 주요 설정:

| 변수 | 설명 | 기본값 |
|------|------|--------|
| `NEO4J_URI` | Neo4j 연결 URI | `bolt://127.0.0.1:7687` |
| `NEO4J_USER` | Neo4j 사용자명 | `neo4j` |
| `NEO4J_PASSWORD` | Neo4j 비밀번호 | (필수) |
| `LLM_API_KEY` | LLM API 키 | (필수) |
| `LLM_MODEL` | 사용할 LLM 모델 | `gpt-4.1` |
| `FILE_CONCURRENCY` | 파일 병렬 처리 수 | `5` |
| `MAX_CONCURRENCY` | 청크 병렬 처리 수 | `5` |

## 생성되는 그래프 구조

### Framework (Java/Kotlin)

**노드 타입**:
- `CLASS`, `INTERFACE`, `ENUM` - 클래스 구조
- `METHOD`, `CONSTRUCTOR` - 메서드
- `FIELD`, `PARAMETER` - 필드/파라미터
- `UserStory`, `AcceptanceCriteria` - 요구사항

**관계 타입**:
- `EXTENDS`, `IMPLEMENTS` - 상속/구현
- `CALLS` - 메서드 호출
- `DEPENDENCY` - 의존성
- `ASSOCIATION`, `COMPOSITION` - 클래스 관계
- `PARENT_OF`, `NEXT` - 코드 구조

### DBMS (PL/SQL)

**노드 타입**:
- `PROCEDURE`, `FUNCTION`, `TRIGGER` - 프로시저
- `Table`, `Column` - 테이블 구조
- `Variable` - 변수

**관계 타입**:
- `CALL` - 프로시저 호출
- `FROM`, `WRITES` - 테이블 접근
- `HAS_COLUMN`, `FK_TO_TABLE` - 테이블 관계
- `PARENT_OF`, `NEXT` - 코드 구조

## 기여하기

1. 이 저장소를 Fork 합니다
2. 기능 브랜치를 생성합니다 (`git checkout -b feature/amazing-feature`)
3. 변경사항을 커밋합니다 (`git commit -m 'Add amazing feature'`)
4. 브랜치에 푸시합니다 (`git push origin feature/amazing-feature`)
5. Pull Request를 생성합니다

## 라이선스

MIT License
