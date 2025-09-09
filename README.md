### 개요

Legacy-Modernizer는 레거시 PL/SQL(패키지/프로시저)과 DDL을 이해(Understanding)하여 Neo4j 그래프로 시각화하고, 해당 비즈니스 로직을 기반으로 Spring Boot(Java) 코드 골격을 자동 생성하는 백엔드 서비스입니다.

- **Understanding**: SQL/PLSQL과 DDL을 해석해 Cypher 쿼리를 생성하고, Neo4j에 노드/관계를 구성하여 의존관계와 흐름을 그래프로 확인
- **Converting**: 해석된 결과를 바탕으로 Entity, Repository, Service, Controller, pom.xml, application.properties, Main 클래스를 순차적으로 생성(스트리밍)


### 주요 기능

- **/cypherQuery/**: 파일 정보를 받아 그래프를 구성하는 Cypher를 생성/실행하고, 그래프 데이터 스트림 반환
- **/springBoot/**: 서비스/컨트롤러 등 Java 프로젝트 파일을 단계별로 생성하여 스트리밍 반환
- **/downloadJava/**: 생성된 Java 프로젝트를 ZIP으로 압축해 다운로드 제공
- **/deleteAll/**: 특정 사용자(Session) 기준 임시 파일과 해당 Neo4j 데이터 정리


### 아키텍처 개요

- `service/service.py`: 핵심 비즈니스 플로우(Understanding/Converting/Zip/정리)
- `service/router.py`: FastAPI 라우팅(엔드포인트 정의)
- `understand/analysis.py`: PL/SQL 해석 로직(스트리밍 파이프라인)
- `understand/neo4j_connection.py`: Neo4j 비동기 드라이버 래퍼(쿼리 실행/그래프 반환/존재여부 확인)
- `convert/*`: Entity/Repository/Service/Controller/pom.xml/properties/Main 생성기
- `prompt/*`: LLM 프롬프트 템플릿(Understanding/Converting 단계별)
- `util/llm_client.py`: OpenAI 호환 LLM 클라이언트 생성 유틸(환경변수 기반)
- `test/test_legacy_modernizer.py`: 단계별/전체 플로우 테스트 러너


### 필수 요구사항

- **Python 3.12+**
- **Neo4j 5.x** (데스크톱/서버 모두 가능)
- (선택) Node.js: 별도 UI가 있을 경우 사용


### 환경 변수

아래 값들을 실행 환경에 맞게 설정하세요(.env 권장).

- Neo4j 연결
  - `NEO4J_URI` (예: bolt://localhost:7687)
  - `NEO4J_USER` (예: neo4j)
  - `NEO4J_PASSWORD`
- LLM(OpenAI 호환) 설정
  - `LLM_API_BASE` (예: https://api.openai.com/v1)
  - `LLM_API_KEY`
  - `LLM_MODEL` (예: gpt-4.1)
- 경로 설정(데이터/산출물 루트)
  - `DOCKER_COMPOSE_CONTEXT` (예: C:\uEngine\Deploy\data 또는 /app/data)
    - 설정 시 해당 경로 하위에 `data/<SESSION-UUID>` 및 `target/java/<SESSION-UUID>`를 사용합니다.

주의: 기본값이 동작하긴 하나, 현업에서는 반드시 위 환경 변수들을 명시적으로 설정하세요.


### 설치 및 실행(로컬)

1) 프로젝트 클론
```bash
git clone <repository-url>
cd Backend
```

2) 가상환경 및 의존성 설치
```bash
pip install pipenv
pipenv install
pipenv shell
```

3) 애플리케이션 실행
```bash
python main.py   # FastAPI on :5502
```


### 입력/출력 디렉터리 구조

서비스는 사용자별(Session-UUID)로 작업 디렉터리를 분리합니다.

- 데이터 입력: `<BASE>/data/<SESSION-UUID>/`
  - `src/`          : 원본 PL/SQL 파일(.sql)
  - `analysis/`     : 각 .sql과 같은 파일명의 ANTLR 분석 JSON(.json)
  - `ddl/`          : 스키마 DDL 파일(파일명에 ddl 포함 권장)
- 산출물 출력: `<BASE>/target/java/<SESSION-UUID>/<project_name>/`
- ZIP 다운로드: `<BASE>/data/<SESSION-UUID>/zipfile/<project_name>.zip`

여기서 `<BASE>`는 `DOCKER_COMPOSE_CONTEXT` 또는 런타임 기준 파생된 경로입니다. 일관된 경로 사용을 위해 `DOCKER_COMPOSE_CONTEXT` 설정을 강력 권장합니다.


### API 사용법(요약)

- 공통 헤더
  - `Session-UUID`: 사용자 세션 ID(필수)
  - `OpenAI-Api-Key` 또는 `Anthropic-Api-Key`: OpenAI 호환 키
    - 특수 값 `EN_TestSession`/`KO_TestSession` 사용 시 환경변수 `LLM_API_KEY` 참조
  - `Accept-Language`: `ko` 또는 `en` (기본: `ko`)

- 요청 바디 형식
  - 공통으로 `fileInfos: [{ "fileName": string, "objectName": string }]` 필요

엔드포인트

1) POST `/cypherQuery/`
   - 설명: 그래프 생성(Understanding) 진행 상황/그래프 데이터를 스트리밍
   - 응답: 바이트 청크 스트림(각 청크 끝에 `send_stream` 토큰)

2) POST `/springBoot/`
   - 설명: Entity → Repository → Service/Controller → pom.xml → properties → Main 순으로 생성하며 스트리밍 반환

3) POST `/downloadJava/`
   - 바디: `{ "projectName": "hospital" }`
   - 설명: 생성된 프로젝트를 ZIP으로 반환

4) DELETE `/deleteAll/`
   - 설명: 해당 `Session-UUID` 기준 임시 디렉터리 및 Neo4j 데이터 제거


### 예시 요청(cURL)

```bash
# 1) Understanding: 그래프 생성
curl -X POST http://localhost:5502/cypherQuery/ \
  -H "Session-UUID: KO_TestSession" \
  -H "OpenAI-Api-Key: $LLM_API_KEY" \
  -H "Accept-Language: ko" \
  -H "Content-Type: application/json" \
  -d '{
    "fileInfos": [
      {"fileName": "FN_DAYSUM_GENTIME.sql", "objectName": "PKG_SAMPLE"}
    ]
  }' --no-buffer

# 2) 프로젝트 생성(스트리밍)
curl -X POST http://localhost:5502/springBoot/ \
  -H "Session-UUID: KO_TestSession" \
  -H "OpenAI-Api-Key: $LLM_API_KEY" \
  -H "Accept-Language: ko" \
  -H "Content-Type: application/json" \
  -d '{
    "fileInfos": [
      {"fileName": "FN_DAYSUM_GENTIME.sql", "objectName": "PKG_SAMPLE"}
    ]
  }' --no-buffer

# 3) ZIP 다운로드
curl -X POST http://localhost:5502/downloadJava/ \
  -H "Session-UUID: KO_TestSession" \
  -H "Content-Type: application/json" \
  -d '{"projectName": "hospital"}' \
  --output hospital.zip

# 4) 전체 정리
curl -X DELETE http://localhost:5502/deleteAll/ \
  -H "Session-UUID: KO_TestSession"
```


### 개발 가이드(확장 포인트)

- PL/SQL 이해 로직: `understand/analysis.py`의 스트리밍 파이프라인을 통해 이벤트(큐) 기반으로 Cypher를 생성/실행합니다.
- Neo4j 접근: `understand/neo4j_connection.py`에서 모든 쿼리 실행/그래프 반환/존재여부 검사를 래핑합니다.
- 코드 생성기(Converting): `convert/` 디렉터리의 각 모듈이 단계를 담당합니다.
  - 엔티티: `create_entity.py`
  - 리포지토리: `create_repository.py`
  - 서비스 전/후처리 + 스켈레톤: `create_service_preprocessing.py`, `validate_service_preprocessing.py`, `create_service_postprocessing.py`, `create_service_skeleton.py`
  - 컨트롤러: `create_controller*.py`
  - 빌드파일: `create_pomxml.py`, `create_properties.py`, `create_main.py`
- LLM 설정 변경: `util/llm_client.py`에서 기본 모델/엔드포인트를 환경변수로 제어합니다.

변경 시 기존 로직 호환성을 위해 조건 분기 추가보다 “기존 로직을 대체”하는 방식으로 수정하세요.


### 테스트

통합 테스트 러너: `test/test_legacy_modernizer.py`

```bash
python -m unittest test/test_legacy_modernizer.py

# 모드/단계 지정 실행 예시
python test/test_legacy_modernizer.py --mode all
python test/test_legacy_modernizer.py --mode convert --step 4
```


### Docker(선택)

`Dockerfile`을 제공하며, 컨테이너 사용 시 `DOCKER_COMPOSE_CONTEXT`를 컨테이너 내 공유 볼륨(예: `/app/data`)로 지정하세요.

```bash
docker build -t legacy-modernizer-backend .
docker run -p 5502:5502 \
  -e NEO4J_URI=bolt://host.docker.internal:7687 \
  -e NEO4J_USER=neo4j -e NEO4J_PASSWORD=**** \
  -e LLM_API_BASE=https://api.openai.com/v1 \
  -e LLM_API_KEY=**** -e LLM_MODEL=gpt-4.1 \
  -e DOCKER_COMPOSE_CONTEXT=/app/data \
  -v $(pwd)/data:/app/data \
  legacy-modernizer-backend
```


### 트러블슈팅

- Neo4j 연결 오류: `NEO4J_*` 환경변수 확인 및 네트워크/포트(7687) 확인
- 그래프가 비어있음: `fileInfos`의 `objectName`이 실제 생성 노드의 `object_name`과 일치해야 조회됩니다.
- 스트리밍 파싱: 응답은 청크 끝에 `send_stream` 토큰이 붙습니다. 클라이언트에서 토큰 기준으로 청크를 분리/파싱하세요.
- 경로 문제: 일관성 유지를 위해 `DOCKER_COMPOSE_CONTEXT` 설정을 권장합니다.


### 실행 포트/헬스체크

- 기본 포트: `5502`
- 헬스 체크: `GET /` → `{ "status": "ok" }`


### 라이선스/문의

사내 프로젝트로 별도 라이선스가 적용될 수 있습니다. 문의: 담당자/레포지토리 이슈 트래커를 이용하세요.