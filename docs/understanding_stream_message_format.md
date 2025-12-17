## 스트림 메시지 구조 (Understanding & Converting)

UNDERSTANDING 및 CONVERTING 과정에서 클라이언트로 전송되는 모든 스트림 메시지의 구조를 정의합니다.

### 메시지 전송 형식

- 모든 메시지는 **NDJSON (Newline Delimited JSON)** 형식으로 전송됩니다.
- 각 JSON 객체는 `\n`으로 구분됩니다.
- UTF-8 인코딩을 사용합니다.

---

## 1. message 이벤트

**타입**: `message`  
**용도**: 현재 단계, 상태, 개략적인 진행 상황을 자연어(한글)로 전달

### 구조

```json
{
  "type": "message",
  "content": "메시지 내용"
}
```

### 메시지 작성 원칙

1. **단계 중심 설명**
   - “무엇을 하고 있는지”를 단계 단위로 설명  
   - 예: `1단계: 코드 구조를 분석하여 그래프를 구성하는 중입니다`
2. **사용자 친화적 표현**
   - 라인 번호, 내부 배치 수, 토큰 수 등 구현 세부 정보는 숨기고,  
     “파일 기준”, “구조 분석”, “AI 분석”, “후처리” 같은 개념만 노출
3. **대략적인 흐름만 전달**
   - 세밀한 퍼센트/라인 정보는 `message`로 보내지 않음  
   - 필요하다면 1단계/2단계/3단계처럼 **단계 수**로만 진행 정도를 표현
4. **일관된 패턴**
   - 시작: “무엇을 시작했는지”
   - 중간: “지금 어떤 단계에서 어떤 작업을 하는지”
   - 끝: “무엇이 얼마나 완료되었는지 (파일 개수, 블록 개수 등)”

> 참고: 라인 번호와 세밀한 진행률은 `data` 이벤트의 `line_number`, `analysis_progress` 필드에만 담고,  
> `message.content`에는 노출하지 않습니다.

---

## 2. data 이벤트

**타입**: `data`  
**용도**: Neo4j 그래프 데이터, 변환 결과, (필요 시) 진행률 정보 전달

### Understanding - 그래프 데이터

```json
{
  "type": "data",
  "graph": {
    "Nodes": [...],
    "Relationships": [...]
  },
  "line_number": 1234,
  "analysis_progress": 75,
  "current_file": "system_name-file_name.sql"
}
```

- `line_number`: 이 그래프 변경이 반영된 기준 라인(대략적인 위치)
- `analysis_progress`: 0~100 사이의 진행률 (파일 기준, 내부 알고리즘에 따라 계산)
- `current_file`: 현재 반영 중인 파일 식별자

### Converting - 생성된 코드

```json
{
  "type": "data",
  "file_type": "entity_class",
  "file_name": "User.java",
  "code": "public class User { ... }"
}
```

---

## 3. status 이벤트

**타입**: `status`  
**용도**: 단계별 진행 상태 (UI 진행 표시기용)

### 구조

```json
{
  "type": "status",
  "step": 1,
  "done": true
}
```

- `step`: 1부터 시작하는 단계 번호 (예: 1=구조 분석, 2=AI 분석, 3=후처리)
- `done`: 해당 단계 완료 여부

UI에서는 `message`의 자연어 설명과 `status.step`을 조합하여 단계 진행 표시를 구성할 수 있습니다.

---

## 4. error 이벤트

**타입**: `error`  
**용도**: 에러 발생 시 에러 정보 전달

### 구조

```json
{
  "type": "error",
  "content": "에러 메시지",
  "errorType": "ExceptionClassName",
  "traceId": "stream-abc12345"
}
```

---

## 5. complete 이벤트

**타입**: `complete`  
**용도**: 스트림 정상 종료 알림

### 구조

```json
{
  "type": "complete",
  "summary": "요약 정보"
}
```

`summary`에는 총 처리된 파일 수, 생성된 코드/노드 개수 등 최종 요약 정보를 넣을 수 있습니다.

---

## UNDERSTANDING 스트림 흐름 예시

### DBMS Understanding (프로시저/함수 분석)

```text
DBMS 코드 분석을 시작합니다
프로젝트 'my_project'의 2개 파일을 분석합니다
데이터베이스 연결이 완료되었습니다
테이블 스키마 정보 수집을 시작합니다 (DDL 파일 2개)
DDL 파일 처리 중: schema.sql (1/2)
DDL 파일 처리 중: tables.sql (2/2)
테이블 스키마 정보 수집이 완료되었습니다 (2개 파일)
프로시저 및 함수 코드 분석을 시작합니다 (2개 파일)

파일 분석 시작: test_data.sql (1/2)
시스템: sample
시스템 정보 등록 완료
소스 파일을 읽는 중입니다
파일 로딩이 완료되었습니다
구문 분석기를 준비하고 있습니다
1단계: 코드 구조를 분석하여 그래프 틀을 만드는 중입니다
  → 구조 생성 중... (2개 처리됨)
  → 구조 생성 중... (4개 처리됨)
  → 구조 생성 중... (6개 처리됨)
1단계 완료: 코드 구조 그래프가 생성되었습니다 (총 7개)
2단계: AI가 코드의 동작과 데이터 흐름을 분석합니다 (총 22개 블록)
  → AI 분석 중... (5/22)
  → AI 분석 중... (10/22)
  → AI 분석 중... (15/22)
  → AI 분석 중... (20/22)
  → AI 분석 중... (22/22)
파일별 코드 분석이 모두 끝났습니다 (구조 7개, AI 분석 22개 블록 처리)
이제 변수 타입을 테이블 메타데이터로 정리하고 있습니다
변수 타입 정리가 완료되었습니다
파일 분석 완료: test_data.sql (1/2)

파일 분석 시작: another.sql (2/2)
... (동일 패턴)
파일 분석 완료: another.sql (2/2)

DBMS 코드 분석이 모두 완료되었습니다 (총 2개 파일 처리)
ALL_ANALYSIS_COMPLETED
```

### Framework Understanding (Java 클래스 분석)

```text
프레임워크 코드 분석을 시작합니다
프로젝트 'my_project'의 3개 파일을 분석합니다
데이터베이스 연결이 완료되었습니다
클래스 및 인터페이스 구조 분석을 시작합니다 (3개 파일)

파일 분석 시작: UserService.java (1/3)
시스템: core
시스템 정보 등록 완료
소스 파일을 읽는 중입니다
파일 로딩이 완료되었습니다
구문 분석기를 준비하고 있습니다
1단계: 클래스와 메서드 구조를 그래프로 구성하는 중입니다
  → 구조 생성 중... (2개 처리됨)
  → 구조 생성 중... (4개 처리됨)
1단계 완료: 클래스 구조 그래프가 생성되었습니다 (총 5개)
2단계: AI가 비즈니스 로직을 분석합니다 (총 3개 블록)
  → AI 분석 중... (1/3)
  → AI 분석 중... (2/3)
  → AI 분석 중... (3/3)
파일별 코드 분석이 모두 끝났습니다 (구조 5개, AI 분석 3개 블록 처리)
파일 분석 완료: UserService.java (1/3)

... (계속)

프레임워크 코드 분석이 모두 완료되었습니다 (총 3개 파일 처리)
ALL_ANALYSIS_COMPLETED
```

---

## CONVERTING 스트림 흐름 예시

### Framework 변환 (Spring Boot)

```text
Spring Boot 프레임워크 변환을 시작합니다
프로젝트 'my_project'의 2개 파일을 변환합니다
엔티티 클래스 생성을 시작합니다
테이블 정보를 기반으로 JPA 엔티티를 생성하고 있습니다
엔티티 생성 완료: User.java (1/3)
엔티티 생성 완료: Order.java (2/3)
엔티티 생성 완료: Product.java (3/3)
엔티티 클래스 생성이 완료되었습니다 (총 3개)
리포지토리 인터페이스 생성을 시작합니다
데이터 접근 레이어 코드를 생성하고 있습니다
리포지토리 생성 완료: UserRepository.java (1/3)
리포지토리 생성 완료: OrderRepository.java (2/3)
리포지토리 생성 완료: ProductRepository.java (3/3)
리포지토리 생성이 완료되었습니다 (총 3개)
서비스 및 컨트롤러 생성을 시작합니다 (2개 파일)
파일 변환 시작: pkg_user (1/2)
시스템: sample
서비스 스켈레톤을 생성하고 있습니다
커맨드 클래스 2개를 생성하고 있습니다
커맨드 클래스 생성 완료: CreateUserCommand.java (1/2)
커맨드 클래스 생성 완료: UpdateUserCommand.java (2/2)
서비스 스켈레톤 생성이 완료되었습니다
AI가 비즈니스 로직을 변환하고 있습니다
서비스 메서드 생성 완료 (1/3)
서비스 메서드 생성 완료 (2/3)
서비스 메서드 생성 완료 (3/3)
컨트롤러 생성 완료: PkgUserController.java
파일 변환 완료: pkg_user (1/2)
... (계속)
설정 파일 및 메인 클래스를 생성하고 있습니다
설정 파일 생성 완료: pom.xml (1/2)
설정 파일 생성 완료: application.properties (2/2)
메인 클래스 생성 완료: MyProjectApplication.java
설정 파일 생성이 완료되었습니다 (2개 설정 파일 + 메인 클래스)
Spring Boot 프레임워크 변환이 모두 완료되었습니다
결과: 엔티티 3개, 리포지토리 3개, 서비스/컨트롤러 2개 파일, 설정 파일 3개
```

### DBMS 변환 (Oracle → PostgreSQL)

```text
DBMS 변환을 시작합니다 (대상: POSTGRESQL)
프로젝트 'my_project'의 1개 파일을 변환합니다
파일 변환 시작: test_data.sql (1/1)
시스템: sample
프로시저 정보를 조회하고 있습니다
발견된 프로시저: 3개
프로시저 변환 시작: create_user (1/3)
프로시저 구조를 분석하고 있습니다
프로시저 구조 분석이 완료되었습니다
POSTGRESQL 코드로 변환하고 있습니다
코드 변환이 완료되었습니다
프로시저 변환 완료: create_user (1/3)
프로시저 변환 시작: update_user (2/3)
프로시저 구조를 분석하고 있습니다
프로시저 구조 분석이 완료되었습니다
POSTGRESQL 코드로 변환하고 있습니다
코드 변환이 완료되었습니다
프로시저 변환 완료: update_user (2/3)
프로시저 변환 시작: delete_user (3/3)
프로시저 구조를 분석하고 있습니다
프로시저 구조 분석이 완료되었습니다
POSTGRESQL 코드로 변환하고 있습니다
코드 변환이 완료되었습니다
프로시저 변환 완료: delete_user (3/3)
파일 변환 완료: test_data.sql (1/1, 프로시저 3개)
DBMS 변환이 모두 완료되었습니다 (대상: POSTGRESQL)
결과: 1개 파일, 3개 프로시저 변환
```

### Architecture 변환 (클래스 다이어그램)

```text
클래스 다이어그램 생성을 시작합니다
프로젝트 'my_project'의 5개 클래스를 분석합니다
클래스 정보를 수집하고 있습니다
대상 클래스: core/UserService (1/5)
대상 클래스: core/UserRepository (2/5)
대상 클래스: core/User (3/5)
대상 클래스: web/UserController (4/5)
대상 클래스: dto/UserDto (5/5)
클래스 구조 및 관계를 분석하고 있습니다
AI가 Mermaid 다이어그램 코드를 생성하고 있습니다
분석이 완료되었습니다 (클래스 5개, 관계 8개)
클래스 다이어그램 생성이 완료되었습니다
결과: 클래스 5개, 관계 8개 (Mermaid 형식)
```

---

## 유틸리티 함수 참조

모든 메시지는 `util/utility_tool.py`의 함수들로 생성됩니다.

| 함수 | 용도 | 반환 |
|------|------|------|
| `emit_message(content)` | message 이벤트 생성 | bytes |
| `emit_data(**fields)` | data 이벤트 생성 | bytes |
| `emit_status(step, done)` | status 이벤트 생성 | bytes |
| `emit_error(content, ...)` | error 이벤트 생성 | bytes |
| `emit_complete(summary)` | complete 이벤트 생성 | bytes |
