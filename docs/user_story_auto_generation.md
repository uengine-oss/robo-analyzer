# 📖 User Story 자동 생성 기능

> 코드를 분석하면 비즈니스 요구사항 문서가 자동으로 만들어집니다

---

## 🔧 어떻게 동작하나요?

### 전체 흐름

```
1. 코드 분석
   ↓
2. 상세 요약 생성
   ↓
3. User Story 도출
   ↓
4. Acceptance Criteria 생성
   ↓
5. 마크다운 문서 출력
```

### 단계별 작업 내용

#### 1단계: 코드 분석

**작업:**
- 각 함수/프로시저/클래스를 개별적으로 분석
- 조건문, 분기, 예외 처리까지 모두 파악
- 실제 코드에 나온 값(상태값, 조건값) 추출

**예시:**
```sql
IF v_customer_grade = 'VIP' THEN
    v_discount := 0.15;
ELSIF v_customer_grade = 'GOLD' THEN
    v_discount := 0.10;
END IF;
```

**분석 결과:**
- VIP 고객은 15% 할인
- GOLD 고객은 10% 할인
- 조건: 고객 등급에 따라 다름

#### 2단계: 상세 요약 생성

**작업:**
- 각 블록의 동작을 3-5문장으로 상세히 요약
- 분기 조건과 비즈니스 규칙 포함
- 데이터 값과 상수 포함

**요약 예시:**
```
이 프로시저는 주문 결제 처리를 담당합니다. 
주문 상태가 'PENDING'이면 결제를 시작하고, 
'CONFIRMED'면 중복 결제 예외를 발생시킵니다. 
고객 등급에 따라 VIP는 15%, GOLD는 10%, SILVER는 5% 할인이 적용됩니다.
```

#### 3단계: User Story 도출

**작업:**
- 프로시저/클래스 전체 요약을 종합
- 비즈니스 관점에서 "누가, 무엇을, 왜" 형식으로 변환

**User Story 예시:**
```
As a 온라인 쇼핑몰 고객
I want 신용카드로 결제하고 확인을 받는다
So that 주문이 확정되고 배송이 시작된다
```

#### 4단계: Acceptance Criteria 생성

**작업:**
- Given-When-Then 형식으로 테스트 케이스 작성
- 정상 케이스, 실패 케이스, 경계 케이스 모두 포함

**Acceptance Criteria 예시:**
```
AC-1-1. VIP 고객 신용카드 결제 성공

Given:
- 고객 등급이 'VIP'이다
- 주문 상태가 'PENDING'이다
- 주문 금액이 100,000원이다

When:
- 신용카드 결제 방식을 선택한다
- 결제 버튼을 클릭한다

Then:
- VIP 할인 15%가 적용되어 결제 금액은 85,000원이다
- 주문 상태가 'CONFIRMED'로 변경된다
```

#### 5단계: 마크다운 문서 출력

**작업:**
- 모든 User Story를 하나의 마크다운 문서로 통합
- ID 자동 할당 (US-1, US-2, ...)
- SSE 이벤트로 전송

---

## 📊 생성되는 문서 형식

### 마크다운 구조

```markdown
# 프로젝트명 - User Stories & Acceptance Criteria

## US-1. VIP 고객 할인 적용

**As a** VIP 등급 고객
**I want** 주문 시 15% 할인을 받는다
**So that** 우대 혜택을 누릴 수 있다

### Acceptance Criteria

#### AC-1-1. VIP 할인 성공
- **Given**: 고객 등급이 'VIP'이다
- **When**: 결제를 진행한다
- **Then**: 15% 할인이 적용된다

#### AC-1-2. GOLD 할인 성공
- **Given**: 고객 등급이 'GOLD'이다
- **When**: 결제를 진행한다
- **Then**: 10% 할인이 적용된다
```

---

## 🎨 품질 향상을 위한 기술

### 1. 상세한 코드 분석

**작업 내용:**
- `analysis.yaml` 프롬프트에서 상세 요약 규칙 정의
- 조건값, 상태값, 상수값을 그대로 포함하도록 지시

**프롬프트 예시:**
```yaml
요약 작성 규칙:
- 3~5문장으로 상세하게 작성
- 코드에 나온 실제 값(상태값, 조건값)을 포함

★ 반드시 포함해야 할 내용:
1. 분기 조건과 비즈니스 규칙 (조건값 포함)
2. 데이터 값과 상수 (상태값, 코드값)
3. 반복 처리 대상과 목적
4. 데이터베이스 작업 상세
5. 예외 처리
```

### 2. 정확한 메서드 호출 추적 (Java 전용)

**작업 내용:**
- 필드 타입을 분석하여 Collection/Map 타입 필드의 메서드 호출 제외
- 실제 비즈니스 로직 호출만 추적

**처리 방법:**
```python
# 필드 타입 캐시에 저장
field_type_cache = {
    'OrderService': {
        'orderService': 'OrderService',
        'salesMap': 'Map<String, String>'
    }
}

# 메서드 호출 필터링
if field_type.startswith(('Map<', 'List<', 'Set<')):
    # Collection 조작은 제외
    continue
else:
    # 비즈니스 로직 호출은 추적
    create_calls_relationship()
```

**예시:**
```
private Map<String, String> salesMap;     → put(), get() 제외 ✅
private OrderService orderService;       → findAll(), save() 추적 ✅
```

### 3. 대용량 코드 처리

**작업 내용:**
- 프로시저/클래스가 너무 크면 자동으로 청크(조각)로 분할
- 각 청크를 순차적으로 요약하여 정보 손실 없음

**처리 방법:**
```python
MAX_SUMMARY_CHUNK_SIZE = 50  # 청크당 최대 블록 수

if summary_count > MAX_SUMMARY_CHUNK_SIZE:
    # 청크로 나누기
    chunks = split_into_chunks(summaries, MAX_SUMMARY_CHUNK_SIZE)
    
    previous_summary = ""
    for chunk in chunks:
        # 이전 요약을 컨텍스트로 포함
        result = summarize(chunk, previous_summary)
        previous_summary = result.summary
        user_stories.extend(result.user_stories)
```

---

## 📁 지원 언어

| 언어 | 분석 단위 | 프롬프트 파일 |
|------|----------|--------------|
| **PL/SQL, Oracle** | 프로시저, 함수, 트리거 | `procedure_summary.yaml` |
| **Java, Kotlin** | 클래스, 인터페이스 | `class_summary.yaml` |

---

## 🚀 사용 방법

### 1. 코드 분석 실행

API를 호출하거나 UI에서 프로젝트를 업로드합니다:

```javascript
POST /api/understand
{
  "project_name": "my-project",
  "file_names": ["file1.sql", "file2.java"]
}
```

### 2. 분석 완료 대기

SSE 스트림에서 진행 상황을 확인합니다:

```javascript
eventSource.onmessage = (event) => {
  const data = JSON.parse(event.data);
  
  if (data.type === 'message' && data.content.includes('User Story')) {
    console.log('User Story 생성 중...');
  }
};
```

### 3. 문서 수신

분석 완료 시 `user_story_document` 필드로 문서를 받습니다:

```javascript
if (data.type === 'data' && data.user_story_document) {
  // 마크다운 문서 저장
  saveMarkdownFile(data.user_story_document);
  
  // 또는 화면에 표시
  displayUserStoryDocument(data.user_story_document);
}
```

---

## 🔍 기술 상세

### 데이터 흐름

```
코드 파일
  ↓
AST 파싱 (ANTLR)
  ↓
블록 단위 분석 (analysis.yaml)
  ↓
상세 요약 생성
  ↓
프로시저/클래스 요약 (procedure_summary.yaml / class_summary.yaml)
  ↓
User Story + AC 도출
  ↓
Neo4j 저장
  ↓
문서 생성 (utility_tool.py)
  ↓
SSE 이벤트로 전송
```

### 주요 파일

| 파일 | 역할 |
|------|------|
| `analysis.yaml` | 개별 블록의 상세 요약 생성 |
| `procedure_summary.yaml` | 프로시저 전체 요약 + User Story 도출 |
| `class_summary.yaml` | 클래스 전체 요약 + User Story 도출 |
| `utility_tool.py` | 문서 생성 유틸리티 함수 |

### Neo4j 저장 구조

```cypher
MATCH (n:PROCEDURE {procedure_name: 'CREATE_ORDER'})
SET n.summary = '상세한 요약...',
    n.user_stories = [
      {
        "id": "US-1",
        "role": "온라인 쇼핑몰 고객",
        "goal": "주문 배송 상태를 확인",
        "benefit": "배송 진행 상황 예측",
        "acceptance_criteria": [
          {
            "id": "AC-1-1",
            "title": "배송 상태 조회 성공",
            "given": ["주문번호가 존재한다"],
            "when": ["배송 상태를 조회한다"],
            "then": ["배송 상태가 반환된다"]
          }
        ]
      }
    ]
```

---

## ⚙️ 설정

| 환경변수 | 기본값 | 설명 |
|----------|--------|------|
| `MAX_SUMMARY_CHUNK_SIZE` | 50 | 청크당 최대 블록 수 |
| `MAX_CONCURRENCY` | 5 | 병렬 처리 수 |

---

## 💡 활용 예시

### 신규 개발자 온보딩

1. 코드 분석 실행
2. User Story 문서 확인
3. 비즈니스 로직 이해

### 테스트 케이스 작성

1. User Story 문서에서 Acceptance Criteria 추출
2. Given-When-Then 형식으로 테스트 시나리오 작성
3. 테스트 코드 작성

### 요구사항 문서화

1. 코드 분석 실행
2. User Story 문서를 요구사항 문서로 활용
3. 프로젝트 문서에 포함
