# User Story & Acceptance Criteria 추출 기능

> Understanding 모듈에서 코드 분석 시 User Story와 Acceptance Criteria를 자동으로 도출하는 기능

---

## 📋 개요

### 배경
- 코드 분석 과정에서 생성되는 summary를 활용하여 User Story와 AC를 자동 추출
- 프로시저/클래스 요약 시점에서 비즈니스 관점의 요구사항 문서화

### 목표
- 상세하고 테스트 가능한 User Story + AC 도출
- 최종 결과물을 마크다운 문서로 제공

---

## 🏗️ 아키텍처

### 데이터 흐름

```
┌─────────────────────────────────────────────────────────────┐
│                        DDL 처리                              │
│  → 테이블/컬럼 메타데이터 Neo4j 저장                          │
└─────────────────────────────────────────────────────────────┘
                              ↓
┌─────────────────────────────────────────────────────────────┐
│                    개별 블록 분석                            │
│  → 상세 summary (분기조건, 데이터값, 비즈니스 규칙 포함)      │
│  → analysis.yaml 프롬프트 사용                              │
└─────────────────────────────────────────────────────────────┘
                              ↓
┌─────────────────────────────────────────────────────────────┐
│                  프로시저/클래스 요약                        │
│  → 상세 summary들 종합                                      │
│  → User Story + AC 도출                                     │
│  → procedure_summary.yaml / class_summary.yaml 사용        │
└─────────────────────────────────────────────────────────────┘
                              ↓
┌─────────────────────────────────────────────────────────────┐
│                    후처리 (DBMS만)                          │
│  → variable_type_resolve (%TYPE, %ROWTYPE 해석)            │
└─────────────────────────────────────────────────────────────┘
                              ↓
┌─────────────────────────────────────────────────────────────┐
│                  User Story 문서 생성                        │
│  → 전체 프로젝트의 User Story 집계                          │
│  → 마크다운 문서 출력                                       │
└─────────────────────────────────────────────────────────────┘
```

---

## 📁 수정된 파일 목록

### 프롬프트 파일

| 파일 | 변경 내용 |
|------|----------|
| `rules/understand/dbms/analysis.yaml` | summary 섹션 상세화 (분기조건, 데이터값, 비즈니스 규칙 포함) |
| `rules/understand/framework/analysis.yaml` | summary 섹션 상세화 |
| `rules/understand/dbms/procedure_summary.yaml` | User Story + AC 도출 로직 추가 (266줄) |
| `rules/understand/framework/class_summary.yaml` | User Story + AC 도출 로직 추가 (215줄) |

### 소스코드 파일

| 파일 | 변경 내용 |
|------|----------|
| `understand/strategy/dbms/analysis.py` | 청크 분할 처리, User Story Neo4j 저장, `understand_summary` 함수 확장 |
| `understand/strategy/framework/analysis.py` | 청크 분할 처리, User Story Neo4j 저장, `understand_class_summary` 함수 확장 |
| `understand/strategy/dbms_strategy.py` | User Story 문서 생성 호출, `_generate_user_story_document` 메서드 추가 |
| `understand/strategy/framework_strategy.py` | User Story 문서 생성 호출, `_generate_user_story_document` 메서드 추가 |
| `util/utility_tool.py` | `generate_user_story_document`, `aggregate_user_stories_from_results` 함수 추가 |

---

## 🔧 주요 변경 사항

### 1. 개별 블록 Summary 상세화 (analysis.yaml)

**변경 전:**
```
요약 작성 규칙:
- 2~3문장으로 핵심 동작 설명
```

**변경 후:**
```yaml
요약 작성 규칙:
- 3~5문장으로 상세하게 작성
- 코드에 나온 실제 값(상태값, 조건값, 상수)을 포함

★ 반드시 포함해야 할 내용:
1. 분기 조건과 비즈니스 규칙 (조건값 포함)
2. 데이터 값과 상수 (상태값, 코드값)
3. 반복 처리 대상과 목적
4. 데이터베이스 작업 상세
5. 예외 처리
```

### 2. 프로시저/클래스 요약 프롬프트 (procedure_summary.yaml, class_summary.yaml)

**새로운 구조:**
```yaml
[TASK_1: 상세 요약]
- 최소 10문장 이상
- 6가지 필수 포함 항목 (상세 예시 포함)

[TASK_2: USER STORY 도출]
- 구체적인 role, goal, benefit 작성 규칙
- 좋은/나쁜 예시 대비

[TASK_3: ACCEPTANCE CRITERIA 도출]
- Given/When/Then 상세 예시
- 정상/실패/경계 케이스 시나리오
```

### 3. 대용량 Summary 청크 분할 처리

```python
MAX_SUMMARY_CHUNK_SIZE = 50  # 환경변수로 설정 가능

# summary 개수가 50개 초과 시 자동 분할
if total_count > MAX_SUMMARY_CHUNK_SIZE:
    chunks = [dict(summary_items[i:i + MAX_SUMMARY_CHUNK_SIZE]) 
              for i in range(0, total_count, MAX_SUMMARY_CHUNK_SIZE)]
    
    for chunk in chunks:
        chunk_result = await understand_summary(chunk, api_key, locale, previous_summary)
        previous_summary = chunk_result.get('summary', '')
        accumulated_user_stories.extend(chunk_result.get('user_stories', []))
```

### 4. Neo4j 저장

```cypher
MATCH (n:PROCEDURE {procedure_name: 'proc_name', ...})
SET n.summary = '상세한 요약...',
    n.user_stories = [
      {
        "id": "US-1",
        "role": "온라인 쇼핑 고객",
        "goal": "주문 배송 상태를 확인",
        "benefit": "배송 진행 상황 예측",
        "acceptance_criteria": [...]
      }
    ]
RETURN n
```

### 5. 문서 생성 유틸리티

```python
# util/utility_tool.py

def generate_user_story_document(user_stories, source_name, source_type):
    """User Story와 AC를 마크다운 문서로 변환"""
    
def aggregate_user_stories_from_results(results):
    """여러 분석 결과에서 User Story 집계 및 ID 재할당"""
```

---

## 📄 출력 문서 형식

### 마크다운 예시

```markdown
# 프로젝트명 - User Stories & Acceptance Criteria

> DBMS 프로시저/함수에서 도출된 사용자 스토리 및 인수 조건

---

## US-1

**As a** 온라인 쇼핑몰 고객

**I want** 신용카드 또는 계좌이체로 주문 금액을 결제하고 결제 완료 확인을 받는다

**So that** 결제가 즉시 처리되어 주문이 확정되고, 빠른 배송 준비가 시작된다

### Acceptance Criteria

#### AC-1-1. VIP 고객 신용카드 결제 성공

**Given**
- 사용자가 로그인된 상태이다
- 주문 상태가 'PENDING'이다
- 고객 등급이 'VIP'이다
- 주문 금액이 100,000원이다

**When**
- 사용자가 '신용카드' 결제 방식을 선택한다
- 결제 버튼을 클릭한다

**Then**
- VIP 할인 15%가 적용되어 결제 금액은 85,000원이다
- 카드사 승인이 성공한다
- 결과 코드 '00'(성공)이 반환된다
- 주문 상태가 'CONFIRMED'로 변경된다
- 850포인트가 적립된다

---

#### AC-1-2. 잔액 부족으로 결제 실패

**Given**
- 주문 금액이 500,000원이다
- 카드 잔액이 300,000원이다

**When**
- 결제 요청을 보낸다

**Then**
- 결과 코드 '01'(잔액 부족)이 반환된다
- 주문 상태가 'PENDING' 그대로 유지된다
- "잔액이 부족합니다" 메시지가 표시된다

---
```

---

## ⚙️ 환경 변수

| 변수명 | 기본값 | 설명 |
|--------|--------|------|
| `MAX_SUMMARY_CHUNK_SIZE` | 50 | 청크 분할 기준 (summary 개수) |
| `MAX_CONCURRENCY` | 5 | 병렬 처리 동시성 |

---

## 🔄 SSE 이벤트

### 새로운 이벤트

```json
{
  "type": "data",
  "current_file": "user_stories.md",
  "user_story_document": "# 프로젝트명 - User Stories...",
  "analysis_progress": 100
}
```

### 메시지 흐름

```
"User Story 문서를 생성하고 있습니다..."
"USER_STORY_DOCUMENT"
"User Story 문서 생성이 완료되었습니다"
"ALL_ANALYSIS_COMPLETED"
```

---

## 📊 결과물 품질 비교

### 이전 (추상적)

```json
{
  "summary": "주문을 처리하고 결제를 진행합니다."
}
```

### 이후 (상세)

```json
{
  "summary": "이 프로시저는 주문 결제 처리를 담당합니다. 입력으로 주문번호(p_order_id)와 결제방식(p_payment_type)을 받습니다. 주문 상태(v_order_status)에 따라 'PENDING'이면 결제를 시작하고, 'CONFIRMED'면 중복 결제 예외를 발생시킵니다. 고객 등급에 따라 VIP는 15%, GOLD는 10%, SILVER는 5% 할인이 적용됩니다. 결제 성공 시 ORDER_MASTER.STATUS를 'CONFIRMED'로 변경하고, PAYMENT_HISTORY에 결제 이력을 INSERT합니다. NO_DATA_FOUND 예외 발생 시 결과코드 '10'을 반환합니다.",
  "user_stories": [
    {
      "id": "US-1",
      "role": "온라인 쇼핑몰 고객",
      "goal": "신용카드 또는 계좌이체로 주문 금액을 결제하고 결제 완료 확인을 받는다",
      "benefit": "결제가 즉시 처리되어 주문이 확정되고, 빠른 배송 준비가 시작된다",
      "acceptance_criteria": [
        {
          "id": "AC-1-1",
          "title": "VIP 고객 신용카드 결제 성공",
          "given": ["주문 상태가 'PENDING'", "고객 등급이 'VIP'", "주문 금액 100,000원"],
          "when": ["신용카드 결제 선택", "결제 버튼 클릭"],
          "then": ["15% 할인 적용, 결제 금액 85,000원", "결과코드 '00' 반환", "주문 상태 'CONFIRMED'로 변경"]
        },
        {
          "id": "AC-1-2",
          "title": "잔액 부족으로 결제 실패",
          "given": ["카드 잔액 300,000원", "주문 금액 500,000원"],
          "when": ["결제 요청"],
          "then": ["결과코드 '01' 반환", "주문 상태 'PENDING' 유지"]
        }
      ]
    }
  ]
}
```

---

## 🚀 사용 방법

1. 기존 Understanding 파이프라인 실행
2. 분석 완료 시 자동으로 User Story 문서 생성
3. SSE의 `user_story_document` 필드에서 마크다운 문서 수신
4. 필요 시 파일로 저장 또는 UI에 표시

---

## 📝 관련 문서

- [Understanding 모듈 가이드](../understanding.md)
- [프롬프트 엔지니어링 가이드](../converting.md)

