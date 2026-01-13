# Neo4j 배치 처리 문제 해결 과정

> 이 문서는 Neo4j 쿼리 성능 개선 과정에서 겪은 문제들과 해결 시도를 정리한 것입니다.

---

## 📌 최초 문제: 너무 느린 Neo4j 저장

### 상황
- AST 분석 결과를 Neo4j에 저장할 때 **쿼리 하나씩 실행**
- 쿼리가 수백 개면 → 네트워크 왕복 수백 번 → **매우 느림**

### 목표
- 여러 쿼리를 **한 번에 묶어서** 실행하고 싶음
- 네트워크 호출 줄이기 = 성능 개선

---

## 🔄 해결 시도들

### 1️⃣ UNWIND 사용 (DDL/벡터) ✅ 성공

**시도:**
```cypher
UNWIND $items AS item
MERGE (t:Table {name: item.name})
SET t.description = item.description
```

**결과:** ✅ 성공!

**왜 됐나:**
- DDL 테이블, 벡터 임베딩은 **같은 구조**의 데이터
- 하나의 쿼리 템플릿 + 데이터 리스트 = 완벽

**한계:**
- AST 분석은 **다 다른 쿼리** (MERGE 노드, CREATE 관계 등)
- UNWIND로 못 묶음

---

### 2️⃣ CALL {} 배치 (AST 분석) ❌ 실패

**시도:**
```cypher
CALL { MERGE (n:FUNCTION {id: '1'}) RETURN n }
CALL { MERGE (m:TABLE {id: '2'}) RETURN m }
CALL { CREATE (n)-[:FROM]->(m) RETURN n }
```

**결과:** ❌ 실패!

**에러:**
```
CypherSyntaxError: Variable `n` already declared in outer scope
```

**원인:**
- 각 CALL 블록에서 `RETURN n` 하면 변수가 외부로 노출됨
- 다음 블록에서 같은 변수명 `n` 쓰면 충돌

---

### 3️⃣ 변수명 인덱싱 (n → n0, n1, n2) ❌ 실패

**시도:**
- 정규식으로 변수명에 숫자 붙이기
- `n` → `n0`, `n1`, `n2` ...

**결과:** ❌ 부분 실패

**문제 1: 문자열 리터럴 내부도 변경됨**
```cypher
SET n.code = 'variable n is used'
             ↑ 이것도 n0으로 바뀜!
```

**해결:** 문자열 리터럴을 임시 플레이스홀더로 치환 후 복원

**문제 2: 정규식이 너무 복잡해짐**
- 이스케이프된 따옴표 `\'` 처리
- 속성명 `n.name` vs 변수 `n` 구분
- 점점 버그 가능성 증가

---

### 4️⃣ 변수명 패턴 변경 (__cy_xxx__) ✅ 성공

**시도:**
- 모든 Cypher 변수명을 `__cy_변수명__` 패턴으로 변경
- 예: `n` → `__cy_n__`, `r` → `__cy_r__`

**결과:** ✅ 성공!

**장점:**
- 절대 다른 문자열과 충돌 안 함
- 정규식 변환 필요 없음
- 코드 단순해짐

**작업 범위:**
- `ast_processor.py`, `dbms_analyzer.py`, `router.py` 등
- robo-analyzer 전체 Cypher 쿼리 변경

---

### 5️⃣ CALL {} 재시도 ❌ 실패 (다른 이유)

**시도:**
- 변수명 충돌 해결했으니 CALL {} 다시 시도

**결과:** ❌ 실패!

**문제: 관계가 절반 이상 누락됨**
```
순차 실행: FROM 관계 120개
CALL {} 배치: FROM 관계 50개 ← 어디 갔어?!
```

**원인:**
```cypher
CALL { MERGE (n:FUNCTION {id: '1'}) }  -- 노드 생성
CALL { MERGE (m:TABLE {id: '2'}) }     -- 노드 생성
CALL { 
  MATCH (n:FUNCTION {id: '1'}), (m:TABLE {id: '2'})
  CREATE (n)-[:FROM]->(m)              -- ❌ n, m 못 찾음!
}
```

**각 CALL {} 블록은 서로 격리됨!**
- 첫 번째 블록에서 만든 노드를 세 번째 블록에서 **못 봄**
- 같은 트랜잭션이지만 블록 간에는 결과 공유 안 됨

---

### 6️⃣ APOC runMany 사용 ❌ 실패

**시도:**
```cypher
CALL apoc.cypher.runMany($queries, {})
```
- 세미콜론으로 구분된 쿼리들을 순차 실행
- 이전 쿼리 결과가 다음 쿼리에서 보임

**결과:** ❌ 실패!

**에러:**
```
Invalid input 'D': expected...
even number of non-escaped quotes
```

**원인:**
- `node_code` 속성에 PL/SQL 원문 코드가 들어감
- 원문에 작은따옴표 `'` 포함: `PROC_ID := 'TEST'`
- APOC이 문자열 파싱할 때 따옴표가 깨짐

---

### 7️⃣ 이중 이스케이프 ❌ 실패

**시도:**
```python
def escape_for_apoc(q: str) -> str:
    return q.replace("\\'", "''")
```
- Cypher에서 문자열 내 `'`는 `''`로 이스케이프

**결과:** ❌ 여전히 실패!

**원인:**
- 원문 코드가 너무 복잡함
- `'`, `\'`, 줄바꿸, 특수문자 등 다 섞여있음
- 이스케이프가 3중, 4중으로 꼬임
  - Python 문자열 → Cypher 문자열 → APOC 문자열 → 값

**결론:** 
> "값을 쿼리 문자열에 직접 넣는 건 한계가 있다"

---

### 8️⃣ 단일 트랜잭션 순차 실행 ✅ 최종 해결

**시도:**
```python
async with await session.begin_transaction() as tx:
    for query in queries:
        await tx.run(query)
    await tx.commit()
```

**결과:** ✅ 성공!

**왜 되나:**
- 모든 쿼리가 **같은 트랜잭션** 내에서 실행
- 첫 번째 쿼리에서 만든 노드가 두 번째 쿼리에서 **보임**
- 순서대로 실행 → 의존성 보장
- 쿼리 문자열 그대로 사용 → 이스케이프 문제 없음

**단점:**
- 네트워크 호출은 쿼리 수만큼 발생
- 하지만 트랜잭션 하나로 묶여서 원자성 보장

---

## 📊 최종 정리

| 방식 | 성능 | 의존성 보장 | 이스케이프 | 결과 |
|------|------|------------|-----------|------|
| UNWIND | ⭐⭐⭐ | N/A | 안전 | ✅ DDL/벡터에 사용 |
| CALL {} 배치 | ⭐⭐⭐ | ❌ 블록 격리 | 안전 | ❌ 관계 누락 |
| APOC runMany | ⭐⭐⭐ | ✅ | ❌ 터짐 | ❌ 따옴표 문제 |
| **단일 트랜잭션** | ⭐⭐ | ✅ | 안전 | ✅ **현재 사용** |

---

## 🚀 미래 개선 방향 (옵션 1)

### 현재 문제
```python
# 쿼리에 값을 직접 삽입 ← 이게 근본 원인
query = f"SET n.node_code = '{escape(code)}'"
```

### 정답: 파라미터 바인딩
```python
# 쿼리 템플릿 + 파라미터 분리
query = "SET n.node_code = $code"
params = {"code": code}  # 이스케이프 필요 없음!
```

### 리팩토링 필요
1. `ast_processor.py`: 쿼리 대신 **데이터 딕셔너리** 반환
2. `neo4j_client.py`: UNWIND로 **배치 처리**

```python
# ast_processor.py
def _build_node_data(self, node):
    return {
        "id": node.id,
        "label": "FUNCTION",
        "node_code": node.code,  # 그대로!
    }

# neo4j_client.py
query = """
UNWIND $nodes AS n
CALL apoc.merge.node([n.label], {id: n.id}, n) YIELD node
RETURN node
"""
await session.run(query, {"nodes": nodes})
```

**장점:**
- 네트워크 1번
- 이스케이프 없음
- 가장 빠르고 안전

---

## 📝 교훈

1. **값은 쿼리 문자열에 직접 넣지 마라** → 파라미터로
2. **CALL {} 블록은 서로 격리된다** → 의존성 있으면 쓰지 마라
3. **APOC runMany + 원문 텍스트 = 💥** → 궁합 안 맞음
4. **급할 땐 단일 트랜잭션 순차** → 안전하게 작동
5. **장기적으론 UNWIND + 파라미터** → 정답

