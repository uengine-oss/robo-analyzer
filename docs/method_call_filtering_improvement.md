# METHOD_CALL 필터링 개선 작업

## 작업 개요

Java 코드 분석에서 METHOD_CALL 노드 처리 시, Collection/Map 조작 메서드 호출을 정확하게 필터링하도록 개선했습니다.

### 문제점

기존 방식은 프롬프트 레벨에서 Map/Collection 조작 메서드를 제외하려고 시도했으나, 다음과 같은 문제가 있었습니다:

1. **LLM의 타입 정보 부족**: LLM은 코드만 보고 필드 타입을 정확히 알 수 없음
2. **오탐 문제**: 
   - `salesMap.put()` (Map 조작) → 제외해야 함 ✅
   - `myObject.put(data)` (사용자 정의 메서드) → 포함해야 함 ❌
   - LLM은 둘을 구분하지 못함
3. **빈 결과 구분 불가**: 빈 결과가 에러인지, 제외 대상인지 구분 불가

## 해결 방안

**필드 타입 기반 코드 레벨 필터링** 방식으로 변경했습니다.

### 핵심 아이디어

이미 분석된 필드 정보(`field_type`)를 활용하여, Collection/Map 타입 필드의 메서드 호출만 제외합니다.

```
private Map<String, String> salesMap;   → field_type: "Map<String, String>"
private OrderService orderService;       → field_type: "OrderService"
private MyClass myObject;                → field_type: "MyClass"
```

이렇게 하면:
- `salesMap.put()` → salesMap의 타입이 `Map<...>` → **제외** ✅
- `orderService.findAll()` → 타입이 `OrderService` → **CALLS 관계 생성** ✅
- `myObject.put()` → 타입이 `MyClass` → **CALLS 관계 생성** ✅

## 구현 내용

### 1. 프롬프트 단순화

**파일**: `rules/understand/framework/method_call.yaml`

- **변경 전**: Map/Collection 조작 메서드를 제외하도록 프롬프트에 규칙 명시
- **변경 후**: 모든 메서드 호출을 수집하도록 변경 (필터링은 코드 레벨에서 처리)

```yaml
[RULES]
각 METHOD_CALL에서 다음을 추출:
- targetClass: 호출 대상 (점 앞 부분 - 변수명 또는 클래스명)
- methodName: 메서드명 (점 뒤 부분)

⚠️ 모든 메서드 호출을 수집합니다 (필터링은 코드 레벨에서 처리).
```

### 2. Collection 타입 상수 추가

**파일**: `understand/strategy/framework/analysis.py`

Collection/Map 타입 프리픽스를 상수로 정의:

```python
# Collection/Map 타입 프리픽스 - 필드 타입 기반 method_call 필터링용
COLLECTION_TYPE_PREFIXES = (
    # Map 계열
    "Map<", "HashMap<", "LinkedHashMap<", "TreeMap<", "ConcurrentHashMap<",
    "Hashtable<", "WeakHashMap<", "IdentityHashMap<", "EnumMap<",
    # List 계열
    "List<", "ArrayList<", "LinkedList<", "CopyOnWriteArrayList<", "Vector<",
    # Set 계열
    "Set<", "HashSet<", "TreeSet<", "LinkedHashSet<", "EnumSet<",
    "ConcurrentSkipListSet<", "CopyOnWriteArraySet<",
    # 기타 Collection 계열
    "Collection<", "Queue<", "Deque<", "Stack<", "PriorityQueue<",
    "ArrayDeque<", "ConcurrentLinkedQueue<", "BlockingQueue<",
)
```

### 3. 필드 타입 캐시 추가

**파일**: `understand/strategy/framework/analysis.py` - `ApplyManager` 클래스

```python
# 필드 타입 캐시: class_key → {field_name: field_type}
# Collection/Map 타입 필드의 메서드 호출 필터링에 사용
self._field_type_cache: Dict[str, Dict[str, str]] = {key: {} for key in classes}
```

### 4. 필드 분석 시 캐시 업데이트

**파일**: `understand/strategy/framework/analysis.py` - `_build_field_queries` 메서드

```python
# 필드 타입 캐시 업데이트 (Collection/Map 필터링용)
if node.class_key and node.class_key in self._field_type_cache:
    # escape 전 원본 필드명과 타입 저장
    original_field_name = field_info.get("field_name") or ""
    self._field_type_cache[node.class_key][original_field_name] = field_type_raw
```

### 5. METHOD_CALL 필터링 로직 적용

**파일**: `understand/strategy/framework/analysis.py` - `_build_method_call_queries` 메서드

```python
# 필드 타입 기반 Collection/Map 필터링
# target_class가 현재 클래스의 필드명이고, 해당 필드가 Collection/Map 타입이면 제외
class_key = parent_node.class_key
if class_key and class_key in self._field_type_cache:
    field_types = self._field_type_cache[class_key]
    if target_class in field_types:
        field_type = field_types[target_class]
        if field_type.startswith(COLLECTION_TYPE_PREFIXES):
            log_process(
                "UNDERSTAND", "APPLY", 
                f"⚠️ METHOD_CALL 제외 (Collection 필드): {target_class}({field_type}).{method_name}"
            )
            continue
```

## 필터링 흐름

```
METHOD_CALL 노드 수집
    ↓
LLM이 모든 메서드 호출 수집 (필터링 없음)
    ↓
_build_method_call_queries에서 필터링:
    1. targetClass 빈 값 체크
    2. _is_valid_class_name_for_calls() 체크 (표준 라이브러리, 짧은 변수명 제외)
    3. 필드 타입 캐시 확인:
       - target_class가 필드명인가?
       - 해당 필드의 타입이 Collection/Map 프리픽스로 시작하는가?
       → YES면 제외
    4. 실제 존재하는 클래스에만 CALLS 관계 생성 (MATCH 쿼리)
```

## 동작 예시

| 코드 | 필드 타입 | 1차 필터<br/>(_is_valid_class_name) | 2차 필터<br/>(필드 타입) | 결과 |
|------|----------|-------------------------------------|--------------------------|------|
| `salesMap.put("k", "v")` | `Map<String, String>` | ✅ 통과 (8글자, camelCase) | ❌ 제외 (Map< 시작) | ❌ 제외 |
| `items.add(item)` | `List<Item>` | ✅ 통과 (5글자) | ❌ 제외 (List< 시작) | ❌ 제외 |
| `orderService.findAll()` | `OrderService` | ✅ 통과 | ✅ 통과 | ✅ CALLS 생성 |
| `myObject.put(data)` | `MyClass` | ✅ 통과 | ✅ 통과 (MyClass는 Collection 아님) | ✅ CALLS 생성 |
| `System.out.println()` | (필드 아님) | ❌ 제외 (System은 표준 라이브러리) | - | ❌ 제외 |
| `Math.max()` | (필드 아님) | ❌ 제외 (Math는 표준 라이브러리) | - | ❌ 제외 |

## 기존 필터와의 관계

### `_is_valid_class_name_for_calls()` vs 필드 타입 필터링

| 필터 | 판단 기준 | 예시 | 결과 |
|------|----------|------|------|
| `_is_valid_class_name_for_calls` | **이름 형태** (길이, 대소문자) | `salesMap.put()` | ✅ 통과 (8글자, camelCase) |
| **필드 타입 필터링** | **실제 타입** (`Map<String, String>`) | `salesMap.put()` | ❌ 제외 |

**둘 다 필요한 이유:**
1. `_is_valid_class_name_for_calls` → 빠른 1차 필터 (표준 라이브러리, 짧은 변수명)
2. 필드 타입 필터링 → 정확한 2차 필터 (Collection/Map 타입 필드 제외)

## 장점

1. **정확성**: 실제 필드 타입 기반 판단으로 오탐 최소화
2. **명확성**: 로그에서 제외 이유가 명확히 표시됨
3. **성능**: LLM 부담 감소 (단순 수집만 수행)
4. **디버깅 용이**: 빈 결과가 에러인지 의도적 제외인지 구분 가능

## 변경 파일 목록

1. `rules/understand/framework/method_call.yaml` - 프롬프트 단순화
2. `understand/strategy/framework/analysis.py` - 필드 타입 캐시 및 필터링 로직 추가

## 버전

- method_call.yaml: `1.0` → `1.1`

