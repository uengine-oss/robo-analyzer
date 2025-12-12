# Framework 클래스 다이어그램 관계 처리 가이드

## 개요

Java 코드를 분석하여 클래스 다이어그램에 필요한 관계들을 Neo4j 그래프로 구축하는 방법을 설명합니다.

---

## 타겟 소스코드 예시

### Main.java
```java
public class Main {
    public static void main(String[] args) {
        Weapon sword = new Weapon("불꽃검", 50);
        Enemy enemy = new Enemy("고블린");
        
        sword.attack(enemy);
        enemy.takeDamage(sword.getPower());
    }
}
```

### Enemy.java
```java
public class Enemy {
    private String name;
    private int hp = 100;

    public Enemy(String name) {
        this.name = name;
    }

    public String getName() {
        return name;
    }

    public void takeDamage(int damage) {
        hp -= damage;
        System.out.println(name + "은(는) " + damage + "의 피해를 입고, 남은 체력: " + hp);
    }
}
```

### Weapon.java
```java
public class Weapon {
    private String name;
    private int power;

    public Weapon(String name, int power) {
        this.name = name;
        this.power = power;
    }

    public void attack(Enemy target) {
        target.takeDamage(power);
    }

    public int getPower() {
        return power;
    }
}
```

### Character.java (상속 예시)
```java
public abstract class Character {
    protected String name;
    protected int hp;
    
    public abstract void attack();
}
```

### Player.java (상속 예시)
```java
public class Player extends Character implements Attackable {
    private Weapon weapon;
    
    @Override
    public void attack() {
        weapon.attack(null);
    }
}
```

---

## 관계 유형별 처리 방법

### 1. 상속 관계 (EXTENDS)

**소스코드:**
```java
public class Player extends Character {
```

**LLM 분석 결과:**
```json
{
  "relations": [
    {
      "toType": "Character",
      "relationType": "EXTENDS",
      "toTypeKind": "CLASS"
    }
  ]
}
```

**Neo4j 쿼리:**
```cypher
MATCH (src:CLASS {startLine: 1, folder_name: 'sample', file_name: 'Player.java', ...})
OPTIONAL MATCH (existing)
WHERE (existing:CLASS OR existing:INTERFACE)
  AND toLower(existing.class_name) = toLower('Character')
  AND existing.user_id = 'TestSession'
  AND existing.project_name = 'testjava'
WITH src, existing
FOREACH(_ IN CASE WHEN existing IS NULL THEN [1] ELSE [] END |
    CREATE (:CLASS:INTERFACE {class_name: 'Character', name: 'Character', ...}))
WITH src
MATCH (dst)
WHERE (dst:CLASS OR dst:INTERFACE)
  AND toLower(dst.class_name) = toLower('Character')
  AND dst.user_id = 'TestSession'
  AND dst.project_name = 'testjava'
MERGE (src)-[:EXTENDS]->(dst)
```

**결과 그래프:**
```
[Player] --EXTENDS--> [Character]
```

---

### 2. 구현 관계 (IMPLEMENTS)

**소스코드:**
```java
public class Player extends Character implements Attackable {
```

**LLM 분석 결과:**
```json
{
  "relations": [
    {
      "toType": "Attackable",
      "relationType": "IMPLEMENTS",
      "toTypeKind": "INTERFACE"
    }
  ]
}
```

**Neo4j 쿼리:**
```cypher
MATCH (src:CLASS {startLine: 1, ...})
OPTIONAL MATCH (existing)
WHERE (existing:CLASS OR existing:INTERFACE)
  AND toLower(existing.class_name) = toLower('Attackable')
  ...
FOREACH(_ IN CASE WHEN existing IS NULL THEN [1] ELSE [] END |
    CREATE (:CLASS:INTERFACE {class_name: 'Attackable', name: 'Attackable', ...}))
WITH src
MATCH (dst) WHERE (dst:CLASS OR dst:INTERFACE) AND toLower(dst.class_name) = toLower('Attackable') ...
MERGE (src)-[:IMPLEMENTS]->(dst)
```

**결과 그래프:**
```
[Player] --IMPLEMENTS--> [Attackable]
```

---

### 3. 필드 연관 관계 (ASSOCIATION / AGGREGATION / COMPOSITION)

**소스코드:**
```java
public class Player extends Character {
    private Weapon weapon;  // Player가 Weapon을 가짐
}
```

**LLM 분석 결과:**
```json
{
  "fields": [
    {
      "name": "weapon",
      "type": "Weapon",
      "targetType": "Weapon",
      "visibility": "private",
      "isStatic": false,
      "isFinal": false,
      "multiplicity": "1",
      "associationType": "ASSOCIATION"
    }
  ]
}
```

**Neo4j 쿼리:**
```cypher
-- Variable 노드 생성
MERGE (v:Variable {name: 'weapon', folder_name: 'sample', file_name: 'Player.java', ...})
SET v.type = 'Weapon', v.visibility = 'private', v.isStatic = false, v.isFinal = false
WITH v
MATCH (f:FIELD {startLine: 3, ...})
MERGE (f)-[:DECLARES]->(v)

-- 연관 관계 생성
MATCH (src:CLASS {startLine: 1, ...})
OPTIONAL MATCH (existing)
WHERE (existing:CLASS OR existing:INTERFACE)
  AND toLower(existing.class_name) = toLower('Weapon')
  ...
FOREACH(_ IN CASE WHEN existing IS NULL THEN [1] ELSE [] END |
    CREATE (:CLASS:INTERFACE {class_name: 'Weapon', name: 'Weapon', ...}))
WITH src
MATCH (dst) WHERE (dst:CLASS OR dst:INTERFACE) AND toLower(dst.class_name) = toLower('Weapon') ...
MERGE (src)-[:ASSOCIATION {viaMemberName: 'weapon', multiplicity: '1'}]->(dst)
```

**결과 그래프:**
```
[Player] --ASSOCIATION {viaMemberName: 'weapon', multiplicity: '1'}--> [Weapon]
```

---

### 4. 메서드 파라미터/반환 타입 의존 (DEPENDENCY)

**소스코드:**
```java
public class Weapon {
    public void attack(Enemy target) {  // Enemy 타입 파라미터
        target.takeDamage(power);
    }
}
```

**LLM 분석 결과:**
```json
{
  "methodName": "attack",
  "returnType": "void",
  "parameters": [
    {"name": "target", "type": "Enemy"}
  ],
  "dependencies": [
    {"targetType": "Enemy", "usage": "parameter"}
  ]
}
```

**Neo4j 쿼리:**
```cypher
-- 메서드 시그니처 저장
MATCH (m:METHOD {startLine: 8, ...})
SET m.methodName = 'attack', m.returnType = 'void', m.visibility = 'public', ...

-- 파라미터 노드 생성
MATCH (m:METHOD {startLine: 8, ...})
MERGE (p:Parameter {name: 'target', methodStartLine: 8, ...})
SET p.type = 'Enemy', p.index = 0
MERGE (m)-[:HAS_PARAMETER]->(p)

-- 의존 관계 생성
MATCH (src:CLASS {startLine: 1, ...})
OPTIONAL MATCH (existing)
WHERE (existing:CLASS OR existing:INTERFACE)
  AND toLower(existing.class_name) = toLower('Enemy')
  ...
FOREACH(_ IN CASE WHEN existing IS NULL THEN [1] ELSE [] END |
    CREATE (:CLASS:INTERFACE {class_name: 'Enemy', name: 'Enemy', ...}))
WITH src
MATCH (dst) WHERE (dst:CLASS OR dst:INTERFACE) AND toLower(dst.class_name) = toLower('Enemy') ...
MERGE (src)-[:DEPENDENCY {usage: 'parameter', viaMemberName: 'attack'}]->(dst)
```

**결과 그래프:**
```
[Weapon] --DEPENDENCY {usage: 'parameter', viaMemberName: 'attack'}--> [Enemy]
```

---

### 5. 메서드 호출 관계 (CALLS)

**소스코드:**
```java
public void attack(Enemy target) {
    target.takeDamage(power);  // Enemy.takeDamage() 호출
}
```

**LLM 분석 결과:**
```json
{
  "summary": "적에게 피해를 입힘",
  "calls": ["target.takeDamage"],
  "variables": ["power"]
}
```

**Neo4j 쿼리:**
```cypher
MATCH (c:METHOD {startLine: 8, ...})
OPTIONAL MATCH (existing)
WHERE (existing:CLASS OR existing:INTERFACE)
  AND toLower(existing.class_name) = toLower('target')  -- 변수명으로 매칭 시도
  ...
FOREACH(_ IN CASE WHEN existing IS NULL THEN [1] ELSE [] END |
    CREATE (:CLASS:INTERFACE {class_name: 'target', name: 'target', ...}))
WITH c
MATCH (t)
WHERE (t:CLASS OR t:INTERFACE)
  AND toLower(t.class_name) = toLower('target')
  ...
MERGE (c)-[:CALLS {method: 'takeDamage'}]->(t)
```

**결과 그래프:**
```
[METHOD attack] --CALLS {method: 'takeDamage'}--> [Enemy]
```

> **주의:** LLM이 변수명(`target`)을 반환하면 실제 타입(`Enemy`)과 대소문자 무시 매칭으로 연결됩니다.

---

### 6. 로컬 변수 의존 관계 (localDependencies)

**소스코드:**
```java
public static void main(String[] args) {
    Weapon sword = new Weapon("불꽃검", 50);  // Weapon 타입 로컬 변수
    Enemy enemy = new Enemy("고블린");        // Enemy 타입 로컬 변수
}
```

**LLM 분석 결과:**
```json
{
  "summary": "무기와 적을 생성하고 공격 수행",
  "localDependencies": ["Weapon", "Enemy"]
}
```

**Neo4j 쿼리:**
```cypher
MATCH (src:CLASS {startLine: 1, ...})
OPTIONAL MATCH (existing)
WHERE (existing:CLASS OR existing:INTERFACE)
  AND toLower(existing.class_name) = toLower('Weapon')
  ...
FOREACH(_ IN CASE WHEN existing IS NULL THEN [1] ELSE [] END |
    CREATE (:CLASS:INTERFACE {class_name: 'Weapon', name: 'Weapon', ...}))
WITH src
MATCH (dst) WHERE (dst:CLASS OR dst:INTERFACE) AND toLower(dst.class_name) = toLower('Weapon') ...
MERGE (src)-[:DEPENDENCY {usage: 'local', viaMemberName: 'METHOD[3]'}]->(dst)
```

**결과 그래프:**
```
[Main] --DEPENDENCY {usage: 'local'}--> [Weapon]
[Main] --DEPENDENCY {usage: 'local'}--> [Enemy]
```

---

## 핵심 패턴: DBMS 스타일 노드 매칭

### 왜 이 패턴을 사용하는가?

**문제:** 분석 순서에 따라 클래스가 참조될 때 아직 해당 클래스가 분석되지 않았을 수 있음

**예시:**
1. `Main.java` 분석 → `Enemy` 클래스 참조
2. `Enemy.java`는 아직 분석 전
3. 임시 노드 생성 필요

**해결:**
```cypher
-- 1. 기존 노드 찾기 (OR 조건으로 CLASS 또는 INTERFACE)
OPTIONAL MATCH (existing)
WHERE (existing:CLASS OR existing:INTERFACE)
  AND toLower(existing.class_name) = toLower('Enemy')
  AND existing.user_id = '...'
  AND existing.project_name = '...'

-- 2. 없으면 임시 노드 생성 (두 레이블 모두 부여)
WITH src, existing
FOREACH(_ IN CASE WHEN existing IS NULL THEN [1] ELSE [] END |
    CREATE (:CLASS:INTERFACE {class_name: 'Enemy', name: 'Enemy', ...}))

-- 3. 다시 찾아서 관계 연결
WITH src
MATCH (dst)
WHERE (dst:CLASS OR dst:INTERFACE)
  AND toLower(dst.class_name) = toLower('Enemy')
  ...
MERGE (src)-[:EXTENDS]->(dst)
```

### 나중에 실제 클래스 분석 시

```cypher
-- 기존 임시 노드를 찾아서 업데이트
OPTIONAL MATCH (existing)
WHERE (existing:CLASS OR existing:INTERFACE)
  AND toLower(existing.class_name) = toLower('Enemy')
  ...
-- 있으면 스킵, 없으면 생성
FOREACH(_ IN CASE WHEN existing IS NULL THEN [1] ELSE [] END |
    CREATE (:CLASS:INTERFACE {...}))
-- 찾아서 속성 업데이트 + 정확한 레이블 설정
WITH 1 as dummy
MATCH (n)
WHERE (n:CLASS OR n:INTERFACE)
  AND toLower(n.class_name) = toLower('Enemy')
  ...
SET n:CLASS, n.startLine = 1, n.folder_name = 'sample', n.file_name = 'Enemy.java', ...
REMOVE n:INTERFACE  -- CLASS면 INTERFACE 제거
```

---

## 최종 클래스 다이어그램 예시

```
┌─────────────────┐
│   Character     │ (abstract)
│─────────────────│
│ # name: String  │
│ # hp: int       │
│─────────────────│
│ + attack()      │
└────────┬────────┘
         │ EXTENDS
         ▼
┌─────────────────┐      IMPLEMENTS      ┌─────────────────┐
│     Player      │ ──────────────────▶  │   Attackable    │
│─────────────────│                      │   (interface)   │
│ - weapon: Weapon│                      └─────────────────┘
│─────────────────│
│ + attack()      │
└────────┬────────┘
         │ ASSOCIATION
         ▼
┌─────────────────┐      DEPENDENCY       ┌─────────────────┐
│     Weapon      │ ───────────────────▶  │      Enemy      │
│─────────────────│  (parameter: target)  │─────────────────│
│ - name: String  │                       │ - name: String  │
│ - power: int    │                       │ - hp: int       │
│─────────────────│                       │─────────────────│
│ + attack(Enemy) │                       │ + takeDamage()  │
│ + getPower()    │                       │ + getName()     │
└─────────────────┘                       └─────────────────┘
```

---

## 요약

| 관계 유형 | 소스코드 패턴 | Neo4j 관계 | 속성 |
|-----------|---------------|------------|------|
| 상속 | `class A extends B` | `EXTENDS` | - |
| 구현 | `class A implements B` | `IMPLEMENTS` | - |
| 필드 연관 | `private B field;` | `ASSOCIATION` | viaMemberName, multiplicity |
| 파라미터 의존 | `void method(B param)` | `DEPENDENCY` | usage: 'parameter' |
| 반환 타입 의존 | `B method()` | `DEPENDENCY` | usage: 'return' |
| 로컬 변수 의존 | `B local = new B();` | `DEPENDENCY` | usage: 'local' |
| 메서드 호출 | `obj.method()` | `CALLS` | method: 'methodName' |

