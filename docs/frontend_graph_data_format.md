# 프론트엔드 그래프 데이터 형식

## 개요

`execute_query_and_return_graph` 메서드가 반환하는 데이터 구조입니다.
영향받은 노드와 관계만 반환되므로, 프론트엔드에서는 기존 그래프에 **병합(merge)** 해야 합니다.

## 데이터 구조

```typescript
interface GraphData {
  Nodes: Node[];
  Relationships: Relationship[];
}

interface Node {
  "Node ID": string;        // Neo4j element_id (고유 식별자)
  "Labels": string[];        // 노드 타입 배열 (예: ["Person"], ["SELECT", "DML"])
  "Properties": {            // 노드 속성
    [key: string]: any;
  };
}

interface Relationship {
  "Relationship ID": string; // Neo4j element_id (고유 식별자)
  "Type": string;            // 관계 타입 (예: "KNOWS", "FROM", "CONTAINS")
  "Properties": {             // 관계 속성
    [key: string]: any;
  };
  "Start Node ID": string;   // 시작 노드의 Node ID
  "End Node ID": string;     // 끝 노드의 Node ID
}
```

## 실제 데이터 예시

### 예시 1: 노드 생성

```json
{
  "Nodes": [
    {
      "Node ID": "4:956df4e3-cf8b-45ca-b6ae-f43bfb436788:0",
      "Labels": ["Person"],
      "Properties": {
        "name": "Alice",
        "id": "person1",
        "age": 30
      }
    },
    {
      "Node ID": "4:956df4e3-cf8b-45ca-b6ae-f43bfb436788:4",
      "Labels": ["Person"],
      "Properties": {
        "name": "Bob",
        "id": "person2",
        "age": 25
      }
    }
  ],
  "Relationships": []
}
```

### 예시 2: 관계 생성

```json
{
  "Nodes": [
    {
      "Node ID": "4:956df4e3-cf8b-45ca-b6ae-f43bfb436788:0",
      "Labels": ["Person"],
      "Properties": {
        "name": "Alice",
        "id": "person1",
        "age": 33
      }
    },
    {
      "Node ID": "4:956df4e3-cf8b-45ca-b6ae-f43bfb436788:4",
      "Labels": ["Person"],
      "Properties": {
        "name": "Bob",
        "id": "person2",
        "age": 25
      }
    },
    {
      "Node ID": "4:956df4e3-cf8b-45ca-b6ae-f43bfb436788:5",
      "Labels": ["Person"],
      "Properties": {
        "name": "Charlie",
        "id": "person3",
        "age": 35
      }
    }
  ],
  "Relationships": [
    {
      "Relationship ID": "5:956df4e3-cf8b-45ca-b6ae-f43bfb436788:1152942395327774720",
      "Type": "KNOWS",
      "Properties": {},
      "Start Node ID": "4:956df4e3-cf8b-45ca-b6ae-f43bfb436788:0",
      "End Node ID": "4:956df4e3-cf8b-45ca-b6ae-f43bfb436788:4"
    },
    {
      "Relationship ID": "5:956df4e3-cf8b-45ca-b6ae-f43bfb436788:1155194195141459972",
      "Type": "KNOWS",
      "Properties": {},
      "Start Node ID": "4:956df4e3-cf8b-45ca-b6ae-f43bfb436788:4",
      "End Node ID": "4:956df4e3-cf8b-45ca-b6ae-f43bfb436788:5"
    }
  ]
}
```

### 예시 3: 노드 업데이트 + 관계 생성 (중복 제거 확인)

```json
{
  "Nodes": [
    {
      "Node ID": "4:956df4e3-cf8b-45ca-b6ae-f43bfb436788:0",
      "Labels": ["Person"],
      "Properties": {
        "city": "Seoul",
        "name": "Alice",
        "id": "person1",
        "age": 33
      }
    },
    {
      "Node ID": "4:956df4e3-cf8b-45ca-b6ae-f43bfb436788:4",
      "Labels": ["Person"],
      "Properties": {
        "city": "Busan",
        "name": "Bob",
        "id": "person2",
        "age": 25
      }
    },
    {
      "Node ID": "4:956df4e3-cf8b-45ca-b6ae-f43bfb436788:5",
      "Labels": ["Person"],
      "Properties": {
        "name": "Charlie",
        "id": "person3",
        "age": 35
      }
    }
  ],
  "Relationships": [
    {
      "Relationship ID": "5:956df4e3-cf8b-45ca-b6ae-f43bfb436788:1155194195141459968",
      "Type": "KNOWS",
      "Properties": {},
      "Start Node ID": "4:956df4e3-cf8b-45ca-b6ae-f43bfb436788:0",
      "End Node ID": "4:956df4e3-cf8b-45ca-b6ae-f43bfb436788:5"
    }
  ]
}
```

**참고**: 같은 노드가 여러 쿼리에서 반환되어도 `Node ID` 기준으로 중복 제거되어 고유 노드만 반환됩니다.

### 예시 4: 관계 속성 포함

```json
{
  "Nodes": [
    {
      "Node ID": "4:956df4e3-cf8b-45ca-b6ae-f43bfb436788:0",
      "Labels": ["Person"],
      "Properties": {
        "name": "Alice",
        "id": "person1",
        "age": 33
      }
    },
    {
      "Node ID": "4:956df4e3-cf8b-45ca-b6ae-f43bfb436788:4",
      "Labels": ["Person"],
      "Properties": {
        "name": "Bob",
        "id": "person2",
        "age": 25
      }
    }
  ],
  "Relationships": [
    {
      "Relationship ID": "5:956df4e3-cf8b-45ca-b6ae-f43bfb436788:1152942395327774720",
      "Type": "KNOWS",
      "Properties": {
        "since": 2023
      },
      "Start Node ID": "4:956df4e3-cf8b-45ca-b6ae-f43bfb436788:0",
      "End Node ID": "4:956df4e3-cf8b-45ca-b6ae-f43bfb436788:4"
    }
  ]
}
```

## 프론트엔드 처리 방법

### 1. 노드 병합

```javascript
// 기존 그래프에 새 노드 추가/업데이트
function mergeNodes(existingNodes, newNodes) {
  const nodeMap = new Map();
  
  // 기존 노드를 Map에 추가
  existingNodes.forEach(node => {
    nodeMap.set(node["Node ID"], node);
  });
  
  // 새 노드로 업데이트 (같은 ID면 덮어쓰기)
  newNodes.forEach(node => {
    nodeMap.set(node["Node ID"], node);
  });
  
  return Array.from(nodeMap.values());
}
```

### 2. 관계 병합

```javascript
// 기존 그래프에 새 관계 추가/업데이트
function mergeRelationships(existingRels, newRels) {
  const relMap = new Map();
  
  // 기존 관계를 Map에 추가
  existingRels.forEach(rel => {
    relMap.set(rel["Relationship ID"], rel);
  });
  
  // 새 관계로 업데이트 (같은 ID면 덮어쓰기)
  newRels.forEach(rel => {
    relMap.set(rel["Relationship ID"], rel);
  });
  
  return Array.from(relMap.values());
}
```

### 3. 전체 그래프 업데이트

```javascript
function updateGraph(currentGraph, newData) {
  return {
    Nodes: mergeNodes(currentGraph.Nodes || [], newData.Nodes || []),
    Relationships: mergeRelationships(
      currentGraph.Relationships || [], 
      newData.Relationships || []
    )
  };
}

// 사용 예시
let graph = { Nodes: [], Relationships: [] };

// 서버에서 새 데이터 받음
const newData = await fetchGraphData();
graph = updateGraph(graph, newData);
```

## 주요 특징

1. **증분 업데이트**: 영향받은 노드/관계만 반환
2. **중복 제거**: 같은 노드/관계가 여러 쿼리에서 반환되어도 고유 항목만 포함
3. **완전한 관계**: 관계 생성 시 시작/끝 노드가 모두 포함됨
4. **최신 상태**: 노드/관계 업데이트 시 최신 속성이 반환됨

## 주의사항

- `Node ID`와 `Relationship ID`는 Neo4j의 `element_id`로, 고유 식별자입니다
- 같은 노드가 여러 쿼리에서 반환되어도 `Node ID`가 같으면 중복 제거됩니다
- 관계의 `Start Node ID`와 `End Node ID`는 항상 `Nodes` 배열에 포함되어 있습니다

