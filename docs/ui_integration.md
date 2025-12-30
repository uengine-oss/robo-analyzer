# 🎨 UI 통합 가이드

> Robo Analyzer를 웹 애플리케이션에 통합하는 방법

---

## 📡 실시간 스트리밍 (SSE) 연결하기

### 1단계: API 엔드포인트 호출

분석을 시작하려면 POST 요청을 보냅니다:

```javascript
const response = await fetch('/api/understand', {
  method: 'POST',
  headers: {
    'Content-Type': 'application/json',
  },
  body: JSON.stringify({
    project_name: 'my-project',
    file_names: ['file1.sql', 'file2.java'],
  }),
});
```

### 2단계: SSE 스트림 연결

서버에서 실시간으로 보내는 메시지를 받기 위해 EventSource를 사용합니다:

```javascript
const eventSource = new EventSource('/api/understand/stream');

eventSource.onmessage = (event) => {
  const data = JSON.parse(event.data);
  console.log('받은 데이터:', data);
};
```

### 3단계: 메시지 타입별 처리

서버에서 보내는 메시지는 5가지 타입이 있습니다:

#### 1. 일반 메시지 (`message`)
```json
{
  "type": "message",
  "content": "🚀 DBMS 코드 분석을 시작합니다"
}
```

**처리 방법:**
```javascript
if (data.type === 'message') {
  // 화면에 메시지 표시
  showMessage(data.content);
}
```

#### 2. 데이터 업데이트 (`data`)
```json
{
  "type": "data",
  "current_file": "PKG_ORDER.sql",
  "analysis_progress": 45,
  "line_number": 120,
  "graph": {
    "Nodes": [...],
    "Relationships": [...]
  }
}
```

**처리 방법:**
```javascript
if (data.type === 'data') {
  // 진행률 업데이트
  updateProgressBar(data.analysis_progress);
  // 현재 파일 표시
  setCurrentFile(data.current_file);
  // 그래프 업데이트
  updateGraph(data.graph);
}
```

#### 3. 노드 생성 이벤트 (`node_event`)
```json
{
  "type": "node_event",
  "action": "created",
  "nodeType": "PROCEDURE",
  "nodeName": "CREATE_ORDER",
  "details": {
    "start_line": 15
  }
}
```

**처리 방법:**
```javascript
if (data.type === 'node_event') {
  // 노드 생성 알림 표시
  showNotification(`노드 생성: ${data.nodeName} (${data.nodeType})`);
  // 그래프에 노드 추가
  addNodeToGraph(data.nodeType, data.nodeName);
}
```

#### 4. 관계 생성 이벤트 (`relationship_event`)
```json
{
  "type": "relationship_event",
  "action": "created",
  "relType": "CALLS",
  "source": "OrderService",
  "target": "OrderRepository"
}
```

**처리 방법:**
```javascript
if (data.type === 'relationship_event') {
  // 관계 생성 알림 표시
  showNotification(`관계 생성: ${data.source} → ${data.target}`);
  // 그래프에 관계 추가
  addRelationship(data.source, data.target, data.relType);
}
```

#### 5. 에러 발생 (`error`)
```json
{
  "type": "error",
  "message": "파일을 읽을 수 없습니다: invalid_file.sql"
}
```

**처리 방법:**
```javascript
if (data.type === 'error') {
  // 에러 메시지 표시
  showError(data.message);
  // 재시도 버튼 표시
  showRetryButton();
}
```

---

## 💻 완전한 예제 코드

### React 컴포넌트 예제

```jsx
import { useState, useEffect } from 'react';

function AnalysisProgress() {
  const [messages, setMessages] = useState([]);
  const [progress, setProgress] = useState(0);
  const [currentFile, setCurrentFile] = useState('');
  const [nodes, setNodes] = useState([]);
  const [relationships, setRelationships] = useState([]);

  useEffect(() => {
    // SSE 연결
    const eventSource = new EventSource('/api/understand/stream');

    eventSource.onmessage = (event) => {
      const data = JSON.parse(event.data);

      switch (data.type) {
        case 'message':
          // 메시지 목록에 추가
          setMessages(prev => [...prev, data.content]);
          break;

        case 'data':
          // 진행률 업데이트
          setProgress(data.analysis_progress);
          setCurrentFile(data.current_file);
          
          // 그래프 데이터 업데이트
          if (data.graph) {
            setNodes(data.graph.Nodes || []);
            setRelationships(data.graph.Relationships || []);
          }
          break;

        case 'node_event':
          // 노드 생성 알림
          console.log(`노드 생성: ${data.nodeName}`);
          break;

        case 'relationship_event':
          // 관계 생성 알림
          console.log(`관계 생성: ${data.source} → ${data.target}`);
          break;

        case 'error':
          // 에러 표시
          alert(`에러 발생: ${data.message}`);
          break;
      }
    };

    // 연결 종료 시 정리
    return () => {
      eventSource.close();
    };
  }, []);

  return (
    <div>
      {/* 진행률 바 */}
      <div>
        <progress value={progress} max={100} />
        <span>{progress}%</span>
      </div>

      {/* 현재 파일 */}
      <div>현재 파일: {currentFile}</div>

      {/* 메시지 목록 */}
      <div>
        {messages.map((msg, idx) => (
          <div key={idx}>{msg}</div>
        ))}
      </div>

      {/* 그래프 시각화 영역 */}
      <GraphVisualization nodes={nodes} relationships={relationships} />
    </div>
  );
}
```

### Vanilla JavaScript 예제

```javascript
class AnalysisProgress {
  constructor() {
    this.messages = [];
    this.progress = 0;
    this.currentFile = '';
    this.eventSource = null;
  }

  start() {
    // SSE 연결
    this.eventSource = new EventSource('/api/understand/stream');

    this.eventSource.onmessage = (event) => {
      const data = JSON.parse(event.data);
      this.handleMessage(data);
    };

    this.eventSource.onerror = () => {
      console.error('SSE 연결 오류');
      // 3초 후 재연결 시도
      setTimeout(() => this.start(), 3000);
    };
  }

  handleMessage(data) {
    switch (data.type) {
      case 'message':
        this.addMessage(data.content);
        break;

      case 'data':
        this.updateProgress(data.analysis_progress);
        this.setCurrentFile(data.current_file);
        if (data.graph) {
          this.updateGraph(data.graph);
        }
        break;

      case 'node_event':
        this.onNodeCreated(data);
        break;

      case 'relationship_event':
        this.onRelationshipCreated(data);
        break;

      case 'error':
        this.showError(data.message);
        break;
    }
  }

  addMessage(content) {
    this.messages.push(content);
    this.renderMessages();
  }

  updateProgress(progress) {
    this.progress = progress;
    document.getElementById('progress-bar').value = progress;
    document.getElementById('progress-text').textContent = `${progress}%`;
  }

  setCurrentFile(filename) {
    this.currentFile = filename;
    document.getElementById('current-file').textContent = filename;
  }

  updateGraph(graph) {
    // 그래프 시각화 라이브러리로 업데이트
    // 예: D3.js, Cytoscape.js 등
  }

  onNodeCreated(data) {
    console.log(`노드 생성: ${data.nodeName} (${data.nodeType})`);
  }

  onRelationshipCreated(data) {
    console.log(`관계 생성: ${data.source} → ${data.target}`);
  }

  showError(message) {
    alert(`에러: ${message}`);
  }

  renderMessages() {
    const container = document.getElementById('messages');
    container.innerHTML = this.messages
      .map(msg => `<div>${msg}</div>`)
      .join('');
  }

  stop() {
    if (this.eventSource) {
      this.eventSource.close();
    }
  }
}

// 사용 예시
const progress = new AnalysisProgress();
progress.start();
```

---

## 🎨 UI 구성 요소 만들기

### 1. 진행 상황 표시

**필요한 정보:**
- 진행률 (0-100%)
- 현재 처리 중인 파일명
- 단계별 상태

**예시 디자인:**
```
━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
📋 [1단계] 테이블 스키마 수집 (3개 DDL)
━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

📄 [1/3] tables.ddl
   ✓ Table 노드 생성/업데이트: 5개
   ✓ Column 노드 생성/업데이트: 23개
```

### 2. 실시간 로그 표시

**표시할 내용:**
- 노드 생성/업데이트 알림
- 관계 생성 알림
- AI 분석 진행 상황

**예시:**
```
→ PROCEDURE 노드 생성: CREATE_ORDER (Line 15)
→ [1/5] PROCEDURE 분석: CREATE_ORDER
   요약: 주문을 생성하고 재고를 확인하는 프로시저...
```

### 3. 그래프 시각화

**업데이트 방법:**
- `type: "data"` 이벤트를 받을 때마다 그래프 업데이트
- `graph` 필드에 노드와 관계 정보가 포함됨

**추천 라이브러리:**
- **D3.js**: 유연하고 강력함
- **Cytoscape.js**: 그래프 전용, 사용하기 쉬움
- **vis.js**: 빠르고 간단함

**예시 코드 (Cytoscape.js):**
```javascript
const cy = cytoscape({
  container: document.getElementById('graph'),
  elements: []
});

// 그래프 업데이트
function updateGraph(graph) {
  const elements = [];
  
  // 노드 추가
  graph.Nodes.forEach(node => {
    elements.push({
      data: {
        id: node.id,
        label: node.name,
        type: node.labels[0]
      }
    });
  });
  
  // 관계 추가
  graph.Relationships.forEach(rel => {
    elements.push({
      data: {
        id: rel.id,
        source: rel.startNode,
        target: rel.endNode,
        label: rel.type
      }
    });
  });
  
  cy.elements().remove();
  cy.add(elements);
}
```

### 4. 에러 처리

**에러 표시 방법:**
- `type: "error"` 이벤트를 받으면 에러 메시지 표시
- 사용자에게 친절한 메시지로 변환
- 재시도 버튼 제공

**예시:**
```javascript
if (data.type === 'error') {
  // 에러 메시지 표시
  showErrorModal({
    title: '분석 중 오류 발생',
    message: data.message,
    onRetry: () => {
      // 분석 다시 시작
      startAnalysis();
    }
  });
}
```

---

## 🔧 고급 기능

### 메시지 필터링

모든 메시지를 처리하지 않고 필요한 것만 처리:

```javascript
const IMPORTANT_TYPES = ['error', 'data', 'node_event'];

eventSource.onmessage = (event) => {
  const data = JSON.parse(event.data);
  
  // 중요한 이벤트만 처리
  if (IMPORTANT_TYPES.includes(data.type)) {
    handleEvent(data);
  }
};
```

### 연결 끊김 처리

연결이 끊어지면 자동으로 재연결:

```javascript
eventSource.onerror = () => {
  console.log('연결 끊김, 재연결 시도...');
  
  // 3초 후 재연결
  setTimeout(() => {
    eventSource = new EventSource('/api/understand/stream');
  }, 3000);
};
```

### 커스텀 이벤트 핸들러

이벤트 타입별로 함수를 분리:

```javascript
const handlers = {
  message: (data) => {
    console.log('메시지:', data.content);
    addToLog(data.content);
  },
  
  node_event: (data) => {
    if (data.action === 'created') {
      addNodeToGraph(data.nodeType, data.nodeName);
    }
  },
  
  relationship_event: (data) => {
    if (data.action === 'created') {
      addRelationship(data.source, data.target, data.relType);
    }
  },
  
  error: (data) => {
    showError(data.message);
  }
};

eventSource.onmessage = (event) => {
  const data = JSON.parse(event.data);
  const handler = handlers[data.type];
  if (handler) {
    handler(data);
  }
};
```

---

## 🐛 문제 해결

### 연결이 안 될 때

1. **서버가 실행 중인지 확인**
2. **CORS 설정 확인** (다른 도메인에서 접근하는 경우)
3. **브라우저 콘솔에서 에러 확인**

### 메시지가 안 올 때

1. **네트워크 탭에서 SSE 연결 확인**
2. **서버 로그 확인**
3. **이벤트 타입 확인** (예상한 타입과 다른지)

### 성능 문제

1. **메시지 필터링 적용** (필요한 것만 처리)
2. **그래프 업데이트를 배치로 처리** (여러 개를 한 번에)
3. **불필요한 렌더링 최소화**

---

## 📚 참고 자료

- [SSE 스펙 문서](https://developer.mozilla.org/en-US/docs/Web/API/Server-sent_events)
- [EventSource API](https://developer.mozilla.org/en-US/docs/Web/API/EventSource)
- [Cytoscape.js 문서](https://js.cytoscape.org/)
- [D3.js 문서](https://d3js.org/)

