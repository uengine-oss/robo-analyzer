# ��� Legacy Modernizer Backend - Understanding 파이프라인 완벽 가이드

> **PL/SQL 코드를 이해하고 Neo4j 그래프로 변환하는 AI 기반 코드 분석 시스템**
> 
> 이 문서는 처음 프로젝트에 합류하는 개발자가 전체 구조와 동작 원리를 완전히 이해하고,
> 어느 부분을 수정해야 하는지 즉시 파악할 수 있도록 작성되었습니다.

[![FastAPI](https://img.shields.io/badge/FastAPI-0.115.12-009688?style=flat&logo=fastapi)](https://fastapi.tiangolo.com/)
[![Python](https://img.shields.io/badge/Python-3.10+-3776AB?style=flat&logo=python&logoColor=white)](https://www.python.org/)
[![Neo4j](https://img.shields.io/badge/Neo4j-5.x-008CC1?style=flat&logo=neo4j&logoColor=white)](https://neo4j.com/)

---

## ��� 목차

1. [프로젝트 개요](#1-프로젝트-개요)
2. [시스템 아키텍처](#2-시스템-아키텍처)
3. [데이터 폴더 구조와 ANTLR JSON](#3-데이터-폴더-구조와-antlr-json)
4. [Understanding 파이프라인 완벽 가이드](#4-understanding-파이프라인-완벽-가이드)
5. [시퀀스 다이어그램](#5-시퀀스-다이어그램)
6. [파일별 상세 가이드](#6-파일별-상세-가이드)
7. [개발 환경 설정](#7-개발-환경-설정)
8. [테스트 실행 가이드](#8-테스트-실행-가이드)
9. [Neo4j 활용 가이드](#9-neo4j-활용-가이드)
10. [트러블슈팅](#10-트러블슈팅)

---

## 1. 프로젝트 개요

### 1.1 Legacy Modernizer란?

Legacy Modernizer는 **레거시 코드를 정밀하게 이해하고 그래프로 재구성한 뒤, 그 그래프를 토대로 원하는 타겟 언어 아키텍처로 전환까지 수행하는 AI 기반 현대화 플랫폼**입니다. 복잡한 저장 프로시저나 스크립트를 텍스트 수준에서만 다루지 않고 구조와 의존 관계를 명확히 드러내어 가독성과 분석 효율을 극대화합니다.

### 1.2 핵심 철학

```
┌─────────────────────────────────────────────────────────────┐
│  "코드는 단순한 텍스트가 아니라 관계의 집합이다"             │
│                                                               │
│  레거시 코드의 흐름과 제약을 그래프로 옮겨야                  │
│  의미를 정확히 파악하고, 안전하게 현대화할 수 있다            │
└─────────────────────────────────────────────────────────────┘
```

그래프화된 표현은 코드 흐름, 데이터 사용 패턴, 컴포넌트 의존성을 한눈에 보여 주기 때문에 구조 분석·리뷰·영향도 파악 같은 작업을 빠르게 수행할 수 있습니다. Understanding 단계에서 얻은 이 그래프는 이후 Converting 단계에서 재사용되는 단일 진실 소스(Single Source of Truth)로, 변환 품질과 일관성을 담보합니다.

### 1.3 Understanding과 Converting: 두 단계 분리 설계

Legacy Modernizer는 다음 두 단계로 구성됩니다.

- **Understanding**: 원본 코드를 대상으로 AST 수집과 LLM 분석을 결합하여 Neo4j 그래프를 생성합니다. 이 그래프는 로직과 데이터 흐름을 직관적으로 보여 주어 가독성 향상, 구조 분석, 사전 검증에 도움이 됩니다.
- **Converting**: Understanding에서 생성한 그래프를 입력으로 삼아 Spring Boot나 FastAPI처럼 원하는 타겟 언어와 클린 아키텍처 패턴으로 코드를 자동 전환합니다. 그래프에 모든 관계와 시그니처가 담겨 있어 일관된 패키징과 규칙 적용이 가능합니다.

### 1.4 각 단계별 역할

| 단계 | 입력 | 출력 | 핵심 작업 |
|------|------|------|----------|
| **Understanding** | PL/SQL 파일<br/>+ ANTLR JSON | Neo4j 그래프 | - AST 파싱<br/>- LLM 의미 분석<br/>- 테이블/컬럼/관계 추출<br/>- 프로시저 호출 관계 파악 |
| **Converting** | Neo4j 그래프 | Spring Boot<br/>또는 FastAPI | - 그래프 조회<br/>- 타겟 언어 문법 변환<br/>- 프로젝트 구조 생성 |

---

## 2. 시스템 아키텍처

### 2.1 전체 시스템 구조

\`\`\`
┌──────────────────────────────────────────────────────────────────┐
│                         Frontend (React)                          │
│   - 파일 업로드                                                   │
│   - 그래프 시각화                                                 │
│   - SSE 스트리밍 수신                                             │
└──────────────────┬───────────────────────────────────────────────┘
                   │ HTTP/SSE
┌──────────────────▼───────────────────────────────────────────────┐
│                    Backend (FastAPI)                              │
│                                                                   │
│  ┌──────────────────────────────────────────────────────────┐   │
│  │          service/router.py (API 엔드포인트)              │   │
│  └────────┬───────────────────────────────────┬───────────────┘   │
│           │                                   │                   │
│  ┌────────▼──────────┐             ┌─────────▼──────────┐        │
│  │ Understanding     │             │ Converting         │        │
│  │ (service.py)      │             │ (strategies/)      │        │
│  └────────┬──────────┘             └────────────────────┘        │
│           │                                                       │
│  ┌────────▼────────────────────────┐                             │
│  │   understand/analysis.py        │                             │
│  │   (Analyzer 핵심 로직)          │                             │
│  └────────┬────────────────────────┘                             │
│           │                                                       │
│  ┌────────▼────────────────────────┐                             │
│  │   prompt/*.py (LLM 프롬프트)    │                             │
│  └─────────────────────────────────┘                             │
└───────────────────────┬───────────────────────────────────────────┘
                        │
┌───────────────────────▼───────────────────────────────────────────┐
│                     Neo4j Graph Database                          │
│   - 노드: PROCEDURE, SELECT, INSERT, Table, Column, Variable     │
│   - 관계: PARENT_OF, NEXT, FROM, WRITES, CALL, HAS_COLUMN        │
└───────────────────────────────────────────────────────────────────┘
\`\`\`

### 2.2 Understanding 모듈 구조

```
understand/
├── analysis.py              # Analyzer 핵심 파이프라인
└── neo4j_connection.py      # Neo4j 연결 및 쿼리 실행

service/
├── service.py               # ServiceOrchestrator (파일 처리, DDL 등)
└── router.py                # FastAPI 라우터 (엔드포인트 정의)

prompt/
├── understand_prompt.py              # 일반 코드 분석 (summary, calls, variables)
├── understand_dml_table_prompt.py    # DML 테이블/컬럼 추출
├── understand_variables_prompt.py    # 변수 선언 분석
├── understand_summarized_prompt.py   # 프로시저 전체 요약
├── understand_ddl.py                 # DDL 파싱
├── understand_column_prompt.py       # 컬럼 역할 분석
└── understand_table_summary_prompt.py # 테이블 설명 요약

util/
├── llm_client.py            # LLM 클라이언트 생성
├── utility_tool.py          # 유틸리티 함수들
└── exception.py             # 커스텀 예외
```

### 3. 데이터 폴더 구조와 ANTLR JSON

### 3.1 데이터 폴더 전체 구조

Understanding 파이프라인은 **반드시 정해진 폴더 구조**를 따라야 합니다.

\`\`\`
data/
└── {user_id}/
    └── {project_name}/
        ├── src/                    # 원본 PL/SQL 파일
        │   └── {folder_name}/
        │       └── {file_name}.sql
        ├── analysis/               # ANTLR이 생성한 AST JSON
        │   └── {folder_name}/
        │       └── {file_name}.json
        └── ddl/                    # DDL 파일 (선택)
            └── {table}.sql
\`\`\`

#### **폴더별 역할**

| 폴더 | 역할 | 필수 여부 | 설명 |
|------|------|-----------|------|
| **src/** | PL/SQL 원본 | ✅ | 분석 대상 저장 프로시저 파일 |
| **analysis/** | ANTLR JSON | ✅ | ANTLR4가 파싱한 AST 구조 |
| **ddl/** | DDL 파일 | ❌ | 테이블 스키마 정의 (있으면 먼저 처리) |

### 3.2 ANTLR JSON 구조 이해

ANTLR JSON은 **Understanding 파이프라인의 핵심 입력 데이터**입니다. 이 JSON은 PL/SQL 코드를 AST(Abstract Syntax Tree) 형태로 표현합니다.

#### **기본 구조**

\`\`\`json
{
  "type": "FILE",
  "startLine": 0,
  "endLine": 0,
  "children": [
    {
      "type": "PROCEDURE",
      "startLine": 1,
      "endLine": 18,
      "children": [
        {
          "type": "SPEC",
          "startLine": 1,
          "endLine": 5,
          "children": []
        },
        {
          "type": "DECLARE",
          "startLine": 6,
          "endLine": 6,
          "children": []
        },
        {
          "type": "SELECT",
          "startLine": 9,
          "endLine": 11,
          "children": []
        },
        {
          "type": "INSERT",
          "startLine": 14,
          "endLine": 15,
          "children": []
        },
        {
          "type": "COMMIT",
          "startLine": 17,
          "endLine": 17,
          "children": []
        }
      ]
    }
  ]
}
\`\`\`

#### **필드 설명**

| 필드 | 타입 | 설명 |
|------|------|------|
| **type** | string | 노드 타입 (FILE, PROCEDURE, SELECT, INSERT 등) |
| **startLine** | int | 시작 라인 번호 (1-based) |
| **endLine** | int | 종료 라인 번호 (1-based) |
| **children** | array | 하위 노드 배열 (재귀적 구조) |

#### **주요 노드 타입**

| 타입 | 의미 | 예시 |
|------|------|------|
| **SYSTEM** | 여러 프로시저 파일을 구분하는 시스템(폴더) 노드 | `SYSTEM(name='ORDER_SYSTEM')` |
| **FILE** | 업로드된 개별 SP 파일 노드 | `orders/proc_create_order.sql` |
| **PROCEDURE** | 프로시저/함수 | `CREATE PROCEDURE proc_name` |
| **SPEC** | 파라미터 선언부 | `IN p_id INTEGER` |
| **DECLARE** | 변수 선언부 | `v_count INTEGER := 0;` |
| **SELECT** | SELECT 문 | `SELECT * FROM orders` |
| **INSERT** | INSERT 문 | `INSERT INTO logs VALUES (...)` |
| **UPDATE** | UPDATE 문 | `UPDATE orders SET status = 'DONE'` |
| **DELETE** | DELETE 문 | `DELETE FROM temp_table` |
| **IF** | 조건문 | `IF v_count > 0 THEN` |
| **LOOP** | 반복문 | `LOOP ... END LOOP;` |
| **COMMIT** | 트랜잭션 커밋 | `COMMIT;` |

### 3.3 ANTLR JSON이 그래프로 변환되는 과정

ANTLR이 생성한 JSON 트리는 아래와 같은 단계를 거쳐 Neo4j 그래프로 바뀝니다. 핵심은 **JSON → StatementNode → Neo4j 노드/관계** 순서입니다.

1. **StatementCollector가 JSON을 순회하면서 StatementNode 리스트를 만든다.**
   - 각 JSON 노드(type, startLine, endLine 등)를 읽어 `StatementNode` 객체로 변환합니다.
   - 변환 과정에서 실제 소스 코드 구간을 잘라 `node.code`에 저장하고, 부모-자식 관계도 연결합니다.

2. **Analyzer가 StatementNode를 이용해 Neo4j에 기본 노드를 생성한다.**
   - `_create_static_nodes()`에서 StatementNode의 `node_type`에 대응하는 라벨(FILE, PROCEDURE, SELECT 등)을 가진 노드를 `MERGE`합니다.
   - `_create_relationships()`에서 부모-자식(`PARENT_OF`), 형제(`NEXT`) 관계를 추가하여 AST 구조를 그대로 그래프로 옮깁니다.

3. **ApplyManager가 LLM 분석 결과를 받아 노드 속성과 테이블/컬럼 관계를 채운다.**
   - 배치별 LLM 호출 결과(요약, 변수 목록, 호출 관계)를 바탕으로 `summary`, `variables` 등의 속성을 업데이트합니다.
   - DML 노드에 대해서는 테이블(`Table`), 컬럼(`Column`) 노드를 `MERGE`하고 `FROM`, `WRITES`, `HAS_COLUMN` 관계를 연결합니다.

아래는 각 단계가 실제 코드와 쿼리에서 어떻게 표현되는지 보여 줍니다.

#### Step 1: JSON → StatementNode

다음 함수는 JSON 노드를 방문할 때마다 동일한 라인 정보를 가진 `StatementNode`를 생성합니다. `code` 필드에는 라인 번호가 붙은 원본 코드가 그대로 들어가고, 이후 부모·자식 연결을 위해 반환된 `statement_node`가 리스트에 쌓입니다.

```python
# understand/analysis.py - StatementCollector 클래스 일부

def _visit(self, node, current_proc, current_type, current_schema):
    start_line = node['startLine']
    end_line = node['endLine']
    node_type = node['type']

    # 라인 범위에 해당하는 실제 코드 추출
    code = get_original_node_code(file_content, start_line, end_line)

    # StatementNode 생성
    statement_node = StatementNode(
        node_id=self._node_id,
        start_line=start_line,
        end_line=end_line,
        node_type=node_type,
        code=code,
        ...
    )
```

#### Step 2: StatementNode → Neo4j 노드

첫 단계에서 만들어진 StatementNode를 기반으로 Cypher가 실행되면, 아래와 같이 동일한 라인 범위·파일 정보를 가진 Neo4j 노드가 생성됩니다. `node_code`, `summary`, `token` 등의 속성은 LLM 분석 결과가 들어올 때마다 갱신됩니다.

```cypher
// 예시: SELECT 노드 생성
MERGE (n:SELECT {
    startLine: 9,
    endLine: 11,
    folder_name: 'SYSTEM',
    file_name: 'proc_order.sql',
    user_id: 'user123',
    project_name: 'ERP_MIGRATION'
})
SET n.node_code = '9: SELECT * FROM orders WHERE order_id = p_order_id',
    n.summary = '주문 정보를 조회합니다.',
    n.token = 25
```

#### Step 3: Neo4j 관계 생성

Analyzer는 StatementNode 간의 parent/child/next 정보를 이용해 `PARENT_OF`, `NEXT` 관계를 붙이고, ApplyManager는 DML 분석 결과를 활용해 `FROM`, `WRITES`, `HAS_COLUMN` 같은 도메인 관계를 추가합니다. 이렇게 구축된 그래프를 통해 프로시저 흐름과 데이터 의존성을 한눈에 살펴볼 수 있습니다.

```cypher
// PROCEDURE와 SELECT의 부모-자식 관계
MERGE (parent:PROCEDURE {startLine: 1, ...})-[:PARENT_OF]->(child:SELECT {startLine: 9, ...})

// SELECT와 Table의 FROM 관계
MERGE (select:SELECT {startLine: 9, ...})-[:FROM]->(table:Table {name: 'ORDERS'})
```

이 과정을 통해 JSON으로 표현된 AST 구조가 Neo4j 그래프에 그대로 옮겨지며, 이후 LLM 결과가 속성으로 차곡차곡 채워집니다.

---

## 4. Understanding 파이프라인 완벽 가이드

### 4.1 전체 프로세스 개요

Understanding 파이프라인은 다음 순서로 실행됩니다:

\`\`\`
[1] API 요청 수신 (router.py)
     │
[2] DDL 파일 처리 (service.py) 
     │
[3] ANTLR JSON 및 PL/SQL 로드
     │
[4] Analyzer 초기화 및 실행 (analysis.py)
     │
[5] AST 수집 (StatementCollector)
     │
[6] 배치 플래닝 (BatchPlanner)
     │
[7] LLM 병렬 호출 (LLMInvoker)
     │
[8] Neo4j 반영 (ApplyManager)
     │
[9] 후처리 (변수 타입 해석, 컬럼 역할 분석)
     │
[10] SSE 스트리밍으로 프론트엔드에 전송
\`\`\`

### 4.2 Step 1: API 요청 수신

#### **엔드포인트**

아래 엔드포인트는 Understanding 파이프라인을 시작하기 위해 클라이언트가 호출하는 핵심 API입니다. 헤더에서 사용자/키 정보를 읽고, 전달된 파일 목록으로 `ServiceOrchestrator`를 만들어 스트리밍 응답을 반환합니다.

```python
# service/router.py

@router.post("/cypherQuery/")
async def understand_data(request: Request):
    # 1. 헤더에서 user_id, api_key 추출
    user_id = request.headers.get('Session-UUID')
    api_key = request.headers.get('OpenAI-Api-Key')
    
    # 2. 요청 본문 파싱
    file_data = await request.json()
    project_name = file_data['projectName']
    dbms = file_data['dbms']
    file_names = [(system['name'], sp) for system in file_data['systems'] for sp in system['sp']]
    
    # 3. ServiceOrchestrator 생성
    orchestrator = ServiceOrchestrator(user_id, api_key, locale, project_name, dbms)
    
    # 4. 스트리밍 응답 반환
    return StreamingResponse(orchestrator.understand_project(file_names))
```

#### **요청 예시**

다음 JSON은 프론트엔드에서 실제로 전달하는 요청 본문 형태입니다. 어떤 시스템 이름 아래 어떤 SP 파일들을 분석할지 지정합니다.

```json
{
  "projectName": "ERP_MIGRATION",
  "dbms": "postgres",
  "systems": [
    {
      "name": "ORDER_SYSTEM",
      "sp": ["proc_create_order.sql", "proc_cancel_order.sql"]
    }
  ]
}
```

### 4.3 Step 2: DDL 파일 처리 (선택)

DDL 파일이 있으면 **먼저 처리**하여 테이블/컬럼 구조를 Neo4j에 저장합니다.

아래 `_process_ddl` 함수는 DDL 파일 하나를 읽어 LLM으로 파싱하고, 결과를 Neo4j로 반영하는 전체 흐름을 보여 줍니다.

```python
# service/service.py

async def _process_ddl(self, ddl_file_path: str, connection, file_name: str):
    # 1. DDL 파일 읽기
    ddl_content = await read_file(ddl_file_path)
    
    # 2. LLM으로 DDL 파싱
    parsed = understand_ddl(ddl_content, self.api_key, self.locale)
    
    # 3. Neo4j 쿼리 생성
    for table in parsed['analysis']:
        # Table 노드 생성
        cypher = f"MERGE (t:Table {{name: '{table['name']}', schema: '{table['schema']}', ...}})"
        
        # Column 노드 및 HAS_COLUMN 관계 생성
        for column in table['columns']:
            cypher += f"MERGE (c:Column {{name: '{column['name']}', dtype: '{column['dtype']}', ...}})"
            cypher += f"MERGE (t)-[:HAS_COLUMN]->(c)"
        
        # FK 관계 생성
        for fk in table['foreignKeys']:
            cypher += f"MERGE (src:Column {{name: '{fk['column']}'}})-[:FK_TO]->(tgt:Column {{name: '{fk['ref']}'}})"
    
    # 4. Neo4j 실행
    await connection.execute_queries(cypher_queries)
```

#### **DDL 처리가 중요한 이유**

- **DML 노드 처리 전에 테이블 정보가 있어야 함**
- DDL이 없으면 DML에서 테이블을 발견할 때마다 동적으로 생성
- DDL이 있으면 정확한 컬럼 타입, Nullable, FK 관계 등을 미리 확보

### 4.4 Step 3: ANTLR JSON 및 PL/SQL 로드

아래 `_load_assets` 함수는 분석할 파일의 PL/SQL 원본과 ANTLR JSON을 동시에 읽어들이는 역할을 합니다. 경로 규칙에 맞춰 파일 위치를 찾고, `aiofiles`를 사용해 비동기적으로 병렬 로드합니다.

```python
# service/service.py

async def _load_assets(self, folder_name: str, file_name: str):
    folder_dir = os.path.join(self.dirs['plsql'], folder_name)
    plsql_file_path = os.path.join(folder_dir, file_name)
    
    base_name = os.path.splitext(file_name)[0]
    analysis_file_path = os.path.join(self.dirs['analysis'], folder_name, f"{base_name}.json")
    
    # 병렬 로드
    async with aiofiles.open(analysis_file_path) as antlr_file, \
               aiofiles.open(plsql_file_path) as plsql_file:
        antlr_data, plsql_content = await asyncio.gather(
            antlr_file.read(),
            plsql_file.readlines()
        )
    
    return json.loads(antlr_data), plsql_content
```

### 4.5 Step 4: Analyzer 초기화 및 실행

이제 준비된 파일 데이터를 가지고 `Analyzer` 인스턴스를 생성한 뒤, `run()`을 비동기로 실행하여 Understanding 파이프라인을 본격적으로 시작합니다.

```python
# service/service.py

analyzer = Analyzer(
    antlr_data=antlr_data,
    file_content=plsql_numbered,
    send_queue=events_from_analyzer,
    receive_queue=events_to_analyzer,
    last_line=last_line,
    folder_name=folder_name,
    file_name=file_name,
    user_id=self.user_id,
    api_key=self.api_key,
    locale=self.locale,
    dbms=self.dbms,
    project_name=self.project_name,
)

# 비동기 실행
analysis_task = asyncio.create_task(analyzer.run())
```

### 4.6 Step 5: AST 수집 (StatementCollector)

#### **StatementCollector의 역할**

ANTLR JSON을 **후위순회(post-order traversal)**하여 `StatementNode` 객체 리스트를 생성합니다.

\`\`\`python
# understand/analysis.py

class StatementCollector:
    def collect(self):
        # 후위순회: 자식 → 부모 순서 보장
        self._visit(self.antlr_data, current_proc=None, ...)
        return self.nodes, self.procedures
    
    def _visit(self, node, current_proc, current_type, current_schema):
        # 1. 자식 먼저 방문 (후위순회)
        for child in node['children']:
            child_node = self._visit(child, ...)
            child_nodes.append(child_node)
        
        # 2. 현재 노드의 코드 추출
        code = get_original_node_code(file_content, start_line, end_line)
        
        # 3. StatementNode 생성
        statement_node = StatementNode(
            node_id=self._node_id,
            start_line=start_line,
            end_line=end_line,
            node_type=node_type,
            code=code,
            token=calculate_code_token(code),
            has_children=bool(child_nodes),
            procedure_key=procedure_key,
            ...
        )
        
        # 4. 부모-자식 관계 설정
        for child_node in child_nodes:
            child_node.parent = statement_node
        statement_node.children = child_nodes
        
        return statement_node
\`\`\`

#### **중요: 후위순회를 사용하는 이유**

- **자식 노드의 요약이 먼저 필요**: 부모 노드는 자식 요약을 기반으로 compact code 생성
- **의존성 해결**: 부모 LLM 호출 전에 자식 LLM 호출이 완료되어야 함

### 4.7 Step 6: 배치 플래닝 (BatchPlanner)

#### **배치 플래닝이란?**

토큰 한도(기본 1000 토큰)를 넘지 않도록 노드를 묶어서 LLM에 전달하는 전략입니다.

\`\`\`python
# understand/analysis.py

class BatchPlanner:
    def plan(self, nodes, folder_file):
        batches = []
        current_nodes = []
        current_tokens = 0
        
        for node in nodes:
            if not node.analyzable:
                continue
            
            # 부모 노드는 단독 배치
            if node.has_children:
                if current_nodes:
                    batches.append(self._create_batch(batch_id, current_nodes))
                    batch_id += 1
                    current_nodes = []
                    current_tokens = 0
                
                # 부모 노드 단독 배치
                batches.append(self._create_batch(batch_id, [node]))
                batch_id += 1
                continue
            
            # 토큰 한도 초과 시 배치 확정
            if current_tokens + node.token > self.token_limit:
                batches.append(self._create_batch(batch_id, current_nodes))
                batch_id += 1
                current_nodes = []
                current_tokens = 0
            
            current_nodes.append(node)
            current_tokens += node.token
        
        return batches
\`\`\`

#### **배치 예시**

\`\`\`
[Batch 1] 리프 노드 (SELECT, INSERT, UPDATE) - 800 tokens
[Batch 2] 부모 노드 (IF) - 200 tokens (단독)
[Batch 3] 리프 노드 (LOOP 내부) - 900 tokens
[Batch 4] 부모 노드 (PROCEDURE) - 300 tokens (단독)
\`\`\`

### 4.8 Step 7: LLM 병렬 호출 (LLMInvoker)

#### **병렬 호출 전략**

\`\`\`python
# understand/analysis.py

class LLMInvoker:
    async def invoke(self, batch):
        # 일반 분석 태스크
        general_task = asyncio.to_thread(
            understand_code,
            batch.build_general_payload(),
            batch.ranges,
            len(batch.ranges),
            self.api_key,
            self.locale,
        )
        
        # DML 테이블 분석 태스크 (병렬)
        table_task = asyncio.to_thread(
            understand_dml_tables,
            batch.build_dml_payload(),
            batch.dml_ranges,
            self.api_key,
            self.locale,
        )
        
        # 병렬 실행
        return await asyncio.gather(general_task, table_task)
\`\`\`

#### **LLM 프롬프트별 역할**

| 프롬프트 파일 | 역할 | 입력 | 출력 |
|--------------|------|------|------|
| **understand_prompt.py** | 코드 동작 분석 | 코드 범위 | summary, calls, variables |
| **understand_dml_table_prompt.py** | DML 테이블/컬럼 추출 | DML 코드 | table, columns, fkRelations, dbLinks |
| **understand_variables_prompt.py** | 변수 선언 분석 | DECLARE/SPEC 코드 | variables (name, type, parameter_type) |
| **understand_summarized_prompt.py** | 프로시저 전체 요약 | 하위 노드 요약들 | 프로시저 전체 summary |
| **understand_column_prompt.py** | 컬럼 역할 분석 | 컬럼 메타 + DML 요약 | 컬럼별 역할 라벨 |
| **understand_table_summary_prompt.py** | 테이블 설명 요약 | 테이블 설명 문장들 | 통합된 tableDescription |

### 4.9 Step 8: Neo4j 반영 (ApplyManager)

#### **Apply Manager의 역할**

LLM 결과를 **순서대로** Neo4j에 반영합니다. (배치 ID 순서 보장)

\`\`\`python
# understand/analysis.py

class ApplyManager:
    async def submit(self, batch, general, table):
        async with self._lock:
            # 순서 보장을 위한 대기
            self._pending[batch.batch_id] = BatchResult(batch, general, table)
            await self._flush_ready()
    
    async def _flush_ready(self):
        # 배치 ID 순서대로 적용
        while self._next_batch_id in self._pending:
            result = self._pending.pop(self._next_batch_id)
            await self._apply_batch(result)
            self._next_batch_id += 1
\`\`\`

#### **Neo4j 쿼리 생성 예시**

\`\`\`python
def _build_node_queries(self, node, analysis):
    summary = analysis.get('summary')
    
    # 노드 속성 업데이트
    query = f"""
    MERGE (n:{node.node_type} {{startLine: {node.start_line}, ...}})
    SET n.summary = '{escape_summary(summary)}',
        n.node_code = '{escape_for_cypher(node.code)}',
        n.token = {node.token}
    """
    
    # 변수 사용 표시
    for var in analysis.get('variables', []):
        query += f"""
        MATCH (v:Variable {{name: '{var}', ...}})
        SET v.\`{node.start_line}_{node.end_line}\` = 'Used'
        """
    
    # 프로시저 호출 관계
    for call in analysis.get('calls', []):
        query += f"""
        MATCH (c:{node.node_type} {{startLine: {node.start_line}, ...}})
        MATCH (p:PROCEDURE {{procedure_name: '{call}', ...}})
        MERGE (c)-[:CALL]->(p)
        """
    
    return [query]
\`\`\`

### 4.10 Step 9: 후처리

#### **후처리가 필요한 이유**

- **변수 타입 해석**: `%ROWTYPE`, `%TYPE` 등은 테이블 정보가 필요

```python
# service/service.py

async def _postprocess_file(self, connection, folder_name, file_name, file_pairs):
    # 변수 타입 해석
    var_rows = await connection.execute_queries(["""
        MATCH (v:Variable {folder_name: '...', file_name: '...'})
        MATCH (t:Table {name: toUpper(tableName)})
        OPTIONAL MATCH (t)-[:HAS_COLUMN]->(c:Column)
        RETURN v.name, v.type, t.schema, t.name, collect(c) AS columns
    """])
    
    for row in var_rows:
        result = await resolve_table_variable_type(
            row['varName'], row['type'], row['schema'], row['table'],
            row['columns'], self.api_key, self.locale
        )
        # 해석된 타입으로 업데이트
        await connection.execute_queries([f"MATCH (v:Variable {{name: '{row['varName']}'}}) SET v.type = '{result['resolvedType']}'"])
```

### 4.11 Step 10: SSE 스트리밍

\`\`\`python
# service/service.py

async def _analyze_file(self, ...):
    while True:
        analysis_result = await events_from_analyzer.get()
        
        if analysis_result['type'] == 'end_analysis':
            # 후처리 실행
            await self._postprocess_file(...)
            # 최종 그래프 전송
            graph_result = await connection.execute_query_and_return_graph(...)
            yield emit_data(graph=graph_result, analysis_progress=100)
            break
        
        # 중간 진행 상황 전송
        await connection.execute_queries(analysis_result['query_data'])
        graph_result = await connection.execute_query_and_return_graph(...)
        yield emit_data(graph=graph_result, line_number=..., analysis_progress=...)
\`\`\`

### 4.12 컨텍스트 최대 토큰 감지 및 배치 분할

#### **문제 상황**

- LLM API에는 최대 컨텍스트 길이 제한이 있음 (예: GPT-4의 경우 128k 토큰)
- 하나의 배치가 이 한도를 초과하면 API 호출이 실패함

#### **해결 방법: 배치 플래닝**

\`\`\`python
# understand/analysis.py

class BatchPlanner:
    def __init__(self, token_limit: int = MAX_BATCH_TOKEN):
        self.token_limit = token_limit  # 기본 1000 토큰
\`\`\`

- **토큰 계산**: tiktoken 라이브러리로 정확한 토큰 수 계산
- **배치 분할**: 누적 토큰이 한도를 초과하기 직전에 배치 확정
- **부모 노드 처리**: 부모 노드는 자식 요약이 완료된 후 단독 배치로 처리

---

## 5. 시퀀스 다이어그램

### 5.1 전체 Understanding 플로우

\`\`\`mermaid
sequenceDiagram
    participant Client as Frontend
    participant Router as service/router.py
    participant Service as service/service.py
    participant Analyzer as understand/analysis.py
    participant LLM as LLM API
    participant Neo4j as Neo4j DB

    Client->>Router: POST /cypherQuery/
    Router->>Service: ServiceOrchestrator.understand_project()
    
    Service->>Neo4j: DDL 파일 처리 (선택)
    Neo4j-->>Service: Table/Column 노드 생성 완료
    
    Service->>Service: ANTLR JSON 및 PL/SQL 로드
    Service->>Analyzer: Analyzer.run()
    
    Analyzer->>Analyzer: StatementCollector.collect()
    Note over Analyzer: AST 후위순회, StatementNode 생성
    
    Analyzer->>Neo4j: 정적 노드 생성 (FILE, PROCEDURE 등)
    Analyzer->>Neo4j: 관계 생성 (PARENT_OF, NEXT)
    Analyzer->>Neo4j: 변수 노드 생성 (DECLARE, SPEC)
    
    Analyzer->>Analyzer: BatchPlanner.plan()
    Note over Analyzer: 토큰 한도 기준 배치 분할
    
    loop 각 배치별 병렬 처리
        Analyzer->>LLM: understand_code (일반 분석)
        Analyzer->>LLM: understand_dml_tables (테이블 분석)
        LLM-->>Analyzer: summary, calls, variables
        LLM-->>Analyzer: table, columns, fkRelations
        
        Analyzer->>Neo4j: 분석 결과 반영 (순서 보장)
        Neo4j-->>Service: 중간 그래프
        Service-->>Client: SSE 이벤트 (progress)
    end
    
    Analyzer->>LLM: understand_summary (프로시저 요약)
    LLM-->>Analyzer: 프로시저 전체 summary
    Analyzer->>Neo4j: 프로시저 요약 반영
    
    Service->>LLM: resolve_table_variable_type (변수 타입 해석)
    LLM-->>Service: 해석된 변수 타입
    Service->>Neo4j: 변수 타입 업데이트
    
    Service->>LLM: understand_column_roles (컬럼 역할 분석)
    LLM-->>Service: 컬럼별 역할
    Service->>Neo4j: 컬럼 역할 업데이트
    
    Neo4j-->>Service: 최종 그래프
    Service-->>Client: SSE 이벤트 (완료)
\`\`\`

### 5.2 클래스별 상호작용

\`\`\`mermaid
classDiagram
    class ServiceOrchestrator {
        +understand_project(file_names)
        -_process_ddl(ddl_file_path)
        -_analyze_file(folder_name, file_name)
        -_postprocess_file(connection, folder, file)
    }
    
    class Analyzer {
        +run()
        -_initialize_static_graph(nodes)
        -_create_static_nodes(nodes)
        -_create_relationships(nodes)
        -_process_variable_nodes(nodes)
        -_wait_for_dependencies(batch)
    }
    
    class StatementCollector {
        +collect()
        -_visit(node, current_proc)
        -_make_proc_key(procedure_name, start_line)
    }
    
    class BatchPlanner {
        +plan(nodes, folder_file)
        -_create_batch(batch_id, nodes)
    }
    
    class LLMInvoker {
        +invoke(batch)
    }
    
    class ApplyManager {
        +submit(batch, general, table)
        +finalize()
        -_flush_ready()
        -_apply_batch(result)
        -_build_node_queries(node, analysis)
        -_build_table_queries(batch, table_result)
        -_finalize_procedure_summary(info)
        -_finalize_table_summaries()
    }
    
    class Neo4jConnection {
        +execute_queries(queries)
        +execute_query_and_return_graph(user_id, file_names)
        +node_exists(user_id, file_names)
    }
    
    ServiceOrchestrator --> Analyzer: 생성 및 실행
    Analyzer --> StatementCollector: AST 수집
    Analyzer --> BatchPlanner: 배치 생성
    Analyzer --> LLMInvoker: LLM 호출
    Analyzer --> ApplyManager: 결과 반영
    ApplyManager --> Neo4jConnection: 쿼리 실행
    ServiceOrchestrator --> Neo4jConnection: DDL/후처리
\`\`\`

---

## 6. 파일별 상세 가이드

### 6.1 main.py - 애플리케이션 진입점

**역할**: FastAPI 앱 초기화 및 서버 실행

\`\`\`python
from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware
import uvicorn
from service.router import router

app = FastAPI()

# CORS 설정
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# 라우터 등록
app.include_router(router)

@app.get("/")
async def health_check():
    return {"status": "ok"}

if __name__ == "__main__":
    uvicorn.run(app, host="0.0.0.0", port=5502)
\`\`\`

**실행 방법**:
\`\`\`bash
python main.py
# 또는
uvicorn main:app --reload --port 5502
\`\`\`

---

### 6.2 service/router.py - API 엔드포인트

**역할**: HTTP 요청을 받아 ServiceOrchestrator로 전달

#### **주요 함수**

| 엔드포인트 | 메서드 | 역할 |
|-----------|--------|------|
| \`/cypherQuery/\` | POST | Understanding 파이프라인 실행 |
| \`/convert/\` | POST | Converting 파이프라인 실행 |
| \`/downloadJava/\` | POST | 생성된 프로젝트 다운로드 |
| \`/deleteAll/\` | DELETE | 사용자 데이터 전체 삭제 |

---

### 6.3 service/service.py - ServiceOrchestrator

**역할**: Understanding/Converting의 최상위 오케스트레이터

#### **주요 메서드**

| 메서드 | 역할 | 비동기 | 핵심 로직 |
|--------|------|--------|-----------|
| \`validate_api_key()\` | API 키 유효성 검증 | ✅ | LLM ping 호출 |
| \`understand_project()\` | 전체 프로젝트 분석 | ✅ | DDL → PL/SQL → 후처리 |
| \`_process_ddl()\` | DDL 파일 처리 | ✅ | LLM 파싱 → Neo4j |
| \`_analyze_file()\` | 단일 파일 분석 | ✅ | Analyzer 실행 |
| \`_postprocess_file()\` | 후처리 (변수/컬럼) | ✅ | 타입 해석, 역할 분석 |
| \`_load_assets()\` | ANTLR JSON/PL/SQL 로드 | ✅ | aiofiles 병렬 읽기 |
| \`_ensure_folder_node()\` | SYSTEM 노드 생성 | ✅ | Neo4j MERGE |

---

### 6.4 understand/analysis.py - Analyzer 핵심

**역할**: Understanding 파이프라인의 심장부

#### **주요 클래스**

##### **StatementNode (데이터 클래스)**

`StatementNode`는 AST에서 수집한 한 구간의 메타데이터를 담는 핵심 모델입니다. 라인 범위, 노드 타입, 원본 코드, 자식 정보 등을 보유하여 이후 배치 생성과 LLM 호출, Neo4j 반영까지 모든 단계에서 공통으로 사용됩니다.

```python
@dataclass(slots=True)
class StatementNode:
    node_id: int
    start_line: int
    end_line: int
    node_type: str
    code: str              # 라인 번호 포함 코드
    token: int
    has_children: bool
    procedure_key: Optional[str]
    procedure_type: Optional[str]
    procedure_name: Optional[str]
    schema_name: Optional[str]
    analyzable: bool       # LLM 분석 대상 여부
    dml: bool              # DML 노드 여부
    lines: List[Tuple[int, str]]
    parent: Optional[StatementNode]
    children: List[StatementNode]
    summary: Optional[str]
    completion_event: asyncio.Event
    
    def get_raw_code(self) -> str:
        """라인 번호 포함 원문 코드"""
        return '\n'.join(f"{line_no}: {text}" for line_no, text in self.lines)
    
    def get_compact_code(self) -> str:
        """자식 요약을 포함한 부모 코드 (LLM 입력용)"""
        # 자식 구간은 요약으로 대체, 부모 고유 코드만 원문 유지
        ...
```

##### **StatementCollector (AST 수집기)**

`StatementCollector`는 ANTLR JSON을 후위순회하며 `StatementNode` 객체를 만들어 냅니다. 프로시저 단위로 노드를 묶고, 부모-자식 관계를 연결하여 이후 배치 및 적용 단계에서 의존성을 추적할 수 있게 합니다.

```python
class StatementCollector:
    def collect(self):
        # 후위순회: 자식 → 부모 순서 보장
        self._visit(self.antlr_data, current_proc=None, ...)
        return self.nodes, self.procedures
    
    def _visit(self, node, current_proc, current_type, current_schema):
        # 1. 자식 먼저 방문 (후위순회)
        for child in children:
            child_node = self._visit(child, ...)
        
        # 2. StatementNode 생성
        statement_node = StatementNode(...)
        
        # 3. 부모-자식 관계 설정
        for child_node in child_nodes:
            child_node.parent = statement_node
        statement_node.children = child_nodes
        
        return statement_node
```

##### **BatchPlanner (배치 생성기)**

`BatchPlanner`는 LLM 토큰 한도를 넘지 않도록 StatementNode 목록을 잘게 묶습니다. 부모 노드는 의존성 때문에 단독으로 보내고, 리프 노드는 토큰 합이 허용 범위 내에서 묶어 전송하는 전략을 사용합니다.

```python
class BatchPlanner:
    def plan(self, nodes, folder_file):
        for node in nodes:
            if node.has_children:
                # 부모는 단독 배치
                batches.append([node])
            elif current_tokens + node.token > limit:
                # 토큰 한도 초과 시 배치 확정
                batches.append(current_nodes)
                current_nodes = []
            else:
                current_nodes.append(node)
        return batches
```

##### **LLMInvoker (LLM 호출기)**

`LLMInvoker`는 하나의 배치를 받아 일반 요약과 DML 테이블 분석을 병렬로 수행합니다. CPU 바운드 LLM 호출을 `asyncio.to_thread`로 감싸 비동기 코드와 조화시키는 것이 특징입니다.

```python
class LLMInvoker:
    async def invoke(self, batch):
        general_task = asyncio.to_thread(understand_code, ...)
        table_task = asyncio.to_thread(understand_dml_tables, ...)
        return await asyncio.gather(general_task, table_task)
```

##### **ApplyManager (결과 반영기)**

`ApplyManager`는 LLM에서 돌아온 결과를 배치 순서에 맞춰 Neo4j에 반영합니다. 순서 보장을 위해 내부 큐를 사용하고, 노드/테이블 쿼리를 생성한 뒤 전송 큐를 통해 Analyzer와 동기화합니다.

```python
class ApplyManager:
    async def submit(self, batch, general, table):
        # 순서 보장
        self._pending[batch.batch_id] = BatchResult(...)
        await self._flush_ready()
    
    async def _apply_batch(self, result):
        # Neo4j 쿼리 생성
        queries = self._build_node_queries(...)
        queries.extend(self._build_table_queries(...))
        await self._send_queries(queries, ...)
```

---

### 6.5 understand/neo4j_connection.py

**역할**: Neo4j 비동기 연결 및 쿼리 실행

#### **주요 메서드**

| 메서드 | 역할 | 반환 타입 |
|--------|------|-----------|
| \`execute_queries(queries)\` | Cypher 쿼리 실행 | List[Dict] |
| \`execute_query_and_return_graph()\` | 그래프 조회 | Dict (nodes, relationships) |
| \`node_exists()\` | 노드 존재 여부 확인 | bool |

\`\`\`python
class Neo4jConnection:
    DATABASE_NAME = "neo4j"
    
    async def execute_queries(self, queries: list) -> list:
        results = []
        async with self.__driver.session(database=self.DATABASE_NAME) as session:
            for query in queries:
                query_result = await session.run(query)
                results.append(await query_result.data())
        return results
\`\`\`

---

### 6.6 prompt/ 폴더 - LLM 프롬프트

#### **understand_prompt.py (일반 코드 분석)**

**역할**: 코드 동작 요약, 변수 사용, 프로시저 호출 식별

**입력**:
\`\`\`python
{
  "code": "9: SELECT * FROM orders WHERE order_id = p_order_id\\n10: INTO v_order_date, v_total_amount;",
  "ranges": [{"startLine": 9, "endLine": 10}],
  "count": 1,
  "locale": "ko"
}
\`\`\`

**출력**:
\`\`\`json
{
  "analysis": [
    {
      "startLine": 9,
      "endLine": 10,
      "summary": "주문 테이블에서 주문 ID로 주문 날짜와 총 금액을 조회합니다.",
      "calls": [],
      "variables": ["p_order_id", "v_order_date", "v_total_amount"]
    }
  ]
}
\`\`\`

#### **understand_dml_table_prompt.py (DML 테이블 분석)**

**역할**: DML 구문에서 테이블, 컬럼, FK 관계, DB 링크 추출

**출력**:
\`\`\`json
{
  "tables": [
    {
      "startLine": 9,
      "endLine": 10,
      "dmlType": "SELECT",
      "table": "SALES.ORDERS",
      "tableDescription": "주문 정보를 조회합니다.",
      "columns": [
        {"name": "ORDER_ID", "dtype": "INTEGER", "nullable": false, "description": "주문 번호 조건"},
        {"name": "ORDER_DATE", "dtype": "DATE", "nullable": true, "description": "주문 날짜 조회"},
        {"name": "TOTAL_AMOUNT", "dtype": "DECIMAL", "nullable": true, "description": "총 금액 조회"}
      ],
      "fkRelations": [],
      "dbLinks": []
    }
  ]
}
\`\`\`

#### **understand_variables_prompt.py (변수 선언 분석)**

**역할**: DECLARE/SPEC 구간의 변수 선언 정보 추출

**출력**:
\`\`\`json
{
  "variables": [
    {"name": "p_order_id", "type": "INTEGER", "value": null, "parameter_type": "IN"},
    {"name": "v_order_date", "type": "DATE", "value": null, "parameter_type": "LOCAL"},
    {"name": "v_total_amount", "type": "DECIMAL", "value": null, "parameter_type": "LOCAL"}
  ],
  "summary": "주문 ID를 입력받아 주문 정보를 조회하기 위한 변수들을 선언합니다."
}
\`\`\`

#### **understand_summarized_prompt.py (프로시저 전체 요약)**

**역할**: 하위 노드 요약들을 모아 프로시저 전체 동작 요약

**입력**:
\`\`\`python
{
  "summaries": {
    "SELECT_9_10": "주문 정보를 조회합니다.",
    "IF_12_15": "주문 금액이 1000을 초과하면 할인을 적용합니다.",
    "INSERT_17_18": "주문 히스토리에 기록합니다."
  },
  "locale": "ko"
}
\`\`\`

**출력**:
\`\`\`json
{
  "summary": "이 프로시저는 주문 ID를 받아 주문 정보를 조회하고, 주문 금액에 따라 할인을 적용한 후, 주문 히스토리에 기록합니다. 최종적으로 처리 결과를 커밋합니다."
}
\`\`\`

#### **understand_column_prompt.py (컬럼 역할 분석)**

**역할**: DML 사용 패턴을 기반으로 컬럼의 역할 라벨 추론

**입력**:
\`\`\`python
{
  "columns_json": [
    {"name": "ORDER_ID", "dtype": "INTEGER", "nullable": false},
    {"name": "ORDER_DATE", "dtype": "DATE", "nullable": true}
  ],
  "dml_summaries_json": ["주문 정보를 조회합니다.", "주문 히스토리에 기록합니다."],
  "locale": "ko"
}
\`\`\`

**출력**:
\`\`\`json
{
  "tableDescription": "주문 마스터 테이블로, 주문 기본 정보를 저장하고 조회/기록하는 데 사용됩니다.",
  "roles": [
    {"name": "ORDER_ID", "role": "주문 식별자"},
    {"name": "ORDER_DATE", "role": "주문 일시"}
  ]
}
\`\`\`

---

### 6.7 util/utility_tool.py - 유틸리티

#### **주요 함수**

| 함수 | 역할 | 입력 | 출력 |
|------|------|------|------|
| \`calculate_code_token(code)\` | 토큰 수 계산 | str/dict/list | int |
| \`add_line_numbers(plsql)\` | 라인 번호 추가 | List[str] | str, List[str] |
| \`escape_for_cypher(text)\` | Cypher 이스케이프 | str | str |
| \`parse_table_identifier(name)\` | 테이블명 파싱 | str | (schema, table, dblink) |
| \`emit_message(content)\` | 메시지 이벤트 | str | bytes |
| \`emit_data(**fields)\` | 데이터 이벤트 | dict | bytes |
| \`emit_error(content)\` | 에러 이벤트 | str | bytes |

#### **parse_table_identifier 예시**

\`\`\`python
parse_table_identifier("SALES.ORDERS@DBLINK1")
# → ("SALES", "ORDERS", "DBLINK1")

parse_table_identifier("ORDERS")
# → ("", "ORDERS", None)
\`\`\`

---

### 6.8 util/llm_client.py - LLM 클라이언트

**역할**: LLM API 클라이언트 생성 (OpenAI 호환)

\`\`\`python
def get_llm(model=None, temperature=0.1, max_tokens=None, api_key=None, base_url=None):
    base_url = base_url or os.getenv("LLM_API_BASE", "https://api.openai.com/v1")
    api_key = api_key or os.getenv("LLM_API_KEY")
    model = model or os.getenv("LLM_MODEL", "gpt-4.1")
    
    return ChatOpenAI(
        model=model,
        openai_api_key=api_key,
        openai_api_base=base_url,
        max_tokens=max_tokens,
        temperature=temperature
    )
\`\`\`

---

## 7. 개발 환경 설정

### 7.1 필수 소프트웨어 설치

| 항목 | 버전 | 설치 방법 |
|------|------|-----------|
| Python | 3.10+ | [python.org](https://www.python.org/) |
| Neo4j Desktop | 5.x | [neo4j.com/download](https://neo4j.com/download/) |

### 7.2 프로젝트 설정

\`\`\`bash
# 1. 저장소 클론
git clone <repository-url>
cd backend

# 2. 가상 환경 생성 및 활성화
uv venv
source .venv/Scripts/activate
# 또는 (pipenv 사용 시)
pipenv shell

# 3. 의존성 설치
uv pip install -r requirements.txt
# 또는 (pipenv 사용 시)
pipenv install

### 7.3 환경 변수 설정 (.env)

\`\`\`bash
# Neo4j 연결 정보
NEO4J_URI=bolt://localhost:7687
NEO4J_USER=neo4j
NEO4J_PASSWORD=your-password

# LLM API 설정
LLM_API_KEY=sk-...
LLM_API_BASE=https://api.openai.com/v1
LLM_MODEL=gpt-4-turbo
LLM_MAX_TOKENS=32768

# 커스텀 LLM (선택)
IS_CUSTOM_LLM=false
COMPANY_NAME=posco

# Docker (선택)
DOCKER_COMPOSE_CONTEXT=/app
\`\`\`

### 7.4 Neo4j 초기 설정

Neo4j Desktop 설치는 아래 문서를 참고하세요.
- [Neo4j Desktop 설치 가이드](https://1004jonghee.tistory.com/entry/Neo4j-Desktop-%EC%84%A4%EC%B9%98)
설치가 완료되면 `.env`에 맞춰 아이디와 비밀번호를 세팅하세요. 

---

## 8. 테스트 실행 가이드

### 8.1 pytest 설정

#### **pytest.ini**

\`\`\`ini
[pytest]
asyncio_mode = auto
pythonpath = .
testpaths = test
python_files = test_*.py
python_classes = Test*
python_functions = test_*
\`\`\`

### 8.2 Understanding 테스트 실행

\`\`\`bash
# 이해 파이프라인 테스트 (기본값: 리팩터)
pytest test/test_understanding.py -v

# 특정 변형(레거시/리팩터)만 실행
UNDERSTANDING_VARIANT=refactor pytest test/test_understanding.py -v

# 비교 모드 (레거시와 리팩터를 순차 실행)
UNDERSTANDING_VARIANT=compare pytest test/test_understanding.py -v
\`\`\`

- `UNDERSTANDING_VARIANT`를 지정하지 않으면 리팩터 Analyzer만 실행됩니다.
- `UNDERSTANDING_VARIANT=legacy`로 설정하면 레거시 Analyzer만 실행합니다. (기존 파이프라인)
- `UNDERSTANDING_VARIANT=refactor`는 리팩터 Analyzer만 실행합니다. (최신 구조, LLM·배치 로직 개선 버전)
- `UNDERSTANDING_VARIANT=compare`는 레거시와 리팩터를 순서대로 실행해 이벤트 수·소요 시간 등을 비교합니다.

#### **테스트 준비 사항**

1. **테스트 전용 data 폴더 구성**
   - `data/<세션ID>/<프로젝트명>/src/` 및 `analysis/` 구조를 직접 준비하세요.
   - 예: `data/KO_TestSession/HOSPITAL_MANAGEMENT/src/SYSTEM/proc_test.sql`
   - 동일한 위치에 ANTLR JSON(`analysis/SYSTEM/proc_test.json`)도 있어야 합니다.

2. **별도 Neo4j 테스트 데이터베이스 추천**
   - 운영 데이터베이스와 분리된 `test` DB를 생성한 뒤 `.env`에서 연결하도록 권장합니다.
   - 테스트 실행 전 해당 DB의 데이터를 비우는 스크립트를 함께 준비하면 안전합니다.

3. **환경 변수**
   ```bash
   export LLM_API_KEY=sk-...
   ```

### 8.3 settings.json 설정

VSCode에서 pytest를 기본 테스트 러너로 사용하려면 `.vscode/settings.json`을 다음과 같이 맞춥니다.

```json
// .vscode/settings.json
{
    "python.testing.pytestArgs": [
        "test"
    ],
    "python.testing.unittestEnabled": false,
    "python.testing.pytestEnabled": true
}
```

### 8.4 launch.json 설정

`launch.json` 파일을 아래처럼 맞추면, 현재 저장소에서 실제 사용하는 디버깅 구성과 동일합니다.

```json
// .vscode/launch.json
{
    "version": "0.2.0",
    "configurations": [
        {
            "name": "Python Debugger: Current File",
            "type": "debugpy",
            "request": "launch",
            "program": "${file}",
            "console": "integratedTerminal"
        },
        {
            "name": "Pytest: Debug Current File",
            "type": "debugpy",
            "request": "launch",
            "module": "pytest",
            "args": [
                "${file}",
                "-s"
            ],
            "console": "integratedTerminal",
            "justMyCode": false
        }
    ]
}
```

- 첫 번째 구성은 현재 열려 있는 Python 파일을 즉시 디버깅할 때 사용합니다.
- 두 번째 구성은 현재 파일을 pytest로 실행하면서 디버그 세션을 붙입니다 (`-s`로 표준 출력 유지).
- FastAPI 서버 디버깅 등이 필요하면 위 구성을 기반으로 별도 엔트리를 추가하면 됩니다.

#### **디버깅 방법**

1. `F5` 키로 실행
2. 중단점 설정: 코드 라인 번호 왼쪽 클릭
3. 변수 확인: Debug Console에서 변수명 입력
4. 단계별 실행: F10 (다음 라인), F11 (함수 내부)

---

## 9. Neo4j 활용 가이드

### 9.1 Neo4j Browser 접속

1. **Neo4j Desktop에서 Start**
2. **Open Browser** 클릭

### 9.2 자주 사용하는 Cypher 쿼리

#### **모든 노드 조회**

\`\`\`cypher
MATCH (n)
RETURN n
\`\`\`

#### **특정 사용자의 노드만 조회**

\`\`\`cypher
MATCH (n {user_id: 'KO_TestSession'})
RETURN n
\`\`\`

#### **프로시저 노드 조회**

\`\`\`cypher
MATCH (p:PROCEDURE)
RETURN p.procedure_name AS name, p.summary AS summary
\`\`\`

#### **테이블 및 컬럼 조회**

\`\`\`cypher
MATCH (t:Table)-[:HAS_COLUMN]->(c:Column)
WHERE t.user_id = 'user123'
RETURN t.name AS table_name, collect(c.name) AS columns
\`\`\`

#### **프로시저 호출 관계**

\`\`\`cypher
MATCH (caller)-[:CALL]->(callee:PROCEDURE)
WHERE caller.user_id = 'user123'
RETURN caller.procedure_name AS caller, callee.procedure_name AS callee
\`\`\`

#### **DML과 테이블 관계**

\`\`\`cypher
MATCH (dml)-[r:FROM|WRITES]->(t:Table)
WHERE dml.user_id = 'user123'
RETURN type(r) AS relation, labels(dml)[0] AS dml_type, t.name AS table_name
\`\`\`

#### **모든 노드 및 관계 삭제 (주의!)**

\`\`\`cypher
MATCH (n {user_id: 'user123'})
DETACH DELETE n
\`\`\`

### 9.3 Neo4j Browser 즐겨찾기 설정

1. **쿼리 입력 후 실행**
2. **⭐ 아이콘 클릭**
3. **이름 입력** (예: "모든 프로시저 조회")
4. **Save** 클릭

### 9.4 노드가 안 보이는 경우 (Limit 해제)

#### **설정 변경 방법**

1. Browser 우측 상단 **⚙️ 아이콘** 클릭
2. **Initial Node Display** 찾기
3. 값 변경: `25` → `1000`
4. **Apply** 클릭

또는 쿼리에 직접 LIMIT 추가:

\`\`\`cypher
MATCH (n {user_id: 'user123'})
RETURN n
LIMIT 1000
\`\`\`

### 9.5 그래프 시각화 팁

- **노드 더블클릭**: 연결된 노드 펼치기
- **노드 고정**: 드래그 후 고정 아이콘 클릭
- **레이아웃 재정렬**: 하단 **Layout** 버튼
- **라벨 표시**: 우측 패널에서 Caption 설정

---

## 11. 마무리 및 다음 단계

### 11.1 이 문서를 읽은 후 할 수 있는 것

✅ Understanding 파이프라인의 전체 흐름 이해
✅ 각 파일과 클래스의 역할 파악
✅ ANTLR JSON 구조와 Neo4j 그래프 매핑 이해
✅ 테스트 환경 구축 및 실행
✅ Neo4j 쿼리로 분석 결과 확인
✅ 문제 발생 시 트러블슈팅

### 11.2 수정이 필요한 경우

#### **프롬프트 수정**

- \`prompt/*.py\` 파일의 프롬프트 템플릿 수정
- LLM 결과 형식 변경 시 파싱 로직도 수정

#### **배치 전략 수정**

- \`understand/analysis.py\` → \`BatchPlanner\` 클래스
- 토큰 한도, 분할 기준 등 조정

#### **Neo4j 스키마 변경**

- \`understand/analysis.py\` → \`ApplyManager._build_*_queries\` 메서드
- 노드 속성, 관계 타입 변경

### 11.3 Converting 단계 (간략 소개)

Converting 단계는 Understanding에서 생성한 Neo4j 그래프를 기반으로 타겟 언어 코드를 생성합니다.

#### **주요 파일**

- \`conversion/strategies/\`: 전략 패턴 (Framework, DBMS)
- \`convert/*.py\`: 코드 생성 모듈 (Entity, Service, Repository 등)
- \`rules/{java|python}/*.yaml\`: 코드 생성 템플릿

#### **Converting 문서**

Converting에 대한 상세한 문서는 별도로 제공됩니다.

---

## 참고 자료

- [FastAPI 공식 문서](https://fastapi.tiangolo.com/)
- [Neo4j Cypher 매뉴얼](https://neo4j.com/docs/cypher-manual/current/)
- [LangChain 문서](https://python.langchain.com/)
- [OpenAI API 문서](https://platform.openai.com/docs/api-reference)

---

## 라이선스

이 프로젝트는 내부 사용을 위한 것입니다.

---

**작성일**: 2025-01-10
**버전**: 2.0.0
**작성자**: Legacy Modernizer Team

---

## 부록: 고급 주제

### A. Understanding 성능 최적화

#### A.1 병렬 처리 전략

Understanding 파이프라인은 여러 단계에서 병렬 처리를 활용합니다:

1. **DDL 파일 병렬 처리**
   \`\`\`python
   # service/service.py
   DDL_MAX_CONCURRENCY = 5
   
   ddl_semaphore = asyncio.Semaphore(DDL_MAX_CONCURRENCY)
   ddl_tasks = []
   
   for ddl_file in ddl_files:
       async with ddl_semaphore:
           await self._process_ddl(ddl_file, ...)
   \`\`\`

2. **배치 LLM 호출 병렬 처리**
   \`\`\`python
   # understand/analysis.py
   MAX_CONCURRENCY = 5
   
   semaphore = asyncio.Semaphore(min(self.max_workers, len(batches)))
   await asyncio.gather(*(worker(batch) for batch in batches))
   \`\`\`

3. **변수 분석 병렬 처리**
   \`\`\`python
   # understand/analysis.py
   VARIABLE_CONCURRENCY = 5
   
   await asyncio.gather(*(worker(node) for node in targets))
   \`\`\`

#### A.2 캐싱 전략

LLM 호출 결과를 캐싱하여 동일한 요청에 대한 중복 호출을 방지합니다:

\`\`\`python
# prompt/*.py
from langchain_core.globals import set_llm_cache
from langchain_community.cache import SQLiteCache

db_path = os.path.join(os.path.dirname(__file__), 'langchain.db')
set_llm_cache(SQLiteCache(database_path=db_path))
\`\`\`

**캐시 파일 위치**: \`prompt/langchain.db\`

**캐시 삭제 방법**:
\`\`\`bash
rm prompt/langchain.db
\`\`\`

#### A.3 메모리 최적화

\`\`\`python
# understand/analysis.py

# slots를 사용한 메모리 최적화
@dataclass(slots=True)
class StatementNode:
    ...
\`\`\`

\`slots=True\`를 사용하면 약 40-50%의 메모리를 절약할 수 있습니다.

---

### B. 노드 타입별 상세 분석

#### B.1 FILE 노드

- **역할**: 파일 최상위 루트 노드
- **속성**:
  - \`name\`: 파일명
  - \`folder_name\`: 폴더명
  - \`summary\`: "파일 노드" 또는 "File Start Node"
- **관계**:
  - \`-[:CONTAINS]->(PROCEDURE)\`
  - \`-[:CONTAINS]->(PACKAGE_VARIABLE)\`

#### B.2 PROCEDURE 노드

- **역할**: 저장 프로시저 또는 함수
- **속성**:
  - \`procedure_name\`: 프로시저 이름
  - \`schema_name\`: 스키마 이름
  - \`summary\`: LLM이 생성한 전체 동작 요약
- **관계**:
  - \`-[:PARENT_OF]->(SPEC|DECLARE|...)\`
  - \`<-[:CALL]-(다른 프로시저)\`

#### B.3 SPEC 노드

- **역할**: 프로시저 파라미터 선언부
- **속성**:
  - \`summary\`: 파라미터 역할 요약
- **관계**:
  - \`-[:SCOPE]->(Variable)\`

#### B.4 DECLARE 노드

- **역할**: 로컬 변수 선언부
- **속성**:
  - \`summary\`: 변수 선언 요약
- **관계**:
  - \`-[:SCOPE]->(Variable)\`

#### B.5 SELECT/INSERT/UPDATE/DELETE 노드

- **역할**: DML 구문
- **속성**:
  - \`summary\`: DML 동작 요약
  - \`node_code\`: 실제 SQL 코드
- **관계**:
  - \`-[:FROM]->(Table)\`: SELECT, FETCH
  - \`-[:WRITES]->(Table)\`: INSERT, UPDATE, DELETE
  - \`-[:HAS_COLUMN]->(Column)\`: (테이블을 통해)

#### B.6 IF/LOOP/CASE 노드

- **역할**: 제어 구조
- **속성**:
  - \`summary\`: 조건/반복 로직 요약
- **관계**:
  - \`-[:PARENT_OF]->(자식 노드들)\`

#### B.7 Variable 노드

- **역할**: 변수 정보
- **속성**:
  - \`name\`: 변수명
  - \`type\`: 데이터 타입
  - \`parameter_type\`: IN/OUT/IN_OUT/LOCAL
  - \`role\`: 변수 역할
  - \`scope\`: Global/Local
  - \`{startLine}_{endLine}\`: 'Used' (사용 여부)
- **관계**:
  - \`<-[:SCOPE]-(DECLARE|SPEC|PACKAGE_VARIABLE)\`

#### B.8 Table 노드

- **역할**: 데이터베이스 테이블
- **속성**:
  - \`name\`: 테이블명
  - \`schema\`: 스키마명
  - \`description\`: 테이블 설명
  - \`table_type\`: BASE TABLE/VIEW
  - \`db\`: DBMS 종류
- **관계**:
  - \`-[:HAS_COLUMN]->(Column)\`
  - \`<-[:FROM]-(SELECT)\`
  - \`<-[:WRITES]-(INSERT|UPDATE|DELETE)\`
  - \`-[:FK_TO_TABLE]->(다른 테이블)\`

#### B.9 Column 노드

- **역할**: 테이블 컬럼
- **속성**:
  - \`name\`: 컬럼명
  - \`dtype\`: 데이터 타입
  - \`nullable\`: Nullable 여부
  - \`description\`: 컬럼 역할 설명
  - \`fqn\`: Fully Qualified Name
- **관계**:
  - \`<-[:HAS_COLUMN]-(Table)\`
  - \`-[:FK_TO]->(다른 Column)\`

---

### C. 관계(Relationship) 타입별 상세

| 관계 타입 | 방향 | 의미 | 예시 |
|----------|------|------|------|
| **CONTAINS** | SYSTEM → 노드 | 폴더가 노드를 포함 | SYSTEM → PROCEDURE |
| **PARENT_OF** | 부모 → 자식 | AST 부모-자식 관계 | PROCEDURE → SELECT |
| **NEXT** | 형제 → 형제 | 실행 순서 | SELECT → INSERT |
| **SCOPE** | 선언부 → Variable | 변수 스코프 | DECLARE → Variable |
| **FROM** | DML → Table | 읽기 관계 | SELECT → Table |
| **WRITES** | DML → Table | 쓰기 관계 | INSERT → Table |
| **CALL** | 호출자 → 피호출자 | 프로시저 호출 | SELECT → PROCEDURE |
| **HAS_COLUMN** | Table → Column | 테이블-컬럼 | Table → Column |
| **FK_TO** | Column → Column | 외래키 | order_id → order_id |
| **FK_TO_TABLE** | Table → Table | 테이블 FK | ORDER_DETAIL → ORDER_MASTER |
| **DB_LINK** | DML → Table | DB 링크 | SELECT → remote_table |

---

## 마무리

이 문서는 Legacy Modernizer의 **Understanding 파이프라인**을 완벽히 이해하고, 수정 및 확장할 수 있도록 작성되었습니다.

### 핵심 포인트

1. **ANTLR JSON → StatementNode → Neo4j 그래프** 변환 과정 이해
2. **후위순회 + 배치 플래닝 + 병렬 LLM 호출** 전략
3. **프롬프트별 역할 분담** (코드 분석, DML 분석, 변수 분석, 요약 등)
4. **순서 보장 적용** (ApplyManager의 배치 ID 기반 순차 처리)
5. **후처리** (변수 타입 해석, 컬럼 역할 분석)

---

**문서 버전**: 2.0.0
**최종 수정일**: 2025-01-10
**작성자**: Legacy Modernizer Development Team
---
