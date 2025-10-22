# 🔄 Legacy Modernizer Backend

> **PL/SQL 코드를 Spring Boot Java 프로젝트로 자동 변환하는 AI 기반 마이그레이션 도구**

[![FastAPI](https://img.shields.io/badge/FastAPI-0.115.12-009688?style=flat&logo=fastapi)](https://fastapi.tiangolo.com/)
[![Python](https://img.shields.io/badge/Python-3.10+-3776AB?style=flat&logo=python&logoColor=white)](https://www.python.org/)
[![Neo4j](https://img.shields.io/badge/Neo4j-5.28.1-008CC1?style=flat&logo=neo4j&logoColor=white)](https://neo4j.com/)
[![OpenAI](https://img.shields.io/badge/OpenAI-Compatible-412991?style=flat&logo=openai)](https://openai.com/)

---

## 📋 목차

- [프로젝트 소개](#-프로젝트-소개)
- [핵심 기능](#-핵심-기능)
- [동작 원리](#-동작-원리)
- [시스템 아키텍처](#-시스템-아키텍처)
- [시작하기](#-시작하기)
- [API 가이드](#-api-가이드)
- [데이터 구조](#-데이터-구조)
- [디렉터리 구조](#-디렉터리-구조)
- [테스트](#-테스트)
- [문제해결](#-문제해결)

---

## 🎯 프로젝트 소개

Legacy Modernizer Backend는 **레거시 PL/SQL 코드를 최신 Spring Boot 프로젝트로 자동 변환**하는 AI 기반 도구입니다.

### 🤔 왜 이 프로젝트가 필요한가요?

많은 기업들이 오래된 Oracle PL/SQL 기반 시스템을 사용하고 있지만, 이를 최신 Java Spring Boot로 마이그레이션하는 것은 매우 어렵고 시간이 많이 걸립니다.

**기존 방식의 문제점:**
- 👨‍💻 수작업 변환: 수개월~수년 소요
- ❌ 높은 오류율: 복잡한 로직 이해 어려움  
- 💰 막대한 비용: 전문 인력 필요
- 📉 일관성 부족: 개발자마다 다른 스타일

**Legacy Modernizer의 해결책:**
- ⚡ 자동 변환: 몇 분 내 완료
- 🎯 정확한 분석: AI가 코드 관계 파악
- 💡 일관된 품질: 표준화된 Spring Boot 코드 생성
- 📊 시각화: Neo4j 그래프로 코드 구조 확인

---

## ✨ 핵심 기능

### 1️⃣ **코드 이해 (Understanding)**
- ANTLR 파서가 PL/SQL을 분석하여 파싱한 결과물을 기반으로, 실제 PL/SQL를 순회하면서 LLM을 활용한 코드 의미 파악합니다.
- 의미 파악이 완료된 SP 코드 구문들을 Neo4j 그래프 데이터베이스에 노드 및 관계 형태로 저장합니다.
- 이때, DDL은 또한 LLM을 활용하여 테이블/컬럼 형태로 그래프 데이터베이스에 저장됩니다.

### 2️⃣ **코드 변환 (Converting)**
- Entity 클래스 생성
- Repository 인터페이스 생성
- Service 로직 변환
- REST Controller 생성
- 빌드 설정 파일 생성 (pom.xml, application.properties)

### 3️⃣ **스트리밍 응답**
- 실시간 진행률 표시
- 단계별 결과 스트리밍
- 그래프 데이터 실시간 업데이트

### 4️⃣ **세션 관리**
- 사용자별 독립적인 작업 공간
- 프로젝트 ZIP 다운로드

---

## 🔍 동작 원리

Legacy Modernizer는 크게 **3단계**로 동작합니다:

### 🔧 0단계: 사전 준비 (파일 업로드 & ANTLR 파싱)

```
Frontend → ANTLR 서버 (업로드 + 파싱 완료) → Frontend 응답
```

**무엇을 하나요?**
1. 🖥️ **파일 업로드**: Frontend에서 PL/SQL 파일을 ANTLR 서버로 전송
2. 💾 **파일 저장**: ANTLR 서버가 파일을 `data/{session-id}/src/` 디렉터리에 저장
3. 🔍 **파싱 요청**: Frontend가 ANTLR 서버에 저장된 파일의 파싱 요청
4. 📊 **AST 생성**: ANTLR이 PL/SQL 코드를 파싱하여 AST(추상 구문 트리) 생성
5. 💾 **JSON 저장**: AST를 JSON 형식으로 `data/{session-id}/analysis/{파일명}.json`에 저장
6. ✅ **완료 응답**: ANTLR 서버가 Frontend에 업로드 및 파싱 완료 메시지 전송

**⚠️ 중요:** 
- 이 단계는 **Backend가 아닌 별도의 ANTLR 서버**에서 처리됩니다
- **업로드와 파싱**이 모두 완료된 후 Frontend에 응답합니다
- Backend API를 호출하기 **전에 반드시 완료**되어야 합니다
- 생성된 JSON 파일이 없으면 다음 분석 단계가 실패합니다

### 📖 1단계: 이해하기 (Understanding)

```
Frontend → Backend API → PL/SQL + AST JSON 읽기 → AI 분석 → Neo4j 그래프 저장
```

**무엇을 하나요?**
1. 📤 **분석 요청**: Frontend가 Backend의 `/cypherQuery/` API 호출 (projectName, dbms, systems 배열 전달)
2. 📂 **파일 경로 구성**: Backend가 전달받은 projectName, systemName, fileName으로 파일 경로 생성
3. 📄 **파일 로딩**: File System에서 원본 PL/SQL 코드와 ANTLR AST JSON 읽기
   - PL/SQL: `data/{session-id}/{projectName}/src/{systemName}/{fileName}`
   - AST JSON: `data/{session-id}/{projectName}/analysis/{systemName}/{baseName}.json`
4. 🔍 **구문 탐색**: 구문 트리를 바탕으로, 각 구문들을 순회하면서, 분석할 구문들을 컨텍스트에 담습니다
5. 🤖 **AI 분석**: 컨텍스트 사이즈가 한계에 도달하면, LLM을 활용하여, 분석을 진행합니다
6. 💾 **그래프 저장**: 분석 결과로 나온 데이터를 활용하여, 사이퍼쿼리 형태로 만든 뒤, Neo4j에 노드와 관계 형태로 저장합니다
7. 🔧 **후처리**: 변수 타입 보정 (DDL 메타 참조), 컬럼 역할 파악
8. 📡 **스트리밍 응답**: Frontend에 실시간으로 진행률과 그래프 데이터 전송

**왜 그래프 데이터베이스를 사용하나요?**

절차형 코드(PL/SQL)는 "A가 B를 호출하고, B가 테이블 C를 읽는다"처럼 **관계**가 복잡합니다. 
그래프 데이터베이스는 이런 관계를 **직관적으로 저장하고 탐색**할 수 있어, 변환 시 참조/의존성을 쉽게 재구성할 수 있습니다.

### 🔨 2단계: 변환하기 (Converting)

```
Frontend → Backend API → Neo4j 그래프 조회 → 코드 생성 → Spring Boot 프로젝트
```

**무엇을 생성하나요?**
1. 📤 **변환 요청**: Frontend가 Backend의 `/springBoot/` API 호출 (projectName, dbms, systems 배열 전달)
2. 📊 **그래프 조회**: Neo4j에서 저장된 관계 데이터 조회
3. 📄 **원본 참조**: File System에서 원본 SP 코드 읽기 (필요시)
4. 🗂️ **Entity 생성**: 테이블 구조를 JPA Entity 클래스로 변환
5. 📦 **Repository 생성**: 데이터 접근 계층 인터페이스 생성
6. ⚙️ **Service 생성**: 비즈니스 로직을 Java 메서드로 변환
7. 🌐 **Controller 생성**: REST API 엔드포인트 생성
8. 🔧 **설정 파일 생성**: pom.xml, application.properties, Main 클래스 생성
9. 💾 **파일 저장**: `target/java/{session-id}/{projectName}/` 디렉터리에 저장
10. 📡 **스트리밍 응답**: Frontend에 생성된 코드 조각 실시간 전송

**변환 순서가 중요합니다:**
```
프로젝트명 생성 → Entity → Repository → Service/Controller → 설정 파일
```
각 단계는 이전 단계의 결과를 사용하므로, 순서를 지켜야 합니다.

#### 🔧 Service 변환 상세 흐름

Service 계층 변환은 **토큰 임계 기반 배치 처리**로 수행됩니다:

**📌 Service 생성 단계 (`create_service_preprocessing.py`)**

```
┌──────────────────────────────────────────────────────┐
│     ServicePreprocessor - 토큰 임계 기반 배치 처리     │
└──────────────────────────────────────────────────────┘
         ↓
   [노드 순회 시작]
         ↓
   ┌─────────────────┐
   │ 노드 분류 판단   │ ← 토큰≥1000 & 자식 있음?
   └─────────────────┘
      ↙           ↘
  [YES]          [NO]
대용량 노드    작은/리프 노드
     ↓              ↓
LLM 스켈레톤    sp_code에 누적
생성 후 저장    total_tokens += 토큰
     ↓              ↓
     └──────┬───────┘
            ↓
   total_tokens ≥ 1000?
            ↓ YES
   ┌─────────────────────────┐
   │ 🎯 변수/JPA 수집 시작   │
   │ - sp_range 범위에서     │
   │   변수 필터링             │
   │ - JPA 메서드 필터링      │
   └─────────────────────────┘
            ↓
   ┌─────────────────────────┐
   │ 🤖 LLM 호출              │
   │ - sp_code 전달           │
   │ - 수집된 변수/JPA 컨텍스트│
   │ - 자바 코드 생성         │
   └─────────────────────────┘
            ↓
   ┌─────────────────────────┐
   │ 📦 코드 병합             │
   │ - 부모 진행 중?          │
   │   YES → java_buffer     │
   │   NO → merged_java_code │
   └─────────────────────────┘
            ↓
   ┌─────────────────────────┐
   │ 🔄 컨텍스트 초기화       │
   │ - total_tokens = 0      │
   │ - sp_code = ""          │
   │ - sp_range 초기화       │
   └─────────────────────────┘
            ↓
   [다음 노드로 계속...]
```

**핵심 메커니즘:**
1. **배치 처리**: 토큰이 1000에 도달할 때까지 노드를 누적한 후, 한 번에 LLM 호출
2. **변수/JPA 수집**: 임계 도달 시에만 `sp_range` 범위에서 필요한 변수/JPA 메서드 필터링
3. **대용량 노드 처리**: 큰 노드(IF/FOR/LOOP 등)는 스켈레톤만 생성하고 `...code...` 플레이스홀더 삽입
4. **단일 컨텍스트**: `merged_java_code`에 모든 결과를 순차적으로 누적
5. **Parent Context 전달**: 자식 노드 처리 시 부모의 Java 스켈레톤을 함께 전달하여 변수 스코프 및 제어 흐름 이해 향상
6. **TRY-EXCEPTION 처리**: TRY 노드를 버퍼에 누적하고, EXCEPTION 노드 발견 시 try-catch 블록으로 조립
7. **LLM 임의 로직 방지**: 프롬프트에 "원본 PL/SQL 구조를 정확히 따르고, 임의로 return 문이나 검증 로직을 추가하지 마세요" 지침 포함

### 📥 3단계: 다운로드 (Download)

```
Frontend → Backend API → ZIP 압축 → 파일 다운로드
```

**무엇을 하나요?**
1. 📤 **다운로드 요청**: Frontend가 Backend의 `/downloadJava/` API 호출
2. 📦 **ZIP 압축**: 생성된 Spring Boot 프로젝트 전체를 ZIP 파일로 압축
3. ⬇️ **파일 전송**: Frontend에 ZIP 파일 전송
4. 💻 **로컬 저장**: 사용자가 ZIP 파일을 로컬에 저장하여 사용

---

## 🏗️ 시스템 아키텍처

### 전체 흐름도

```mermaid
flowchart TB
    subgraph "0️⃣ 사전 준비 - ANTLR 서비스"
        A1[🖥️ Frontend<br/>파일 업로드] -->|1. PL/SQL 파일 전송| A2[🔧 ANTLR 서버]
        A2 -->|2. 파일 저장| A3[💾 파일 시스템<br/>src/]
        A1 -->|3. 파싱 요청| A2
        A2 -->|4. AST 생성| A4[💾 파일 시스템<br/>analysis/*.json]
        A4 -->|5. 완료 응답| A1
    end
    
    subgraph "1️⃣ 이해 단계 - Backend"
        B1[🖥️ Frontend<br/>분석 요청] -->|POST /cypherQuery/| B2[⚡ Backend API]
        B2 --> B3[📖 파일 로딩<br/>SP + AST JSON + DDL]
        B3 --> B4[🔍 AST 순회<br/>코드 분석]
        B4 --> B5[🤖 LLM API 호출<br/>관계 추출]
        B5 --> B6[💾 Neo4j<br/>그래프 저장]
        B6 --> B7[🔧 후처리<br/>타입/컬럼 보정]
        B7 -->|스트리밍 응답| B1
    end
    
    subgraph "2️⃣ 변환 단계 - Backend"
        C1[🖥️ Frontend<br/>변환 요청] -->|POST /springBoot/| C2[⚡ Backend API]
        C2 --> C3[📝 프로젝트명 생성]
        C3 --> C4[🗂️ Entity 생성]
        C4 --> C5[📦 Repository 생성]
        C5 --> C6[⚙️ Service 생성]
        C6 --> C7[🌐 Controller 생성]
        C7 --> C8[🔧 설정 파일 생성<br/>pom/properties/Main]
        C8 -->|스트리밍 응답| C1
    end
    
    subgraph "3️⃣ 다운로드 - Backend"
        D1[🖥️ Frontend<br/>다운로드 요청] -->|POST /downloadJava/| D2[⚡ Backend API]
        D2 --> D3[📦 ZIP 압축]
        D3 -->|파일 다운로드| D1
    end
    
    A1 -.->|완료 후| B1
    B1 -.->|완료 후| C1
    C1 -.->|완료 후| D1
    
    style A1 fill:#e3f2fd
    style B1 fill:#fff3e0
    style C1 fill:#e8f5e9
    style D1 fill:#f3e5f5
```

### 외부 서비스 연동

```mermaid
flowchart LR
    FE[🖥️ Frontend]
    ANTLR[🔧 ANTLR 서버]
    API[⚡ Backend API]
    FS[📁 File System]
    NEO[🗄️ Neo4j]
    LLM[🤖 LLM API]
    
    FE -->|① 업로드 & 파싱| ANTLR
    ANTLR <--> FS
    
    FE -->|② 분석 요청| API
    FE -->|③ 변환 요청| API
    FE -->|④ 다운로드| API
    
    API <--> FS
    API <--> NEO
    API <--> LLM
    
    style FE fill:#e3f2fd
    style ANTLR fill:#e0f2f1
    style API fill:#fff3e0
    style NEO fill:#e8f5e9
    style LLM fill:#f3e5f5
    style FS fill:#fce4ec
```

**각 구성 요소 역할:**

| 구성 요소 | 역할 | 기술 스택 |
|---------|------|----------|
| 🖥️ **Frontend** | 사용자 인터페이스, 파일 업로드, API 요청 | React/Vue 등 |
| 🔧 **ANTLR 서버** | PL/SQL 파싱, AST JSON 생성 | ANTLR 4 |
| ⚡ **Backend API** | 분석/변환 파이프라인 실행 | FastAPI (Python) |
| 📁 **File System** | 파일 저장소 (SP 코드, AST, Java 코드) | 로컬 디스크 |
| 🗄️ **Neo4j** | 코드 관계 그래프 저장/조회 | Neo4j 5.x |
| 🤖 **LLM API** | 코드 분석 및 생성 | OpenAI 호환 |

**주요 데이터 흐름:**
- **① ANTLR 단계**: Frontend → ANTLR ↔ File System (업로드 & 파싱)
- **② 분석 단계**: Frontend → Backend ↔ File System/Neo4j/LLM (코드 분석)
- **③ 변환 단계**: Frontend → Backend ↔ Neo4j/LLM/File System (코드 생성)
- **④ 다운로드**: Frontend → Backend (ZIP 다운로드)

**📌 실제 호출 순서:**

### [0단계: ANTLR 서비스 - 업로드 & 파싱]

| 순서 | 흐름 | 설명 |
|-----|------|------|
| 1 | Frontend → ANTLR | PL/SQL 파일 업로드 (projectName 포함) |
| 2 | ANTLR → File System | 원본 파일 저장 (`data/{session-id}/{projectName}/src/{systemName}/{fileName}`) |
| 3 | Frontend → ANTLR | 파싱 요청 |
| 4 | ANTLR → File System | AST JSON 생성 및 저장 (`data/{session-id}/{projectName}/analysis/{systemName}/{baseName}.json`) |
| 5 | ANTLR → Frontend | ✅ 업로드 및 파싱 완료 응답 (프로젝트명, 시스템명, 파일명 포함) |

### [1단계: Backend - 분석]

| 순서 | 흐름 | 설명 |
|-----|------|------|
| 6 | Frontend → Backend | `/cypherQuery/` 분석 요청 (projectName, dbms, systems 배열 전달) |
| 7 | Backend → File System | 📂 **전달받은 경로로 파일 읽기**<br/>- SP 코드: `data/{session-id}/{projectName}/src/{systemName}/{fileName}`<br/>- AST JSON: `data/{session-id}/{projectName}/analysis/{systemName}/{baseName}.json` |
| 8 | Backend → LLM | 코드 분석 요청 (SP 코드 + AST 기반) |
| 9 | Backend → Neo4j | 분석 결과 그래프 저장 |
| 10 | Backend → Frontend | ✅ 스트리밍 응답 (그래프 + 진행률) |

### [2단계: Backend - 변환]

| 순서 | 흐름 | 설명 |
|-----|------|------|
| 11 | Frontend → Backend | `/springBoot/` 변환 요청 (projectName, dbms, systems 배열 전달) |
| 12 | Backend → Neo4j | 그래프 데이터 조회 |
| 13 | Backend → File System | 📂 원본 SP 코드 읽기 (필요시) |
| 14 | Backend → LLM | 코드 생성 요청 |
| 15 | Backend → File System | 💾 생성된 Java 파일 저장 (`target/java/{session-id}/{projectName}/...`) |
| 16 | Backend → Frontend | ✅ 스트리밍 응답 (생성 코드) |

### [3단계: Backend - 다운로드]

| 순서 | 흐름 | 설명 |
|-----|------|------|
| 17 | Frontend → Backend | `/downloadJava/` 다운로드 요청 (프로젝트명) |
| 18 | Backend → File System | 📦 생성된 프로젝트 ZIP 압축 |
| 19 | Backend → Frontend | ✅ ZIP 파일 전송 |

---

## 🚀 시작하기

### 📋 사전 요구사항

시작하기 전에 다음 항목들이 설치되어 있어야 합니다:

- ✅ **Python 3.10 이상**
- ✅ **Neo4j 5.x** (로컬 또는 원격)
- ✅ **Git**
- ✅ **OpenAI 호환 API 키** (OpenAI, Anthropic 등)

### 📥 1단계: 저장소 클론

```bash
git clone <repository-url>
```

### 📦 2단계: 의존성 설치

**방법 A: uv pip 사용**
  ```bash
  uv venv
  uv pip install -r requirements.txt
  source .venv/Scripts/activate
  ```

**방법 B: Pipenv 사용**
  ```bash
# Pipenv 설치 (없는 경우)
pip install pipenv

# 가상 환경 활성화
  pipenv shell

# 의존성 설치
  pipenv install
  ```

### ⚙️ 3단계: 환경 변수 설정

프로젝트 루트에 `.env` 파일을 생성하고 다음 내용을 입력하세요:

```env
# Neo4j 데이터베이스 설정
NEO4J_URI=bolt://localhost:7687
NEO4J_USER=neo4j
NEO4J_PASSWORD=your-password-here

# LLM API 설정 (OpenAI 호환)
LLM_API_BASE=https://api.openai.com/v1
LLM_API_KEY=sk-your-api-key-here
LLM_MODEL=gpt-4.1
LLM_MAX_TOKENS=32768
```

**⚠️ 보안 주의사항:**
- `.env` 파일은 절대 Git에 커밋하지 마세요
- 운영 환경에서는 반드시 강력한 암호를 사용하세요
- API 키는 외부에 노출되지 않도록 주의하세요

### 🎬 4단계: 서버 실행

```bash
python main.py
```


## 📡 API 가이드

### 공통 헤더

모든 API 요청에는 다음 헤더가 필요합니다:

| 헤더 | 필수 | 설명 | 예시 |
|-----|------|------|------|
| `Session-UUID` | ✅ | 세션 식별자 (사용자별 고유 ID) | `user-session-12345` |
| `OpenAI-Api-Key` 또는 `Anthropic-Api-Key` | ⚠️ | LLM API 키 (테스트 세션 제외) | `sk-...` |
| `Accept-Language` | ❌ | 응답 언어 (기본: `ko`) | `ko` 또는 `en` |

**⚠️ 테스트 세션 예외:**
- `Session-UUID`가 `EN_TestSession` 또는 `KO_TestSession`인 경우
- 환경 변수의 `LLM_API_KEY` 또는 `API_KEY`를 사용
- 헤더에 API 키를 포함하지 않아도 됨

---

### 🔍 API 1: 코드 분석 (Understanding)

**엔드포인트:** `POST /cypherQuery/`

**역할:** PL/SQL 코드를 분석하여 Neo4j 그래프로 저장하고, 실시간 진행률을 스트리밍으로 반환합니다.

**사전 준비:**
1. ANTLR 서비스에서 파일 업로드 및 파싱 완료
2. ANTLR로부터 받은 **시스템명(`systemName`)** 과 **파일명(`fileName`)** 을 요청 바디에 포함
3. File System에 다음 파일들이 존재해야 함:
   - 원본 PL/SQL: `data/{session-id}/{projectName}/src/{systemName}/{fileName}`
   - AST JSON: `data/{session-id}/{projectName}/analysis/{systemName}/{baseName}.json`
   - (선택) DDL: `data/{session-id}/{projectName}/ddl/*.sql`

**요청 바디 스키마:**

```json
{
  "projectName": "string",     // 프로젝트 이름 (필수)
  "dbms": "string",           // 데이터베이스 종류 (기본: "postgres")
  "targetLang": "string",     // 타겟 언어 (기본: "java")
  "systems": [                // 시스템 및 파일 목록 (필수)
    {
      "name": "string",       // 시스템명 (필수)
      "sp": ["string"]        // SP 파일명 배열 (필수)
    }
  ]
}
```

**요청 예시:**

```bash
curl -N -X POST "http://localhost:5502/cypherQuery/" \
  -H "Content-Type: application/json" \
  -H "Session-UUID: my-session-123" \
  -H "OpenAI-Api-Key: sk-..." \
  -H "Accept-Language: ko" \
  -d '{
    "projectName": "OrderSystem",
    "dbms": "postgres",
    "targetLang": "java",
    "systems": [
      {
        "name": "PKG_ORDER",
        "sp": ["ORDER_PKG.sql"]
      },
      {
        "name": "PKG_USER",
        "sp": ["USER_PKG.sql"]
      }
    ]
  }'
```

**응답 형식 (Streaming):**

응답은 스트리밍 방식으로 전달되며, 각 청크는 `send_stream`으로 구분됩니다.

```json
{"type":"ALARM","MESSAGE":"Preparing Analysis Data"}send_stream
{"type":"ALARM","MESSAGE":"START DDL PROCESSING","file":"TABLES.sql"}send_stream
{"type":"DATA","graph":{"Nodes":[...],"Relationships":[...]},"line_number":45,"analysis_progress":30,"current_file":"PKG_ORDER-ORDER_PKG.sql"}send_stream
{"type":"DATA","graph":{"Nodes":[...],"Relationships":[...]},"line_number":120,"analysis_progress":80,"current_file":"PKG_ORDER-ORDER_PKG.sql"}send_stream
{"type":"ALARM","MESSAGE":"ALL_ANALYSIS_COMPLETED"}send_stream
```

**응답 필드 설명:**

| 필드 | 타입 | 설명 |
|-----|------|------|
| `type` | String | `ALARM` (알림) 또는 `DATA` (데이터) |
| `MESSAGE` | String | 알림 메시지 (ALARM 타입만) |
| `graph` | Object | Neo4j 그래프 객체 (DATA 타입만) |
| `line_number` | Integer | 현재 분석 중인 라인 번호 |
| `analysis_progress` | Integer | 진행률 (0~100) |
| `current_file` | String | 현재 분석 중인 파일 |

**Graph 객체 구조:**

  ```json
  {
  "Nodes": [
    {
      "Node ID": "n1",
      "Labels": ["SELECT"],
      "Properties": {
        "startLine": 30,
        "endLine": 45,
        "file_name": "ORDER_PKG.sql",
        "folder_name": "PKG_ORDER",
        "user_id": "my-session-123",
        "project_name": "OrderSystem",
        "summary": "주문 정보를 조회합니다"
      }
    }
  ],
  "Relationships": [
    {
      "Relationship ID": "r1",
      "Type": "FROM",
      "Start Node ID": "n1",
      "End Node ID": "t1",
      "Properties": {}
    }
    ]
  }
  ```

---

### 🔨 API 2: 코드 변환 (Converting)

**엔드포인트:** `POST /springBoot/`

**역할:** 분석된 그래프 데이터를 기반으로 Spring Boot 프로젝트를 생성하고, 생성된 코드를 스트리밍으로 반환합니다.

**사전 준비:**
- `/cypherQuery/` API를 먼저 호출하여 분석이 완료되어야 함
- ANTLR로부터 받은 **시스템명(`systemName`)** 과 **파일명(`fileName`)** 을 요청 바디에 포함

**요청 바디 스키마:**

```json
{
  "projectName": "string",     // 프로젝트 이름 (필수)
  "dbms": "string",           // 데이터베이스 종류 (기본: "postgres")
  "targetLang": "string",     // 타겟 언어 (기본: "java")
  "systems": [                // 시스템 및 파일 목록 (필수)
    {
      "name": "string",       // 시스템명 (필수)
      "sp": ["string"]        // SP 파일명 배열 (필수)
    }
  ]
}
```

**요청 예시:**

```bash
curl -N -X POST "http://localhost:5502/springBoot/" \
  -H "Content-Type: application/json" \
  -H "Session-UUID: my-session-123" \
  -H "OpenAI-Api-Key: sk-..." \
  -H "Accept-Language: ko" \
  -d '{
    "projectName": "OrderSystem",
    "dbms": "postgres",
    "targetLang": "java",
    "systems": [
      {
        "name": "PKG_ORDER",
        "sp": ["ORDER_PKG.sql"]
      }
    ]
  }'
```

**🆕 targetLang 파라미터:**
- `targetLang` (선택): 타겟 언어 (기본값: `java`)
  - 지원: `java`, `python` (향후 확장)
  - 예시: `"targetLang": "python"` → SQLAlchemy Model 생성
  - Rule 파일 경로: `rules/{targetLang}/*.yaml`

**응답 형식 (Streaming):**

```json
{"data_type":"data","file_type":"project_name","project_name":"OrderSystem"}send_stream
{"data_type":"message","step":1,"content":"ORDER_PKG - Generating Entity Class"}send_stream
{"data_type":"data","file_type":"entity_class","file_name":"Order.java","code":"package com.ordersystem.entity;\n\nimport jakarta.persistence.*;\n\n@Entity\n@Table(name = \"ORDERS\")\npublic class Order {\n    @Id\n    @GeneratedValue(strategy = GenerationType.IDENTITY)\n    private Long id;\n    ...\n}"}send_stream
{"data_type":"Done","step":1,"file_count":1,"current_count":1}send_stream
{"data_type":"message","step":2,"content":"ORDER_PKG - Generating Repository Interface"}send_stream
{"data_type":"data","file_type":"repository_class","file_name":"OrderRepository.java","code":"..."}send_stream
{"data_type":"Done","step":2,"file_count":1,"current_count":1}send_stream
...
{"data_type":"Done"}send_stream
```

**응답 필드 설명:**

| 필드 | 타입 | 설명 |
|-----|------|------|
| `data_type` | String | `message` (진행 메시지), `data` (코드), `Done` (완료) |
| `step` | Integer | 현재 단계 (1~7) |
| `content` | String | 진행 메시지 내용 |
| `file_type` | String | 파일 유형 (`entity_class`, `repository_class`, `service_class`, `controller_class`, `command_class`, `pom`, `properties`, `main`, `project_name`) |
| `file_name` | String | 생성된 파일 이름 |
| `code` | String | 생성된 소스 코드 |
| `project_name` | String | 생성된 프로젝트 이름 |

**변환 단계:**

| 단계 | 내용 | 출력 파일 |
|-----|------|-----------|
| 0 | 프로젝트명 생성 | - |
| 1 | Entity 클래스 생성 | `*.java` (Entity) |
| 2 | Repository 인터페이스 생성 | `*Repository.java` |
| 3 | Command 클래스 생성 | `*Command.java` |
| 4 | Service/Controller 생성 | `*Service.java`, `*Controller.java` |
| 5 | pom.xml 생성 | `pom.xml` |
| 6 | application.properties 생성 | `application.properties` |
| 7 | Main 클래스 생성 | `*Application.java` |

---

### 📥 API 3: 프로젝트 다운로드

**엔드포인트:** `POST /downloadJava/`

**역할:** 생성된 Spring Boot 프로젝트를 ZIP 파일로 압축하여 다운로드합니다.

**사전 준비:**
- `/springBoot/` API를 먼저 호출하여 프로젝트 생성이 완료되어야 함

**요청 예시:**

```bash
curl -X POST "http://localhost:5502/downloadJava/" \
  -H "Content-Type: application/json" \
  -H "Session-UUID: my-session-123" \
  -d '{
    "projectName": "OrderSystem"
  }' \
  --output OrderSystem.zip
```

**응답:**
- Content-Type: `application/octet-stream`
- 파일명: `{projectName}.zip`
- ZIP 파일에는 완전한 Spring Boot 프로젝트 구조가 포함됨

---

### 🗑️ API 4: 데이터 삭제

**엔드포인트:** `DELETE /deleteAll/`

**역할:** 현재 세션의 모든 임시 파일과 Neo4j 그래프 데이터를 삭제합니다.

**요청 예시:**

```bash
curl -X DELETE "http://localhost:5502/deleteAll/" \
  -H "Session-UUID: my-session-123"
```

**응답:**

```json
{
  "message": "모든 임시 파일이 삭제되었습니다."
}
```

**삭제되는 항목:**
- ✅ `data/{Session-UUID}/` 디렉터리 전체
- ✅ `target/java/{Session-UUID}/` 디렉터리 전체
- ✅ Neo4j에서 `user_id`가 일치하는 모든 노드 및 관계

---

## 📁 데이터 구조

### 세션별 파일 레이아웃

```
BASE_DIR/  (프로젝트 루트 또는 DOCKER_COMPOSE_CONTEXT)
├── data/
│   └── {Session-UUID}/              # 세션별 작업 공간
│       └── {ProjectName}/           # 프로젝트별 작업 공간
│           ├── src/                 # 원본 PL/SQL 파일
│           │   └── {SystemName}/
│           │       └── {fileName}.sql
│           ├── analysis/            # ANTLR 파싱 결과
│           │   └── {SystemName}/
│           │       └── {baseName}.json
│           ├── ddl/                 # DDL 파일 (선택)
│           │   └── *.sql
│           └── zipfile/             # 다운로드용 ZIP
│               └── {projectName}.zip
└── target/
    └── java/
        └── {Session-UUID}/          # 생성된 Spring Boot 프로젝트
            └── {projectName}/
                ├── src/
                │   └── main/
                │       ├── java/
                │       │   └── com/example/{projectName}/
                │       │       ├── entity/
                │       │       ├── repository/
                │       │       ├── service/
                │       │       ├── controller/
                │       │       └── {ProjectName}Application.java
                │       └── resources/
                │           └── application.properties
                └── pom.xml
```

### 파일 이름 규칙

| 입력 | 출력 |
|-----|------|
| `ORDER_PKG.sql` | `analysis/PKG_ORDER/ORDER_PKG.json` (ANTLR) |
| `ORDER_PKG.sql` | `src/PKG_ORDER/ORDER_PKG.sql` (원본) |
| `ORDER_PKG.sql` | `entity/Order.java` (Entity) |
| `ORDER_PKG.sql` | `repository/OrderRepository.java` |
| `ORDER_PKG.sql` | `service/OrderService.java` |
| `ORDER_PKG.sql` | `controller/OrderController.java` |

**⚠️ 중요:**
- `systems[].name`과 `systems[].sp[]`는 실제 파일 경로와 **정확히 일치**해야 합니다
- 실제 파일 경로: `data/{session-id}/{projectName}/src/{systemName}/{fileName}`
- ANTLR JSON 경로: `data/{session-id}/{projectName}/analysis/{systemName}/{baseName}.json`
- `projectName`은 요청 바디에 **필수**로 포함되어야 합니다
- 대소문자를 구분합니다

---

## 🗄️ Neo4j 그래프 모델

### 노드 타입

| 노드 라벨 | 설명 | 모든 속성 |
|---------|------|----------|
| `Folder` | 폴더 (패키지) | `user_id`, `name`, `project_name`, `has_children` |
| `FILE` | PL/SQL 파일 | `user_id`, `project_name`, `folder_name`, `file_name`, `startLine`, `endLine`, `name`, `summary`, `has_children` |
| `PROCEDURE` | 프로시저 | `user_id`, `project_name`, `folder_name`, `file_name`, `procedure_name`, `startLine`, `endLine`, `name`, `summary`, `summarized_code`, `node_code`, `token`, `has_children` |
| `FUNCTION` | 함수 | `user_id`, `project_name`, `folder_name`, `file_name`, `procedure_name`, `startLine`, `endLine`, `name`, `summary`, `summarized_code`, `node_code`, `token`, `has_children` |
| `SELECT` | SELECT 구문 | `user_id`, `project_name`, `folder_name`, `file_name`, `procedure_name`, `startLine`, `endLine`, `name`, `summary`, `node_code`, `token`, `has_children` |
| `INSERT` | INSERT 구문 | `user_id`, `project_name`, `folder_name`, `file_name`, `procedure_name`, `startLine`, `endLine`, `name`, `summary`, `node_code`, `token`, `has_children` |
| `UPDATE` | UPDATE 구문 | `user_id`, `project_name`, `folder_name`, `file_name`, `procedure_name`, `startLine`, `endLine`, `name`, `summary`, `node_code`, `token`, `has_children` |
| `DELETE` | DELETE 구문 | `user_id`, `project_name`, `folder_name`, `file_name`, `procedure_name`, `startLine`, `endLine`, `name`, `summary`, `node_code`, `token`, `has_children` |
| `MERGE` | MERGE 구문 | `user_id`, `project_name`, `folder_name`, `file_name`, `procedure_name`, `startLine`, `endLine`, `name`, `summary`, `node_code`, `token`, `has_children` |
| `FETCH` | FETCH 구문 | `user_id`, `project_name`, `folder_name`, `file_name`, `procedure_name`, `startLine`, `endLine`, `name`, `summary`, `node_code`, `token`, `has_children` |
| `CALL` | 프로시저 호출 | `user_id`, `project_name`, `folder_name`, `file_name`, `procedure_name`, `startLine`, `endLine`, `name`, `summary`, `node_code`, `token`, `has_children` |
| `ASSIGNMENT` | 변수 할당 | `user_id`, `project_name`, `folder_name`, `file_name`, `procedure_name`, `startLine`, `endLine`, `name`, `summary`, `node_code`, `token`, `has_children` |
| `EXECUTE_IMMEDIATE` | 동적 SQL 실행 | `user_id`, `project_name`, `folder_name`, `file_name`, `procedure_name`, `startLine`, `endLine`, `name`, `summary`, `node_code`, `token`, `has_children` |
| `TRY` | TRY 블록 | `user_id`, `project_name`, `folder_name`, `file_name`, `procedure_name`, `startLine`, `endLine`, `name`, `summary`, `node_code`, `java_code`, `token`, `has_children` |
| `EXCEPTION` | EXCEPTION 블록 | `user_id`, `project_name`, `folder_name`, `file_name`, `procedure_name`, `startLine`, `endLine`, `name`, `summary`, `node_code`, `java_code`, `token`, `has_children` |
| `SPEC` | 매개변수 선언부 | `user_id`, `project_name`, `folder_name`, `file_name`, `procedure_name`, `startLine`, `endLine`, `name`, `summary`, `node_code`, `token`, `has_children` |
| `DECLARE` | 변수 선언부 | `user_id`, `project_name`, `folder_name`, `file_name`, `procedure_name`, `startLine`, `endLine`, `name`, `summary`, `node_code`, `token`, `has_children` |
| `PACKAGE_VARIABLE` | 패키지 전역 변수 | `user_id`, `project_name`, `folder_name`, `file_name`, `startLine`, `endLine`, `name`, `summary`, `node_code`, `token`, `has_children` |
| `Table` | 테이블 | `user_id`, `project_name`, `schema`, `name`, `description`, `table_type`, `db`, `folder_name`, `db_link` (선택) |
| `Column` | 컬럼 | `user_id`, `name`, `fqn`, `dtype`, `description`, `nullable`, `pk_constraint` (선택) |
| `Variable` | 변수 | `user_id`, `project_name`, `folder_name`, `file_name`, `name`, `type`, `parameter_type`, `procedure_name` (선택), `role`, `scope`, `value`, `resolved` (선택) |
| `DBLink` | DB 링크 | `user_id`, `project_name`, `name` |

**속성 설명:**
- `user_id`: 세션 식별자 (모든 노드에 필수)
- `project_name`: 프로젝트 이름 (그래프 분리용, 대부분의 노드에 포함)
- `folder_name`: 폴더명 (패키지명)
- `file_name`: 파일명
- `procedure_name`: 프로시저/함수명
- `startLine`, `endLine`: 코드 시작/종료 라인 번호
- `name`: 노드 표시명
- `summary`: LLM이 생성한 요약
- `summarized_code`: 자식 노드를 플레이스홀더로 치환한 코드
- `node_code`: 원본 PL/SQL 코드
- `java_code`: 변환된 Java 코드 (전처리 단계에서 생성, TRY/EXCEPTION 노드에 주로 존재)
- `token`: 토큰 수
- `has_children`: 자식 노드 존재 여부
- `fqn`: Fully Qualified Name (schema.table.column)
- `dtype`: 데이터 타입
- `nullable`: NULL 허용 여부
- `pk_constraint`: Primary Key 제약 조건명
- `scope`: 변수 스코프 (Local/Global)
- `role`: 변수 역할 설명
- `resolved`: 변수 타입 해석 완료 여부

### 관계 타입

| 관계 | 설명 | 시작 노드 | 종료 노드 | 속성 |
|-----|------|----------|----------|------|
| `CONTAINS` | 포함 관계 | `Folder` | `Table`, `PROCEDURE`, `FUNCTION`, `Variable`, 구문 노드 | - |
| `PARENT_OF` | 부모-자식 관계 | 모든 구문 노드 | 자식 구문 노드 | - |
| `NEXT` | 순차 실행 관계 | 구문 노드 | 다음 구문 노드 | - |
| `FROM` | 읽기 관계 | `SELECT`, `FETCH` | `Table` | - |
| `WRITES` | 쓰기 관계 | `INSERT`, `UPDATE`, `DELETE`, `MERGE` | `Table` | - |
| `EXECUTE` | 실행 관계 | `EXECUTE_IMMEDIATE`, `ASSIGNMENT` | `PROCEDURE`, `FUNCTION` | - |
| `CALL` | 호출 관계 | `CALL` | `PROCEDURE`, `FUNCTION` | `scope`: `internal` 또는 `external` |
| `HAS_COLUMN` | 컬럼 보유 | `Table` | `Column` | - |
| `FK_TO` | 외래 키 (컬럼 간) | `Column` | `Column` | - |
| `FK_TO_TABLE` | 외래 키 (테이블 간) | `Table` | `Table` | - |
| `SCOPE` | 변수 스코프 | `DECLARE`, `SPEC`, `PACKAGE_VARIABLE` | `Variable` | - |
| `DB_LINK` | DB 링크 사용 | 구문 노드 | `Table` | `mode`: `r` (read) 또는 `w` (write) |

**관계 패턴 예시:**
```cypher
// 프로시저가 테이블을 읽는 패턴
(p:PROCEDURE)-[:PARENT_OF]->(s:SELECT)-[:FROM]->(t:Table)

// 변수 사용 추적 패턴
(d:DECLARE)-[:SCOPE]->(v:Variable)
(v)-[:{range}]-() WHERE {range} = 'Used'

// 프로시저 호출 체인
(p1:PROCEDURE)-[:PARENT_OF]->(c:CALL)-[:CALL {scope:'internal'}]->(p2:PROCEDURE)
```

### 그래프 예시

```
[Folder: PKG_ORDER]
    ↓ CONTAINS
[PROCEDURE: CREATE_ORDER]
    ↓ PARENT_OF
[SELECT: "주문 정보 조회"]
    ↓ FROM
[Table: ORDERS]
    ↓ HAS_COLUMN
[Column: ORDER_ID (PK)]
```

---

## 📂 디렉터리 구조

```
Backend/
├── 📄 main.py                      # FastAPI 애플리케이션 진입점
├── 📄 Dockerfile                   # Docker 빌드 설정
├── 📄 requirements.txt             # Python 의존성 목록
├── 📄 Pipfile                      # Pipenv 설정
├── 📄 Pipfile.lock                 # Pipenv 잠금 파일
├── 📄 README.md                    # 프로젝트 문서 (이 파일)
│
├── 📁 service/                     # API 서비스 계층
│   ├── router.py                   # API 엔드포인트 정의
│   └── service.py                  # 핵심 비즈니스 로직 (이해/변환 파이프라인)
│
├── 📁 understand/                  # 이해(Understanding) 단계
│   ├── analysis.py                 # AST 분석기 (Analyzer 클래스)
│   └── neo4j_connection.py         # Neo4j 연결 및 쿼리 실행
│
├── 📁 convert/                     # 변환(Converting) 단계
│   ├── create_entity.py            # Entity 생성 (Rule 파일 사용)
│   ├── create_repository.py        # Repository 생성 (Rule 파일 사용)
│   ├── create_service_skeleton.py  # Service Skeleton 생성 (Rule 파일 사용)
│   ├── create_service_preprocessing.py   # Service 전처리 (Rule 파일 사용: service, service_summarized, service_exception)
│   ├── create_controller.py        # Controller 생성 (통합 - Skeleton + 메서드)
│   ├── create_main.py              # Main 클래스 생성
│   └── create_config_files.py      # pom.xml 및 application.properties 생성
│
├── 📁 rules/                         # 🆕 Rule 파일 (프롬프트 설정)
│   ├── java/                      # Java 타겟용 프롬프트
│   │   ├── entity.yaml            # Entity 생성 프롬프트
│   │   ├── repository.yaml        # Repository 생성 프롬프트
│   │   ├── variable.yaml          # 변수 타입 변환 프롬프트
│   │   ├── command.yaml           # Command DTO 생성 프롬프트
│   │   ├── service_skeleton.yaml  # Service Skeleton 프롬프트
│   │   ├── service.yaml           # Service 메서드 바디 프롬프트
│   │   ├── service_summarized.yaml # Service 대용량 노드 스켈레톤 프롬프트
│   │   ├── service_exception.yaml # Service 예외처리 프롬프트
│   │   └── controller.yaml        # Controller REST API 프롬프트
│   │
│   └── python/                    # Python 타겟용 프롬프트 (확장 예시)
│       ├── entity.yaml            # SQLAlchemy Model 생성 프롬프트
│       └── ...                    # 추가 프롬프트
│
├── 📁 prompt/                      # LLM 프롬프트 정의 (레거시 - Understanding용으로만 사용)
│   ├── understand_ddl.py           # DDL 분석 프롬프트
│   ├── understand_prompt.py        # 코드 분석 프롬프트
│   ├── understand_summarized_prompt.py    # 요약 프롬프트
│   ├── understand_column_prompt.py        # 컬럼 역할 분석
│   └── understand_variables_prompt.py     # 변수 분석
│
├── 📁 util/                        # 유틸리티
│   ├── utility_tool.py             # 공통 유틸 함수 (라인 번호, 토큰 계산 등)
│   ├── llm_client.py               # LLM API 클라이언트
│   ├── prompt_loader.py            # 🆕 Rule 파일 로더 (YAML 기반)
│   └── exception.py                # 커스텀 예외 정의
│
└── 📁 test/                        # 테스트 코드
    ├── test_understanding.py       # 이해 단계 테스트
    ├── test_converting.py          # 변환 단계 테스트
    └── test_converting_results.json # 테스트 결과 저장 파일
```

### 주요 모듈 설명

#### 📡 `service/router.py`
API 엔드포인트를 정의하고 요청을 처리합니다.

**주요 엔드포인트:**
- `understand_data()`: `/cypherQuery/` - PL/SQL 코드 분석
- `convert_spring_project()`: `/springBoot/` - Spring Boot 프로젝트 생성
- `download_spring_project()`: `/downloadJava/` - ZIP 파일 다운로드
- `delete_all_data()`: `/deleteAll/` - 데이터 삭제

#### ⚙️ `service/service.py` - ServiceOrchestrator 클래스
이해/변환 파이프라인의 핵심 오케스트레이션을 담당합니다.

**주요 메서드:**
- `validate_api_key()`: LLM API 키 검증
- `understand_project()`: PL/SQL 분석 파이프라인 실행
  - `_analyze_file()`: 단일 파일 분석
  - `_postprocess_file()`: 변수 타입 해석 및 컬럼 역할 산출
  - `_process_ddl()`: DDL 파일 처리
  - `_ensure_folder_node()`: 폴더 노드 생성
- `convert_to_springboot()`: Spring Boot 프로젝트 생성 파이프라인 실행
- `zip_project()`: 프로젝트 ZIP 압축
- `cleanup_all_data()`: 사용자 데이터 전체 삭제 (파일 + Neo4j)

#### 🔍 `understand/analysis.py`
ANTLR AST를 DFS 순회하며 코드를 분석합니다.

**주요 클래스:**
- `Analyzer`: AST 분석기
  - `__init__()`: 생성자 - ANTLR 데이터, 파일 내용, 큐, 사용자 정보 등 초기화
  - `run()`: 전체 분석 파이프라인 실행 (DFS 순회→잔여 배치 플러시→완료 이벤트 송신)
  - `analyze_statement_tree()`: 구문 트리 DFS 순회 및 분석
    - 노드/관계 생성
    - 요약 코드 조립
    - 토큰 임계 도달 시 배치 플러시
  - `execute_analysis_and_reset_state()`: LLM 분석 실행 및 상태 초기화
  - `process_analysis_output_to_cypher()`: LLM 분석 결과를 사이퍼 쿼리로 변환
    - 변수 사용 기록
    - 프로시저 호출 관계 생성
    - 테이블 관계 생성
    - FK 관계 및 DBLink 처리
  - `analyze_variable_declarations()`: 변수 선언부 분석 (SPEC/DECLARE/PACKAGE_VARIABLE)
  - `send_analysis_event_and_wait()`: 분석 결과 이벤트 송신 및 Neo4j 저장 완료 대기

#### 🗄️ `understand/neo4j_connection.py`
Neo4j 데이터베이스 연결 및 쿼리 실행을 담당합니다.

**주요 클래스:**
- `Neo4jConnection`: Neo4j 비동기 드라이버 래퍼
  - `__init__()`: 환경변수에서 연결 정보를 읽어 드라이버 초기화
  - `close()`: 데이터베이스 연결 종료
  - `execute_queries()`: 사이퍼 쿼리 순차 실행 및 결과 반환
  - `execute_query_and_return_graph()`: 노드/관계 조회하여 그래프 딕셔너리로 반환
  - `node_exists()`: 노드 존재 여부 확인

#### 🔨 `convert/*`
Spring Boot 프로젝트의 각 구성 요소를 생성합니다.

| 파일 | 생성 대상 | 주요 역할 |
|-----|----------|----------|
| `create_entity.py` | JPA Entity 클래스 | DDL 테이블을 Entity로 변환 |
| `create_repository.py` | Repository 인터페이스 | 데이터 접근 계층 생성 |
| `create_service_skeleton.py` | Service 클래스 뼈대 | 메서드 시그니처 생성<br/>- 프로시저/함수별 메서드 시그니처 생성<br/>- Command 클래스 자동 생성 |
| `create_service_preprocessing.py` | Service 메서드 바디 | **토큰 임계(1000) 기반 배치 처리**<br/>- 노드 순회하며 sp_code 누적<br/>- 임계 도달 시 변수/JPA 수집 후 LLM 호출<br/>- 대용량 노드는 스켈레톤 생성 후 자식 삽입<br/>- **Parent Context 전달**로 변수 스코프 및 제어 흐름 이해 향상<br/>- **TRY-EXCEPTION 처리**: TRY 노드 버퍼 관리 및 조립 |
| `create_controller_skeleton.py` | Controller 뼈대 | REST API 엔드포인트 골격 |
| `create_controller.py` | Controller 메서드 | HTTP 요청 처리 로직 |
| `create_main.py` | Main 클래스 | Spring Boot 애플리케이션 진입점 |
| `create_config_files.py` | pom.xml & application.properties | 빌드 설정 및 DB 연결 설정 |

#### 🎨 `rules/*` - Rule 파일 시스템 (프롬프트 관리) 🆕

**Converting 단계는 Rule 파일 기반으로 동작합니다.**

Rule 파일은 YAML 형식으로 프롬프트를 정의하며, **코드 수정 없이 프롬프트만 교체 가능**합니다.

**Java 타겟 프롬프트 (`rules/java/`):**
- `entity.yaml`: JPA Entity 클래스 생성
- `repository.yaml`: JPA Repository 인터페이스 생성
- `variable.yaml`: PL/SQL 변수 → Java 타입 변환
- `command.yaml`: Command DTO 클래스 생성
- `service_skeleton.yaml`: Service 메서드 시그니처 생성
- `service.yaml`: **Service 메서드 바디 생성** (Parent Context 지원)
- `service_summarized.yaml`: **대용량 노드 스켈레톤 생성** (자식 `...code...` 처리)
- `service_exception.yaml`: **예외처리 try-catch 블록 생성**
- `controller.yaml`: REST Controller 메서드 생성

**Python 타겟 프롬프트 (`rules/python/`):**
- `entity.yaml`: SQLAlchemy Model 클래스 생성 (확장 예시)
- *향후 추가 예정...*

**Rule 파일 구조:**
```yaml
name: "역할 이름"
description: "설명"
version: "1.0"

input_schema:
  required:
    - field1  # 필수 입력
    - field2
  optional:
    - field3:
        default: ""

prompt: |
  프롬프트 내용 (Jinja2 템플릿)
  {{field1}}
  {{field2}}
```

**⚠️ 중요 변경사항:**
- `llm_config` 섹션 제거됨 (LLM 파라미터는 `util/llm_client.py`에서 중앙 관리)
- Rule 파일은 프롬프트 내용만 포함

**장점:**
- ✅ **코드 변경 없이 프롬프트 수정** (YAML만 편집)
- ✅ **다국어/다타겟 언어 확장 용이** (python, typescript 등 추가 가능)
- ✅ **버전 관리 쉬움** (Git으로 프롬프트 이력 추적)
- ✅ **LLM 파라미터 중앙 관리** (llm_client.py에서 일괄 설정)
- ✅ **A/B 테스트 간편** (파일 교체만)
- ✅ **비개발자 접근 가능** (프롬프트 엔지니어가 직접 튜닝)

#### 💬 `prompt/*` - Understanding 프롬프트 (레거시)
LLM에게 전달할 프롬프트를 정의합니다. **(Understanding 단계용으로만 사용)**

**이해 단계 프롬프트:**
- `understand_ddl.py`: DDL 테이블/컬럼 분석
- `understand_prompt.py`: PL/SQL 구문 분석 및 관계 추출
- `understand_summarized_prompt.py`: 큰 노드 요약 생성
- `understand_column_prompt.py`: 컬럼 역할 파악
- `understand_variables_prompt.py`: 변수 분석

*※ Converting 단계는 `rules/` 디렉토리의 YAML 파일을 사용합니다.*

#### 🛠️ `util/llm_client.py`
LLM API 클라이언트를 생성합니다.

**주요 함수:**
- `get_llm()`: ChatOpenAI 클라이언트 생성
  - 환경변수 또는 파라미터로 API 키, 모델, base URL 설정
  - 기본 모델: `gpt-4.1`
  - 기본 base URL: `https://api.openai.com/v1`
- `resolve_defaults()`: 환경변수 기본값 해결
- `get_openai_client()`: OpenAI 클라이언트 생성

#### 🛠️ `util/utility_tool.py`
공통 유틸리티 함수를 제공합니다.

**파일 처리:**
- `save_file()`: 비동기 파일 저장

**경로 유틸리티:**
- `build_java_base_path()`: Java 소스 저장 경로 생성

**문자열 변환:**
- `convert_to_pascal_case()`: 스네이크 케이스 → 파스칼 케이스
- `convert_to_camel_case()`: 스네이크 케이스 → 카멜 케이스
- `convert_to_upper_snake_case()`: 파스칼/카멜 케이스 → 대문자 스네이크 케이스
- `add_line_numbers()`: 코드에 라인 번호 추가

**스키마/테이블 파싱:**
- `parse_table_identifier()`: 테이블 식별자 파싱 (schema.table@dblink 분리)

**코드 분석:**
- `calculate_code_token()`: 토큰 수 계산 (tiktoken 사용)
- `build_variable_index()`: 변수 노드를 startLine 기준으로 인덱싱
- `extract_used_variable_nodes()`: 특정 라인에서 사용된 변수 추출
- `collect_variables_in_range()`: **특정 라인 범위 내 변수 수집** (전처리 단계에서 사용)
- `extract_used_query_methods()`: **특정 라인 범위 내 JPA 메서드 수집** (전처리 단계에서 사용)

---

## 🔧 문제해결

### 자주 발생하는 문제

#### 1. 생성된 Java 코드에 임의 return 문이 추가됨

**증상:**
```java
vPatientExists = tpjPatientRepository.findPatientByPatientKey(pPatientKey);
if (vPatientExists == 0) {
    return "환자가 존재하지 않습니다.";  // ❌ 원본에 없던 return!
}
// 이후 코드가 실행되지 않음
```

**원인:**
- LLM이 SELECT 결과를 검증하는 로직을 임의로 추가
- 원본 PL/SQL은 단순히 COUNT만 수행하는데, LLM이 "의미를 추론"하여 검증 로직 생성

**해결책:**
프롬프트에 다음 지침 강화:
```
⚠️ 중요: 원본 PL/SQL 구조를 정확히 따르세요. 
- 원본에 없는 return 문, if 문, 검증 로직을 추가하지 마세요
- SELECT는 단순히 변수에 값을 할당하는 것입니다
- 제어 흐름은 원본과 동일해야 합니다
```

#### 2. 변수 타입이 잘못 변환됨

**증상:**
```java
String pResultCode = 0L;  // ❌ String에 Long 할당
```

**원인:**
- Parent Context 없이 변환하여 변수 타입 정보 부족

**해결책:**
- Parent Context 전달 기능이 구현됨 (v2.0)
- 부모 노드의 Java 코드를 자식 노드 변환 시 참조

#### 3. IF-ELSIF-ELSE 구조가 잘못 변환됨

**증상:**
```java
if (condition1) {
    // ...
} else if (condition2) {  // ❌ 중괄호 누락
    // ...
    if (condition3) {     // ❌ 잘못된 중첩
```

**원인:**
- LLM이 복잡한 중첩 구조를 제대로 파악하지 못함

**해결책:**
- Parent Context로 전체 제어 흐름 구조 제공
- 프롬프트에 "IF-ELSIF는 Java의 if-else if-else로 변환" 명시

---

## 🎨 Rule 파일 커스터마이징 가이드

### 📝 프롬프트 수정 방법

Rule 파일을 수정하면 **코드 재배포 없이 즉시 적용**됩니다.

#### 예시 1: Entity 생성 지침 수정

```bash
# rules/java/entity.yaml 편집
nano rules/java/entity.yaml
```

```yaml
# 데이터 타입 매핑 규칙 수정
5. 데이터 타입 매핑
   - NUMBER, NUMERIC -> Long  # 기존
   - NUMBER, NUMERIC -> BigDecimal  # 수정 (더 정확한 숫자 처리)
```

**결과**: 다음 변환부터 자동 적용 (서버 재시작 불필요)

#### 예시 2: Service 변환에 로깅 추가

```yaml
# rules/java/service.yaml 편집
prompt: |
  ...기존 내용...
  
  ### 🆕 추가 규칙
  - 모든 메서드 시작 부분에 로깅 추가:
    log.info("메서드 시작: {메서드명}");
  - 메서드 종료 전에 로깅 추가:
    log.info("메서드 완료: {메서드명}");
```

#### 예시 3: Repository 성능 힌트 추가

```yaml
# rules/java/repository.yaml 편집
prompt: |
  ...기존 내용...
  
  ### 성능 최적화 규칙
  1. SELECT는 가능하면 Projection 사용
  2. 대량 조회는 Pageable 파라미터 추가
  3. INDEX가 있는 컬럼은 메서드명에 우선 배치
```

---

### 🌍 다국어 지원

여러 언어 버전의 프롬프트를 준비하고 전환할 수 있습니다.

```bash
# 영어 버전 생성
cp -r rules/java rules/java_en
nano rules/java_en/entity.yaml  # 영어로 번역

# 요청 시 언어 선택
{
  "projectName": "OrderSystem",
  "targetLang": "java_en"  # ← 영어 프롬프트 사용
}
```

---

### 🔧 새로운 타겟 언어 추가 (예: Python)

#### Step 1: Rule 파일 디렉토리 생성

```bash
mkdir -p rules/python
```

#### Step 2: Python용 프롬프트 작성

```yaml
# rules/python/entity.yaml
name: "SQLAlchemy Model 생성"
prompt: |
  당신은 Python SQLAlchemy 전문가입니다.
  다음 테이블을 SQLAlchemy Model로 변환하세요:
  
  {{table_json_data}}
  
  [변환 규칙]
  - Base = declarative_base() 사용
  - Column, Integer, String 등 타입 정의
  - __tablename__ 설정
  ...
```

#### Step 3: API 요청

```json
{
  "projectName": "OrderSystem",
  "dbms": "postgres",
  "targetLang": "python",  # ← Python으로 변환
  "systems": [...]
}
```

**결과**: Python FastAPI + SQLAlchemy 프로젝트 생성!

---

### 📊 A/B 테스트

두 가지 프롬프트 버전을 테스트할 수 있습니다.

```bash
# 버전 1 백업
cp roles/java/service.yaml roles/java/service_v1.yaml

# 버전 2 작성
nano roles/java/service.yaml  # 새로운 지침 추가

# 테스트 후 롤백
cp roles/java/service_v1.yaml roles/java/service.yaml
```

---

### ⚡ 성능 최적화: Rule 파일 캐싱

`PromptLoader`는 LRU 캐싱을 사용합니다:
- 로드한 YAML 파일은 메모리에 캐싱
- 최대 32개 rule 파일 캐시
- 파일 수정 시 서버 재시작하면 캐시 갱신

**캐시 수동 초기화:**
```python
# 필요 시 캐시 클리어
loader = PromptLoader(target_lang='java')
loader.clear_cache()
```

---

## 🧪 테스트

이 프로젝트는 pytest 기반 테스트를 제공합니다.

### 환경 변수 설정

테스트 실행 전 다음 환경 변수를 설정하세요:

```bash
# 테스트 세션 UUID (실제 사용자 세션과 구분하기 위한 값)
export TEST_SESSION_UUID="TestSession_5"

# LLM API 키 (테스트 세션이 EN_TestSession 또는 KO_TestSession이 아닌 경우 필수)
export LLM_API_KEY="your-api-key"

# 응답 언어 설정
export TEST_LOCALE="ko"
```

**테스트 세션 특수 처리:**
- `Session-UUID`가 `EN_TestSession` 또는 `KO_TestSession`인 경우
- 환경 변수의 `LLM_API_KEY` 또는 `API_KEY`를 자동으로 사용
- 헤더에 API 키를 포함하지 않아도 됨

### 테스트 실행 방법

#### 1️⃣ Understanding 테스트 (통합)

```bash
# 전체 파이프라인 테스트
pipenv run pytest test/test_understanding.py -v -s

# 또는
pipenv run python test/test_understanding.py
```

**동작:**
- `understand_project()` 전체 파이프라인 실행
- 스트리밍 응답 검증
- Neo4j 그래프 데이터 검증
- DDL 처리 검증

---

#### 2️⃣ Converting 테스트

**A. 개별 테스트 (단계별 디버깅용)**

```bash
# Entity만 테스트
pipenv run pytest test/test_converting.py::TestEntityGeneration -v -s

# Repository만 테스트
pipenv run pytest test/test_converting.py::TestRepositoryGeneration -v -s

# Service Skeleton만 테스트
pipenv run pytest test/test_converting.py::TestServiceGeneration -v -s

# Config 파일만 테스트
pipenv run pytest test/test_converting.py::TestConfigGeneration -v -s

# 전체 개별 테스트 순차 실행
pipenv run python test/test_converting.py
```

**동작:**
- 각 Generator를 **독립적으로** 실행
- `test/test_converting_results.json`에 중간 결과 저장
- 다음 테스트가 이전 결과 사용
- **빠른 검증** 가능 (특정 단계만)

**B. 통합 테스트 (실제 API 동작 검증) 🆕**

```bash
# convert_to_springboot() 전체 파이프라인 테스트
pipenv run pytest test/test_converting.py::TestConvertingPipeline -v -s
```

**동작:**
- `convert_to_springboot()` 전체 파이프라인 실행
- 스트리밍 응답 검증
- 모든 파일 생성 검증 (Entity, Repository, Service, Controller, Config, Main)
- 단계 간 데이터 전달 검증
- **실제 API와 동일한 동작**

---

#### 3️⃣ 전체 테스트 (배포 전 필수)

```bash
# 모든 테스트 실행
pipenv run pytest test/ -v -s

# 또는 빠른 실행 (출력 최소화)
pipenv run pytest test/ -v
```

---

### 테스트 전략

| 상황 | 명령어 | 소요 시간 |
|------|--------|----------|
| **빠른 확인** | `pytest test/test_converting.py::TestEntityGeneration -v` | ~30초 |
| **단계별 디버깅** | `pytest test/test_converting.py::TestRepositoryGeneration -v` | ~1분 |
| **API 동작 검증** | `pytest test/test_converting.py::TestConvertingPipeline -v` | ~5분 |
| **배포 전 전체** | `pytest test/ -v` | ~10분 |

### 테스트 데이터 준비

테스트를 실행하려면 다음 구조로 데이터를 준비하세요:

```
data/{TEST_SESSION_UUID}/{TEST_PROJECT_NAME}/
├── src/{SystemName}/{fileName}.sql               # 원본 PL/SQL 파일
├── analysis/{SystemName}/{baseName}.json         # ANTLR 분석 결과
└── ddl/*.sql                                     # DDL 파일 (선택사항)
```

**예시:**
```
data/TestSession/HOSPITAL_PROJECT/
├── src/
│   └── HOSPITAL_RECEPTION/
│       └── SP_HOSPITAL_RECEPTION.sql
├── analysis/
│   └── HOSPITAL_RECEPTION/
│       └── SP_HOSPITAL_RECEPTION.json
└── ddl/
    └── DDL_HOSPITAL_RECEPTION.sql
```
---

#### Neo4j 데이터 확인

Neo4j Browser에서 데이터를 직접 조회할 수 있습니다:

```cypher
// 모든 정보 조회
MATCH (n) RETURN n

//포함 관게 제외 모두 조회
MATCH (n)-[r]->(m)
WHERE type(r) <> 'CONTAINS'
RETURN n, r, m;

// 모든 정보 삭제
MATCH (n)
DETACH DELETE n
```
---

## 🎓 추가 자료

### 관련 기술 문서

- [FastAPI 공식 문서](https://fastapi.tiangolo.com/)
- [Neo4j 공식 문서](https://neo4j.com/docs/)
- [ANTLR 공식 문서](https://www.antlr.org/)
- [Spring Boot 공식 문서](https://spring.io/projects/spring-boot)
- [OpenAI API 문서](https://platform.openai.com/docs/)

### 프로젝트 철학

> **"사람 입장에서 이해하기 쉬운 관계를 먼저 만들고, 그 위에 코드 생성을 쌓습니다."**

- 📊 **그래프 우선**: 코드의 관계를 그래프로 표현하여 직관적으로 이해
- 🔄 **단계별 처리**: 이해 → 변환으로 명확히 분리하여 각 단계에 집중
- 📝 **보수적 병합**: MERGE를 사용하여 중복 방지, 기존 데이터 보존
- 📢 **투명한 진행**: 스트리밍으로 실시간 진행 상황 공개
- 💬 **쉬운 표현**: "읽는다/쓴다/호출한다" 같은 직관적인 관계명 사용

---

## 📞 지원

문제가 발생하거나 질문이 있으시면 이슈를 생성해 주세요.

---

## 📄 라이선스

이 프로젝트는 [라이선스명]에 따라 배포됩니다.

---

<div align="center">

**Made with ❤️ by Legacy Modernizer Team**

⭐ 이 프로젝트가 유용하다면 Star를 눌러주세요!

</div>
