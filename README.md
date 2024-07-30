# Legacy-Modernizer 프로젝트 소개

Legacy-Modernizer는 비즈니스 로직이 포함된 SQL 및 PLSQL 문을 사이퍼 쿼리로 변환한 후, 이를 Neo4j를 통해 그래프로 표현하는 프로젝트입니다. 이를 통해 복잡한 데이터 관계를 시각적으로 이해할 수 있으며, 기존의 레거시 코드를 현대적인 데이터베이스 시스템으로 마이그레이션하는 데 도움을 줍니다.
<br>
<br>

### 사전 요구사항

프로젝트를 실행하기 전에 다음 도구와 라이브러리가 설치되어 있어야 합니다

- Node.js
- Neo4J DeskTop
- Python 3.12 이상
<br>
<br>

## 시작하기

**1. 프로젝트 클론**

```bash
git clone <repository-url>
```

**2. 가상환경 설치 및 설정**
```bash
pip install pipenv # 가상환경 설치
pipenv install     # 패키지 설치  
pipenv shell       # 가상환경 생성 및 활성화
ctrl + shift + p   # 생성된 가상환경을 파이썬 인터프리터로 설정
```

**3. 애플리케이션 실행**
```bash
python main.py
```