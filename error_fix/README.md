# 오류 수정 스크립트

컴파일 오류가 발생한 변환된 코드를 자동으로 수정하고 재병합하는 스크립트입니다.

## 기능

1. **오류 메시지 파싱**: 컴파일 오류에서 오류 번호와 라인 번호 추출
2. **블록 찾기**: Neo4j에서 오류가 발생한 CONVERSION_BLOCK 찾기 (자식 노드 우선)
3. **코드 수정**: LLM을 사용하여 오류 수정
4. **코드 업데이트**: Neo4j의 CONVERSION_BLOCK 노드 업데이트
5. **코드 병합**: 수정된 블록을 기존 코드 구조에 재병합 (부모-자식 관계 고려)

## 사용법

### Python 모듈로 사용

```python
from error_fix.main import fix_conversion_error

fixed_code = await fix_conversion_error(
    error_message="ORA-00942: table or view does not exist at line 10",
    folder_name="HOSPITAL_RECEPTION",
    file_name="SP_HOSPITAL_RECEPTION.sql",
    procedure_name="TPX_HOSPITAL_RECEPTION",
    user_id="KO_TestSession",
    project_name="HOSPITAL_MANAGEMENT",
    api_key="your-api-key",
    locale="ko",
    conversion_type="dbms",
    target="oracle"
)
```

### CLI로 실행

```bash
python -m error_fix.main \
    "ORA-00942: table or view does not exist at line 10" \
    "HOSPITAL_RECEPTION" \
    "SP_HOSPITAL_RECEPTION.sql" \
    "TPX_HOSPITAL_RECEPTION" \
    "KO_TestSession" \
    "HOSPITAL_MANAGEMENT" \
    "your-api-key" \
    "ko" \
    "dbms" \
    "oracle"
```

## 구조

```
error_fix/
├── __init__.py          # 모듈 초기화
├── error_parser.py      # 오류 메시지 파싱
├── block_finder.py      # Neo4j에서 블록 찾기
├── code_fixer.py        # LLM을 통한 코드 수정
├── code_merger.py       # 코드 병합 로직
├── main.py              # 메인 진입점
└── README.md           # 이 파일
```

## 동작 원리

1. **오류 파싱**: 오류 메시지에서 라인 번호와 오류 코드 추출
   - Oracle: `ORA-00942: ... at line 10`
   - SQL Server: `Msg 102, ... Line 5`

2. **블록 검색**: 
   - CONVERTING 노드 찾기
   - 해당 라인 번호를 포함하는 CONVERSION_BLOCK 찾기
   - 자식 노드가 있으면 자식 노드를 우선 선택 (더 구체적인 범위)

3. **코드 수정**:
   - 원본 코드, 변환된 코드, 오류 메시지를 LLM에 전달
   - 수정된 코드 생성

4. **노드 업데이트**:
   - Neo4j의 CONVERSION_BLOCK 노드의 `converted_code` 업데이트
   - `updated_at` 타임스탬프 추가

5. **코드 병합**:
   - 모든 CONVERSION_BLOCK을 NEXT 관계 순서대로 정렬
   - 부모 블록은 자식 블록들을 포함하여 병합
   - 스켈레톤 코드와 최종 병합

## 주의사항

- 이 스크립트는 별도 리포지토리로 관리될 예정입니다
- Neo4j 연결이 필요합니다
- LLM API 키가 필요합니다
- 스켈레톤 코드는 매번 재생성됩니다

