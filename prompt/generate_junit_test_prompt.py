import os
import json
import logging
from langchain.globals import set_llm_cache
from langchain_community.cache import SQLiteCache
from langchain_openai import ChatOpenAI
from langchain_anthropic import ChatAnthropic
from langchain_core.output_parsers import JsonOutputParser
from langchain.prompts import PromptTemplate
from langchain.schema.runnable import RunnablePassthrough

db_path = os.path.join(os.path.dirname(__file__), 'langchain.db')
set_llm_cache(SQLiteCache(database_path=db_path))
llm = ChatAnthropic(model="claude-3-5-sonnet-20241022", max_tokens=8000, temperature=0.0)
prompt = PromptTemplate.from_template(
"""
당신은 Java JUnit 테스트 코드 생성 전문가입니다. 프로시저 호출과 데이터 변경 로그를 분석하여 완벽한 테스트 코드를 생성해야 합니다.


[입력 정보 상세]
===============================================
1. 프로시저 호출 정보:
{procedure_info}
- 형식: (프로시저_이름, 파라미터_목록)
- 예시: ("updateUserStatus", ["userId", "status"])

2. Given 로그 데이터:
{given_log}
- before: 프로시저 실행 전 초기 데이터 상태
- after: 초기 데이터 설정 후 상태
- 이 데이터를 기반으로 테스트의 초기 상태를 설정

3. Then 로그 데이터:
{then_log}
- before: 프로시저 실행 직전 데이터 상태
- after: 프로시저 실행 후 최종 데이터 상태
- 이 데이터를 기반으로 테스트의 검증 로직 구성


[테스트 코드 생성 규칙]
===============================================
1. 테스트 클래스 구조:
   - @SpringBootTest 또는 적절한 테스트 어노테이션 사용
   - 필요한 의존성 @Autowired로 주입
   - 테스트 클래스명은 프로시저명 + Test로 구성

2. Given 섹션 작성:
   - given_log의 before/after 데이터를 분석하여 초기 상태 설정
   - 필요한 모든 엔티티 객체 생성 및 초기화
   - Repository를 통한 데이터 저장
   - @BeforeEach 사용이 필요한 경우 별도 메서드로 분리

3. When 섹션 작성:
   - procedure_info를 분석하여 정확한 메서드 호출 구문 작성
   - 프로시저의 모든 파라미터를 올바른 순서로 전달
   - 예외 발생 가능성이 있는 경우 try-catch 구문 사용

4. Then 섹션 작성:
   - then_log의 before/after 비교하여 변경된 항목 검증
   - 모든 변경 사항에 대해 구체적인 검증 로직 구현
   - assertThat(), assertEquals() 등 적절한 검증 메서드 사용
   - 데이터베이스 상태 검증을 위한 Repository 조회 포함

5. 테스트 케이스 명명:
   - 테스트 클래스명은 핵심 시나리오 값을 포함하여 구체적으로 작성
   - "[프로시저명]_[주요입력값]_[핵심결과값]Test" 형식 사용
   - 예시: 
     - UpdateUserStatus_UserId123_InactiveTest
     - CancelOrder_OrderId456_StockPlus5Test
     - CalculateSalary_Grade3_Amount5000000Test

     
[코드 스타일 가이드]
===============================================
1. 들여쓰기: 4칸 공백 사용
2. 주석: 각 섹션 구분을 위한 명확한 주석 포함
3. 변수명: 카멜케이스 사용, 명확한 의미 전달
4. 검증문: 실패 메시지 포함하여 작성
5. 코드 포맷팅: 자바 표준 컨벤션 준수


[예시 코드]
===============================================
입력:
프로시저 정보: ("calculateSalary", ["employeeId", "workDays"])
Given 로그: {{
    "before": {{"employeeId": "E001", "baseSalary": 1000}},
    "after": {{"employeeId": "E001", "baseSalary": 1000}}
}}
Then 로그: {{
    "before": {{"employeeId": "E001", "baseSalary": 1000}},
    "after": {{"employeeId": "E001", "baseSalary": 1000, "finalSalary": 1500}}
}}

출력:
{{
    "className": "CalculateSalaryTest",
    "testCode": "
        import org.junit.jupiter.api.Test;
        import org.springframework.boot.test.context.SpringBootTest;
        import static org.assertj.core.api.Assertions.assertThat;

        @SpringBootTest
        class CalculateSalaryTest {{
            @Autowired
            private EmployeeRepository employeeRepository;
            
            @Autowired
            private SalaryService salaryService;

            @Test
            void should_calculate_salary_when_valid_employee_and_workdays() {{
                // Given
                Employee employee = new Employee();
                employee.setEmployeeId("E001");
                employee.setBaseSalary(1000);
                employeeRepository.save(employee);

                // When
                salaryService.calculateSalary("E001", 5);

                // Then
                Employee result = employeeRepository.findById("E001")
                    .orElseThrow(() -> new AssertionError("직원을 찾을 수 없습니다"));
                assertThat(result.getFinalSalary())
                    .as("최종 급여가 1500이어야 합니다")
                    .isEqualTo(1500);
            }}
        }}
    "
}}


[출력 형식]
===============================================
부가 설명이나 주석 없이, 응답은 다음 두 가지 정보를 JSON 형식으로 반환합니다:
{
    "className": "테스트 클래스 이름",
    "testCode": "전체 테스트 코드 (import 구문 포함)"
}



"""
)


def generate_test_code(procedure_info: dict, given_log: dict, then_log: dict) -> str:
    try:
        chain = (
            RunnablePassthrough()
            | prompt
            | llm
            | str
        )
        
        result = chain.invoke({
            "procedure_info": json.dumps(procedure_info, indent=2),
            "given_log": json.dumps(given_log, indent=2),
            "then_log": json.dumps(then_log, indent=2)
        })
        
        return result
    except Exception:
        err_msg = "테스트 코드 생성 중 오류가 발생했습니다."
        logging.error(err_msg, exc_info=True)
        raise Exception(err_msg)