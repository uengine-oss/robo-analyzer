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


[테이블 및 프로시저 정보]
===============================================
1. 테이블 정보:
{table_names}
- 각 테이블에 대해 다음 형식으로 import와 Repository 선언 필요:
  - import: 
    import com.example.demo.repository.[테이블명PascalCase]Repository
    import com.example.demo.entity.[테이블명PascalCase]
  - @Autowired:
    @Autowired
    private [테이블명PascalCase]Repository [테이블명camelCase]Repository;

2. 패키지 정보:
- 패키지명: {package_name}
- import:
  import com.example.demo.service.[패키지명PascalCase]Service
- @Autowired:
  @Autowired
  private [패키지명PascalCase]Service [패키지명camelCase]Service;

3. 호출될 프로시저: {procedure_name}
- When 섹션에서 다음 형식으로 호출:
  [패키지명camelCase]Service.[호출프로시저명camelCase]([파라미터])


[입력 정보 상세]
===============================================
1. 프로시저 호출 정보:
{procedure_info}
- 형식: (프로시저_이름, 파라미터_목록)
- 예시: ("updateUserStatus", ["userId", "status"])

2. Given 로그 데이터:
{given_log}
- 이 데이터를 기반으로 테스트의 초기 상태를 설정

3. Then 로그 데이터:
{then_log}
- 이 데이터를 기반으로 테스트의 검증 로직 구성

4. Jpa 쿼리 메서드 :
@Query("SELECT COUNT(s) FROM TpjSalary s WHERE s.empKey = :empKey AND s.payDate = :payDate")
Long countByEmpKeyAndPayDate(@Param("empKey") String empKey, @Param("payDate") LocalDate payDate);

@Query("SELECT s FROM TpjSalary s WHERE s.empKey = :empKey AND s.payDate = :payDate")
TpjSalary findByEmpKeyAndPayDate(@Param("empKey") String empKey, @Param("payDate") LocalDate payDate);

@Query("SELECT s FROM TpjSalary s WHERE s.empKey = :empKey AND s.payDate = :payDate")
TpjSalary findSalaryForUpdate(@Param("empKey") String empKey, @Param("payDate") LocalDate payDate);
- 이 데이터를 기반으로 테스트의 검증 로직 구성


[테스트 코드 생성 규칙]
===============================================
1. 테스트 클래스 구조:
   - @SpringBootTest 또는 적절한 테스트 어노테이션 사용
   - 필요한 의존성 @Autowired로 주입

2. Given 섹션 작성:
   - given_log의 데이터를 분석하여 초기 상태 설정
   - 필요한 모든 엔티티 객체 생성 및 초기화
   - Repository를 통한 데이터 저장
   - @BeforeEach 사용이 필요한 경우 별도 메서드로 분리

3. When 섹션 작성:
   - procedure_info를 분석하여 정확한 메서드 호출 구문 작성
   - 프로시저의 모든 파라미터를 올바른 순서로 전달
   - 예외 발생 가능성이 있는 경우 try-catch 구문 사용

4. Then 섹션 작성:
   - then_log의 데이터를 분석하여 변경된 항목 검증
   - 모든 변경 사항에 대해 구체적인 검증 로직 구현
   - assertThat(), assertEquals() 등 적절한 검증 메서드 사용
   - 데이터베이스 상태 검증을 위한 Repository 조회 포함

5. 테스트 케이스 명명:
   - 테스트 클래스명은 핵심 시나리오 값을 포함하여 구체적으로 작성
   - "[프로시저명]_[주요입력값]_[핵심결과값]Test" 형식 사용
   - 클래스 명은 30 글자를 넘기지 마세요.
   - 예시: 
     - TpjSalaryFinal500AmountTest (26자)
     - TpjSalaryDeductMinus300Test (26자)
     - TpjEmployeeDept001ExistTest (26자)

6. 데이터 타입 규칙:
   - 숫자형 데이터:
     - 정수: Long 타입 사용 (값 뒤에 'L' 접미사 추가)
     - 실수: Double 타입 사용
   - 날짜/시간:
     - 날짜만 있는 경우: LocalDate 사용
     - 시간 포함: LocalDateTime 사용
     - Date 키워드만 있는 경우 LocalDate 타입으로 로그에 Time까지 있어도 무시하고 날짜만 저장하세요. 절대로 LocalDateTime으로 parse해서 저장하지말고 날짜만 저장하세요.


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
테이블: ["EMPLOYEE"]
패키지명: "SALARY_CALCULATOR"
호출프로시저명: "CALCULATE_MONTHLY_SALARY"
프로시저 정보: ("calculateMonthlySalary", ["employeeId", "workDays"])
Given 로그: {{
    "operation": "c",
    "table": "EMPLOYEE",
    "data": {{
        "employeeId": "E001",
        "baseSalary": 1000
    }}
}}
Then 로그: {{
    "operation": "u",
    "table": "EMPLOYEE",
    "data": {{
        "employeeId": "E001",
        "baseSalary": 1500
    }}
}}

출력:
{{
    "className": "TpjSalaryFinal1500AmountTest",
    "testCode": "
        package com.example.demo;
        import org.junit.jupiter.api.Test;
        import org.springframework.boot.test.context.SpringBootTest;
        import static org.assertj.core.api.Assertions.assertThat;
        import com.example.demo.repository.EmployeeRepository;
        import com.example.demo.entity.Employee;
        import com.example.demo.service.SalaryCalculatorService;
        import java.time.LocalDate;
        import java.time.LocalDateTime;

        @SpringBootTest
        class TpjSalaryFinal1500AmountTest {{

            @Autowired
            private EmployeeRepository employeeRepository;
            
            @Autowired
            private SalaryCalculatorService salaryCalculatorService;

            @Test
            void calculateSalary_WhenEmp001WorkDays5_ThenFinalSalary1500() {{
                // Given
                Employee employee = new Employee();
                employee.setEmployeeId("E001");
                employee.setBaseSalary(1000);
                employeeRepository.save(employee);

                // When
                salaryCalculatorService.calculateMonthlySalary("E001", 5);

                // Then
                Employee result = employeeRepository.findByEmployeeId("E001")
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
{{
    "className": "테스트 클래스 이름",
    "testCode": "전체 테스트 코드 (import 구문 포함)"
}}



"""
)


def generate_test_code(table_names: list, package_name: str, procedure_name: str, procedure_info: dict, given_log: dict, then_log: dict) -> str:
    try:
        chain = (
            RunnablePassthrough()
            | prompt
            | llm
            | JsonOutputParser()
        )
        
        result = chain.invoke({
            "table_names": table_names,
            "package_name": package_name,
            "procedure_name": procedure_name,
            "procedure_info": json.dumps(procedure_info, indent=2, ensure_ascii=False),
            "given_log": json.dumps(given_log, indent=2, ensure_ascii=False),
            "then_log": json.dumps(then_log, indent=2, ensure_ascii=False)
        })
        
        return result
    except Exception:
        err_msg = "테스트 코드 생성 중 오류가 발생했습니다."
        logging.error(err_msg, exc_info=True)
        raise Exception(err_msg)