import json
import logging
from langchain.globals import set_llm_cache
from langchain_community.cache import SQLiteCache
from langchain_openai import ChatOpenAI
from langchain_anthropic import ChatAnthropic
from langchain_core.output_parsers import JsonOutputParser
from langchain.prompts import PromptTemplate
from langchain.schema.runnable import RunnablePassthrough

prompt = PromptTemplate.from_template(
"""
당신은 Java JUnit 테스트 코드 생성 전문가입니다. 주어진 정보를 분석하여 Given-When-Then 패턴의 테스트 코드를 생성해야 합니다.

[입력 정보]
===============================================
1. 프로시저 호출 정보:
{procedure_info}

2. Given 로그 데이터:
{given_log}

3. Then 로그 데이터:
{then_log}

[요구사항]
===============================================
1. JUnit5 기반의 테스트 코드를 생성합니다.
2. Given-When-Then 패턴을 명확히 구분하여 작성합니다.
3. Given 섹션:
   - 로그 데이터를 분석하여 필요한 엔티티 객체를 생성
   - Repository를 통한 데이터 저장
4. When 섹션:
   - 프로시저 호출 정보를 기반으로 메서드 호출 코드 작성
5. Then 섹션:
   - Then 로그 데이터를 기반으로 검증 코드 작성
   - assertEquals 등 적절한 검증 메서드 사용

[예시]
===============================================
입력:
프로시저 정보: calculateSalary(employeeId, workDays)
Given 로그: {"employeeId": "E001", "baseSalary": 1000}
Then 로그: {"employeeId": "E001", "finalSalary": 1500}

출력:
@Test
void calculateSalaryTest() {
    // Given
    Employee employee = new Employee();
    employee.setEmployeeId("E001");
    employee.setBaseSalary(1000);
    employeeRepository.save(employee);

    // When
    salaryService.calculateSalary("E001", 5);

    // Then
    employeeRepository.findById("E001").ifPresent(result -> {
        assertEquals(1500, result.getFinalSalary());
    });
}

[출력 형식]
===============================================
테스트 클래스와 메서드를 포함한 완전한 JUnit 테스트 코드를 생성합니다.
필요한 import 구문도 포함해주세요.
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