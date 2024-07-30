import json
import os
import anthropic
import sys

api_key = os.environ.get('ANTHROPIC_API_KEY')
client = anthropic.Anthropic(api_key=api_key)

prompt_template = """
당신은 클린 아키텍처 원칙을 따르는 스프링부트 기반의 자바 애플리케이션을 개발하는 소프트웨어 엔지니어입니다. 주어진 JSON 형식의 테이블 데이터를 기반으로 자바 Entity 클래스를 생성하는 작업을 맡았습니다.

테이블 데이터(JSON)입니다:
{table_json_data}

테이블 데이터(JSON)을 Entity 클래스로 전환할 때, 아래를 참고하여 작업하세요:
1. 각 테이블(JSON) 객체는 하나의 Entity 클래스로 변환되어야 합니다.
2. 각 테이블(JSON) 객체의 'name'은 파스칼 표기법을 적용한 클래스 이름으로 사용됩니다. (예: B_Plcy_Month -> BPlcyMonth)
3. 클래스의 이름과 'entityName'은 복수형이 아닌 단수형으로 표현하세요. (예: Employees -> Employee)
4. 'fields' 배열의 각 항목은 카멜 표기법을 적용한 클래스의 속성으로 사용됩니다. (예: B_Plcy_Month -> bPlcyMonth)
5. 각 속성은 적절한 자바 데이터 타입과 함께 private 접근 제한자를 가집니다. (데이터 타입의 경우, 되도록이면 int 대신 long을 사용하세요.)

아래는 자바 Entity 클래스의 기본 구조입니다:
package com.example.{project_name}.entity;

import jakarta.persistence.*;
import lombok.Data;
import java.time.LocalDate

@Entity
@Table(name = "TableName")
@Data
public class EntityName {{
    @Id
    @GeneratedValue(strategy = GenerationType.AUTO)
    private Long id;
    private DataType fieldName1;
    private DataType fieldName2;
    ...
}}

아래는 결과 예시로, 부가 설명 없이 결과만을 포함하여, 다음 JSON 형식으로 반환하세요:
{{
    "analysis": [
        {{
            "entityName": "EntityName",
            "code": "Java Code"
        }}
    ]
}}
"""

def convert_entity_code():
    try:
        print("함수 시작", file=sys.stderr)
        
        # 샘플 테이블 데이터
        sample_table_data = [
            {
                "name": "B_Plcy_Month",
                "fields": [
                    {"name": "plcy_no", "type": "VARCHAR(20)", "nullable": False},
                    {"name": "plcy_month", "type": "VARCHAR(6)", "nullable": False},
                    {"name": "plcy_stat", "type": "VARCHAR(2)", "nullable": True},
                    {"name": "prem_amt", "type": "DECIMAL(15,2)", "nullable": True},
                    {"name": "create_dt", "type": "DATE", "nullable": False}
                ]
            },
            {
                "name": "Customers",
                "fields": [
                    {"name": "customer_id", "type": "INT", "nullable": False},
                    {"name": "first_name", "type": "VARCHAR(50)", "nullable": False},
                    {"name": "last_name", "type": "VARCHAR(50)", "nullable": False},
                    {"name": "email", "type": "VARCHAR(100)", "nullable": True},
                    {"name": "phone_number", "type": "VARCHAR(20)", "nullable": True},
                    {"name": "registration_date", "type": "DATE", "nullable": False}
                ]
            }
        ]

        # 프로젝트 이름
        project_name = "insurance"

        print("데이터 준비 완료", file=sys.stderr)

        # 프롬프트 템플릿에 실제 데이터 삽입
        prompt = prompt_template.format(table_json_data=json.dumps(sample_table_data), project_name=project_name)
        
        print("API 호출 시작", file=sys.stderr)
        message = client.messages.create(
            model="claude-3-sonnet-20240229",
            max_tokens=2000,
            temperature=0.0,
            system=prompt,
            messages=[
                {"role": "user", "content": "테이블 데이터를 Entity 클래스로 변환해주세요."}
            ]
        )
        print("API 호출 완료", file=sys.stderr)
        
        # 결과 반환 (추가 파싱 없이)
        return message.content

    except Exception as e:
        print(f"오류 발생: {str(e)}", file=sys.stderr)
        return None

if __name__ == "__main__":
    print("스크립트 시작", file=sys.stderr)
    result = convert_entity_code()
    print("최종 결과:", result, file=sys.stderr)
    print("스크립트 종료", file=sys.stderr)