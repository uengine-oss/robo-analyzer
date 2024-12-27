import json
import logging
import os
from langchain.globals import set_llm_cache
from langchain_community.cache import SQLiteCache
from langchain_anthropic import ChatAnthropic
from langchain_core.output_parsers import JsonOutputParser
from langchain.prompts import PromptTemplate
from langchain.schema.runnable import RunnablePassthrough
from util.exception import LLMCallError

db_path = os.path.join(os.path.dirname(__file__), 'langchain.db')
set_llm_cache(SQLiteCache(database_path=db_path))
llm = ChatAnthropic(model="claude-3-5-sonnet-20241022", max_tokens=8000, temperature=0.1)

xml_prompt = PromptTemplate.from_template("""
당신은 MyBatis를 사용하는 스프링부트 기반의 자바 애플리케이션을 개발하는 소프트웨어 엔지니어입니다.
주어진 엔티티 이름, 엔티티 코드, 쿼리 메소드 정보를 기반으로 MyBatis Mapper XML 파일을 생성하는 작업을 맡았습니다.

엔티티 이름: {entity_name}
엔티티 코드: {entity_code}
쿼리 메소드 정보: {query_methods}
시퀀스 정보: {sequence_data}

                                          
[SECTION 1] Mapper XML 생성 규칙
===============================================
1. 기본 구조
   - XML 선언과 DOCTYPE 선언 포함
   - mapper 태그의 namespace는 "com.example.repository.엔티티명Repository" 형식 사용

2. resultMap 규칙
   - id는 "baseResultMap"으로 통일
   - type은 "com.example.dto.엔티티명Dto" 형식 사용
   - 엔티티의 모든 필드를 매핑
   - 컬럼명은 스네이크 케이스, 프로퍼티명은 카멜 케이스로 매핑

3. CRUD 쿼리 생성 규칙
   - 제공된 쿼리 메소드 정보를 기반으로 SQL 작성
   - 메소드명 분석을 통한 쿼리 유형 결정
   - 동적 쿼리는 필요한 경우 조건절 사용
   - 페이징 처리가 필요한 경우 LIMIT, OFFSET 구문 추가

4. 시퀀스 처리
   - 시퀀스가 있는 경우 INSERT 문에서 시퀀스.NEXTVAL 사용
   - 시퀀스명은 제공된 시퀀스 정보에서 참조

[SECTION 2] Mapper XML 기본 템플릿
===============================================
<?xml version="1.0" encoding="UTF-8"?>
<!DOCTYPE mapper PUBLIC "-//mybatis.org//DTD Mapper 3.0//EN" "http://mybatis.org/dtd/mybatis-3-mapper.dtd">
<mapper namespace="com.example.repository.UserRepository">
    
    <!-- 기본 resultMap -->
    <resultMap id="baseResultMap" type="com.example.dto.UserDto">
        <id column="user_id" property="userId"/>
        <result column="user_name" property="userName"/>
        <result column="reg_date" property="regDate"/>
    </resultMap>

    <!-- 기본 조회 쿼리 -->
    <select id="findById" resultMap="baseResultMap">
        SELECT * FROM users WHERE user_id = #{{userId}}
    </select>

    <!-- 시퀀스를 사용하는 등록 쿼리 예시 -->
    <insert id="save">
        INSERT INTO users (
            user_id,
            user_name,
            reg_date
        ) VALUES (
            SEQ_USER.NEXTVAL,
            #{{userName}},
            SYSDATE
        )
    </insert>
</mapper>
                                          

[SECTION 3] JSON 출력 형식
===============================================
부가 설명 없이 결과만을 포함하여, 다음 JSON 형식으로 반환하세요:
{{
    "analysis": [
        {{
            "mapperName": "mapperName",
            "code": "XML Code"
        }}
    ]
}}
""")

# 역할: 엔티티 정보와 쿼리 메서드 정보를 기반으로 MyBatis Mapper XML 파일을 생성하는 함수입니다.
#
# 매개변수: 
#   - entity_name: 엔티티 이름
#   - entity_code: 엔티티 코드
#   - query_methods: 쿼리 메서드 정보
#   - sequence_data: 시퀀스 정보
#
# 반환값: 
#   - 생성된 MyBatis Mapper XML 파일 내용
def convert_xml_mapper(entity_name: str, entity_code: str, query_methods: dict, sequence_data: dict) -> dict:
    try:
        prompt_data = {
            "entity_name": entity_name,
            "entity_code": entity_code,
            "query_methods": json.dumps(query_methods, ensure_ascii=False, indent=2),
            "sequence_data": json.dumps(sequence_data, ensure_ascii=False, indent=2)
        }

        chain = (
            RunnablePassthrough()
            | xml_prompt
            | llm
            | JsonOutputParser()
        )
        result = chain.invoke(prompt_data)
        return result
    except Exception:
        err_msg = "매퍼 XML 생성 과정에서 LLM 호출하는 도중 오류가 발생했습니다."
        logging.error(err_msg)
        raise LLMCallError(err_msg)