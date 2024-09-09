import logging
import os
from langchain.globals import set_llm_cache
from langchain_community.cache import SQLiteCache
from langchain.prompts import PromptTemplate
from langchain.schema.runnable import RunnablePassthrough
from langchain_core.output_parsers import StrOutputParser
from langchain_openai import ChatOpenAI
from util.exception import LLMCallError

db_path = os.path.join(os.path.dirname(__file__), 'langchain.db')
set_llm_cache(SQLiteCache(database_path=db_path))
llm = ChatOpenAI(model_name="gpt-4o")

prompt = PromptTemplate.from_template(
"""
You are an expert in object-oriented programming and domain-driven design


A Cypher query that needs to be converted into a Java architecture:
{code}


Design the tables from the Cypher query as object-oriented Java classes, considering the following requirements to create them as Aggregate Roots using JPA:
1. Derive aggregates based on data cohesion: Define tables that should be in a composition relationship, considering related data processing operations, as Value Objects in a containment relationship
2. If there is logic that separates processing based on certain field values, use polymorphism to divide this table into an inheritance structure
3. Add related operations as Java methods in the appropriate member classes.(important)


Previous Java Code(chatHistory):
{chatHistory}


User's Chat (requirements):
{chat}


If you are given the previous source code and chat, do the following:
1. The chat will contain your requirements. You can apply this requirement to the previous code to return the result
2. If the chat consists of content that seems to be personal and unrelated to the programming or source code, output "Incorrect information provided.\n\n\n" in text format.


Please String format the output as follows:
1. Don't start with a batik ``` code block at first
2. Output the results in String format only, without any additional explanations or descriptions.
3. Each entity class should have a title, emphasized with '##' in text format, and the code should be formatted in markdown code blocks.

\\## Title 1(must be text)
example code for Title 1

\\## Title 2
example code for Title 2

\\## Summary:
Provide a brief summary of the Java architecture derived from the Cypher query. 
""")


# 역할: 테이블과 2단계 깊이의 노드들, 사용자의 요구사항, 이전 히스토리를 받아서, 간단한 자바 코드로 전환하는 함수
# 매개변수 : 
#   - twoDeths_nodes : 테이블과 2단계 깊이 기준 연결된 노드 목록
#   - chatHistory : 채팅(요구사항), 
#   - chat : 이전 히스토리
# 반환값: 
#   - 스트림 : 자바 코드
async def convert_2deths_java(twoDeths_nodes, chatHistory, chat):
    try:
        logging.info(f"\n Start conversion to Java \n")
        
        chain = (
            RunnablePassthrough()
            | prompt
            | llm
            | StrOutputParser()
        )

        async for chunk in chain.astream({"code": twoDeths_nodes, "chatHistory": chatHistory, "chat": chat}):
            print(chunk, end=" ", flush=True)
            yield chunk
        
        logging.info("\n All stream data delivered \n")
    except Exception:
        err_msg = "LLM으로 부터 스트림으로 자바 코드를 받는 도중 문제가 발생했습니다"
        logging.exception(err_msg)
        raise LLMCallError(err_msg)