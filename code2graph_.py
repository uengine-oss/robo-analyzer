from langchain_community.document_loaders.generic import GenericLoader
from langchain_community.document_loaders.parsers import LanguageParser
from langchain_text_splitters import Language
from cypher_to_context import convert_cypher_to_context

repo_path = "/Users/uengine/Documents/modernizer"


loader = GenericLoader.from_filesystem(
    repo_path,
    glob="**/*",
    suffixes=[".txt"],
)
documents = loader.load()
len(documents)

from langchain_text_splitters import CharacterTextSplitter
from langchain_text_splitters import RecursiveCharacterTextSplitter


# text_splitter = CharacterTextSplitter.from_tiktoken_encoder(
#     chunk_size=100, chunk_overlap=0
# )
# split_docs = text_splitter.split_documents(documents)

text_splitter = RecursiveCharacterTextSplitter(chunk_size=300, chunk_overlap=0)
split_docs = text_splitter.split_documents(documents)

print(len(split_docs))


#캐시 적용
#from langchain.cache import InMemoryCache
from langchain.globals import set_llm_cache

# set_llm_cache(InMemoryCache())
from langchain.cache import SQLiteCache

set_llm_cache(SQLiteCache(database_path=".langchain.db"))



from langchain.chains import ConversationalRetrievalChain
from langchain.memory import ConversationSummaryMemory
from langchain_openai import ChatOpenAI
from langchain_core.output_parsers import StrOutputParser
from langchain.prompts import PromptTemplate
from langchain.schema.runnable import RunnablePassthrough

get_context_prompt = PromptTemplate.from_template(
    """
코드에 포함된 프로시져 각 구문들의 Opening 과 Closing을 분석할거야

구문 유형:
- IF, ELSE-IF, ELSE, FOR, SELECT, INSERT, UPDATE, DELETE, CALL


그리고 이렇게 완료한 결과를 바탕으로 POP이 미결된 STATEMENT 들을 스택처럼 쌓아서 트리구조로 다음과 같이 표현해:

<완료되지 않은 STATEMENT1> LineNumber: <LineNumber>
  <완료되지 않은 STATEMEN2> LineNumber: <LineNumber>
    <완료되지 않은 STATEMENT3> LineNumber: <LineNumber>

이때 정확히 코드상에 매칭되는 완료 구문이 보일때만 POP을 할 수 있어.

예시입력:

1: CREATE OR REPLACE PROCEDURE your_procedure_name
2: IS
3: BEGIN
4:   IF (condition_1) THEN
5:     SELECT column_name INTO variable_name FROM table_name WHERE condition;
6:     
7:     IF (condition_2) THEN
8:       SELECT column_name INTO variable_name FROM table_name WHERE condition;
9:       
10:       IF (condition_3) THEN
11:         SELECT column_name INTO variable_name FROM table_name WHERE condition;
12:       END IF

결과:

PROCEDURE 1
  IF 4
    IF 7

*** IF 10은 END IF가 존재하므로 스택에서 제외됨 ***


예시입력2:

1: CREATE OR REPLACE PROCEDURE your_procedure_name
2: IS
3: BEGIN
4:   IF (condition_1) THEN
5:     SELECT column_name INTO variable_name FROM table_name WHERE condition;
6:     
7:     IF (condition_2) THEN
8:       SELECT column_name INTO variable_name FROM table_name WHERE condition;

결과:

PROCEDURE 1
  IF 4
    IF 7

*** IF 10은 END IF가 존재하므로 스택에서 제외됨 ***
    
    
자 아래의 코드에 대하여 결과만을 리턴해줘. 어떤 설명도 포함하지 말고 결과만 출력해.

코드내용:

{code}


    """)


prompt = PromptTemplate.from_template(
    """
프로시져 각 구문들을 다음과 같이 Graph DB에 담아낼 수 있도록 할거야:

Entity Types:
- IF, ELSE-IF, ELSE, FOR, SELECT, INSERT, UPDATE, DELETE, CALL

Attributes or Relations:
- 공통 attribute:
1. id: 해당 statement 를 구분할 수 있는 유일한 id. id 는 해당 STATEMENT의 선언부가 시작되는 곳의 라인넘버를 사용하여 선언.
2. name: 적당한 구문에 대한 한국어 설명 (짧게)
3. source: "실제 소스코드 내용 부분 발췌. 하위가 있는경우는 자신의 body 부분에 ...child code... 로만 표시"
4. closed: 주어진 코드상에서 해당 statement에 매치되는 ENDING 선언이 e.g. END IF, END LOOP 등이 "확실히" 확인되었다면 true, Open된 상태로 있다면 false
5. endLine: "해당 구문의 마지막 라인수. 주어진 코드에 해당 statement의 endLine이 발견되지 않은 경우는 -1을 대입"
 
- IF, ELSE-IF, FOR 인 경우:  attribute 로 condition: 만족조건, relation 으로 PARENT-OF: 하위 statement
e.g. 

CREATE (if1:IF {{id: "해당IF의 시작 라인넘버", name: "짧은 설명", condition: "어떤 조건이면"}})
CREATE (call1:CALL {{id: "해당CALL의 시작 라인넘버"}})
CREATE (if1)-[:PARENT-OF]-(call1) 

만약, 주어진 코드상에서 상위구문을 찾을 수 없이 하위구문이 발견되는 경우, 아래의 상위구문에 대한 정보를 기반으로 설정:

MATCH (parent1:상위구문유형 {{id: "상위구문의 id"}})
CREATE (call1:CALL {{id: "해당CALL의 시작 라인넘버"}})
CREATE (parent1)-[:PARENT-OF]-(call1) 

현재의 상위구문 정보:

{context}

- SELECT 인경우: relation 으로 FROM -> 소스 테이블에 대한  연결relation


 e.g. 

MATCH(stmt: SELECT {{id: 'SELECT의 라인넘버'}}, (table:Table {{id: 'EMPLOYEE'}}) CREATE (stmt)-[:FROM]->(table);

- INSERT, UPDATE, DELETE인 경우: 
relation 으로 WRITES -> 타겟 테이블에 대한  연결relation
 
e.g. 

MATCH(stmt: INSERT {{id: 'INSERT의 라인넘버'}}, (table:Table {{id: 'EMPLOYEE'}}) CREATE (stmt)-[:WRITES]->(table);

** SELECT, UPDATE, INSERT인 경우, 그  속에 포함된 복합 쿼리에 대하여 parent-of 릴레이션을 이용하여 모든 SELECT 등의 Statement 들은 독립적인 Graph QL Entity 가 될 수 있도록 해야함

결과는 어떤 설명도 포함하지 말고 cypher query만 출력해.

----

코드내용:

{code}


    """)

llm = ChatOpenAI(model_name="gpt-4")
gpt3_5 = ChatOpenAI(model_name="gpt-3.5-turbo")


contexts=[
    """
     No Parent Context
    """,
    """
    PROCEDURE{id:1}
        IF{id:4}
            IF{id:7}
    """,
    """
    PROCEDURE{id:1}
        IF{id:4}
            IF{id:7}
    """,
]
i=0

for doc in split_docs[:3]:  # Process only the first 5 documents
    print(f"\ndoc: \n{doc.page_content}\n")

    chain = (
        RunnablePassthrough()
        | get_context_prompt
        | gpt3_5
        | StrOutputParser()
    )
    print("\n-----context-----\n")
    result = (chain.invoke({"code": doc}))
    print(result)

    chain = (
        RunnablePassthrough()
        | prompt
        | llm
        | StrOutputParser()
    )
    print("\n-----result-----\n")
    result = (chain.invoke({"code": doc, "context": contexts[i]}))

    context = convert_cypher_to_context(result)

    print(result)
    i=i+1


