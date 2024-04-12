from langchain_community.document_loaders.generic import GenericLoader
from langchain_community.document_loaders.parsers import LanguageParser
from langchain_text_splitters import Language
from cypher_to_context import convert_cypher_to_context
from process_stack_commands import process_stack_commands

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

#chunk_size = 300 is for testing with small code, 10000 is for production
text_splitter = RecursiveCharacterTextSplitter(chunk_size=10000, chunk_overlap=0)
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

구문 유형: PROCEDURE, IF, FOR, DECLARE, SELECT, INSERT, UPDATE, DELETE, CALL
** 이 구문 외의 BEGIN 등은 상위구문에 포함된 요소이기 때문에 구문으로 식별하면 안됨.


- PROCEDURE인 경우: BEGIN~END까지 포함하여 한 구문임.
- FOR인 경우:  FOR var IN (SELECT..) LOOP ~ END LOOP 까지가 한 구문임.

각 구문이 시작되면
PUSH <STATEMENT TYPE> LineNumber: <LineNumber>

이때 정확히 코드상에 매칭되는 완료 구문이 보일때만 POP을 할 수 있어.


이렇게 출력해줘. 


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


결과:

PUSH PROCEDURE 1
PUSH IF 4
PUSH SELECT 5
POP SELECT 5
PUSH IF 7
PUSH SELECT 8
POP SELECT 8
PUSH IF 10
PUSH SELECT 11
POP SELECT 11

예시입력2:

16:         UPDATE column_name SET column_value = 'new_value' WHERE condition;
17:       END IF;
18:       UPDATE column_name SET column_value = 'new_value' WHERE condition;
19:     END IF;
20:     UPDATE column_name SET column_value = 'new_value' WHERE condition;
21:   END IF;

기대값:

PUSH UPDATE 16
POP UPDATE 16
POP IF 17
PUSH UPDATE 18
POP UPDATE 18
POP IF 19
PUSH UPDATE 20
POP UPDATE 20
POP IF 21

SELF CRITICISM:

1. PUSH할때: 구문 시작절이 "정확히" 보이지 않으면 현재 스택 컨텍스트에서 이어진 구문일 수 있으니  현재 스택 컨텍스트를 참고하여 PUSH 하지 말것. 예1: DECLARE 절이 보이지 않는 선언부는 PUSH 생략. 예2: FROM work_log WHERE employee_id = rec.employee_id 로 시작하면 SELECT 절로 식별할 수 있으나 정확히 SELECT가 포함되지 않았으므로 PUSH하지 말것.

2. POP할때 : 현재 스택 컨텍스트가 아래와 같으니, 이걸 기반으로 이전 상황이 보이지 않아도 구문이 닫아져야 하는 라인이 보이면 POP. 
이때 정확히 코드상에 매칭되는 완료 구문이 보일때만 POP을 할 수 있어.

현재 스택 컨텍스트:

{stack}

자 아래의 코드에 대하여 결과만을 리턴해줘. 어떤 설명도 포함하지 말고 결과만 출력해.

코드내용:

{code}


    """)


prompt = PromptTemplate.from_template(
    """
프로시져 각 구문들을 다음과 같이 Graph DB에 담아낼 수 있도록 할거야:

Entity Types:
- PROCEDURE, IF, FOR, DECLARE, SELECT, INSERT, UPDATE, DELETE, CALL
* Table은 생성하면 안됨

Attributes or Relations:
- 공통 attribute:
1. id: 해당 statement 를 구분할 수 있는 유일한 id. id 는 해당 STATEMENT의 선언부가 시작되는 곳의 라인넘버를 사용하여 선언.
2. name: 적당한 구문에 대한 한국어 설명 (짧게)
3. source: "실제 소스코드 내용 부분 발췌. 하위가 있는경우는 자신의 body 부분에 ...child code... 로만 표시"
4. closed: 주어진 코드상에서 해당 statement에 매치되는 ENDING 선언이 e.g. END IF, END LOOP 등이 "확실히" 확인되었다면 true, Open된 상태로 있다면 false
5. endLine: "해당 구문의 마지막 라인수. 주어진 코드에 해당 statement의 endLine이 발견되지 않은 경우는 -1을 대입"
 
- PROCEDURE인 경우: BEGIN~END까지 포함하여 한 구문임.
- FOR인 경우:  FOR var IN (SELECT..) LOOP ~ END LOOP 까지가 한 구문임.
- IF, ELSE-IF, FOR 인 경우:  attribute 로 condition: 만족조건, relation 으로 PARENT-OF: 하위 statement
e.g. 

CREATE (if1:IF {{id: <해당IF의 시작 라인넘버>, name: "짧은 설명", condition: "어떤 조건이면"}})
CREATE (call1:CALL {{id: <해당CALL의 시작 라인넘버>}})
CREATE (if1)-[:PARENTOF]-(call1) 

만약, 주어진 코드상에서 상위구문을 찾을 수 없이 하위구문이 발견되는 경우, 아래의 상위구문에 대한 정보를 기반으로 설정:

MATCH (parent1:상위구문유형 {{id: <상위구문의 id>}})
CREATE (call1:CALL {{id: <해당CALL의 시작 라인넘버>}})
CREATE (parent1)-[:PARENTOF]-(call1);

이때, 하나의 MATCH~CREATE 문들은 끝에 ;으로 분할 되어야 하고 MATCH가 항상 맨앞에 나와야 해.

모든 노드는 자신의 상위노드를 가져야 하니, 상위노드 정보가 No Parent Context가 아닌이상은 상위노드 정보를 기반으로 PARENT-OF를 설정해줘.

- DECLARE 인 경우: DECLARE~BEGIN~END 구문으로 하나의 구문으로 식별

- SELECT 인경우: relation 으로 FROM -> 소스 테이블에 대한  연결 relation 을 꼭 만들어줘:

 e.g. 

MATCH(stmt: SELECT {{id: <해당 SELECT의 id>}}, (table:Table {{id: 'EMPLOYEE'}}) CREATE (stmt)-[:FROM]->(table);

- INSERT, UPDATE, DELETE인 경우: 
relation 으로 WRITES -> 타겟 테이블에 대한  연결 relation을 꼭 만들어줘:
 
e.g. 

MATCH(stmt: INSERT {{id: <해당 INSERT의 id>}}, (table:Table {{id: 'EMPLOYEE'}}) CREATE (stmt)-[:WRITES]->(table);

** SELECT, UPDATE, INSERT인 경우, 그  속에 포함된 복합 쿼리에 대하여 parent-of 릴레이션을 이용하여 모든 SELECT 등의 Statement 들은 독립적인 Graph QL Entity 가 될 수 있도록 해야함


결과는 어떤 설명도 포함하지 말고 cypher query만 출력해.

----

현재의 상위구문 정보:

{stack}

코드내용:

{code}


    """)




sibling_order_prompt = PromptTemplate.from_template(
    """
아래 cypher query에서 등장하는 구문들의 PARENT가 같은 경우, 아래와 같은 해당 sibling 들의 순서를 추가하는 Relation을 생성하는 cypher query를 생성해줘:

예시)
MATCH (prev: SELECT {{id: 11}}), (next: IF {{id:17}}) CREATE (prev)-[NEXT]->(next);


아래 쿼리에 대하여 위의 예시처럼 결과를 출력해줘.

{query}



    """)

llm = ChatOpenAI(model_name="gpt-4")
gpt3_5 = ChatOpenAI(model_name="gpt-3.5-turbo")


sample_contexts=[
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

stack = []
stack_context = "No Parent Context"

def make_cypher_query_executable(cypher_query):
    # Splitting the input into lines for processing
    lines = cypher_query.split('\n')

    # Separating CREATE and MATCH statements
    create_statements = [line for line in lines if line.startswith('CREATE')]
    match_statements = [line for line in lines if line.startswith('MATCH')]

    # Removing the trailing semicolon from CREATE statements and adding a single semicolon at the end
    create_statements = [line.rstrip(';') for line in create_statements] + [';']

    # Reassembling the query with CREATE statements followed by MATCH statements
    reformatted_query = '\n'.join(create_statements + match_statements)

    # Reformatting the MATCH statements to include references to CREATE variables if necessary
    enhanced_match_statements = []
    for match in match_statements:
        
        # Extracting the variable used in the CREATE statement that follows the MATCH
        create_reference = match.split('CREATE ')[1].split('-')[0].strip()
        
        create_var_id = create_reference.strip().split(' ')[0].strip('()')
        # Finding the corresponding CREATE statement to extract the id
        for create in create_statements:
            if create_var_id in create:
                # Extracting the id value from the CREATE statement
                id_value = create.split('{id: ')[1].split(',')[0].strip()
                type_part = create.split('(')[1].split('{')[0].split(':')[1].strip()
                # Adding the reference to the MATCH statement
                match = match.replace('MATCH (', f'MATCH ({create_var_id}:{type_part} {{id: {id_value}}}), (')
                break

        enhanced_match_statements.append(match)

    # Reassembling the query with enhanced MATCH statements
    reformatted_query_with_enhanced_match = '\n'.join(create_statements + enhanced_match_statements)


    return (reformatted_query_with_enhanced_match)

for doc in split_docs:  # Process only the first 5 documents
    print(f"\n----- doc: ------\n{doc.page_content}\n")

    

    chain = (
        RunnablePassthrough()
        | prompt
        | llm
        | StrOutputParser()
        | make_cypher_query_executable

    )
    print("\n- result\n")
    result = (chain.invoke({"code": doc.page_content, "stack": stack_context}))

    print(result)
    
    with open("output_cypher_queries.txt", "a") as file:
        file.write(result + "\n\n")


    chain = (
        RunnablePassthrough()
        | sibling_order_prompt
        | gpt3_5
        | StrOutputParser()
    )
    print("\n- result\n")
    result = (chain.invoke({"query": result}))

    print(result)



    i=i+1

    chain = (
        RunnablePassthrough()
        | get_context_prompt
        | llm
        | StrOutputParser()
    )
    print("\n- context\n")
    result = (chain.invoke({"code": doc.page_content, "stack": stack_context}))
    print(result)

    stack_context = process_stack_commands(stack, result)
    print(f"summary: \n{stack_context}")








"""

이걸루 대체:





----



프로시져 각 구문들을 다음과 같이 Graph DB에 담아낼 수 있도록 할거야:

Entity Types:
- PROCEDURE, IF, FOR, DECLARE, SELECT, INSERT, UPDATE, DELETE, CALL

Attributes or Relations:
- 공통 attribute:
1. id: 해당 statement 를 구분할 수 있는 유일한 id. id 는 해당 STATEMENT의 선언부가 시작되는 곳의 라인넘버를 사용하여 선언.
2. name: 적당한 구문에 대한 한국어 설명 (짧게)
3. source: "실제코드의 핵심 로직 부위만 발췌하여 요약"
4. closed: 주어진 코드상에서 해당 statement에 매치되는 ENDING 선언이 e.g. END IF, END LOOP 등이 "확실히" 확인되었다면 true, Open된 상태로 있다면 false
5. endLine: "해당 구문의 마지막 라인수. 주어진 코드에 해당 statement의 endLine이 발견되지 않은 경우는 -1을 대입"
 
- PROCEDURE인 경우: BEGIN~END까지 포함하여 한 구문임.
- IF, ELSE-IF, FOR 인 경우:  attribute 로 condition: 만족조건, relation 으로 PARENT-OF: 하위 statement
e.g. 

CREATE (if1:IF {id: "해당IF의 시작 라인넘버", name: "짧은 설명", condition: "어떤 조건이면"})
CREATE (call1:CALL {id: "해당CALL의 시작 라인넘버"})
CREATE (if1)-[:PARENTOF]-(call1) 

만약, 주어진 코드상에서 상위구문을 찾을 수 없이 하위구문이 발견되는 경우, 아래의 상위구문에 대한 정보를 기반으로 설정:

MATCH (parent1:상위구문유형 {id: "상위구문의 id"})
CREATE (call1:CALL {id: "해당CALL의 시작 라인넘버"})
CREATE (parent1)-[:PARENTOF]-(call1);

이때, 하나의 MATCH~CREATE 문들은 끝에 ;으로 분할 되어야 하고 MATCH가 항상 맨앞에 나와야 해.

모든 노드는 자신의 상위노드를 가져야 하니, 상위노드 정보가 No Parent Context가 아닌이상은 상위노드 정보를 기반으로 PARENT-OF를 설정해줘.

- DECLARE 인 경우: DECLARE~BEGIN~END 구문으로 하나의 구문으로 식별

- SELECT 인경우: relation 으로 FROM -> 소스 테이블에 대한  연결relation


 e.g. 

MATCH(stmt: SELECT {id: 'SELECT의 라인넘버'}, (table:Table {id: 'EMPLOYEE'}) CREATE (stmt)-[:FROM]->(table);

- INSERT, UPDATE, DELETE인 경우: 
relation 으로 WRITES -> 타겟 테이블에 대한  연결relation
 
e.g. 

MATCH(stmt: INSERT {id: 'INSERT의 라인넘버'}, (table:Table {id: 'EMPLOYEE'}) CREATE (stmt)-[:WRITES]->(table);

** SELECT, UPDATE, INSERT인 경우, 그  속에 포함된 복합 쿼리에 대하여 parent-of 릴레이션을 이용하여 모든 SELECT 등의 Statement 들은 독립적인 Graph QL Entity 가 될 수 있도록 해야함

결과는 어떤 설명도 포함하지 말고 cypher query만 출력해.

----

현재의 상위구문 정보:

No Parent Context

코드내용:

1: CREATE OR REPLACE PROCEDURE calculate_payroll AS 
2: BEGIN
3:     FOR rec IN (SELECT e.employee_id, e.base_salary, e.employee_type, e.contract_tax_rate
4:                 FROM employees e) LOOP
5:         -- 야근 수당 계산
6:         DECLARE
7:             overtime_hours NUMBER;
8:             overtime_rate NUMBER := 1.5;  -- 야근 수당 비율
9:             overtime_pay NUMBER;
10:         BEGIN
11:             SELECT SUM(over_hours)
12:             INTO overtime_hours
13:             FROM work_logs
14:             WHERE employee_id = rec.employee_id
15:               AND work_date BETWEEN trunc(sysdate, 'MM') AND last_day(sysdate);  -- 현재 월에 해당하는 기록만 선택
16:             
17:             IF overtime_hours IS NULL THEN
18:                 overtime_hours := 0;
19:             END IF;
20:             
21:             overtime_pay := overtime_hours * (rec.base_salary / 160) * overtime_rate;  -- 160시간 기준
22:         END;
23:         
24:         -- 무급 휴가 공제 계산
25:         DECLARE
26:             unpaid_leave_days NUMBER;
27:             unpaid_deduction NUMBER;
28:         BEGIN
29:             SELECT SUM(leave_days)
30:             INTO unpaid_leave_days
31:             FROM leave_records
32:             WHERE employee_id = rec.employee_id
33:               AND leave_type = 'Unpaid'
34:               AND leave_date BETWEEN trunc(sysdate, 'MM') AND last_day(sysdate);  -- 현재 월에 해당하는 기록만 선택
35:             
36:             IF unpaid_leave_days IS NULL THEN
37:                 unpaid_leave_days := 0;
38:             END IF;
39:             
40:             unpaid_deduction := (rec.base_salary / 20) * unpaid_leave_days;  -- 월 기준 20일로 계산
41:         END;
42:         
43:         -- 세금 공제 계산
44:         DECLARE
45:             tax_rate NUMBER := 0.1;  -- 기본 세금 비율 10%
46:             contract_tax_rate NUMBER;
47:             tax_deduction NUMBER;
48:         BEGIN
49:             -- 계약직인 경우 세금율을 employees 테이블에서 가져온 값으로 설정
50:             IF rec.employee_type = 'Contract' THEN
51:                 contract_tax_rate := rec.contract_tax_rate;
52:             ELSE
53:                 contract_tax_rate := tax_rate;  -- 정규직인 경우 기본 세금율 사용
54:             END IF;
55:             
56:             tax_deduction := (rec.base_salary + overtime_pay - unpaid_deduction) * contract_tax_rate;
57:         END;
58:         
59:         -- 최종 급여 업데이트
60:         UPDATE employees
61:         SET final_salary = rec.base_salary + overtime_pay - unpaid_deduction - tax_deduction
62:         WHERE employee_id = rec.employee_id;
63:     END LOOP;
64:     
65:     COMMIT;
66: END;
67: /



---


그리고 관계된 문제들:

1. 때때로 라인넘버를 문자열처럼 감싸는 ("") 경우도 있고 그냥 숫자로 표현하는 경우도 있고
2. MATCH를 중간에 넣어서 안되는 경우, CREATE의 끝에 ;를 넣어서 참조가 안되는경우
3. 테이블명을 잘못참조하는 경우, EMPLOYEE 등

--> 잘못되는 패턴에 따라 나타난 오류에 대한 적절한 쿼리수정을 자동으로 시켜야 함.

    
"""