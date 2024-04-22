#캐시 적용
#from langchain.cache import InMemoryCache
from langchain.globals import set_llm_cache

# set_llm_cache(InMemoryCache())
from langchain.cache import SQLiteCache

set_llm_cache(SQLiteCache(database_path=".langchain.db"))



from langchain_openai import ChatOpenAI
from langchain_core.output_parsers import JsonOutputParser
from langchain.prompts import PromptTemplate
from langchain.schema.runnable import RunnablePassthrough

prompt = PromptTemplate.from_template(
    """
You are an expert in Oracle PLSQL. Analyze the following Stored Procedure.

Code:
{code}

For the content of the code from line {startLine} to {endLine} "only", extract the following information:
1. A description of this part of the code.
2. The type of statement this part of the code is. Identify the statement type for the specified line range, not the entire code.
3. If there are any CRUD operations on tables or variables in this code area, mention which values are being referenced.

Summarize/analyze in the following json format:

{{"summary": "summerization of the code",
 "statementType": "PROCEDURE" | "DECLARE" | "IF" | "FOR" | "SELECT" | "UPDATE" | "DELETE" | "ASSIGN" | "COMMIT",
 "selectTables": ["table1", "table2"],
 "updateTables": ["table3", "table4"]    
}}
```
    """)



llm = ChatOpenAI(model_name="gpt-4")
gpt3_5 = ChatOpenAI(model_name="gpt-3.5-turbo")

from langchain_community.llms import Ollama
llama3 = Ollama(model="llama3:8b")

def understand_code(code, context):

    chain = (
        RunnablePassthrough()
        | prompt
        | gpt3_5
        | JsonOutputParser()

    )
    result = (chain.invoke({"code": code, "startLine": context["startLine"], "endLine": context["endLine"]}))

    print("입력코드:", code)

    return (result)
    



