from langchain_community.document_loaders.generic import GenericLoader
from langchain_community.document_loaders.parsers import LanguageParser
from langchain_text_splitters import Language

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


prompt = PromptTemplate.from_template(
    """
넌 Oracle PLSQL전문가야.
아래의 프로시져 내용 전반을 요약하려고 해.

Statement Types:
- PROCEDURE, IF-ELSE-IF, FOR, SELECT, INSERT, UPDATE, DELETE, CALL

위 구문들에 대하여 내용이 시작되는 라인과 끝 라인만을 남기고 나머지 내용부분을 제거하는 일을 할거야.
주석도 제외하고 위에 언급된 Statement 들이 아니어도 모두 제외시켜

Example:

<input>

CREATE OR REPLACE PACKAGE BODY BOKERP."PG_PAY_PAY" IS

   PROCEDURE pr_pay_rmcl_work_proc (psv_proc_by       IN     VARCHAR2,
                                    psv_rmcl_yymm     IN     VARCHAR2,
                                    psv_rmcl_obj_cd   IN     VARCHAR2,
                                    psv_rmcl_no       IN     VARCHAR2,
                                    psv_work_gb_cd    IN     VARCHAR2,
                                                                     )
   IS
      lsv_pay_objr_01_cnt          NUMBER;
      lsv_set_emp                  VARCHAR2 (7);
      lsv_rta_cnt                  NUMBER;
   BEGIN
      SELECT rmcl_wrk_no,
             rmcl_yymm,
             rmcl_obj_cd,
        INTO lsv_rmcl_wrk_no,
             lsv_rmcl_yymm,
             lsv_rmcl_obj_cd,
        FROM tb_pay_rmclwrk
       WHERE rmcl_yymm = psv_rmcl_yymm
         AND rmcl_obj_cd = psv_rmcl_obj_cd
         AND rmcl_no = psv_rmcl_no
         AND ((psv_rmcl_obj_cd = '40' AND rmcl_orgnz_cd = psv_orgnz_cd) OR (psv_rmcl_obj_cd != '40'));

<output>


CREATE OR REPLACE PACKAGE BODY BOKERP."PG_PAY_PAY" IS
   PROCEDURE pr_pay_rmcl_work_proc (psv_proc_by       IN     VARCHAR2,
      SELECT rmcl_wrk_no,
         AND ((psv_rmcl_obj_cd = '40' AND rmcl_orgnz_cd = psv_orgnz_cd) OR (psv_rmcl_obj_cd != '40'));



아래의 코드 내용을 위의 예제 처럼 요약하고 설명없이 결과만 리턴해줘. 이때 각 라인의 맨앞에 포함된 라인넘버를 빠뜨리지 말고 있는 그대로 리턴해.:


{code}

    """)

llm = ChatOpenAI(model_name="gpt-4")


from langchain.callbacks import get_openai_callback

with get_openai_callback() as cb:
    summary = ""
    for doc in split_docs[:3]:  # Process only the first 5 documents
        chain = (
            RunnablePassthrough()
            | prompt
            | llm
            | StrOutputParser()
        )
        result = chain.invoke({"code": doc})
        summary = summary + result

        print(result)

    summary = summary.replace("\\n", "\n")

    print("==========================================")
    print(summary)
    print(f"Total Tokens: {cb.total_tokens}")
    print(f"Prompt Tokens: {cb.prompt_tokens}")
    print(f"Completion Tokens: {cb.completion_tokens}")
    print(f"Total Cost (USD): ${cb.total_cost}")



