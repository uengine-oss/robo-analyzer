from langchain_community.document_loaders.generic import GenericLoader
from langchain_community.document_loaders.parsers import LanguageParser
from langchain_openai import ChatOpenAI
from langchain_core.output_parsers import StrOutputParser
from langchain.prompts import PromptTemplate
from langchain.schema.runnable import RunnablePassthrough
from langchain_text_splitters import RecursiveCharacterTextSplitter


repo_path = "/Users/uengine/Documents/modernizer"


loader = GenericLoader.from_filesystem(
    repo_path,
    glob="**/*",
    suffixes=[".txt"],
)
documents = loader.load()
len(documents)

text_splitter = RecursiveCharacterTextSplitter(chunk_size=10000, chunk_overlap=0)
split_docs = text_splitter.split_documents(documents)

print(len(split_docs))


prompt = PromptTemplate.from_template(
"""
프로시져내에서 추정되는 사용테이블들과 관계들을 다음과 같은 cypher query로 

CREATE (테이블명1:Table {{name:'테이블1한글명', id: '테이블명1', field명1: "field설명1", ..}})
CREATE (테이블명2:Table {{name:'테이블2한글명', id: '테이블명2', field명1: "field설명1", ..}})
CREATE (테이블명1)-[:REFERENCES{{referencedBy:"foreignKey field명"}}]->(테이블명2)

프로시져 코드:

{code}

"""
)


llm = ChatOpenAI(model_name="gpt-4")
gpt3_5 = ChatOpenAI(model_name="gpt-3.5-turbo")



for doc in split_docs:  # Process only the first 5 documents
    print(f"\n----- doc: ------\n{doc.page_content}\n")

    chain = (
        RunnablePassthrough()
        | prompt
        | llm
        | StrOutputParser()
    )

    result = (chain.invoke({"code": doc.page_content}))
    print(result)

