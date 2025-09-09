import json
import logging
import os
from langchain.globals import set_llm_cache
from langchain_community.cache import SQLiteCache
from util.llm_client import get_llm
from langchain_core.output_parsers import JsonOutputParser
from langchain.prompts import PromptTemplate
from langchain.schema.runnable import RunnablePassthrough
from util.exception import LLMCallError
import openai

db_path = os.path.join(os.path.dirname(__file__), 'langchain.db')
set_llm_cache(SQLiteCache(database_path=db_path))


prompt = PromptTemplate.from_template(
"""
You are an expert at analyzing and summarizing the behavior of PL/SQL procedures and functions.
Based on the provided code analysis summaries, please clearly and concisely describe the core functionality of the overall procedure/function.


User language setting: {locale}. Reflect this when generating the result.


Analyzed summary content:
{summaries}

[Analysis Rules]
===============================================
1. Identify Core Functionality
   - The main tasks performed by the procedure/function
   - The flow of inputs and outputs
   - Important business logic

2. Summarization Style
   - Provide a detailed summary of at least 3–4 sentences
   - Minimize technical jargon
   - Explain in terms that are easy to understand from a business perspective
   Example) This procedure updates employee HR information,
         processes department transfers, title changes, and salary adjustments based on the provided employee ID,
         records the changes automatically in the HR history table,
         and sends email notifications to the relevant department head and HR team."

   Example) This procedure performs monthly payroll processing,
         aggregates attendance records and allowances for the month to calculate the net payable amount,
         generates a payroll statement for each employee,
         and submits a batch transfer request to the designated bank accounts."
  

[JSON Output Format]
===============================================
You must follow these requirements.
1) Return exactly one valid JSON object as the entire output.
2) Output the JSON only, with no additional text.
Format:
```json
{{
    "summary": "A concise explanation summarizing the flow of the procedure/function"
}}
```
"""
)

def understand_summary(summaries, api_key, locale):
    try:
        # 입력을 안전한 JSON 문자열로 직렬화하여 게이트웨이 파서 오류를 예방
        summaries_str = json.dumps(summaries, ensure_ascii=False)

        llm = get_llm(max_tokens=8192, api_key=api_key)

        chain = (
            RunnablePassthrough()
            | prompt
            | llm
            | JsonOutputParser()
        )
        result = chain.invoke({"summaries": summaries_str, "locale": locale})
        return result
    except Exception as e:
        err_msg = f"Understanding 과정에서 요약 관련 LLM 호출하는 도중 오류가 발생했습니다: {str(e)}"
        logging.error(err_msg)
        raise LLMCallError(err_msg)