import os
import sys
from pathlib import Path

import pytest
from langchain_core.prompts import PromptTemplate

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from client.llm_client import get_llm


template = """
너는 도움을 주는 시스템이야. 사용자의 질문/말을 보고 답을 해.

질문: {question}
"""


prompt = PromptTemplate(
    input_variables=["question"],
    template=template,
)


@pytest.mark.skipif(
    not os.getenv("LLM_API_KEY"),
    reason="LLM_API_KEY가 설정되지 않아 커스텀 LLM 호출을 건너뜁니다.",
)
def test_custom_llm_chain_invocation():
    api_key = os.environ["LLM_API_KEY"]
    llm = get_llm(api_key=api_key, is_custom_llm=True, company_name="")

    chain = prompt | llm
    response = chain.invoke({"question": "안녕하세요?"})

    assert isinstance(response, str) and response.strip()