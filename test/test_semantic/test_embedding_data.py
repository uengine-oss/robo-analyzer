import unittest
import logging
import sys
import os
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..', '..')))
from transformers import RobertaTokenizer, RobertaModel
import torch

# 로그 설정
logging.basicConfig(
    level=logging.INFO,
    format='%(levelname)s: %(message)s',
    force=True
)

noisy_loggers = [
    'asyncio', 
    'anthropic', 
    'langchain',
    'urllib3',
    'anthropic._base_client',
    'anthropic._client',
    'langchain_core',
    'langchain_anthropic',
    'uvicorn',
    'fastapi'
]

for logger_name in noisy_loggers:
    logging.getLogger(logger_name).setLevel(logging.CRITICAL)

# 키워드를 통해서 유사성을 계산하는 테스트 모듈 
class TestEmbeddingData(unittest.IsolatedAsyncioTestCase):

    @classmethod
    def setUpClass(cls):
        cls.tokenizer = RobertaTokenizer.from_pretrained('microsoft/codebert-base')
        cls.model = RobertaModel.from_pretrained('microsoft/codebert-base')

    def calculate_cosine_similarity(self, vector1, vector2):
        cos = torch.nn.CosineSimilarity(dim=1, eps=1e-6)
        return cos(vector1, vector2)

    async def test_cosine_similarity(self):
        try:
            keyword = "overtime_hours"
            included_code = "This code checks if the overtime_hours variable is null and, if so, sets it to 0. The check and assignment are repeated twice within nested IF statements."

            inputs_keyword = self.tokenizer(keyword, return_tensors="pt")
            outputs_keyword = self.model(**inputs_keyword)
            vector_keyword = outputs_keyword.last_hidden_state[:, 0, :]

            inputs_included_code = self.tokenizer(included_code, return_tensors="pt")
            outputs_included_code = self.model(**inputs_included_code)
            vector_included_code = outputs_included_code.last_hidden_state[:, 0, :]

            similarity = self.calculate_cosine_similarity(vector_keyword, vector_included_code)
            logging.info(f"코사인 유사도: {similarity.item()}")
            
            self.assertTrue(similarity.item() > 0, "유사도가 0보다 커야 합니다")
            
        except Exception as e:
            self.fail(f"임베딩 유사도 계산 중 예외 발생: {str(e)}")

if __name__ == '__main__':
    unittest.main()