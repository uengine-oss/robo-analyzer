import unittest
from transformers import RobertaTokenizer, RobertaModel
import torch

# 키워드를 통해서 유사성을 계산하는 테스트 모듈 
class TestEmbeddingData(unittest.TestCase):


    @classmethod
    # 모델과 토크나이저 초기화
    def setUpClass(cls):
        cls.tokenizer = RobertaTokenizer.from_pretrained('microsoft/codebert-base')
        cls.model = RobertaModel.from_pretrained('microsoft/codebert-base')


    # 코사인 유사성 계산하는 메서드
    def calculate_cosine_similarity(self, vector1, vector2):
        cos = torch.nn.CosineSimilarity(dim=1, eps=1e-6)
        return cos(vector1, vector2)

    
    # 입력된 텍스트를 토큰화하고, 이를 벡터로 변환하여 유사성을 계산합니다.
    def test_cosine_similarity(self):
        keyword = "overtime_hours"
        included_code = "This code checks if the overtime_hours variable is null and, if so, sets it to 0. The check and assignment are repeated twice within nested IF statements."

        inputs_keyword = self.tokenizer(keyword, return_tensors="pt")
        outputs_keyword = self.model(**inputs_keyword)
        vector_keyword = outputs_keyword.last_hidden_state[:, 0, :]  # 첫 번째 토큰의 벡터

        inputs_included_code = self.tokenizer(included_code, return_tensors="pt")
        outputs_included_code = self.model(**inputs_included_code)
        vector_included_code = outputs_included_code.last_hidden_state[:, 0, :]  # 첫 번째 토큰의 벡터

        similarity = self.calculate_cosine_similarity(vector_keyword, vector_included_code)
        print("Cosine Similarity with Included Code:", similarity.item())


if __name__ == '__main__':
    unittest.main()