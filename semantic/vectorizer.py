from sentence_transformers import SentenceTransformer

model = SentenceTransformer('sentence-transformers/all-MiniLM-L6-v2')

# 'all-MiniLM-L6-v2' 모델은:
# - 384 차원의 벡터 생성
# - 다국어 지원
# - 문장의 의미를 잘 포착
def vectorize_text(text):
    vector = model.encode(text)  # text -> 384차원 벡터로 변환
    return vector  # shape: (384,)