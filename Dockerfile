FROM python:3.12.0

WORKDIR /app

COPY . /app

RUN pip install -r requirements.txt

EXPOSE 5502

CMD ["python", "main.py"]