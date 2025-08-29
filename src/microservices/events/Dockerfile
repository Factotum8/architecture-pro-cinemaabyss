FROM python:3.10

WORKDIR /app
COPY . /app

RUN pip install fastapi uvicorn kafka-python

CMD ["uvicorn", "app:app", "--reload", "--host", "0.0.0.0", "--port", "8082"]