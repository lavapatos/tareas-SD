FROM python:3.11-slim

WORKDIR /app

RUN pip install --no-cache-dir kafka-python

COPY reintentos.py ./

CMD ["python", "reintentos.py"]
