FROM python:3.11-slim

WORKDIR /app
ENV PYTHONBUFFERED=1
RUN pip install --no-cache-dir kafka-python

COPY reintentos.py ./

CMD ["python", "reintentos.py"]
