FROM python:3.11-slim

WORKDIR /app

RUN pip install --no-cache-dir fastapi uvicorn polars numpy

COPY main.py datos.py queries.py ./

CMD ["uvicorn", "main:app", "--host", "0.0.0.0", "--port", "8002"]
