FROM python:3.11-slim

WORKDIR /app

RUN pip install --no-cache-dir redis numpy kafka-python

COPY recolector.py analizador.py ver_lag.py ./

CMD ["python", "recolector.py"]
