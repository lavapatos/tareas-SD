import json
import time
import sys
import os
from kafka import KafkaConsumer, KafkaProducer
from kafka.errors import NoBrokersAvailable


kafka_servers = os.environ.get("KAFKA_BOOTSTRAP_SERVERS", "kafka:9092")
kafka_topic = os.environ.get("KAFKA_TOPIC", "consultas")


def conectar_consumer(servers, intentos_max=30):
    for i in range(intentos_max):
        try:
            consumer = KafkaConsumer(
                "reintentos",
                bootstrap_servers=servers,
                group_id="grupo_reintentos",
                value_deserializer=lambda m: json.loads(m.decode("utf-8")),
                auto_offset_reset="earliest",
                enable_auto_commit=True
            )
            return consumer
        except NoBrokersAvailable:
            time.sleep(3)
        except Exception:
            time.sleep(3)
    print("no se pudo conectar a kafka como consumer")
    sys.exit(1)


def conectar_producer(servers, intentos_max=30):
    for i in range(intentos_max):
        try:
            producer = KafkaProducer(
                bootstrap_servers=servers,
                value_serializer=lambda v: json.dumps(v).encode("utf-8")
            )
            return producer
        except NoBrokersAvailable:
            time.sleep(3)
        except Exception:
            time.sleep(3)
    print("no se pudo conectar a kafka como producer")
    sys.exit(1)


print(f"republicando a: {kafka_topic}")

consumer = conectar_consumer(kafka_servers)
producer = conectar_producer(kafka_servers)

print("conectado a kafka")

import threading

def procesar_y_republicar(consulta, prod, topic):
    retry_count = consulta.get("retry_count", 0)
    espera = 2 ** retry_count
    print(f"  [{consulta.get('id', '?')}] reintento {retry_count}, esperando {espera}s")
    time.sleep(espera)
    prod.send(topic, value=consulta)
    print(f"  [{consulta.get('id', '?')}] republicado a {topic}")

for mensaje in consumer:
    consulta = mensaje.value
    t = threading.Thread(target=procesar_y_republicar, args=(consulta, producer, kafka_topic))
    t.daemon = True
    t.start()
