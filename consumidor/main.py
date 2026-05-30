import json
import time
import sys
import os
import requests
from kafka import KafkaConsumer, KafkaProducer
from kafka.errors import NoBrokersAvailable
import redis


# --- configuracion ---

kafka_servers = os.environ.get("KAFKA_BOOTSTRAP_SERVERS", "kafka:9092")
kafka_topic = os.environ.get("KAFKA_TOPIC", "consultas")
kafka_group = os.environ.get("KAFKA_GROUP", "grupo_consumidores")
max_reintentos = int(os.environ.get("MAX_RETRIES", "3"))

# para saber cual consumidor proceso cada consulta
consumer_id = os.environ.get("CONSUMER_ID", "consumer-0")

# donde esta el cache
url_cache = os.environ.get("CACHE_URL", "http://cache_service:8001")

# redis para guardar metricas (misma instancia que usa el cache)
redis_host = os.environ.get("REDIS_HOST", "redis")
redis_port = int(os.environ.get("REDIS_PORT", "6379"))


def conectar_redis(host, port, intentos_max=30):
    # esperar a que redis este listo
    for i in range(intentos_max):
        try:
            r = redis.Redis(host=host, port=port, db=0, decode_responses=True)
            r.ping()
            return r
        except:
            time.sleep(2)
    print("redis no responde")
    sys.exit(1)


def conectar_kafka_consumer(servers, topic, group, intentos_max=30):
    # kafka se demora en arrancar, hay que reintentar
    for i in range(intentos_max):
        try:
            consumer = KafkaConsumer(
                topic,
                bootstrap_servers=servers,
                group_id=group,
                value_deserializer=lambda m: json.loads(m.decode("utf-8")),
                auto_offset_reset="earliest",
                enable_auto_commit=True
            )
            return consumer
        except NoBrokersAvailable:
            time.sleep(3)
        except Exception as e:
            time.sleep(3)
    print("no se pudo conectar a kafka")
    sys.exit(1)


def conectar_kafka_producer(servers, intentos_max=30):
    for i in range(intentos_max):
        try:
            producer = KafkaProducer(
                bootstrap_servers=servers,
                value_serializer=lambda v: json.dumps(v).encode("utf-8")
            )
            return producer
        except NoBrokersAvailable:
            time.sleep(3)
        except:
            time.sleep(3)
    print("no se pudo crear producer kafka")
    sys.exit(1)


def guardar_metrica(r, evento_tipo, consulta, latencia_total, latencia_cache, latencia_db, source):
    # guardar en la misma lista de redis que usaba la tarea 1
    evento = {
        "timestamp": time.time(),
        "evento": evento_tipo,
        "query_type": consulta.get("query_type", "?"),
        "query_id": consulta.get("id", "?"),
        "retry_count": consulta.get("retry_count", 0),
        "cache_key": "",
        "latencia_total_ms": latencia_total,
        "latencia_cache_ms": latencia_cache,
        "latencia_db_ms": latencia_db,
        "source": source,
        "consumer_id": consumer_id
    }
    r.rpush("metricas", json.dumps(evento))


def esperar_cache(url, intentos_max=60):
    # el cache_service se demora porque espera al generador de respuestas
    # que a su vez espera cargar el dataset de 2gb
    for i in range(intentos_max):
        try:
            resp = requests.get(url + "/docs", timeout=3)
            if resp.status_code == 200:
                return True
        except:
            pass
        time.sleep(3)
    return False


# --- arrancar ---

print(f"consumidor kafka [{consumer_id}]")
print(f"  topic: {kafka_topic}")
print(f"  grupo: {kafka_group}")
print(f"  max reintentos: {max_reintentos}")
print(f"  cache: {url_cache}")

# conectar a todo
r = conectar_redis(redis_host, redis_port)
consumer = conectar_kafka_consumer(kafka_servers, kafka_topic, kafka_group)
producer = conectar_kafka_producer(kafka_servers)

print("conectado a redis y kafka")

# esperar a que el cache service este listo
print("esperando al cache service...")
if not esperar_cache(url_cache):
    print("cache service no responde, saliendo")
    sys.exit(1)
print("cache service listo")

# --- loop principal ---

contador = 0

for mensaje in consumer:
    consulta = mensaje.value
    query_id = consulta.get("id", "?")
    retry_count = consulta.get("retry_count", 0)

    # armar el body para mandar al cache (sin los campos de kafka)
    body_cache = {}
    for campo in consulta:
        if campo not in ["id", "retry_count", "created_at", "last_error", "last_retry_at"]:
            body_cache[campo] = consulta[campo]

    t_inicio = time.time()

    try:
        resp = requests.post(url_cache + "/query", json=body_cache, timeout=15)

        if resp.status_code == 200:
            t_fin = time.time()
            latencia_total = round((t_fin - t_inicio) * 1000, 2)

            datos = resp.json()
            source = datos.get("source", "?")
            latencia_cache = datos.get("latency_ms", 0)

            # si fue miss, la latencia de db es lo que tardo el generador de respuestas
            if source == "cache":
                latencia_db = 0
            else:
                latencia_db = latencia_cache

            guardar_metrica(r, "exito", consulta, latencia_total, latencia_cache, latencia_db, source)
            contador = contador + 1

        else:
            # el cache respondio pero con error
            raise Exception(f"cache respondio con status {resp.status_code}")

    except Exception as error:
        # fallo, ver si mandamos a reintentos o a dlq
        if retry_count < max_reintentos:
            consulta["retry_count"] = retry_count + 1
            consulta["last_error"] = str(error)
            consulta["last_retry_at"] = time.time()
            producer.send("reintentos", value=consulta)
            guardar_metrica(r, "reintento", consulta, 0, 0, 0, "error")
        else:
            consulta["final_error"] = str(error)
            producer.send("dlq", value=consulta)
            guardar_metrica(r, "dlq", consulta, 0, 0, 0, "error")

    # progreso cada 100
    if (contador + 1) % 100 == 0:
        print(f"  [{consumer_id}] procesadas: {contador}")
