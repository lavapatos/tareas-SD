from fastapi import FastAPI
import redis
import json
import time
import requests

app = FastAPI()

# conectar a redis
# host redis porque asi se va a llamar el contenedor en docker compose
r = redis.Redis(host="localhost", port=6379, db=0, decode_responses=True)

# la url del generador de respuestas (otro contenedor)
URL_GENERADOR = "http://localhost:8002"


def armar_cache_key(body):
    # armar keys segun tipo de query
    # enunciado dice que las keys tienen que ser asi
    tipo = body["query_type"]
    conf = body.get("confidence_min", 0.0)

    if tipo == "Q1":
        key = f"count:{body['zone_id']}:conf={conf}"
    elif tipo == "Q2":
        key = f"area:{body['zone_id']}:conf={conf}"
    elif tipo == "Q3":
        key = f"density:{body['zone_id']}:conf={conf}"
    elif tipo == "Q4":
        key = f"compare:density:{body['zone_id_a']}:{body['zone_id_b']}:conf={conf}"
    elif tipo == "Q5":
        bins = body.get("bins", 5)
        key = f"confidence_dist:{body['zone_id']}:bins={bins}"
    else:
        key = f"unknown:{tipo}"

    return key


@app.post("/query")
def recibir_consulta(body: dict):
    cache_key = armar_cache_key(body)
    t_inicio = time.time()

    # ver si esta en redis
    valor_cacheado = r.get(cache_key)

    if valor_cacheado is not None:
        # HIT - estaba en cache
        t_fin = time.time()
        latencia = round((t_fin - t_inicio) * 1000, 2)

        resultado = json.loads(valor_cacheado)

        # guardar la metrica del hit
        guardar_metrica("hit", body["query_type"], cache_key, latencia, 0)

        return {
            "source": "cache",
            "cache_key": cache_key,
            "result": resultado,
            "latency_ms": latencia
        }

    else:
        # MISS - no estaba, hay que pedirle al back generador de respuestas
        t_antes_db = time.time()

        resp = requests.post(f"{URL_GENERADOR}/query", json=body)
        datos_resp = resp.json()

        t_despues_db = time.time()
        latencia_db = round((t_despues_db - t_antes_db) * 1000, 2)

        resultado = datos_resp["result"]

        # meter en redis con TTL
        ttl = obtener_ttl(body["query_type"])
        r.setex(cache_key, ttl, json.dumps(resultado))

        t_fin = time.time()
        latencia_total = round((t_fin - t_inicio) * 1000, 2)

        # guardar metrica del miss
        guardar_metrica("miss", body["query_type"], cache_key, latencia_total, latencia_db)

        return {
            "source": "database",
            "cache_key": cache_key,
            "result": resultado,
            "latency_ms": latencia_total
        }


def obtener_ttl(query_type):
    # ttl en segundos por tipo de consulta
    # los mas simples (conteo) expiran mas rapido
    # los mas pesados (comparar, distribucion) duran mas
    ttls = {
        "Q1": 30,
        "Q2": 45,
        "Q3": 45,
        "Q4": 60,
        "Q5": 60
    }
    return ttls.get(query_type, 30)


def guardar_metrica(tipo_evento, query_type, cache_key, latencia_ms, latencia_db_ms):
    # pedro sacar de aca para analizar
    # feel free to you know you know bababooey
    evento = {
        "timestamp": time.time(),
        "evento": tipo_evento,
        "query_type": query_type,
        "cache_key": cache_key,
        "latencia_total_ms": latencia_ms,
        "latencia_db_ms": latencia_db_ms
    }
    r.rpush("metricas", json.dumps(evento))
