import redis
import json
import sys
import time


r = redis.Redis(host="localhost", port=6379, db=0, decode_responses=True)



ruta_salida = "../output/metricas_raw.json"

# Para nada copiado de un tal "lavapatos"
listo = False
intentos = 0
while not listo and intentos < 30:
    try:
        r.ping()
        listo = True
    except:
        time.sleep(2)
        intentos = intentos + 1

if not listo:
    print("redis no responde, saliendo")
    sys.exit(1)

eventos_raw = r.lrange("metricas", 0, -1)

eventos = []
i = 0
while i < len(eventos_raw):
    eventos.append(json.loads(eventos_raw[i]))
    i = i + 1

print(f"eventos: {len(eventos)}")

# sacar info de redis para evictions y memoria
info_stats = r.info("stats")
info_memory = r.info("memory")
total_keys = r.dbsize()

redis_info = {
    "evicted_keys": info_stats.get("evicted_keys", 0),
    "keyspace_hits": info_stats.get("keyspace_hits", 0),
    "keyspace_misses": info_stats.get("keyspace_misses", 0),
    "used_memory_bytes": info_memory.get("used_memory", 0),
    "used_memory_human": info_memory.get("used_memory_human", "?"),
    "maxmemory": info_memory.get("maxmemory", 0),
    "maxmemory_policy": info_memory.get("maxmemory_policy", "?"),
    "total_keys": total_keys
}

# armar el json final con todo
salida = {
    "eventos": eventos,
    "redis_info": redis_info,
}

# guardar en el archivo
with open(ruta_salida, "w", encoding="utf-8") as f:
    json.dump(salida, f, indent=2, ensure_ascii=False)

print(f"{len(eventos)} eventos guardados en {ruta_salida}")
print(f"redis memoria: {redis_info['used_memory_human']}")
print(f"redis policy: {redis_info['maxmemory_policy']}")
print(f"evicted keys: {redis_info['evicted_keys']}")
print(f"keys activas: {redis_info['total_keys']}")

# limpiar la lista de metricas en redis para que si se hace otra empiece limpia
r.delete("metricas")

r.flushdb() # tambien el cache

