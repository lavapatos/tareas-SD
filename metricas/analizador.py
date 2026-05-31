import json
import sys
import numpy as np


ruta_json = "/app/output/metricas_raw.json"

with open(ruta_json, "r", encoding="utf-8") as f:
    datos = json.load(f)

eventos = datos["eventos"]
redis_info = datos["redis_info"]

if len(eventos) == 0:
    sys.exit(0)

print(f"eventos: {len(eventos)}")
print()

# separar en dos listas
hits = []
misses = []
i = 0
while i < len(eventos):
    if eventos[i]["evento"] == "hit":
        hits.append(eventos[i])
    else:
        misses.append(eventos[i])
    i = i + 1

n_hits = len(hits)
n_misses = len(misses)
n_total = n_hits + n_misses

tipos_query = ["Q1", "Q2", "Q3", "Q4", "Q5"]


print("METRICA 1: HIT RATE")

if n_total > 0:
    hit_rate_global = n_hits / n_total
    miss_rate_global = n_misses / n_total
else:
    hit_rate_global = 0
    miss_rate_global = 0

print(f"global:")
print(f"hits:  {n_hits}")
print(f"misses:  {n_misses}")
print(f"total:  {n_total}")
print(f"hit rate:  {round(hit_rate_global * 100, 2)}%") 
print(f"miss rate:  {round(miss_rate_global * 100, 2)}%")
print()

# lo mismo pero separado por Q1, Q2, etc
print(f"por tipo de query:")
j = 0
while j < len(tipos_query):
    qt = tipos_query[j]

    h = 0
    m = 0
    k = 0
    while k < len(eventos):
        if eventos[k]["query_type"] == qt:
            if eventos[k]["evento"] == "hit":
                h = h + 1
            else:
                m = m + 1
        k = k + 1

    t = h + m
    if t > 0:
        rate = round(h / t * 100, 2)
    else:
        rate = 0

    print(f"{qt}: {h} hits, {m} misses, total {t}, hit rate {rate}%")
    j = j + 1

print()


print("METRICA 2: THROUGHPUT")

# sacamos el primer y ultimo timestamp para saber cuanto duro
timestamps = []
i = 0
while i < len(eventos):
    timestamps.append(eventos[i]["timestamp"])
    i = i + 1

t_min = min(timestamps)
t_max = max(timestamps)
duracion_s = t_max - t_min

if duracion_s > 0:
    throughput = n_total / duracion_s
else:
    throughput = n_total

print(f"duracion total:  {round(duracion_s, 2)} segundos")
print(f"consultas:  {n_total}")
print(f"throughput:  {round(throughput, 2)} consultas/s")
print()

print("METRICA 3: LATENCIA p50 / p95")

# separar latencias por tipo para comparar
todas_latencias = []
latencias_hits = []
latencias_misses = []
latencias_db = []

i = 0
while i < len(eventos):
    lat = eventos[i]["latencia_total_ms"]
    todas_latencias.append(lat)

    if eventos[i]["evento"] == "hit":
        latencias_hits.append(lat)
    else:
        latencias_misses.append(lat)
        # latencia_db_ms es solo lo que tardo el generador de repsuestas
        if eventos[i]["latencia_db_ms"] > 0:
            latencias_db.append(eventos[i]["latencia_db_ms"])

    i = i + 1

print(f"total ({len(todas_latencias)} eventos):")
p50 = round(float(np.percentile(todas_latencias, 50)), 2)
p95 = round(float(np.percentile(todas_latencias, 95)), 2)
avg = round(float(np.mean(todas_latencias)), 2)
print(f"p50: {p50} ms")
print(f"p95: {p95} ms")
print(f"promedio: {avg} ms") # se que no lo pide pero le agregue avg y min y max para tener mas data
print(f"min: {round(min(todas_latencias), 2)} ms")
print(f"max: {round(max(todas_latencias), 2)} ms")
print()

if len(latencias_hits) > 0:
    print(f"hits / cache ({len(latencias_hits)} eventos):")
    p50 = round(float(np.percentile(latencias_hits, 50)), 2)
    p95 = round(float(np.percentile(latencias_hits, 95)), 2)
    avg = round(float(np.mean(latencias_hits)), 2)
    print(f"p50: {p50} ms")
    print(f"p95: {p95} ms")
    print(f"promedio: {avg} ms")
    print()

if len(latencias_misses) > 0:
    print(f"misses / db ({len(latencias_misses)} eventos):")
    p50 = round(float(np.percentile(latencias_misses, 50)), 2)
    p95 = round(float(np.percentile(latencias_misses, 95)), 2)
    avg = round(float(np.mean(latencias_misses)), 2)
    print(f"p50: {p50} ms")
    print(f"p95: {p95} ms")
    print(f"promedio: {avg} ms")
    print()

if len(latencias_db) > 0:
    print(f"solo tiempo db ({len(latencias_db)} eventos):")
    p50 = round(float(np.percentile(latencias_db, 50)), 2)
    p95 = round(float(np.percentile(latencias_db, 95)), 2)
    avg = round(float(np.mean(latencias_db)), 2)
    print(f"p50: {p50} ms")
    print(f"p95: {p95} ms")
    print(f"promedio: {avg} ms")
    print()

print(f"latencia por tipo de query:")
j = 0
while j < len(tipos_query):
    qt = tipos_query[j]

    lats = []
    k = 0
    while k < len(eventos):
        if eventos[k]["query_type"] == qt:
            lats.append(eventos[k]["latencia_total_ms"])
        k = k + 1

    if len(lats) > 0:
        p50 = round(float(np.percentile(lats, 50)), 2)
        p95 = round(float(np.percentile(lats, 95)), 2)
        avg = round(float(np.mean(lats)), 2)
        print(f"{qt}: p50={p50}ms, p95={p95}ms, avg={avg}ms")
    else:
        print(f"{qt}: sin datos")

    j = j + 1

print()

# cuantas keys boto el redis por falta de memoria, por minuto
print("METRICA 4: EVICTION RATE")

evicted = redis_info.get("evicted_keys", 0)

if duracion_s > 0:
    duracion_min = duracion_s / 60
else:
    duracion_min = 1

if duracion_min > 0:
    eviction_rate = round(evicted / duracion_min, 2)
else:
    eviction_rate = 0

print(f"evicted keys:  {evicted}")
print(f"duracion:  {round(duracion_min, 2)} minutos")
print(f"eviction rate:  {eviction_rate} evictions/min")
print(f"maxmemory:  {redis_info.get('maxmemory', 0)} bytes")
print(f"policy:  {redis_info.get('maxmemory_policy', '?')}")
print(f"memoria usada:  {redis_info.get('used_memory_human', '?')}")
print()


print("METRICA 5: CACHE EFFICIENCY")

if len(latencias_hits) > 0:
    avg_t_cache = float(np.mean(latencias_hits))
else:
    avg_t_cache = 0

if len(latencias_db) > 0:
    avg_t_db = float(np.mean(latencias_db))
else:
    avg_t_db = 0

total_time = 0
i = 0
while i < len(todas_latencias):
    total_time = total_time + todas_latencias[i]
    i = i + 1

numerador = n_hits * avg_t_cache

denominador = total_time - (n_misses * avg_t_db)

if denominador != 0:
    cache_efficiency = round(numerador / denominador, 4)
else:
    cache_efficiency = 0

print(f"avg latencia hit (t_cache):  {round(avg_t_cache, 2)} ms")
print(f"avg latencia db (t_db):  {round(avg_t_db, 2)} ms")
print(f"tiempo total acumulado:  {round(total_time, 2)} ms")
print(f"cache efficiency:  {cache_efficiency}")
print()

# las 5 metricas juntas
print("RESUMEN")

p50_total = round(float(np.percentile(todas_latencias, 50)), 2)
p95_total = round(float(np.percentile(todas_latencias, 95)), 2)

print(f"hit rate:  {round(hit_rate_global * 100, 2)}%")
print(f"throughput:  {round(throughput, 2)} consultas/s")
print(f"latencia p50:  {p50_total} ms")
print(f"latencia p95:  {p95_total} ms")
print(f"eviction rate:  {eviction_rate} evictions/min")
print(f"cache efficiency:  {cache_efficiency}")
print()

# metricas de la tarea 2
print("METRICAS TAREA 2")

total_eventos = len(eventos)

# contar cuantos de reintento y dlq hay
cant_reintentos = 0
cant_dlq = 0
cant_exito_recuperado = 0

tiempo_primer_reintento = None
tiempo_ultimo_exito_recuperado = None

i = 0
while i < len(eventos):
    ev = eventos[i]
    tipo = ev["evento"]
    rc = ev.get("retry_count", 0)
    
    if tipo == "reintento":
        cant_reintentos = cant_reintentos + 1
        
        if tiempo_primer_reintento is None:
            tiempo_primer_reintento = ev["timestamp"]
        else:
            if ev["timestamp"] < tiempo_primer_reintento:
                tiempo_primer_reintento = ev["timestamp"]
                
    elif tipo == "dlq":
        cant_dlq = cant_dlq + 1
        
    elif tipo == "exito":
        if rc > 0:
            cant_exito_recuperado = cant_exito_recuperado + 1
            
            if tiempo_ultimo_exito_recuperado is None:
                tiempo_ultimo_exito_recuperado = ev["timestamp"]
            else:
                if ev["timestamp"] > tiempo_ultimo_exito_recuperado:
                    tiempo_ultimo_exito_recuperado = ev["timestamp"]
                    
    i = i + 1

# retry rate
if total_eventos > 0:
    retry_rate = cant_reintentos / total_eventos
else:
    retry_rate = 0

# recovery rate
if cant_reintentos > 0:
    recovery_rate = cant_exito_recuperado / cant_reintentos
else:
    recovery_rate = 0

# DLQ rate
if total_eventos > 0:
    dlq_rate = cant_dlq / total_eventos
else:
    dlq_rate = 0

# recovery time
if tiempo_primer_reintento is not None and tiempo_ultimo_exito_recuperado is not None:
    recovery_time = tiempo_ultimo_exito_recuperado - tiempo_primer_reintento
else:
    recovery_time = 0

if recovery_time < 0:
    recovery_time = 0

print(f"retry rate: {round(retry_rate * 100, 2)}%")
print(f"recovery rate: {round(recovery_rate * 100, 2)}%")
print(f"dlq rate: {round(dlq_rate * 100, 2)}%")
print(f"recovery time: {round(recovery_time, 2)} segundos")
print()

print("BACKLOG SIZE")
try:
    with open("/app/output/kafka_lag.json", "r", encoding="utf-8") as f:
        datos_lag = json.load(f)
        
    max_lag_consultas = 0
    sum_lag_consultas = 0
    
    max_lag_reintentos = 0
    sum_lag_reintentos = 0
    
    max_lag_dlq = 0
    sum_lag_dlq = 0
    
    muestras = len(datos_lag)
    
    if muestras > 0:
        j = 0
        while j < muestras:
            muestra = datos_lag[j]
            
            lag_c = muestra.get("consultas", {}).get("lag", 0)
            lag_r = muestra.get("reintentos", {}).get("lag", 0)
            lag_d = muestra.get("dlq", {}).get("lag", 0)
            
            if lag_c > max_lag_consultas:
                max_lag_consultas = lag_c
            sum_lag_consultas = sum_lag_consultas + lag_c
            
            if lag_r > max_lag_reintentos:
                max_lag_reintentos = lag_r
            sum_lag_reintentos = sum_lag_reintentos + lag_r
            
            if lag_d > max_lag_dlq:
                max_lag_dlq = lag_d
            sum_lag_dlq = sum_lag_dlq + lag_d
                
            j = j + 1
            
        print(f"consultas: max {max_lag_consultas}, promedio {round(sum_lag_consultas / muestras, 2)}")
        print(f"reintentos: max {max_lag_reintentos}, promedio {round(sum_lag_reintentos / muestras, 2)}")
        print(f"dlq: max {max_lag_dlq}, promedio {round(sum_lag_dlq / muestras, 2)}")
    else:
        print("no hay muestras de lag")
except Exception as e:
    print("no se pudo leer el kafka_lag.json")

print()
