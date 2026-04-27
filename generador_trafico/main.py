import numpy as np
import requests
import time
import sys
import json

# el cache service
URL_CACHE = "http://cache_service:8001"

# zonas y tipos de queries que existen
zonas = [
    "Z1_providencia",
    "Z2_las_condes",
    "Z3_Maipu",
    "Z4_Santiago_Centro",
    "Z5_Pudahuel"
]

tipos_query = ["Q1", "Q2", "Q3", "Q4", "Q5"]

# valores de confidence que vamos a usar
# no todos, unos pocos nomas para que se repitan y haya hits
valores_confianza = [0.0, 0.3, 0.5, 0.7, 0.9]

# bins para Q5
valores_bins = [5, 10]


def armar_todas_las_consultas():
    # armamos todas las combinaciones posibles de consultas
    # las metemos en una lista y despues elegimos de ahi con la distribucion
    todas = []

    for zona in zonas:
        for conf in valores_confianza:
            # Q1
            todas.append({
                "query_type": "Q1",
                "zone_id": zona,
                "confidence_min": conf
            })
            # Q2
            todas.append({
                "query_type": "Q2",
                "zone_id": zona,
                "confidence_min": conf
            })
            # Q3
            todas.append({
                "query_type": "Q3",
                "zone_id": zona,
                "confidence_min": conf
            })

    # Q4 compara dos zonas distintas
    for i in range(len(zonas)):
        for j in range(len(zonas)):
            if i != j:
                for conf in valores_confianza:
                    todas.append({
                        "query_type": "Q4",
                        "zone_id_a": zonas[i],
                        "zone_id_b": zonas[j],
                        "confidence_min": conf
                    })

    # Q5 con distintos bins
    for zona in zonas:
        for b in valores_bins:
            todas.append({
                "query_type": "Q5",
                "zone_id": zona,
                "bins": b
            })

    return todas


def generar_indices_zipf(n_consultas, total_opciones, alpha=1.5):
    indices = []
    while len(indices) < n_consultas:
        lote = np.random.zipf(alpha, n_consultas * 2)
        # filtrar los fuera de rango
        buenos = [x - 1 for x in lote if x <= total_opciones]
        indices.extend(buenos)

    # tamaño
    indices = indices[:n_consultas]
    return indices


def generar_indices_uniforme(n_consultas, total_opciones):
    # uniforme, random entre 0 y el total
    indices = np.random.randint(0, total_opciones, n_consultas)
    return list(indices)


# parametros por linea de comando o defaults
if len(sys.argv) > 1:
    n_consultas = int(sys.argv[1])
else:
    n_consultas = 1000

if len(sys.argv) > 2:
    distribucion = sys.argv[2]
else:
    distribucion = "zipf"

print(f"generador de trafico")
print(f"  consultas: {n_consultas}")
print(f"  distribucion: {distribucion}")

# catalogo de consultas posibles
todas_las_consultas = armar_todas_las_consultas()
total = len(todas_las_consultas)
print(f"  consultas posibles: {total}")

# generar indices segun la distribucion
if distribucion == "zipf":
    indices = generar_indices_zipf(n_consultas, total)
elif distribucion == "uniforme":
    indices = generar_indices_uniforme(n_consultas, total)
else: # default
    print(f"distribucion '{distribucion}' no existe, usando zipf")
    indices = generar_indices_zipf(n_consultas, total)

# esperar a que el cache service este listo
listo = False
intentos = 0
while not listo and intentos < 30:
    try:
        requests.get(f"{URL_CACHE}/docs", timeout=2)
        listo = True
    except:
        time.sleep(2)
        intentos = intentos + 1

if not listo:
    print("cache service no responde, abortando")
    sys.exit(1)

inicio_total = time.time()

for i, idx in enumerate(indices):
    consulta = todas_las_consultas[idx]

    try:
        resp = requests.post(f"{URL_CACHE}/query", json=consulta, timeout=10)
    except Exception as e:
        print(f"  [{i}] error: {e}")
        continue

    # cada 100 consultas mostramos progreso
    if (i + 1) % 100 == 0:
        transcurrido = round(time.time() - inicio_total, 1)
        print(f"  [{i + 1}/{n_consultas}] {transcurrido}s")

fin_total = time.time()
duracion = round(fin_total - inicio_total, 2)
print(f"listo. {n_consultas} consultas en {duracion} segundos")
