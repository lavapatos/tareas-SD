import numpy as np
import time
import sys
import json
import os
from datetime import datetime, timezone
from kafka import KafkaProducer
from kafka.errors import NoBrokersAvailable

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


def armar_mensaje(consulta, id_consulta):
    # id unico, contador de reintentos y timestamp de creacion
    mensaje = {
        "id": id_consulta,
        "retry_count": 0,
        "created_at": datetime.now(timezone.utc).isoformat(),
    }
    # agregar los campos de la consulta al mensaje
    for clave in consulta:
        mensaje[clave] = consulta[clave]
    return mensaje


def convertir_mensaje(mensaje):
    # convertir el diccionario a JSON para enviarlo al kafka
    return json.dumps(mensaje).encode("utf-8")


def esperar_kafka(bootstrap_servers, max_intentos=30):
    # esperar a que el kafka este listo
    for intento in range(max_intentos):
        try:
            producer = KafkaProducer(
                bootstrap_servers=bootstrap_servers,
                value_serializer=convertir_mensaje
            )
            return producer
        except NoBrokersAvailable:
            time.sleep(3)
        except Exception as e:
            time.sleep(3)

    print("kafka no responde")
    sys.exit(1)


# --- leer parametros desde env ---

if os.environ.get("N_CONSULTAS", "") != "":
    n_consultas = int(os.environ["N_CONSULTAS"])
elif len(sys.argv) > 1:
    n_consultas = int(sys.argv[1])
else:
    n_consultas = 1000

if os.environ.get("DISTRIBUCION", "") != "":
    distribucion = os.environ["DISTRIBUCION"]
elif len(sys.argv) > 2:
    distribucion = sys.argv[2]
else:
    distribucion = "zipf"

# kafka
kafka_servers = os.environ.get("KAFKA_BOOTSTRAP_SERVERS", "kafka:9092")
kafka_topic   = os.environ.get("KAFKA_TOPIC", "consultas")

# tasa de envio
send_rate = float(os.environ.get("SEND_RATE", "0"))
if send_rate > 0:
    delay_entre_mensajes = 1.0 / send_rate
else:
    delay_entre_mensajes = 0.0

# modo spike
spike_enabled  = os.environ.get("SPIKE_ENABLED", "false").lower() == "true"
spike_at_pct   = int(os.environ.get("SPIKE_AT", "50"))        # % del total donde empieza el spike
spike_duration = int(os.environ.get("SPIKE_DURATION", "200")) # cantidad de consultas del spike
spike_rate     = float(os.environ.get("SPIKE_RATE", "200"))   # consultas/seg durante el spike

# indice donde empieza y termina el spike
spike_start = int(n_consultas * spike_at_pct / 100)
spike_end   = spike_start + spike_duration

print(f"generador de trafico")
print(f"  consultas: {n_consultas}")
print(f"  distribucion: {distribucion}")
print(f"  kafka: {kafka_servers}")
print(f"  topico: {kafka_topic}")
if send_rate > 0:
    print(f"  tasa de envio: {send_rate} q/s")
else:
    print(f"  tasa de envio: sin limite")
if spike_enabled:
    print(f"  spike: activado (desde consulta {spike_start} hasta {spike_end}, {spike_rate} q/s)")
else:
    print(f"  spike: desactivado")

# catalogo de consultas posibles
todas_las_consultas = armar_todas_las_consultas()
total = len(todas_las_consultas)
print(f"  consultas posibles: {total}")

# generar indices segun la distribucion
if distribucion == "zipf":
    indices = generar_indices_zipf(n_consultas, total)
elif distribucion == "uniforme":
    indices = generar_indices_uniforme(n_consultas, total)
else:  # default
    print(f"distribucion '{distribucion}' no existe, usando zipf")
    indices = generar_indices_zipf(n_consultas, total)

# conectar a kafka
producer = esperar_kafka(kafka_servers)

inicio_total = time.time()

for i, idx in enumerate(indices):
    consulta = todas_las_consultas[idx]
    mensaje = armar_mensaje(consulta, i)

    try:
        producer.send(kafka_topic, value=mensaje)
    except Exception as e:
        print(f"  [{i}] error enviando a kafka: {e}")
        continue

    # control de tasa de envio
    en_spike = spike_enabled and spike_start <= i and i < spike_end
    if en_spike:
        # spike sin delay
        if spike_rate > 0:
            time.sleep(1.0 / spike_rate)
    else:
        if delay_entre_mensajes > 0:
            time.sleep(delay_entre_mensajes)

    # cada 100 consultas mostramos progreso
    if (i + 1) % 100 == 0:
        transcurrido = round(time.time() - inicio_total, 1)
        if en_spike:
            modo = "SPIKE"
        else:
            modo = "normal"
        print(f"  [{i + 1}/{n_consultas}] {transcurrido}s [{modo}]")

producer.flush()
producer.close()

fin_total = time.time()
duracion = round(fin_total - inicio_total, 2)
print(f"{n_consultas} consultas publicadas en {duracion} segundos")
