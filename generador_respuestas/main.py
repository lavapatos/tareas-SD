from fastapi import FastAPI
from fastapi.responses import JSONResponse
from datos import cargar_zonas
from queries import q1_conteo, q2_area, q3_densidad, q4_comparar, q5_distribucion_confianza
import time
import random
import os

app = FastAPI()

# fallas y latencia artificial (tarea 2)
falla_rate = float(os.environ.get("FALLA_RATE", "0"))
latency_artificial = float(os.environ.get("LATENCY_ARTIFICIAL", "0"))

# cargar dataset cuando parta el server
# se me demora mucho (chequear pedro)
inicio = time.time()
datos = cargar_zonas("/app/dataset/967_buildings.csv")
fin = time.time()
print(f"dataset cargado en {round(fin - inicio, 2)} segundos")

# mostrar cuantos edificios hay por zona para checkeo
for zona in datos:
    print(f"  {zona}: {len(datos[zona])} edificios")


@app.post("/query")
def procesar_consulta(body: dict):
    if falla_rate > 0 and random.random() < falla_rate:
        return JSONResponse(status_code=503, content={"error": "falla simulada"})

    if latency_artificial > 0:
        time.sleep(latency_artificial)

    # sacar los parametros del body
    tipo = body["query_type"]
    zona_id = body.get("zone_id", None)
    confianza_min = body.get("confidence_min", 0.0)

    # medir cuanto tarda la consulta
    t_inicio = time.time()

    if tipo == "Q1":
        df = datos[zona_id]
        respuesta = q1_conteo(df, confianza_min)

    elif tipo == "Q2":
        df = datos[zona_id]
        respuesta = q2_area(df, confianza_min)

    elif tipo == "Q3":
        df = datos[zona_id]
        respuesta = q3_densidad(df, zona_id, confianza_min)

    elif tipo == "Q4":
        zona_a = body["zone_id_a"]
        zona_b = body["zone_id_b"]
        df_a = datos[zona_a]
        df_b = datos[zona_b]
        respuesta = q4_comparar(df_a, zona_a, df_b, zona_b, confianza_min)

    elif tipo == "Q5":
        df = datos[zona_id]
        bins = body.get("bins", 5)
        respuesta = q5_distribucion_confianza(df, bins)

    else:
        return {"error": f"no conozco el tipo de consulta {tipo}"}

    t_fin = time.time()
    latencia_ms = round((t_fin - t_inicio) * 1000, 2)

    # fastapi >>>>>> flask
    return {
        "query_type": tipo,
        "result": respuesta,
        "latency_ms": latencia_ms
    }