import math
import numpy as np

# las areas de cada zona en km2 son para Q3
# para sacarlas hay que pasar de grados a km
# 1 grado de latitud = aprox 111 km siempre
# 1 grado de longitud = 111 * cos(latitud) km por que? no se, wikipedia
# el peor articulo de wikipedia https://en.wikipedia.org/wiki/Geographic_coordinate_system#Length_of_a_degree

limites = {
    "Z1_providencia": {
        "lat_min": -33.445, "lat_max": -33.420,
        "lon_min": -70.640, "lon_max": -70.600
    },
    "Z2_las_condes": {
        "lat_min": -33.420, "lat_max": -33.390,
        "lon_min": -70.600, "lon_max": -70.550
    },
    "Z3_Maipu": {
        "lat_min": -33.530, "lat_max": -33.490,
        "lon_min": -70.790, "lon_max": -70.740
    },
    "Z4_Santiago_Centro": {
        "lat_min": -33.460, "lat_max": -33.430,
        "lon_min": -70.670, "lon_max": -70.630
    },
    "Z5_Pudahuel": {
        "lat_min": -33.470, "lat_max": -33.430,
        "lon_min": -70.810, "lon_max": -70.760
    },
}


def calcular_area_km2(lims):
    # latitud promedio para el coseno
    lat_medio = (lims["lat_min"] + lims["lat_max"]) / 2
    lat_medio_rad = math.radians(abs(lat_medio))

    # diferencia en grados * 111 = km
    alto = abs(lims["lat_max"] - lims["lat_min"]) * 111.0
    # para longitud hay que multiplicar por cos(lat) tambien
    ancho = abs(lims["lon_max"] - lims["lon_min"]) * 111.0 * math.cos(lat_medio_rad)

    return alto * ancho


# calculamos de una las areas para tenerlas listas
areas_km2 = {}
for z, l in limites.items():
    areas_km2[z] = calcular_area_km2(l)


# Q1 - contar edificios en una zona
def q1_conteo(df_zona, confianza_min=0.0):
    # si piden filtrar por confianza
    if confianza_min > 0.0:
        df_filtrado = df_zona.filter(df_zona["confidence"] >= confianza_min)
    else:
        df_filtrado = df_zona

    cantidad = len(df_filtrado)
    return cantidad


# Q2 - area promedio y total
def q2_area(df_zona, confianza_min=0.0):
    # mismo filtro que arriba
    if confianza_min > 0.0:
        df_filtrado = df_zona.filter(df_zona["confidence"] >= confianza_min)
    else:
        df_filtrado = df_zona

    # si queda vacio, 0
    if len(df_filtrado) == 0:
        return {"avg_area": 0.0, "total_area": 0.0, "n": 0}

    col_areas = df_filtrado["area_in_meters"]
    promedio = float(col_areas.mean())
    total = float(col_areas.sum())
    n = len(df_filtrado)

    return {"avg_area": promedio, "total_area": total, "n": n}


# Q3 - densidad por km2
# q1 para sacar el conteo y dividir por area de la zona
def q3_densidad(df_zona, zona_id, confianza_min=0.0):
    conteo = q1_conteo(df_zona, confianza_min)
    area = areas_km2[zona_id]

    if area == 0:
        return 0.0

    return round(conteo / area, 4)


# Q4 - comparar densidades entre dos zonas
def q4_comparar(df_zona_a, zona_id_a, df_zona_b, zona_id_b, confianza_min=0.0):
    dens_a = q3_densidad(df_zona_a, zona_id_a, confianza_min)
    dens_b = q3_densidad(df_zona_b, zona_id_b, confianza_min)

    if dens_a > dens_b:
        ganador = zona_id_a
    else:
        ganador = zona_id_b

    return {"zone_a": dens_a, "zone_b": dens_b, "winner": ganador}


# Q5 - distribucion de confianza en bins
def q5_distribucion_confianza(df_zona, num_bins=5):
    # para usar np.histogram
    scores = df_zona["confidence"].to_numpy()

    conteos, bordes = np.histogram(scores, bins=num_bins, range=(0.0, 1.0))

    # lista con cada bucket
    resultado = []
    i = 0
    while i < num_bins:
        un_bucket = {
            "bucket": i,
            "min": round(float(bordes[i]), 4),
            "max": round(float(bordes[i + 1]), 4),
            "count": int(conteos[i])
        }
        resultado.append(un_bucket)
        i = i + 1

    return resultado

