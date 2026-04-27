import polars as pl

def cargar_zonas(ruta_csv: str) -> dict:

    limites_zonas = {  # hacemos un dic anidado para guardar los limites
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
 
    escaneado = pl.scan_csv( # cargamos el csv
        ruta_csv,
        schema_overrides={
            "latitude": pl.Float32,
            "longitude": pl.Float32,
            "area_in_meters": pl.Float32,
            "confidence": pl.Float32
        }
    ).select([
        "latitude",
        "longitude",
        "area_in_meters",
        "confidence"
    ])

    dataset_final = {}

    for zona_id, limites in limites_zonas.items(): # usamos el dic de antes para iterar y guardamos el resultado en el dic y guardamos el resultado en el dic final
        df_filtrado = escaneado.filter(
            pl.col("latitude").is_between(limites["lat_min"], limites["lat_max"])
            & pl.col("longitude").is_between(limites["lon_min"], limites["lon_max"])
        ).collect()

        dataset_final[zona_id] = df_filtrado

    return dataset_final