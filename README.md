# Que vamos a usar (flujo / propuesta):

# para el generador de respuestas (el dataset) use polars para cargar el csv y hacer los filtrados, es mas rapido que pandas y sufren menos los notes humildes.

# para que el dataset le responda al cache cuando este le pida algo que usamos? tipo para que se comuniquen. Estuve viendo que aprovechando el docker compose, con fastAPI se puede manejar facil, porque segun yo con el puro docker no se pueden comunicar (lo que entiendo) entre el dataset y el redis.

# En el caso hipotetico que usaramos el fastAPI para comunicacion, tambien seria para el generador de trafico hacia el sistema de cache (redis).

# Osea seria como  Generador de trafico -> HTTP -> intermediario (fastAPI) -> Redis. y si falla (no hit) seria como Redis -> intermediario -> HTTP -> Generador de respuestas -> calculo de la querie -> devuelve al cache -> devuelve al redis y listo.