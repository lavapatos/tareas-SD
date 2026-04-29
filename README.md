# Tarea 1 - Sistemas Distribuidos

Sistema de caché distribuido para consultas geoespaciales usando el dataset de Google Open Buildings (región metropolitana).

## Integrantes

- Edicson Solar
- Pedro Morales

## Qué hace

Simula un sistema donde se consultan zonas de Santiago, el sistema tiene 4 módulos:

1. **Generador de tráfico**: genera consultas automáticas con distribución zipf o uniforme
2. **Caché**: intercepta las consultas, si ya tiene la respuesta la devuelve al tiro (hit), si no la pide al generador de respuestas (miss). Usa redis con TTL y políticas de evicción configurables
3. **Generador de respuestas**: procesa las consultas Q1-Q5 sobre el dataset precargado en memoria con polars
4. **Métricas**: registra y analiza hits, misses, latencias, throughput y eviction rate

## Estructura del proyecto

```
├── cache/                    # servicio de cache con redis
│   ├── main.py
│   └── Dockerfile
├── generador_respuestas/     # procesa las queries Q1-Q5
│   ├── main.py
│   ├── datos.py
│   ├── queries.py
│   └── Dockerfile
├── generador_trafico/        # genera consultas sintéticas
│   ├── main.py
│   └── Dockerfile
├── metricas/                 # recolector y analizador de métricas
│   ├── recolector.py
│   ├── analizador.py
│   └── Dockerfile
├── data/                     # dataset (no incluido en el repo por peso)
├── output/                   # donde se guardan los resultados
├── docker-compose.yml
├── .env                      # parámetros configurables
└── experimentos.txt          # lista de experimentos para el análisis
```

## Cómo correrlo

### Requisitos

- docker y docker compose
- el dataset `967_buildings.csv` en la carpeta `data/`

### Pasos

1. Configurar los parámetros en `.env`:

```
N_CONSULTAS=10000
DISTRIBUCION=zipf
REDIS_MAXMEMORY=50mb
REDIS_POLICY=allkeys-lru
TTL_BASE=
PADDING_KB=300
```

1. Levantar los servicios:

```bash
docker compose up --build redis generador_respuestas cache_service generador_trafico
```

1. Esperar a que el tráfico termine (dice "listo. X consultas en Y segundos")

2. Recolectar y analizar métricas:

```bash
docker compose run recolector
docker compose run analizador
```

1. Para limpiar todo:

```bash
docker compose down --remove-orphans
```

## Parámetros configurables (.env)

| Variable | Qué hace | Valores |
|---|---|---|
| N_CONSULTAS | Cantidad de consultas a generar | 1000, 10000, etc |
| DISTRIBUCION | Tipo de distribución del tráfico | zipf, uniforme |
| REDIS_MAXMEMORY | Tamaño máximo del caché | 50mb, 200mb, 500mb |
| REDIS_POLICY | Política de evicción de redis | allkeys-lru, allkeys-lfu, allkeys-random |
| TTL_BASE | TTL fijo para todas las queries (vacío = usar defaults por tipo) | 10, 60, 300, o vacío |
| PADDING_KB | Padding en KB para que las entries pesen más | 0, 300, 1024 |

## Queries implementadas

- **Q1**: conteo de edificios en una zona
- **Q2**: área promedio y total de edificaciones
- **Q3**: densidad de edificaciones por km²
- **Q4**: comparación de densidad entre dos zonas
- **Q5**: distribución de confianza en una zona

## Métricas que se miden

- hit rate (global y por tipo de query)
- throughput (consultas/segundo)
- latencia p50 y p95 (total, hits, misses)
- eviction rate (evictions/minuto)
- cache efficiency

## Tecnologías

- python 3.11
- fastAPI + uvicorn (comunicación entre servicios)
- redis 7 (caché con TTL y políticas de evicción)
- polars (carga y procesamiento del dataset)
- docker compose (orquestación)
