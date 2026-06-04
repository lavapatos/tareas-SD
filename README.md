# Tarea 2 - Sistemas Distribuidos

Sistema distribuido para consultas geoespaciales usando el dataset de google open buildings. Esta versión mantiene el caché de la tarea 1 y agrega kafka para procesar consultas en cola, usar múltiples consumidores, manejar reintentos y registrar mensajes en DLQ.

## Integrantes

- Edicson Solar Salinas
- Pedro Morales Nadal

## Qué hace

Simula un sistema donde se consultan zonas de Santiago. En la tarea 1 el flujo era directo entre generador de tráfico, caché y generador de respuestas. En esta tarea las consultas pasan primero por Kafka, por lo que pueden quedar en cola, ser procesadas por varios consumidores o reintentarse si el generador de respuestas falla.

El sistema tiene estos módulos:

1. **Generador de tráfico**: genera consultas automáticas Q1-Q5 con distribución zipf o uniforme y las publica en Kafka.
2. **Kafka**: guarda las consultas en el tópico principal `consultas`. También se usan los tópicos `reintentos` y `dlq`.
3. **Consumidores Kafka**: leen consultas desde Kafka, consultan el caché y registran éxito, reintento o DLQ.
4. **Caché**: intercepta las consultas. Si ya tiene la respuesta la devuelve como hit; si no, llama al generador de respuestas. Usa Redis con TTL y política de evicción configurable.
5. **Generador de respuestas**: procesa las consultas Q1-Q5 sobre el dataset precargado en memoria. Permite simular falla aleatoria y latencia artificial.
6. **Gestor de reintentos**: toma mensajes desde `reintentos`, espera con backoff exponencial y los vuelve a publicar en `consultas`.
7. **Métricas**: registra y analiza hit rate, throughput, latencia, evictions, retries, recovery, DLQ y backlog.

## Estructura del proyecto

```text
├── cache/                    # servicio de caché con Redis
│   ├── main.py
│   └── Dockerfile
├── consumidor/               # consumidores Kafka
│   ├── main.py
│   └── Dockerfile
├── generador_respuestas/     # procesa las queries Q1-Q5
│   ├── main.py
│   ├── datos.py
│   ├── queries.py
│   └── Dockerfile
├── generador_trafico/        # genera consultas sintéticas y publica en Kafka
│   ├── main.py
│   └── Dockerfile
├── gestor_reintentos/        # consume reintentos y republica mensajes
│   ├── reintentos.py
│   └── Dockerfile
├── metricas/                 # recolector, analizador y monitor de lag
│   ├── recolector.py
│   ├── analizador.py
│   ├── ver_lag.py
│   └── Dockerfile
├── data/                     # dataset local, no incluido en el repo
├── output/                   # salidas de métricas
├── docker-compose.yml
├── .env                      # parámetros configurables
└── experimentos_t2.txt       # lista de escenarios usados para el análisis
```

## Cómo correrlo

### Requisitos

- Docker y Docker Compose
- El dataset `967_buildings.csv` en la carpeta `data/`

El CSV no forma parte del repositorio por tamaño.

### Pasos

1. Configurar los parámetros en `.env`:

```env
N_CONSULTAS=10000
DISTRIBUCION=zipf
REDIS_MAXMEMORY=50mb
REDIS_POLICY=volatile-lfu
TTL_BASE=60
PADDING_KB=1024
KAFKA_BOOTSTRAP_SERVERS=kafka:9092
KAFKA_TOPIC=consultas
SEND_RATE=50
SPIKE_ENABLED=false
SPIKE_AT=40
SPIKE_DURATION=2000
SPIKE_RATE=200
MAX_RETRIES=3
FALLA_RATE=0.0
LATENCY_ARTIFICIAL=0.0
```

2. Levantar los servicios con 1 consumidor:

```bash
docker compose down --volumes --remove-orphans
docker compose up --build -d --scale consumidor=1 \
  zookeeper kafka redis generador_respuestas cache_service \
  generador_trafico consumidor gestor_reintentos monitor_lag
```

3. Esperar a que termine el generador de tráfico:

```bash
docker wait $(docker compose ps -q generador_trafico)
```

4. Recolectar y analizar métricas:

```bash
docker compose run --rm recolector python recolector.py
docker compose run --rm analizador python analizador.py
```

5. Para limpiar todo:

```bash
docker compose down --volumes --remove-orphans
```

## Parámetros configurables (.env)

| Variable | Qué hace | Valores |
|---|---|---|
| `N_CONSULTAS` | Cantidad de consultas a generar | `1000`, `10000`, etc |
| `DISTRIBUCION` | Tipo de distribución del tráfico | `zipf`, `uniforme` |
| `REDIS_MAXMEMORY` | Tamaño máximo del caché | `50mb`, `200mb`, `500mb` |
| `REDIS_POLICY` | Política de evicción de Redis | `volatile-lru`, `volatile-lfu`, `volatile-random` |
| `TTL_BASE` | TTL fijo para las entradas del caché | `10`, `60`, `300` |
| `PADDING_KB` | Padding en KB para que las entradas pesen más | `0`, `300`, `1024` |
| `KAFKA_BOOTSTRAP_SERVERS` | Dirección del broker Kafka dentro de Docker | `kafka:9092` |
| `KAFKA_TOPIC` | Tópico principal de consultas | `consultas` |
| `SEND_RATE` | Tasa base de envío | `50`, `200`, etc |
| `SPIKE_ENABLED` | Activa spike de tráfico | `true`, `false` |
| `SPIKE_AT` | Porcentaje donde empieza el spike | `40`, `50`, etc |
| `SPIKE_DURATION` | Cantidad de consultas enviadas bajo spike | `200`, `2000`, etc |
| `SPIKE_RATE` | Tasa durante spike | `200`, etc |
| `MAX_RETRIES` | Máximo de reintentos antes de DLQ | `3` |
| `FALLA_RATE` | Probabilidad de falla del generador de respuestas | `0`, `0.3`, `0.9` |
| `LATENCY_ARTIFICIAL` | Latencia artificial del generador de respuestas | `0`, `0.5`, `1.0`, `2.0` |

## Consumidores

Para probar escalamiento horizontal se cambia la escala del servicio `consumidor`.

Ejemplo con 4 consumidores:

```bash
docker compose down --volumes --remove-orphans
docker compose up --build -d --scale consumidor=4 \
  zookeeper kafka redis generador_respuestas cache_service \
  generador_trafico consumidor gestor_reintentos monitor_lag
```

Kafka se configuró con 8 particiones para poder comparar 1, 2, 4 y 8 consumidores.

## Fallas y recuperación

### Falla temporal

La caída temporal del generador de respuestas se simula deteniendo el servicio durante la ejecución:

```bash
docker compose stop generador_respuestas
```

Después de la ventana de falla, se vuelve a levantar:

```bash
docker compose start generador_respuestas
```

Las consultas que fallan se envían a `reintentos`. Si una consulta supera `MAX_RETRIES`, se publica en `dlq`.

### Falla aleatoria

La falla por consulta se configura con `FALLA_RATE`:

```env
FALLA_RATE=0.3
LATENCY_ARTIFICIAL=0.0
```

### Latencia artificial

La latencia artificial del generador de respuestas se configura con `LATENCY_ARTIFICIAL`:

```env
FALLA_RATE=0.0
LATENCY_ARTIFICIAL=1.0
```

### Spike de tráfico

El spike se activa con:

```env
SPIKE_ENABLED=true
SPIKE_AT=40
SPIKE_DURATION=2000
SPIKE_RATE=200
```

## Queries implementadas

- **Q1**: conteo de edificios en una zona.
- **Q2**: área promedio y total de edificaciones.
- **Q3**: densidad de edificaciones por km².
- **Q4**: comparación de densidad entre dos zonas.
- **Q5**: distribución de confianza en una zona.

## Métricas que se miden

Métricas heredadas de la tarea 1:

- hit rate global y por tipo de query
- throughput
- latencia p50 y p95
- eviction rate
- cache efficiency

Métricas agregadas en la tarea 2:

- retry rate
- recovery rate
- DLQ rate
- recovery time
- backlog máximo y promedio por tópico Kafka

Las salidas principales quedan en:

- `output/metricas_raw.json`
- `output/kafka_lag.json`

## Tecnologías

- Python 3.11
- FastAPI + Uvicorn
- Redis 7
- Apache Kafka
- Docker Compose
