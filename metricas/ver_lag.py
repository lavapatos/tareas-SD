import os
import time
import json
from kafka import KafkaConsumer
from kafka.structs import TopicPartition

servers = os.environ.get("KAFKA_BOOTSTRAP_SERVERS", "kafka:9092")
ruta_salida = "/app/output/kafka_lag.json"

topicos = ["consultas", "reintentos", "dlq"]
historial = []

print("inicio ver lag")

# consumidor npc para ver los offsets
consumidor = KafkaConsumer(
    bootstrap_servers=servers,
    group_id="grupo_consumidores"
)

while True:
    try:
        medicion = {}
        medicion["timestamp"] = time.time()
        
        i = 0
        while i < len(topicos):
            topico = topicos[i]
            
            particiones_id = consumidor.partitions_for_topic(topico)
            
            lag_total = 0
            end_total = 0
            comm_total = 0
            
            if particiones_id is not None:
                particiones = []
                for p in particiones_id:
                    particiones.append(TopicPartition(topico, p))
                    
                end_offsets = consumidor.end_offsets(particiones)
                
                j = 0
                while j < len(particiones):
                    tp = particiones[j]
                    end_off = end_offsets[tp]
                    
                    # sacar el offset que va el consumidor
                    comm_off = consumidor.committed(tp)
                    if comm_off is None:
                        comm_off = 0
                        
                    lag = end_off - comm_off
                    if lag < 0:
                        lag = 0
                        
                    lag_total = lag_total + lag
                    end_total = end_total + end_off
                    comm_total = comm_total + comm_off
                    
                    j = j + 1
                    
            info = {}
            info["lag"] = lag_total
            info["end_offset"] = end_total
            info["committed"] = comm_total
            
            medicion[topico] = info
            i = i + 1
            
        historial.append(medicion)
        
        with open(ruta_salida, "w", encoding="utf-8") as archivo:
            json.dump(historial, archivo, indent=2)
            
        print("lag guardado, espero 5 seg")
        time.sleep(5)
        
    except Exception as e:
        print("error en ver_lag:", e)
        time.sleep(5)
