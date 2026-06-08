import os
import json
import time
from datetime import datetime, timedelta, timezone
from dotenv import load_dotenv
from kafka import KafkaConsumer
from pymongo import MongoClient, UpdateOne, InsertOne
from pymongo.errors import BulkWriteError

# Carica le variabili d'ambiente dal file .env
load_dotenv()

KAFKA_BOOTSTRAP_SERVERS = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092").split(",")
MONGO_URI = os.getenv("MONGO_URI", "mongodb://localhost:27017")
MONGO_DB = os.getenv("MONGO_DB", "smart_factory")

# Soglie di allarme definite nel contratto dati
THRESHOLDS = {
    "temperatura_motore": 85.0,
    "vibrazione": 7.5,
    "consumo_energetico": 150.0,
    "temperatura_ambientale": 40.0,
    "umidita": 85.0,
    "co2": 1000.0,
    "rpm": 3500.0,
    "scarti": 50.0
}

# Contatori globali per la reportistica richiesti ogni 1000 messaggi
total_read = 0
total_discarded = 0
total_alerts = 0

def get_fascia_oraria(dt):
    """Calcola la fascia oraria basata sul timestamp del messaggio."""
    hour = dt.hour
    if 0 <= hour < 6:
        return "notte"
    elif 6 <= hour < 12:
        return "mattina"
    elif 12 <= hour < 18:
        return "pomeriggio"
    else:
        return "sera"

def main():
    global total_read, total_discarded, total_alerts

    print("--- Avvio dello Stream Processor (Studente B) ---")
    print(f"Connessione a Kafka: {KAFKA_BOOTSTRAP_SERVERS}")
    print(f"Connessione a MongoDB: {MONGO_URI} (DB: {MONGO_DB})")

    # Inizializzazione Client MongoDB e setup indici velocizzazione query business
    mongo_client = MongoClient(MONGO_URI)
    db = mongo_client[MONGO_DB]
    
    db.processed_events.create_index([("device_id", 1), ("timestamp", 1)])
    db.alerts.create_index([("device_id", 1), ("timestamp", 1)])
    db.aggregations.create_index([("device_id", 1), ("window_start", 1)])
    print("Indici MongoDB verificati/creati con successo.")

    # Inizializzazione Kafka Consumer per i 4 topic d'ambito
    topics = ['iot.ambientale', 'iot.macchinari', 'iot.logistica', 'iot.qualita']
    consumer = KafkaConsumer(
        *topics,
        bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS,
        group_id='smart-factory-consumers',
        auto_offset_reset='earliest',
        session_timeout_ms=10000,
        max_poll_records=100
    )
    print(f"Iscritto ai topic: {topics}")

    # Buffer per le operazioni in blocco (Bulk)
    processed_batch = []
    alert_batch = []
    aggregation_ops = []
    
    last_flush_time = time.time()
    BULK_SIZE = 500
    TIME_THRESHOLD_SEC = 5

    def flush_buffers():
        """Svuota i buffer locali scrivendo in blocco su MongoDB."""
        nonlocal last_flush_time
        
        flush_count = 0
        try:
            if processed_batch:
                db.processed_events.insert_many(processed_batch, ordered=False)
                flush_count += len(processed_batch)
            
            if alert_batch:
                db.alerts.insert_many(alert_batch, ordered=False)
                flush_count += len(alert_batch)
                    
            if aggregation_ops:
                db.aggregations.bulk_write(aggregation_ops, ordered=False)
                flush_count += len(aggregation_ops)
            
            if flush_count > 0:
                print(f"[FLUSH] Scritti {flush_count} documenti su MongoDB")
        
        except BulkWriteError as bwe:
            print(f"[ERRORE] BulkWriteError durante flush: {bwe.details}")
        except Exception as e:
            print(f"[ERRORE] Errore durante flush dei buffer: {str(e)}")

        # Reset dei buffer e del timer
        processed_batch.clear()
        alert_batch.clear()
        aggregation_ops.clear()
        last_flush_time = time.time()

    print("In attesa di messaggi dalla coda Kafka...")
    
    try:
        for message in consumer:
            # Gestisci timeout della connessione
            if message is None:
                continue
            
            total_read += 1
            
            # 1. Parsing e validazione base JSON
            try:
                msg_bytes = message.value
                msg_data = json.loads(msg_bytes.decode('utf-8'))
            except Exception:
                total_discarded += 1
                continue

            # 2. Validazione di conformità al Contratto Dati
            required_fields = ["device_id", "ambito", "timestamp", "tipo", "valore", "unita", "reparto", "linea"]
            is_valid = True
            
            for field in required_fields:
                if field not in msg_data or msg_data[field] is None:
                    is_valid = False
                    break
            
            if is_valid:
                if not isinstance(msg_data["valore"], (int, float)):
                    is_valid = False

            if not is_valid:
                total_discarded += 1
                continue

            # 3. Estrazione e parsing della data per Arricchimento
            try:
                ts_str = msg_data["timestamp"]
                # Normalizza il formato ISO 8601
                if ts_str.endswith("Z"):
                    ts_str = ts_str.replace("Z", "+00:00")
                dt_message = datetime.fromisoformat(ts_str)
                # Assicura timezone UTC
                if dt_message.tzinfo is None:
                    dt_message = dt_message.replace(tzinfo=timezone.utc)
            except Exception as e:
                total_discarded += 1
                continue

            # 4. Arricchimento dati
            tipo_misura = msg_data["tipo"]
            valore_misura = msg_data["valore"]
            
            # Controllo soglia d'allarme
            is_alert = False
            if tipo_misura in THRESHOLDS and valore_misura > THRESHOLDS[tipo_misura]:
                is_alert = True
                total_alerts += 1

            msg_data["is_alert"] = is_alert
            msg_data["fascia_oraria"] = get_fascia_oraria(dt_message)
            msg_data["processed_at"] = datetime.now(timezone.utc).isoformat()

            # 5. Accodamento nei rispettivi buffer di staging/serving
            processed_batch.append(msg_data)
            if is_alert:
                # Inseriamo una copia separata dell'evento arricchito nella collection degli alert
                alert_batch.append(dict(msg_data))

            # 6. Preparazione pipeline di aggregazione a finestra (1 minuto tumble) via UpdateOne
            window_start = dt_message.replace(second=0, microsecond=0)
            window_end = window_start + timedelta(minutes=1)
            
            agg_query = {
                "device_id": msg_data["device_id"],
                "tipo": tipo_misura,
                "window_start": window_start
            }
            
            # Pipeline dinamica per aggiornare al volo conteggio, somma e media matematica nel DB
            agg_pipeline = [
                {
                    "$set": {
                        "window_end": window_end,
                        "count": {"$add": [{"$ifNull": ["$count", 0]}, 1]},
                        "sum_valore": {"$add": [{"$ifNull": ["$sum_valore", 0]}, valore_misura]}
                    }
                },
                {
                    "$set": {
                        "mean_valore": {"$divide": ["$sum_valore", "$count"]}
                    }
                }
            ]
            aggregation_ops.append(UpdateOne(agg_query, agg_pipeline, upsert=True))

            # 7. Verifica delle condizioni di Flush (ogni 500 messaggi o ogni 5 secondi)
            if len(processed_batch) >= BULK_SIZE or (time.time() - last_flush_time) >= TIME_THRESHOLD_SEC:
                flush_buffers()

            # 8. Log periodico delle metriche ogni 1000 messaggi letti
            if total_read % 1000 == 0:
                print(f"[LOG STATS] Letti: {total_read} | Scartati: {total_discarded} | Alert Rilevati: {total_alerts}")

    except KeyboardInterrupt:
        print("\n[INFO] Interruzione rilevata da tastiera. Esecuzione del flush finale dei buffer...")
    finally:
        flush_buffers()
        mongo_client.close()
        print(f"[INFO] Pipeline interrotta in sicurezza. Statistiche finali -> Letti totali: {total_read}, Scartati: {total_discarded}, Alert: {total_alerts}")

if __name__ == "__main__":
    main()