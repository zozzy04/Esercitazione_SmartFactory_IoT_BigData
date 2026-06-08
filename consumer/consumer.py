import os
import json
import time
from datetime import datetime, timedelta, timezone
from dotenv import load_dotenv
from kafka import KafkaConsumer
from pymongo import MongoClient, UpdateOne
from pymongo.errors import BulkWriteError

load_dotenv()

KAFKA_BOOTSTRAP_SERVERS = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092").split(",")
MONGO_URI = os.getenv("MONGO_URI", "mongodb://localhost:27017")
MONGO_DB = os.getenv("MONGO_DB", "smart_factory")

# soglie di allarme concordate nel contratto dati
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

total_read = 0
total_discarded = 0
total_alerts = 0


def get_fascia_oraria(dt):
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
    print(f"Kafka: {KAFKA_BOOTSTRAP_SERVERS}")
    print(f"MongoDB: {MONGO_URI} — db: {MONGO_DB}")

    mongo_client = MongoClient(MONGO_URI)
    db = mongo_client[MONGO_DB]

    db.processed_events.create_index([("device_id", 1), ("timestamp", 1)])
    db.alerts.create_index([("device_id", 1), ("timestamp", 1)])
    db.aggregations.create_index([("device_id", 1), ("window_start", 1)])
    print("Indici creati.")

    topics = ["iot.ambientale", "iot.macchinari", "iot.logistica", "iot.qualita"]
    consumer = KafkaConsumer(
        *topics,
        bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS,
        group_id="smart-factory-consumers",
        auto_offset_reset="earliest",
        session_timeout_ms=10000,
        max_poll_records=500
    )
    print(f"Iscritto ai topic: {topics}")

    processed_batch = []
    alert_batch = []
    aggregation_ops = []
    last_flush_time = time.time()
    BULK_SIZE = 500
    FLUSH_INTERVAL = 5  # secondi

    def flush_buffers():
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
                print(f"[FLUSH] {flush_count} documenti scritti su MongoDB")
        except BulkWriteError as bwe:
            print(f"[ERRORE] BulkWriteError: {bwe.details}")
        except Exception as e:
            print(f"[ERRORE] flush fallito: {e}")

        processed_batch.clear()
        alert_batch.clear()
        aggregation_ops.clear()
        last_flush_time = time.time()

    print("In attesa di messaggi dalla coda Kafka...")

    try:
        for message in consumer:
            total_read += 1

            # parsing JSON
            try:
                msg_data = json.loads(message.value.decode("utf-8"))
            except Exception:
                total_discarded += 1
                continue

            # validazione campi obbligatori del contratto dati
            required_fields = ["device_id", "ambito", "timestamp", "tipo", "valore", "unita", "reparto", "linea"]
            is_valid = all(field in msg_data and msg_data[field] is not None for field in required_fields)
            if is_valid and not isinstance(msg_data["valore"], (int, float)):
                is_valid = False

            if not is_valid:
                total_discarded += 1
                continue

            # parsing timestamp
            try:
                ts_str = msg_data["timestamp"].replace("Z", "+00:00")
                dt_message = datetime.fromisoformat(ts_str)
                if dt_message.tzinfo is None:
                    dt_message = dt_message.replace(tzinfo=timezone.utc)
            except Exception:
                total_discarded += 1
                continue

            # arricchimento: allarme, fascia oraria, timestamp elaborazione
            tipo_misura = msg_data["tipo"]
            valore_misura = msg_data["valore"]

            is_alert = tipo_misura in THRESHOLDS and valore_misura > THRESHOLDS[tipo_misura]
            if is_alert:
                total_alerts += 1

            msg_data["is_alert"] = is_alert
            msg_data["fascia_oraria"] = get_fascia_oraria(dt_message)
            msg_data["processed_at"] = datetime.now(timezone.utc).isoformat()

            processed_batch.append(msg_data)
            if is_alert:
                alert_batch.append(dict(msg_data))

            # aggregazione a finestra temporale di 1 minuto
            window_start = dt_message.replace(second=0, microsecond=0)
            window_end = window_start + timedelta(minutes=1)

            agg_query = {
                "device_id": msg_data["device_id"],
                "tipo": tipo_misura,
                "window_start": window_start
            }
            # aggiorna conteggio, somma e media incrementalmente senza rileggere il documento
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

            if len(processed_batch) >= BULK_SIZE or (time.time() - last_flush_time) >= FLUSH_INTERVAL:
                flush_buffers()

            if total_read % 1000 == 0:
                print(f"[STATS] letti={total_read} | scartati={total_discarded} | alert={total_alerts}")

    except KeyboardInterrupt:
        print("\nInterruzione ricevuta, flush finale in corso...")
    finally:
        flush_buffers()
        consumer.close()
        mongo_client.close()
        print(f"[FINE] letti={total_read} | scartati={total_discarded} | alert={total_alerts}")


if __name__ == "__main__":
    main()
