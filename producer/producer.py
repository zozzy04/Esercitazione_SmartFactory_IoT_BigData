import json
import os
import random
import time
from datetime import datetime, timezone

from dotenv import load_dotenv
from faker import Faker
from kafka import KafkaProducer
from pymongo import ASCENDING, MongoClient

load_dotenv()

fake = Faker()

# misure disponibili per ogni ambito, con range realistici
MISURE = {
    "ambientale": [
        {"tipo": "temperatura_ambientale", "unita": "C",   "range": (15.0, 45.0)},
        {"tipo": "umidita",                "unita": "%",   "range": (20.0, 95.0)},
        {"tipo": "co2",                    "unita": "ppm", "range": (300.0, 1500.0)},
    ],
    "macchinari": [
        {"tipo": "temperatura_motore",  "unita": "C",    "range": (40.0, 110.0)},
        {"tipo": "vibrazione",          "unita": "mm/s", "range": (0.1, 12.0)},
        {"tipo": "rpm",                 "unita": "rpm",  "range": (500.0, 4000.0)},
        {"tipo": "consumo_energetico",  "unita": "kWh",  "range": (10.0, 200.0)},
    ],
    "logistica": [
        {"tipo": "posizione_lat", "unita": "deg",   "range": (45.40, 45.50)},
        {"tipo": "posizione_lon", "unita": "deg",   "range": (9.10, 9.20)},
        {"tipo": "rfid_lettura",  "unita": "count", "range": (0, 1)},
    ],
    "qualita": [
        {"tipo": "pezzi_prodotti",  "unita": "count", "range": (0, 500)},
        {"tipo": "scarti",          "unita": "count", "range": (0, 80)},
        {"tipo": "esito_controllo", "unita": "bool",  "range": (0, 1)},
    ],
}

TOPICS = {
    "ambientale": "iot.ambientale",
    "macchinari": "iot.macchinari",
    "logistica":  "iot.logistica",
    "qualita":    "iot.qualita",
}

REPARTI = ["Assemblaggio", "Verniciatura", "Magazzino", "Controllo Qualita", "Spedizione"]
LINEE = ["L1", "L2", "L3", "L4"]

# unità di misura che producono valori interi
INTEGER_UNITS = {"count", "bool"}


def load_config():
    return {
        "kafka_servers": os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092"),
        "mongo_uri":     os.getenv("MONGO_URI", "mongodb://localhost:27017"),
        "mongo_db":      os.getenv("MONGO_DB", "smart_factory"),
        "total_records": int(os.getenv("TOTAL_RECORDS", "1000000")),
        "batch_size":    int(os.getenv("BATCH_SIZE", "500")),
    }


def build_device_pool(devices_per_ambito=50):
    """Genera un pool fisso di dispositivi per ogni ambito.
    Ogni device ha un reparto e una linea assegnati una volta sola,
    come un sensore reale che non cambia posizione."""
    pool = {}
    for ambito in MISURE:
        pool[ambito] = [
            {
                "device_id": fake.mac_address().upper(),
                "reparto":   random.choice(REPARTI),
                "linea":     random.choice(LINEE),
            }
            for _ in range(devices_per_ambito)
        ]
    return pool


def generate_message(device, ambito, tipo_spec):
    lo, hi = tipo_spec["range"]
    if tipo_spec["unita"] in INTEGER_UNITS:
        valore = random.randint(int(lo), int(hi))
    else:
        valore = round(random.uniform(lo, hi), 2)
    return {
        "device_id": device["device_id"],
        "ambito":    ambito,
        "timestamp": datetime.now(timezone.utc).isoformat(),
        "tipo":      tipo_spec["tipo"],
        "valore":    valore,
        "unita":     tipo_spec["unita"],
        "reparto":   device["reparto"],
        "linea":     device["linea"],
    }


def run_producer(cfg):
    try:
        kafka_producer = KafkaProducer(
            bootstrap_servers=cfg["kafka_servers"],
            value_serializer=lambda v: json.dumps(v).encode("utf-8"),
            key_serializer=lambda k: k.encode("utf-8"),
        )
    except Exception as e:
        raise RuntimeError(f"Connessione a Kafka fallita ({cfg['kafka_servers']}): {e}") from e

    try:
        mongo_client = MongoClient(cfg["mongo_uri"], serverSelectionTimeoutMS=5000)
        mongo_client.server_info()
    except Exception as e:
        raise RuntimeError(f"Connessione a MongoDB fallita ({cfg['mongo_uri']}): {e}") from e

    collection = mongo_client[cfg["mongo_db"]]["raw_telemetry"]
    collection.create_index([("device_id", ASCENDING), ("timestamp", ASCENDING)])

    device_pool = build_device_pool()
    ambiti = list(MISURE.keys())
    total = cfg["total_records"]
    batch_size = cfg["batch_size"]
    buffer = []
    start = time.perf_counter()

    for i in range(total):
        ambito = random.choice(ambiti)
        tipo_spec = random.choice(MISURE[ambito])
        device = random.choice(device_pool[ambito])
        msg = generate_message(device, ambito, tipo_spec)

        # pubblica su Kafka usando device_id come chiave di partizione
        kafka_producer.send(TOPICS[ambito], key=msg["device_id"], value=msg)
        buffer.append(msg)

        if len(buffer) >= batch_size:
            collection.insert_many(buffer, ordered=False)
            buffer = []

        if (i + 1) % 10_000 == 0:
            elapsed = time.perf_counter() - start
            print(f"[{i + 1:>10,}]  {(i + 1) / elapsed:>10,.0f} rec/s")

    # flush del buffer residuo
    if buffer:
        collection.insert_many(buffer, ordered=False)

    kafka_producer.flush()
    elapsed = time.perf_counter() - start
    print(f"\nDone: {total:,} record in {elapsed:.1f}s — media {total / elapsed:,.0f} rec/s")

    kafka_producer.close()
    mongo_client.close()


if __name__ == "__main__":
    config = load_config()
    print(f"Producer avviato — {config['total_records']:,} record, batch={config['batch_size']}")
    run_producer(config)
