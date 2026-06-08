# Smart Factory — IoT Telemetry Pipeline

Pipeline IoT in tempo reale che simula la telemetria di una fabbrica industriale.  
Producer/Consumer su **Apache Kafka**, persistenza su **MongoDB**, sviluppato in coppia con ruoli distinti.

---

## Architettura

```
┌─────────────────────────────────────────────────────────────────────┐
│                        SMART FACTORY                                │
│                                                                     │
│  ┌──────────┐     ┌──────────┐     ┌─────────────────────────────┐ │
│  │ Sensori  │     │          │     │       Apache Kafka           │ │
│  │simulati  │────▶│Producer  │────▶│  iot.ambientale             │ │
│  │          │     │(Parte A) │     │  iot.macchinari             │ │
│  │ Faker +  │     │          │     │  iot.logistica              │ │
│  │ random   │     │ Salva ▼  │     │  iot.qualita                │ │
│  └──────────┘     └──────────┘     └────────────┬────────────────┘ │
│                        │                        │                   │
│                        ▼                        ▼                   │
│               ┌─────────────────┐     ┌─────────────────┐         │
│               │    MongoDB      │     │    Consumer      │         │
│               │                 │     │    (Parte B)     │         │
│               │ raw_telemetry   │     │                  │         │
│               │  (staging)      │     │ valida, arricch. │         │
│               └─────────────────┘     │ aggrega          │         │
│                                       └────────┬─────────┘         │
│                                                │                   │
│                                                ▼                   │
│                                   ┌─────────────────────────┐      │
│                                   │        MongoDB          │      │
│                                   │                         │      │
│                                   │  processed_events       │      │
│                                   │  alerts                 │      │
│                                   │  aggregations           │      │
│                                   │     (serving)           │      │
│                                   └─────────────────────────┘      │
└─────────────────────────────────────────────────────────────────────┘
```

---

## Divisione dei ruoli

| Ruolo | Branch | Responsabilità |
|---|---|---|
| **Studente A — Producer** | `feature/part-a-producer` | Genera milioni di record sintetici, pubblica su Kafka, salva il grezzo su MongoDB (`raw_telemetry`) |
| **Studente B — Consumer** | `feature/part-b-consumer` | Legge da Kafka, valida, arricchisce, salva su MongoDB (`processed_events`, `alerts`, `aggregations`) |

---

## Contratto dati

Ogni messaggio che viaggia su Kafka rispetta questo schema JSON.  
**Entrambi i ruoli devono conformarsi — nessuna modifica senza accordo.**

```json
{
  "device_id":  "A1:B2:C3:D4:E5:F6",
  "ambito":     "macchinari",
  "timestamp":  "2026-06-08T09:30:00Z",
  "tipo":       "temperatura_motore",
  "valore":     78.4,
  "unita":      "C",
  "reparto":    "Assemblaggio",
  "linea":      "L2"
}
```

### Campi obbligatori

| Campo | Tipo | Valori ammessi |
|---|---|---|
| `device_id` | string | MAC address (es. `A1:B2:C3:D4:E5:F6`) |
| `ambito` | string | `ambientale` · `macchinari` · `logistica` · `qualita` |
| `timestamp` | string | ISO 8601 UTC |
| `tipo` | string | vedi tabella misure sotto |
| `valore` | number | dipende dal tipo |
| `unita` | string | `C` · `%` · `ppm` · `mm/s` · `rpm` · `kWh` · `deg` · `count` · `bool` |
| `reparto` | string | Assemblaggio · Verniciatura · Magazzino · Controllo Qualita · Spedizione |
| `linea` | string | `L1` · `L2` · `L3` · `L4` |

### Misure per ambito

| Ambito | Tipo | Unità | Range |
|---|---|---|---|
| ambientale | `temperatura_ambientale` | C | 15 – 45 |
| ambientale | `umidita` | % | 20 – 95 |
| ambientale | `co2` | ppm | 300 – 1500 |
| macchinari | `temperatura_motore` | C | 40 – 110 |
| macchinari | `vibrazione` | mm/s | 0.1 – 12.0 |
| macchinari | `rpm` | rpm | 500 – 4000 |
| macchinari | `consumo_energetico` | kWh | 10 – 200 |
| logistica | `posizione_lat` | deg | 45.40 – 45.50 |
| logistica | `posizione_lon` | deg | 9.10 – 9.20 |
| logistica | `rfid_lettura` | count | 0 · 1 |
| qualita | `pezzi_prodotti` | count | 0 – 500 |
| qualita | `scarti` | count | 0 – 80 |
| qualita | `esito_controllo` | bool | 0 · 1 |

### Soglie di allarme

| Tipo | Condizione | Soglia |
|---|---|---|
| `temperatura_motore` | > | 85.0 C |
| `vibrazione` | > | 7.5 mm/s |
| `consumo_energetico` | > | 150.0 kWh |
| `temperatura_ambientale` | > | 40.0 C |
| `umidita` | > | 85.0 % |
| `co2` | > | 1000 ppm |
| `rpm` | > | 3500 rpm |
| `scarti` | > | 50 count |

---

## Struttura repository

```
Esercitazione_SmartFactory_IoT_BigData/
│
├── contract/
│   └── data_contract.json       # contratto dati condiviso (fonte di verità)
│
├── producer/
│   └── producer.py              # Parte A — IoT Gateway / Producer
│
├── consumer/
│   └── consumer.py              # Parte B — Stream Processor / Consumer
│
├── .env.example                 # template variabili d'ambiente
├── .gitignore
├── requirements.txt
└── README.md
```

---

## MongoDB — Collections

| Collection | Owner | Pattern | Contenuto |
|---|---|---|---|
| `raw_telemetry` | Parte A | staging | Messaggi grezzi, così come generati |
| `processed_events` | Parte B | serving | Messaggi validati e arricchiti (`is_alert`, `fascia_oraria`) |
| `alerts` | Parte B | serving | Solo misure che superano la soglia |
| `aggregations` | Parte B | serving | Media per `(device_id, tipo)` a finestra di 1 minuto |

Database: `smart_factory`  
Indici: `device_id + timestamp` su tutte le collection (tranne `aggregations`: `device_id + window_start`).

---

## Setup

### Prerequisiti

- Python 3.10+
- Apache Kafka in esecuzione (fornito dal docente)
- MongoDB in esecuzione (fornito dal docente)

### Installazione

```bash
git clone https://github.com/zozzy04/Esercitazione_SmartFactory_IoT_BigData.git
cd Esercitazione_SmartFactory_IoT_BigData

pip install -r requirements.txt

cp .env.example .env
# Modifica .env con gli indirizzi Kafka e MongoDB del laboratorio
```

### Variabili d'ambiente (`.env`)

```env
KAFKA_BOOTSTRAP_SERVERS=localhost:9092
MONGO_URI=mongodb://localhost:27017
MONGO_DB=smart_factory

# Solo per il Producer
TOTAL_RECORDS=1000000
BATCH_SIZE=500
```

---

## Esecuzione

I due applicativi girano in parallelo, ciascuno nel proprio terminale.

**Terminale 1 — Producer (Parte A):**
```bash
python producer/producer.py
```

Output atteso:
```
Producer starting — 1,000,000 records, batch=500
[    10,000]      85,432 rec/s
[    20,000]      87,210 rec/s
...
Done: 1,000,000 records in 11.7s — avg 85,470 rec/s
```

**Terminale 2 — Consumer (Parte B):**
```bash
python consumer/consumer.py
```

---

## Le 12 domande di business

Al termine, entrambi i ruoli rispondono interrogando MongoDB con l'aggregation framework.

| # | Domanda |
|---|---|
| 1 | Quanti record totali e come si distribuiscono tra i 4 ambiti? |
| 2 | Quali sono i 5 dispositivi con più messaggi? |
| 3 | Temperatura media e massima del motore per ciascun macchinario? |
| 4 | Quante letture hanno superato la soglia di allarme e per quali dispositivi? |
| 5 | Consumo energetico totale (kWh) per reparto? |
| 6 | In quale finestra temporale (al minuto) si concentra il maggior numero di allarmi? |
| 7 | Tasso di scarto (pezzi difettosi / totale prodotto) per linea di produzione? |
| 8 | Vibrazione media e di picco per ciascun macchinario? |
| 9 | Throughput del Producer (record/secondo) e tempo totale? |
| 10 | Scarto tra record in `raw_telemetry` e `processed_events` — quanti scartati in validazione? |
| 11 | Livello medio di CO₂ per reparto — quali superano più spesso la soglia? |
| 12 | Trend della temperatura media di una macchina nel tempo — ci sono picchi o anomalie? |

---

## Tech stack

| Tecnologia | Ruolo |
|---|---|
| **Apache Kafka** | Message broker — disaccoppia Producer e Consumer |
| **MongoDB** | Persistenza — staging (raw) e serving (elaborato) |
| **kafka-python** | Client Kafka per Python |
| **pymongo** | Client MongoDB per Python |
| **Faker** | Generazione dati sintetici realistici |
| **python-dotenv** | Gestione configurazione via `.env` |
