# Smart Factory — Risposte alle 12 Domande di Business

**Progetto:** Pipeline IoT in tempo reale — Smart Factory  
**Corso:** ITS Corso Big Data  
**Studenti:** Riccardo Zozzolotto (Parte A — Producer) · Simone Breda (Parte B — Consumer)  
**Data esecuzione:** 2026-06-08  
**Volume:** 1.000.000 record

---

## Setup del run

| Parametro | Valore |
|---|---|
| Record generati | 1.000.000 |
| Batch size (Producer) | 500 |
| Batch size (Consumer) | 500 |
| Consumer group | `smart-factory-consumers` |
| Database | `smart_factory` |

---

## Q1 — Distribuzione dei record per ambito

> Quanti record sono stati generati in totale e come si distribuiscono tra i quattro ambiti?

**Query:**
```js
db.raw_telemetry.aggregate([
  { $group: { _id: "$ambito", count: { $sum: 1 } } },
  { $sort: { count: -1 } }
])
```

**Risultato:**

| Ambito | Record | % |
|---|---|---|
| ambientale | 250.883 | 25.1% |
| qualita | 249.905 | 25.0% |
| macchinari | 249.883 | 25.0% |
| logistica | 249.329 | 24.9% |
| **Totale** | **1.000.000** | **100%** |

La distribuzione è uniforme tra i quattro ambiti, come atteso dalla generazione casuale del Producer.

---

## Q2 — Top 5 dispositivi per volume di messaggi

> Quali sono i 5 dispositivi che hanno prodotto il maggior numero di messaggi?

**Query:**
```js
db.raw_telemetry.aggregate([
  { $group: { _id: "$device_id", count: { $sum: 1 } } },
  { $sort: { count: -1 } },
  { $limit: 5 }
])
```

**Risultato:**

| # | Device ID | Messaggi |
|---|---|---|
| 1 | E8:E0:16:B8:1A:A6 | 5.203 |
| 2 | 18:CF:A3:7E:01:F5 | 5.184 |
| 3 | 4A:89:BA:30:63:E7 | 5.158 |
| 4 | 1A:3C:88:24:9F:1E | 5.140 |
| 5 | 92:95:96:94:DA:C6 | 5.137 |

---

## Q3 — Temperatura media e massima del motore per macchinario

> Qual è la temperatura media e massima del motore per ciascun macchinario sull'intera sessione?

**Query:**
```js
db.processed_events.aggregate([
  { $match: { tipo: "temperatura_motore" } },
  { $group: {
      _id: "$device_id",
      media: { $avg: "$valore" },
      massima: { $max: "$valore" },
      count: { $sum: 1 }
  }},
  { $sort: { media: -1 } }
])
```

**Risultato (top 10 per temperatura media):**

| Device ID | Media (°C) | Massima (°C) | Letture |
|---|---|---|---|
| A0:25:E3:D3:96:A9 | 76.1 | 110.0 | 1.227 |
| 98:9B:BE:25:5D:C0 | 76.1 | 109.9 | 1.228 |
| 06:D6:55:D4:8D:93 | 75.9 | 110.0 | 1.216 |
| 3C:D2:D2:4B:FF:9D | 75.9 | 110.0 | 1.246 |
| 3E:A9:2D:AF:EF:B6 | 75.8 | 109.9 | 1.227 |
| AC:80:30:FB:58:5C | 75.6 | 110.0 | 1.244 |
| F6:68:B3:CC:F7:4B | 75.5 | 110.0 | 1.227 |
| 36:BD:E0:21:96:DB | 75.5 | 110.0 | 1.238 |
| 56:31:2E:BA:AD:B7 | 75.4 | 109.8 | 1.172 |
| 4A:87:96:7E:E4:95 | 75.4 | 110.0 | 1.206 |

La soglia di allarme è 85°C. Le medie si attestano intorno ai 75°C ma i picchi raggiungono il massimo del range (110°C), indicando eventi critici sporadici.

---

## Q4 — Letture oltre soglia di allarme

> Quante letture hanno superato la soglia di allarme e per quali tipi di misura?

**Query:**
```js
db.alerts.aggregate([
  { $group: { _id: "$tipo", count: { $sum: 1 } } },
  { $sort: { count: -1 } }
])
```

**Risultato:**

| Tipo misura | Alert | Soglia |
|---|---|---|
| co2 | 34.863 | > 1000 ppm |
| scarti | 30.714 | > 50 pezzi |
| vibrazione | 23.452 | > 7.5 mm/s |
| temperatura_motore | 22.185 | > 85 °C |
| consumo_energetico | 16.379 | > 150 kWh |
| temperatura_ambientale | 13.831 | > 40 °C |
| umidita | 11.235 | > 85 % |
| rpm | 9.114 | > 3500 rpm |
| **Totale** | **161.773** | |

**Alert rate: 16.2%** dei messaggi processati. Il CO₂ è il sensore più critico, seguito dagli scarti di produzione.

---

## Q5 — Consumo energetico totale per reparto

> Qual è il consumo energetico totale (kWh) per reparto?

**Query:**
```js
db.processed_events.aggregate([
  { $match: { tipo: "consumo_energetico" } },
  { $group: {
      _id: "$reparto",
      totale_kwh: { $sum: "$valore" },
      media_kwh: { $avg: "$valore" }
  }},
  { $sort: { totale_kwh: -1 } }
])
```

**Risultato:**

| Reparto | Totale (kWh) | Media (kWh) |
|---|---|---|
| Verniciatura | 1.576.565.7 | 105.2 |
| Controllo Qualita | 1.546.687.9 | 104.0 |
| Magazzino | 1.195.233.0 | 104.8 |
| Assemblaggio | 1.176.974.2 | 104.3 |
| Spedizione | 1.057.706.8 | 104.9 |

Verniciatura e Controllo Qualità sono i reparti con il maggior consumo energetico totale. La media per misura è uniforme (~104-105 kWh).

---

## Q6 — Finestra temporale con più allarmi

> In quale finestra temporale si concentra il maggior numero di allarmi?

**Query:**
```js
db.alerts.aggregate([
  { $addFields: {
      minute: { $dateToString: {
        format: "%Y-%m-%dT%H:%M",
        date: { $dateFromString: { dateString: "$timestamp" } }
      }}
  }},
  { $group: { _id: "$minute", count: { $sum: 1 } } },
  { $sort: { count: -1 } },
  { $limit: 5 }
])
```

**Risultato:**

| Finestra (minuto) | Alert |
|---|---|
| 2026-06-08T13:27 | 132.655 |
| 2026-06-08T13:28 | 29.118 |

Il picco di allarmi si concentra nel primo minuto di esecuzione (13:27), quando il Producer stava generando la maggior parte dei messaggi. Distribuzione attesa per una generazione sincrona in pochi minuti.

---

## Q7 — Tasso di scarto per linea di produzione

> Qual è il tasso di scarto (pezzi difettosi sul totale prodotto) per ciascuna linea?

**Query:**
```js
db.processed_events.aggregate([
  { $match: { tipo: { $in: ["pezzi_prodotti", "scarti"] } } },
  { $group: { _id: { linea: "$linea", tipo: "$tipo" }, totale: { $sum: "$valore" } } },
  { $group: { _id: "$_id.linea", valori: { $push: { tipo: "$_id.tipo", totale: "$totale" } } } }
])
```

**Risultato:**

| Linea | Pezzi prodotti | Scarti | Tasso scarto |
|---|---|---|---|
| L1 | 5.000.797 | 800.138 | 16.00% |
| L3 | 5.824.919 | 929.147 | 15.95% |
| L4 | 4.546.902 | 723.407 | 15.91% |
| L2 | 5.507.235 | 864.179 | 15.69% |

Il tasso di scarto è omogeneo tra le linee (~16%), coerente con i range sintetici definiti nel contratto dati (0-80 scarti su 0-500 pezzi).

---

## Q8 — Vibrazione media e di picco per macchinario

> Qual è la vibrazione media e di picco per ciascun macchinario? Ci sono macchine fuori norma?

**Query:**
```js
db.processed_events.aggregate([
  { $match: { tipo: "vibrazione" } },
  { $group: {
      _id: "$device_id",
      media: { $avg: "$valore" },
      picco: { $max: "$valore" },
      count: { $sum: 1 }
  }},
  { $sort: { picco: -1 } },
  { $limit: 10 }
])
```

**Risultato (top 10 per picco):**

| Device ID | Media (mm/s) | Picco (mm/s) | Fuori norma |
|---|---|---|---|
| FC:30:69:54:80:F4 | 5.97 | 12.00 | ⚠ sì |
| 0E:B2:4C:13:8E:5F | 6.01 | 12.00 | ⚠ sì |
| 3C:D2:D2:4B:FF:9D | 5.93 | 12.00 | ⚠ sì |
| 30:AB:D3:CC:C1:3A | 6.05 | 12.00 | ⚠ sì |
| 9E:D7:CA:9B:B2:26 | 6.02 | 12.00 | ⚠ sì |
| DA:48:2B:E5:9C:81 | 6.14 | 12.00 | ⚠ sì |
| 56:7D:04:35:17:4D | 6.15 | 12.00 | ⚠ sì |
| 80:A3:E7:9A:2A:5C | 5.93 | 12.00 | ⚠ sì |
| 36:BD:E0:21:96:DB | 6.23 | 12.00 | ⚠ sì |
| 56:31:2E:BA:AD:B7 | 6.11 | 12.00 | ⚠ sì |

Soglia: 7.5 mm/s. Tutti i macchinari mostrano picchi al massimo del range (12 mm/s), con medie intorno a 6 mm/s — al di sopra del normale operativo.

---

## Q9 — Throughput del Producer

> Qual è il throughput raggiunto dal Producer (record al secondo) e quanto tempo è servito?

| Metrica | Valore |
|---|---|
| Record totali | 1.000.000 |
| Tempo totale | 35.9 s |
| Throughput medio | **27.893 rec/s** |
| Throughput di picco | ~29.700 rec/s |

Il Producer raggiunge il regime dopo circa 100k record, stabilizzandosi intorno a 28-29k rec/s. Il leggero calo finale è dovuto alla pressione di back-pressure di Kafka sotto carico sostenuto.

---

## Q10 — Scarto tra raw_telemetry e processed_events

> C'è uno scarto tra i record grezzi e quelli elaborati? Quanti messaggi sono stati scartati in validazione?

| Collection | Documenti |
|---|---|
| `raw_telemetry` | 1.000.000 |
| `processed_events` | 999.501 |
| **Scarto** | **499 (0.05%)** |

**Causa:** I 499 messaggi mancanti non sono stati scartati dalla validazione (scartati = 0 sul run da 1M) ma erano nel buffer in-memory del Consumer al momento dell'interruzione del test preliminare. I Kafka offset erano già stati committati, quindi non è stato possibile riprocessarli.

**Lezione:** In produzione si usa `enable.auto.commit=false` con commit manuale post-flush per evitare la perdita del buffer.

---

## Q11 — CO₂ medio per reparto

> Qual è il livello medio di CO₂ per reparto e quali reparti superano più spesso la soglia?

**Query:**
```js
db.processed_events.aggregate([
  { $match: { tipo: "co2" } },
  { $group: {
      _id: "$reparto",
      media_co2: { $avg: "$valore" },
      max_co2: { $max: "$valore" },
      superamenti_soglia: { $sum: { $cond: [{ $gt: ["$valore", 1000] }, 1, 0] } }
  }},
  { $sort: { media_co2: -1 } }
])
```

**Risultato:**

| Reparto | Media CO₂ (ppm) | Max (ppm) | Superamenti soglia (> 1000 ppm) |
|---|---|---|---|
| Spedizione | 905.8 | 1500.0 | 7.788 |
| Verniciatura | 899.8 | 1500.0 | 8.415 |
| Assemblaggio | 899.3 | 1500.0 | 5.498 |
| Magazzino | 898.7 | 1499.8 | 6.992 |
| Controllo Qualita | 898.3 | 1499.8 | 6.170 |

Soglia: 1000 ppm. **Verniciatura** è il reparto con più superamenti assoluti (8.415), seguito da Spedizione. Tutti i reparti mostrano medie intorno a 900 ppm, con picchi al limite del range.

---

## Q12 — Trend della temperatura media nel tempo

> Guardando le aggregazioni a finestra temporale, qual è l'andamento della temperatura media? Si individuano picchi o anomalie?

**Query:**
```js
db.aggregations.aggregate([
  { $match: { tipo: "temperatura_motore" } },
  { $group: {
      _id: "$window_start",
      media_globale: { $avg: "$mean_valore" },
      picco: { $max: "$mean_valore" },
      num_macchine: { $sum: 1 }
  }},
  { $sort: { _id: 1 } }
])
```

**Risultato:**

| Finestra | Media globale (°C) | Picco (°C) | Macchine attive |
|---|---|---|---|
| 2026-06-08 13:27 | 74.9 | 76.0 | 50 |
| 2026-06-08 13:28 | 75.0 | 79.9 | 50 |

Il run si è concluso in meno di 2 minuti, quindi le finestre temporali sono solo 2. La temperatura media globale è stabile intorno a 75°C con un lieve aumento nella seconda finestra (+0.1°C media, +3.9°C picco). Con un run più lungo si osserverebbero trend e anomalie più significativi.

---

## Metriche di sintesi

| Metrica | Valore |
|---|---|
| Record prodotti | 1.000.000 |
| Throughput Producer | 27.893 rec/s |
| Tempo Producer | 35.9 s |
| Record processati | 999.501 (99.95%) |
| Messaggi scartati in validazione | 0 |
| Alert generati | 161.773 (16.2%) |
| Finestre di aggregazione | 1.300 |
| Alert rate per tipo più alto | CO₂: 34.863 (21.5% degli alert) |
| Reparto con più consumo | Verniciatura: 1.576.565 kWh |
| Linea con tasso scarto più alto | L1: 16.00% |
