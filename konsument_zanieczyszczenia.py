import json
import sqlite3
from kafka import KafkaConsumer

DB = "air_quality.db"
TOPIC = "pollution"

def ensure_table(name: str):
    """Utwórz tabelę, jeśli nie istnieje."""
    with sqlite3.connect(DB) as conn:
        c = conn.cursor()
        c.execute(f"""
            CREATE TABLE IF NOT EXISTS {name} (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                timestamp TEXT,
                station_code TEXT,
                value REAL,
                unit TEXT
            )
        """)
        conn.commit()

def insert(name: str, ts: str, code: str, value: float, unit: str):
    """Wstaw rekord do tabeli."""
    with sqlite3.connect(DB) as conn:
        c = conn.cursor()
        c.execute(f"""
            INSERT INTO {name} (timestamp, station_code, value, unit)
            VALUES (?, ?, ?, ?)
        """, (ts, code, value, unit))
        conn.commit()

# Konfiguracja Kafka Consumer
consumer = KafkaConsumer(
    TOPIC,
    bootstrap_servers='broker:9092',
    value_deserializer=lambda m: json.loads(m.decode('utf-8')),
    auto_offset_reset='earliest',
    enable_auto_commit=True,
    group_id='aq-group'
)

print(f"[Consumer] Nasłuchiwanie topicu '{TOPIC}'…")
for msg in consumer:
    rec = msg.value
    # walidacja pól
    if not all(k in rec for k in ("pollutant", "timestamp", "station_code", "value")):
        print(f"[Consumer] ⚠️ Brakuje kluczowych pól, pomijam: {rec}")
        continue

    # tabelka np. PM2.5 → PM25
    table = rec["pollutant"].replace(".", "").upper()
    ensure_table(table)

    try:
        insert(
            table,
            rec["timestamp"],
            rec["station_code"],
            float(rec["value"]),
            rec.get("unit", "")
        )
        print(f"[Consumer] Zapisano do {table}: {rec['timestamp']} | {rec['station_code']} = {rec['value']} {rec.get('unit','')}")
    except Exception as e:
        print(f"[Consumer] ❌ Błąd zapisu do {table}: {e}")
