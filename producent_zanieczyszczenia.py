import json
import time
from pathlib import Path

import pandas as pd
from kafka import KafkaProducer

# --- Kafka Producer --------------------------------------------------------
producer = KafkaProducer(
    bootstrap_servers='broker:9092',
    value_serializer=lambda v: json.dumps(v).encode('utf-8')
)
TOPIC = "pollution"

# --- Pliki z danymi --------------------------------------------------------
DATA_DIR = Path("/home/jovyan/notebooks/data")
FILES = {
    "PM10":  "2023_PM10_1g.xlsx",
    "PM2.5": "2023_PM25_1g.xlsx",
    "NO2":   "2023_NO2_1g.xlsx",
    "O3":    "2023_O3_1g.xlsx",
    "SO2":   "2023_SO2_1g.xlsx"
}

def load_and_flatten(path: Path, pollutant: str) -> pd.DataFrame:
    # 1) Wczytaj cały arkusz bez nagłówków
    raw = pd.read_excel(path, header=None, dtype=str)

    # 2) Znajdź indeks wiersza, w którym w pierwszej kolumnie jest "Kod stanowiska"
    idx = raw.index[raw.iloc[:,0] == "Kod stanowiska"]
    if len(idx) != 1:
        raise ValueError(f"Nie znalazłem dokładnie jednego wiersza 'Kod stanowiska' w {path}")
    idx = idx[0]

    # 3) Wyciągnij stacje z tego wiersza (od kolumny 1 do końca)
    station_codes = raw.iloc[idx, 1:].tolist()

    # 4) Dane zaczynają się od wiersza idx+1
    data = raw.iloc[idx+1 :, :].copy()
    data.columns = ["timestamp"] + station_codes

    # 5) Odfiltruj puste timestampy i zamień na datetime
    data = data.dropna(subset=["timestamp"])
    data["timestamp"] = pd.to_datetime(data["timestamp"], dayfirst=True, errors="coerce")
    data = data.dropna(subset=["timestamp"])

    # 6) Rozwiń „długi” format: każda stacja jako oddzielny wiersz
    df = data.melt(
        id_vars=["timestamp"],
        value_vars=station_codes,
        var_name="station_code",
        value_name="value"
    ).dropna(subset=["value"])

    # 7) Dodaj metadane
    df["pollutant"] = pollutant
    df["unit"]      = "µg/m³"
    # konwersja wartości na float
    df["value"]     = df["value"].str.replace(",", ".").astype(float)

    return df

def build_dataset() -> pd.DataFrame:
    # Zbierz wszystkie wskaźniki
    frames = []
    for pol, fname in FILES.items():
        path = DATA_DIR / fname
        frames.append(load_and_flatten(path, pol))
    return pd.concat(frames, ignore_index=True).sort_values("timestamp")

if __name__ == "__main__":
    data = build_dataset()
    grouped = data.groupby("timestamp")

    print(f"[Producer] Przygotowano {len(grouped)} pakietów godzinnych…")
    for ts, group in grouped:
        iso_ts = ts.isoformat()
        print(f"[Producer] → godzina {iso_ts}, {len(group)} pomiarów")
        for _, row in group.iterrows():
            payload = {
                "timestamp":    iso_ts,
                "station_code": row.station_code,
                "pollutant":    row.pollutant,
                "unit":         row.unit,
                "value":        row.value
            }
            producer.send(TOPIC, payload)
            print(f"   • {payload}")
        # symulacja: 5 s przerwy między godzinami
        time.sleep(5)

    print("[Producer] Wszystkie dane zostały wysłane.")
