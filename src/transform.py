import os
import pandas as pd
from datetime import datetime
from glob import glob

# Paths
ROOT = os.path.dirname(os.path.dirname(__file__))
RAW_DIR = os.path.join(ROOT, "data", "raw")
PROCESSED_DIR = os.path.join(ROOT, "data", "processed")
os.makedirs(PROCESSED_DIR, exist_ok=True)


def get_latest_raw():

    files = glob(os.path.join(RAW_DIR, "weather_*.csv"))
    if not files:
        raise FileNotFoundError("No hay archivos raw en data/raw/")
    latest_file = max(files, key=os.path.getmtime)
    return latest_file


def transform_raw(df: pd.DataFrame) -> pd.DataFrame:

    # Dates
    df["obs_timestamp_utc"] = pd.to_datetime(df["obs_timestamp_utc"], utc=True)
    df["obs_timestamp_local"] = pd.to_datetime(df["obs_timestamp_local"])
    
    # Numeric types
    numeric_cols = ["temp", "feels_like", "humidity", "wind_speed", "visibility",
                    "pressure", "cloudiness", "rain_mm"]
    for col in numeric_cols:
        df[col] = pd.to_numeric(df[col], errors="coerce")

    # weather_main as string
    df["weather_main"] = df["weather_main"].astype(str)

    return df


def main():
    latest_csv = get_latest_raw()
    print("Latest raw file:", latest_csv)

    df = pd.read_csv(latest_csv)
    df_transformed = transform_raw(df)

    local_ts = df_transformed["obs_timestamp_local"].iloc[0]
    fname = f"weather_processed_{pd.to_datetime(local_ts).strftime('%Y%m%d_%H%M')}.csv"
    path = os.path.join(PROCESSED_DIR, fname)
    df_transformed.to_csv(path, index=False)
    print("Saved processed CSV to:", path)

    # TERMINAL NOTIFICATION
    preview = df_transformed.iloc[0].to_dict()
    print("Preview transformed row:")
    print(f"  UTC: {preview['obs_timestamp_utc']}")
    print(f"  Local: {preview['obs_timestamp_local']}")
    print(f"  Temp: {preview['temp']}")
    print(f"  Weather: {preview['weather_main']}")
    print(f"  Humidity: {preview['humidity']}")
    print(f"  Rain mm: {preview['rain_mm']}")


# SEPARATE FUNCTION FOR MAIN PIPELINE EXECUTION
def transform(latest_raw):

    df = pd.read_csv(latest_raw)
    df_transformed = transform_raw(df)

    local_ts = df_transformed["obs_timestamp_local"].iloc[0]
    fname = f"weather_processed_{pd.to_datetime(local_ts).strftime('%Y%m%d_%H%M')}.csv"
    path = os.path.join(PROCESSED_DIR, fname)
    df_transformed.to_csv(path, index=False)
    return path


if __name__ == "__main__":
    main()


