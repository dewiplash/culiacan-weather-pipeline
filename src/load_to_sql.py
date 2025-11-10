import os
import pandas as pd
from glob import glob
import pyodbc

# Paths
ROOT = os.path.dirname(os.path.dirname(__file__))
PROCESSED_DIR = os.path.join(ROOT, "data", "processed")


# SQL Connection parameters
conn_str = (
    "DRIVER={ODBC Driver 17 for SQL Server};"
    "SERVER=localhost;"
    "DATABASE=proyecto;"
    "Trusted_Connection=yes"
    )


#SQL connection
conn = pyodbc.connect(conn_str)
cursor = conn.cursor()



def get_latest_processed():

    files = glob(os.path.join(PROCESSED_DIR, "weather_processed_*.csv"))
    if not files:
        raise FileNotFoundError("No hay archivos processed en data/processed/")
    return max(files, key=os.path.getmtime)


def load_to_sql(df: pd.DataFrame):

    for _, row in df.iterrows():
    # NaN -> None for SQL (will be taken as NULL)
        temp_rain = row["rain_mm"] if pd.notna(row["rain_mm"]) else None
        temp_feels_like = row["feels_like"] if pd.notna(row["feels_like"]) else None
        temp_wind = row["wind_speed"] if pd.notna(row["wind_speed"]) else None
        temp_visibility = row["visibility"] if pd.notna(row["visibility"]) else None
        temp_pressure = row["pressure"] if pd.notna(row["pressure"]) else None
        temp_cloud = row["cloudiness"] if pd.notna(row["cloudiness"]) else None


        cursor.execute("""
            IF NOT EXISTS (SELECT 1 FROM weather_observation WHERE obs_timestamp_utc = ?)
            INSERT INTO weather_observation 
            (obs_timestamp_utc, obs_timestamp_local, temp, feels_like, humidity, wind_speed, visibility, pressure, weather_main, cloudiness, rain_mm)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """,
        row["obs_timestamp_utc"],
        row["obs_timestamp_utc"],
        row["obs_timestamp_local"],
        row["temp"],
        temp_feels_like,
        row["humidity"],
        temp_wind,
        temp_visibility,
        temp_pressure,
        row["weather_main"],
        temp_cloud,
        temp_rain
    )

    conn.commit()


def main():
    
    latest_csv = get_latest_processed()
    print("Loading processed file:", latest_csv)
    df = pd.read_csv(latest_csv)
    load_to_sql(df)
    print(f"{len(df)} row(s) loaded into SQL Server table 'weather_observation'.")



if __name__ == "__main__":
    main()

