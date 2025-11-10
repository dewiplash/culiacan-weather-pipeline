import os
import csv
from datetime import datetime
from dotenv import load_dotenv
import requests
import pytz


load_dotenv()

API_KEY = os.getenv("OPENWEATHER_API_KEY")


# Culiacan coordinates
LAT = 24.8091
LON = -107.3940

#Parameters for API call
URL = "https://api.openweathermap.org/data/2.5/weather"
PARAMS = {
    "lat": LAT,
    "lon": LON,
    "appid": API_KEY,
    "units": "metric"
}

# Paths
ROOT = os.path.dirname(os.path.dirname(__file__)) 
RAW_DIR = os.path.join(ROOT, "data", "raw")
os.makedirs(RAW_DIR, exist_ok=True)

# Timezones
TZ_UTC = pytz.utc
TZ_LOCAL = pytz.timezone("America/Mazatlan")


def safe_get(dct, *keys, default=None):

    cur = dct
    for k in keys:
        if not isinstance(cur, dict):
            return default
        cur = cur.get(k, default)
        if cur is default:
            return default
    return cur


def fetch_weather(): #API CALL

    resp = requests.get(URL, params=PARAMS, timeout=20)
    return resp.status_code, resp.json()


def build_record(data):

    dt_unix = safe_get(data, "dt")
    if dt_unix is None:
        obs_utc = datetime.now(TZ_UTC)
    else:
        obs_utc = datetime.fromtimestamp(int(dt_unix), TZ_UTC)

    obs_local = obs_utc.astimezone(TZ_LOCAL)


    main = data.get("main", {}) if isinstance(data.get("main", {}), dict) else {}
    wind = data.get("wind", {}) if isinstance(data.get("wind", {}), dict) else {}

    temp = safe_get(main, "temp")
    feels_like = safe_get(main, "feels_like")
    humidity = safe_get(main, "humidity")
    pressure = safe_get(main, "pressure")
    wind_speed = safe_get(wind, "speed")
    visibility = safe_get(data, "visibility")
    cloudiness = safe_get(safe_get(data, "clouds", default={}), "all")
    weather_main = None
    try:
        weather_main = data.get("weather", [])[0].get("main")
    except Exception:
        weather_main = None


    rain_mm = None
    rain_mm = safe_get(data, "rain", "1h", default=None)
    if rain_mm is None:
        rain_mm = safe_get(data, "rain", "3h", default=None)


    record = {
        "obs_timestamp_utc": obs_utc.isoformat(),
        "obs_timestamp_local": obs_local.isoformat(),
        "temp": temp,
        "feels_like": feels_like,
        "humidity": humidity,
        "wind_speed": wind_speed,
        "visibility": visibility,
        "pressure": pressure,
        "weather_main": weather_main,
        "cloudiness": cloudiness,
        "rain_mm": rain_mm
    }
    return record


def write_csv(record):

    # filename: weather_YYYYMMDD_HHMM.csv using local time
    local_dt = datetime.fromisoformat(record["obs_timestamp_local"])
    fname = f"weather_{local_dt.strftime('%Y%m%d_%H%M')}.csv"
    path = os.path.join(RAW_DIR, fname)


    headers = [
        "obs_timestamp_utc",
        "obs_timestamp_local",
        "temp",
        "feels_like",
        "humidity",
        "wind_speed",
        "visibility",
        "pressure",
        "weather_main",
        "cloudiness",
        "rain_mm"
    ]


    with open(path, mode="w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=headers)
        writer.writeheader()
        writer.writerow(record)

    return path


def main():

    print("Requesting OpenWeather API...")
    status, payload = fetch_weather()
    print("Status Code:", status)

    if status != 200:
        print("API error response (no se guardará snapshot):")
        print(payload)
        raise SystemExit(f"OpenWeather API returned {status}")

    record = build_record(payload)
    csv_path = write_csv(record)
    print("Saved raw snapshot to:", csv_path)
    # Terminal notification
    print("Preview:")
    print(f"  UTC: {record['obs_timestamp_utc']}")
    print(f"  Local: {record['obs_timestamp_local']}")
    print(f"  Temp: {record['temp']}")
    print(f"  Weather: {record['weather_main']}")
    print(f"  Humidity: {record['humidity']}")
    if record["rain_mm"] is not None:
        print(f"  Rain mm (1h/3h): {record['rain_mm']}")


# SEPARATE FUNCTION FOR MAIN PIPELINE EXECUTION
def extract():

    status, payload = fetch_weather()

    if status != 200:
        print("API error response (no se guardará snapshot):")
        print(payload)
        raise SystemExit(f"OpenWeather API returned {status}")

    record = build_record(payload)
    csv_path = write_csv(record)
    return csv_path


if __name__ == "__main__":
    main()