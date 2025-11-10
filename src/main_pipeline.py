from extract import extract
from transform import transform
from load_to_sql import get_latest_processed, load_to_sql
import pandas as pd


def run_pipeline():

    print("=== START PIPELINE ===\n")

    # EXTRACT
    print("[1/3] Extracting...")
    raw_csv_path = extract()
    print("Raw file generated:", raw_csv_path, "\n")

    # TRANSFORM
    print("[2/3] Transforming...")
    processed_csv_path = transform(raw_csv_path)
    print("Processed file generated:", processed_csv_path, "\n")

    # LOAD
    print("[3/3] Loading to SQL Server...\n")
    df = pd.read_csv(processed_csv_path)
    load_to_sql(df)

    print("=== PIPELINE FINISHED SUCCESSFULLY ===")


if __name__ == "__main__":
    run_pipeline()
