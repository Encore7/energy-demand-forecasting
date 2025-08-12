import io
import os
import zipfile

import pandas as pd
import requests
from sqlalchemy import create_engine

DB_URL = os.getenv(
    "DATABASE_URL", "postgresql://postgres:postgres@localhost:5432/forecasting"
)
ENGINE = create_engine(DB_URL)

# OPSD time series (load for Germany (DE))
OPSD_ZIP = "https://data.open-power-system-data.org/time_series/2024-10-01/time_series_60min_singleindex.csv.zip"


def main():
    r = requests.get(OPSD_ZIP, timeout=60)
    z = zipfile.ZipFile(io.BytesIO(r.content))
    csv_name = [n for n in z.namelist() if n.endswith(".csv")][0]
    df = pd.read_csv(z.open(csv_name), parse_dates=["utc_timestamp"])

    keep = df[["utc_timestamp", "load_actual_de"]].rename(
        columns={"utc_timestamp": "timestamp", "load_actual_de": "demand_mwh"}
    )
    keep["region"] = "DE_total"
    keep = keep.dropna().sort_values("timestamp")

    with ENGINE.begin() as conn:
        conn.exec_driver_sql(
            """
            create table if not exists raw_energy(
                region text,
                timestamp timestamptz,
                demand_mwh double precision
            );
            delete from raw_energy where region='DE_total';
        """
        )
        keep.to_sql("raw_energy", conn, if_exists="append", index=False)

    print(f"[OPSD] rows loaded: {len(keep)}")


if __name__ == "__main__":
    main()
