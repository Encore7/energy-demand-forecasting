from __future__ import annotations

import os
from datetime import date, timedelta

import numpy as np
import pandas as pd
from sqlalchemy import create_engine


def main() -> None:
    run_date_str = os.environ["RUN_DATE"]
    run_date = date.fromisoformat(run_date_str)
    target_date = run_date + timedelta(days=1)

    forecast_store_dsn = os.environ["FORECAST_STORE_DSN"]
    engine = create_engine(forecast_store_dsn)

    # Join forecasts + actuals for a given target_date
    query = """
    SELECT
      f.event_ts_utc,
      f.series_id,
      f.y_hat_mw,
      a.load_mw
    FROM day_ahead_forecasts f
    JOIN day_ahead_actuals a
      ON f.event_ts_utc = a.event_ts_utc
     AND f.series_id = a.series_id
    WHERE f.target_date = %(target_date)s
    ORDER BY f.event_ts_utc
    """

    df = pd.read_sql(query, engine, params={"target_date": target_date})
    if df.empty:
        raise RuntimeError(
            f"No joined forecast+actual rows for target_date={target_date}"
        )

    y_true = df["load_mw"].to_numpy()
    y_pred = df["y_hat_mw"].to_numpy()

    # basic MAPE (guard zero loads)
    mask = y_true != 0
    mape = np.mean(np.abs((y_true[mask] - y_pred[mask]) / y_true[mask])) * 100.0
    mae = np.mean(np.abs(y_true - y_pred))

    metrics_df = pd.DataFrame(
        {
            "target_date": [target_date],
            "series_id": ["DE"],
            "mape": [mape],
            "mae": [mae],
            "created_at_utc": [pd.Timestamp.utcnow()],
        }
    )

    metrics_df.to_sql(
        "day_ahead_metrics",
        engine,
        if_exists="append",
        index=False,
    )

    print(f"[metrics] target_date={target_date}, MAPE={mape:.2f}, MAE={mae:.2f}")


if __name__ == "__main__":
    main()
