from __future__ import annotations

import os

import lightgbm as lgb
import mlflow
import mlflow.lightgbm
import numpy as np
import pandas as pd
import pyarrow.parquet as pq
import s3fs
from sklearn.metrics import mean_absolute_error, mean_squared_error

LABEL_COL = "target_load_mw"
META_DROP = {
    "event_ts_utc",
    "dt",
}  # series_id & load_mw can be kept as features if numeric


def _load_parquet_to_df(fs: s3fs.S3FileSystem, bucket: str, key: str) -> pd.DataFrame:
    with fs.open(f"{bucket}/{key}", "rb") as f:
        table = pq.read_table(f)
    return table.to_pandas()


def _select_features(df: pd.DataFrame) -> pd.DataFrame:
    # Features = all numeric columns except label, and excluding obviously non-feature meta cols
    df_num = df.select_dtypes(include=["number"]).copy()

    for col in [LABEL_COL]:
        if col in df_num.columns:
            df_num = df_num.drop(columns=[col])

    for col in META_DROP:
        if col in df_num.columns:
            df_num = df_num.drop(columns=[col])

    return df_num


def _mape(y_true: np.ndarray, y_pred: np.ndarray) -> float:
    denom = np.where(y_true == 0, np.nan, y_true)
    mape = np.nanmean(np.abs((y_true - y_pred) / denom)) * 100.0
    return float(mape)


def main() -> None:
    run_date = os.environ["RUN_DATE"]
    repo = os.environ["LAKEFS_REPO"]
    branch = os.environ["LAKEFS_BRANCH"]

    lakefs_s3_endpoint = os.environ["LAKEFS_S3_ENDPOINT_INTERNAL"]
    aws_key = os.environ["LAKEFS_ACCESS_KEY_ID"]
    aws_secret = os.environ["LAKEFS_SECRET_ACCESS_KEY"]
    region = os.environ["S3_REGION"]

    mlflow_tracking_uri = os.environ["MLFLOW_TRACKING_URI"]
    experiment_name = os.environ.get("MLFLOW_EXPERIMENT_NAME", "day_ahead_lgbm")
    model_name = os.environ.get("MODEL_NAME", "day_ahead_lgbm")

    fs = s3fs.S3FileSystem(
        key=aws_key,
        secret=aws_secret,
        client_kwargs={"endpoint_url": lakefs_s3_endpoint, "region_name": region},
    )

    base_prefix = f"{branch}/artifacts/day_ahead/trainsets/run_date={run_date}/"
    train_key = f"{base_prefix}train.parquet"
    val_key = f"{base_prefix}val.parquet"

    print(f"[train_lgbm] Loading train from s3://{repo}/{train_key}")
    print(f"[train_lgbm] Loading val   from s3://{repo}/{val_key}")

    train_df = _load_parquet_to_df(fs, repo, train_key)
    val_df = _load_parquet_to_df(fs, repo, val_key)

    if LABEL_COL not in train_df.columns or LABEL_COL not in val_df.columns:
        raise RuntimeError(f"Label column '{LABEL_COL}' missing from train/val")

    X_train = _select_features(train_df)
    y_train = train_df[LABEL_COL].values

    X_val = _select_features(val_df)
    y_val = val_df[LABEL_COL].values

    feature_names = list(X_train.columns)
    print(f"[train_lgbm] n_features={len(feature_names)}")

    mlflow.set_tracking_uri(mlflow_tracking_uri)
    mlflow.set_experiment(experiment_name)

    params = {
        "objective": "regression",
        "metric": "rmse",
        "learning_rate": float(os.environ.get("LGBM_LR", "0.05")),
        "num_leaves": int(os.environ.get("LGBM_NUM_LEAVES", "64")),
        "min_data_in_leaf": int(os.environ.get("LGBM_MIN_DATA_IN_LEAF", "50")),
        "feature_fraction": float(os.environ.get("LGBM_FEATURE_FRACTION", "0.9")),
        "bagging_fraction": float(os.environ.get("LGBM_BAGGING_FRACTION", "0.8")),
        "bagging_freq": int(os.environ.get("LGBM_BAGGING_FREQ", "1")),
    }

    train_data = lgb.Dataset(X_train, label=y_train, feature_name=feature_names)
    val_data = lgb.Dataset(
        X_val, label=y_val, feature_name=feature_names, reference=train_data
    )

    with mlflow.start_run(run_name=f"day_ahead_lgbm_{run_date}") as run:
        mlflow.log_params(params)
        mlflow.log_param("run_date", run_date)
        mlflow.log_param("n_features", len(feature_names))
        mlflow.log_param("feature_names", ",".join(feature_names))

        model = lgb.train(
            params,
            train_data,
            num_boost_round=int(os.environ.get("LGBM_NUM_BOOST_ROUND", "1000")),
            valid_sets=[train_data, val_data],
            valid_names=["train", "val"],
            callbacks=[
                lgb.early_stopping(
                    stopping_rounds=int(
                        os.environ.get("LGBM_EARLY_STOPPING_ROUNDS", "50")
                    ),
                    verbose=False,
                )
            ],
        )

        y_pred_val = model.predict(X_val)
        rmse = float(np.sqrt(mean_squared_error(y_val, y_pred_val)))
        mae = float(mean_absolute_error(y_val, y_pred_val))
        mape = _mape(y_val, y_pred_val)

        mlflow.log_metric("rmse", rmse)
        mlflow.log_metric("mae", mae)
        mlflow.log_metric("mape", mape)

        print(
            f"[train_lgbm] val_rmse={rmse:.3f}, val_mae={mae:.3f}, val_mape={mape:.2f}%"
        )

        # Log model
        mlflow.lightgbm.log_model(
            lgb_model=model,
            artifact_path="model",
            registered_model_name=model_name,
        )

        print(
            f"[train_lgbm] Logged model to MLflow under '{model_name}' with run_id={run.info.run_id}"
        )


if __name__ == "__main__":
    main()
