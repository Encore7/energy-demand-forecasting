from __future__ import annotations

import os
from typing import List

import mlflow
import numpy as np
import pandas as pd
import pyarrow.parquet as pq
import s3fs
import torch
from pytorch_forecasting import TemporalFusionTransformer, TimeSeriesDataSet
from pytorch_forecasting.metrics import QuantileLoss
from pytorch_lightning import Trainer
from sklearn.metrics import mean_absolute_error, mean_squared_error

LABEL_COL = "target_load_mw"


def _load_parquet_to_df(fs: s3fs.S3FileSystem, bucket: str, key: str) -> pd.DataFrame:
    with fs.open(f"{bucket}/{key}", "rb") as f:
        table = pq.read_table(f)
    return table.to_pandas()


def _mape(y_true: np.ndarray, y_pred: np.ndarray) -> float:
    denom = np.where(y_true == 0, np.nan, y_true)
    mape = np.nanmean(np.abs((y_true - y_pred) / denom)) * 100.0
    return float(mape)


def _prepare_time_index(df: pd.DataFrame) -> pd.DataFrame:
    # Single national series; still keep series_id column for future scaling to sub-regions.
    if "series_id" not in df.columns:
        df["series_id"] = "DE"

    df = df.sort_values(["series_id", "event_ts_utc"]).copy()
    # Create integer time index per series for TFT
    df["time_idx"] = (
        df.groupby("series_id")["event_ts_utc"].rank(method="first").astype(int) - 1
    )
    return df


def _build_tsdataset(
    df: pd.DataFrame,
    max_encoder_length: int,
    max_prediction_length: int,
    feature_cols: List[str],
) -> TimeSeriesDataSet:
    return TimeSeriesDataSet(
        df,
        time_idx="time_idx",
        target=LABEL_COL,
        group_ids=["series_id"],
        max_encoder_length=max_encoder_length,
        max_prediction_length=max_prediction_length,
        time_varying_known_real=feature_cols,
        time_varying_unknown_real=[LABEL_COL],
        # static_categoricals / static_reals can be added later if we add regions, etc.
    )


def main() -> None:
    run_date = os.environ["RUN_DATE"]
    repo = os.environ["LAKEFS_REPO"]
    branch = os.environ["LAKEFS_BRANCH"]

    lakefs_s3_endpoint = os.environ["LAKEFS_S3_ENDPOINT_INTERNAL"]
    aws_key = os.environ["LAKEFS_ACCESS_KEY_ID"]
    aws_secret = os.environ["LAKEFS_SECRET_ACCESS_KEY"]
    region = os.environ["S3_REGION"]

    mlflow_tracking_uri = os.environ["MLFLOW_TRACKING_URI"]
    experiment_name = os.environ.get("MLFLOW_EXPERIMENT_NAME_TFT", "day_ahead_tft")
    model_name = os.environ.get("MODEL_NAME_TFT", "day_ahead_tft")

    fs = s3fs.S3FileSystem(
        key=aws_key,
        secret=aws_secret,
        client_kwargs={"endpoint_url": lakefs_s3_endpoint, "region_name": region},
    )

    base_prefix = f"{branch}/artifacts/day_ahead/trainsets/run_date={run_date}/"
    train_key = f"{base_prefix}train.parquet"
    val_key = f"{base_prefix}val.parquet"

    print(f"[train_tft] Loading train from s3://{repo}/{train_key}")
    print(f"[train_tft] Loading val   from s3://{repo}/{val_key}")

    train_df = _load_parquet_to_df(fs, repo, train_key)
    val_df = _load_parquet_to_df(fs, repo, val_key)

    if LABEL_COL not in train_df.columns or LABEL_COL not in val_df.columns:
        raise RuntimeError(f"Label column '{LABEL_COL}' missing from train/val")

    # Ensure event_ts_utc is datetime
    train_df["event_ts_utc"] = pd.to_datetime(train_df["event_ts_utc"])
    val_df["event_ts_utc"] = pd.to_datetime(val_df["event_ts_utc"])

    train_df = _prepare_time_index(train_df)
    val_df = _prepare_time_index(val_df)

    # Feature columns: all numeric except label + trivial meta
    meta_drop = {"event_ts_utc", "dt"}
    num_cols = train_df.select_dtypes(include=["number"]).columns.tolist()
    feature_cols = [c for c in num_cols if c not in {LABEL_COL} | meta_drop]

    print(f"[train_tft] n_features={len(feature_cols)}")

    max_encoder_length = int(
        os.environ.get("TFT_MAX_ENCODER_LENGTH", "168")
    )  # 7 days * 24h
    max_prediction_length = int(
        os.environ.get("TFT_MAX_PRED_LENGTH", "1")
    )  # predict 1 step (t+24h label)

    training = _build_tsdataset(
        train_df, max_encoder_length, max_prediction_length, feature_cols
    )
    validation = _build_tsdataset(
        val_df, max_encoder_length, max_prediction_length, feature_cols
    )

    train_dataloader = training.to_dataloader(
        train=True,
        batch_size=int(os.environ.get("TFT_BATCH_SIZE", "64")),
        num_workers=0,
    )
    val_dataloader = validation.to_dataloader(
        train=False,
        batch_size=int(os.environ.get("TFT_BATCH_SIZE", "64")),
        num_workers=0,
    )

    mlflow.set_tracking_uri(mlflow_tracking_uri)
    mlflow.set_experiment(experiment_name)

    # Basic TFT hyperparams – tune later
    tft_hidden_size = int(os.environ.get("TFT_HIDDEN_SIZE", "32"))
    tft_lstm_layers = int(os.environ.get("TFT_LSTM_LAYERS", "1"))
    tft_attention_head_size = int(os.environ.get("TFT_ATTENTION_HEAD_SIZE", "4"))
    tft_dropout = float(os.environ.get("TFT_DROPOUT", "0.1"))
    max_epochs = int(os.environ.get("TFT_MAX_EPOCHS", "30"))

    pl_trainer = Trainer(
        max_epochs=max_epochs,
        accelerator="cpu",  # change to "gpu" if available
        enable_model_summary=True,
        logger=False,
        enable_checkpointing=False,
    )

    with mlflow.start_run(run_name=f"day_ahead_tft_{run_date}") as run:
        mlflow.log_param("run_date", run_date)
        mlflow.log_param("n_features", len(feature_cols))
        mlflow.log_param("feature_names", ",".join(feature_cols))
        mlflow.log_param("max_encoder_length", max_encoder_length)
        mlflow.log_param("max_prediction_length", max_prediction_length)
        mlflow.log_param("tft_hidden_size", tft_hidden_size)
        mlflow.log_param("tft_lstm_layers", tft_lstm_layers)
        mlflow.log_param("tft_attention_head_size", tft_attention_head_size)
        mlflow.log_param("tft_dropout", tft_dropout)
        mlflow.log_param("max_epochs", max_epochs)

        tft = TemporalFusionTransformer.from_dataset(
            training,
            learning_rate=float(os.environ.get("TFT_LR", "1e-3")),
            hidden_size=tft_hidden_size,
            lstm_layers=tft_lstm_layers,
            attention_head_size=tft_attention_head_size,
            dropout=tft_dropout,
            loss=QuantileLoss(),
            log_interval=10,
            log_val_interval=1,
        )

        pl_trainer.fit(
            tft,
            train_dataloaders=train_dataloader,
            val_dataloaders=val_dataloader,
        )

        # Evaluate on val set
        preds, index = tft.predict(
            val_dataloader,
            mode="prediction",
            return_index=True,
            show_progress_bar=False,
        )

        # preds shape: [N, max_prediction_length]; we set max_prediction_length=1
        preds = preds.squeeze(-1).numpy()
        # Align with ground truth label at that index
        val_target = val_df.iloc[index["time_idx"].to_numpy()][LABEL_COL].to_numpy()

        rmse = float(np.sqrt(mean_squared_error(val_target, preds)))
        mae = float(mean_absolute_error(val_target, preds))
        mape = _mape(val_target, preds)

        mlflow.log_metric("rmse", rmse)
        mlflow.log_metric("mae", mae)
        mlflow.log_metric("mape", mape)

        print(
            f"[train_tft] val_rmse={rmse:.3f}, val_mae={mae:.3f}, val_mape={mape:.2f}%"
        )

        # Log model; store as a TorchScript or pickled model
        model_path = "tft_model"
        tft.to_torchscript(file_path=model_path + ".pt")
        mlflow.log_artifact(model_path + ".pt", artifact_path="model")

        mlflow.set_tag("model_name", model_name)
        print(
            f"[train_tft] Logged TFT model artifact to MLflow run_id={run.info.run_id}"
        )
