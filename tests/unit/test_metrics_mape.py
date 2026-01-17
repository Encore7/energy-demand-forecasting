import numpy as np


def compute_mape(y_true, y_pred):
    y_true = np.asarray(y_true, dtype=float)
    y_pred = np.asarray(y_pred, dtype=float)
    mask = y_true != 0
    return float(np.mean(np.abs((y_true[mask] - y_pred[mask]) / y_true[mask])) * 100.0)


def test_mape_basic_case():
    y_true = [100, 200, 300]
    y_pred = [
        110,
        190,
        330,
    ]  # errors: 10, 10, 30 => relative: 0.1, 0.05, 0.1 => mean=0.0833
    mape = compute_mape(y_true, y_pred)
    assert 8.0 < mape < 9.0  # ~8.33%


def test_mape_ignores_zero_targets():
    y_true = [0, 100, 0, 200]
    y_pred = [10, 110, 20, 180]

    mape = compute_mape(y_true, y_pred)
    # Only non-zero targets: (100, 200) and preds (110, 180)
    # rel errors: 0.1, 0.1 -> MAPE=10%
    assert 9.0 < mape < 11.0
