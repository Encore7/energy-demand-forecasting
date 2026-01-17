import numpy as np

from pipelines.monitoring.detect_drift_day_ahead import psi  # if you exposed psi there


def test_psi_zero_when_distributions_equal():
    sample = np.random.normal(loc=0.0, scale=1.0, size=1000)
    val = psi(sample, sample, bins=10)
    assert abs(val) < 1e-3  # basically no drift


def test_psi_positive_when_shifted():
    base = np.random.normal(loc=0.0, scale=1.0, size=1000)
    recent = np.random.normal(loc=2.0, scale=1.0, size=1000)
    val = psi(base, recent, bins=10)
    assert val > 0.1  # drift should be non-trivial
