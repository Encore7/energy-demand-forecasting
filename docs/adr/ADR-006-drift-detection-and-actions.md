# ADR-006: Drift Detection & Actions

## Decision
Drift detection is implemented as a **first-class ML system capability**.

## Drift Types
- Feature distribution drift
- Prediction residual drift
- Data freshness / volume anomalies

## Actions
- Alert
- Shadow evaluation
- Model rollback
- Retraining trigger

## Invariant
Drift detection applies to **both** day-ahead and intraday pipelines independently.
