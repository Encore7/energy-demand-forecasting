# ADR-004: Feature Store Design

## Decision
Maintain **separate offline and online feature stores** with shared feature definitions.

## Offline Features
- Used for training and backtesting
- Generated via batch pipelines
- Stored in immutable, versioned datasets

## Online Features
- Used for intraday inference
- Continuously updated via streaming pipelines
- Stored in low-latency store (Redis)

## Rationale
- Training–serving consistency
- Low-latency inference without recomputation
- Clear feature ownership and contracts

## Invariant
> Models never compute features themselves.
