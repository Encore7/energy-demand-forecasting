# Assumptions & Constraints

## Data Assumptions
- Demand data may arrive late or out-of-order
- Intraday data arrives at 15-minute granularity
- Weather inputs are probabilistic and imperfect

## Modeling Assumptions
- Classical baselines provide reasonable performance
- ML/DL models improve residuals and uncertainty estimates
- Intraday models act as **correction layers**, not replacements

## Operational Constraints
- Local-first development
- Open-source tooling
- Cloud deployment optional but architecture-compatible