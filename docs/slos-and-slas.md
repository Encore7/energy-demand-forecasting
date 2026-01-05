# SLAs and SLOs

## Data SLAs
| Metric | Target |
|------|--------|
| Demand freshness | < 2 minutes |
| Weather freshness | < 10 minutes |

## Model SLOs
| Metric | Target |
|------|--------|
| Intraday pinball loss degradation | < 15% vs baseline |
| Day-ahead pinball loss degradation | < 10% vs baseline |

## System SLOs
| Metric | Target |
|------|--------|
| Intraday inference latency | < 1 minute |
| API p95 latency | < 200 ms |
| Streaming lag | < 30 seconds |
