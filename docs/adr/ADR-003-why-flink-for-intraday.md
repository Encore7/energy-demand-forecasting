# ADR-003: Why Flink for Intraday

## Decision
Use Apache Flink for intraday aggregation.

## Rationale
- Event-time windows
- Bounded lateness
- Stateful operators
- Fault-tolerant checkpoints

## Alternatives
- Custom Kafka consumers: rejected (reinventing Flink)
- Spark Streaming: rejected (micro-batch latency)
