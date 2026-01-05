# ADR-003: Why Flink for Intraday Feature Materialization

## Decision
Use **Apache Flink** to compute intraday features in streaming mode.

## Scope
Flink is used **only** for:
- event-time windowing
- stateful aggregation
- bounded lateness handling
- producing online features

Flink is **not** used for inference.

## Rationale
- Correct handling of event time (15-min blocks)
- Deterministic window semantics
- Built-in fault tolerance and checkpoints
- Avoids re-implementing streaming logic in application code

## Alternatives Considered
- Custom Kafka consumers → rejected (reinventing stream processing)
- Spark Structured Streaming → rejected (micro-batch semantics)
