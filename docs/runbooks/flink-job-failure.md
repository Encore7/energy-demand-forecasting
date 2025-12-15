# Runbook: Flink Job Failure

## Symptoms
- No intraday aggregates being produced
- Streaming lag spikes
- Flink JobManager reports FAILED state

## Impact
- Intraday forecasts frozen or stale
- Increased operational risk

## Investigation Steps
1. Check Flink dashboard (JobManager UI)
2. Inspect failed job logs
3. Check Kafka consumer offsets
4. Review OTEL traces for streaming-service

## Mitigation
- Restart Flink job from latest checkpoint
- If checkpoint unavailable, restart from Kafka offsets
- Validate state backend (e.g. RocksDB)

## Escalation
- If repeated failures occur, disable intraday forecasts
- Fall back to day-ahead baseline temporarily
