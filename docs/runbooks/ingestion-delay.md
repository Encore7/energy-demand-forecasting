# Runbook: Data Ingestion Delay

## Symptoms
- Missing or delayed demand data
- Kafka topic lag increasing
- Intraday forecasts not updating

## Impact
- Reduced forecast accuracy
- Potential blind spots for intraday correction

## Investigation Steps
1. Check ingestion-service health endpoint
2. Verify Kafka/Redpanda topic lag
3. Inspect recent ingestion logs (trace_id correlated)
4. Confirm upstream source availability

## Mitigation
- Restart ingestion-service if stalled
- Temporarily switch intraday forecast to last known good data
- Increase allowed lateness window if needed

## Escalation
- If delay > SLA threshold, notify downstream services
- Log incident for postmortem
