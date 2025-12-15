# Runbook: Drift Detection Alert

## Symptoms
- Feature drift alert triggered
- Residual distribution shift detected

## Impact
- Model predictions may become unreliable

## Investigation Steps
1. Identify drift type (feature vs residual)
2. Inspect affected features
3. Check recent upstream data changes
4. Validate data freshness and volume

## Mitigation
- Trigger retraining workflow
- Increase monitoring frequency
- Adjust feature preprocessing if required

## Escalation
- If drift persists after retraining, escalate to model owners
