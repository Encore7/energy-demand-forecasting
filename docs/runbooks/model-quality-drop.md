# Runbook: Model Quality Degradation

## Symptoms
- MAE/RMSE exceeds SLO thresholds
- Alert triggered from monitoring dashboard

## Impact
- Reduced forecast reliability
- Potential business impact

## Investigation Steps
1. Identify affected horizon (day-ahead vs intraday)
2. Compare against baseline model
3. Check recent feature distributions
4. Review recent data ingestion anomalies

## Mitigation
- Roll back to last approved model
- Enable shadow evaluation for candidate models
- Trigger retraining DAG if degradation persists

## Escalation
- Notify stakeholders if degradation > tolerance
- Schedule model review
