# Runbook: Model Rollback

## When to Roll Back
- Severe model performance degradation
- Incorrect predictions detected
- Failed deployment

## Steps
1. Identify last stable model version in MLflow
2. Update model registry to mark rollback version as active
3. Restart affected forecast services
4. Verify predictions via canary requests

## Post-Rollback
- Document incident
- Analyze root cause
- Update ADRs or training pipeline if needed
