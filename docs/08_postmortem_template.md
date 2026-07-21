# Incident Postmortem Template

## Summary

- Incident ID:
- Date/Time (UTC):
- Duration:
- Severity:
- Owner:
- Impacted business process:

## What happened

- Trigger event:
- Detection channel:
- First customer/stakeholder impact observed:
- Last known good batch:

## Timeline

- T0:
- T+15:
- T+30:
- T+60:
- Recovery complete:

## Root cause

- Primary technical cause:
- Contributing causes:
- Why existing controls did not prevent it:

## Detection and response quality

- Was alert actionable?
- Was ownership clear?
- Did communication cadence meet runbook expectations?

## Corrective actions

- Immediate remediation:
- Long-term prevention:

## Mandatory reliability upgrade (required)

At least one systemic control must be added/updated from this incident:

- [ ] Contract validation
- [ ] Schema drift guard
- [ ] Reconciliation check
- [ ] Alert quality/business-impact routing
- [ ] Idempotent replay/recovery automation
- [ ] Documentation/runbook update

## Verification

- Test evidence for fix:
- Rollback plan validated:
- CI checks updated:

## Sign-off

- Engineering:
- Analytics/Finance stakeholder:
- Compliance (if applicable):
