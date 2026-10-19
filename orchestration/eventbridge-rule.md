# EventBridge rule: healthcare-claims-on-batch-ready

Captured from AWS Console screenshots on 2026-10-19. This is a configuration snapshot, not IaC.

- AWS Region: us-east-1
- Event bus: default
- Rule type: Standard
- Rule status at teardown preparation: Disabled
- Description: Start healthcare claims ETL when a batch ready manifest arrives
- Input to target: Matched event (entire EventBridge event)
- Target: Step Functions state machine `healthcare-claims-pipeline`
- Target ARN: `arn:aws:states:us-east-1:146699354444:stateMachine:healthcare-claims-pipeline`
- EventBridge target execution role (Console): `Amazon_EventBridge_Invoke_Step_Functions_1273437584` (permissions/trust relationship **not** exported)
- Retry policy (Console): 24h maximum age, 3 retry attempts
- Dead-letter queue: None
- Additional target parameters: None

## Event pattern

```json
{
  "source": ["aws.s3"],
  "detail-type": ["Object Created"],
  "detail": {
    "bucket": {
      "name": ["healthcare-upstream-146699354444-us-east-1-an"]
    },
    "object": {
      "key": [{
        "wildcard": "incoming/batch_*/ready.json"
      }]
    }
  }
}
```

The manifest is uploaded last, so the rule responds only to the matching `ready.json` S3 object creation. Verify S3-to-EventBridge notification setting separately when redeploying. Do not re-enable the rule when only archiving the project.

## Not yet exported

EventBridge target IAM role trust/permission documents; any S3 bucket EventBridge notification configuration. These are required for full reproduction.
