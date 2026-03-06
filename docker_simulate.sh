curl -X POST http://localhost:9000/2015-03-31/functions/function/invocations \
  -H 'Content-Type: application/json' \
  -d '{
    "version": "0",
    "id": "test-event-123",
    "detail-type": "Scheduled Event",
    "source": "aws.events",
    "account": "123456789012",
    "time": "2020-10-01T01:00:00Z",
    "region": "ap-southeast-2",
    "resources": ["arn:aws:events:ap-southeast-2:123456789012:rule/cloudtrail-to-parquet-daily"],
    "detail": {}
  }'
