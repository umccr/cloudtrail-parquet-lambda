curl -X POST http://localhost:9000/2015-03-31/functions/function/invocations \
  -H 'Content-Type: application/json' \
  -d '{
    "baseInputPath": "test_input/",
    "baseOuptutPath": "test_output/",
    "processDate": "2020-09-30",
    "accountId": "811596193553",
    "organisationId": "o-abcd"
  }'
