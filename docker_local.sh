docker build --platform linux/arm64 --target local -t cloudtrail-to-parquet-lambda:local .

docker run --platform linux/arm64 \
  -p 9000:8080 \
  -e CLOUDTRAIL_BASE_PATH=test_input/ \
  -e OUTPUT_PATH=test_output/ \
  -e ORGANISATION_ID=o-abcd \
  cloudtrail-to-parquet-lambda:local
