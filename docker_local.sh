docker build --platform linux/arm64 --target local -t cloudtrail-to-parquet-lambda:local .

# need to make this bind in the current directory so that the simulated event works
# is fine currently because all we are really testing is that the lambda is invoked

docker run --platform linux/arm64 \
  -p 9000:8080 \
  cloudtrail-to-parquet-lambda:local
