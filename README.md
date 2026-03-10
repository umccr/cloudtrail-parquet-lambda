# cloudtrail-parquet-lambda

There seems to be very little tooling around making efficient
versions of CloudTrail logs.

AWS have a built-in Glue processor that can convert the compressed
JSON into Parquet which is the default technique.

(here is an example of terraform to deploy that)
https://github.com/alsmola/cloudtrail-parquet-glue

We are looking at a different approach – a Lambda that
can efficiently do the conversion – with the following features:

- Precise control of Parquet output
  - Compression
  - Row group sizing
  - Inclusion of stats
  - Use of 'variant' for JSON (future)
  - Output folders compatible with Hive partitioning
- Entries that slop over into the next day are correctly returned to the actual day
- Schema evolution
- Deploy via Terraform with just a Lambda and event bridge
- Lambda that can be used to backfill/upgrade previous data

## Use downstream

### Polars

```python
import polars as pl

df = pl.scan_parquet("s3://my-bucket/my-prefix/",
                     hive_partitioning=True,
                     hive_schema={"account": pl.String, "region": pl.String, "year": pl.Int16, "month": pl.Int8, "day": pl.Int8},)

df = df.filter(pl.col("account").is_in(["123456789"]))
df = df.filter(pl.col("region").is_in(["ap-southeast-1", "ap-southeast-2"]))

start = Date("2026-01-01")
end = Date("2026-01-30")

df = df.filter(pl.col("year").ge(start.year).and_(pl.col("year").le(end.year)))
df = df.filter(pl.col("month").ge(start.month).and_(pl.col("month").le(end.month)))
df = df.filter(pl.col("day").ge(start.day).and_(pl.col("day").le(end.day)))

return df.collect()
```

### Duck DB

```python
import duckdb

con = duckdb.connect()

con.execute(
    f"""
    CREATE VIEW cloudtrail_logs AS
    SELECT *
    FROM read_parquet(
            's3://my-bucket/my-prefix/**/*.parquet',
            hive_partitioning = true,
            hive_types={{'year': BIGINT, 'month': BIGINT, 'day': BIGINT}}
         )
    """)

logs = con.table("cloudtrail_logs")

result = (
    logs
    .filter("region = 'ap-southeast-2'")
    .filter(f"year >= {start.year} AND year <= {end.year}")
    .filter(f"month >= {start.month} AND month <= {end.month}")
    .filter(f"day >= {start.day} AND day <= {end.day}")
    .pl(lazy=True)
)

return result.collect()
```
## Design decisions

We choose to upgrade all entries to the latest schema. So 1.02 is not
recorded as a 1.02 Parquet - but upgraded to a Parquet consistent with
1.11. This means simpler tooling on the consumption side (all the same
schema, no need for Iceberg etc) - but does mean
we might need to re-run the converter on some schema upgrades.

