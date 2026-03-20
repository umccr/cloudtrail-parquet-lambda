# cloudtrail-parquet-lambda

There seems to be very little tooling around making efficient
versions of CloudTrail logs.

AWS have a built-in Glue processor that can convert the compressed
JSON into Parquet – which is the default technique used in most
organisations.

(here is an example of terraform to deploy that)
https://github.com/alsmola/cloudtrail-parquet-glue

We are looking at a different approach – a Lambda that
can efficiently do the conversion – with the following features:

- Precise control of Parquet output
  - Compression
  - Suppression of useless large fields (certain tokens)
  - Row group sizing
  - Inclusion of stats
  - Use of 'variant' for JSON (future)
  - Output folders compatible with Hive partitioning
- Discarding high-volume events of limited analytics value (e.g. KMS from AWS managed services)
- Entries that slop over into the next day are correctly returned to the actual day
- Schema evolution control
- Deploy via Terraform with just a Lambda and event bridge
- Lambda that can be used to backfill/upgrade previous data

## Use downstream

We have optimised the generated Parquet files to be as easy to use downstream as possible.

Parquet files are collated per day – including bringing in entries that have been delayed and written
into a later day.

Each set of Parquet for a single day are stored in a folder named according to Hive partitioning
principles. We have maximised the ability to push-down predicates to avoid unnecessary reads.

Fields in the partition are:
* account
* region
* dt (date)

Note that the handling of types with hive partitioning is not always consistent – care should be taken
to interpret all of them as strings (if account is treated as an integer in some cirumstances
accounts with leading zeros can be problems).

### Polars

```python
import polars as pl

df = pl.scan_parquet("s3://my-bucket/my-prefix/",
                     hive_partitioning=True,
                     hive_schema={"account": pl.String, "region": pl.String, "dt": pl.String,)

df = df.filter(pl.col("account").is_in(["123456789"]))
df = df.filter(pl.col("region").is_in(["ap-southeast-1", "ap-southeast-2"]))

start = Date("2026-01-01")
end = Date("2026-01-30")

df = df.filter(pl.col("dt").ge(start).and_(pl.col("dt").le(end)))

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
            hive_partitioning = true
         )
    """)

logs = con.table("cloudtrail_logs")

result = (
    logs
    .filter("region = 'ap-southeast-2'")
    .filter(f"dt >= {start} AND dt <= {end}")
    .pl(lazy=True)
)

return result.collect()
```
## Design decisions

We choose to upgrade all entries to the latest schema. So 1.02 is not
recorded as a 1.02 Parquet – but upgraded to a Parquet consistent with
1.11. This means simpler tooling on the consumption side (all the same
schema, no need for Iceberg, etc) - but does mean
we might need to re-run the converter on some schema upgrades.

