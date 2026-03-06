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
- Schema evolution
- Deploy via Terraform with just a Lambda and event bridge
- Lambda that can be used to backfill/upgrade previous data

## Design decisions

We choose to upgrade all entries to the latest schema. So 1.02 is not
recorded as a 1.02 Parquet - but upgraded to a Parquet consistent with
1.11. This means simpler tooling on the consumption side (all the same
schema, no need for Iceberg etc) - but does mean
we might need to re-run the converter on some schema upgrades.
