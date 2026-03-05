There seems to be very little tooling around making efficient
versions of CloudTrail logs.

AWS have a built in Glue processor that can convert the compressed
JSON into Parquet equivalents - and it appears that everyone uses that.

(here is an example of terraform to deploy that)
https://github.com/alsmola/cloudtrail-parquet-glue

I personally think Glue is a bit of a mess, and there is very little
documentation about what is produced (i.e. where is the Parquet schema
defined, how does it evolve over time as CloudTrail evolves).

To some extent - this is all fixed by CloudTrail Lake - which replaces
the Glue blackbox, with a different blackbox. However, now we are needing
to use CloudTrail Lake SQL for queries.

## Design decisions

We choose to upgrade all entries to the latest schema. So 1.02 is not
recorded as a 1.02 Parquet - but upgraded to a Parquet consistent with
1.11. This means simpler tooling on the consumption side (all the same
schema, no need for Iceberg etc) - but does mean
we might need to re-run the converter on some schema upgrades.
