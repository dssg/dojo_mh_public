# ETL Pipeline
This pipeline is adapted from the [DSSG 2018 project](https://github.com/dssg/johnson-county-ddj/tree/master/ETL).

### How to use the ETL pipeline
Go to `etl_configs.sh` and set values for the ETL and Postgres relevant variables. In particular, set
- `DUMP_FILE_NAME`: the path to the psql dump object
- `PSQL_ROLE`: desired psql role name
- `DUMP_DATABASE_NAME`: name of the dump-generating database
- `DATABASE_NAME`: name of the database
- `READ_PSQL_ROLE`: read-permission psql role
- `CLEANING`: whether to clean the data in, addition to dumping

Next, run `etl_make.sh` with the appropriate flags. E.g.,
```
./etl_make.sh --extract-dump
```

Once finished, the database should be populated. Check `etl.log` for task progress and errors.

### Minor changes from original version
    - Flattened the folder structure so that config is now in this folder rather than the one above.
    - Removed functions / sql files we do not need at this stage (e.g. for creating metadata, entities, events).
    - Only creates the raw schema, does not clean the tables.
