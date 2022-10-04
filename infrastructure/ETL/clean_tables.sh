#!/bin/bash

exec >> etl.log 2>&1
echo 'Cleaning tables ...'

# This file was created to run exclusively the cleaning portion of etl_make.sh

## Get configurations
### Get personal configurations
eval $(cat config/db_default_profile.sh)

### Get ETL configs
eval $(cat etl_configs.sh)

## Create functions
### Function to create schema and assign privileges
create_schema(){
    psql -v schema=$1 -v write=$PSQL_ROLE -v read=$READ_PSQL_ROLE -f "utils/create_schema.sql"
}

### Function to run SQL files using write role and output errors
run_psql_file(){
    echo "$(date) $filename"
        errorlog=$(mktemp)
    ret_val="$(psql $PSQL_VARIABLES -f "$filename" 2> "$errorlog")"
    error_val=`cat "$errorlog" | grep 'ERROR'`
        if [ "$error_val" != "" ]
        then
        echo "$(date) ERRORS running $filename"
        echo "$(date) $error_val"
                return 1
        else
                return 0
        fi
}

PSQL_VARIABLES=""

## Create schema and functions
create_schema $ETL_OUTPUT_SCHEMA_NAME
PSQL_VARIABLES="-v input_schema=$DUMP_UPDATED_SCHEMA_NAME -v output_schema=$ETL_OUTPUT_SCHEMA_NAME -v role=$PSQL_ROLE -v limit=$SQL_LIMIT"

create_schema $ETL_OUTPUT_SEMICLEAN_SCHEMA_NAME

# Create tables
for script in `ls data_cleaning/clean_*.sql`
do
	filename=$script
	run_psql_file $ETL_OUTPUT_SCHEMA_NAME || exit 1
done


# Create semantic tables
python semantics/demographics.py
python semantics/client_events.py

filename='semantics/sql/create_ambulance_runs.sql'
run_psql_file || exit 1

filename='semantics/sql/create_jail_bookings.sql'
run_psql_file || exit 1

echo "$(date) Cleaned tables and created semantic tables!!"
