#!/bin/bash

exec >> etl.log 2>&1
echo 'Creating semantic tables ...'

# This file was created to run exclusively the creating the semantic portion of etl_make.sh

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

## Get configurations
### Get personal configurations
eval $(cat config/db_default_profile.sh)

### Get ETL configs
eval $(cat etl_configs.sh)

# Create semantic tables
python semantics/demographics.py $PSQL_ROLE
python semantics/client_events.py $PSQL_ROLE

filename='semantics/sql/create_ambulance_runs.sql'
run_psql_file || exit 1

filename='semantics/sql/create_jail_bookings.sql'
run_psql_file || exit 1

filename='semantics/sql/create_joco_calls.sql'
run_psql_file || exit 1

filename='semantics/sql/create_empty_diagnoses.sql'
run_psql_file || exit 1

filename='semantics/sql/create_empty_demographics_eval.sql'
run_psql_file || exit 1

echo "$(date) Cleaned tables and created semantic tables!!"
