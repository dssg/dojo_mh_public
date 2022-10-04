#!/bin/bash

exec >> etl.log 2>&1

echo 'hello'

# 0. SETUP UP
## Get input variables
TEST=false
EXTRACT_DUMP=false
#UPDATE_ENTITIES=false
DROP_CLEANED=false
for i in "$@"
do
    case $i in
		--test)
			TEST=true
			shift
			;;
		--extract-dump)
			EXTRACT_DUMP=true
			shift
			;;
		--drop-cleaned)
			DROP_CLEANED=true
			shift
			;;
	esac
done

echo 'before reading in config!'

## Get configurations
### Get personal configurations
eval $(cat config/db_default_profile.sh)

### Get ETL configs
eval $(cat etl_configs.sh) 

echo 'after reading in config!'


## Change parameters if in test mode
if $TEST
then
	export DUMP_UPDATED_SCHEMA_NAME=$TEST_PREFIX$DUMP_UPDATED_SCHEMA_NAME
	psql -c "SET ROLE $PSQL_ROLE; CREATE SCHEMA IF NOT EXISTS $DUMP_UPDATED_SCHEMA_NAME;"
        export ETL_OUTPUT_SCHEMA_NAME=$TEST_PREFIX$ETL_OUTPUT_SCHEMA_NAME
        export ENTITIES_SCHEMA=$TEST_PREFIX$ENTITIES_SCHEMA
	export CLEAN_DUMP_SQL_PATH=raw/clean_mock.sql
	export SQL_LIMIT=5
else
	export SQL_LIMIT=ALL
fi

echo 'after test mode settings'

## Create functions
### Function to create schema and assign privileges
create_schema(){
    psql -v schema=$1 -v write=$PSQL_ROLE -v read=$READ_PSQL_ROLE -f "utils/create_schema.sql"
}

### Function to drop schema
drop_schema(){
    psql -v schema=$1 -v write=$PSQL_ROLE -f "utils/drop_schema.sql"
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

# Fabian: Not using this at the moment, might use it later when we create metadata
### Function to create metadata schema and tables
create_metadata(){
    echo "$(date) Creating metadata about ${1}. This may take more than 10 minutes."
    METADATA_SCHEMA_NAME="${1}_metadata"
    create_schema $METADATA_SCHEMA_NAME
    PSQL_VARIABLES="-v input_schema=$1 -v output_schema=$METADATA_SCHEMA_NAME -v role=$PSQL_ROLE"
    filename='utils/create_row_counts.sql'
    run_psql_file || exit 1
}

# 1. EXTRACT DUMP
if $EXTRACT_DUMP
then
    ## Run extraction of dump if given extract dump
    create_schema $DUMP_UPDATED_SCHEMA_NAME
    echo "$(date) EXTRACTING DUMP"
	./raw/clean_dump.sh -o=$CLEAN_DUMP_SQL_PATH -v=DUMP_UPDATED_SCHEMA_NAME:$DUMP_UPDATED_SCHEMA_NAME -v=PSQL_ROLE:$PSQL_ROLE -v=DUMP_ORIGINAL_SCHEMA_NAME:$DUMP_ORIGINAL_SCHEMA_NAME -v=DUMP_FILE_NAME:$DUMP_FILE_NAME -v=READ_PSQL_ROLE:$READ_PSQL_ROLE
	psql -f "$CLEAN_DUMP_SQL_PATH" || exit 1
    
    ## Generate some metadata about the loaded tables
	# Fabian: Erika said create_row_counts is pretty slow and we don't need this at this stage, so commented out
    #create_metadata $DUMP_UPDATED_SCHEMA_NAME

else
    echo "$(date) NO NEED TO EXTRACT DUMP"
fi

# 2. UPDATE ENTITIES
if $ENTITIES
then
	## Update entity records
	echo "UPDATING $(ENTITIES_SCHEMA)"
	PSQL_VARIABLES="-v input_schema=$DUMP_UPDATED_SCHEMA_NAME -v output_schema=$ENTITIES_SCHEMA -v write=$PSQL_ROLE -v read=$READ_PSQL_ROLE"
	for script in `ls entities/*`
	do
		filename=$script
		run_psql_file || exit 1
	done
else
	echo "$(date) NO NEED TO UPDATE $(ENTITIES_SCHEMA)"
fi


# 3. DATA CLEANING
PSQL_VARIABLES=""

if $CLEANING
then
    ## Create schema and functions
    create_schema $ETL_OUTPUT_SCHEMA_NAME
	PSQL_VARIABLES="-v input_schema=$DUMP_UPDATED_SCHEMA_NAME -v output_schema=$ETL_OUTPUT_SCHEMA_NAME -v role=$PSQL_ROLE -v limit=$SQL_LIMIT"
	filename=data_cleaning/_functions.sql
	run_psql_file || exit 1

    # Create tables
	for script in `ls data_cleaning/clean_*.sql`
	do		
		# TODO: Might need to prepend 'data_cleaning/' to filename
		filename=$script
		run_psql_file $ETL_OUTPUT_SCHEMA_NAME || exit 1
	done
    # create_metadata $ETL_OUTPUT_SCHEMA_NAME

	# TODO: Test block below and uncomment to ensure semi-cleaning python scripts are run
	# for script in `ls data_cleaning/*clean*.py`
	# do
	# 	python $script
	# done
fi

if $TEST
then
      exit 0
fi

# 4. RECORDS CREATION
#    Create a records table. At the end of this step,
#    every event type should have exactly 1 table we
#    need to handle.
if $RECORDS
then
    echo 'in the loop!'
    create_schema $RECORDS_SCHEMA
	PSQL_VARIABLES="-v input_schema=$ETL_OUTPUT_SCHEMA_NAME -v output_schema=$RECORDS_SCHEMA -v role=$PSQL_ROLE"
	## Create tables
    for script in `ls records/create_*_records.sql`
	do
		filename=$script
		run_psql_file || exit 1
	done

    ## Populate tables
	for script in `ls records/insert_*_records.sql`
	do
		filename=$script
		run_psql_file || exit 1
	done
fi

# 5. Add entity_id to records tables using _to_entities table.
if $RECORDS
then
	filename=utils/add_entity_id.sql
    PSQL_VARIABLES=" -v output_schema=$RECORDS_SCHEMA -v input_schema=$ENTITIES_SCHEMA -v role=$PSQL_ROLE"
	run_psql_file || exit 1
fi

# 6. Drop cleaned schema after records creation if flag
if $DROP_CLEANED
then
	echo "DROP_CLEANED is $DROP_CLEANED"
	drop_schema $ETL_OUTPUT_SCHEMA_NAME 
fi

# 7. Create events table
if $EVENTS
then
    PSQL_VARIABLES="-v output_schema=$SEMANTIC_SCHEMA -v input_schema=$RECORDS_SCHEMA -v role=$PSQL_ROLE -v entities_schema=$ENTITIES_SCHEMA -v read=$READ_PSQL_ROLE -v write=$PSQL_ROLE"

    ## Create schema if needed, populate tables
    for script in `ls semantics/*.sql`
    do
        filename=$script
        run_psql_file || exit 1
    done
fi

if $CLEANING
then
    /bin/bash clean_tables.sh
fi

if $CREATE_SEMANTIC_TABLES
then
    /bin/bash create_semantic.sh
fi

echo "$(date) Ding! Ding! Ding! All done!!"
