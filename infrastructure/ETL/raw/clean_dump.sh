#!/bin/bash
eval $(cat config/db_default_profile.sh)


# obtain command line variables
while [ ! -z $1 ]; do
    echo "$(date) $1"
    PARAM=`echo $1 | awk -F= '{print $1}'`
	case $PARAM in
		-v|--var)
			VARIABLE_NAME=`echo $1 | awk -F= '{print $2}' | awk -F: '{print $1}'`
			VARIABLE_VAL=`echo $1 | awk -F= '{print $2}' | awk -F: '{print $2}'`
			eval "export $VARIABLE_NAME=$VARIABLE_VAL"
			;;
		-o|--outpath)
			VARIABLE_VAL=`echo $1 | awk -F= '{print $2}'`
			eval "export OUTPUT_PATH=$VARIABLE_VAL"
			;;
	esac
	shift
done

# Choosing exporting method
INPUT_FILE_TYPE=`echo $DUMP_FILE_NAME | awk -F. '{print $2}'`
case $INPUT_FILE_TYPE in
	zip)
		eval "export ACTION='unzip -p'"
		;;
	"")
		eval "export ACTION='cat'"
		;;
esac

# run actual code
export psql_remove_tables=$(psql -t --quiet -c "select tablename from pg_tables where schemaname='$DUMP_UPDATED_SCHEMA_NAME';" | sed "s/^ /drop table if exists $DUMP_UPDATED_SCHEMA_NAME./g; s/\$/ CASCADE;/g" | sed "1s/^/SET ROLE $PSQL_ROLE;\n/g"| head -n -2)
#echo $psql_remove_tables
eval "$ACTION $DUMP_FILE_NAME.backup" |  
	grep -v '^-- *\|^$\|DROP DATABASE\|CREATE DATABASE\|ALTER DATABASE\|ALTER SCHEMA\|ALTER TABLE\|CREATE EXTENSION\|COMMENT ON SCHEMA\|^\\connect \|COMMENT ON EXTENSION\|postgres\|REVOKE ALL ON SCHEMA\|GRANT ALL ON SCHEMA' |
	sed "s/CREATE SCHEMA $DUMP_ORIGINAL_SCHEMA_NAME/CREATE SCHEMA IF NOT EXISTS $DUMP_UPDATED_SCHEMA_NAME/g; s/SET search_path = $DUMP_ORIGINAL_SCHEMA_NAME/SET search_path = $DUMP_UPDATED_SCHEMA_NAME/g; s/CREATE TABLE $DUMP_ORIGINAL_SCHEMA_NAME/CREATE TABLE IF NOT EXISTS $DUMP_UPDATED_SCHEMA_NAME/g; s/COPY $DUMP_ORIGINAL_SCHEMA_NAME/COPY $DUMP_UPDATED_SCHEMA_NAME/g" |
    sed "1s/^/SET ROLE $PSQL_ROLE; /" |
       	awk -v var="$psql_remove_tables" 'BEGINFILE{print var}{print}' > "${OUTPUT_PATH}"
