/*
Create empty demographics events table.
*/

set role {psql_role};

drop table if exists semantic.demographics;

create table semantic.demographics (
	joid int,
	event_date date,
    table_name varchar,
	demographics_type varchar,
    demographics_value varchar
);
