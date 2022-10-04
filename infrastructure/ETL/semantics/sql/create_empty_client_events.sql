/*
Create empty client events table.
*/

set role {psql_role};

drop table if exists semantic.client_events;

create table semantic.client_events (
	joid int,
	event_date date,
	event_type varchar,
    table_name varchar
);
